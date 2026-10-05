//! Webhook deliveries: somewhere for a callback to land (AgentTests #345).
//!
//! An agent asked to "run the test ping before you post a link" needs a URL
//! that receives. With `hooks_port` set, the bridge listens there (loopback)
//! for `POST /hooks/<name>` and nothing else, so a tunnel or proxy pointed at
//! that port exposes only this. Each delivery is written to `hooks/` beside
//! the state, newest 100 kept, and the agent reads them with the
//! `trellis_webhooks` tool. Nothing here is trusted: a delivery is data the
//! agent inspects (a signature to check, a body to compare), never an order.

use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Where senders reach this bridge's hooks; set when the listener starts.
static RECEIVE_AT: OnceLock<String> = OnceLock::new();

const MAX_BODY: u64 = 1024 * 1024;
const KEEP: usize = 100;
/// Headers whose values are never written down.
const REDACTED: &[&str] = &["authorization", "cookie", "proxy-authorization"];

pub fn dir() -> Result<PathBuf, String> {
    Ok(crate::config::state_path()?.with_file_name("hooks"))
}

pub fn serve(port: u16, public_url: String) -> Result<(), String> {
    let addr = format!("127.0.0.1:{port}");
    let server = tiny_http::Server::http(&addr).map_err(|e| format!("hooks {addr}: {e}"))?;
    let dir = dir()?;
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let shown = if public_url.is_empty() { format!("http://{addr}") } else { public_url.trim_end_matches('/').to_string() };
    println!("hooks     POST {shown}/hooks/<name> is recorded for trellis_webhooks");
    let _ = RECEIVE_AT.set(format!("{shown}/hooks/<name>"));
    std::thread::spawn(move || {
        for mut req in server.incoming_requests() {
            let (status, body) = receive(&dir, &mut req);
            let resp = tiny_http::Response::from_string(body).with_status_code(status).with_header(
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).expect("static header"),
            );
            let _ = req.respond(resp);
        }
    });
    Ok(())
}

fn receive(dir: &Path, req: &mut tiny_http::Request) -> (u16, String) {
    let url = req.url().to_string();
    let (path, query) = url.split_once('?').unwrap_or((&url, ""));
    let Some(name) = path.strip_prefix("/hooks/").filter(|n| valid_name(n)) else {
        return (404, json!({ "error": "POST /hooks/<name> (letters, digits, - and _)" }).to_string());
    };
    if req.method() != &tiny_http::Method::Post {
        return (405, json!({ "error": "POST only" }).to_string());
    }
    let mut body = Vec::new();
    use std::io::Read;
    match req.as_reader().take(MAX_BODY + 1).read_to_end(&mut body) {
        Ok(n) if n as u64 > MAX_BODY => return (413, json!({ "error": "body over 1 MB" }).to_string()),
        Ok(_) => {}
        Err(e) => return (400, json!({ "error": format!("unreadable body: {e}") }).to_string()),
    }
    let headers: Vec<(String, String)> =
        req.headers().iter().map(|h| (h.field.as_str().as_str().to_string(), h.value.as_str().to_string())).collect();
    match record(dir, name, query, &headers, &body) {
        Ok(id) => (200, json!({ "received": id }).to_string()),
        Err(e) => {
            eprintln!("trellisbridge: webhook not recorded: {e}");
            (500, json!({ "error": "not recorded" }).to_string())
        }
    }
}

fn valid_name(n: &str) -> bool {
    !n.is_empty() && n.len() <= 64 && n.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Write one delivery and drop the oldest past `KEEP`. Returns its id.
pub fn record(dir: &Path, name: &str, query: &str, headers: &[(String, String)], body: &[u8]) -> Result<String, String> {
    let ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
    let mut id = format!("{ms}-{name}");
    let mut n = 1;
    while dir.join(format!("{id}.json")).exists() {
        n += 1;
        id = format!("{ms}-{name}-{n}");
    }
    let headers: Vec<Value> = headers
        .iter()
        .map(|(k, v)| {
            let v = if REDACTED.contains(&k.to_ascii_lowercase().as_str()) { "[redacted]" } else { v.as_str() };
            json!([k, v])
        })
        .collect();
    let (text, binary) = match std::str::from_utf8(body) {
        Ok(t) => (t.to_string(), false),
        Err(_) => (String::from_utf8_lossy(body).into_owned(), true),
    };
    let rec = json!({
        "id": id, "name": name, "at_unix_ms": ms as u64, "query": query,
        "headers": headers, "bytes": body.len(), "binary": binary, "body": text,
    });
    let path = dir.join(format!("{id}.json"));
    std::fs::write(&path, rec.to_string()).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut all = ids(dir);
    while all.len() > KEEP {
        let _ = std::fs::remove_file(dir.join(format!("{}.json", all.remove(0))));
    }
    Ok(id)
}

/// Every recorded id, oldest first (ids start with the time in ms).
fn ids(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| e.file_name().to_str().and_then(|n| n.strip_suffix(".json")).map(str::to_string))
        .collect();
    v.sort_by_key(|id| (id.split('-').next().and_then(|t| t.parse::<u128>().ok()).unwrap_or(0), id.clone()));
    v
}

/// The MCP tool's answer for this bridge.
pub fn answer(id: Option<&str>) -> String {
    let Some(at) = RECEIVE_AT.get() else {
        return json!({ "error": "webhooks are off on this bridge: the operator sets hooks_port (and hooks_url, \
            the public address a tunnel or proxy gives that port) in its config" }).to_string();
    };
    match dir() {
        Ok(d) => tool(&d, id, at),
        Err(e) => json!({ "error": e }).to_string(),
    }
}

/// The `trellis_webhooks` tool: newest first without an id, one in full with it.
pub fn tool(dir: &Path, id: Option<&str>, receive_url: &str) -> String {
    if let Some(id) = id {
        if !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
            return json!({ "error": "no such delivery" }).to_string();
        }
        return std::fs::read_to_string(dir.join(format!("{id}.json")))
            .unwrap_or_else(|_| json!({ "error": "no such delivery" }).to_string());
    }
    let list: Vec<Value> = ids(dir)
        .iter()
        .rev()
        .filter_map(|id| std::fs::read_to_string(dir.join(format!("{id}.json"))).ok())
        .filter_map(|t| serde_json::from_str::<Value>(&t).ok())
        .map(|r| json!({ "id": r["id"], "name": r["name"], "at_unix_ms": r["at_unix_ms"], "bytes": r["bytes"] }))
        .collect();
    json!({ "receive_at": receive_url, "count": list.len(), "deliveries": list }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deliveries_are_kept_newest_last_and_secrets_are_not_written() {
        let dir = std::env::temp_dir().join(format!("tb-hooks-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let h = vec![("X-Signature".to_string(), "sha256=ab".to_string()), ("Authorization".to_string(), "Bearer s3cret".to_string())];
        for i in 0..(KEEP + 3) {
            record(&dir, "ping", "", &h, format!("{{\"n\":{i}}}").as_bytes()).unwrap();
        }
        let all = ids(&dir);
        assert_eq!(all.len(), KEEP);
        let newest = std::fs::read_to_string(dir.join(format!("{}.json", all.last().unwrap()))).unwrap();
        assert!(newest.contains(&format!("\\\"n\\\":{}", KEEP + 2)), "{newest}");
        assert!(newest.contains("sha256=ab") && !newest.contains("s3cret"), "{newest}");
        let list = tool(&dir, None, "https://x/hooks/");
        assert!(list.contains("\"count\":100"), "{list}");
        assert!(tool(&dir, Some("../state"), "").contains("no such delivery"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn names_are_plain() {
        assert!(valid_name("reapption-test_1"));
        assert!(!valid_name("") && !valid_name("a/b") && !valid_name("..") && !valid_name(&"x".repeat(65)));
    }
}
