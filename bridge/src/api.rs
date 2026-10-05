//! The HTTP API. Loopback, blocking, no async runtime.
//!
//! There are no approval steps and no queues: every call either does the thing
//! or returns an error saying why it could not. A caller must never have to
//! wait for a human to click something.
//!
//! Each request gets its own thread, because `GET /api/events?wait=` holds a
//! request open and one held request must not stall the others.

use crate::bridge::{Bridge, Chan};
use crate::trellis::in_doc;
use crate::config::Config;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The longest `wait` a caller may ask for, in seconds.
const MAX_WAIT: u64 = 30;

pub fn serve(cfg: &Config, bridge: Arc<Bridge>) -> Result<(), String> {
    let addr = format!("127.0.0.1:{}", cfg.port);
    let server = tiny_http::Server::http(&addr).map_err(|e| format!("{addr}: {e}"))?;
    println!("trellisbridge {VERSION} on http://{addr}");

    for mut req in server.incoming_requests() {
        let bridge = Arc::clone(&bridge);
        let key = cfg.api_key.clone();
        std::thread::spawn(move || {
            let url = req.url().to_string();
            let (path, query) = url.split_once('?').unwrap_or((&url, ""));
            let method = req.method().as_str().to_string();
            let authed = has_key(&req, &key);
            let mut body = Vec::new();
            use std::io::Read;
            // Base64 of a 40 MB file, plus the JSON around it.
            let limit = crate::trellis::MAX_FILE * 4 / 3 + 64 * 1024;
            let reply = match req.as_reader().take(limit + 1).read_to_end(&mut body) {
                Ok(n) if n as u64 > limit => Reply::json(413, error("body over the 40 MB file limit")),
                Ok(_) => route_bytes(&bridge, &method, path, query, &body, authed),
                Err(e) => Reply::json(400, error(&format!("unreadable body: {e}"))),
            };
            let mut response = tiny_http::Response::from_data(reply.body)
                .with_status_code(reply.status)
                .with_header(
                    tiny_http::Header::from_bytes(&b"Content-Type"[..], reply.content_type.as_bytes())
                        .unwrap_or_else(|_| json_header()),
                );
            if let Some(name) = reply.filename {
                let v = format!("attachment; filename=\"{}\"", name.replace('"', ""));
                if let Ok(h) = tiny_http::Header::from_bytes(&b"Content-Disposition"[..], v.as_bytes()) {
                    response = response.with_header(h);
                }
            }
            let _ = req.respond(response);
        });
    }
    Ok(())
}

/// A response: JSON almost always, the bytes of a file for `GET /api/files`.
pub struct Reply {
    pub status: u16,
    pub body: Vec<u8>,
    pub content_type: String,
    pub filename: Option<String>,
}

impl Reply {
    fn json(status: u16, body: String) -> Reply {
        Reply { status, body: body.into_bytes(), content_type: "application/json".into(), filename: None }
    }
}

/// The byte-level router: files and MCP here, everything JSON in `route`.
pub fn route_bytes(bridge: &Bridge, method: &str, path: &str, query: &str, body: &[u8], authed: bool) -> Reply {
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    match (method, parts.as_slice()) {
        _ if !authed && path != "/api/health" => Reply::json(401, error("missing or invalid API key")),
        ("POST", ["mcp"]) => {
            let (s, b) = crate::mcp::handle(bridge, body);
            Reply::json(s, b)
        }
        ("GET", ["mcp"]) => Reply::json(405, error("MCP here is POST only (JSON responses, no SSE stream)")),
        // Any route that answers bytes — a card's image, an inline image, an
        // export (png, pdf, svg…), an attachment — for the plugin to save to a
        // file. Same refusals as the MCP tool.
        ("GET", ["api", "trellis-bytes"]) => {
            let Some(target) = param(query, "path").map(unescape) else {
                return Reply::json(400, error("?path=/api/… is required"));
            };
            if let Some(why) = crate::mcp::refuse("GET", &target) {
                return Reply::json(403, error(&why));
            }
            let client = match bridge.client() {
                Ok(c) => c,
                Err(e) => return Reply::json(503, error(&e.message)),
            };
            let target = match param(query, "document") {
                Some(d) => in_doc(&target, &unescape(d)),
                None => target,
            };
            match client.bytes(&target) {
                Ok((bytes, name, ctype)) => Reply { status: 200, body: bytes, content_type: ctype, filename: name },
                Err(e) => Reply::json(e.status.unwrap_or(503), error(&e.message)),
            }
        }
        ("GET", ["api", "files", card, idx]) => {
            let (Ok(card), Ok(idx)) = (card.parse::<u64>(), idx.parse::<u64>()) else {
                return Reply::json(400, error("GET /api/files/{card}/{index}, both numbers"));
            };
            let client = match bridge.client() {
                Ok(c) => c,
                Err(e) => return Reply::json(503, error(&e.message)),
            };
            let path = format!("/api/cards/{card}/attachments/{idx}");
            let path = match param(query, "document") {
                Some(d) => in_doc(&path, &unescape(d)),
                None => path,
            };
            match client.bytes(&path) {
                Ok((bytes, name, ctype)) => Reply { status: 200, body: bytes, content_type: ctype, filename: name },
                Err(e) => Reply::json(e.status.unwrap_or(503), error(&e.message)),
            }
        }
        _ => match std::str::from_utf8(body) {
            Ok(text) => {
                let (s, b) = route(bridge, method, path, query, text, authed);
                Reply::json(s, b)
            }
            Err(_) => Reply::json(400, error("body is not UTF-8 JSON")),
        },
    }
}

fn has_key(req: &tiny_http::Request, key: &str) -> bool {
    req.headers().iter().any(|h| {
        let name = h.field.as_str().as_str().to_ascii_lowercase();
        let value = h.value.as_str();
        (name == "x-api-key" && value == key)
            || (name == "authorization" && value.strip_prefix("Bearer ") == Some(key))
    })
}

/// Routing is a pure function of the request and the bridge's state, so it can
/// be tested without binding a port.
pub fn route(bridge: &Bridge, method: &str, path: &str, query: &str, body: &str, authed: bool) -> (u16, String) {
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    match (method, parts.as_slice()) {
        // Health answers without a key, and reports the version — so a caller
        // told "that fix shipped" can check rather than conclude.
        ("GET", ["api", "health"]) => (200, health(bridge).to_string()),
        _ if !authed => (401, error("missing or invalid API key")),
        ("GET", ["api", "events"]) => {
            let after = param(query, "after").and_then(|v| v.parse().ok()).unwrap_or(0);
            let wait = param(query, "wait").and_then(|v| v.parse().ok()).unwrap_or(0u64).min(MAX_WAIT);
            let events = bridge.events(after, Duration::from_secs(wait));
            (200, json!({ "count": events.len(), "events": events }).to_string())
        }
        ("POST", ["api", "events", id, "ack"]) => {
            let Ok(id) = id.parse::<u64>() else { return (400, error("event id must be a number")) };
            match bridge.store.lock() {
                Ok(mut s) => match s.ack(id) {
                    Ok(was) => (200, json!({ "id": id, "acked": was }).to_string()),
                    Err(e) => (500, error(&e)),
                },
                Err(_) => (500, error("store lock poisoned")),
            }
        }
        ("POST", ["api", "say"]) => say(bridge, body),
        // The plugin says which version it is on connecting, for health.
        ("POST", ["api", "plugin"]) => {
            let v: Value = serde_json::from_str(body).unwrap_or(Value::Null);
            match v["version"].as_str() {
                Some(version) => {
                    bridge.set_plugin(version);
                    (200, json!({ "bridge": VERSION }).to_string())
                }
                None => (400, error("body {version}")),
            }
        }
        ("POST", ["api", "react"]) => react(bridge, body),
        // Any JSON route, with a body the plugin has put a file into
        // (`image_base64`, `data_base64`…). The model names the route; the
        // bytes never pass through it.
        ("POST", ["api", "trellis"]) => {
            let v: Value = match serde_json::from_str(body) {
                Ok(v) => v,
                Err(e) => return (400, error(&format!("body is not JSON: {e}"))),
            };
            let method = v["method"].as_str().unwrap_or("GET").to_ascii_uppercase();
            let Some(target) = v["path"].as_str() else { return (400, error("{method, path, body?} is required")) };
            if let Some(why) = crate::mcp::refuse(&method, target) {
                return (403, error(&why));
            }
            let inner = v.get("body").filter(|b| !b.is_null());
            let client = match bridge.client() {
                Ok(c) => c,
                Err(e) => return (503, error(&e.message)),
            };
            let target = match v["document"].as_str() {
                Some(d) => in_doc(target, d),
                None => target.to_string(),
            };
            match client.raw(&method, &target, inner) {
                Ok((status, out)) => (200, json!({ "status": status, "body": out }).to_string()),
                Err(e) => (503, error(&e.message)),
            }
        }
        ("GET", ["api", "files", card]) => {
            let Ok(card) = card.parse::<u64>() else { return (400, error("card must be a number")) };
            passthrough(bridge, "GET", &with_doc(&format!("/api/cards/{card}/attachments"), query), None)
        }
        ("POST", ["api", "files", card]) => {
            let Ok(card) = card.parse::<u64>() else { return (400, error("card must be a number")) };
            let v: Value = match serde_json::from_str(body) {
                Ok(v) => v,
                Err(e) => return (400, error(&format!("body is not JSON: {e}"))),
            };
            let (Some(name), Some(data)) = (v["name"].as_str(), v["data_base64"].as_str()) else {
                return (400, error("{name, data_base64} is required"));
            };
            passthrough(bridge, "POST", &with_doc(&format!("/api/cards/{card}/attachments"), query), Some(&json!({ "name": name, "data_base64": data })))
        }
        _ => (404, error("no such endpoint")),
    }
}

/// A Trellis call whose status and body go back as they came.
/// `path` in the document named by `?document=`, if the request named one.
fn with_doc(path: &str, query: &str) -> String {
    match param(query, "document") {
        Some(d) => in_doc(path, &unescape(d)),
        None => path.to_string(),
    }
}

fn passthrough(bridge: &Bridge, method: &str, path: &str, body: Option<&Value>) -> (u16, String) {
    let client = match bridge.client() {
        Ok(c) => c,
        Err(e) => return (503, error(&e.message)),
    };
    match client.raw(method, path, body) {
        Ok((status, v)) => (status, v.to_string()),
        Err(e) => (503, error(&e.message)),
    }
}

fn say(bridge: &Bridge, body: &str) -> (u16, String) {
    let v: Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return (400, error(&format!("body is not JSON: {e}"))),
    };
    // An unknown field is refused, not ignored: a typo answered 200 is a
    // message the agent thinks it sent.
    if let Some(obj) = v.as_object() {
        if let Some(k) = obj.keys().find(|k| !matches!(k.as_str(), "card" | "text" | "files" | "document" | "reply_to")) {
            return (400, error(&format!("unknown field `{k}` — say takes {{card, text, files?, document?, reply_to?}}")));
        }
    }
    let Some(card) = v["card"].as_u64() else { return (400, error("`card` (a number) is required")) };
    // The message this one answers (trellis-web 0.88.0, desktop 0.215.0): the
    // seq of a numbered message already in the channel.
    let reply_to = match &v["reply_to"] {
        Value::Null => None,
        r => match r.as_u64().filter(|s| *s > 0) {
            Some(s) => Some(s),
            None => return (400, error("`reply_to` is the seq of a numbered message (a number above 0)")),
        },
    };
    let text = v["text"].as_str().unwrap_or("");
    let mut files = Vec::new();
    for f in v["files"].as_array().map(Vec::as_slice).unwrap_or(&[]) {
        match (f["name"].as_str(), f["data_base64"].as_str()) {
            (Some(n), Some(d)) if !n.trim().is_empty() && !d.is_empty() => files.push((n.to_string(), d.to_string())),
            _ => return (400, error("each file is {name, data_base64}")),
        }
    }
    if text.trim().is_empty() && files.is_empty() {
        return (400, error("`text` or `files` is required"));
    }
    let chan = Chan::new(v["document"].as_str().unwrap_or(&bridge.document), card);
    if !bridge.owns(&chan) {
        return (403, error(&format!("#{card} is not a channel this agent answers in")));
    }
    match bridge.say(&chan, text, &files, reply_to) {
        Ok(r) => {
            let files = if r["files"].is_null() { json!([]) } else { r["files"].clone() };
            let reply_to = if r["reply_to"].is_null() { Value::Null } else { r["reply_to"].clone() };
            (200, json!({ "card": card, "seq": r["seq"], "files": files, "native": r["native"], "reply_to": reply_to }).to_string())
        }
        // No answer from Trellis: retryable, and nothing was written.
        Err(e) if e.status.is_none() => (503, error(&e.message)),
        // Trellis refused it (a `---` line, say): pass its reason through.
        Err(e) => (e.status.unwrap_or(502), error(&e.message)),
    }
}

/// React to a message in an owned channel (trellis-web 0.75.0):
/// `{card, seq, emoji, document?}` adds this agent's reaction; with
/// `remove: true` it takes this agent's `emoji` off, or all of its reactions on
/// that message when `emoji` is left out. A reaction is not a message: it wakes
/// nobody and moves no cursor, on the server or here.
fn react(bridge: &Bridge, body: &str) -> (u16, String) {
    let v: Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => return (400, error(&format!("body is not JSON: {e}"))),
    };
    if let Some(obj) = v.as_object() {
        if let Some(k) = obj.keys().find(|k| !matches!(k.as_str(), "card" | "seq" | "emoji" | "remove" | "document")) {
            return (400, error(&format!("unknown field `{k}` — react takes {{card, seq, emoji?, remove?, document?}}")));
        }
    }
    let Some(card) = v["card"].as_u64() else { return (400, error("`card` (a number) is required")) };
    let Some(seq) = v["seq"].as_u64().filter(|s| *s > 0) else {
        return (400, error("`seq` (a numbered message, not 0) is required"));
    };
    let remove = v["remove"].as_bool().unwrap_or(false);
    let emoji = v["emoji"].as_str().map(str::trim).filter(|e| !e.is_empty());
    if !remove && emoji.is_none() {
        return (400, error("`emoji` is required to add a reaction"));
    }
    let chan = Chan::new(v["document"].as_str().unwrap_or(&bridge.document), card);
    if !bridge.owns(&chan) {
        return (403, error(&format!("#{card} is not a channel this agent answers in")));
    }
    let client = match bridge.client() {
        Ok(c) => c,
        Err(e) => return (503, error(&e.message)),
    };
    let path = format!("/api/cards/{card}/channel/{seq}/react");
    let sent = if remove {
        let q = emoji.map(|e| format!("{path}?emoji={}", pct(e))).unwrap_or(path);
        client.call("DELETE", &in_doc(&q, &chan.doc), None)
    } else {
        client.post(&in_doc(&path, &chan.doc), &json!({ "emoji": emoji }))
    };
    match sent {
        Ok(r) => (200, json!({ "card": card, "seq": seq, "reaction_counts": r["reaction_counts"] }).to_string()),
        Err(e) if e.status.is_none() => (503, error(&e.message)),
        Err(e) => (e.status.unwrap_or(502), error(&e.message)),
    }
}

/// Percent-encode a query value (an emoji is several UTF-8 bytes).
fn pct(s: &str) -> String {
    s.bytes()
        .map(|b| if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) { (b as char).to_string() } else { format!("%{b:02X}") })
        .collect()
}

fn health(bridge: &Bridge) -> Value {
    let st = bridge.status();
    let (pending, cursors) = match bridge.store.lock() {
        Ok(s) => (
            s.pending(),
            bridge.owned().iter().map(|c| json!({ "document": c.doc, "card": c.card, "seq": s.cursor(&c.key()) })).collect(),
        ),
        Err(_) => (0, Vec::new()),
    };
    json!({
        "status": "ok",
        "version": VERSION,
        "agent": bridge.agent,
        "trellis": { "reachable": st.trellis_ok, "last_error": st.last_error },
        "key": st.key.as_ref().map(|k| {
            let mut k = k.clone();
            k["checked_s_ago"] = json!(st.key_read.map(|t| t.elapsed().as_secs()));
            k
        }),
        "plugin": st.plugin,
        "pending": pending,
        "channels": cursors,
        "document": bridge.document,
        "documents": bridge.docs(),
        "builtin_agents": bridge.builtins.lock().map(|b| b.values().cloned().collect::<Vec<_>>()).unwrap_or_default(),
        "claimed": bridge.claimed.lock().map(|c| c.iter().map(|c| json!({ "document": c.doc, "card": c.card })).collect::<Vec<_>>()).unwrap_or_default(),
        "home": bridge.home_for(&bridge.document).map(|c| json!({ "document": c.doc, "card": c.card })),
        "watch": st.watch,
        "stream": match bridge.mode() {
            crate::bridge::MODE_STREAM => "on",
            crate::bridge::MODE_POLL => "off: long-poll",
            _ => "connecting",
        },
        "refused": st.refused.iter().map(|(c, h)| json!({ "document": c.doc, "card": c.card, "claimed_by": h })).collect::<Vec<_>>(),
    })
}

/// `%2F` and friends in a query value; `+` is a space.
fn unescape(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                match u8::from_str_radix(std::str::from_utf8(&b[i + 1..i + 3]).unwrap_or("zz"), 16) {
                    Ok(v) => {
                        out.push(v);
                        i += 3;
                        continue;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            b'+' => out.push(b' '),
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn param<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query.split('&').find_map(|kv| kv.strip_prefix(name)?.strip_prefix('='))
}

fn error(msg: &str) -> String {
    json!({ "error": msg }).to_string()
}

fn json_header() -> tiny_http::Header {
    tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
        .expect("static header is valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::for_test;

    fn get(b: &Bridge, path: &str, query: &str, authed: bool) -> (u16, String) {
        route(b, "GET", path, query, "", authed)
    }

    #[test]
    fn health_needs_no_key_and_reports_a_version() {
        let (status, body) = get(&for_test(vec![]), "/api/health", "", false);
        assert_eq!(status, 200);
        assert!(body.contains("\"version\""));
    }

    #[test]
    fn health_shows_the_plugin_version_it_was_told() {
        let b = for_test(vec![]);
        assert_eq!(route(&b, "POST", "/api/plugin", "", r#"{"version":"0.4.14"}"#, false).0, 401);
        assert_eq!(route(&b, "POST", "/api/plugin", "", r#"{}"#, true).0, 400);
        assert_eq!(route(&b, "POST", "/api/plugin", "", r#"{"version":"0.4.14"}"#, true).0, 200);
        let (_, body) = get(&b, "/api/health", "", false);
        let v: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["plugin"], "0.4.14");
        assert!(v["key"].is_null(), "no Trellis client, no key read: {body}");
    }

    #[test]
    fn everything_else_needs_a_key() {
        let b = for_test(vec![]);
        assert_eq!(get(&b, "/api/events", "", false).0, 401);
        assert_eq!(get(&b, "/api/events", "", true).0, 200);
    }

    /// An unknown path must 404 for an authenticated caller, not 401 — telling
    /// a valid caller "bad key" sends them to re-check a key that was fine.
    #[test]
    fn unknown_path_is_404_when_authed() {
        assert_eq!(get(&for_test(vec![]), "/api/nope", "", true).0, 404);
    }

    #[test]
    fn say_refuses_a_channel_it_does_not_own_and_unknown_fields() {
        let b = for_test(vec![9]);
        let r = route(&b, "POST", "/api/say", "", r#"{"card":10,"text":"hi"}"#, true);
        assert_eq!(r.0, 403, "{}", r.1);
        let r = route(&b, "POST", "/api/say", "", r#"{"card":9,"text":"hi","reply":1}"#, true);
        assert_eq!(r.0, 400, "{}", r.1);
        for bad in [r#"0"#, r#""3""#, r#"-1"#] {
            let r = route(&b, "POST", "/api/say", "", &format!(r#"{{"card":9,"text":"hi","reply_to":{bad}}}"#), true);
            assert_eq!(r.0, 400, "reply_to {bad}: {}", r.1);
            assert!(r.1.contains("reply_to"), "{}", r.1);
        }
        let r = route(&b, "POST", "/api/say", "", r#"{"card":9,"text":"  "}"#, true);
        assert_eq!(r.0, 400, "{}", r.1);
    }

    #[test]
    fn ack_of_an_unknown_event_is_200_not_pending() {
        let r = route(&for_test(vec![]), "POST", "/api/events/42/ack", "", "", true);
        assert_eq!(r.0, 200);
        assert!(r.1.contains("\"acked\":false"));
    }

    #[test]
    fn mcp_and_files_need_the_key() {
        let b = for_test(vec![]);
        assert_eq!(route_bytes(&b, "POST", "/mcp", "", b"{}", false).status, 401);
        assert_eq!(route_bytes(&b, "GET", "/api/files/1/0", "", b"", false).status, 401);
        assert_eq!(route_bytes(&b, "GET", "/api/health", "", b"", false).status, 200);
        let r = route_bytes(&b, "POST", "/mcp", "", br#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#, true);
        assert_eq!(r.status, 200);
    }

    #[test]
    fn say_needs_text_or_files_and_well_formed_files() {
        let b = for_test(vec![9]);
        let r = route(&b, "POST", "/api/say", "", r#"{"card":9,"files":[{"name":"a"}]}"#, true);
        assert_eq!(r.0, 400, "{}", r.1);
        let r = route(&b, "POST", "/api/say", "", r#"{"card":9}"#, true);
        assert_eq!(r.0, 400, "{}", r.1);
    }

    #[test]
    fn query_values_are_unescaped() {
        assert_eq!(unescape("%2Fapi%2Fcards%2F7%2Fexport%3Fformat%3Dpng"), "/api/cards/7/export?format=png");
        assert_eq!(unescape("a+b%"), "a b%");
    }

    #[test]
    fn the_generic_routes_refuse_what_mcp_refuses() {
        let b = for_test(vec![]);
        let r = route(&b, "POST", "/api/trellis", "", r#"{"method":"POST","path":"/api/keys"}"#, true);
        assert_eq!(r.0, 403, "{}", r.1);
        let r = route_bytes(&b, "GET", "/api/trellis-bytes", "path=%2Fapi%2Fauth%2Fcallback", b"", true);
        assert_eq!(r.status, 403);
    }

    #[test]
    fn say_is_scoped_to_the_named_document() {
        let b = for_test(vec![9]);
        let r = route(&b, "POST", "/api/say", "", r#"{"card":9,"document":"E","text":"hi"}"#, true);
        assert_eq!(r.0, 403, "card 9 in another document is another channel: {}", r.1);
    }

    #[test]
    fn react_is_checked_before_trellis_is_asked() {
        let b = crate::bridge::for_test(vec![9]);
        let r = route(&b, "POST", "/api/react", "", r#"{"card":10,"seq":3,"emoji":"👀"}"#, true);
        assert_eq!(r.0, 403, "not our channel: {}", r.1);
        let r = route(&b, "POST", "/api/react", "", r#"{"card":9,"seq":0,"emoji":"👀"}"#, true);
        assert_eq!(r.0, 400, "loose text has no number: {}", r.1);
        let r = route(&b, "POST", "/api/react", "", r#"{"card":9,"seq":3}"#, true);
        assert_eq!(r.0, 400, "adding needs an emoji: {}", r.1);
        let r = route(&b, "POST", "/api/react", "", r#"{"card":9,"seq":3,"emoji":"👀","why":1}"#, true);
        assert_eq!(r.0, 400, "unknown field: {}", r.1);
        assert_eq!(pct("👀"), "%F0%9F%91%80");
    }

    #[test]
    fn query_params_are_read_by_exact_name() {
        assert_eq!(param("after=3&wait=5", "wait"), Some("5"));
        assert_eq!(param("waiting=1", "wait"), None);
    }
}
