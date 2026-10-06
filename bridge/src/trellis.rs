//! The Trellis client. Only the documented agent API, with the
//! document-scoped key (DESIGN D1). Every call carries `X-Agent`, and
//! `?document=` once the document is known.
//!
//! Every call returns the HTTP status with the body, and a non-2xx is an error
//! carrying the server's own message — never a 200 assumed from a transport
//! that did not fail.

use crate::secret::Secret;
use serde_json::Value;

/// A failed call. `status` is `None` when no HTTP answer came back at all —
/// Trellis unreachable — which is the one case worth retrying blind.
#[derive(Debug)]
pub struct Failed {
    pub status: Option<u16>,
    pub message: String,
}

impl From<Failed> for String {
    fn from(f: Failed) -> String {
        f.message
    }
}

/// Trellis refuses a file over 40 MB; the bridge refuses it first.
pub const MAX_FILE: u64 = 40 * 1024 * 1024;

/// An error body in one line: the server's `error`, an HTML page's `<title>`
/// (a proxy's 502 page is ~100 lines), or the first line, at most 300 chars.
fn brief(text: &str) -> String {
    if let Some(e) = serde_json::from_str::<Value>(text).ok().and_then(|v| v["error"].as_str().map(str::to_string)) {
        return e;
    }
    let lower = text.to_ascii_lowercase();
    let line = match (lower.find("<title>"), lower.find("</title>")) {
        (Some(a), Some(b)) if b > a => &text[a + 7..b],
        _ => text.trim().lines().next().unwrap_or(""),
    };
    let line = line.trim();
    match line.char_indices().nth(300) {
        Some((i, _)) => format!("{}…", &line[..i]),
        None => line.to_string(),
    }
}

/// `attachment; filename="x.png"` → `x.png`.
fn filename(cd: &str) -> Option<String> {
    let rest = cd.split("filename=").nth(1)?;
    let name = rest.trim().trim_start_matches('"');
    let name = name.split('"').next()?.split(';').next()?.trim();
    (!name.is_empty()).then(|| name.to_string())
}

/// `path` with `document=<doc>` added, which the client then leaves alone —
/// how a call names a document other than the default.
pub fn in_doc(path: &str, doc: &str) -> String {
    if path.contains("document=") {
        return path.to_string();
    }
    format!("{path}{}document={doc}", if path.contains('?') { '&' } else { '?' })
}

pub struct Client {
    base: String,
    key: Secret,
    agent: String,
    document: Option<String>,
    /// Desktop Trellis: one document per port, and it refuses `?document=`,
    /// so none is ever sent — not the default, not one a caller named.
    desktop: bool,
}

/// The server's answer to a query field a route does not take, when that
/// field is `document`.
fn refuses_document(code: u16, text: &str) -> bool {
    code == 400 && text.contains("unknown field `document`")
}

/// `path` without any `document=` parameter.
fn without_document(path: &str) -> String {
    let Some((route, query)) = path.split_once('?') else { return path.to_string() };
    let rest: Vec<&str> = query.split('&').filter(|p| !p.starts_with("document=")).collect();
    if rest.is_empty() {
        route.to_string()
    } else {
        format!("{route}?{}", rest.join("&"))
    }
}

impl Client {
    pub fn new(base: &str, key: Secret, agent: &str, document: Option<String>) -> Client {
        Client {
            base: base.trim_end_matches('/').to_string(),
            key,
            agent: agent.to_string(),
            document,
            desktop: false,
        }
    }

    /// Ask the server which app it is. `GET /api/health` answers
    /// `app: "trellis"` on the desktop and `"trellis-server"` on trellis-web,
    /// with no key needed. A desktop gets the no-document mode.
    pub fn detect(&mut self) -> Result<(), Failed> {
        let health = self.get("/api/health")?;
        self.desktop = health["app"].as_str() == Some("trellis");
        Ok(())
    }

    /// Fill in the name once it is known: an empty config name is the one the
    /// key is bound to, read with this same client.
    pub fn set_agent(&mut self, agent: &str) {
        self.agent = agent.to_string();
    }

    pub fn is_desktop(&self) -> bool {
        self.desktop
    }

    pub fn get(&self, path: &str) -> Result<Value, Failed> {
        self.call("GET", path, None)
    }

    pub fn post(&self, path: &str, body: &Value) -> Result<Value, Failed> {
        self.call("POST", path, Some(body))
    }

    pub fn call(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value, Failed> {
        self.send(method, path, body, true)
    }

    /// The same call with no `X-Agent`, so the server attributes it to the
    /// account holder. Only `trellisbridge call --as-operator` uses it, to
    /// play the operator in an end-to-end test.
    pub fn call_as_operator(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value, Failed> {
        self.send(method, path, body, false)
    }

    /// One call that reports the status instead of failing on it: for the
    /// MCP `trellis_api` tool, which hands Trellis's own answer — refusals
    /// included — back to the agent. `Failed` only when no answer came at all.
    pub fn raw(&self, method: &str, path: &str, body: Option<&Value>) -> Result<(u16, Value), Failed> {
        let (code, text) = self
            .exchange_for(method, path, body, true, true)
            .map_err(|e| Failed { status: None, message: format!("{method} {path}: {e}") })?;
        let v = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) if code >= 400 => Value::String(brief(&text)),
            Err(_) => Value::String(text),
        };
        Ok((code, v))
    }

    /// The bytes of a file route (`GET /api/cards/{cid}/attachments/{n}`), with
    /// the name from `Content-Disposition` and the type Trellis reports.
    pub fn bytes(&self, path: &str) -> Result<(Vec<u8>, Option<String>, String), Failed> {
        let url = self.url(path);
        let fail = |status: Option<u16>, msg: String| Failed {
            status,
            message: format!("GET {path}{}: {msg}", status.map(|c| format!(" → {c}")).unwrap_or_default()),
        };
        let r = ureq::get(&url)
            .timeout(std::time::Duration::from_secs(120))
            .set("Authorization", &format!("Bearer {}", self.key.expose()))
            .set("X-Agent", &self.agent)
            .call();
        let r = match r {
            Ok(r) => r,
            Err(ureq::Error::Status(code, r)) => return Err(fail(Some(code), brief(&r.into_string().unwrap_or_default()))),
            Err(e) => return Err(fail(None, e.to_string())),
        };
        let ctype = r.content_type().to_string();
        let name = r.header("Content-Disposition").and_then(filename);
        let mut buf = Vec::new();
        use std::io::Read;
        r.into_reader()
            .take(MAX_FILE + 1)
            .read_to_end(&mut buf)
            .map_err(|e| fail(None, e.to_string()))?;
        if buf.len() as u64 > MAX_FILE {
            return Err(fail(None, "file is over 40 MB".into()));
        }
        Ok((buf, name, ctype))
    }

    fn send(&self, method: &str, path: &str, body: Option<&Value>, as_agent: bool) -> Result<Value, Failed> {
        let fail = |status: Option<u16>, msg: String| Failed {
            status,
            message: format!("{method} {path}{}: {msg}", status.map(|c| format!(" → {c}")).unwrap_or_default()),
        };
        let (code, text) = self.exchange_for(method, path, body, as_agent, false).map_err(|e| fail(None, e))?;
        if code >= 400 {
            return Err(fail(Some(code), brief(&text)));
        }
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text).map_err(|e| fail(Some(code), format!("not JSON: {e}")))
    }

    /// One request: its status and body text, or why no answer came.
    fn exchange(&self, method: &str, url: &str, body: Option<&Value>, as_agent: bool, json: bool) -> Result<(u16, String), String> {
        // The wait route holds a request ~25s by design; the timeout sits
        // well past that so a normal "nothing changed" is never an error.
        let mut req = ureq::request(method, url)
            .timeout(std::time::Duration::from_secs(60))
            .set("Authorization", &format!("Bearer {}", self.key.expose()));
        if as_agent {
            req = req.set("X-Agent", &self.agent);
        }
        if json {
            req = req.set("Accept", "application/json");
        }
        let sent = match body {
            Some(b) => req.send_json(b),
            None => req.call(),
        };
        match sent {
            Ok(r) => {
                let code = r.status();
                r.into_string().map(|t| (code, t)).map_err(|e| e.to_string())
            }
            Err(ureq::Error::Status(code, r)) => Ok((code, r.into_string().unwrap_or_default())),
            Err(e) => Err(e.to_string()),
        }
    }

    /// `exchange` for `path`, sent again without the default document when the
    /// route refuses one: a route across the whole account (web 0.106.0
    /// `GET /api/agents/{id}/hops`) rejects any query field it does not know.
    /// A 400 is a request the server did not act on, so sending it again is safe.
    fn exchange_for(&self, method: &str, path: &str, body: Option<&Value>, as_agent: bool, json: bool) -> Result<(u16, String), String> {
        let (code, text) = self.exchange(method, &self.url(path), body, as_agent, json)?;
        if self.adds_document(path) && refuses_document(code, &text) {
            return self.exchange(method, &format!("{}{path}", self.base), body, as_agent, json);
        }
        Ok((code, text))
    }

    /// Open the agent event stream (DESIGN D14): `text/event-stream`, one per
    /// key across every document, so no `?document=` is added. Reads time out
    /// after 45 s, three keep-alives (15 s each) missed.
    pub fn open_stream(&self, path: &str, last_id: Option<&str>) -> Result<Box<dyn std::io::BufRead + Send>, Failed> {
        let fail = |status: Option<u16>, msg: String| Failed {
            status,
            message: format!("GET {path}{}: {msg}", status.map(|c| format!(" → {c}")).unwrap_or_default()),
        };
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(std::time::Duration::from_secs(15))
            .timeout_read(std::time::Duration::from_secs(45))
            .build();
        let mut req = agent
            .get(&format!("{}{path}", self.base))
            .set("Authorization", &format!("Bearer {}", self.key.expose()))
            .set("X-Agent", &self.agent)
            .set("Accept", "text/event-stream")
            // Uncompressed: Cloudflare compresses (zstd/gzip) whenever the client
            // accepts it, `no-transform` or not, and a compressed stream arrives
            // in blocks, not events (found live on web 0.84.0).
            .set("Accept-Encoding", "identity");
        if let Some(id) = last_id {
            req = req.set("Last-Event-ID", id);
        }
        match req.call() {
            Ok(r) => Ok(Box::new(std::io::BufReader::new(r.into_reader()))),
            Err(ureq::Error::Status(code, r)) => Err(fail(Some(code), brief(&r.into_string().unwrap_or_default()))),
            Err(e) => Err(fail(None, e.to_string())),
        }
    }

    /// A POST that names no document: for routes across the whole key, such
    /// as the stream's ack.
    pub fn post_root(&self, path: &str, body: &Value) -> Result<(), Failed> {
        let r = ureq::post(&format!("{}{path}", self.base))
            .timeout(std::time::Duration::from_secs(30))
            .set("Authorization", &format!("Bearer {}", self.key.expose()))
            .set("X-Agent", &self.agent)
            .send_json(body);
        match r {
            Ok(_) => Ok(()),
            Err(ureq::Error::Status(code, r)) => Err(Failed { status: Some(code), message: format!("POST {path} → {code}: {}", brief(&r.into_string().unwrap_or_default())) }),
            Err(e) => Err(Failed { status: None, message: format!("POST {path}: {e}") }),
        }
    }

    /// Whether `url` adds the default document to `path`.
    fn adds_document(&self, path: &str) -> bool {
        !self.desktop && self.document.is_some() && !path.contains("document=")
    }

    fn url(&self, path: &str) -> String {
        if self.desktop {
            return format!("{}{}", self.base, without_document(path));
        }
        let mut url = format!("{}{path}", self.base);
        if let Some(doc) = &self.document {
            if !path.contains("document=") {
                url.push(if path.contains('?') { '&' } else { '?' });
                url.push_str("document=");
                url.push_str(doc);
            }
        }
        url
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(doc: Option<&str>) -> Client {
        let key = Secret::for_test("k");
        Client::new("https://t.example/", key, "a", doc.map(str::to_string))
    }

    #[test]
    fn in_doc_names_the_document_once() {
        assert_eq!(in_doc("/api/x", "E"), "/api/x?document=E");
        assert_eq!(in_doc("/api/x?a=1", "E"), "/api/x?a=1&document=E");
        assert_eq!(in_doc("/api/x?document=F", "E"), "/api/x?document=F");
        let c = client(Some("D"));
        assert_eq!(c.url(&in_doc("/api/x", "E")), "https://t.example/api/x?document=E", "the default is not added twice");
    }

    #[test]
    fn a_route_that_refuses_the_default_document_is_asked_again_without_it() {
        // web 0.106.0, GET /api/agents/{id}/hops with the default appended:
        let body = r#"{"error":"Failed to deserialize query string: unknown field `document`, expected `since` or `limit`."}"#;
        assert!(refuses_document(400, body));
        assert!(!refuses_document(404, body));
        assert!(!refuses_document(400, r#"{"error":"unknown field `since`"}"#));
        // Only the document the client added itself is dropped, never one the caller named.
        let c = client(Some("D"));
        assert!(c.adds_document("/api/agents/x/hops"));
        assert!(!c.adds_document("/api/agents/x/hops?document=E"));
        assert!(!client(None).adds_document("/api/agents/x/hops"));
    }

    #[test]
    fn error_bodies_are_one_line() {
        assert_eq!(brief(r#"{"error":"no such card"}"#), "no such card");
        let page = "<!DOCTYPE html>\n<html><head>\n<title>trellis-cards.com | 502: Bad gateway</title>\n</head><body>…</body></html>";
        assert_eq!(brief(page), "trellis-cards.com | 502: Bad gateway");
        assert_eq!(brief("  plain text\nsecond line"), "plain text");
        assert_eq!(brief(&"é".repeat(400)).chars().count(), 301);
    }

    #[test]
    fn filename_is_read_from_content_disposition() {
        assert_eq!(filename(r#"attachment; filename="a b.png""#).as_deref(), Some("a b.png"));
        assert_eq!(filename("attachment; filename=x.pdf").as_deref(), Some("x.pdf"));
        assert_eq!(filename("attachment"), None);
    }

    #[test]
    fn document_is_added_once_with_the_right_separator() {
        let c = client(Some("D"));
        assert_eq!(c.url("/api/x"), "https://t.example/api/x?document=D");
        assert_eq!(c.url("/api/x?a=1"), "https://t.example/api/x?a=1&document=D");
        assert_eq!(c.url("/api/x?document=E"), "https://t.example/api/x?document=E");
        assert_eq!(client(None).url("/api/x"), "https://t.example/api/x");
    }

    #[test]
    fn a_desktop_is_never_sent_a_document() {
        let mut c = client(Some("D"));
        c.desktop = true;
        assert_eq!(c.url("/api/x"), "https://t.example/api/x");
        assert_eq!(c.url(&in_doc("/api/x", "E")), "https://t.example/api/x");
        assert_eq!(c.url("/api/x?a=1&document=E&b=2"), "https://t.example/api/x?a=1&b=2");
        assert_eq!(c.url("/api/x?document=E&since=3"), "https://t.example/api/x?since=3");
    }
}
