//! The agent event stream (DESIGN D14): one `GET /api/agent/stream` per key,
//! in place of a long-poll per document and the timers beside it.
//!
//! The stream is a wake source over the paths the bridge already trusts. A
//! `message` sweeps its channel through the channel cursor (or, in someone
//! else's group channel, goes to `hear`). A `mention`, `property` or
//! `signoff_requested` is raised on the home channel with the writer's
//! attestation, run through the same `speaker` rule as the change log.
//! An `edited` (desktop 0.219.0) of a message the agent has already been
//! given, by the operator, is raised in that channel: the order changed or
//! was withdrawn after the agent acted on it.
//! `claim`, `participants`, `agents` and `access` trigger an immediate
//! re-read, and `reset` re-sweeps the documents it names. Unknown types are
//! ignored, and so are unknown fields.
//!
//! Until a frame has arrived the stream is unproven and the change-log
//! watchers and channel pollers (`MODE_POLL`) carry everything. When the
//! server has no stream (404) it is tried again every 30 minutes; a proven
//! stream that drops gets 5 minutes to come back before the long-poll resumes.

use crate::bridge::{Bridge, Chan, MODE_POLL, MODE_STREAM};
use crate::trellis::{in_doc, Client};
use crate::watch::{is_channel, speaker, ASSIGN_KEYS};
use serde_json::{json, Value};
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PATH: &str = "/api/agent/stream?types=hello,message,mention,property,signoff_requested,edited,claim,participants,agents,access,reset,replaced,auth";
const ACK_EVERY: Duration = Duration::from_secs(30);
/// How long the stream may keep failing before the long-poll takes over.
const GIVE_UP_AFTER: Duration = Duration::from_secs(300);
const RETRY_UNSUPPORTED: Duration = Duration::from_secs(1800);
const RETRY_REPLACED: Duration = Duration::from_secs(600);
const SEEN: usize = 512;

/// One Server-Sent Events frame.
#[derive(Debug, Default, PartialEq)]
pub struct Frame {
    pub id: Option<String>,
    pub event: Option<String>,
    pub data: String,
}

/// Lines in, frames out (the SSE rules: `field: value`, one leading space
/// dropped, `data` lines joined by `\n`, a blank line dispatches, `:` is a
/// comment).
#[derive(Default)]
pub struct Parser {
    id: Option<String>,
    event: Option<String>,
    data: Vec<String>,
    pub retry: Option<u64>,
}

impl Parser {
    pub fn line(&mut self, raw: &str) -> Option<Frame> {
        let line = raw.trim_end_matches(['\n', '\r']);
        if line.is_empty() {
            if self.data.is_empty() && self.event.is_none() {
                return None;
            }
            return Some(Frame { id: self.id.take(), event: self.event.take(), data: std::mem::take(&mut self.data).join("\n") });
        }
        if line.starts_with(':') {
            return None;
        }
        let (field, value) = match line.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        match field {
            "id" => self.id = Some(value.to_string()),
            "event" => self.event = Some(value.to_string()),
            "data" => self.data.push(value.to_string()),
            "retry" => self.retry = value.parse().ok(),
            _ => {}
        }
        None
    }
}

/// Why one connection ended.
#[derive(Debug, PartialEq)]
pub enum Ended {
    /// No stream on this server (404/405/501): use the long-poll.
    Unsupported(String),
    /// A newer stream for the same bound name took over: another bridge runs.
    Replaced,
    /// The key was revoked or refused.
    Auth(String),
    /// The connection dropped; resume from the last id.
    Dropped(String),
}

/// What one event asks of the bridge, besides what it raised itself.
#[derive(Debug, PartialEq)]
pub enum After {
    Continue,
    End(Ended),
}

impl Bridge {
    /// Keep a stream open for as long as the bridge runs, falling back to the
    /// long-poll when there is none.
    pub fn stream_supervisor(self: &Arc<Self>) {
        let Some(client) = self.client.as_ref() else {
            self.set_mode(MODE_POLL);
            return;
        };
        let mut backoff = Duration::from_secs(1);
        let mut failing_since: Option<Instant> = None;
        loop {
            let (ended, delivered) = self.stream_once(client);
            if delivered {
                // It worked for a while: a drop now starts a fresh clock.
                failing_since = None;
                backoff = Duration::from_secs(1);
            }
            match ended {
                Ended::Unsupported(why) => {
                    if self.mode() != MODE_POLL {
                        println!("stream    not offered ({why}) — long-polling; trying again in 30 min");
                    }
                    self.fall_back();
                    std::thread::sleep(RETRY_UNSUPPORTED);
                }
                Ended::Replaced => {
                    self.set_err(&format!("stream replaced: another bridge is running as {} — two would answer twice", self.agent));
                    self.fall_back();
                    std::thread::sleep(RETRY_REPLACED);
                }
                Ended::Auth(why) => {
                    self.set_err(&format!("stream refused the key: {why}"));
                    self.fall_back();
                    std::thread::sleep(RETRY_UNSUPPORTED);
                }
                Ended::Dropped(why) => {
                    if failing_since.is_none() {
                        eprintln!("trellisbridge: stream dropped ({why}) — reconnecting");
                    }
                    let since = *failing_since.get_or_insert_with(Instant::now);
                    // A stream that never delivered a frame has proven nothing:
                    // the long-poll runs at once. One that has gets 5 minutes
                    // to come back before the long-poll takes over.
                    let unproven = self.mode() != MODE_STREAM;
                    if (unproven || since.elapsed() > GIVE_UP_AFTER) && self.mode() != MODE_POLL {
                        eprintln!("trellisbridge: stream down for {}s ({why}) — long-polling meanwhile", since.elapsed().as_secs());
                        self.fall_back();
                    }
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                    continue;
                }
            }
            failing_since = None;
            backoff = Duration::from_secs(1);
        }
    }

    /// Mark the stream healthy: called once a frame has actually arrived, so
    /// a connection that opens and delivers nothing never passes for one.
    fn stream_ok(&self) {
        if self.mode() != MODE_STREAM {
            println!("stream    open — events arrive as they happen; the long-poll rests");
        }
        self.set_mode(MODE_STREAM);
    }

    fn fall_back(&self) {
        if self.mode() == MODE_STREAM {
            // Read every owned channel again under the pollers' rules.
            if let Ok(mut s) = self.swept.lock() {
                s.clear();
            }
        }
        self.set_mode(MODE_POLL);
        self.refresh.store(true, Ordering::SeqCst);
    }

    /// One connection, from the last id this bridge stored.
    /// Also says whether any frame arrived on it.
    fn stream_once(&self, client: &Client) -> (Ended, bool) {
        let mut delivered = false;
        let ended = self.stream_read(client, &mut delivered);
        (ended, delivered)
    }

    fn stream_read(&self, client: &Client, delivered: &mut bool) -> Ended {
        let last = self.store.lock().ok().and_then(|s| s.meta("stream:last").and_then(Value::as_str).map(str::to_string));
        let mut reader = match client.open_stream(PATH, last.as_deref()) {
            Ok(r) => r,
            Err(e) => {
                return match e.status {
                    Some(404 | 405 | 501) => Ended::Unsupported(e.message),
                    Some(401) => Ended::Auth(e.message),
                    _ => Ended::Dropped(e.message),
                }
            }
        };
        self.refresh.store(true, Ordering::SeqCst);
        let mut parser = Parser::default();
        let mut seen: VecDeque<String> = VecDeque::new();
        let mut seen_set: HashSet<String> = HashSet::new();
        let mut acked = last.clone();
        let mut newest = last;
        let mut ack_at = Instant::now();
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => return Ended::Dropped("closed by the server".into()),
                Ok(_) => {}
                Err(e) => return Ended::Dropped(e.to_string()),
            }
            if let Some(f) = parser.line(&line) {
                *delivered = true;
                self.stream_ok();
                if let Some(id) = &f.id {
                    if !seen_set.insert(id.clone()) {
                        continue; // at least once: a replayed id is dropped
                    }
                    seen.push_back(id.clone());
                    if seen.len() > SEEN {
                        if let Some(old) = seen.pop_front() {
                            seen_set.remove(&old);
                        }
                    }
                }
                let after = match serde_json::from_str::<Value>(&f.data) {
                    Ok(env) => self.on_event(Some(client), &env),
                    Err(_) => After::Continue, // not ours to read: ignored
                };
                if let Some(id) = f.id {
                    if let Ok(mut s) = self.store.lock() {
                        let _ = s.set_meta("stream:last", json!(id));
                    }
                    newest = Some(id);
                }
                self.set_ok();
                if let After::End(why) = after {
                    return why;
                }
            }
            if ack_at.elapsed() >= ACK_EVERY && newest != acked {
                if let Some(id) = &newest {
                    if client.post_root("/api/agent/stream/ack", &json!({ "cursor": id })).is_ok() {
                        acked = newest.clone();
                    }
                }
                ack_at = Instant::now();
            }
        }
    }

    /// One event's envelope `{v, type, id, at, document?, card?, data}`.
    pub fn on_event(&self, client: Option<&Client>, env: &Value) -> After {
        let data = &env["data"];
        // A desktop serves one document; its stream names it with the run's
        // epoch, which the bridge does not use.
        let doc = match (client.map(Client::is_desktop), env["document"].as_str()) {
            (Some(false), Some(d)) => d.to_string(),
            _ => self.document.clone(),
        };
        let card = env["card"].as_u64().or_else(|| data["card"].as_u64());
        let me = |n: &Value| n.as_str().is_some_and(|n| n.eq_ignore_ascii_case(&self.agent));
        match env["type"].as_str().unwrap_or("") {
            "hello" => {
                println!(
                    "stream    {} {} — v{}, {} event types",
                    data["server"].as_str().unwrap_or("?"),
                    data["version"].as_str().unwrap_or("?"),
                    data["v"],
                    data["types"].as_array().map(Vec::len).unwrap_or(0)
                );
            }
            "message" => {
                let Some(card) = card else { return After::Continue };
                if me(&data["from"]) {
                    return After::Continue; // our own word
                }
                let ch = Chan::new(&doc, card);
                if self.owns(&ch) {
                    self.sweep(&ch);
                } else if data["to"].as_array().is_some_and(|t| t.iter().any(me)) {
                    if let Some(c) = client {
                        self.hear(c, &doc, card);
                    }
                }
            }
            "mention" => {
                let Some(card) = card else { return After::Continue };
                if !data["names"].as_array().is_some_and(|n| n.iter().any(me)) || self.owns(&Chan::new(&doc, card)) {
                    return After::Continue;
                }
                // A channel that is not ours is someone else's conversation:
                // its messages arrive as `message`, with `to` (D14, #306).
                if let Some(c) = client {
                    if let Ok(v) = c.get(&in_doc(&format!("/api/cards/{card}"), &doc)) {
                        let cardv = if v.get("card").is_some() { &v["card"] } else { &v };
                        if is_channel(cardv) {
                            return After::Continue;
                        }
                    }
                }
                let raw: Vec<String> = data["lines"].as_array().into_iter().flatten().filter_map(|l| l.as_str()).map(|l| l.trim().to_string()).collect();
                let fresh = self.unseen_mentions(&doc, card, &raw);
                if fresh.is_empty() {
                    return After::Continue; // the change-log watcher raised it first
                }
                let lines: Vec<String> = fresh.iter().map(|l| format!("> {l}")).collect();
                self.raise_streamed(&doc, card, env, "mention", |from| {
                    format!(
                        "[@mention on card #{card}, by {from}]\n{}\n\nRead the card with GET /api/cards/{card}; answer here, or on the card if that is what was asked.",
                        lines.join("\n")
                    )
                });
            }
            "property" => {
                let Some(card) = card else { return After::Continue };
                let key = data["key"].as_str().unwrap_or("").to_ascii_lowercase();
                let val = data["value"].as_str().unwrap_or("");
                if !ASSIGN_KEYS.contains(&key.as_str())
                    || !val.to_ascii_lowercase().contains(&self.agent.to_ascii_lowercase())
                    || !self.first_time(&format!("assigned:{doc}:{card}:{key}"), val)
                {
                    return After::Continue;
                }
                if self.assigned_recently(&doc, card, "property") {
                    return After::Continue; // the same write's `task` event raised it
                }
                self.raise_streamed(&doc, card, env, "assigned", |from| {
                    format!("[assigned to you: card #{card} — {key}:: {val}, set by {from}]\n\nRead it with GET /api/cards/{card}.")
                });
            }
            // One checklist line's task field (trellis-web 0.104.5, 2754 #437).
            // The card's `property` event carries only the first line's value
            // per key, so a later line assigned to this agent arrives only here.
            "task" => {
                let Some(card) = card else { return After::Continue };
                let key = data["key"].as_str().unwrap_or("").to_ascii_lowercase();
                let val = data["value"].as_str().unwrap_or("");
                let item = data["item"].as_u64().unwrap_or(0);
                if !ASSIGN_KEYS.contains(&key.as_str())
                    || !val.to_ascii_lowercase().contains(&self.agent.to_ascii_lowercase())
                    || !self.first_time(&format!("assigned:{doc}:{card}:{item}:{key}"), val)
                    || self.assigned_recently(&doc, card, "task")
                {
                    return After::Continue;
                }
                // The card's own key too, so the change-log watcher catching up
                // after a stream drop does not raise the card again.
                self.first_time(&format!("assigned:{doc}:{card}:{key}"), val);
                let task = data["text"].as_str().unwrap_or("").to_string();
                self.raise_streamed(&doc, card, env, "assigned", |from| {
                    format!(
                        "[assigned to you: line {item} of checklist card #{card}, \"{task}\" — {key}:: {val}, set by {from}]\n\nRead it with GET /api/cards/{card}; POST /api/cards/{card}/complete {{\"item\": {item}}} marks it done."
                    )
                });
            }
            "signoff_requested" => {
                let Some(card) = card else { return After::Continue };
                let title = data["title"].as_str().unwrap_or("").to_string();
                let digest = data["digest"].as_str().unwrap_or("").to_string();
                self.raise_streamed(&doc, card, env, "signoff", |from| {
                    format!(
                        "[sign-off asked of you: card #{card} \"{title}\", by {from}]\n\nRead it with GET /api/cards/{card}, then POST /api/cards/{card}/signoff {{\"verdict\": \"approved\" | \"changes-requested\" | \"rejected\", \"note\"?, \"digest\": \"{digest}\"}}. A 409 means it changed since: read it again and decide on what it says now."
                    )
                });
            }
            "edited" => {
                let Some(card) = card else { return After::Continue };
                let ch = Chan::new(&doc, card);
                let seq = data["seq"].as_u64().unwrap_or(0);
                if me(&data["from"]) || !self.owns(&ch) || seq == 0 {
                    return After::Continue;
                }
                let cursor = self.store.lock().map(|s| s.cursor(&ch.key())).unwrap_or(0);
                if seq > cursor {
                    // Not read yet: the sweep reads it as it now stands.
                    self.sweep(&ch);
                    return After::Continue;
                }
                // Only the operator's own change matters: a peer's edit is
                // conversation, and a peer's words were never orders (D12).
                let owned = self.owned_docs.lock().map(|o| o.contains(&doc)).unwrap_or(false);
                if crate::bridge::provenance(data, &self.operators, self.e2e_operator.as_deref(), owned) != "operator" {
                    return After::Continue;
                }
                let deleted = data["deleted"].as_bool() == Some(true);
                let stamp = data["edited_at"].as_str().unwrap_or(if deleted { "deleted" } else { "edited" });
                if !self.first_time(&format!("edited:{doc}:{card}:{seq}"), stamp) {
                    return After::Continue;
                }
                let from = data["from"].as_str().unwrap_or("operator").to_string();
                let text = if deleted {
                    format!("[#{seq} was deleted by {from} after you received it.] If you are still working on what it asked, stop, and say what you had already done.")
                } else {
                    let quoted: Vec<String> = data["text"].as_str().unwrap_or("").lines().map(|l| format!("> {l}")).collect();
                    format!("[#{seq} was edited by {from} after you received it. It now reads:]\n{}\n\nIf this changes what you did or are doing, adjust and say so; if not, reply NO_REPLY.", quoted.join("\n"))
                };
                self.raise(&ch, &doc, card, data, "edited", &from, "operator", text);
            }
            "claim" | "participants" | "agents" | "access" => self.refresh.store(true, Ordering::SeqCst),
            "reset" => {
                let named: Vec<String> = data["documents"].as_array().into_iter().flatten().filter_map(|d| d.as_str()).map(str::to_string).collect();
                let desktop = client.is_some_and(Client::is_desktop);
                for c in self.owned() {
                    if desktop || named.is_empty() || named.contains(&c.doc) {
                        self.sweep(&c);
                    }
                }
                self.refresh.store(true, Ordering::SeqCst);
            }
            "replaced" => return After::End(Ended::Replaced),
            "auth" => return After::End(Ended::Auth(data["reason"].as_str().unwrap_or("revoked").to_string())),
            _ => {} // a type this bridge does not know: ignored (D14)
        }
        After::Continue
    }

    /// Raise a streamed request on the home channel. Who wrote it is judged by
    /// the same `speaker` rule as a change-log entry, from the attestation the
    /// event carries; without one it is unverified, never trusted.
    /// True when the other event kind raised an assignment on this card in
    /// the last minute; otherwise records this one. One write can raise both
    /// `property` (the card's first value) and `task` (the line): the agent
    /// hears it once. Two `task` events (two lines) are both raised.
    fn assigned_recently(&self, doc: &str, card: u64, by: &str) -> bool {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        let key = format!("assigned_at:{doc}:{card}");
        let Ok(mut s) = self.store.lock() else { return false };
        let last = s.meta(&key).cloned().unwrap_or(Value::Null);
        if last["by"].as_str().is_some_and(|b| b != by) && last["at"].as_u64().is_some_and(|t| now.saturating_sub(t) < 60) {
            return true;
        }
        let _ = s.set_meta(&key, json!({ "by": by, "at": now }));
        false
    }

    fn raise_streamed(&self, doc: &str, card: u64, env: &Value, kind: &str, text: impl FnOnce(&str) -> String) {
        let Some(home) = self.home_for(doc) else { return };
        // Catch the home channel up first. On the stream a message addressed to
        // someone else never arrives, so an operator's word there ("@trellis
        // make a card for @Orbit") would not reset the loop guard, and the
        // request it leads to would be held as a fifth agent message in a row.
        self.sweep(&home);
        let d = &env["data"];
        let entry = json!({
            "entity": "card", "id": card, "node": d["node"], "title": d["title"],
            "agent": d["agent"].as_str().or(d["by"].as_str()).or(d["from"].as_str()),
            "kind": d["kind"], "via": d["via"], "actor": d["actor"],
            "agent_verified": d["agent_verified"], "from_key_owner": d["from_key_owner"],
        });
        if entry["agent"].as_str().is_some_and(|a| a.eq_ignore_ascii_case(&self.agent)) {
            return; // our own write
        }
        let owned = self.owned_docs.lock().map(|o| o.contains(doc)).unwrap_or(false);
        let (from, provenance) = speaker(&entry, &self.operators, self.e2e_operator.as_deref(), owned);
        let body = text(&from);
        self.raise(&home, doc, card, &entry, kind, &from, provenance, body);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frames(text: &str) -> Vec<Frame> {
        let mut p = Parser::default();
        text.split_inclusive('\n').filter_map(|l| p.line(l)).collect()
    }

    /// The desktop core's pinned `message` fixture (trellis v0.212.3), as `to_sse` frames it.
    const FIXTURE: &str = "id: e1:8\nevent: message\ndata: {\"at\":\"2026-10-01T22:00:00Z\",\"card\":42,\"data\":{\"from\":\"operator\",\"kind\":\"person\",\"seq\":3,\"text\":\"@Alice look\",\"to\":[\"Alice\"],\"via\":\"session\"},\"document\":\"Personal.ron\",\"id\":\"e1:8\",\"type\":\"message\",\"v\":1}\n\n";

    #[test]
    fn the_core_fixture_parses_as_one_frame() {
        let f = frames(&format!(": keep-alive\n\n{FIXTURE}"));
        assert_eq!(f.len(), 1, "a keep-alive is not a frame");
        assert_eq!(f[0].id.as_deref(), Some("e1:8"));
        assert_eq!(f[0].event.as_deref(), Some("message"));
        let env: Value = serde_json::from_str(&f[0].data).unwrap();
        assert_eq!(env["data"]["to"][0], "Alice");
        assert_eq!(env["v"], 1);
    }

    #[test]
    fn sse_rules_crlf_multiline_data_and_retry() {
        let mut p = Parser::default();
        assert!(p.line("retry: 5000\r\n").is_none());
        assert!(p.line("data: a\r\n").is_none());
        assert!(p.line("data:b\r\n").is_none());
        let f = p.line("\r\n").unwrap();
        assert_eq!(f.data, "a\nb");
        assert_eq!(p.retry, Some(5000));
        assert!(p.line("\n").is_none(), "an empty dispatch is nothing");
    }

    fn env(ty: &str, data: Value) -> Value {
        json!({"v": 1, "type": ty, "id": "D:1:9", "at": "2026-10-01T22:00:00Z", "document": "D", "card": 7, "data": data})
    }

    #[test]
    fn an_assignment_from_the_operator_is_raised_trusted() {
        let b = crate::bridge::for_test(vec![5]);
        let after = b.on_event(None, &env("property", json!({"key": "Owner", "value": "Me", "by": "alice", "kind": "person", "via": "session", "from_key_owner": true})));
        assert_eq!(after, After::Continue);
        let ev = b.events(0, Duration::from_millis(10));
        assert_eq!(ev.len(), 1);
        assert!(ev[0].trusted, "person + session + owner is the operator");
        assert_eq!(ev[0].card, 5, "raised on the home channel");
        assert!(ev[0].text.contains("assigned to you: card #7"));
    }

    #[test]
    fn a_checklist_line_assigned_to_the_agent_is_raised_once_with_its_text() {
        let b = crate::bridge::for_test(vec![5]);
        let who = json!({"by": "operator", "kind": "person", "via": "session", "from_key_owner": true});
        let task = |item: u64, text: &str, value: &str| {
            let mut d = json!({"item": item, "text": text, "key": "assignee", "value": value, "old": null});
            d.as_object_mut().unwrap().extend(who.as_object().unwrap().clone());
            env("task", d)
        };
        // One write: the card's property (first line wins) and the line's task event.
        let mut p = json!({"key": "assignee", "value": "Me"});
        p.as_object_mut().unwrap().extend(who.as_object().unwrap().clone());
        b.on_event(None, &task(1, "Walk dog", "Me"));
        b.on_event(None, &env("property", p));
        b.on_event(None, &task(1, "Walk dog", "Me"));
        // Another line, another write: raised too. A line for someone else: not.
        b.on_event(None, &task(2, "Feed cat", "Me"));
        b.on_event(None, &task(3, "Mow", "Alice"));
        let ev = b.events(0, Duration::from_millis(10));
        assert_eq!(ev.len(), 2, "{:?}", ev.iter().map(|e| &e.text).collect::<Vec<_>>());
        assert!(ev[0].trusted && ev[0].text.contains("line 1 of checklist card #7, \"Walk dog\""), "{}", ev[0].text);
        assert!(ev[1].text.contains("\"Feed cat\""), "{}", ev[1].text);
    }

    #[test]
    fn the_same_request_is_raised_once() {
        let b = crate::bridge::for_test(vec![5]);
        let p = json!({"key": "owner", "value": "Me", "by": "alice", "kind": "person", "via": "session", "from_key_owner": true});
        b.on_event(None, &env("property", p.clone()));
        b.on_event(None, &env("property", p));
        let m = json!({"lines": ["@Me look"], "names": ["Me"], "by": "alice", "kind": "person", "via": "session", "from_key_owner": true});
        b.on_event(None, &env("mention", m.clone()));
        b.on_event(None, &env("mention", m));
        assert_eq!(b.events(0, Duration::from_millis(10)).len(), 2, "one assignment, one mention");
    }

    #[test]
    fn an_operator_edit_of_a_message_already_given_is_raised_in_that_channel() {
        let b = crate::bridge::for_test(vec![7]);
        b.store.lock().unwrap().reset_cursor(&Chan::new("D", 7).key(), 10).unwrap();
        let op = |extra: Value| {
            let mut d = json!({"seq": 9, "from": "alice", "kind": "person", "via": "session", "from_key_owner": true, "text": "use the blue one", "edited_at": "2026-10-02T22:00:00Z"});
            d.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            d
        };
        b.on_event(None, &env("edited", op(json!({}))));
        b.on_event(None, &env("edited", op(json!({}))));
        let ev = b.events(0, Duration::from_millis(10));
        assert_eq!(ev.len(), 1, "raised once per edit");
        assert!(ev[0].trusted && ev[0].card == 7, "in the channel itself, as the operator");
        assert!(ev[0].text.contains("#9 was edited") && ev[0].text.contains("> use the blue one"));
        b.on_event(None, &env("edited", op(json!({"deleted": true, "text": "(deleted)", "edited_at": "2026-10-02T22:01:00Z"}))));
        let ev = b.events(ev[0].id, Duration::from_millis(10));
        assert!(ev.len() == 1 && ev[0].text.contains("#9 was deleted"));
    }

    #[test]
    fn edits_by_peers_by_itself_or_elsewhere_raise_nothing() {
        let b = crate::bridge::for_test(vec![7]);
        b.store.lock().unwrap().reset_cursor(&Chan::new("D", 7).key(), 10).unwrap();
        b.on_event(None, &env("edited", json!({"seq": 9, "from": "Orbit", "kind": "agent", "via": "api", "text": "x"})));
        b.on_event(None, &env("edited", json!({"seq": 9, "from": "Me", "kind": "agent", "text": "x"})));
        b.on_event(None, &env("edited", json!({"seq": 9, "from": "alice", "kind": "person", "via": "api", "text": "x"})));
        let mut other = env("edited", json!({"seq": 9, "from": "alice", "kind": "person", "via": "session", "from_key_owner": true, "text": "x"}));
        other["card"] = json!(8);
        b.on_event(None, &other);
        assert!(b.events(0, Duration::from_millis(10)).is_empty());
    }

    #[test]
    fn a_bare_name_is_never_trusted() {
        let b = crate::bridge::for_test(vec![5]);
        b.on_event(None, &env("property", json!({"key": "assignee", "value": "Me", "by": "operator"})));
        assert!(b.events(0, Duration::from_millis(10)).is_empty(), "no attestation: unverified, so the gate drops it");
    }

    #[test]
    fn other_properties_unknown_types_and_own_writes_raise_nothing() {
        let b = crate::bridge::for_test(vec![5]);
        b.on_event(None, &env("property", json!({"key": "status", "value": "done", "by": "alice", "kind": "person", "via": "session"})));
        b.on_event(None, &env("property", json!({"key": "owner", "value": "Me", "agent": "Me", "kind": "agent"})));
        b.on_event(None, &env("something_new", json!({"x": 1})));
        assert!(b.events(0, Duration::from_millis(10)).is_empty());
    }

    #[test]
    fn a_signoff_request_names_the_digest() {
        let b = crate::bridge::for_test(vec![5]);
        b.on_event(None, &env("signoff_requested", json!({"reason": "signoff", "card": 7, "title": "Plan", "from": "alice", "digest": "abc", "kind": "person", "via": "session", "from_key_owner": true})));
        let ev = b.events(0, Duration::from_millis(10));
        assert_eq!(ev.len(), 1);
        assert!(ev[0].text.contains("\"digest\": \"abc\""));
        assert!(ev[0].trusted);
    }

    #[test]
    fn structural_events_ask_for_a_refresh_and_replaced_ends_the_stream() {
        let b = crate::bridge::for_test(vec![5]);
        b.on_event(None, &env("claim", json!({})));
        assert!(b.refresh.load(Ordering::SeqCst));
        assert_eq!(b.on_event(None, &env("replaced", json!({}))), After::End(Ended::Replaced));
        assert!(matches!(b.on_event(None, &env("auth", json!({"reason": "revoked"}))), After::End(Ended::Auth(_))));
    }

    #[test]
    fn a_mention_of_someone_else_raises_nothing() {
        let b = crate::bridge::for_test(vec![5]);
        b.on_event(None, &env("mention", json!({"lines": ["@Bob look"], "names": ["Bob"], "by": "alice", "kind": "person", "via": "session"})));
        assert!(b.events(0, Duration::from_millis(10)).is_empty());
        b.on_event(None, &env("mention", json!({"lines": ["@me look"], "names": ["me"], "by": "alice", "kind": "person", "via": "session", "from_key_owner": true})));
        let ev = b.events(0, Duration::from_millis(10));
        assert_eq!(ev.len(), 1);
        assert!(ev[0].text.contains("> @me look"));
    }
}
