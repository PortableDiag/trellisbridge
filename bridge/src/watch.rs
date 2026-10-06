//! The change watcher: @mentions and assignments anywhere in the key's scope.
//!
//! Channels are conversations; the rest of the workspace is where work is
//! asked for too — "@Outrider summarise this" on a card, or `assignee::
//! Outrider` on a task. The watcher reads the server's change log (`GET
//! /api/changes`), and for each card someone else touched, looks for:
//!
//! - an **@mention** line that was not there before — only the new lines, so
//!   editing a card that already mentions the agent does not re-ask it;
//! - an **assignment**: a property `assignee` / `assigned` / `owner` / `agent`
//!   set to the agent's name.
//!
//! Each becomes an event on the home channel, labelled with what it is about,
//! so the conversation about a card happens where the operator already talks
//! to the agent. Provenance comes from the change log's `actor`, which the
//! server takes from the credential, not from anything the writer declared.
//!
//! Woken by `/api/wait?rev=` when the key may hold one; a key scoped narrower
//! than the document gets a 404 there, and the watcher polls the log every
//! 10 s instead (reported to trellis-web).

use crate::bridge::{Bridge, Chan};
use crate::store::Event;
use crate::trellis::in_doc;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::Duration;

pub(crate) const ASSIGN_KEYS: &[&str] = &["assignee", "assigned", "assigned_to", "owner", "agent"];
const POLL: Duration = Duration::from_secs(10);

impl Bridge {
    /// One document's change log, for as long as the bridge runs.
    pub fn watch(&self, doc: &str) {
        let Some(client) = self.client.as_ref() else { return };
        let meta_key = format!("changes:{doc}");
        let mut can_wait = true;
        // After a refusal, try the document wait again now and then: a server
        // upgrade (trellis-web 0.49.0) makes it available to scoped keys.
        let mut polls_since_refused = 0u32;
        let mut rev = match self.store.lock().ok().and_then(|s| s.meta(&meta_key).and_then(|v| v["rev"].as_u64())) {
            Some(r) => r,
            None => loop {
                // First run: start at the present. Mentions made before the
                // agent was listening are history.
                match client.get(&in_doc("/api/changes?since=0&limit=1", doc)) {
                    Ok(v) => {
                        let r = v["rev"].as_u64().unwrap_or(0);
                        self.save_rev(doc, r, v["epoch"].as_u64());
                        println!("watching  {doc} from rev {r}");
                        break r;
                    }
                    Err(e) => {
                        self.set_err(&e.message);
                        std::thread::sleep(POLL);
                    }
                }
            },
        };
        loop {
            // The event stream carries this document while it is up (D14).
            // If the bridge falls back after streaming, start from the present:
            // the stream delivered what came before.
            if self.mode() == crate::bridge::MODE_STREAM {
                while self.mode() == crate::bridge::MODE_STREAM {
                    std::thread::sleep(Duration::from_secs(2));
                }
                {
                    // Not from an old rev: that would replay what the stream
                    // already delivered. Wait until the server says where it is.
                    loop {
                        match client.get(&in_doc("/api/changes?since=0&limit=1", doc)) {
                            Ok(v) => {
                                rev = v["rev"].as_u64().unwrap_or(rev);
                                self.save_rev(doc, rev, v["epoch"].as_u64());
                                break;
                            }
                            Err(e) => {
                                self.set_err(&e.message);
                                std::thread::sleep(POLL);
                                if self.mode() == crate::bridge::MODE_STREAM {
                                    break; // the stream came back first
                                }
                            }
                        }
                    }
                    if self.mode() == crate::bridge::MODE_STREAM {
                        continue;
                    }
                    println!("watching  {doc}: back on the change log from rev {rev}");
                }
            }
            // Wake: a held wait when allowed, otherwise a fixed interval.
            if can_wait {
                match client.get(&in_doc(&format!("/api/wait?rev={rev}"), doc)) {
                    Ok(_) => {
                        if self.watch_mode(doc) == "poll" {
                            println!("watching  {doc}: the document wait is available again — no more polling");
                        }
                        self.set_watch(doc, "wait");
                    }
                    Err(e) if matches!(e.status, Some(403) | Some(404)) => {
                        if self.watch_mode(doc) != "poll" {
                            eprintln!("trellisbridge: {doc}: document wait refused ({}) — polling the change log every {}s", e.message, POLL.as_secs());
                        }
                        can_wait = false;
                        self.set_watch(doc, "poll");
                    }
                    Err(e) => {
                        self.set_err(&e.message);
                        std::thread::sleep(POLL);
                    }
                }
            } else {
                std::thread::sleep(POLL);
                polls_since_refused += 1;
                if polls_since_refused >= 60 {
                    polls_since_refused = 0;
                    can_wait = true;
                }
            }
            match client.get(&in_doc(&format!("/api/changes?since={rev}&limit=500"), doc)) {
                Ok(v) => {
                    let epoch = v["epoch"].as_u64();
                    let saved_epoch = self.store.lock().ok().and_then(|s| s.meta(&meta_key).and_then(|m| m["epoch"].as_u64()));
                    let lost = v["truncated"].as_bool() == Some(true) || (saved_epoch.is_some() && epoch != saved_epoch);
                    if lost {
                        // Entries were pruned or the log restarted: nothing
                        // incremental can be trusted. Start again from now.
                        rev = v["rev"].as_u64().unwrap_or(rev);
                        self.save_rev(doc, rev, epoch);
                        continue;
                    }
                    let changes = v["changes"].as_array().cloned().unwrap_or_default();
                    for c in &changes {
                        self.consider(client, doc, c);
                    }
                    // Channels carried by the document wait: read each owned
                    // one that someone else said something in.
                    if self.watch_mode(doc) == "wait" {
                        for card in said_in(&changes, &self.agent) {
                            let ch = Chan::new(doc, card);
                            if self.owns(&ch) {
                                self.sweep(&ch);
                            }
                        }
                    }
                    let top = changes.iter().filter_map(|c| c["seq"].as_u64()).max().unwrap_or(rev);
                    if top > rev {
                        rev = top;
                        self.save_rev(doc, rev, epoch);
                    }
                    self.set_ok();
                }
                Err(e) => {
                    self.set_err(&e.message);
                    std::thread::sleep(POLL);
                }
            }
        }
    }

    /// Has this request (an assignment's value, or a set of mention lines)
    /// not been raised yet? Records it either way, so the change-log watcher and
    /// the event stream (D14) never raise the same one twice while they hand
    /// over.
    pub(crate) fn first_time(&self, key: &str, val: &str) -> bool {
        let Ok(mut s) = self.store.lock() else { return true };
        if s.meta(key).and_then(Value::as_str) == Some(val) {
            return false;
        }
        let _ = s.set_meta(key, json!(val));
        true
    }

    /// The mention lines on a card not raised yet, recorded as raised.
    pub(crate) fn unseen_mentions(&self, doc: &str, card: u64, lines: &[String]) -> Vec<String> {
        let key = format!("mentions:{doc}:{card}");
        let Ok(mut s) = self.store.lock() else { return lines.to_vec() };
        let mut seen: Vec<String> = s.meta(&key).cloned().and_then(|v| serde_json::from_value(v).ok()).unwrap_or_default();
        let new: Vec<String> = lines.iter().filter(|l| !seen.contains(l)).cloned().collect();
        seen.extend(new.iter().cloned());
        let _ = s.set_meta(&key, json!(seen));
        new
    }

    fn save_rev(&self, doc: &str, rev: u64, epoch: Option<u64>) {
        if let Ok(mut s) = self.store.lock() {
            let _ = s.set_meta(&format!("changes:{doc}"), json!({ "rev": rev, "epoch": epoch }));
        }
    }

    pub fn watch_mode(&self, doc: &str) -> String {
        self.status().watch.get(doc).cloned().unwrap_or_default()
    }

    fn set_watch(&self, doc: &str, how: &str) {
        if let Ok(mut s) = self.status.lock() {
            s.watch.insert(doc.to_string(), how.to_string());
        }
    }

    /// One change-log entry: is it something this agent was asked to do?
    fn consider(&self, client: &crate::trellis::Client, doc: &str, c: &Value) {
        if c["entity"] != "card" || c["op"] == "deleted" {
            return;
        }
        if c["agent"].as_str() == Some(self.agent.as_str()) {
            return; // our own write
        }
        if c["kind"] == "system" {
            return; // the server's own housekeeping (a mirror poll…) is never a request
        }
        let Some(card) = c["id"].as_u64() else { return };
        if self.owns(&Chan::new(doc, card)) {
            return; // a conversation the pollers already carry
        }
        let Some(home) = self.home_for(doc) else { return };
        if let Some(label) = c["agent"].as_str().filter(|a| !a.is_empty()) {
            if self.builtin(label).is_none() && !self.operators.iter().any(|o| o == label) {
                self.refresh_builtins();
            }
        }
        let owned = self.owned_docs.lock().map(|o| o.contains(doc)).unwrap_or(false);
        let (from, provenance) = speaker(c, &self.operators, self.e2e_operator.as_deref(), owned);

        // Assignment: set on a property, named in the entry itself.
        if let Some(p) = c["property"].as_array() {
            let key = p.first().and_then(Value::as_str).unwrap_or("").to_ascii_lowercase();
            let val = p.get(1).and_then(Value::as_str).unwrap_or("");
            if ASSIGN_KEYS.contains(&key.as_str())
                && val.to_ascii_lowercase().contains(&self.agent.to_ascii_lowercase())
                && self.first_time(&format!("assigned:{doc}:{card}:{key}"), val)
            {
                let text = format!(
                    "[assigned to you: card #{card} \"{}\" — {key}:: {val}, set by {from}]\n\nRead it with GET /api/cards/{card}.",
                    c["title"].as_str().unwrap_or("")
                );
                self.raise(&home, doc, card, c, "assigned", &from, provenance, text);
                return;
            }
        }

        // Mention: only a body (or new card) can carry one; read the card.
        let fields: Vec<&str> = c["fields"].as_array().map(|f| f.iter().filter_map(Value::as_str).collect()).unwrap_or_default();
        let touched_text = c["op"] == "created" || fields.iter().any(|f| f.starts_with("body") || f.starts_with("title") || f.starts_with("items") || f.starts_with("channel"));
        if !touched_text {
            return;
        }
        let Ok(v) = client.get(&in_doc(&format!("/api/cards/{card}"), doc)) else { return };
        let cardv = if v.get("card").is_some() { &v["card"] } else { &v };
        // A channel that is not ours is a conversation between others: the
        // operator briefing a built-in agent may well quote "@Outrider …" for
        // it to pass on. Found live — Outrider acted on the quote and made a
        // duplicate. So a mention there is no request; only being called by
        // name is (`hear`). Mentions count on ordinary cards (and our own
        // channels carry their messages through the pollers).
        if is_channel(cardv) {
            self.hear(client, doc, card);
            return;
        }
        let lines = mention_lines(&card_text(cardv), &self.agent);
        let key = format!("mentions:{doc}:{card}");
        let seen: BTreeSet<String> = self
            .store
            .lock()
            .ok()
            .and_then(|s| s.meta(&key).cloned())
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();
        let new: Vec<&String> = lines.iter().filter(|l| !seen.contains(*l)).collect();
        if let Ok(mut s) = self.store.lock() {
            let _ = s.set_meta(&key, json!(lines));
        }
        if new.is_empty() {
            return;
        }
        let quoted: Vec<String> = new.iter().map(|l| format!("> {l}")).collect();
        let text = format!(
            "[@mention on card #{card} \"{}\", by {from}]\n{}\n\nRead the card with GET /api/cards/{card}; answer here, or on the card if that is what was asked.",
            cardv["title"].as_str().unwrap_or(""),
            quoted.join("\n")
        );
        self.raise(&home, doc, card, c, "mention", &from, provenance, text);
    }

    /// Someone else's group channel moved. If a message there calls this
    /// agent by name (`called_by_name`), join the channel from that message
    /// on: the pollers then carry what is addressed to it, and it answers in
    /// place. Found live (2026-10-01): "@Nexus you there?" in Orbit's #21 was
    /// addressed to Nexus by the server and never reached it.
    pub(crate) fn hear(&self, client: &crate::trellis::Client, doc: &str, card: u64) {
        let ch = Chan::new(doc, card);
        let key = format!("heard:{doc}:{card}");
        let last = self.store.lock().ok().and_then(|s| s.meta(&key).and_then(Value::as_u64));
        let since = match last {
            Some(s) => s,
            // First sight: only the message that moved it. Earlier ones are history.
            None => match client.get(&in_doc(&format!("/api/cards/{card}/channel?since={}", u64::MAX >> 12), doc)) {
                Ok(v) => v["seq"].as_u64().unwrap_or(0).saturating_sub(1),
                Err(_) => return,
            },
        };
        let Ok(v) = client.get(&in_doc(&format!("/api/cards/{card}/channel?since={since}"), doc)) else { return };
        let seq = v["seq"].as_u64().unwrap_or(since);
        if let Ok(mut s) = self.store.lock() {
            let _ = s.set_meta(&key, json!(seq));
        }
        if v["group"].as_bool() != Some(true) {
            return; // a one-to-one channel is between its two ends
        }
        let first = v["messages"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|m| m["seq"].as_u64().unwrap_or(0) > since)
            .find(|m| called_by_name(m, &self.agent));
        let Some(m) = first else { return };
        let at = m["seq"].as_u64().unwrap_or(1);
        let all = {
            let Ok(mut called) = self.called.lock() else { return };
            if !called.insert(ch.clone()) {
                return;
            }
            called.iter().map(|c| json!([c.doc, c.card])).collect::<Vec<_>>()
        };
        if let Ok(mut s) = self.store.lock() {
            let _ = s.set_meta("called", json!(all));
            if let Some(to) = call_cursor(s.cursor(&ch.key()), at) {
                let _ = s.reset_cursor(&ch.key(), to);
            }
        }
        println!("channel   {ch}: called by {} at seq {at} — answering there", m["from"].as_str().unwrap_or("?"));
        self.sweep(&ch);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn raise(&self, home: &Chan, doc: &str, card: u64, c: &Value, kind: &str, from: &str, provenance: &str, text: String) {
        let owned_doc = self.owned_docs.lock().map(|o| o.contains(doc)).unwrap_or(false);
        // A card in another document than the home channel's: say which.
        let text = if doc != home.doc { format!("{text}\n(That card is in document {doc} — pass document={doc}.)") } else { text };
        let e = Event {
            id: 0,
            document: home.doc.clone(),
            card: home.card,
            node: 0,
            seq: 0,
            from: from.to_string(),
            provenance: provenance.to_string(),
            at: c["ts"].as_u64().map(|t| t.to_string()).unwrap_or_default(),
            text,
            files: Vec::new(),
            about: Some(json!({ "kind": kind, "document": doc, "card": card, "node": c["node"], "title": c["title"] })),
            builtin: if provenance == "agent" || provenance == "builtin" { self.builtin(from) } else { None },
            // The same rule as channel messages: a built-in agent is trusted
            // only when the server recorded the change as `builtin`
            // (trellis-web 0.53.0) and /api/agents lists it here. An entry
            // without `kind` can only ever trust the operator.
            trusted: crate::bridge::trusted(provenance, self.builtin(from).as_ref(), doc, c["from_key_owner"].as_bool(), owned_doc),
            peer: false,
            from_key_owner: c["from_key_owner"].as_bool(),
            lead_only: false,
            broadcast: false,
            expect: None,
            origin: c["origin"].as_str().map(str::to_string),
        };
        let mut e = e;
        e.peer = !e.trusted && matches!(provenance, "agent" | "person" | "builtin");
        if !self.admit(&home.key(), &e) {
            return;
        }
        println!("{kind:<9} card #{card} in {doc} → event on home channel {home}");
        let added = self.store.lock().ok().map(|mut s| s.push(vec![e]).unwrap_or_default()).unwrap_or_default();
        if !added.is_empty() {
            self.woke.notify_all();
        }
    }
}

/// Who wrote it. An agent label wins whatever the `actor` — a built-in agent
/// works inside the server, and its writes must never read as the person's
/// (trellis-web 0.51.0). With no label: `ui` is the person in the browser, and
/// an API write with no `X-Agent` is the account's own key, which the server
/// also attributes to the person.
pub(crate) fn speaker(c: &Value, operators: &[String], e2e_operator: Option<&str>, owned: bool) -> (String, &'static str) {
    let op = operators.get(1).cloned().unwrap_or_else(|| "operator".into());
    // The e2e stand-in, as in `bridge::provenance`: only when configured and
    // only when the server verified the bound name.
    if let (Some(name), Some("agent")) = (e2e_operator, c["kind"].as_str()) {
        if c["agent"].as_str() == Some(name)
            && c["agent_verified"].as_bool() == Some(true)
            && c["from_key_owner"].as_bool() == Some(true)
        {
            return (op, "operator");
        }
    }
    // Every key on the account is `from_key_owner`, and an unbound key with
    // no `X-Agent` is recorded `kind: person` (#2951 seq 66). The change log
    // says how a change was made (`actor`, and `via` since 0.59.3): a key is
    // `api`, never the operator. A browser, the operator's linked Telegram
    // chat, or the operator's phone on a device key (`via: app`) is.
    let via = c["via"].as_str();
    let via_key = via == Some("api") || (c["actor"].as_str() == Some("api") && via != Some("app"));
    // A person's change is the operator's only when the server says it came
    // from the key owner, or — until it says — in a document the owner owns.
    let person = |op: String| -> (String, &'static str) {
        match c["from_key_owner"].as_bool() {
            _ if via_key => ("person".into(), "person"),
            Some(true) => (op, "operator"),
            Some(false) => ("person".into(), "person"),
            None if owned => (op, "operator"),
            None => ("person".into(), "person"),
        }
    };
    let label = c["agent"].as_str().filter(|a| !a.is_empty());
    // trellis-web 0.53.0 records `kind` when the change is written; it wins.
    // `builtin` keeps the agent's name in `agent`.
    match (c["kind"].as_str(), label) {
        (Some("builtin"), Some(a)) => (a.to_string(), "builtin"),
        (Some("builtin"), None) => ("builtin".into(), "builtin"),
        (Some("agent"), Some(a)) => (a.to_string(), "agent"),
        (Some("agent"), None) => ("agent".into(), "agent"),
        (Some("person"), _) => person(op),
        // trellis-web 0.53.0 kinds every change-log entry; one without `kind`
        // (older, or another server) is unverified whoever it names.
        (Some("system"), _) => ("system".into(), "system"),
        _ => (label.unwrap_or("unknown").to_string(), "unverified"),
    }
}

/// A channel card, as `GET /api/cards/{id}` shows it.
/// The cards someone other than `agent` changed, in log order, once each: a
/// message said in a channel is a change to its card (`channel.say`), and so is
/// text typed into it from the app.
pub fn said_in(changes: &[Value], agent: &str) -> Vec<u64> {
    let mut seen = BTreeSet::new();
    changes
        .iter()
        .filter(|c| c["entity"] == "card" && c["op"] != "deleted")
        .filter(|c| c["agent"].as_str() != Some(agent))
        // A reaction is not a message (web/desktop, 2754 #249–#251): nothing to read.
        .filter(|c| c["fields"].as_array().is_none_or(|f| f.is_empty() || f.iter().any(|x| x != "reactions")))
        .filter_map(|c| c["id"].as_u64())
        .filter(|id| seen.insert(*id))
        .collect()
}

/// A channel message that calls `agent` by name: the server addressed it to
/// the agent (`to`), and the agent is the first one it @-mentions. "@trellis
/// tell @Nexus …" is for trellis; "@Nexus you there?" is for Nexus. A sender's
/// explicit list (`say {to}`, `to_source: "list"`, desktop 0.217.0) is the
/// addressing itself, so there the text's @mentions are prose and `to` alone
/// decides.
pub fn called_by_name(m: &Value, agent: &str) -> bool {
    if m["from"].as_str().is_some_and(|f| f.eq_ignore_ascii_case(agent)) {
        return false;
    }
    let to = m["to"].as_array().is_some_and(|t| t.iter().any(|n| n.as_str().is_some_and(|n| n.eq_ignore_ascii_case(agent))));
    if m["to_source"].as_str() == Some("list") {
        return to;
    }
    to && first_mention(m["text"].as_str().unwrap_or("")).is_some_and(|n| n.eq_ignore_ascii_case(agent))
}

/// The first @name in a text, at a word start.
fn first_mention(text: &str) -> Option<&str> {
    let word = |ch: char| ch.is_alphanumeric() || ch == '_';
    let mut prev: Option<char> = None;
    for (i, ch) in text.char_indices() {
        if ch == '@' && !prev.is_some_and(word) {
            let rest = &text[i + 1..];
            let end = rest.find(|c: char| !word(c)).unwrap_or(rest.len());
            if end > 0 {
                return Some(&rest[..end]);
            }
        }
        prev = Some(ch);
    }
    None
}

pub(crate) fn is_channel(c: &Value) -> bool {
    c.get("channel").map(|ch| !ch.is_null()).unwrap_or(false)
}

/// Everything readable on a card: title, body, checklist lines, table cells.
fn card_text(c: &Value) -> String {
    let mut out = vec![c["title"].as_str().unwrap_or("").to_string(), c["body"].as_str().unwrap_or("").to_string()];
    if let Some(items) = c["items"].as_array() {
        out.extend(items.iter().filter_map(|i| i["text"].as_str().map(str::to_string)));
    }
    if let Some(rows) = c["rows"].as_array() {
        for r in rows {
            if let Some(cells) = r.as_array() {
                out.push(cells.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(" | "));
            }
        }
    }
    out.join("\n")
}

/// Lines that @-mention the agent, as a whole word, case-insensitively.
pub fn mention_lines(text: &str, agent: &str) -> Vec<String> {
    let needle = format!("@{}", agent.to_ascii_lowercase());
    text.lines()
        .filter(|l| {
            let low = l.to_ascii_lowercase();
            low.match_indices(&needle).any(|(i, _)| {
                let end = i + needle.len();
                let word = |ch: char| ch.is_alphanumeric() || ch == '_';
                let before_ok = !low[..i].chars().next_back().map(word).unwrap_or(false);
                let after_ok = !low[end..].chars().next().map(word).unwrap_or(false);
                before_ok && after_ok
            })
        })
        .map(|l| l.trim().to_string())
        .collect()
}

/// Where a channel's cursor goes when a message at `at` calls the agent in:
/// just before the call, skipping an old cursor's backlog, but never back
/// over what was already read. Found live (2026-10-03): a restart re-heard a
/// call at #21 2118 that Nexus had long answered, moved its cursor back
/// there, and Nexus answered old messages again.
fn call_cursor(cursor: u64, at: u64) -> Option<u64> {
    (cursor < at).then(|| at - 1)
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_call_never_moves_the_cursor_back_over_what_was_read() {
        use super::call_cursor;
        assert_eq!(call_cursor(0, 2118), Some(2117), "a new channel reads from the call");
        assert_eq!(call_cursor(1500, 2118), Some(2117), "an old cursor's backlog is skipped");
        assert_eq!(call_cursor(2352, 2118), None, "already read: left alone");
        assert_eq!(call_cursor(2118, 2118), None, "the call itself was read");
    }

    use super::*;

    #[test]
    fn a_say_by_someone_else_wakes_its_channel_once() {
        let log = vec![
            json!({"entity": "card", "op": "updated", "id": 21, "fields": ["channel.say"], "agent": "E2EOperator"}),
            json!({"entity": "card", "op": "updated", "id": 21, "fields": ["channel.say"]}),
            json!({"entity": "card", "op": "updated", "id": 28, "fields": ["channel.say"], "agent": "Outrider"}),
            json!({"entity": "card", "op": "deleted", "id": 30}),
            json!({"entity": "node", "op": "updated", "id": 5}),
            json!({"entity": "card", "op": "updated", "id": 66, "fields": ["body"]}),
            json!({"entity": "card", "op": "updated", "id": 77, "fields": ["reactions"]}),
        ];
        assert_eq!(said_in(&log, "Outrider"), vec![21, 66]);
    }

    #[test]
    fn mentions_are_whole_words_any_case() {
        let t = "hi @Outrider can you look\n@outriders is someone else\nemail a@outrider.dev? no\nplain line\nfinal @OUTRIDER.";
        assert_eq!(mention_lines(t, "Outrider"), vec!["hi @Outrider can you look", "final @OUTRIDER."]);
    }

    #[test]
    fn provenance_comes_from_the_credential() {
        let ops = vec!["operator".to_string(), "alice".to_string()];
        // No `kind`: unverified, whatever the actor or label says.
        assert_eq!(speaker(&json!({"actor":"ui"}), &ops, None, true).1, "unverified");
        assert_eq!(speaker(&json!({"actor":"api","agent":"Robot"}), &ops, None, true), ("Robot".into(), "unverified"));
        assert_eq!(speaker(&json!({"actor":"ui","agent":"Helper"}), &ops, None, true).1, "unverified");
        // 0.53.0: the recorded kind wins.
        assert_eq!(speaker(&json!({"kind":"builtin","actor":"ui","agent":"Helper"}), &ops, None, true), ("Helper".into(), "builtin"));
        assert_eq!(speaker(&json!({"kind":"agent","agent":"TrellisWebAgent"}), &ops, None, true), ("TrellisWebAgent".into(), "agent"), "a key using a built-in's name");
        assert_eq!(speaker(&json!({"kind":"person","actor":"ui"}), &ops, None, true), ("alice".into(), "operator"));
        assert_eq!(speaker(&json!({"kind":"person","actor":"api"}), &ops, None, true).1, "person", "an unbound key with no X-Agent (#66)");
        assert_eq!(speaker(&json!({"kind":"person","via":"api"}), &ops, None, true).1, "person");
        assert_eq!(speaker(&json!({"kind":"person","via":"telegram","from_key_owner":true}), &ops, None, true).1, "operator");
        assert_eq!(speaker(&json!({"kind":"person","actor":"api","via":"app","from_key_owner":true}), &ops, None, true).1, "operator", "the operator's phone (2754 #403)");
        assert_eq!(speaker(&json!({"kind":"person","actor":"api","via":"app","from_key_owner":false}), &ops, None, true).1, "person");
        assert_eq!(speaker(&json!({"kind":"person","actor":"app","from_key_owner":true}), &ops, None, true).1, "operator", "/api/changes names a device key actor app (2754 #408)");
        let e2e = json!({"kind":"agent","actor":"api","agent":"E2EOperator","agent_verified":true,"from_key_owner":true});
        assert_eq!(speaker(&e2e, &ops, Some("E2EOperator"), true).1, "operator");
        assert_eq!(speaker(&e2e, &ops, None, true).1, "agent");
        assert_eq!(speaker(&json!({"kind":"agent","agent":"E2EOperator","from_key_owner":true}), &ops, Some("E2EOperator"), true).1, "agent", "unverified");
        assert_eq!(speaker(&json!({"kind":"person"}), &ops, None, false).1, "person", "not owned: a namesake is a person");
        assert_eq!(speaker(&json!({"kind":"person","from_key_owner":false}), &ops, None, true).1, "person");
    }

    #[test]
    fn called_by_name_only_when_first_and_addressed() {
        let m = |text: &str, to: Value| json!({"from": "alice", "text": text, "to": to});
        assert!(called_by_name(&m("@Nexus you there?", json!(["Nexus"])), "Nexus"));
        assert!(called_by_name(&m("hey @nexus, status", json!(["Nexus"])), "Nexus"));
        assert!(!called_by_name(&m("@trellis tell @Nexus to look", json!(["trellis", "Nexus"])), "Nexus"), "a quote for another agent");
        assert!(!called_by_name(&m("@Nexus you there?", json!([])), "Nexus"), "the server did not address it");
        assert!(!called_by_name(&m("mail a@Nexus.dev", json!(["Nexus"])), "Nexus"));
        assert!(!called_by_name(&json!({"from": "Nexus", "text": "@Nexus", "to": ["Nexus"]}), "Nexus"), "its own");
        let l = |text: &str, to: Value| json!({"from": "alice", "text": text, "to": to, "to_source": "list"});
        assert!(called_by_name(&l("you there?", json!(["Nexus"])), "Nexus"), "an explicit list needs no @mention");
        assert!(called_by_name(&l("@trellis said to ask you", json!(["Nexus"])), "Nexus"), "with a list, @mentions are prose");
        assert!(!called_by_name(&l("@Nexus look", json!(["trellis"])), "Nexus"), "a list without the agent");
        assert!(!called_by_name(&json!({"from": "alice", "text": "@trellis tell @Nexus", "to": ["trellis", "Nexus"], "to_source": "mentions"}), "Nexus"));
    }

    #[test]
    fn a_channel_card_is_recognised() {
        assert!(is_channel(&json!({"channel": {"participants": ["X"]}})));
        assert!(!is_channel(&json!({"channel": null})));
        assert!(!is_channel(&json!({"title": "plain"})));
    }

    #[test]
    fn card_text_covers_checklists_and_tables() {
        let c = json!({"title":"T","body":"b","items":[{"text":"do @Outrider"}],"rows":[["a","b"]]});
        let t = card_text(&c);
        assert!(t.contains("do @Outrider") && t.contains("a | b"));
    }
}
