//! Cursors and undelivered events — the bridge owns both (DESIGN D3).
//!
//! Hermes gets no catch-up of its own, so everything it has not acked is kept
//! here and survives a restart. A message is offered until it is acked and
//! never after; the per-channel cursor is the highest Trellis `seq` turned into
//! an event, so a restart neither replays nor skips one.
//!
//! Saved as JSON beside the config, written to a temp file and renamed. The
//! config dir is on the home volume, not the exfat one, so the rename is
//! atomic.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub id: u64,
    pub document: String,
    pub card: u64,
    pub node: u64,
    pub seq: u64,
    pub from: String,
    /// `operator` for the account holder, `agent` for any other named speaker.
    /// The agent is told who wrote it; it is never told to obey it (D7).
    pub provenance: String,
    pub at: String,
    pub text: String,
    /// Files that came with the message, `[{index, name, ext, bytes}]`, when
    /// Trellis reports them; fetch one with `GET /api/files/{card}/{index}`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<serde_json::Value>,
    /// For an event that is not a channel message — an @mention or an
    /// assignment on some other card — what it is about:
    /// `{kind, card, node, title}`. The event itself is delivered to the home
    /// channel, where the conversation about it happens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub about: Option<serde_json::Value>,
    /// When the sender is one of the server's own built-in agents: who it is
    /// (`{name, reach, document, node, card}` from `GET /api/agents`). Its
    /// work, not the operator's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub builtin: Option<serde_json::Value>,
    /// May the agent act on this as a request? True only for the operator,
    /// and for one of the operator's own built-in agents whose message the
    /// server recorded as `kind: builtin` (an API key cannot produce that) and
    /// which `GET /api/agents` lists in that document. Everything else — other
    /// agents, other people — reaches the agent as data at most.
    #[serde(default)]
    pub trusted: bool,
    /// Another agent, or another person: someone to talk with, not someone
    /// to take orders from. The agent may discuss, share and help; it does
    /// not delete, send outside, or change things on a peer's say-so.
    #[serde(default)]
    pub peer: bool,
    /// The server's word on whether this came from the key owner's account
    /// (trellis-web, planned): the operator, or the operator's own built-in
    /// agent. Absent until the server sends it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_key_owner: Option<bool>,
    /// A group-channel message that reached this agent only as the channel's
    /// lead: it names nobody, or only someone else ("Alice, that's not it").
    /// Silence is a fair answer to it, so the plugin posts no warning when
    /// the agent stays quiet.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub lead_only: bool,
    /// A group-channel message that names this agent only by a group word
    /// (@agents, @all, @everyone): a note to the room, which not every agent
    /// must answer. Silence is fair here too; no warning is posted.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub broadcast: bool,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Saved {
    next_id: u64,
    /// card id → highest `seq` already turned into an event (or skipped as
    /// our own). JSON keys are strings, so the ids are stored that way.
    cursors: BTreeMap<String, u64>,
    events: Vec<Event>,
    /// Small named values that must survive a restart: the change-log
    /// position, and which @mention lines have already been answered.
    #[serde(default)]
    meta: BTreeMap<String, serde_json::Value>,
}

pub struct Store {
    path: Option<PathBuf>,
    saved: Saved,
}

impl Store {
    pub fn open(path: &Path) -> Result<Store, String> {
        let saved = match std::fs::read_to_string(path) {
            Ok(text) => serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Saved::default(),
            Err(e) => return Err(format!("{}: {e}", path.display())),
        };
        Ok(Store { path: Some(path.to_path_buf()), saved })
    }

    #[cfg(test)]
    pub fn in_memory() -> Store {
        Store { path: None, saved: Saved::default() }
    }

    /// Channel cursors were keyed by card id alone before the bridge followed
    /// several documents; they belong to the default document. Once, on open.
    pub fn migrate(&mut self, default_doc: &str) -> Result<(), String> {
        let old: Vec<String> = self.saved.cursors.keys().filter(|k| !k.contains(':')).cloned().collect();
        if old.is_empty() {
            return Ok(());
        }
        for k in old {
            if let Some(v) = self.saved.cursors.remove(&k) {
                self.saved.cursors.insert(format!("{default_doc}:{k}"), v);
            }
        }
        if let Some(v) = self.saved.meta.remove("changes") {
            self.saved.meta.insert(format!("changes:{default_doc}"), v);
        }
        let mentions: Vec<String> = self.saved.meta.keys().filter(|k| k.starts_with("mentions:") && k.matches(':').count() == 1).cloned().collect();
        for k in mentions {
            if let Some(v) = self.saved.meta.remove(&k) {
                let card = k.trim_start_matches("mentions:");
                self.saved.meta.insert(format!("mentions:{default_doc}:{card}"), v);
            }
        }
        self.save()
    }

    /// `false` for a channel never read before — a first run.
    pub fn knows(&self, key: &str) -> bool {
        self.saved.cursors.contains_key(key)
    }

    pub fn cursor(&self, key: &str) -> u64 {
        self.saved.cursors.get(key).copied().unwrap_or(0)
    }

    /// Record everything read from one channel up to `seq`, adding `new` as
    /// events. One save covers both, so a crash cannot move the cursor past a
    /// message that was never stored.
    pub fn advance(&mut self, key: &str, seq: u64, new: Vec<Event>) -> Result<Vec<Event>, String> {
        let mut added = Vec::new();
        for mut e in new {
            self.saved.next_id += 1;
            e.id = self.saved.next_id;
            added.push(e.clone());
            self.saved.events.push(e);
        }
        let cur = self.saved.cursors.entry(key.to_string()).or_insert(0);
        if seq > *cur {
            *cur = seq;
        }
        self.save()?;
        Ok(added)
    }

    /// A channel whose `seq` went backwards was cleared and re-made; start it
    /// over from what the server now says rather than waiting for numbers that
    /// will never come.
    pub fn reset_cursor(&mut self, key: &str, seq: u64) -> Result<(), String> {
        self.saved.cursors.insert(key.to_string(), seq);
        self.save()
    }

    /// Undelivered events after `after`, oldest first.
    pub fn after(&self, after: u64) -> Vec<Event> {
        self.saved.events.iter().filter(|e| e.id > after).cloned().collect()
    }

    /// `Ok(true)` if it was pending, `Ok(false)` if it was already acked or
    /// never existed — both mean it will not be offered again.
    pub fn ack(&mut self, id: u64) -> Result<bool, String> {
        let before = self.saved.events.len();
        self.saved.events.retain(|e| e.id != id);
        let removed = self.saved.events.len() != before;
        if removed {
            self.save()?;
        }
        Ok(removed)
    }

    /// Add events that belong to no channel cursor (mentions, assignments).
    pub fn push(&mut self, new: Vec<Event>) -> Result<Vec<Event>, String> {
        let mut added = Vec::new();
        for mut e in new {
            self.saved.next_id += 1;
            e.id = self.saved.next_id;
            added.push(e.clone());
            self.saved.events.push(e);
        }
        if !added.is_empty() {
            self.save()?;
        }
        Ok(added)
    }

    pub fn meta(&self, key: &str) -> Option<&serde_json::Value> {
        self.saved.meta.get(key)
    }

    pub fn set_meta(&mut self, key: &str, v: serde_json::Value) -> Result<(), String> {
        self.saved.meta.insert(key.to_string(), v);
        self.save()
    }

    /// Group channels joined because someone called the agent there by name.
    pub fn called(&self) -> std::collections::BTreeSet<crate::bridge::Chan> {
        self.meta("called")
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default()
            .iter()
            .filter_map(|c| Some(crate::bridge::Chan::new(c[0].as_str()?, c[1].as_u64()?)))
            .collect()
    }

    pub fn pending(&self) -> usize {
        self.saved.events.len()
    }

    fn save(&self) -> Result<(), String> {
        let Some(path) = &self.path else { return Ok(()) };
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        }
        let tmp = path.with_extension("json.tmp");
        let text = serde_json::to_string_pretty(&self.saved).map_err(|e| e.to_string())?;
        std::fs::write(&tmp, text).map_err(|e| format!("{}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, path).map_err(|e| format!("{}: {e}", path.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(card: u64, seq: u64) -> Event {
        Event {
            id: 0,
            document: "d".into(),
            card,
            node: 5,
            seq,
            from: "operator".into(),
            provenance: "operator".into(),
            at: String::new(),
            text: format!("m{seq}"),
            files: Vec::new(),
            about: None,
            builtin: None,
            trusted: false,
            peer: false,
            from_key_owner: None,
            lead_only: false,
            broadcast: false,
        }
    }

    #[test]
    fn events_get_increasing_ids_and_ack_removes_them() {
        let mut s = Store::in_memory();
        let added = s.advance("D:7", 2, vec![ev(7, 1), ev(7, 2)]).unwrap();
        assert_eq!(added.iter().map(|e| e.id).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(s.cursor("D:7"), 2);
        assert_eq!(s.after(1).len(), 1);
        assert!(s.ack(1).unwrap());
        assert!(!s.ack(1).unwrap(), "a second ack is not an error, and not pending");
        assert_eq!(s.after(0).iter().map(|e| e.id).collect::<Vec<_>>(), [2]);
    }

    #[test]
    fn cursor_never_moves_backwards_on_advance() {
        let mut s = Store::in_memory();
        s.advance("D:7", 5, vec![]).unwrap();
        s.advance("D:7", 3, vec![]).unwrap();
        assert_eq!(s.cursor("D:7"), 5);
        s.reset_cursor("D:7", 1).unwrap();
        assert_eq!(s.cursor("D:7"), 1, "a cleared channel is reset explicitly");
    }

    #[test]
    fn pushed_events_and_meta_share_the_id_space_and_persist() {
        let dir = std::env::temp_dir().join(format!("tb-meta-{}", std::process::id()));
        let path = dir.join("state.json");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let mut s = Store::open(&path).unwrap();
            s.advance("D:7", 1, vec![ev(7, 1)]).unwrap();
            let p = s.push(vec![ev(0, 0)]).unwrap();
            assert_eq!(p[0].id, 2);
            s.set_meta("changes", serde_json::json!({"rev": 9})).unwrap();
        }
        let s = Store::open(&path).unwrap();
        assert_eq!(s.meta("changes").unwrap()["rev"], 9);
        assert_eq!(s.after(0).len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn old_card_keys_move_to_the_default_document() {
        let mut s = Store::in_memory();
        s.saved.cursors.insert("21".into(), 40);
        s.saved.meta.insert("changes".into(), serde_json::json!({"rev": 9}));
        s.saved.meta.insert("mentions:29".into(), serde_json::json!(["x"]));
        s.migrate("D").unwrap();
        assert_eq!(s.cursor("D:21"), 40);
        assert!(!s.knows("21"));
        assert_eq!(s.meta("changes:D").unwrap()["rev"], 9);
        assert!(s.meta("mentions:D:29").is_some());
        s.migrate("D").unwrap();
        assert_eq!(s.cursor("D:21"), 40, "a second run changes nothing");
    }

    #[test]
    fn survives_a_restart() {
        let dir = std::env::temp_dir().join(format!("tb-store-{}", std::process::id()));
        let path = dir.join("state.json");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let mut s = Store::open(&path).unwrap();
            s.advance("D:7", 2, vec![ev(7, 1), ev(7, 2)]).unwrap();
            s.ack(1).unwrap();
        }
        let s = Store::open(&path).unwrap();
        assert_eq!(s.cursor("D:7"), 2);
        assert_eq!(s.after(0).iter().map(|e| e.id).collect::<Vec<_>>(), [2]);
        let mut s = s;
        let next = s.advance("D:7", 3, vec![ev(7, 3)]).unwrap();
        assert_eq!(next[0].id, 3, "ids are never reused after a restart");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
