//! The bridge proper: which channels it answers in, the pollers that turn
//! Trellis messages into events, and what `say` is allowed to reach.
//!
//! Pull, not push (D2). A document the change watcher can hold a wait on
//! (`/api/wait?rev=`) needs nothing more: every `say` is a change-log entry,
//! so the watcher reads each owned channel it names with `?since=` against the
//! bridge's own cursor — one held request per document, however many channels.
//! Every owned channel is also swept every 5 minutes. Not the server's inbox
//! (web 0.73.0): its "addressed to you" rule is narrower than the bridge's, and
//! a message it leaves out would never wake anything. Where the watcher can
//! only poll, each owned channel gets its own thread that reads with `?since=`
//! and then blocks on `/api/wait?card=&seq=`. Read first, wait second — the
//! reference's order, because a wait started first can miss what landed while
//! it was starting.
//!
//! A channel is owned when it is configured, or when it is claimed for this
//! agent (`claimed_by`) — so the operator hands Outrider a new conversation by
//! claiming it, in the UI or by asking, with no config change. A channel whose
//! claim goes elsewhere is dropped and its poller stops.
//!
//! **Many documents, one key.** A key may reach several documents (an
//! account-scoped key reaches all of them). Card ids are only unique inside a
//! document, so a channel is a `Chan {doc, card}`, every Trellis call names its
//! document, and claims and the change log are followed per document.

use crate::store::{Event, Store};
use crate::trellis::{in_doc, Client, Failed};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// A channel card, in its document.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Chan {
    pub doc: String,
    pub card: u64,
}

impl Chan {
    pub fn new(doc: &str, card: u64) -> Chan {
        Chan { doc: doc.to_string(), card }
    }

    /// The store's key, and the plugin's chat id outside the default document.
    pub fn key(&self) -> String {
        format!("{}:{}", self.doc, self.card)
    }
}

impl std::fmt::Display for Chan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let short: String = self.doc.chars().take(8).collect();
        write!(f, "#{} ({short}…)", self.card)
    }
}

pub struct Bridge {
    pub store: Mutex<Store>,
    /// Signalled whenever events are added, so `GET /api/events?wait=` wakes.
    pub woke: Condvar,
    pub client: Option<Client>,
    pub agent: String,
    /// The names a message from the account holder carries. Desktop writes
    /// `operator`; trellis-web writes the account's name (the email's local
    /// part), so the real one is read from `/api/me` at startup.
    pub operators: Vec<String>,
    /// The e2e suites' stand-in for the operator: a key bound to this name
    /// (trellis-web 0.59.0), trusted as the operator only on a message the
    /// server marks `agent_verified`. None in production. See `provenance`.
    pub e2e_operator: Option<String>,
    /// The default document: where a request that names none goes.
    pub document: String,
    /// Documents followed. Fixed from the config, or refreshed from
    /// `GET /api/agent` (what the key may reach) when the config names none.
    pub documents: Mutex<Vec<String>>,
    pub discover: bool,
    /// Configured channels (D5), claimed at startup. The first is the home
    /// channel, where @mentions and assignments elsewhere are raised.
    pub channels: Vec<Chan>,
    /// Channels claimed for this agent in Trellis, refreshed continuously.
    pub claimed: Mutex<BTreeSet<Chan>>,
    /// Group channels it is not a participant in, where someone addressed it
    /// by name (see `watch::called_by_name`). Answered like a joined group
    /// channel: only what is addressed to it. Kept in the store (`called`).
    pub called: Mutex<BTreeSet<Chan>>,
    /// Channels with a poller running.
    pub running: Mutex<BTreeSet<Chan>>,
    /// Documents with a change watcher running.
    pub watching: Mutex<BTreeSet<String>>,
    /// Channels read at least once by a sweep (see `sweep_new`).
    pub swept: Mutex<BTreeSet<Chan>>,
    /// Held while a channel is read, so a sweep, the watcher and a poller never
    /// read the same channel against the same cursor at once.
    pub reading: Mutex<()>,
    /// The server's own built-in agents acting in the followed documents
    /// (trellis-web 0.51.0), by name: `{name, reach, document, node, card}`.
    /// Read at startup, every 5 min, and when an unknown agent name appears.
    pub builtins: Mutex<std::collections::BTreeMap<String, Value>>,
    pub builtins_read: Mutex<Option<Instant>>,
    /// Per channel: agent messages admitted since the operator last spoke, and
    /// when — the loop guard (see `admit`).
    pub talk: Mutex<std::collections::HashMap<String, Talk>>,
    /// Documents the key owner OWNS (`access: owner` in `GET /api/agent`).
    /// Until the server marks each message `from_key_owner`, a person's
    /// message counts as the operator's only here: names are not unique
    /// across accounts, and a shared document could hold a namesake.
    pub owned_docs: Mutex<BTreeSet<String>>,
    pub status: Mutex<Status>,
    /// How work reaches the bridge (DESIGN D14): `MODE_UNKNOWN` at start,
    /// `MODE_STREAM` while the agent event stream carries it, `MODE_POLL`
    /// when the server has no stream (or it keeps failing) and the change-log
    /// watchers and channel pollers do.
    pub mode: AtomicU8,
    /// Set by a stream event (`claim`, `participants`, `agents`, `access`,
    /// `reset`): re-read claims, built-ins and documents now, not on a timer.
    pub refresh: AtomicBool,
}

pub const MODE_UNKNOWN: u8 = 0;
pub const MODE_STREAM: u8 = 1;
pub const MODE_POLL: u8 = 2;

/// At most this many messages from agents (built-in or peer) in a row reach
/// the agent in one channel before the operator speaks again. The server's
/// quiet spell stops addressing agents after 8 agent messages in a row in the
/// room, which ends a two-agent exchange first; this is the backstop for a
/// server without one. At 4 it cut real work short (#21 2248, MindSwarmAgent's
/// answer to Orbit, dropped mid-review).
pub const MAX_AGENT_RUN: u32 = 8;
/// And at most this many from agents per channel per hour.
pub const MAX_AGENT_PER_HOUR: usize = 30;

#[derive(Debug, Default)]
pub struct Talk {
    pub run: u32,
    pub recent: std::collections::VecDeque<Instant>,
}

#[derive(Debug, Default, Clone)]
pub struct Status {
    pub trellis_ok: bool,
    pub last_error: Option<String>,
    /// Channels refused because another agent holds the claim. Not polled,
    /// and `say` refuses them.
    pub refused: Vec<(Chan, String)>,
    /// How each document's change watcher is woken: `wait` or `poll`.
    pub watch: std::collections::BTreeMap<String, String>,
    /// The key as Trellis last described it (`GET /api/whoami`): label,
    /// binding, expiry, or the refusal. Re-read with the built-in agents, so
    /// health answers "is the credential still good" without a call.
    pub key: Option<Value>,
    pub key_read: Option<Instant>,
    /// The Hermes plugin's version, as it reported it on connecting.
    pub plugin: Option<String>,
}

impl Bridge {
    pub fn mode(&self) -> u8 {
        self.mode.load(Ordering::SeqCst)
    }

    pub fn set_mode(&self, m: u8) {
        self.mode.store(m, Ordering::SeqCst);
    }

    pub fn owns(&self, c: &Chan) -> bool {
        let listed = self.channels.contains(c)
            || self.claimed.lock().map(|s| s.contains(c)).unwrap_or(false)
            || self.called.lock().map(|s| s.contains(c)).unwrap_or(false);
        listed && !self.status().refused.iter().any(|(r, _)| r == c)
    }

    /// Every channel currently owned.
    pub fn owned(&self) -> Vec<Chan> {
        let mut all: BTreeSet<Chan> = self.channels.iter().cloned().collect();
        if let Ok(c) = self.claimed.lock() {
            all.extend(c.iter().cloned());
        }
        if let Ok(c) = self.called.lock() {
            all.extend(c.iter().cloned());
        }
        all.into_iter().filter(|c| self.owns(c)).collect()
    }

    /// Where @mentions and assignments are raised: the first configured
    /// channel, else the first owned one in that document, else any.
    pub fn home_for(&self, doc: &str) -> Option<Chan> {
        if let Some(c) = self.channels.iter().find(|c| self.owns(c)) {
            return Some(c.clone());
        }
        let owned = self.owned();
        owned.iter().find(|c| c.doc == doc).or_else(|| owned.first()).cloned()
    }

    pub fn docs(&self) -> Vec<String> {
        self.documents.lock().map(|d| d.clone()).unwrap_or_default()
    }

    pub fn status(&self) -> Status {
        self.status.lock().map(|s| s.clone()).unwrap_or_default()
    }

    pub fn set_ok(&self) {
        if let Ok(mut s) = self.status.lock() {
            s.trellis_ok = true;
            s.last_error = None;
        }
    }

    pub fn set_err(&self, e: &str) {
        eprintln!("trellisbridge: {e}");
        if let Ok(mut s) = self.status.lock() {
            s.trellis_ok = false;
            s.last_error = Some(e.to_string());
        }
    }

    pub fn client(&self) -> Result<&Client, Failed> {
        self.client.as_ref().ok_or(Failed { status: None, message: "no Trellis client".into() })
    }

    /// Which documents the key reaches, from the server's own answer.
    pub fn discover_documents(&self) -> Result<Vec<String>, Failed> {
        // A desktop serves one document, the operator's own file, and has no
        // /api/agent.
        if self.client()?.is_desktop() {
            if let Ok(mut o) = self.owned_docs.lock() {
                *o = [self.document.clone()].into_iter().collect();
            }
            return Ok(vec![self.document.clone()]);
        }
        let v = self.client()?.get("/api/agent")?;
        let owned: BTreeSet<String> = v["documents"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|d| d["access"] == "owner")
            .filter_map(|d| d["id"].as_str().map(str::to_string))
            .collect();
        if let Ok(mut o) = self.owned_docs.lock() {
            *o = owned;
        }
        let mut docs: Vec<String> = v["documents"]
            .as_array()
            .map(|a| a.iter().filter_map(|d| d["id"].as_str().map(str::to_string)).collect())
            .unwrap_or_default();
        if docs.is_empty() {
            if let Some(d) = v["you"]["scope"]["document"].as_str() {
                docs.push(d.to_string());
            }
        }
        if !docs.contains(&self.document) {
            docs.insert(0, self.document.clone());
        }
        Ok(docs)
    }

    /// The loop guard. The operator always gets through and resets the run.
    /// Anyone else is admitted only while the run of agent messages since the
    /// operator last spoke, and the hour's count, stay under the limits — so
    /// two agents cannot talk each other round in circles. Unverified
    /// messages are never admitted.
    pub fn admit(&self, key: &str, e: &Event) -> bool {
        let Ok(mut talk) = self.talk.lock() else { return false };
        let t = talk.entry(key.to_string()).or_default();
        if e.provenance == "operator" {
            t.run = 0;
            return true;
        }
        if !(e.trusted || e.peer) {
            return false;
        }
        let hour_ago = Instant::now().checked_sub(Duration::from_secs(3600));
        while let (Some(front), Some(cut)) = (t.recent.front(), hour_ago) {
            if *front < cut {
                t.recent.pop_front();
            } else {
                break;
            }
        }
        if t.run >= MAX_AGENT_RUN || t.recent.len() >= MAX_AGENT_PER_HOUR {
            eprintln!(
                "trellisbridge: loop guard: not passing {} from {} on {key} ({} in a row, {} this hour) — waiting for the operator",
                e.provenance, e.from, t.run, t.recent.len()
            );
            return false;
        }
        t.run += 1;
        t.recent.push_back(Instant::now());
        true
    }

    /// Re-read `GET /api/agents` in every followed document. At most once a
    /// minute, whoever asks.
    pub fn refresh_builtins(&self) {
        let Some(client) = self.client.as_ref() else { return };
        if let Ok(mut last) = self.builtins_read.lock() {
            if last.map(|t| t.elapsed() < Duration::from_secs(60)).unwrap_or(false) {
                return;
            }
            *last = Some(Instant::now());
        }
        let mut found = std::collections::BTreeMap::new();
        for doc in self.docs() {
            match client.get(&in_doc("/api/agents", &doc)) {
                Ok(v) => {
                    for a in v["agents"].as_array().into_iter().flatten() {
                        if let Some(name) = a["name"].as_str() {
                            found.insert(name.to_string(), builtin_row(a));
                        }
                    }
                }
                // An older server has no such route: nothing is built in.
                Err(Failed { status: Some(404), .. }) => {}
                Err(e) => self.set_err(&e.message),
            }
        }
        if let Ok(mut b) = self.builtins.lock() {
            for n in found.keys().filter(|n| !b.contains_key(*n)) {
                println!("agent     built-in agent {n} acts in a followed document");
            }
            *b = found;
        }
    }

    /// Re-read what Trellis says about this key, at most every 10 minutes.
    /// A 401 or 403 is kept as `ok: false` with Trellis's words.
    pub fn refresh_key(&self) {
        let Some(client) = self.client.as_ref() else { return };
        let due = self.status().key_read.map(|t| t.elapsed() >= Duration::from_secs(600)).unwrap_or(true);
        if !due {
            return;
        }
        let key = match client.get("/api/whoami") {
            Ok(v) => Some(key_row(&v)),
            Err(Failed { status: Some(s @ (401 | 403)), message }) => {
                Some(json!({ "ok": false, "status": s, "error": message }))
            }
            Err(_) => None, // unreachable says nothing about the key: keep the last answer
        };
        if let Ok(mut st) = self.status.lock() {
            st.key_read = Some(Instant::now());
            if key.is_some() {
                st.key = key;
            }
        }
    }

    pub fn set_plugin(&self, version: &str) {
        if let Ok(mut st) = self.status.lock() {
            st.plugin = Some(version.chars().take(40).collect());
        }
    }

    pub fn builtin(&self, name: &str) -> Option<Value> {
        self.builtins.lock().ok().and_then(|b| b.get(name).cloned())
    }

    /// Claim every configured channel under our `X-Agent` name, so workspace
    /// agents stand aside there. A 409 means another agent holds it: that
    /// channel is left alone rather than contested — handing one over is the
    /// operator's call.
    ///
    /// Nothing here stops the bridge: a configured card that is no longer a
    /// channel (the operator turned it back into a card), or one that is gone,
    /// is set aside with the reason and shows in /api/health; the rest carry
    /// on. Only "Trellis did not answer" is retried, by the pollers.
    pub fn claim_all(&self) -> Result<(), String> {
        let client = self.client.as_ref().ok_or("no Trellis client")?;
        // The desktop has no claims (no `…/channel/claim` route): a configured
        // channel is answered as configured, and a 404 there is not "gone".
        if client.is_desktop() {
            for c in &self.channels {
                println!("channel   {c}: the desktop has no claims — answering there as configured");
            }
            return Ok(());
        }
        for c in &self.channels {
            let body = json!({ "agent": self.agent });
            match client.post(&in_doc(&format!("/api/cards/{}/channel/claim", c.card), &c.doc), &body) {
                Ok(_) => println!("claimed   channel {c} as {}", self.agent),
                Err(Failed { status: Some(409), message }) => {
                    let holder = message.rsplit(": ").next().unwrap_or("another agent").to_string();
                    eprintln!("trellisbridge: channel {c} is claimed by someone else — not answering there ({message})");
                    self.set_aside(c, &holder);
                }
                Err(Failed { status: Some(code @ (400 | 403 | 404)), message }) => {
                    eprintln!("trellisbridge: channel {c} set aside ({code}: {message})");
                    self.set_aside(c, &format!("{code}: {message}"));
                }
                Err(e) => eprintln!("trellisbridge: claiming {c}: {} — will keep trying through the poller", e.message),
            }
        }
        Ok(())
    }

    /// Stop treating a channel as ours, recording why.
    pub fn set_aside(&self, c: &Chan, why: &str) {
        // A channel joined by being called is not coming back as ours: forget it.
        let left = self.called.lock().ok().and_then(|mut called| {
            called.remove(c).then(|| called.iter().map(|c| serde_json::json!([c.doc, c.card])).collect::<Vec<_>>())
        });
        if let (Some(all), Ok(mut s)) = (left, self.store.lock()) {
            let _ = s.set_meta("called", serde_json::json!(all));
            return;
        }
        if let Ok(mut s) = self.status.lock() {
            if !s.refused.iter().any(|(r, _)| r == c) {
                s.refused.push((c.clone(), why.to_string()));
            }
        }
    }

    /// Start a poller per owned channel, the claim follower, and a change
    /// watcher per document.
    pub fn start(self: &Arc<Self>) {
        for d in self.docs() {
            self.spawn_watcher(d);
        }
        let me = Arc::clone(self);
        std::thread::spawn(move || me.follow_claims());
        let me = Arc::clone(self);
        std::thread::spawn(move || me.stream_supervisor());
    }

    fn spawn_poller(self: &Arc<Self>, c: Chan) {
        let fresh = self.running.lock().map(|mut r| r.insert(c.clone())).unwrap_or(false);
        if fresh {
            let me = Arc::clone(self);
            std::thread::spawn(move || {
                me.poll(&c);
                if let Ok(mut r) = me.running.lock() {
                    r.remove(&c);
                }
            });
        }
    }

    fn spawn_watcher(self: &Arc<Self>, doc: String) {
        let fresh = self.watching.lock().map(|mut w| w.insert(doc.clone())).unwrap_or(false);
        if fresh {
            let me = Arc::clone(self);
            std::thread::spawn(move || me.watch(&doc));
        }
    }

    /// Every 30 s: which channels are claimed for this agent, in every
    /// followed document. New ones get a poller; one claimed away is dropped,
    /// and its poller stops. Every 5 min, when discovering, the document list
    /// is refreshed — a document shared with the key later is picked up.
    fn follow_claims(self: &Arc<Self>) {
        let Some(client) = self.client.as_ref() else { return };
        let mut tick = 0u32;
        loop {
            // While the stream carries the work, claims, built-ins and
            // documents are re-read when it says they changed; the timer
            // stays only as a 10-minute safety net.
            let forced = self.refresh.swap(false, Ordering::SeqCst);
            let streaming = self.mode() == MODE_STREAM;
            if streaming && !forced && !tick.is_multiple_of(20) {
                tick = tick.wrapping_add(1);
                for c in self.owned() {
                    self.sweep_new(&c);
                }
                self.nap(30);
                continue;
            }
            if tick.is_multiple_of(10) || forced {
                self.refresh_builtins();
                self.refresh_key();
            }
            if self.discover && ((tick.is_multiple_of(10) && tick > 0) || forced) {
                if let Ok(docs) = self.discover_documents() {
                    for d in docs.iter().filter(|d| !self.docs().contains(d)) {
                        println!("document  {d} is now reachable — following it");
                    }
                    if let Ok(mut cur) = self.documents.lock() {
                        *cur = docs;
                    }
                    for d in self.docs() {
                        self.spawn_watcher(d);
                    }
                }
            }
            tick = tick.wrapping_add(1);
            let mut now = BTreeSet::new();
            let mut ok = true;
            for doc in self.docs() {
                match client.get(&in_doc("/api/channels", &doc)) {
                    Ok(v) => {
                        for row in v["channels"].as_array().into_iter().flatten() {
                            if joins(row, &self.agent) {
                                if let Some(card) = row["card"].as_u64() {
                                    now.insert(Chan::new(&doc, card));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        ok = false;
                        self.set_err(&e.message);
                    }
                }
            }
            if ok {
                // A channel set aside earlier (turned into a card, gone) that
                // is claimed for us again is ours again: set-aside is not
                // forever, it lasts until Trellis says otherwise.
                if let Ok(mut st) = self.status.lock() {
                    let back: Vec<Chan> = st.refused.iter().filter(|(c, _)| now.contains(c)).map(|(c, _)| c.clone()).collect();
                    for c in &back {
                        println!("channel   {c} is a claimed channel again — answering there");
                    }
                    st.refused.retain(|(c, _)| !now.contains(c));
                }
                let before = self.claimed.lock().map(|c| c.clone()).unwrap_or_default();
                for c in now.difference(&before) {
                    println!("channel   {c} claimed for {} — answering there", self.agent);
                }
                for c in before.difference(&now) {
                    if !self.channels.contains(c) {
                        println!("channel   {c} no longer claimed for {} — stopping", self.agent);
                    }
                }
                if let Ok(mut c) = self.claimed.lock() {
                    *c = now;
                }
            }
            // Where the watcher holds a document wait, it carries the channels;
            // elsewhere (or before it has said which) each gets a poller.
            // Every 5 min (30 while streaming), read every owned channel again.
            if tick.is_multiple_of(if streaming { 60 } else { 10 }) {
                if let Ok(mut s) = self.swept.lock() {
                    s.clear();
                }
            }
            for c in self.owned() {
                if streaming || self.watch_mode(&c.doc) == "wait" {
                    self.sweep_new(&c);
                } else {
                    self.spawn_poller(c);
                }
            }
            self.nap(30);
        }
    }

    /// Sleep up to `secs`, waking early when a stream event asks for a refresh.
    fn nap(&self, secs: u64) {
        for _ in 0..secs {
            if self.refresh.load(Ordering::SeqCst) {
                return;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    fn poll(&self, c: &Chan) {
        let Some(client) = self.client.as_ref() else { return };
        let mut backoff = Duration::from_secs(1);
        loop {
            if !self.owns(c) {
                return;
            }
            if self.mode() == MODE_STREAM {
                println!("channel   {c}: the event stream carries it now — poller stopping");
                self.sweep(c);
                return;
            }
            if self.watch_mode(&c.doc) == "wait" {
                println!("channel   {c}: the document wait carries it now — poller stopping");
                self.sweep(c);
                return;
            }
            let read = {
                let _one = self.reading.lock();
                self.catch_up(client, c)
            };
            match read {
                Ok(seq) => {
                    self.set_ok();
                    backoff = Duration::from_secs(1);
                    // Wait until the channel moves past what we hold. A
                    // `changed:false` is the normal ~25s answer: ask again.
                    loop {
                        match client.get(&in_doc(&format!("/api/wait?card={}&seq={seq}", c.card), &c.doc)) {
                            Ok(v) if v["changed"].as_bool() == Some(true) => break,
                            Ok(_) if !self.owns(c) => return,
                            Ok(_) if self.watch_mode(&c.doc) == "wait" => break,
                            Ok(_) => continue,
                            Err(e) => {
                                self.set_err(&e.message);
                                std::thread::sleep(backoff);
                                break;
                            }
                        }
                    }
                }
                Err(Failed { status: Some(code @ (400 | 403 | 404)), message }) => {
                    // Not a channel any more, gone, or out of scope: answering
                    // there is over. Retrying would only fill an error log.
                    eprintln!("trellisbridge: channel {c} set aside ({code}: {message}) — stopping its poller");
                    self.set_aside(c, &format!("{code}: {message}"));
                    return;
                }
                Err(e) => {
                    // Trellis down is not the bridge down (D8): keep what we
                    // have, report it on /api/health, and back off to a minute.
                    self.set_err(&e.message);
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                }
            }
        }
    }

    /// Read one owned channel up to date, setting it aside if it is not a
    /// channel any more (the poller's rule).
    pub fn sweep(&self, c: &Chan) {
        let Some(client) = self.client.as_ref() else { return };
        let read = {
            let _one = self.reading.lock();
            self.catch_up(client, c)
        };
        match read {
            Ok(_) => {
                self.set_ok();
                if let Ok(mut s) = self.swept.lock() {
                    s.insert(c.clone());
                }
            }
            Err(Failed { status: Some(code @ (400 | 403 | 404)), message }) => {
                eprintln!("trellisbridge: channel {c} set aside ({code}: {message})");
                self.set_aside(c, &format!("{code}: {message}"));
            }
            Err(e) => self.set_err(&e.message),
        }
    }

    /// Sweep a channel the bridge has not read since the last full sweep: one
    /// newly claimed, or all of them every 5 minutes.
    fn sweep_new(&self, c: &Chan) {
        if !self.swept.lock().map(|s| s.contains(c)).unwrap_or(false) {
            self.sweep(c);
        }
    }

    /// Read everything after our cursor and store it. Returns the channel's
    /// `seq` to wait on.
    fn catch_up(&self, client: &Client, c: &Chan) -> Result<u64, Failed> {
        let internal = |message: String| Failed { status: None, message };
        let key = c.key();
        let (known, cursor) = self.store.lock().map(|s| (s.knows(&key), s.cursor(&key))).unwrap_or((true, 0));
        if !known {
            // First time this bridge sees the channel: start at its present.
            // Its history was written before this agent was listening, and
            // answering it would reply to conversations long finished.
            let ch = client.get(&in_doc(&format!("/api/cards/{}/channel?since=0", c.card), &c.doc))?;
            let seq = ch["seq"].as_u64().unwrap_or(0);
            // In a group channel a message addressed to us may be waiting from
            // before we joined (someone asked @Outrider, then added it). Start
            // after our own last word there instead, so those are answered;
            // only messages addressed to us become events.
            let start = if ch["group"].as_bool() == Some(true) {
                ch["messages"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|m| m["from"].as_str().map(|f| f.eq_ignore_ascii_case(&self.agent)).unwrap_or(false))
                    .filter_map(|m| m["seq"].as_u64())
                    .max()
                    .unwrap_or(0)
            } else {
                seq
            };
            self.store.lock().map_err(|_| internal("store lock".into()))?.reset_cursor(&key, start).map_err(internal)?;
            if start < seq {
                println!("channel   {c} first seen (group): picking up what is addressed to {} after seq {start}", self.agent);
                return self.catch_up(client, c);
            }
            println!("channel   {c} first seen at seq {seq}; earlier messages are history, not events");
            return Ok(seq);
        }
        let ch = client.get(&in_doc(&format!("/api/cards/{}/channel?since={cursor}", c.card), &c.doc))?;
        let seq = ch["seq"].as_u64().unwrap_or(0);
        if seq < cursor {
            // Re-made channel: numbering went backwards. Start from here;
            // resending old messages would re-answer a conversation.
            eprintln!("trellisbridge: channel {c} seq went {cursor} → {seq}; resyncing");
            self.store.lock().map_err(|_| internal("store lock".into()))?.reset_cursor(&key, seq).map_err(internal)?;
            return Ok(seq);
        }
        let owned = self.owned_docs.lock().map(|o| o.contains(&c.doc)).unwrap_or(false);
        // Any word from the operator in the channel — addressed to us or not
        // — ends a run of agent talk, as on the server ("until a person
        // speaks").
        let operator_spoke = ch["messages"].as_array().into_iter().flatten().any(|m| {
            m["seq"].as_u64().unwrap_or(0) > cursor
                && provenance(m, &self.operators, self.e2e_operator.as_deref(), owned) == "operator"
        });
        if operator_spoke {
            if let Ok(mut talk) = self.talk.lock() {
                talk.entry(key.clone()).or_default().run = 0;
            }
        }
        let mut new = events_from(&ch, c, cursor, &self.agent, &self.operators, self.e2e_operator.as_deref(), owned);
        for e in new.iter_mut() {
            if e.provenance == "agent" || e.provenance == "builtin" {
                if self.builtin(&e.from).is_none() {
                    self.refresh_builtins();
                }
                e.builtin = self.builtin(&e.from);
            }
            e.trusted = trusted(&e.provenance, e.builtin.as_ref(), &c.doc, e.from_key_owner, owned);
            e.peer = !e.trusted && matches!(e.provenance.as_str(), "agent" | "person" | "builtin");
        }
        let new: Vec<Event> = new.into_iter().filter(|e| self.admit(&key, e)).collect();
        if seq > cursor || !new.is_empty() {
            let mut store = self.store.lock().map_err(|_| internal("store lock".into()))?;
            let added = store.advance(&key, seq, new).map_err(internal)?;
            drop(store);
            if !added.is_empty() {
                self.woke.notify_all();
            }
        }
        Ok(seq)
    }

    /// Post to an owned channel as this agent, with any files.
    ///
    /// Native first: `say {text, files}` (trellis-web 0.49.0, desktop
    /// 0.199.3), where the files belong to the message itself. A server
    /// without it refuses the unknown field with a 400 — then each file is
    /// attached to the channel card and the message names it, which is the
    /// same bytes in the same card, only linked by text.
    ///
    /// `reply_to` threads the message under the one it answers. It is only a
    /// display hint: a server without threading, or one that refuses that seq,
    /// gets the message unthreaded rather than not at all.
    pub fn say(&self, c: &Chan, text: &str, files: &[(String, String)], reply_to: Option<u64>) -> Result<Value, Failed> {
        let client = self.client()?;
        let path = in_doc(&format!("/api/cards/{}/say", c.card), &c.doc);
        if files.is_empty() {
            return post_say(client, &path, json!({ "text": text }), reply_to);
        }
        let native: Vec<Value> = files.iter().map(|(n, d)| json!({ "name": n, "data_base64": d })).collect();
        match post_say(client, &path, json!({ "text": text, "files": native }), reply_to) {
            Ok(mut r) => {
                if r.get("files").is_none() {
                    r["files"] = json!(files.iter().map(|(n, _)| json!({ "name": n })).collect::<Vec<_>>());
                }
                r["native"] = json!(true);
                Ok(r)
            }
            Err(Failed { status: Some(400), message }) if message.contains("files") => self.say_attached(client, c, text, files, reply_to),
            Err(e) => Err(e),
        }
    }

    fn say_attached(&self, client: &Client, c: &Chan, text: &str, files: &[(String, String)], reply_to: Option<u64>) -> Result<Value, Failed> {
        let mut text = text.trim_end().to_string();
        let mut attached = Vec::new();
        for (name, b64) in files {
            let r = client.post(
                &in_doc(&format!("/api/cards/{}/attachments", c.card), &c.doc),
                &json!({ "name": name, "data_base64": b64 }),
            )?;
            let index = r["index"].as_u64().unwrap_or(0);
            let bytes = r["bytes"].as_u64().unwrap_or(0);
            text.push_str(&format!("\n\n📎 **{name}** ({}) — attachment #{index} on this card", human(bytes)));
            attached.push(json!({ "index": index, "name": name, "bytes": bytes }));
        }
        let mut r = post_say(client, &in_doc(&format!("/api/cards/{}/say", c.card), &c.doc), json!({ "text": text.trim_start() }), reply_to)?;
        r["files"] = json!(attached);
        r["native"] = json!(false);
        Ok(r)
    }

    /// Events after `after`, waiting up to `wait` for one to arrive.
    pub fn events(&self, after: u64, wait: Duration) -> Vec<Event> {
        let deadline = Instant::now() + wait;
        let Ok(mut store) = self.store.lock() else { return Vec::new() };
        loop {
            let found = store.after(after);
            let now = Instant::now();
            if !found.is_empty() || now >= deadline {
                return found;
            }
            match self.woke.wait_timeout(store, deadline - now) {
                Ok((s, _)) => store = s,
                Err(_) => return Vec::new(),
            }
        }
    }
}

/// `say`, threaded under `reply_to` when given. A 400 that names `reply_to`
/// (a server without threading, or a seq it will not thread under) is retried
/// once without it: the message matters, the thread is presentation.
fn post_say(client: &Client, path: &str, mut body: Value, reply_to: Option<u64>) -> Result<Value, Failed> {
    let Some(seq) = reply_to else { return client.post(path, &body) };
    body["reply_to"] = json!(seq);
    match client.post(path, &body) {
        Err(Failed { status: Some(400), message }) if message.contains("reply_to") => {
            eprintln!("trellisbridge: say on {path}: not threaded under #{seq} ({message})");
            if let Some(o) = body.as_object_mut() {
                o.remove("reply_to");
            }
            client.post(path, &body)
        }
        r => r,
    }
}

/// Whether a channel is this agent's to be in: claimed for it, or naming it
/// as a participant (trellis-web 0.56.0 group channels) — unless another
/// agent holds the claim on a one-to-one channel, which is that agent's.
/// Names compare ignoring case, as the server's do.
pub fn joins(row: &Value, agent: &str) -> bool {
    let me = |v: &Value| v.as_str().map(|s| s.eq_ignore_ascii_case(agent)).unwrap_or(false);
    if me(&row["claimed_by"]) {
        return true;
    }
    let listed = row["participants"].as_array().map(|p| p.iter().any(me)).unwrap_or(false);
    let group = row["group"].as_bool().unwrap_or(false);
    let claimed_by_other = row["claimed_by"].as_str().is_some();
    listed && (group || !claimed_by_other)
}

/// In a group channel the server decides who each message is for (`to`);
/// only those answer. Absent `to` (an older server), an @mention decides.
fn addressed_to_me(ch: &Value, m: &Value, agent: &str) -> bool {
    if ch["group"].as_bool() != Some(true) {
        return true;
    }
    match m["to"].as_array() {
        Some(to) => to.iter().any(|n| n.as_str().map(|s| s.eq_ignore_ascii_case(agent)).unwrap_or(false)),
        None => !crate::watch::mention_lines(m["text"].as_str().unwrap_or(""), agent).is_empty(),
    }
}

/// A channel message's provenance: `provenance_of` on its fields, plus the
/// e2e suites' operator stand-in. That is a key bound to `e2e_operator`, on the
/// owner's account, whose message the server marks `agent_verified` — a name
/// no other key can send. Only when the config names it.
pub fn provenance(m: &Value, operators: &[String], e2e_operator: Option<&str>, owned: bool) -> &'static str {
    let from = m["from"].as_str().unwrap_or("");
    if let Some(name) = e2e_operator {
        if m["kind"].as_str() == Some("agent")
            && from == name
            && m["agent_verified"].as_bool() == Some(true)
            && m["from_key_owner"].as_bool() == Some(true)
        {
            return "operator";
        }
    }
    provenance_of(m["kind"].as_str(), from, operators, m["from_key_owner"].as_bool(), m["via"].as_str(), owned)
}

/// How a message was posted. trellis-web 0.52.0 records `kind` when it
/// numbers a message — `person`, `builtin` or `agent` — so that is used when
/// present. A `person` is the operator only when it is the key owner's name;
/// in a shared document another person is someone else, not an agent and not
/// the operator. With no `kind` (an older server, an older message, text typed
/// straight into the card) the name decides, as before: a label, not an
/// authentication, so the agent treats all of it as data (D7).
///
/// Who the operator is: when the server says so (`from_key_owner`, the same
/// account as the calling key), that decides. Without it, the name decides —
/// and only in a document the key owner owns, since names are not unique
/// across accounts.
pub fn provenance_of(kind: Option<&str>, from: &str, operators: &[String], from_key_owner: Option<bool>, via: Option<&str>, owned: bool) -> &'static str {
    let is_operator = match from_key_owner {
        Some(v) => v,
        None => owned && operators.iter().any(|o| o == from),
    };
    // `from_key_owner` is true for EVERY key on the account, and an unbound
    // key that sends no `X-Agent` is recorded `kind: person` (LANAgent's probe,
    // #2951 seq 66). Only the operator's own authenticated surfaces count, as
    // the server sets `via` from the credential: a signed-in browser
    // (`session`, trellis-web 0.59.3) or the Telegram chat the operator linked
    // from a signed-in browser (`telegram`, 0.65.0; operator's call), or the
    // operator's phone app on a device key minted from a signed-in session
    // (`app`; such a key may not carry `X-Agent`, 2754 #403). Never `api`.
    // Absent on older messages, where the rule above stands.
    let is_operator = is_operator && matches!(via, None | Some("session") | Some("telegram") | Some("app"));
    // The operator needs BOTH a server-recorded `kind: person` AND the owner
    // check: agent keys belong to the owner's account too (they post as
    // `agent`), and a message with no `kind` is text written into the card
    // body — its header, name included, is whatever the writer typed (#43).
    // Since trellis-web 0.52.0 every real post carries `kind`, so no `kind`
    // is "unverified": shown with its name, never trusted.
    match kind {
        Some("person") if is_operator => "operator",
        Some("person") => "person",
        Some("builtin") => "builtin",
        Some("agent") => "agent",
        _ if from == "webhook" => "webhook",
        _ => "unverified",
    }
}

/// The trust rule, in one place. The operator; or a built-in agent that the
/// server recorded as `builtin` AND that `/api/agents` lists as acting in this
/// document. A name alone is never enough: an API key posting as a built-in
/// agent's name is recorded `agent`, and a message with no `kind` proves
/// nothing either way.
///
/// `builtin` alone is not "the operator's" built-in agent: in a shared
/// document a collaborator's built-in agent is `builtin` too (#55). The
/// server's `from_key_owner` settles it — the operator's own built-in agents
/// act as the operator's account, so they read `true`. Until the server sends
/// it, a built-in agent counts only in a document the operator owns.
pub fn trusted(provenance: &str, builtin: Option<&Value>, doc: &str, from_key_owner: Option<bool>, owned: bool) -> bool {
    match provenance {
        "operator" => true,
        "builtin" => {
            let listed = builtin.is_some_and(|b| {
                // Home here, or an account/document reach that /api/agents
                // listed for this document (it lists only agents acting in it).
                b["document"].as_str() == Some(doc) || matches!(b["reach"].as_str(), Some("account") | Some("document"))
            });
            let owners = match from_key_owner {
                Some(v) => v,
                None => owned,
            };
            listed && owners
        }
        _ => false,
    }
}

/// The parts of a `GET /api/agents` row worth handing on: who, how far it
/// reaches, and where it lives.
pub fn builtin_row(a: &Value) -> Value {
    json!({ "name": a["name"], "reach": a["reach"], "document": a["document"],
            "document_name": a["document_name"], "node": a["node"], "card": a["card"] })
}

fn human(bytes: u64) -> String {
    match bytes {
        b if b >= 1 << 20 => format!("{:.1} MB", b as f64 / (1u64 << 20) as f64),
        b if b >= 1 << 10 => format!("{:.0} KB", b as f64 / 1024.0),
        b => format!("{b} B"),
    }
}

/// Turn a channel read into events: everything after `cursor` that someone
/// other than us wrote. Our own messages advance the cursor but are never
/// offered back to the agent that wrote them.
pub fn events_from(ch: &Value, c: &Chan, cursor: u64, agent: &str, operators: &[String], e2e_operator: Option<&str>, owned: bool) -> Vec<Event> {
    let node = ch["node"].as_u64().unwrap_or(0);
    let Some(msgs) = ch["messages"].as_array() else { return Vec::new() };
    msgs.iter()
        .filter_map(|m| {
            let seq = m["seq"].as_u64()?;
            let from = m["from"].as_str().unwrap_or("").to_string();
            if seq <= cursor || from == agent {
                return None;
            }
            let provenance = provenance(m, operators, e2e_operator, owned);
            // The server addresses a person's message that names nobody to the
            // channel's lead, but the e2e stand-in posts as an agent, whose
            // unaddressed message goes to nobody. Give it the person's rule.
            let stand_in = provenance == "operator" && e2e_operator == Some(from.as_str());
            let names_nobody = m["to"].as_array().is_none_or(|t| t.is_empty());
            let to_lead = stand_in && names_nobody && ch["lead"].as_str().is_some_and(|l| l.eq_ignore_ascii_case(agent));
            if !(addressed_to_me(ch, m, agent) || to_lead) {
                return None;
            }
            // Once the bridge has decided the stand-in speaks for the operator,
            // it is presented as the operator, as the watcher does. Under its
            // own name the agent rightly treats it as a claim (it held a
            // Telegram send, seq 301).
            let from = if stand_in { operators.get(1).or(operators.first()).cloned().unwrap_or(from) } else { from };
            let naming = naming(ch, m, agent);
            Some(Event {
                id: 0,
                document: c.doc.clone(),
                card: c.card,
                node,
                seq,
                provenance: provenance.to_string(),
                from,
                at: m["at"].as_str().unwrap_or("").to_string(),
                text: m["text"].as_str().unwrap_or("").to_string(),
                files: m["files"].as_array().cloned().unwrap_or_default(),
                about: None,
                builtin: None,
                trusted: false,
                peer: false,
                from_key_owner: m["from_key_owner"].as_bool(),
                lead_only: naming == Naming::Nobody,
                broadcast: naming == Naming::Group,
                expect: m.get("expect").filter(|e| e.is_object()).cloned(),
                origin: m["origin"].as_str().map(str::to_string),
            })
        })
        .collect()
}

/// How a channel message names this agent.
#[derive(Debug, PartialEq)]
enum Naming {
    /// By name: always in a one-to-one channel; in a group, an explicit list
    /// (`to_source: "list"`) that holds it, or an @mention of it.
    Me,
    /// Only by a group word (@all, @agents, @everyone).
    Group,
    /// Not at all: it reached us as the channel's lead.
    Nobody,
}

fn naming(ch: &Value, m: &Value, agent: &str) -> Naming {
    if ch["group"].as_bool() != Some(true) {
        return Naming::Me;
    }
    let text = m["text"].as_str().unwrap_or("");
    let listed = m["to_source"].as_str() == Some("list")
        && m["to"].as_array().is_some_and(|t| t.iter().any(|n| n.as_str().is_some_and(|s| s.eq_ignore_ascii_case(agent))));
    let mentioned = |n: &str| !crate::watch::mention_lines(text, n).is_empty();
    if listed || mentioned(agent) {
        Naming::Me
    } else if ["all", "agents", "everyone"].iter().any(|n| mentioned(n)) {
        Naming::Group
    } else {
        Naming::Nobody
    }
}

/// The parts of `GET /api/whoami` that say whether the key still works. A
/// desktop answers without a `key` object: then only `ok`.
fn key_row(who: &Value) -> Value {
    let k = &who["key"];
    json!({
        "ok": true,
        "label": k["label"],
        "bound_agent": k["bound_agent"],
        "expires_at": k["expires_at"],
    })
}

#[cfg(test)]
pub fn for_test(channels: Vec<u64>) -> Bridge {
    Bridge {
        store: Mutex::new(Store::in_memory()),
        woke: Condvar::new(),
        client: None,
        agent: "Me".into(),
        operators: vec!["operator".into(), "alice".into()],
        e2e_operator: None,
        document: "D".into(),
        documents: Mutex::new(vec!["D".into()]),
        discover: false,
        channels: channels.into_iter().map(|c| Chan::new("D", c)).collect(),
        claimed: Mutex::new(BTreeSet::new()),
        called: Mutex::new(BTreeSet::new()),
        running: Mutex::new(BTreeSet::new()),
        watching: Mutex::new(BTreeSet::new()),
        swept: Mutex::new(BTreeSet::new()),
        reading: Mutex::new(()),
        builtins: Mutex::new(Default::default()),
        builtins_read: Mutex::new(None),
        owned_docs: Mutex::new(["D".to_string()].into_iter().collect()),
        talk: Mutex::new(Default::default()),
        status: Mutex::new(Status::default()),
        mode: AtomicU8::new(MODE_POLL),
        refresh: AtomicBool::new(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desktop_0_211_4_messages() {
        // Desktop: no from_key_owner; the owner's document, signed `operator`.
        let ops = vec!["operator".to_string()];
        assert_eq!(provenance_of(Some("person"), "operator", &ops, None, Some("session"), true), "operator", "typed in the app");
        assert_eq!(provenance_of(Some("person"), "operator", &ops, None, Some("api"), true), "person", "a keyed call with no X-Agent");
        assert_eq!(provenance_of(Some("agent"), "Nexus", &ops, None, Some("api"), true), "agent");
        assert_eq!(provenance_of(None, "operator", &ops, None, None, true), "unverified", "before 0.211.4");
    }

    fn ops() -> Vec<String> {
        vec!["operator".into(), "alice".into()]
    }

    fn d(card: u64) -> Chan {
        Chan::new("D", card)
    }

    #[test]
    fn the_web_account_name_is_an_operator_too() {
        let ch = json!({"node": 5, "seq": 1, "messages": [{"seq": 1, "from": "alice", "kind": "person", "text": "x"}]});
        assert_eq!(events_from(&ch, &d(9), 0, "Me", &ops(), None, true)[0].provenance, "operator");
    }

    fn channel() -> Value {
        json!({"card": 9, "node": 5, "seq": 4, "messages": [
            {"seq": 2, "from": "operator", "kind": "person", "at": "t2", "text": "old"},
            {"seq": 3, "from": "Me", "kind": "agent", "at": "t3", "text": "mine"},
            {"seq": 4, "from": "OtherAgent", "kind": "agent", "at": "t4", "text": "hi"}
        ]})
    }

    #[test]
    fn the_servers_origin_verdict_reaches_the_plugin() {
        let mut ch = channel();
        ch["messages"][2]["origin"] = json!("agent");
        let ev = events_from(&ch, &d(9), 2, "Me", &ops(), None, true);
        assert_eq!(ev[0].origin.as_deref(), Some("agent"));
        assert_eq!(serde_json::to_value(&ev[0]).unwrap()["origin"], "agent");
        let ev = events_from(&channel(), &d(9), 2, "Me", &ops(), None, true);
        assert!(serde_json::to_value(&ev[0]).unwrap().get("origin").is_none(), "absent when the server sends none (desktop)");
    }

    #[test]
    fn own_and_already_seen_messages_are_not_events() {
        let ev = events_from(&channel(), &d(9), 2, "Me", &ops(), None, true);
        assert_eq!(ev.len(), 1);
        assert_eq!((ev[0].seq, ev[0].from.as_str(), ev[0].provenance.as_str()), (4, "OtherAgent", "agent"));
        assert_eq!((ev[0].node, ev[0].document.as_str()), (5, "D"));
    }

    #[test]
    fn a_group_message_that_names_someone_else_is_lead_only_and_a_group_word_is_broadcast() {
        let msg = |seq: u64, text: &str, to: Value, src: &str| json!({"seq": seq, "from": "operator", "kind": "person", "text": text, "to": to, "to_source": src});
        let ch = json!({"node": 5, "seq": 6, "group": true, "lead": "Me", "messages": [
            msg(1, "Alice that's not a great result", json!(["Me"]), "mentions"),
            msg(2, "@Me what now", json!(["Me"]), "mentions"),
            msg(3, "@agents report", json!(["Me", "Alice"]), "mentions"),
            msg(4, "check this", json!(["Me"]), "list"),
            msg(5, "@alice and @mex", json!(["Me"]), "mentions"),
            msg(6, "@agents and @Me especially", json!(["Me", "Alice"]), "mentions"),
        ]});
        let ev = events_from(&ch, &d(9), 0, "Me", &ops(), None, true);
        let lead: Vec<(u64, bool, bool)> = ev.iter().map(|e| (e.seq, e.lead_only, e.broadcast)).collect();
        assert_eq!(lead, vec![(1, true, false), (2, false, false), (3, false, true), (4, false, false), (5, true, false), (6, false, false)]);
        let one_to_one = json!({"node": 5, "seq": 1, "messages": [{"seq": 1, "from": "operator", "kind": "person", "text": "hmm"}]});
        assert!(!events_from(&one_to_one, &d(9), 0, "Me", &ops(), None, true)[0].lead_only, "a one-to-one message always names us");
    }

    #[test]
    fn the_reply_shape_a_message_asks_for_reaches_the_agent() {
        let ch = json!({"node": 5, "seq": 2, "messages": [
            {"seq": 1, "from": "operator", "kind": "person", "text": "ping", "expect": {"shape": "exact", "value": "pong-7"}},
            {"seq": 2, "from": "operator", "kind": "person", "text": "fyi"},
        ]});
        let ev = events_from(&ch, &d(9), 0, "Me", &ops(), None, true);
        assert_eq!(ev[0].expect, Some(json!({"shape": "exact", "value": "pong-7"})));
        assert_eq!(ev[1].expect, None);
    }

    #[test]
    fn the_account_holder_is_labelled_operator() {
        let ev = events_from(&channel(), &d(9), 0, "Me", &ops(), None, true);
        assert_eq!(ev[0].provenance, "operator");
    }

    #[test]
    fn events_returns_at_once_when_something_is_pending() {
        let b = for_test(vec![9]);
        b.store.lock().unwrap().advance("D:9", 4, events_from(&channel(), &d(9), 0, "Me", &ops(), None, true)).unwrap();
        let got = b.events(0, Duration::from_secs(5));
        assert_eq!(got.len(), 2);
        assert!(b.events(got[1].id, Duration::from_millis(10)).is_empty());
    }

    #[test]
    fn a_waiting_reader_is_woken_by_new_events() {
        let b = Arc::new(for_test(vec![9]));
        let b2 = Arc::clone(&b);
        let t = std::thread::spawn(move || b2.events(0, Duration::from_secs(10)));
        std::thread::sleep(Duration::from_millis(50));
        b.store.lock().unwrap().advance("D:9", 4, events_from(&channel(), &d(9), 3, "Me", &ops(), None, true)).unwrap();
        b.woke.notify_all();
        let start = Instant::now();
        assert_eq!(t.join().unwrap().len(), 1);
        assert!(start.elapsed() < Duration::from_secs(5), "woken, not timed out");
    }

    #[test]
    fn a_claimed_channel_is_owned_until_the_claim_goes() {
        let b = for_test(vec![9]);
        assert!(!b.owns(&d(12)));
        b.claimed.lock().unwrap().insert(d(12));
        assert!(b.owns(&d(12)));
        assert_eq!(b.owned(), vec![d(9), d(12)]);
        assert_eq!(b.home_for("D"), Some(d(9)), "the configured channel stays home");
        b.claimed.lock().unwrap().clear();
        assert!(!b.owns(&d(12)));
    }

    #[test]
    fn the_same_card_id_in_two_documents_is_two_channels() {
        let b = for_test(vec![9]);
        assert!(b.owns(&d(9)));
        assert!(!b.owns(&Chan::new("E", 9)), "card ids are only unique inside a document");
        b.claimed.lock().unwrap().insert(Chan::new("E", 9));
        assert_eq!(b.owned().len(), 2);
    }

    fn ev_from(prov: &str, trusted: bool, peer: bool) -> Event {
        let mut e = events_from(&channel(), &d(9), 0, "Me", &ops(), None, true).remove(0);
        e.provenance = prov.into();
        e.trusted = trusted;
        e.peer = peer;
        e
    }

    #[test]
    fn the_loop_guard_stops_agents_until_the_operator_speaks() {
        let b = for_test(vec![9]);
        let peer = ev_from("agent", false, true);
        for i in 0..MAX_AGENT_RUN {
            assert!(b.admit("D:9", &peer), "agent message {i} gets through");
        }
        assert!(!b.admit("D:9", &peer), "then the run is over");
        assert!(!b.admit("D:9", &ev_from("builtin", true, false)), "trusted agents count too");
        assert!(b.admit("D:9", &ev_from("operator", true, false)), "the operator always gets through");
        assert!(b.admit("D:9", &peer), "and resets the run");
        assert!(b.admit("D:10", &peer), "each channel counts on its own");
        assert!(!b.admit("D:9", &ev_from("unverified", false, false)), "unverified: never");
    }

    #[test]
    fn group_channels_deliver_only_what_is_addressed_to_the_agent() {
        let ch = json!({"node": 5, "seq": 3, "group": true, "messages": [
            {"seq": 1, "from": "alice", "kind": "person", "to": ["Me"], "text": "for you"},
            {"seq": 2, "from": "Helper", "kind": "builtin", "to": ["Bot"], "text": "for someone else"},
            {"seq": 3, "from": "Helper", "kind": "builtin", "to": ["ME"], "text": "@me hello"}
        ]});
        let ev = events_from(&ch, &d(9), 0, "Me", &ops(), None, true);
        assert_eq!(ev.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 3], "not #2: addressed to Bot; names ignore case");
        let one_to_one = json!({"node": 5, "seq": 1, "messages": [{"seq": 1, "from": "alice", "text": "hi"}]});
        assert_eq!(events_from(&one_to_one, &d(9), 0, "Me", &ops(), None, true).len(), 1, "a 1:1 channel is unchanged");
    }

    #[test]
    fn joining_follows_claims_and_group_participation() {
        assert!(joins(&json!({"claimed_by": "Me", "participants": []}), "Me"));
        assert!(joins(&json!({"claimed_by": null, "group": true, "participants": ["X", "me"]}), "Me"));
        assert!(joins(&json!({"claimed_by": "X", "group": true, "participants": ["X", "Me"]}), "Me"), "a group's claim only picks the lead");
        assert!(joins(&json!({"claimed_by": null, "group": false, "participants": ["Me", "operator"]}), "Me"));
        assert!(!joins(&json!({"claimed_by": "X", "group": false, "participants": ["Me", "operator"]}), "Me"), "a 1:1 claimed by another agent is theirs");
        assert!(!joins(&json!({"claimed_by": null, "group": true, "participants": ["X", "operator"]}), "Me"));
    }

    #[test]
    fn kind_decides_when_the_server_records_it() {
        let ops = ops();
        assert_eq!(provenance_of(Some("person"), "alice", &ops, None, None, true), "operator");
        assert_eq!(provenance_of(Some("person"), "bob", &ops, None, None, true), "person", "another person in a shared document");
        assert_eq!(provenance_of(Some("builtin"), "Helper", &ops, None, None, true), "builtin");
        assert_eq!(provenance_of(Some("agent"), "alice", &ops, None, None, true), "agent", "a person posting with X-Agent is posting as an agent");
        assert_eq!(provenance_of(None, "alice", &ops, None, None, true), "unverified", "no kind: a header typed into the body proves nothing");
        assert_eq!(provenance_of(None, "alice", &ops, Some(true), None, true), "unverified", "not even with from_key_owner: agent keys are the owner's too");
        assert_eq!(provenance_of(Some("agent"), "alice", &ops, Some(true), None, true), "agent", "an agent key on the owner's account is still an agent");
        assert_eq!(provenance_of(None, "Robot", &ops, None, None, true), "unverified");
        assert_eq!(provenance_of(None, "webhook", &ops, None, None, true), "webhook");
        // Names are not unique across accounts.
        assert_eq!(provenance_of(Some("person"), "alice", &ops, None, None, false), "person", "a namesake in a document the owner does not own");
        assert_eq!(provenance_of(Some("person"), "alice", &ops, Some(false), None, true), "person", "the server says: not the key owner");
        assert_eq!(provenance_of(Some("person"), "anyname", &ops, Some(true), None, false), "operator", "the server says: the key owner");
        // via (trellis-web 0.59.3): only a signed-in browser is the operator.
        assert_eq!(provenance_of(Some("person"), "alice", &ops, Some(true), Some("session"), true), "operator");
        assert_eq!(provenance_of(Some("person"), "alice", &ops, Some(true), Some("api"), true), "person", "an unbound key with no X-Agent (#66)");
        assert_eq!(provenance_of(Some("person"), "alice", &ops, Some(true), Some("telegram"), true), "operator", "the operator's linked Telegram chat (web 0.65.0)");
        assert_eq!(provenance_of(Some("person"), "alice", &ops, Some(false), Some("telegram"), true), "person", "another account's Telegram");
        assert_eq!(provenance_of(Some("person"), "alice", &ops, Some(true), Some("app"), true), "operator", "the operator's phone on a device key (2754 #403)");
        assert_eq!(provenance_of(Some("person"), "alice", &ops, Some(false), Some("app"), true), "person", "another account's phone");
        assert_eq!(provenance_of(Some("agent"), "alice", &ops, Some(true), Some("app"), true), "agent");
        assert_eq!(provenance_of(Some("person"), "alice", &ops, Some(true), Some("internal"), true), "person");
        // The e2e stand-in: bound, verified, on the owner's account, and configured.
        let e2e = json!({"kind":"agent","from":"E2EOperator","agent_verified":true,"from_key_owner":true});
        assert_eq!(provenance(&e2e, &ops, Some("E2EOperator"), true), "operator");
        let group = json!({"node": 1, "group": true, "lead": "Me", "messages": [
            {"seq": 5, "from": "E2EOperator", "kind": "agent", "agent_verified": true, "from_key_owner": true, "to": [], "text": "ping"},
            {"seq": 6, "from": "Robot", "kind": "agent", "to": [], "text": "ping"}]});
        let ev = events_from(&group, &Chan::new("D", 9), 0, "Me", &ops, Some("E2EOperator"), true);
        assert_eq!(ev.len(), 1, "the stand-in reaches the lead like a person; another agent naming nobody does not");
        assert_eq!(ev[0].provenance, "operator");
        assert_eq!(ev[0].from, "alice", "presented as the operator");
        assert!(events_from(&group, &Chan::new("D", 9), 0, "Me", &ops, None, true).is_empty(), "not configured: nobody");
        assert_eq!(provenance(&e2e, &ops, None, true), "agent", "not configured: just an agent");
        let unverified = json!({"kind":"agent","from":"E2EOperator","from_key_owner":true});
        assert_eq!(provenance(&unverified, &ops, Some("E2EOperator"), true), "agent", "an unbound key using the name");
        let other = json!({"kind":"agent","from":"E2EOperator","agent_verified":true,"from_key_owner":false});
        assert_eq!(provenance(&other, &ops, Some("E2EOperator"), true), "agent", "another account's key");
    }

    #[test]
    fn only_the_operator_and_verified_builtins_are_trusted() {
        let home = json!({"name":"Helper","reach":"basket","document":"D"});
        assert!(trusted("operator", None, "D", None, true));
        assert!(trusted("builtin", Some(&home), "D", None, true));
        assert!(!trusted("builtin", None, "D", None, true), "recorded builtin but not listed by /api/agents");
        assert!(!trusted("builtin", Some(&home), "E", None, true), "a basket-reach agent acting in another document");
        assert!(trusted("builtin", Some(&json!({"reach":"account","document":"D"})), "E", None, true));
        assert!(!trusted("agent", Some(&home), "D", None, true), "the NAME of a built-in, posted by a key, is not the agent");
        assert!(!trusted("person", None, "D", None, true), "another person in a shared document");
        assert!(!trusted("webhook", None, "D", None, true));
        // #55: a collaborator's built-in agent in a shared document is builtin too.
        assert!(!trusted("builtin", Some(&home), "D", None, false), "not the operator's document: not trusted by kind alone");
        assert!(!trusted("builtin", Some(&home), "D", Some(false), true), "the server says: not the key owner's agent");
        assert!(trusted("builtin", Some(&home), "D", Some(true), false), "the server says: the key owner's own built-in agent");
    }

    #[test]
    fn a_builtin_row_keeps_who_and_where_and_drops_the_rest() {
        let r = builtin_row(&json!({"name":"Helper","reach":"document","document":"D","node":1,"card":10,"model":"x","turns_today":3}));
        assert_eq!(r["reach"], "document");
        assert!(r.get("model").is_none() && r.get("turns_today").is_none());
    }

    #[test]
    fn a_set_aside_channel_is_not_home_and_not_owned() {
        let b = for_test(vec![9, 10]);
        b.set_aside(&d(9), "400: that card is not a channel");
        assert!(!b.owns(&d(9)));
        assert_eq!(b.home_for("D"), Some(d(10)), "home moves to the next configured channel");
        b.set_aside(&d(9), "again");
        assert_eq!(b.status().refused.len(), 1, "recorded once");
    }

    #[test]
    fn owns_only_listed_unrefused_channels() {
        let b = for_test(vec![9, 10]);
        b.status.lock().unwrap().refused.push((d(10), "X".into()));
        assert!(b.owns(&d(9)));
        assert!(!b.owns(&d(10)));
        assert!(!b.owns(&d(11)));
    }
}
