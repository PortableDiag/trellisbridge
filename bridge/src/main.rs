//! TrellisBridge
//!
//! House conventions: flat modules, `Result<T, String>`, a blocking HTTP
//! server, and no async runtime.

mod api;
mod bridge;
mod config;
mod hooks;
mod mcp;
mod secret;
mod store;
mod stream;
mod watch;
mod trellis;

fn main() {
    if let Err(e) = run() {
        eprintln!("trellisbridge: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1).peekable();
    // `--config PATH` before the command: one bridge per agent, each with its
    // own file (and its state beside it). The same as TRELLISBRIDGE_CONFIG.
    if args.peek().map(String::as_str) == Some("--config") {
        args.next();
        let path = args.next().ok_or("--config PATH")?;
        std::env::set_var("TRELLISBRIDGE_CONFIG", path);
    }
    match args.next().as_deref() {
        Some("serve") | None => {
            let cfg = config::Config::load_or_create(None)?;
            serve(&cfg)
        }
        Some("key") => {
            let cfg = config::Config::load_or_create(None)?;
            println!("{}", cfg.api_key);
            Ok(())
        }
        Some("check") => {
            let cfg = config::Config::load_or_create(None)?;
            check(&cfg.trellis)
        }
        Some("init") => init(args.collect()),
        Some("card") => card(args.collect()),
        Some("call") => {
            let cfg = config::Config::load_or_create(None)?;
            let mut method = args.next().ok_or("call METHOD PATH [JSON]")?;
            let as_operator = method == "--as-operator";
            // `--as-person`: the test key with no X-Agent, which trellis-web
            // records as a person (`via: api`), never the operator. The suites
            // use it to end the server's run of agent messages, which a bound
            // e2e operator (an agent) cannot.
            let as_person = method == "--as-person";
            if as_operator || as_person {
                method = args.next().ok_or("call METHOD PATH [JSON]")?;
            }
            // `--as NAME`: post under another X-Agent name — for testing that
            // a borrowed name gets no trust.
            let mut as_name: Option<String> = None;
            if method == "--as" {
                as_name = Some(args.next().ok_or("--as NAME")?);
                method = args.next().ok_or("call METHOD PATH [JSON]")?;
            }
            let path = args.next().ok_or("call METHOD PATH [JSON]")?;
            // `@path` reads the body from a file: a body with a file in it is
            // too big for an argument list.
            let body = match args.next() {
                Some(b) => {
                    let text = match b.strip_prefix('@') {
                        Some(path) => std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?,
                        None => b,
                    };
                    Some(serde_json::from_str(&text).map_err(|e| format!("body: {e}"))?)
                }
                None => None,
            };
            let mut t = cfg.trellis.clone();
            if let Some(n) = &as_name {
                t.agent = n.clone();
            }
            // Where the bridge honours an e2e operator stand-in, playing the
            // operator means posting as it: a key with no X-Agent is only a
            // person since trellis-web 0.59.3 (`via: api`).
            let as_e2e_operator = as_operator && !t.e2e_operator.is_empty();
            if as_e2e_operator {
                t.agent = t.e2e_operator.clone();
                match t.e2e_operator_key_file.clone() {
                    Some(f) => t.key_file = Some(f),
                    None => t.ksm_secret = t.e2e_operator_ksm_secret.clone(),
                }
            } else if as_operator || as_person || as_name.is_some() {
                use_test_key(&mut t);
            }
            let client = connect(&mut t, "trellisbridge call")?;
            let method = method.to_ascii_uppercase();
            let v = if (as_operator && !as_e2e_operator) || as_person {
                client.call_as_operator(&method, &path, body.as_ref())?
            } else {
                client.call(&method, &path, body.as_ref())?
            };
            println!("{}", serde_json::to_string_pretty(&v).map_err(|e| e.to_string())?);
            Ok(())
        }
        Some("--version" | "-V") => {
            println!("trellisbridge {}", api::VERSION);
            Ok(())
        }
        Some("--help" | "-h") => {
            println!("TrellisBridge\n");
            println!("  trellisbridge [--config PATH] COMMAND   (or TRELLISBRIDGE_CONFIG; one config per agent)\n");
            println!("  trellisbridge init --key-file PATH [--agent NAME] [--port N] [--url URL] [--force]");
            println!("                           write the config for an agent key: its name and document from GET /api/whoami");
            println!("  trellisbridge card [--avatar PATH | --no-avatar] [--description TEXT]");
            println!("                           save the agent card's settings, then publish it (avatar shown in channels);");
            println!("                           only what you pass changes, and the card's skills stay. At startup the");
            println!("                           bridge publishes the card only if the agent has none yet");
            println!("  trellisbridge serve      run the API (default)");
            println!("  trellisbridge key        print the API key");
            println!("  trellisbridge check      load the Trellis key and prove it works");
            println!("  trellisbridge call [--as-operator] METHOD PATH [JSON | @file.json]");
            println!("                           one Trellis call through the bridge's key; non-2xx exits 1.");
            println!("                           --as-operator omits X-Agent, so it is attributed to the account holder (tests)");
            println!("                           --as NAME sends X-Agent: NAME (tests: a borrowed name must get no trust)");
            println!("                           both use the test key (test_key_file / test_ksm_secret) when one is set;");
            println!("                           with e2e_operator set, --as-operator posts as that bound name with its own key");
            println!("                           --as-person: the test key with no X-Agent — a person, never the operator (tests)");
            println!("  trellisbridge --version");
            Ok(())
        }
        Some(other) => Err(format!("unknown command {other:?} — try --help")),
    }
}

/// Reveal the key, then prove it against the live server: health (no key),
/// then an authenticated read. Prints what the key reaches, never the key.
fn check(t: &config::Trellis) -> Result<(), String> {
    let mut t = t.clone();
    let client = connect(&mut t, "trellisbridge check")?;
    println!("agent     {}", t.agent);
    match &t.key_file {
        Some(p) => println!("key       from {}", p.display()),
        None => println!("key       {} from keystore-manager", t.ksm_secret),
    }
    let health = client.get("/api/health")?;
    println!("trellis   {} {}", t.url, health["version"].as_str().unwrap_or("?"));
    if client.is_desktop() {
        // A desktop has no /api/agent: the port is the document, and its
        // instance key reaches all of it.
        let inst = client.get("/api/instance")?;
        println!("desktop   {} {} — no ?document= is sent", inst["document"].as_str().unwrap_or("?"), inst["version"].as_str().unwrap_or("?"));
        let me = client.get("/api/me")?;
        println!("scope     {}", me["can"]["reach"].as_str().unwrap_or("?"));
        return Ok(());
    }
    // `GET /api/agent` is the server's own answer to "what may this key
    // touch" — a key can be narrower than its document, and a 404 outside the
    // scope means "not yours", not "not there".
    let agent = client.get("/api/agent")?;
    let scope = &agent["you"]["scope"];
    println!(
        "scope     {} — document {}, basket {}",
        scope["kind"].as_str().unwrap_or("?"),
        scope["document"].as_str().unwrap_or("?"),
        scope["node"]
    );
    if let Some(means) = scope["means"].as_str() {
        println!("          {means}");
    }
    Ok(())
}

/// Point a `call` that plays someone else at the test suites' key (see
/// `Trellis::test_key_file`). With none configured the main key is used, which
/// works only while it is unbound.
fn use_test_key(t: &mut config::Trellis) {
    let file = std::env::var_os("TRELLISBRIDGE_TEST_KEY_FILE").map(std::path::PathBuf::from).or_else(|| t.test_key_file.clone());
    if let Some(f) = file {
        t.key_file = Some(f);
    } else if t.key_file.is_none() && !t.test_ksm_secret.is_empty() {
        t.ksm_secret = t.test_ksm_secret.clone();
    }
}

/// Get the Trellis key — from `key_file` when set, otherwise revealed from
/// keystore-manager, whose audit log records `reason` — and build a client.
/// An empty `agent` becomes the name the key is bound to, written back to `t`.
fn connect(t: &mut config::Trellis, reason: &str) -> Result<trellis::Client, String> {
    let key = match &t.key_file {
        Some(path) => secret::from_file(path)?,
        None if !t.ksm_secret.is_empty() => secret::reveal(&t.ksm_url, &t.ksm_key_file, &t.ksm_secret, reason)?,
        None => return Err("no Trellis key: set trellis.key_file (a mode-600 file) — `trellisbridge init` does".into()),
    };
    let mut client = trellis::Client::new(&t.url, key, &t.agent, t.document.clone());
    client.detect()?;
    if t.agent.is_empty() {
        t.agent = bound_agent(&client)?;
        client.set_agent(&t.agent);
    }
    Ok(client)
}

/// The name the key is bound to. Asked without an `X-Agent`, which a bound key
/// fills in with its own name. An unbound key has none, and an agent needs one.
fn bound_agent(client: &trellis::Client) -> Result<String, String> {
    if client.is_desktop() {
        return Err("set trellis.agent: a desktop key is bound to no name".into());
    }
    let who = client.call_as_operator("GET", "/api/whoami", None)?;
    who["key"]["bound_agent"]
        .as_str()
        .filter(|n| !n.is_empty())
        .map(str::to_string)
        .ok_or_else(|| "set trellis.agent: this key is not bound to an agent name (bind it in Trellis → Keys, or name it here)".into())
}

/// Write a config for one agent key: the bound name (or `--agent`) and the
/// key's document from `GET /api/whoami`, then prove it with the same calls
/// `check` makes. An existing config is kept unless `--force`.
fn init(args: Vec<String>) -> Result<(), String> {
    let mut it = args.into_iter();
    let (mut key_file, mut agent, mut port, mut url, mut force) = (None, String::new(), None, None, false);
    while let Some(a) = it.next() {
        let mut val = || it.next().ok_or_else(|| format!("{a} needs a value"));
        match a.as_str() {
            "--key-file" => key_file = Some(std::path::PathBuf::from(val()?)),
            "--agent" => agent = val()?,
            "--port" => port = Some(val()?.parse::<u16>().map_err(|e| format!("--port: {e}"))?),
            "--url" => url = Some(val()?),
            "--force" => force = true,
            other => return Err(format!("init: unknown option {other:?} — try --help")),
        }
    }
    let key_file = key_file.ok_or("init --key-file PATH: the agent's Trellis key, mode 600")?;
    let key_file = std::fs::canonicalize(&key_file).map_err(|e| format!("{}: {e}", key_file.display()))?;
    let path = config::config_path()?;
    if path.exists() && !force {
        return Err(format!("{} exists — --force to rewrite it (its api_key is kept)", path.display()));
    }
    let mut cfg = match path.exists() {
        true => config::Config::load_or_create(Some(&path))?,
        false => config::Config::default(),
    };
    let t = &mut cfg.trellis;
    t.key_file = Some(key_file);
    t.agent = agent;
    if let Some(u) = url {
        t.url = u;
    }
    if let Some(p) = port {
        cfg.port = p;
    }
    let mut t = cfg.trellis.clone();
    let client = connect(&mut t, "trellisbridge init")?;
    if !client.is_desktop() && t.document.is_none() {
        let who = client.get("/api/whoami")?;
        t.document = who["scope"]["document"]
            .as_str()
            .or_else(|| who["documents"].as_array().and_then(|d| d.iter().find(|d| d["owner"] == true)).and_then(|d| d["id"].as_str()))
            .map(str::to_string);
        if t.document.is_none() {
            return Err("the key reaches no document — check its scope in Trellis → Keys".into());
        }
    }
    cfg.trellis = t;
    cfg.save(&path)?;
    println!("config    {}", path.display());
    println!("agent     {}", cfg.trellis.agent);
    println!("document  {}", cfg.trellis.document.as_deref().unwrap_or("(desktop: the port is the document)"));
    println!("port      {} (127.0.0.1)", cfg.port);
    Ok(())
}

/// What a desktop calls its one document (`GET /api/instance` → `document`,
/// the file name): the bridge's label for it, since no id is ever sent.
fn desktop_document(client: &trellis::Client) -> Result<String, String> {
    let v = client.get("/api/instance")?;
    Ok(v["document"].as_str().unwrap_or("desktop").to_string())
}

fn serve(cfg: &config::Config) -> Result<(), String> {
    let mut t = cfg.trellis.clone();
    let client = connect(&mut t, "trellisbridge serve")?;
    let t = &t;
    println!("agent     {} on 127.0.0.1:{}", t.agent, cfg.port);
    let document = match (&t.document, client.is_desktop()) {
        (Some(d), _) => d.clone(),
        (None, true) => desktop_document(&client)?,
        (None, false) => return Err("set trellis.document in the config — `trellisbridge check` names it".into()),
    };
    if client.is_desktop() {
        println!("desktop   {} {document}: one document per port, no ?document= sent", t.url);
        // Before desktop 0.211.4 channel messages carried no `kind` or `via`,
        // and a keyed caller with no X-Agent is written `operator` just as the
        // app is — so nothing there proves the operator (D11). From 0.211.4
        // `say` records both, and `via: session` (the app's own compose row)
        // is the operator, as on the web. Older messages stay unverified.
        let v = client.get("/api/instance").ok().and_then(|v| v["version"].as_str().map(str::to_string)).unwrap_or_default();
        if version_at_least(&v, (0, 211, 4)) {
            println!("trust     desktop {v} records kind/via: the operator is `person` + `via: session`; keyed calls (`api`) never are");
        } else {
            println!("trust     desktop {v} records no kind/via (0.211.4 does): none is verified as the operator, so none is answered");
        }
    }
    if t.channels.is_empty() {
        println!("channels  none configured — answering only in channels claimed for {}", t.agent);
    }
    let operators = operator_names(&client)?;
    println!("operator  messages from {} are the operator's", operators.join(" or "));
    if !t.e2e_operator.is_empty() {
        println!("e2e       {} (bound, server-verified) also counts as the operator — test hosts only", t.e2e_operator);
    }
    let channels = t
        .channels
        .iter()
        .map(|c| c.resolve(&document).map(|(d, card)| bridge::Chan::new(&d, card)))
        .collect::<Result<Vec<_>, _>>()?;
    let mut store = store::Store::open(&config::state_path()?)?;
    store.migrate(&document)?;
    let discover = t.documents.is_empty();
    let called = store.called();
    let bridge = std::sync::Arc::new(bridge::Bridge {
        store: std::sync::Mutex::new(store),
        woke: std::sync::Condvar::new(),
        client: Some(client),
        agent: t.agent.clone(),
        operators,
        e2e_operator: (!t.e2e_operator.is_empty()).then(|| t.e2e_operator.clone()),
        documents: std::sync::Mutex::new(if discover { vec![document.clone()] } else { t.documents.clone() }),
        document,
        discover,
        channels,
        claimed: std::sync::Mutex::new(Default::default()),
        called: std::sync::Mutex::new(called),
        running: std::sync::Mutex::new(Default::default()),
        watching: std::sync::Mutex::new(Default::default()),
        swept: std::sync::Mutex::new(Default::default()),
        reading: std::sync::Mutex::new(()),
        builtins: std::sync::Mutex::new(Default::default()),
        builtins_read: std::sync::Mutex::new(None),
        owned_docs: std::sync::Mutex::new(Default::default()),
        talk: std::sync::Mutex::new(Default::default()),
        status: std::sync::Mutex::new(bridge::Status::default()),
        mode: std::sync::atomic::AtomicU8::new(bridge::MODE_UNKNOWN),
        refresh: std::sync::atomic::AtomicBool::new(false),
    });
    // Always read /api/agent: it says which documents the owner owns, which
    // the operator check needs even when the document list is fixed.
    let found = bridge.discover_documents();
    if !bridge.discover {
        if let Err(e) = &found {
            eprintln!("trellisbridge: reading /api/agent: {} — no document counts as owned", e.message);
        }
    }
    if bridge.discover {
        match found {
            Ok(docs) => {
                println!("documents {} reachable: {}", docs.len(), docs.join(", "));
                if let Ok(mut d) = bridge.documents.lock() {
                    *d = docs;
                }
            }
            Err(e) => eprintln!("trellisbridge: listing documents: {} — following only {}", e.message, bridge.document),
        }
    }
    // A card that exists is the agent's to keep: it may have changed its own
    // picture or skills since, and a restart must not put the old ones back.
    if let Some(client) = &bridge.client {
        match current_card(client) {
            Ok(Some(c)) => println!("card      kept as published ({} skills); `trellisbridge card` changes it", c["skills"].as_array().map_or(0, |s| s.len())),
            Ok(None) => match publish_card(client, t, None, None, Picture::Set) {
                Ok(line) => println!("card      {line}"),
                Err(e) => eprintln!("trellisbridge: agent card not published: {e}"),
            },
            Err(e) => eprintln!("trellisbridge: agent card not checked: {e} — left as it is"),
        }
    }
    bridge.refresh_builtins();
    bridge.refresh_key();
    bridge.claim_all()?;
    bridge.start();
    if let Some(port) = cfg.hooks_port {
        hooks::serve(port, cfg.hooks_url.clone())?;
    }
    api::serve(cfg, bridge)
}

/// Most the server takes for a picture (decoded): web and desktop agree.
const MAX_AVATAR: u64 = 256 * 1024;

/// What a card write does with the picture.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Picture {
    /// Leave the card's picture as it is.
    Keep,
    /// Send the configured `avatar` (when one is set).
    Set,
    /// `icon_base64: null`, the server's "no picture".
    Remove,
}

/// The agent's card as published now, or `None` when it has none (404).
fn current_card(client: &trellis::Client) -> Result<Option<serde_json::Value>, String> {
    match client.get("/api/agents/card") {
        Ok(v) => Ok(Some(v.get("card").cloned().unwrap_or(v))),
        Err(e) if e.status == Some(404) => Ok(None),
        Err(e) => Err(e.message),
    }
}

/// The body of a card write. A write replaces the card, so everything the
/// live card has (`base`) goes back unless this write changes it: the
/// description unless `description` is given, and always its skills and
/// version, which only the agent itself sets. With no card yet, the
/// description is the configured one or a one-line default.
fn card_body(base: Option<&serde_json::Value>, description: Option<&str>, agent: &str, icon: Option<serde_json::Value>) -> serde_json::Value {
    let description = match (description.map(str::trim), base.and_then(|b| b["description"].as_str())) {
        (Some(d), _) if !d.is_empty() => d.to_string(),
        (_, Some(d)) if !d.is_empty() => d.to_string(),
        _ => format!("{agent}: a Hermes agent in this workspace, connected through TrellisBridge."),
    };
    let mut body = serde_json::json!({ "description": description });
    for k in ["skills", "version"] {
        if let Some(v) = base.and_then(|b| b.get(k)).filter(|v| !v.is_null()) {
            body[k] = v.clone();
        }
    }
    if let Some(icon) = icon {
        body["icon_base64"] = icon;
    }
    body
}

/// Write the agent's card over `base` (the live card, if any): `description`
/// replaces its text, and `picture` says what happens to its picture. Needs
/// a key bound to the agent's name; an unbound one gets the server's own 403.
fn publish_card(client: &trellis::Client, t: &config::Trellis, base: Option<&serde_json::Value>, description: Option<&str>, picture: Picture) -> Result<String, String> {
    use base64::Engine;
    let mut note = "picture kept".to_string();
    let icon = match (picture, &t.avatar) {
        (Picture::Remove, _) => {
            note = "picture removed".into();
            Some(serde_json::Value::Null)
        }
        (Picture::Set, Some(path)) => {
            let bytes = std::fs::read(path).map_err(|e| format!("avatar {}: {e}", path.display()))?;
            if bytes.len() as u64 > MAX_AVATAR {
                return Err(format!("avatar {} is {} KB; the server takes at most 256 KB — shrink it (256×256 is plenty)", path.display(), bytes.len() / 1024));
            }
            note = format!("avatar {}", path.display());
            Some(base64::engine::general_purpose::STANDARD.encode(&bytes).into())
        }
        (Picture::Set, None) if base.is_none() => {
            note = "no picture set".into();
            None
        }
        _ => None,
    };
    let description = description.or(base.is_none().then_some(t.description.as_str()));
    let body = card_body(base, description, &t.agent, icon);
    let v = client.post("/api/agents/card", &body)?;
    let icon = v["icon_url"].as_str().or_else(|| v["card"]["icon_url"].as_str()).unwrap_or("none");
    Ok(format!("published as {} — {note}; icon {icon}", t.agent))
}

/// `trellisbridge card`: change the card's settings in the config, then
/// publish it now, so a new picture shows without a restart.
fn card(args: Vec<String>) -> Result<(), String> {
    let path = config::config_path()?;
    let mut cfg = config::Config::load_or_create(Some(&path))?;
    let (mut picture, mut description) = (Picture::Keep, None);
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--avatar" => {
                let p = it.next().ok_or("--avatar PATH")?;
                let p = std::fs::canonicalize(&p).map_err(|e| format!("{p}: {e}"))?;
                cfg.trellis.avatar = Some(p);
                picture = Picture::Set;
            }
            "--no-avatar" => {
                cfg.trellis.avatar = None;
                picture = Picture::Remove;
            }
            "--description" => {
                let d = it.next().ok_or("--description TEXT")?;
                cfg.trellis.description = d.clone();
                description = Some(d);
            }
            other => return Err(format!("card: unknown option {other:?} — try --help")),
        }
    }
    let changed = picture != Picture::Keep || description.is_some();
    let mut t = cfg.trellis.clone();
    let client = connect(&mut t, "trellisbridge card")?;
    let base = current_card(&client)?;
    // No card yet: publish the saved settings in full.
    if base.is_none() && picture == Picture::Keep {
        picture = Picture::Set;
    }
    let line = publish_card(&client, &t, base.as_ref(), description.as_deref(), picture)?;
    if changed {
        cfg.save(&path)?;
    }
    println!("card      {line}");
    Ok(())
}

/// Who the account holder shows up as in a channel: `operator` on the desktop,
/// and on trellis-web the display name or, without one, the email's local
/// part — the name it wrote on a live channel.
fn operator_names(client: &trellis::Client) -> Result<Vec<String>, String> {
    let me = client.get("/api/me")?;
    let mut names = vec!["operator".to_string()];
    if let Some(n) = me["display_name"].as_str().filter(|n| !n.is_empty()) {
        names.push(n.to_string());
    }
    if let Some(local) = me["email"].as_str().and_then(|e| e.split('@').next()) {
        names.push(local.to_string());
    }
    Ok(names)
}

/// "0.211.4" ≥ (0, 211, 4). Anything unparseable is older.
fn version_at_least(v: &str, min: (u64, u64, u64)) -> bool {
    let mut n = v.split(|c: char| !c.is_ascii_digit()).filter(|p| !p.is_empty()).map(|p| p.parse::<u64>().unwrap_or(0));
    match (n.next(), n.next(), n.next()) {
        (Some(a), Some(b), Some(c)) => (a, b, c) >= min,
        _ => false,
    }
}

#[cfg(test)]
mod version_tests {
    #[test]
    fn desktop_versions_compare() {
        assert!(super::version_at_least("0.211.4", (0, 211, 4)));
        assert!(super::version_at_least("0.212.0", (0, 211, 4)));
        assert!(!super::version_at_least("0.211.2", (0, 211, 4)));
        assert!(!super::version_at_least("", (0, 211, 4)));
    }
}

#[cfg(test)]
mod card_tests {
    use super::card_body;
    use serde_json::json;

    #[test]
    fn a_write_keeps_what_the_agent_set() {
        let live = json!({"name": "Nexus", "description": "mine", "skills": [{"name": "a"}], "version": "1.0.1", "icon_url": "/api/avatars/x"});
        // A picture-only write: text, skills and version go back unchanged.
        let b = card_body(Some(&live), None, "Nexus", Some(json!("AAAA")));
        assert_eq!(b, json!({"description": "mine", "skills": [{"name": "a"}], "version": "1.0.1", "icon_base64": "AAAA"}));
        // A new description: still the skills, and no picture field (kept).
        let b = card_body(Some(&live), Some("new"), "Nexus", None);
        assert_eq!(b, json!({"description": "new", "skills": [{"name": "a"}], "version": "1.0.1"}));
    }

    #[test]
    fn a_first_card_uses_the_config_or_a_default() {
        assert_eq!(card_body(None, Some("set"), "Orbit", None), json!({"description": "set"}));
        let b = card_body(None, Some("  "), "Orbit", None);
        assert!(b["description"].as_str().unwrap().starts_with("Orbit: a Hermes agent"));
        assert!(b.get("skills").is_none());
    }
}
