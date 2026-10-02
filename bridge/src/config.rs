//! Config and the API key.
//!
//! The key is generated on first run and stored mode-600. It is never printed
//! after that and never logged — an agent reads it from the file, the same way
//! every other service on this machine works.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const APP: &str = "trellisbridge";
pub const DEFAULT_PORT: u16 = 8791;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub port: u16,
    pub api_key: String,
    /// Where Trellis is, and how to reach it. Absent in a config written
    /// before these existed, so each has a default.
    #[serde(default)]
    pub trellis: Trellis,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Trellis {
    pub url: String,
    /// The document uuid the key is scoped to. Unset until it is known;
    /// `trellisbridge check` lists what the key can see.
    pub document: Option<String>,
    /// The `X-Agent` name on every call: the agent's name in every channel.
    /// Empty: the name the key is bound to (`GET /api/whoami`).
    pub agent: String,
    pub ksm_url: String,
    pub ksm_key_file: PathBuf,
    /// The keystore-manager secret holding the Trellis key.
    pub ksm_secret: String,
    /// Read the Trellis key from this mode-600 file instead of
    /// keystore-manager — the usual setup; `trellisbridge init` writes it.
    /// Unset means keystore-manager (`ksm_secret`).
    pub key_file: Option<PathBuf>,
    /// The test suites' key, for `call --as-operator` and `call --as NAME`
    /// only. An agent key bound to its name (trellis-web 0.59.0)
    /// cannot post as anyone else, and with no `X-Agent` it still signs as
    /// that name. Playing the operator or a peer needs an unbound key: this
    /// mode-600 file, else `test_ksm_secret` when the main key comes from
    /// keystore-manager. `TRELLISBRIDGE_TEST_KEY_FILE` overrides both.
    pub test_key_file: Option<PathBuf>,
    pub test_ksm_secret: String,
    /// Set only where the e2e suites run: the agent name of their operator
    /// key, bound to it in trellis-web. A message from that key, verified by
    /// the server, counts as the operator's. Empty (the default) disables it.
    pub e2e_operator: String,
    /// That key: a mode-600 file, else this keystore-manager secret. Used by
    /// `call --as-operator` when `e2e_operator` is set.
    pub e2e_operator_key_file: Option<PathBuf>,
    pub e2e_operator_ksm_secret: String,
    /// Channels this agent answers in (DESIGN D5): a card id (in `document`)
    /// or `"<document>:<card>"`. Channels claimed for the agent in Trellis are
    /// added at run time; the bridge never adopts one merely because it can
    /// see it.
    pub channels: Vec<ChannelRef>,
    /// Documents to follow. Empty: every document the key reaches, from
    /// `GET /api/agent`, refreshed as that changes.
    pub documents: Vec<String>,
    /// The agent's card (`POST /api/agents/card`, web 0.78.0 / desktop):
    /// what other agents and channel readers see. Published at startup and by
    /// `trellisbridge card`. Empty: a one-line default naming the agent.
    pub description: String,
    /// Its picture in channels (web 0.81.0, desktop 0.211.0): png, jpeg, webp
    /// or gif, at most 256 KB; the server crops and shrinks it to 128×128.
    /// Unset: the card is published without touching the picture.
    pub avatar: Option<PathBuf>,
}

/// `21` or `"ce81f807-…:21"` in the config.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ChannelRef {
    Card(u64),
    InDoc(String),
}

impl ChannelRef {
    /// `(document, card)`, with `default` for a bare card id.
    pub fn resolve(&self, default: &str) -> Result<(String, u64), String> {
        match self {
            ChannelRef::Card(c) => Ok((default.to_string(), *c)),
            ChannelRef::InDoc(s) => {
                let (doc, card) = s.rsplit_once(':').ok_or_else(|| format!("channel {s:?}: expected <document>:<card>"))?;
                let card = card.parse().map_err(|_| format!("channel {s:?}: card is not a number"))?;
                Ok((doc.to_string(), card))
            }
        }
    }
}

impl Default for Trellis {
    fn default() -> Self {
        Trellis {
            url: "https://trellis-cards.com".into(),
            document: None,
            agent: String::new(),
            ksm_url: "http://127.0.0.1:7474".into(),
            ksm_key_file: PathBuf::new(),
            ksm_secret: String::new(),
            key_file: None,
            test_key_file: None,
            test_ksm_secret: String::new(),
            e2e_operator: String::new(),
            e2e_operator_key_file: None,
            e2e_operator_ksm_secret: String::new(),
            channels: Vec::new(),
            documents: Vec::new(),
            description: String::new(),
            avatar: None,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            port: DEFAULT_PORT,
            api_key: generate_key(),
            trellis: Trellis::default(),
        }
    }
}

/// 32 hex characters from the OS CSPRNG.
///
/// Reading /dev/urandom directly is Unix-only, which is fine here — this
/// service binds loopback on a Linux box — but it is the line to change first
/// if that ever stops being true.
fn generate_key() -> String {
    use std::io::Read;
    let mut bytes = [0u8; 16];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        let _ = f.read_exact(&mut bytes);
    }
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Cursors and undelivered events, beside the config.
pub fn state_path() -> Result<PathBuf, String> {
    Ok(config_path()?.with_file_name("state.json"))
}

/// `TRELLISBRIDGE_CONFIG` (or `--config`), else the per-user default. One
/// config per agent: several bridges on one host each get their own file, and
/// their state sits beside it.
pub fn config_path() -> Result<PathBuf, String> {
    if let Some(p) = std::env::var_os("TRELLISBRIDGE_CONFIG").filter(|p| !p.is_empty()) {
        return Ok(PathBuf::from(p));
    }
    let dirs = directories::ProjectDirs::from("", "", APP)
        .ok_or_else(|| "no config directory on this platform".to_string())?;
    Ok(dirs.config_dir().join("config.toml"))
}

impl Config {
    /// Load, or create on first run. A missing config is not an error.
    pub fn load_or_create(path: Option<&Path>) -> Result<Config, String> {
        let path = match path {
            Some(p) => p.to_path_buf(),
            None => config_path()?,
        };
        match std::fs::read_to_string(&path) {
            Ok(text) => toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let cfg = Config::default();
                cfg.save(&path)?;
                Ok(cfg)
            }
            Err(e) => Err(format!("{}: {e}", path.display())),
        }
    }

    pub fn save(&self, path: &Path) -> Result<(), String> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        }
        let text = toml::to_string_pretty(self).map_err(|e| e.to_string())?;
        std::fs::write(path, text).map_err(|e| format!("{}: {e}", path.display()))?;
        set_private(path)
    }
}

#[cfg(unix)]
fn set_private(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| format!("{}: {e}", path.display()))
}

#[cfg(not(unix))]
fn set_private(_path: &Path) -> Result<(), String> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channels_are_card_ids_or_document_card_pairs() {
        let t: Trellis = toml::from_str("channels = [21, \"E:7\"]").unwrap();
        assert_eq!(t.channels[0].resolve("D").unwrap(), ("D".into(), 21));
        assert_eq!(t.channels[1].resolve("D").unwrap(), ("E".into(), 7));
        assert!(ChannelRef::InDoc("nocard".into()).resolve("D").is_err());
    }

    #[test]
    fn generated_keys_are_hex_and_not_constant() {
        let a = generate_key();
        let b = generate_key();
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        // A CSPRNG that returns the same key twice means the read failed and
        // every install would share a key — worth failing loudly over.
        assert_ne!(a, b);
    }
}
