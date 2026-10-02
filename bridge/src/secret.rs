//! A secret held in memory, and fetching one from keystore-manager.
//!
//! The Trellis key lives in keystore-manager and is revealed once at startup,
//! with a reason that lands in its audit log. It is never written to disk by
//! this process and never printed: `Secret` has no `Display`, and its `Debug`
//! shows only the length.

use base64::Engine;
use std::path::Path;

pub struct Secret(String);

impl Secret {
    pub fn expose(&self) -> &str {
        &self.0
    }

    #[cfg(test)]
    pub fn for_test(v: &str) -> Secret {
        Secret(v.to_string())
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Secret(<{} bytes>)", self.0.len())
    }
}

/// `POST {ksm}/api/secrets/{name}/reveal`, authenticated with the agent key in
/// `key_file`. A 202 means the secret's policy is `approve` and a human has to
/// click; this service does not wait on a human, so that is an error naming
/// the fix.
pub fn reveal(ksm_url: &str, key_file: &Path, name: &str, reason: &str) -> Result<Secret, String> {
    let agent_key = std::fs::read_to_string(key_file)
        .map_err(|e| format!("keystore-manager agent key {}: {e}", key_file.display()))?;
    let url = format!("{}/api/secrets/{name}/reveal", ksm_url.trim_end_matches('/'));
    let resp = ureq::post(&url)
        .set("X-API-Key", agent_key.trim())
        .send_json(serde_json::json!({ "reason": reason }));
    let body: serde_json::Value = match resp {
        Ok(r) if r.status() == 200 => r.into_json().map_err(|e| format!("{url}: {e}"))?,
        Ok(r) => {
            return Err(format!(
                "{url} → {}: secret `{name}` needs approval — set its policy to `allow` in keystore-manager",
                r.status()
            ))
        }
        Err(ureq::Error::Status(code, r)) => {
            let msg = r.into_string().unwrap_or_default();
            return Err(format!("{url} → {code}: {}", msg.trim()));
        }
        Err(e) => return Err(format!("{url}: {e}")),
    };
    let b64 = body["value_b64"]
        .as_str()
        .ok_or_else(|| format!("{url}: no `value_b64` in the reply"))?;
    decode(b64).map_err(|e| format!("secret `{name}`: {e}"))
}

/// Read a key from a file that only its owner can read. Anything wider is
/// refused rather than used: a group- or world-readable key is already leaked
/// to everyone on the box.
pub fn from_file(path: &Path) -> Result<Secret, String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = std::fs::metadata(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(format!("{} is mode {mode:o} — chmod 600 it; a key others can read is not used", path.display()));
        }
    }
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let text = text.trim().to_string();
    if text.is_empty() {
        return Err(format!("{} is empty", path.display()));
    }
    Ok(Secret(text))
}

fn decode(b64: &str) -> Result<Secret, String> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| format!("not base64: {e}"))?;
    let text = String::from_utf8(bytes).map_err(|_| "not UTF-8 text".to_string())?;
    // A key pasted into the desktop app often carries a trailing newline.
    let text = text.trim().to_string();
    if text.is_empty() {
        return Err("empty".into());
    }
    Ok(Secret(text))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_shows_the_value() {
        let s = decode("c2VjcmV0LXZhbHVlCg==").unwrap();
        assert_eq!(s.expose(), "secret-value");
        let shown = format!("{s:?}");
        assert!(!shown.contains("secret-value"), "{shown}");
        assert!(shown.contains("12 bytes"), "{shown}");
    }

    #[cfg(unix)]
    #[test]
    fn a_key_file_others_can_read_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let p = std::env::temp_dir().join(format!("tb-key-{}", std::process::id()));
        std::fs::write(&p, "k\n").unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(from_file(&p).unwrap_err().contains("chmod 600"));
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(from_file(&p).unwrap().expose(), "k");
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn empty_and_malformed_values_are_refused() {
        assert!(decode("").is_err());
        assert!(decode("!!!").is_err());
    }
}
