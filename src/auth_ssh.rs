use anyhow::{Context, bail};
use log::{info, warn};
use ssh_key::{HashAlg, PublicKey};
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::SystemTime;

use crate::Authenticator;
use varlink_http_bridge::SSHAUTH_MAGIC_PREFIX;

struct KeyCache {
    keys: HashMap<String, PublicKey>,
    mtime: SystemTime,
}

pub(crate) struct SshKeyAuthenticator {
    path: String,
    max_skew: u64,
    cache: RwLock<KeyCache>,
}

/// Parse an `authorized_keys` file, returning only supported (non-RSA) keys.
fn load_keys(path: &str) -> anyhow::Result<HashMap<String, PublicKey>> {
    let keys_vec = sshauth::keyfile::parse_authorized_keys(path, true)
        .with_context(|| format!("failed to read authorized keys from {path}"))?;

    let mut keys = HashMap::new();
    let mut rsa_skipped = 0u32;
    for key in keys_vec {
        if matches!(key.algorithm(), ssh_key::Algorithm::Rsa { .. }) {
            warn!(
                "ignoring RSA key {} ({}): RSA signing is not supported, use Ed25519 or ECDSA",
                key.fingerprint(HashAlg::Sha256),
                key.comment(),
            );
            rsa_skipped += 1;
            continue;
        }
        let fp = key.fingerprint(HashAlg::Sha256).to_string();
        keys.insert(fp, key);
    }

    if keys.is_empty() {
        if rsa_skipped > 0 {
            bail!(
                "no supported SSH public keys in {path}: found {rsa_skipped} RSA key(s) but RSA is not supported, use Ed25519 or ECDSA"
            );
        }
        bail!("no valid SSH public keys found in {path}");
    }

    Ok(keys)
}

impl SshKeyAuthenticator {
    pub(crate) fn new(path: &str) -> anyhow::Result<Self> {
        let keys = load_keys(path)?;
        let mtime = std::fs::metadata(path)
            .and_then(|m| m.modified())
            .with_context(|| format!("failed to stat {path}"))?;

        Ok(Self {
            path: path.to_string(),
            max_skew: 300,
            cache: RwLock::new(KeyCache { keys, mtime }),
        })
    }

    pub(crate) fn key_count(&self) -> usize {
        self.cache.read().unwrap().keys.len()
    }

    #[cfg(test)]
    pub(crate) fn with_max_skew(mut self, max_skew: u64) -> Self {
        self.max_skew = max_skew;
        self
    }

    /// Reload the `authorized_keys` file if its mtime has changed.
    fn maybe_reload(&self) {
        let current_mtime = match std::fs::metadata(&self.path).and_then(|m| m.modified()) {
            Ok(m) => m,
            Err(e) => {
                warn!("cannot stat {}: {e}, keeping cached keys", self.path);
                return;
            }
        };

        // Fast path: mtime unchanged (read lock only)
        if self.cache.read().unwrap().mtime == current_mtime {
            return;
        }

        // Slow path: mtime changed, reload under write lock
        let mut cache = self.cache.write().unwrap();
        // Double-check after acquiring write lock
        if cache.mtime == current_mtime {
            return;
        }

        match load_keys(&self.path) {
            Ok(keys) => {
                info!(
                    "reloaded {} SSH key(s) from {} (file changed)",
                    keys.len(),
                    self.path,
                );
                cache.keys = keys;
                cache.mtime = current_mtime;
            }
            Err(e) => {
                warn!(
                    "failed to reload {}: {e:#}, keeping {} cached key(s)",
                    self.path,
                    cache.keys.len(),
                );
                // Update mtime so we don't retry on every request
                cache.mtime = current_mtime;
            }
        }
    }
}

// XXX: hu? public key material is fine? Manual Debug impl to avoid exposing key material
impl std::fmt::Debug for SshKeyAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SshKeyAuthenticator")
            .field("path", &self.path)
            .field("max_skew", &self.max_skew)
            .finish_non_exhaustive()
    }
}

impl Authenticator for SshKeyAuthenticator {
    fn check_request(&self, method: &str, path: &str, auth_header: &str) -> Result<(), String> {
        self.maybe_reload();

        let token_str = auth_header
            .strip_prefix("Bearer ")
            .ok_or("Authorization header must start with 'Bearer '")?;

        let token = sshauth::UnverifiedToken::try_from(token_str)
            .map_err(|e| format!("invalid token: {e}"))?;

        let fp = token
            .untrusted_fingerprint()
            .ok_or("token does not contain a fingerprint")?;

        let fp_str = fp.to_string();
        let cache = self.cache.read().unwrap();
        let pubkey = cache
            .keys
            .get(&fp_str)
            .ok_or_else(|| format!("unknown key fingerprint: {fp_str}"))?;

        let mut v = token.verify_for();
        v.magic_prefix(SSHAUTH_MAGIC_PREFIX)
            .max_skew_seconds(self.max_skew)
            .action("method", method)
            .action("path", path);
        v.with_key(pubkey)
            .map_err(|e| format!("token verification failed: {e}"))?;

        Ok(())
    }
}
