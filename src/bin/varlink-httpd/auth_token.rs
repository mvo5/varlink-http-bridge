// SPDX-License-Identifier: LGPL-2.1-or-later

use anyhow::{Context, bail};
use log::{info, warn};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::SystemTime;

use crate::Authenticator;

/// Prefix for generated tokens so a leaked token is recognizable
/// (e.g. by secret scanners) as belonging to this service.
const TOKEN_PREFIX: &str = "vhb_";

const SHA256_LEN: usize = 32;

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

fn hex_decode_sha256(hex: &str) -> anyhow::Result<[u8; SHA256_LEN]> {
    if !hex.is_ascii() || hex.len() != SHA256_LEN * 2 {
        bail!("expected {} hex characters, got {:?}", SHA256_LEN * 2, hex);
    }
    let mut digest = [0u8; SHA256_LEN];
    for (i, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .with_context(|| format!("invalid hex in {hex:?}"))?;
    }
    Ok(digest)
}

/// One accepted token, stored as its SHA-256 digest so the tokens file
/// never contains the secret itself. The name identifies the token in
/// logs and makes revocation (deleting its line) practical.
struct TokenEntry {
    digest: [u8; SHA256_LEN],
    name: String,
}

impl TokenEntry {
    /// Parse a `sha256:<hex> [name]` line; empty lines and `#` comments
    /// yield `None`.
    fn parse_line(line: &str) -> anyhow::Result<Option<Self>> {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return Ok(None);
        }
        let mut fields = line.split_whitespace();
        let hex = fields
            .next()
            .expect("non-empty line has a first field")
            .strip_prefix("sha256:")
            .context("token line must start with 'sha256:'")?;
        let digest = hex_decode_sha256(hex)?;
        let name = fields.next().unwrap_or(&hex[..8]).to_string();
        Ok(Some(Self { digest, name }))
    }
}

/// One tracked tokens file: its mtime when last read and the entries it
/// contained (mirrors `AuthKeysFile` in `auth_ssh`).
struct TokensFile {
    mtime: SystemTime,
    entries: Vec<TokenEntry>,
}

impl TokensFile {
    /// Stat `path`, folding `NotFound` into `Ok(None)` so missing files are
    /// treated as "tracked absence" rather than a hard error.
    fn stat_mtime(path: &str) -> std::io::Result<Option<SystemTime>> {
        match std::fs::metadata(path).and_then(|m| m.modified()) {
            Ok(m) => Ok(Some(m)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Parse a tokens file. Malformed lines are skipped with a warning:
    /// a bad line can only reduce access, and dropping the whole file on
    /// one typo would lock out every other token.
    fn parse_tokens(path: &str) -> anyhow::Result<Vec<TokenEntry>> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read tokens from {path}"))?;
        let mut entries = Vec::new();
        for (nr, line) in content.lines().enumerate() {
            match TokenEntry::parse_line(line) {
                Ok(Some(entry)) => entries.push(entry),
                Ok(None) => {}
                Err(e) => warn!("{path}:{}: skipping token line: {e:#}", nr + 1),
            }
        }
        Ok(entries)
    }

    /// Stat and parse `path`. Returns `Ok(None)` if the file does not
    /// exist yet (it will be picked up by `maybe_reload` once it appears).
    fn load(path: &str) -> anyhow::Result<Option<Self>> {
        let mtime = match Self::stat_mtime(path) {
            Ok(Some(m)) => m,
            Ok(None) => return Ok(None),
            Err(e) => {
                return Err(anyhow::Error::new(e).context(format!("failed to stat {path}")));
            }
        };
        let entries = Self::parse_tokens(path)?;
        Ok(Some(Self { mtime, entries }))
    }
}

struct TokenCache {
    files: HashMap<String, TokensFile>,
}

impl TokenCache {
    /// Initial load of all tracked paths. Files that do not (yet) exist
    /// are silently skipped; they will be picked up by `reload` once
    /// they appear. Read errors propagate (startup should fail loud).
    fn load_all(paths: &[String]) -> anyhow::Result<Self> {
        let mut files = HashMap::new();
        for path in paths {
            match TokensFile::load(path)? {
                Some(f) => {
                    files.insert(path.clone(), f);
                }
                None => info!("tokens file {path} does not exist yet, skipping"),
            }
        }
        Ok(Self { files })
    }

    fn token_count(&self) -> usize {
        self.files.values().map(|f| f.entries.len()).sum()
    }

    /// Find the name of the token matching `digest`. Comparing digests in
    /// constant time is cheap insurance even though a timing side channel
    /// on a digest does not reveal the token itself.
    fn lookup(&self, digest: &[u8; SHA256_LEN]) -> Option<String> {
        self.files
            .values()
            .flat_map(|f| &f.entries)
            .find(|e| openssl::memcmp::eq(&e.digest, digest))
            .map(|e| e.name.clone())
    }

    /// Ok(true) if any `path` in `paths` has an mtime that differs from
    /// what this cache has recorded (including "file now exists" and
    /// "file now gone").
    fn any_mtime_changed(&self, paths: &[String]) -> Result<bool, (String, std::io::Error)> {
        for path in paths {
            let now = TokensFile::stat_mtime(path).map_err(|e| (path.clone(), e))?;
            let cached = self.files.get(path).map(|f| f.mtime);
            if now != cached {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// If any tracked path has changed on disk, re-read it; transient
    /// stat errors are logged and the cache is left untouched (retried
    /// on the next call).
    fn maybe_reload(&mut self, paths: &[String]) {
        match self.any_mtime_changed(paths) {
            Ok(false) => {}
            Ok(true) => self.reload(paths),
            Err((path, e)) => {
                warn!("cannot stat {path}: {e}, skipping reload (keeping cached tokens)");
            }
        }
    }

    fn reload(&mut self, paths: &[String]) {
        let mut new_files = HashMap::new();
        for path in paths {
            let Ok(Some(mtime)) = TokensFile::stat_mtime(path) else {
                continue; // file is gone or unreadable; drop its cached tokens
            };
            let entries = match TokensFile::parse_tokens(path) {
                Ok(entries) => {
                    info!(
                        "reloaded {count} token(s) from {path} (file changed)",
                        count = entries.len(),
                    );
                    entries
                }
                Err(e) => {
                    warn!("failed to reload {path}: {e:#}, skipping this source");
                    Vec::new()
                }
            };
            new_files.insert(path.clone(), TokensFile { mtime, entries });
        }

        self.files = new_files;
        if self.token_count() == 0 {
            warn!("all token sources are empty, token auth will reject all requests");
        }
    }
}

pub(crate) struct TokenAuthenticator {
    paths: Vec<String>,
    tokens: Mutex<TokenCache>,
}

impl TokenAuthenticator {
    pub(crate) fn new(paths: Vec<String>) -> anyhow::Result<Self> {
        let cache = TokenCache::load_all(&paths)?;
        Ok(Self {
            paths,
            tokens: Mutex::new(cache),
        })
    }

    pub(crate) fn token_count(&self) -> usize {
        self.tokens.lock().unwrap().token_count()
    }
}

impl std::fmt::Debug for TokenAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenAuthenticator")
            .field("paths", &self.paths)
            .field("token_count", &self.token_count())
            .finish_non_exhaustive()
    }
}

impl Authenticator for TokenAuthenticator {
    fn check_request(
        &self,
        method: &str,
        path: &str,
        auth_header: Option<&str>,
        _nonce: Option<&str>,
        _channel_binding: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut tokens = self.tokens.lock().unwrap();
        tokens.maybe_reload(&self.paths);

        let auth_header = auth_header.context("missing Authorization header")?;
        let token = auth_header
            .strip_prefix("Bearer ")
            .context("Authorization header must start with 'Bearer '")?;
        let digest = openssl::sha::sha256(token.as_bytes());
        let name = tokens.lookup(&digest).context("unknown bearer token")?;

        info!("token auth OK: {method} {path} token={name}");
        Ok(())
    }
}

/// Create a token authenticator, or `None` when token auth is not
/// configured. An explicit `--tokens` flag always enables it (even for a
/// not-yet-existing file, which is picked up on reload); the well-known
/// locations enable it only when present, so a bridge without any token
/// configuration does not grow an authenticator that rejects everything.
pub(crate) fn create_token_authenticator(
    cli_tokens: Option<String>,
    creds_dir: Option<&std::path::Path>,
    root: &std::path::Path,
) -> anyhow::Result<Option<TokenAuthenticator>> {
    let paths: Vec<String> = if let Some(cli_path) = cli_tokens {
        vec![cli_path]
    } else {
        let mut paths = vec![
            root.join("etc/varlink-httpd/tokens")
                .to_string_lossy()
                .to_string(),
        ];
        if let Some(d) = creds_dir {
            paths.push(d.join("tokens").to_string_lossy().to_string());
        }
        paths.retain(|p| std::path::Path::new(p).exists());
        if paths.is_empty() {
            return Ok(None);
        }
        paths
    };

    let token_auth = TokenAuthenticator::new(paths.clone())?;
    if token_auth.token_count() == 0 {
        warn!(
            "no tokens in {}; token auth will reject all requests until tokens appear",
            paths.join(", "),
        );
    }
    info!(
        "Authenticator: adding bearer tokens ({count} token(s) from {sources})",
        count = token_auth.token_count(),
        sources = paths.join(", "),
    );
    Ok(Some(token_auth))
}

#[derive(Debug)]
pub(crate) struct GenToken {
    pub name: Option<String>,
    pub output: Option<String>,
}

fn default_tokens_path() -> String {
    if rustix::process::getuid().is_root() {
        return "/etc/varlink-httpd/tokens".to_string();
    }
    let config_dir = std::env::var_os("XDG_CONFIG_HOME").map_or_else(
        || {
            let home = std::env::var_os("HOME").unwrap_or_else(|| "/root".into());
            std::path::Path::new(&home).join(".config")
        },
        std::path::PathBuf::from,
    );
    config_dir
        .join("varlink-httpd/tokens")
        .to_string_lossy()
        .into_owned()
}

pub(crate) fn generate_token() -> String {
    let mut buf = [0u8; 32];
    openssl::rand::rand_bytes(&mut buf).expect("openssl PRNG failed");
    format!("{TOKEN_PREFIX}{}", hex_encode(&buf))
}

/// Append the hash line for `token` to the tokens file at `path`,
/// returning the name it was stored under.
pub(crate) fn append_token(
    path: &std::path::Path,
    token: &str,
    name: Option<&str>,
) -> anyhow::Result<String> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let hex = hex_encode(&openssl::sha::sha256(token.as_bytes()));
    let name = name.unwrap_or(&hex[..8]);
    // The name is a whitespace-separated field on the token line.
    if name.chars().any(char::is_whitespace) {
        bail!("token name must not contain whitespace: {name:?}");
    }

    let parent = path
        .parent()
        .with_context(|| format!("cannot determine parent directory of {}", path.display()))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create directory {}", parent.display()))?;
    // 0600: the file only holds hashes, but there is no reason to share it.
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    writeln!(f, "sha256:{hex} {name}")
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(name.to_string())
}

pub(crate) fn run_gen_token(cmd: GenToken) -> anyhow::Result<()> {
    let output_path = cmd.output.unwrap_or_else(default_tokens_path);
    let token = generate_token();
    let name = append_token(
        std::path::Path::new(&output_path),
        &token,
        cmd.name.as_deref(),
    )?;

    // The token itself goes to stdout (and nowhere else) so that
    // `TOKEN=$(varlink-httpd gen-token)` works; only its hash is stored.
    println!("{token}");
    eprintln!("Appended hash of token '{name}' to {output_path}, run with:");
    if output_path == "/etc/varlink-httpd/tokens" {
        eprintln!("  varlink-httpd");
    } else {
        eprintln!("  varlink-httpd --tokens={output_path}");
    }
    Ok(())
}
