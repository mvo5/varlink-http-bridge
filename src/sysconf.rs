// SPDX-License-Identifier: LGPL-2.1-or-later

//! Locating service configuration the systemd way: credentials passed via
//! `$CREDENTIALS_DIRECTORY` (see systemd.exec(5) / systemd.system-credentials(7)),
//! and config files in the `/etc` > `/run` > `/usr/lib` precedence hierarchy.

use std::path::{Path, PathBuf};

/// Mirrors (and extends) libsystemd's `CredentialsLoader`: one file per
/// credential, the filename is the credential id. On top of the lookup by
/// id, [`Self::find`] selects credentials by `ImportCredential=` patterns.
pub struct CredentialsLoader {
    dir: PathBuf,
}

impl CredentialsLoader {
    // TODO: make this a constructor, i.e. return Option<Self> here
    #[must_use]
    pub fn path_from_env() -> Option<PathBuf> {
        std::env::var_os("CREDENTIALS_DIRECTORY").map(PathBuf::from)
    }

    /// Loader from at an explicit directory (mainly for tests).
    pub fn from_dir(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// Path of credential `id`, if the file exists.
    #[must_use]
    pub fn path(&self, id: &str) -> Option<PathBuf> {
        let path = self.dir.join(id);
        path.exists().then_some(path)
    }

    /// Paths of the credentials matching `patterns`, sorted so the merge
    /// order is stable.
    ///
    /// Patterns use the `ImportCredential=` syntax of systemd.exec(5): an
    /// exact id, or a prefix with a trailing `*`. The latter is for
    /// per-provider credentials (`<id>.<provider>`).
    ///
    /// # Errors
    /// A missing directory means no credentials; other errors propagate so the
    /// caller can keep what it already loaded rather than lose sources silently.
    pub fn find(&self, patterns: &[&str]) -> std::io::Result<Vec<PathBuf>> {
        self.find_with(patterns, |id| self.dir.join(id))
    }

    /// [`Self::find`], with each matching id handed to `f` in sorted order.
    ///
    /// # Errors
    /// As for [`Self::find`].
    pub fn find_with<T>(
        &self,
        patterns: &[&str],
        f: impl Fn(&str) -> T,
    ) -> std::io::Result<Vec<T>> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut ids = Vec::new();
        for entry in entries {
            let Some(id) = entry?.file_name().to_str().map(String::from) else {
                continue;
            };
            if patterns.iter().any(|p| pattern_matches(p, &id)) {
                ids.push(id);
            }
        }
        ids.sort();
        Ok(ids.iter().map(|id| f(id)).collect())
    }
}

/// `ImportCredential=` matching: only a trailing `*` is a glob.
fn pattern_matches(pattern: &str, name: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => {
            debug_assert!(
                !prefix.contains('*'),
                "only a trailing * is a glob: {pattern}"
            );
            name.starts_with(prefix)
        }
        None => name == pattern,
    }
}

/// Highest-precedence existing config file for `rel`, following the systemd
/// hierarchy (`/etc` over `/run` over `/usr/lib`). `root` is `/` in
/// production, a tempdir in tests.
#[must_use]
pub fn find_config(rel: &str, root: &Path) -> Option<PathBuf> {
    ["etc", "run", "usr/lib"]
        .into_iter()
        .map(|base| root.join(base).join(rel))
        .find(|path| path.exists())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credentials_loader_path() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("cert"), "dummy").unwrap();

        let loader = CredentialsLoader::from_dir(dir.path());
        assert_eq!(loader.path("cert"), Some(dir.path().join("cert")));
        assert_eq!(loader.path("missing"), None);
    }

    #[test]
    fn test_credentials_loader_find_import_credential_patterns() {
        const PATTERNS: &[&str] = &[
            "varlink-httpd.api-keys",
            "ssh.authorized_keys.root",
            "varlink-httpd.api-keys.*",
        ];
        let dir = tempfile::tempdir().unwrap();
        for name in [
            "varlink-httpd.api-keys.b",
            "varlink-httpd.api-keys.a",
            "varlink-httpd.api-keys",
            "varlink-httpd.api-keys-backup",
            "ssh.authorized_keys.root",
            "api-keys",
            "unrelated",
        ] {
            std::fs::write(dir.path().join(name), "dummy").unwrap();
        }

        let loader = CredentialsLoader::from_dir(dir.path());
        assert_eq!(
            loader.find(PATTERNS).unwrap(),
            vec![
                dir.path().join("ssh.authorized_keys.root"),
                dir.path().join("varlink-httpd.api-keys"),
                dir.path().join("varlink-httpd.api-keys.a"),
                dir.path().join("varlink-httpd.api-keys.b"),
            ],
            "exact ids and trailing-* globs, sorted; no look-alikes"
        );
        assert_eq!(
            loader
                .find_with(PATTERNS, |id| format!("{id} (unused)"))
                .unwrap(),
            vec![
                "ssh.authorized_keys.root (unused)",
                "varlink-httpd.api-keys (unused)",
                "varlink-httpd.api-keys.a (unused)",
                "varlink-httpd.api-keys.b (unused)",
            ],
            "the closure sees the ids, in the same order as find()"
        );
        assert!(
            loader.find(&["nomatch.*"]).unwrap().is_empty(),
            "no match is not an error"
        );

        let missing = CredentialsLoader::from_dir(dir.path().join("nonexistent"));
        assert!(
            missing.find(PATTERNS).unwrap().is_empty(),
            "a missing credentials directory means no credentials"
        );
        assert!(
            missing
                .find_with(PATTERNS, str::to_string)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn test_find_config_precedence() {
        let root = tempfile::tempdir().unwrap();
        let rel = "varlink-httpd/api-keys";
        let write = |base: &str| {
            let p = root.path().join(base).join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(&p, base).unwrap();
            p
        };

        assert_eq!(find_config(rel, root.path()), None);

        let usr = write("usr/lib");
        assert_eq!(find_config(rel, root.path()), Some(usr));
        let run = write("run");
        assert_eq!(find_config(rel, root.path()), Some(run));
        let etc = write("etc");
        assert_eq!(find_config(rel, root.path()), Some(etc));
    }
}
