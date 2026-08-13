// SPDX-License-Identifier: LGPL-2.1-or-later

//! Locating service configuration the systemd way: credentials passed via
//! `$CREDENTIALS_DIRECTORY` (see systemd.exec(5) / systemd.system-credentials(7)),
//! and config files in the `/etc` > `/run` > `/usr/lib` precedence hierarchy.

use std::path::{Path, PathBuf};

/// Mirrors libsystemd's `CredentialsLoader`: one file per credential,
/// filename = credential id.
pub struct CredentialsLoader {
    dir: PathBuf,
}

impl CredentialsLoader {
    #[must_use]
    pub fn path_from_env() -> Option<PathBuf> {
        std::env::var_os("CREDENTIALS_DIRECTORY").map(PathBuf::from)
    }

    /// Loader rooted at an explicit directory (mainly for tests).
    pub fn from_dir(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// Where credential `id` would live, existing or not, for callers that
    /// watch the path so a credential appearing later is picked up.
    #[must_use]
    pub fn candidate(&self, id: &str) -> PathBuf {
        self.dir.join(id)
    }

    /// Path of credential `id`, if the file exists.
    #[must_use]
    pub fn path(&self, id: &str) -> Option<PathBuf> {
        let path = self.candidate(id);
        path.exists().then_some(path)
    }
}

/// Every location `rel` could live in, highest precedence first, existing
/// or not, for callers that watch all of them so a higher-precedence file
/// appearing later takes over.
#[must_use]
pub fn config_candidates(rel: &str, root: &Path) -> Vec<PathBuf> {
    ["etc", "run", "usr/lib"]
        .into_iter()
        .map(|base| root.join(base).join(rel))
        .collect()
}

/// Highest-precedence existing config file for `rel`, following the systemd
/// hierarchy (`/etc` over `/run` over `/usr/lib`). `root` is `/` in
/// production, a tempdir in tests.
#[must_use]
pub fn find_config(rel: &str, root: &Path) -> Option<PathBuf> {
    config_candidates(rel, root)
        .into_iter()
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
    fn test_credentials_loader_candidate_ignores_existence() {
        let dir = tempfile::tempdir().unwrap();
        let loader = CredentialsLoader::from_dir(dir.path());
        assert_eq!(loader.candidate("missing"), dir.path().join("missing"));
        assert_eq!(loader.path("missing"), None);
    }

    #[test]
    fn test_config_candidates_order_ignores_existence() {
        let root = std::path::Path::new("/fake-root");
        assert_eq!(
            config_candidates("varlink-httpd/api-keys", root),
            vec![
                root.join("etc/varlink-httpd/api-keys"),
                root.join("run/varlink-httpd/api-keys"),
                root.join("usr/lib/varlink-httpd/api-keys"),
            ]
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
