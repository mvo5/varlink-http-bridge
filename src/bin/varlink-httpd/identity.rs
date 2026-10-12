// SPDX-License-Identifier: LGPL-2.1-or-later

//! Who the bridge authenticated, as reported by whichever authenticator
//! accepted the request, and the table that maps a connection's tag back
//! to them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::socket_tag::ConnectionTag;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Identity {
    /// `api-key`, `ssh`, `mtls`, or `anonymous` when auth was disabled.
    pub(crate) mechanism: &'static str,
    /// Named as the mechanism names them: an API key's label, an SSH key
    /// fingerprint, a certificate subject. `None` only for anonymous.
    pub(crate) caller: Option<String>,
}

impl Identity {
    pub(crate) fn new(mechanism: &'static str, caller: impl Into<String>) -> Self {
        Self {
            mechanism,
            caller: Some(caller.into()),
        }
    }

    /// A real identity rather than the absence of one: "nobody
    /// authenticated" and "authenticated, nothing matched" must stay
    /// distinguishable.
    pub(crate) fn anonymous() -> Self {
        Self {
            mechanism: "anonymous",
            caller: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mechanism_and_caller_both_distinguish() {
        assert_ne!(Identity::anonymous(), Identity::new("api-key", "ci"));
        assert_ne!(
            Identity::new("api-key", "alice"),
            Identity::new("ssh", "alice")
        );
        assert_eq!(Identity::anonymous().caller, None);
    }
}

/// Tags currently bound to a live connection, process-wide, because the
/// resolver answers for every connection.
#[derive(Clone, Default)]
pub(crate) struct IdentityTable {
    inner: Arc<Mutex<HashMap<String, Identity>>>,
}

impl IdentityTable {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Make `tag` resolve to `identity` until the returned lease is dropped.
    pub(crate) fn bind(&self, tag: ConnectionTag, identity: Identity) -> Lease {
        self.lock().insert(tag.reference().to_string(), identity);
        Lease {
            tag,
            table: self.clone(),
        }
    }

    #[allow(dead_code)] // the resolver, next
    pub(crate) fn resolve(&self, reference: &str) -> Option<Identity> {
        self.lock().get(reference).cloned()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }

    // a poisoned lock still holds a consistent map; every write is one insert or remove
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Identity>> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Keeps a tag resolvable for exactly as long as its connection; dropping
/// it is what stops a released name from resolving if someone rebinds it.
pub(crate) struct Lease {
    tag: ConnectionTag,
    table: IdentityTable,
}

impl Lease {
    pub(crate) fn reference(&self) -> &str {
        self.tag.reference()
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.table.lock().remove(self.tag.reference());
    }
}

#[cfg(test)]
mod table_tests {
    use super::*;

    #[test]
    fn bound_tag_resolves_until_the_lease_drops() {
        let table = IdentityTable::new();
        let tag = ConnectionTag::generate().unwrap();
        let reference = tag.reference().to_string();

        let lease = table.bind(tag, Identity::new("api-key", "ci"));
        assert_eq!(
            table.resolve(&reference),
            Some(Identity::new("api-key", "ci"))
        );
        assert_eq!(table.len(), 1);

        drop(lease);
        assert_eq!(table.resolve(&reference), None);
        assert_eq!(table.len(), 0);
    }
}
