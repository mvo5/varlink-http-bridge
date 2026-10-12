// SPDX-License-Identifier: LGPL-2.1-or-later

//! Who the bridge authenticated, as reported by whichever authenticator
//! accepted the request.

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
