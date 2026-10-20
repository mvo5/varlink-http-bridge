// SPDX-License-Identifier: LGPL-2.1-or-later

pub mod mptcp;

#[cfg(feature = "sshauth")]
/// Namespace prefix for SSH-based authentication tokens, analogous to
/// `ssh-keygen -Y sign -n <namespace>`.  Binds signatures to this application
/// so they cannot be replayed against other services.
pub const SSHAUTH_MAGIC_PREFIX: [u8; 8] = *b"vhbridge";

#[cfg(feature = "sshauth")]
/// HTTP header carrying the random nonce that is included in the signed
/// token payload to prevent replay attacks.
pub const SSHAUTH_NONCE_HEADER: &str = "x-auth-nonce";
