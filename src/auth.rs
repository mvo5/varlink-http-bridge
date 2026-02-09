use axum::{
    extract::Request,
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use log::{debug, warn};
use serde_json::json;
use std::ffi::{c_char, c_int, c_void, CString};
use std::ptr;
use std::sync::Arc;

/// Abstract interface for authenticating HTTP requests.
///
/// Implementations are called from a blocking context (`spawn_blocking`)
/// so they may perform blocking I/O (PAM, LDAP, file reads, etc.).
pub trait Authenticator: Send + Sync + 'static {
    fn authenticate(&self, username: &str, password: &str) -> bool;
}

/// Authenticator that verifies credentials via PAM.
///
/// Requires the process to have sufficient privileges to call into PAM
/// (typically root for authenticating system users).
pub struct PamAuthenticator {
    service: String,
}

impl PamAuthenticator {
    pub fn new(service: impl Into<String>) -> Self {
        Self {
            service: service.into(),
        }
    }
}

impl Authenticator for PamAuthenticator {
    fn authenticate(&self, username: &str, password: &str) -> bool {
        pam_check(&self.service, username, password)
    }
}

/// Authenticator that accepts any credentials. For development/testing only.
pub struct InsecureAuthenticator;

impl Authenticator for InsecureAuthenticator {
    fn authenticate(&self, _username: &str, _password: &str) -> bool {
        warn!("insecure authenticator: accepting any credentials");
        true
    }
}

// --- Minimal PAM FFI (avoids heavy bindgen/pam-sys dependency) ---

const PAM_SUCCESS: c_int = 0;
const PAM_PROMPT_ECHO_OFF: c_int = 1;

#[repr(C)]
struct PamMessage {
    msg_style: c_int,
    msg: *const c_char,
}

#[repr(C)]
struct PamResponse {
    resp: *mut c_char,
    resp_retcode: c_int,
}

#[repr(C)]
struct PamConv {
    conv: unsafe extern "C" fn(
        c_int,
        *mut *const PamMessage,
        *mut *mut PamResponse,
        *mut c_void,
    ) -> c_int,
    appdata_ptr: *mut c_void,
}

#[link(name = "pam")]
unsafe extern "C" {
    fn pam_start(
        service_name: *const c_char,
        user: *const c_char,
        pam_conversation: *const PamConv,
        pamh: *mut *mut c_void,
    ) -> c_int;
    fn pam_authenticate(pamh: *mut c_void, flags: c_int) -> c_int;
    fn pam_end(pamh: *mut c_void, pam_status: c_int) -> c_int;
}

unsafe extern "C" {
    fn calloc(nmemb: usize, size: usize) -> *mut c_void;
    fn strdup(s: *const c_char) -> *mut c_char;
}

/// PAM conversation callback. Responds to password prompts with the
/// password stored in `appdata_ptr` (a C string).
unsafe extern "C" fn pam_conversation(
    num_msg: c_int,
    msg: *mut *const PamMessage,
    resp: *mut *mut PamResponse,
    appdata_ptr: *mut c_void,
) -> c_int {
    unsafe {
        let n = num_msg as usize;
        let responses = calloc(n, size_of::<PamResponse>()) as *mut PamResponse;
        if responses.is_null() {
            return 5; // PAM_BUF_ERR
        }
        for i in 0..n {
            let m = *msg.add(i);
            if (*m).msg_style == PAM_PROMPT_ECHO_OFF {
                (*responses.add(i)).resp = strdup(appdata_ptr as *const c_char);
            }
        }
        *resp = responses;
        PAM_SUCCESS
    }
}

fn pam_check(service: &str, username: &str, password: &str) -> bool {
    let Ok(c_service) = CString::new(service) else {
        return false;
    };
    let Ok(c_user) = CString::new(username) else {
        return false;
    };
    let Ok(c_pass) = CString::new(password) else {
        return false;
    };

    let conv = PamConv {
        conv: pam_conversation,
        appdata_ptr: c_pass.as_ptr() as *mut c_void,
    };

    let mut pamh: *mut c_void = ptr::null_mut();
    unsafe {
        let ret = pam_start(c_service.as_ptr(), c_user.as_ptr(), &conv, &mut pamh);
        if ret != PAM_SUCCESS {
            warn!("pam_start failed with code {ret}");
            return false;
        }
        let ret = pam_authenticate(pamh, 0);
        pam_end(pamh, ret);
        ret == PAM_SUCCESS
    }
}

/// Axum middleware that enforces HTTP Basic Authentication.
///
/// The `/health` endpoint is excluded from authentication.
pub async fn basic_auth_middleware(
    request: Request,
    next: Next,
    authenticator: Arc<dyn Authenticator>,
) -> Response {
    if request.uri().path() == "/health" {
        return next.run(request).await;
    }

    let Some((username, password)) = extract_basic_credentials(&request) else {
        return unauthorized();
    };

    let auth = authenticator.clone();
    let ok = tokio::task::spawn_blocking(move || auth.authenticate(&username, &password))
        .await
        .unwrap_or(false);

    if !ok {
        debug!("basic auth failed");
        return unauthorized();
    }

    next.run(request).await
}

fn extract_basic_credentials(req: &Request) -> Option<(String, String)> {
    let value = req.headers().get(header::AUTHORIZATION)?.to_str().ok()?;
    let encoded = value.strip_prefix("Basic ")?;
    let decoded = STANDARD.decode(encoded).ok()?;
    let text = String::from_utf8(decoded).ok()?;
    let (user, pass) = text.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(
            header::WWW_AUTHENTICATE,
            "Basic realm=\"varlink-http-bridge\"",
        )],
        axum::Json(json!({"error": "unauthorized"})),
    )
        .into_response()
}
