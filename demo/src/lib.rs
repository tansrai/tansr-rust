//! CLI presentation and configuration shared by the three examples.
//! Protocol, execution and persistence remain in the public SDK.
//!
//! Embedded handler output uses the context negotiated by Runner. Capture is
//! not an ACK; Runner owns the final seal. This function compiles without
//! creating a runtime or executing an operation:
//!
//! ```no_run
//! use tansr_sdk::executor::{ToolContext, ToolError};
//! async fn progress(context: &ToolContext) -> Result<(), ToolError> {
//!     if context.cancellation.is_cancelled() {
//!         return Err(ToolError::Rejected("cancelled_before_work".into()));
//!     }
//!     if let Some(output) = &context.output {
//!         output.capture("stdout", b"lookup started\n").await
//!             .map_err(|_| ToolError::Unknown("output_capture_unknown".into()))?;
//!     }
//!     Ok(())
//! }
//! ```
//!
//! A material request received earlier retains its original deadline, including
//! time spent on access checks. Persist the returned response before calling
//! `submit_materials`; a `received` receipt is not `core-consumed`:
//!
//! ```no_run
//! use std::time::SystemTime;
//! use tansr_sdk::{Result, archive::{
//!     ArchiveClient, ArchiveStore, MaterialRequest, MaterialResponse, RequestIdentity,
//! }};
//! async fn prepare_requested_materials(
//!     client: &ArchiveClient, store: &dyn ArchiveStore,
//!     request: &MaterialRequest, identity: RequestIdentity,
//!     original_deadline: SystemTime,
//! ) -> Result<MaterialResponse> {
//!     client.prepare_materials_before(store, request, identity, original_deadline).await
//! }
//! ```

use rand::RngCore;
use std::{
    collections::BTreeMap,
    io::Read,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tansr_sdk::api::{ApiClient, ClientBuilder, Error, Result};
use tansr_sdk::session::WriteOptions;
use tokio_util::sync::CancellationToken;

/// Small explicit parser. Unknown flags and duplicate values are rejected.
pub struct Args(BTreeMap<String, String>);

impl Args {
    pub fn parse() -> Result<Self> {
        Self::from_values(std::env::args().skip(1))
    }

    pub fn from_values(values: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut parsed = BTreeMap::new();
        let mut values = values.into_iter();
        while let Some(flag) = values.next() {
            if !flag.starts_with("--") || flag.len() == 2 {
                return Err(Error::InvalidInput(
                    "use --name value options; positional arguments are not accepted".into(),
                ));
            }
            let value = if matches!(flag.as_str(), "--help" | "--require-output") {
                "true".into()
            } else {
                values
                    .next()
                    .filter(|v| !v.starts_with("--"))
                    .ok_or_else(|| Error::InvalidInput("option requires a value".into()))?
            };
            if parsed.insert(flag, value).is_some() {
                return Err(Error::InvalidInput(
                    "an option was supplied more than once".into(),
                ));
            }
        }
        Ok(Self(parsed))
    }

    pub fn help(&mut self) -> bool {
        self.0.remove("--help").is_some()
    }
    pub fn take(&mut self, name: &str) -> Option<String> {
        self.0.remove(name)
    }
    pub fn value(&mut self, name: &str, default: &str) -> String {
        self.take(name).unwrap_or_else(|| default.into())
    }
    pub fn required(&mut self, name: &str) -> Result<String> {
        self.take(name)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::InvalidInput(format!("required option {name} is missing")))
    }
    pub fn finish(self) -> Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(Error::InvalidInput("unknown option; see --help".into()))
        }
    }
}

pub fn base_url(args: &mut Args) -> String {
    args.take("--base")
        .or_else(|| std::env::var("TANSR_BASE_URL").ok())
        .unwrap_or_else(|| "http://127.0.0.1:8787".into())
}

pub fn family(args: &mut Args) -> Result<String> {
    let value = args
        .take("--family")
        .or_else(|| std::env::var("TANSR_SESSION_FAMILY").ok())
        .unwrap_or_else(|| "sdk1".into());
    if value != "sdk1" && value != "sdk2-offload-v1" {
        return Err(Error::InvalidInput(
            "family must be sdk1 or sdk2-offload-v1".into(),
        ));
    }
    Ok(value)
}

pub fn env_required(args: &mut Args, flag: &str, name: &str) -> Result<String> {
    args.take(flag)
        .or_else(|| std::env::var(name).ok())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Error::InvalidInput(format!("set {flag} or {name}")))
}

/// A file is reopened before every request. Renew only the same principal.
/// Switching users requires new SDK/session/executor/store instances.
pub fn client(base: &str, family: &str) -> Result<ApiClient> {
    let path = std::env::var_os("TANSR_TOKEN_FILE")
        .map(std::path::PathBuf::from)
        .ok_or_else(|| {
            Error::InvalidInput("set TANSR_TOKEN_FILE to a short-lived Serve token file".into())
        })?;
    let provider = Arc::new(move || {
        let path = path.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || read_token(&path))
                .await
                .map_err(|_| Error::Io("token reader task failed".into()))?
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<String>> + Send>>
    });
    ClientBuilder::new(base)
        .session_family(family)
        .token_provider(provider)
        .build()
}

fn read_bounded(path: &Path, max: usize) -> Result<Vec<u8>> {
    let file =
        std::fs::File::open(path).map_err(|_| Error::Io("cannot open credential file".into()))?;
    let mut bytes = Vec::new();
    file.take(max as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Io("cannot read credential file".into()))?;
    if bytes.len() > max {
        return Err(Error::InvalidInput(
            "credential file exceeds allowed size".into(),
        ));
    }
    Ok(bytes)
}

fn read_token(path: &Path) -> Result<String> {
    let bytes = read_bounded(path, 8192)?;
    let token = std::str::from_utf8(&bytes)
        .map_err(|_| Error::InvalidInput("token file is not UTF-8".into()))?
        .trim_end_matches(['\r', '\n']);
    if token.is_empty() || !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(Error::InvalidInput(
            "token must be a nonempty single-line ASCII value".into(),
        ));
    }
    Ok(token.to_owned())
}

/// Load a 32-byte key encoded as 64 hexadecimal digits. No key is generated,
/// printed, stored with the archive or silently replaced on decryption errors.
pub fn archive_key() -> Result<[u8; 32]> {
    let path = std::env::var_os("TANSR_ARCHIVE_KEY_FILE").ok_or_else(|| {
        Error::InvalidInput("set TANSR_ARCHIVE_KEY_FILE to a 64-digit hex key file".into())
    })?;
    archive_key_from_file(Path::new(&path))
}

/// Read an explicitly selected host key file, without changing process environment.
pub fn archive_key_from_file(path: &Path) -> Result<[u8; 32]> {
    let bytes = read_bounded(path, 128)?;
    let hex = std::str::from_utf8(&bytes)
        .map_err(|_| Error::InvalidInput("invalid archive key file".into()))?
        .trim_end_matches(['\r', '\n']);
    decode_key(hex)
}

fn decode_key(hex: &str) -> Result<[u8; 32]> {
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::InvalidInput(
            "archive key must contain exactly 64 hexadecimal digits".into(),
        ));
    }
    let mut key = [0; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
            .map_err(|_| Error::InvalidInput("invalid archive key file".into()))?;
    }
    Ok(key)
}

pub fn request_id() -> String {
    let mut bytes = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let mut id = String::from("rust-");
    use std::fmt::Write;
    for byte in bytes {
        write!(&mut id, "{byte:02x}").expect("writing to String cannot fail");
    }
    id
}

pub fn write_options(cancel: &CancellationToken) -> WriteOptions {
    let id = request_id();
    println!("request: {id}");
    WriteOptions {
        idempotency_key: Some(id),
        deadline: Some(SystemTime::now() + Duration::from_secs(30)),
        cancellation: cancel.clone(),
    }
}

/// Remove control and directional override characters from untrusted terminal
/// content. This is presentation only: protocol payloads remain untouched.
pub fn safe_text(value: &str) -> String {
    value.chars().filter(|c| {
        (*c == '\n' || *c == '\t' || !c.is_control())
            && !matches!(*c, '\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
    }).collect()
}

/// Never print arbitrary transport text, request bodies, tool arguments or
/// server messages: they may contain credentials or user data.
pub fn describe(error: &Error) -> String {
    match error {
        Error::Api(remote) => format!(
            "Serve code={} retryAction={} status={}",
            safe_text(remote.code.as_str()),
            safe_text(remote.retry_action.as_str()),
            remote.status
        ),
        Error::Cancelled => {
            "local wait cancelled; Serve completion or interruption is not confirmed".into()
        }
        Error::Transport(_) | Error::Unknown(_) => {
            "request outcome is unknown; query the original session/operation before retrying"
                .into()
        }
        Error::OutputIncomplete { .. } => {
            "business result is durably recorded, but output is not confirmed; preserve the journal and reconcile the original receipt; do not rerun the handler"
                .into()
        }
        Error::InvalidInput(message) => format!("invalid input: {}", safe_text(message)),
        Error::Contract(_) => {
            "contract validation failed; no fallback or automatic retry was performed".into()
        }
        Error::Domain { family, status, .. } => format!(
            "Serve domain error: family={} status={status}; original response withheld",
            safe_text(family)
        ),
        Error::Io(_) => {
            "local file operation failed; preserve the existing file and check access/storage"
                .into()
        }
    }
}

pub fn report(program: &str, result: Result<()>) -> std::process::ExitCode {
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{program}: {}", describe(&error));
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn terminal_controls_cannot_inject_escape_or_reorder_text() {
        assert_eq!(safe_text("a\x1b[2J\r\u{0085}\u{202e}b\n\t"), "a[2Jb\n\t");
    }
    #[test]
    fn errors_do_not_expose_transport_or_contract_secrets() {
        let secret = "fake-token-secret";
        assert!(!describe(&Error::Transport(secret.into())).contains(secret));
        assert!(!describe(&Error::Contract(secret.into())).contains(secret));
        assert!(!describe(&Error::Unknown(secret.into())).contains(secret));
        assert!(!describe(&Error::Io(secret.into())).contains(secret));
        let incomplete = describe(&Error::OutputIncomplete {
            operation_id: secret.into(),
            receipt_id: secret.into(),
        });
        assert!(!incomplete.contains(secret));
        assert!(incomplete.contains("durably recorded"));
        assert!(incomplete.contains("do not rerun"));
        assert!(
            !describe(&Error::Domain {
                family: "archive-sync-v1".into(),
                status: 403,
                body: serde_json::json!({"message":secret,"token":secret}),
            })
            .contains(secret)
        );
        let server: tansr_sdk::api::ApiError = serde_json::from_value(serde_json::json!({
            "code":"forbidden", "retryAction":"none", "message":secret,
            "detail":{"token":secret}, "traceId":secret, "requestId":secret
        }))
        .unwrap();
        assert!(!describe(&Error::Api(Box::new(server))).contains(secret));
        assert!(describe(&Error::Cancelled).contains("not confirmed"));
    }
    #[test]
    fn key_is_exact_hex_not_arbitrary_json_or_passphrase() {
        assert_eq!(decode_key(&"ab".repeat(32)).unwrap(), [0xab; 32]);
        assert!(decode_key("password").is_err());
        assert!(decode_key(&"é".repeat(32)).is_err());
    }
    #[test]
    fn arguments_reject_duplicate_unknown_and_missing_values() {
        assert!(Args::from_values(["--base", "a", "--base", "b"].map(String::from)).is_err());
        assert!(Args::from_values(["--base"].map(String::from)).is_err());
        assert!(
            Args::from_values(["--secret", "hidden"].map(String::from))
                .unwrap()
                .finish()
                .is_err()
        );
    }

    #[test]
    fn required_output_is_an_explicit_boolean_flag() {
        let mut args = Args::from_values(["--require-output"].map(String::from)).unwrap();
        assert_eq!(args.take("--require-output").as_deref(), Some("true"));
        args.finish().unwrap();
    }
}
