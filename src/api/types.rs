use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, pin::Pin, sync::Arc, time::SystemTime};
use tokio_util::sync::CancellationToken;

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("contract violation: {0}")]
    Contract(String),
    #[error("transport failure; request outcome may be unknown: {0}")]
    Transport(String),
    #[error("server error: {0}")]
    Api(Box<ApiError>),
    #[error("local operation cancelled; server outcome is not implied")]
    Cancelled,
    #[error("storage failure: {0}")]
    Io(String),
    #[error("outcome unknown: {0}")]
    Unknown(String),
    /// 业务结果已耐久保存；此错误只表示观察输出未获确认，不允许重新执行业务。
    #[error(
        "tool result is durably recorded, but output confirmation is incomplete; reconcile the original receipt"
    )]
    OutputIncomplete {
        operation_id: String,
        receipt_id: String,
    },
    #[error("domain error from {family} ({status})")]
    Domain {
        family: String,
        status: u16,
        body: Value,
    },
}
impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.kind().to_string())
    }
}
impl From<serde_json::Error> for Error {
    fn from(_: serde_json::Error) -> Self {
        Self::Contract("invalid JSON or value shape".into())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiError {
    pub code: super::ErrorCode,
    pub retry_action: super::RetryAction,
    pub message: String,
    #[serde(default)]
    pub detail: Value,
    #[serde(default)]
    pub retry_after_ms: Option<u64>,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub trace_id: Option<String>,
    #[serde(skip)]
    pub status: u16,
    #[serde(skip)]
    pub(crate) replay: Option<ReplayIdentity>,
}
#[derive(Clone, Debug)]
pub(crate) struct ReplayIdentity {
    pub client: Arc<()>,
    pub operation: String,
    pub digest: [u8; 32],
}
impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.code, self.status)
    }
}
#[derive(Clone, Debug, Default)]
pub struct CallOptions {
    pub params: BTreeMap<String, String>,
    pub query: BTreeMap<String, String>,
    pub body: Option<Value>,
    pub raw_body: Option<Vec<u8>>,
    pub closure_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub if_match: Option<String>,
    pub deadline: Option<SystemTime>,
    pub last_event_id: Option<String>,
    pub cancellation: CancellationToken,
    pub max_response_bytes: Option<usize>,
}
#[derive(Clone, Debug)]
pub struct ResponseMeta {
    pub manifest_revision: u64,
    pub schema_hash: String,
    pub domain: String,
    pub etag: Option<String>,
    pub closure_id: Option<String>,
    pub content_type: String,
    pub retry_after_ms: Option<u64>,
}
#[derive(Clone, Debug)]
pub struct ApiResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub meta: ResponseMeta,
}
impl ApiResponse {
    pub fn json(&self) -> Result<Value> {
        crate::canonical::parse_json(&self.body, self.body.len().max(1))
    }
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EventEnvelope {
    pub contract: String,
    pub event_id: Option<String>,
    pub domain: String,
    pub r#type: Option<String>,
    pub cursor_set: Value,
    pub terminal_status: Option<String>,
    pub raw: Value,
}
impl<'de> Deserialize<'de> for EventEnvelope {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        super::schema::validate("EventEnvelope", &value).map_err(serde::de::Error::custom)?;
        // Validation preserves required-nullable fields and the exact seven-key
        // contract even when a consumer deserializes this public type directly.
        let optional = |key: &str| value[key].as_str().map(str::to_owned);
        Ok(Self {
            contract: value["contract"]
                .as_str()
                .expect("validated contract")
                .into(),
            event_id: optional("eventId"),
            domain: value["domain"].as_str().expect("validated domain").into(),
            r#type: optional("type"),
            cursor_set: value["cursorSet"].clone(),
            terminal_status: optional("terminalStatus"),
            raw: value["raw"].clone(),
        })
    }
}
pub type EventStream = Pin<Box<dyn futures_core::Stream<Item = Result<EventEnvelope>> + Send>>;
