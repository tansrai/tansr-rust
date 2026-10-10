use crate::api::{Error, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub const PROTOCOL: &str = "sdk2-ext-v1";
pub(crate) const CONTROL_BYTES: usize = 262_144;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Scope {
    pub application_scope_id: String,
    pub end_user_id: String,
    pub authorization_revision: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Platform {
    pub platform: String,
    pub arch: String,
    pub language: String,
    pub runtime_version: String,
    pub adapter_version: String,
}
impl Platform {
    pub fn current() -> Self {
        Self {
            platform: if cfg!(target_os = "macos") {
                "macos"
            } else {
                std::env::consts::OS
            }
            .into(),
            arch: match std::env::consts::ARCH {
                "x86_64" => "amd64",
                "aarch64" => "arm64",
                x => x,
            }
            .into(),
            language: "rust".into(),
            runtime_version: "native".into(),
            adapter_version: format!("rust-executor/{}", env!("CARGO_PKG_VERSION")),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Workspace {
    pub workspace_id: String,
    pub revision: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolDefinition {
    pub name: String,
    pub definition_digest: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Interpreter {
    pub id: String,
    pub revision: String,
    pub host_shell: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Registration {
    pub protocol: String,
    pub executor_id: String,
    pub platform: Platform,
    pub workspaces: Vec<Workspace>,
    pub operations: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolDefinition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interpreter: Option<Interpreter>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Connection {
    pub protocol: String,
    pub executor_id: String,
    pub connection_id: String,
    pub connection_revision: String,
    pub expires_at: String,
    pub heartbeat_after_ms: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Target {
    pub executor_id: String,
    pub connection_id: String,
    pub connection_revision: String,
    pub workspace_id: String,
    pub workspace_revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interpreter: Option<Interpreter>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Binding {
    pub binding_id: String,
    pub revision: String,
    pub target: Target,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EffectiveTool {
    pub name: String,
    pub execution_kind: String,
    pub available: bool,
    pub unavailable_reason: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Capabilities {
    pub protocol: String,
    pub session_id: String,
    pub platform: Option<Platform>,
    pub capability_revision: String,
    pub effective_tools: Vec<EffectiveTool>,
    pub binding: Option<Binding>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Resource {
    pub operation: String,
    pub args: Value,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Operation {
    pub protocol: String,
    pub operation_id: String,
    pub session_id: String,
    pub scope: Scope,
    pub binding: Binding,
    pub tool_name: String,
    pub request: Resource,
    pub digest: String,
    pub expires_at: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Batch {
    pub protocol: String,
    pub executor_id: String,
    pub connection_id: String,
    pub operations: Vec<Operation>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Receipt {
    pub protocol: String,
    pub executor_id: String,
    pub connection_id: String,
    pub operation_id: String,
    pub digest: String,
    pub status: String,
    pub result: Option<Resource>,
    pub error_code: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Status {
    pub protocol: String,
    pub operation: Operation,
    pub status: String,
    pub receipt: Option<Receipt>,
}
#[derive(Clone)]
pub struct ToolContext {
    pub cancellation: CancellationToken,
    /// Available only after negotiation and confirmation of this operation's
    /// authorized empty output window. Runner owns the final seal/ACK barrier.
    pub output: Option<super::OutputWriter>,
}
#[derive(Debug, thiserror::Error)]
pub enum ToolError {
    /// The host guarantees no effect occurred; ordinary errors must use Unknown.
    #[error("tool rejected: {0}")]
    Rejected(String),
    #[error("tool outcome unknown: {0}")]
    Unknown(String),
}
#[async_trait]
pub trait ToolHandler: Send + Sync {
    /// Reserved storage profiles require the original execution envelope.
    fn is_memory_publication_host(&self) -> bool {
        false
    }
    /// Explicit opt-in for the v1 storage profile, independent of legacy storage.
    fn is_terminal_persistence_host(&self) -> bool {
        false
    }
    /// Additive dispatch hook; existing business handlers retain invoke behavior.
    async fn invoke_operation(
        &self,
        context: ToolContext,
        _operation: &Operation,
        args: Value,
    ) -> std::result::Result<Value, ToolError> {
        self.invoke(context, args).await
    }
    async fn invoke(
        &self,
        context: ToolContext,
        args: Value,
    ) -> std::result::Result<Value, ToolError>;
}
#[derive(Clone)]
pub struct Tool {
    pub definition_digest: String,
    pub handler: Arc<dyn ToolHandler>,
}
impl Tool {
    pub fn new(definition_digest: String, handler: impl ToolHandler + 'static) -> Self {
        Self {
            definition_digest,
            handler: Arc::new(handler),
        }
    }
}
#[async_trait]
pub trait Authorizer: Send + Sync {
    async fn authorize(&self, operation: &Operation) -> Result<()>;
}
pub enum ClaimResult {
    Claimed,
    Pending,
    Receipt(Box<Receipt>),
}
#[async_trait]
pub trait Journal: Send + Sync {
    /// True only when durable claims and receipts (including result bodies) are encrypted.
    fn encrypted_at_rest(&self) -> bool {
        false
    }
    async fn claim(&self, operation: &Operation) -> Result<ClaimResult>;
    async fn complete(&self, operation: &Operation, receipt: &Receipt) -> Result<()>;
}
pub(crate) fn invalid(message: &str) -> Error {
    Error::Contract(format!("executor: {message}"))
}
pub(crate) fn value<T: Serialize>(v: &T) -> Result<Value> {
    Ok(serde_json::to_value(v)?)
}
pub(crate) fn validate<T: Serialize>(name: &str, v: &T) -> Result<()> {
    crate::api::validate_wire(PROTOCOL, name, &value(v)?)
}
pub(crate) fn expiry(text: &str) -> Result<std::time::SystemTime> {
    let t = time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)
        .map_err(|_| invalid("invalid expiry"))?;
    std::time::UNIX_EPOCH
        .checked_add(std::time::Duration::from_nanos(
            u64::try_from(t.unix_timestamp_nanos()).map_err(|_| invalid("expiry range"))?,
        ))
        .ok_or_else(|| invalid("expiry range"))
}
pub(crate) fn live(c: &Connection) -> Result<()> {
    validate("ExecutorConnection", c)?;
    if expiry(&c.expires_at)? <= std::time::SystemTime::now() {
        return Err(invalid("connection lease expired"));
    }
    Ok(())
}
pub(crate) fn receipt_for(
    op: &Operation,
    status: &str,
    error: Option<&str>,
    result: Option<Resource>,
) -> Receipt {
    Receipt {
        protocol: PROTOCOL.into(),
        executor_id: op.binding.target.executor_id.clone(),
        connection_id: op.binding.target.connection_id.clone(),
        operation_id: op.operation_id.clone(),
        digest: op.digest.clone(),
        status: status.into(),
        result,
        error_code: error.map(str::to_string),
    }
}
