//! Durable storage for the frozen terminal MemoryPublication profile.
//! This module transports opaque UTF-8 publication bytes; Serve owns all memory
//! selection, governance, deletion policy and business decisions.
mod host;
mod store;
use crate::{
    Error, Result,
    executor::{Binding, Scope},
};
use async_trait::async_trait;
pub use host::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
pub use store::*;

pub const TOOL_NAME: &str = "TansrTerminalMemoryPublication";
pub const TOOL_DIGEST: &str = "8532a582d40d2d8993a80db59412a89671670ed7f994eaf9bf972bd544a111b0";
pub const CONTRACT: &str = "terminal-services-v1";
pub const MAX_BODY: usize = 4_194_304;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Identity {
    pub application_scope_id: String,
    pub end_user_id: String,
    pub source_id: String,
    pub source_generation: String,
    pub domain_key: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Owner {
    pub scope: Scope,
    pub session_id: String,
    pub binding: Binding,
}
impl Owner {
    pub fn from_operation(op: &crate::executor::Operation) -> Self {
        Self {
            scope: op.scope.clone(),
            session_id: op.session_id.clone(),
            binding: op.binding.clone(),
        }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        crate::api::validate_wire("sdk2-ext-v1", "Scope", &serde_json::to_value(&self.scope)?)?;
        crate::api::validate_wire("sdk2-ext-v1", "LegacyId", &json!(self.session_id))?;
        crate::api::validate_wire(
            "sdk2-ext-v1",
            "ExecutionBinding",
            &serde_json::to_value(&self.binding)?,
        )
    }
}
/// Reads current authenticated host state, including the live binding and authorization
/// revision. It must not reenter the store. Restored strings are not authority.
pub type ReadContext = Arc<dyn Fn() -> Result<Owner> + Send + Sync>;
/// Optional read-only reconciliation under a new connection. The host must
/// establish old authority revocation and current access before returning true.
pub type AuthorizeRecovery = Arc<dyn Fn(&Identity, &Owner, &Owner, &str) -> bool + Send + Sync>;
#[async_trait]
pub trait MemoryPublicationStore: Send + Sync {
    fn identity(&self) -> &Identity;
    fn atomic_durable_publication(&self) -> bool;
    fn encrypted_at_rest(&self) -> bool {
        false
    }
    /// Request/response use the unchanged vendored MemoryPublication schemas.
    /// Successful mutations must be durable before returning. Unknown outcomes
    /// must remain errors and never become permission to repeat a business action.
    async fn execute(&self, request: Value, owner: Owner) -> Result<Value>;
}
fn validate_request(request: &Value, identity: &Identity) -> Result<()> {
    crate::canonical::encode_limited(request, 32768)?;
    crate::api::validate_wire(CONTRACT, "MemoryPublicationRequest", request)?;
    if request["sourceId"] != identity.source_id
        || request["sourceGeneration"] != identity.source_generation
        || request["domainKey"] != identity.domain_key
    {
        return Err(reject("stale_generation"));
    }
    Ok(())
}
fn request(identity: &Identity, action: &str) -> Value {
    json!({"contract":CONTRACT,"sourceId":identity.source_id,"sourceGeneration":identity.source_generation,"domainKey":identity.domain_key,"action":action})
}
fn reject(code: &str) -> Error {
    Error::InvalidInput(format!("memory publication: {code}"))
}
fn integrity() -> Error {
    Error::Unknown("memory publication integrity or identity mismatch; preserve original".into())
}
