//! Explicit terminal-persistence-v1 storage. Values and keys are opaque; Serve
//! retains receipt eligibility, memory selection and every business decision.
mod host;
mod store;
pub use crate::memory_publication::{AuthorizeRecovery, Identity, OpenMode, Owner, ReadContext};
use crate::{Error, Result};
use async_trait::async_trait;
pub use host::*;
use serde_json::{Value, json};
pub use store::*;

pub const TOOL_NAME: &str = "TansrTerminalPersistenceV1";
pub const TOOL_DIGEST: &str = "33029a264edf81f3fda2a13fc382403d0cd7ffefa1f38088bb9366f387a13587";
pub const CONTRACT: &str = "terminal-persistence-v1";
pub const BLOCK_BYTES: usize = 12_288;
pub const MAX_BODY: usize = 4_194_304;

#[async_trait]
pub trait PersistenceStore: Send + Sync {
    fn identity(&self) -> &Identity;
    fn atomic_durable_publication(&self) -> bool;
    fn encrypted_at_rest(&self) -> bool;
    async fn execute(&self, request: Value, owner: Owner) -> Result<Value>;
}
fn reject(code: &str) -> Error {
    Error::InvalidInput(format!("terminal persistence: {code}"))
}
fn integrity() -> Error {
    Error::Unknown("terminal persistence integrity or identity mismatch; preserve original".into())
}
fn validate_request(v: &Value, identity: &Identity) -> Result<()> {
    crate::canonical::encode_limited(v, 32768)?;
    crate::api::validate_wire(CONTRACT, "Request", v)?;
    if v["sourceId"] != identity.source_id
        || v["sourceGeneration"] != identity.source_generation
        || v["domainKey"] != identity.domain_key
    {
        return Err(reject("stale_generation"));
    }
    Ok(())
}
fn request(i: &Identity, action: &str) -> Value {
    json!({"contract":CONTRACT,"sourceId":i.source_id,"sourceGeneration":i.source_generation,"domainKey":i.domain_key,"action":action})
}
