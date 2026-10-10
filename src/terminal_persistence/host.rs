use super::*;
use crate::executor::{Authorizer, Operation, Tool, ToolContext, ToolError, ToolHandler};
use std::sync::Arc;
/// Dedicated trusted host. The explicit authorizer must validate current scope,
/// session and binding even when this handler is called outside Runner.
pub struct Host {
    store: Arc<dyn PersistenceStore>,
    authorize: Arc<dyn Authorizer>,
    identity: Identity,
}
impl Host {
    pub fn new(store: Arc<dyn PersistenceStore>, authorize: Arc<dyn Authorizer>) -> Result<Self> {
        if !store.atomic_durable_publication() || !store.encrypted_at_rest() {
            return Err(reject("encrypted_durable_store_required"));
        }
        let identity = store.identity().clone();
        Ok(Self {
            store,
            authorize,
            identity,
        })
    }
    /// Register this reserved transport profile, never a model-facing tool.
    pub fn into_tool(self) -> Tool {
        Tool::new(TOOL_DIGEST.into(), self)
    }
}
pub(crate) fn valid_profile(op: &Operation) -> bool {
    op.tool_name == "MemoryPublication"
        && op.request.operation == "tool.invoke"
        && op.request.args["name"] == TOOL_NAME
        && op.request.args["definitionDigest"] == TOOL_DIGEST
        && op.request.args["argsJson"].as_str().is_some_and(|text| {
            crate::canonical::parse_json(text.as_bytes(), 32768)
                .is_ok_and(|v| crate::api::validate_wire(CONTRACT, "Request", &v).is_ok())
        })
}
#[async_trait]
impl ToolHandler for Host {
    fn is_terminal_persistence_host(&self) -> bool {
        true
    }
    async fn invoke(
        &self,
        _context: ToolContext,
        _args: Value,
    ) -> std::result::Result<Value, ToolError> {
        Err(ToolError::Rejected(
            "publication requires original execution operation".into(),
        ))
    }
    async fn invoke_operation(
        &self,
        context: ToolContext,
        operation: &Operation,
        args: Value,
    ) -> std::result::Result<Value, ToolError> {
        if crate::executor::expiry(&operation.expires_at)
            .map_or(true, |expires| expires <= std::time::SystemTime::now())
            || !valid_profile(operation)
            || crate::executor::operation_digest(operation).ok().as_deref()
                != Some(&operation.digest)
        {
            return Err(ToolError::Rejected("invalid publication profile".into()));
        }
        let identity = &self.identity;
        if self.store.identity() != identity
            || !self.store.atomic_durable_publication()
            || !self.store.encrypted_at_rest()
        {
            return Err(ToolError::Rejected(
                "persistence store capability changed".into(),
            ));
        }
        if operation.scope.application_scope_id != identity.application_scope_id
            || operation.scope.end_user_id != identity.end_user_id
            || validate_request(&args, identity).is_err()
            || operation.request.args["argsJson"]
                .as_str()
                .and_then(|v| crate::canonical::parse_json(v.as_bytes(), 32768).ok())
                .as_ref()
                != Some(&args)
        {
            return Err(ToolError::Rejected(
                "publication identity or arguments mismatch".into(),
            ));
        }
        self.authorize
            .authorize(operation)
            .await
            .map_err(|_| ToolError::Rejected("publication authority denied".into()))?;
        if context.cancellation.is_cancelled()
            || self.store.identity() != identity
            || !self.store.atomic_durable_publication()
            || !self.store.encrypted_at_rest()
        {
            return Err(ToolError::Rejected(
                "publication cancelled before storage".into(),
            ));
        }
        match self
            .store
            .execute(args.clone(), Owner::from_operation(operation))
            .await
        {
            Ok(response) => {
                let authorized = self.authorize.authorize(operation).await.is_ok();
                if !authorized
                    || self.store.identity() != identity
                    || !self.store.atomic_durable_publication()
                    || !self.store.encrypted_at_rest()
                    || context.cancellation.is_cancelled()
                {
                    return Err(ToolError::Unknown(
                        "persistence authority changed after invocation".into(),
                    ));
                }
                correlate(&args, &response, identity).map_err(|_| {
                    ToolError::Unknown("persistence response correlation failed".into())
                })?;
                crate::api::validate_wire(CONTRACT, "Response", &response)
                    .map_err(|_| ToolError::Unknown("publication response invalid".into()))?;
                let text =
                    String::from_utf8(crate::canonical::encode_limited(&response, 32768).map_err(
                        |_| ToolError::Unknown("persistence response control limit".into()),
                    )?)
                    .map_err(|_| ToolError::Unknown("persistence response encoding".into()))?;
                // A durable rejection is a definite tool error for writes; query
                // retains the full original fact for reconciliation. Only map
                // after authority, correlation, schema and encoding checks.
                if matches!(args["action"].as_str(), Some("begin" | "put" | "commit"))
                    && response["transfer"]["status"] == "rejected"
                {
                    return Ok(
                        json!({"status":"error","message":response["transfer"]["rejection"]["code"]}),
                    );
                }
                Ok(json!({"status":"ok","content":[{"t":"text","text":text}]}))
            }
            Err(Error::InvalidInput(message)) => {
                let code = message
                    .strip_prefix("terminal persistence: ")
                    .unwrap_or("unknown");
                if matches!(
                    code,
                    "invalid_request"
                        | "request_conflict"
                        | "revision_conflict"
                        | "capacity_exceeded"
                        | "integrity_mismatch"
                        | "stale_generation"
                ) {
                    Ok(json!({"status":"error","message":code}))
                } else {
                    Err(ToolError::Unknown(
                        "publication access or storage changed; reconcile original transfer".into(),
                    ))
                }
            }
            Err(_) => Err(ToolError::Unknown(
                "publication storage outcome unknown; reconcile original transfer".into(),
            )),
        }
    }
}

fn correlate(request: &Value, response: &Value, identity: &Identity) -> Result<()> {
    use base64::Engine;
    use sha2::{Digest, Sha256};
    for root in [
        &response["root"],
        &response["transfer"]["result"],
        &response["transfer"]["rejection"]["observedRoot"],
    ] {
        if !root.is_null() {
            super::store::verify_root(root, identity)?;
        }
    }
    for key in [
        "contract",
        "sourceId",
        "sourceGeneration",
        "domainKey",
        "action",
    ] {
        if request[key] != response[key] {
            return Err(integrity());
        }
    }
    match request["action"].as_str() {
        Some("begin" | "put" | "commit" | "query") => {
            for key in ["transferId", "intentSha256"] {
                if request[key] != response["transfer"][key] {
                    return Err(integrity());
                }
            }
        }
        Some("read") => {
            let encoded = response["base64"].as_str().ok_or_else(integrity)?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| integrity())?;
            if base64::engine::general_purpose::STANDARD.encode(&bytes) != encoded
                || response["byteLength"] != bytes.len()
                || response["payloadDigest"] != format!("{:x}", Sha256::digest(&bytes))
            {
                return Err(integrity());
            }
            if request["part"] == "body"
                && (bytes.len() as u64 > request["length"].as_u64().ok_or_else(integrity)?
                    || response["nextOffset"].as_u64()
                        != request["offset"]
                            .as_u64()
                            .and_then(|n| n.checked_add(bytes.len() as u64)))
            {
                return Err(integrity());
            }
            for key in ["part", "commitRoot"] {
                if request[key] != response[key] {
                    return Err(integrity());
                }
            }
            let key = if request["part"] == "body" {
                "offset"
            } else {
                "pageIndex"
            };
            if request[key] != response[key] {
                return Err(integrity());
            }
        }
        Some("lookup") => {
            if request["commitRoot"] != response["commitRoot"] {
                return Err(integrity());
            }
            let entry = &response["entry"];
            if !entry.is_null() {
                let key = if request["key"]["kind"] == "primary" {
                    "primaryKey"
                } else {
                    "secondaryKey"
                };
                let encoded = entry["base64"].as_str().ok_or_else(integrity)?;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map_err(|_| integrity())?;
                if entry[key] != request["key"]["digest"]
                    || base64::engine::general_purpose::STANDARD.encode(&bytes) != encoded
                    || entry["value"]["byteLength"] != bytes.len()
                    || entry["value"]["sha256"] != format!("{:x}", Sha256::digest(&bytes))
                {
                    return Err(integrity());
                }
            }
        }
        _ => {}
    }
    Ok(())
}
