use super::*;
use crate::executor::{Authorizer, Operation, Tool, ToolContext, ToolError, ToolHandler};
/// Dedicated trusted host. The explicit authorizer must validate current scope,
/// session and binding even when this handler is called outside Runner.
pub struct Host {
    store: Arc<dyn MemoryPublicationStore>,
    authorize: Arc<dyn Authorizer>,
}
impl Host {
    pub fn new(
        store: Arc<dyn MemoryPublicationStore>,
        authorize: Arc<dyn Authorizer>,
    ) -> Result<Self> {
        if !store.atomic_durable_publication() || !store.encrypted_at_rest() {
            return Err(reject("encrypted_durable_store_required"));
        }
        Ok(Self { store, authorize })
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
            crate::canonical::parse_json(text.as_bytes(), 32768).is_ok_and(|v| {
                crate::api::validate_wire(CONTRACT, "MemoryPublicationRequest", &v).is_ok()
            })
        })
}
#[async_trait]
impl ToolHandler for Host {
    fn is_memory_publication_host(&self) -> bool {
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
        let identity = self.store.identity();
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
        if context.cancellation.is_cancelled() {
            return Err(ToolError::Rejected(
                "publication cancelled before storage".into(),
            ));
        }
        match self
            .store
            .execute(args, Owner::from_operation(operation))
            .await
        {
            Ok(response) => {
                crate::api::validate_wire(CONTRACT, "MemoryPublicationResponse", &response)
                    .map_err(|_| ToolError::Unknown("publication response invalid".into()))?;
                Ok(json!({"status":"ok","content":[{"t":"text","text":response.to_string()}]}))
            }
            Err(Error::InvalidInput(message)) => {
                let code = message
                    .strip_prefix("memory publication: ")
                    .unwrap_or("invalid_request");
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
