use super::{TerminalSessionReference, types::*};
use crate::{
    api::{ApiClient, CallOptions, Result},
    canonical,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};

#[derive(Clone)]
pub struct Client {
    api: ApiClient,
    scope: Scope,
}
impl Client {
    pub fn new(api: ApiClient, scope: Scope) -> Result<Self> {
        validate("Scope", &scope)?;
        Ok(Self { api, scope })
    }
    pub fn api(&self) -> &ApiClient {
        &self.api
    }
    pub fn scope(&self) -> &Scope {
        &self.scope
    }
    /// Explicitly negotiate the existing terminal output contract. Missing output
    /// capability is an error; this helper never silently degrades or elevates a ticket.
    pub async fn negotiate_output(
        &self,
        session: &TerminalSessionReference,
        binding: &Binding,
        request_id: &str,
    ) -> Result<super::TerminalOptions> {
        let body = json!({"contract":"terminal-services-v1","requestId":request_id,"session":session,"executionBinding":binding,"required":["execution-stream-v1"],"optional":[]});
        crate::api::validate_wire("terminal-services-v1", "BindingRequest", &body)?;
        let r = self
            .api
            .call(
                "terminal.binding.create",
                CallOptions {
                    body: Some(body),
                    max_response_bytes: Some(CONTROL_BYTES),
                    ..Default::default()
                },
            )
            .await?;
        if r.status != 200 {
            return Err(invalid("terminal binding HTTP"));
        }
        let v = r.json()?;
        crate::api::validate_wire("terminal-services-v1", "BindingResponse", &v)?;
        if v["requestId"] != request_id
            || v["session"] != value(session)?
            || v["executionBinding"] != value(binding)?
            || v["scope"] != value(&self.scope)?
            || !v["accepted"]
                .as_array()
                .is_some_and(|a| a.iter().any(|v| v == "execution-stream-v1"))
        {
            return Err(invalid("terminal binding identity or output capability"));
        }
        Ok(super::TerminalOptions {
            session_contract: session.session_contract.clone(),
            limits: serde_json::from_value(v["limits"].clone())?,
        })
    }
    async fn call<T: DeserializeOwned>(
        &self,
        op: &str,
        schema: &str,
        opts: CallOptions,
    ) -> Result<T> {
        let r = self
            .api
            .call(
                op,
                CallOptions {
                    max_response_bytes: Some(CONTROL_BYTES),
                    ..opts
                },
            )
            .await?;
        if r.status != if op == "executor.register" { 201 } else { 200 }
            || r.meta.content_type != "application/json"
        {
            return Err(invalid("unexpected HTTP status or media type"));
        }
        let v = r.json()?;
        crate::api::validate_wire(PROTOCOL, schema, &v)?;
        Ok(serde_json::from_value(v)?)
    }
    pub async fn register(&self, r: &Registration) -> Result<Connection> {
        validate_registration(r)?;
        let c: Connection = self
            .call(
                "executor.register",
                "ExecutorConnection",
                CallOptions {
                    body: Some(value(r)?),
                    ..Default::default()
                },
            )
            .await?;
        if c.executor_id != r.executor_id {
            return Err(invalid("executor identity"));
        }
        live(&c)?;
        Ok(c)
    }
    pub async fn heartbeat(&self, c: &Connection) -> Result<Connection> {
        live(c)?;
        let r:Connection=self.call("executor.heartbeat","ExecutorConnection",CallOptions{params:params(&[("id",&c.executor_id)]),body:Some(json!({"protocol":PROTOCOL,"executorId":c.executor_id,"connectionId":c.connection_id})),deadline:Some(expiry(&c.expires_at)?),..Default::default()}).await?;
        if r.executor_id != c.executor_id || r.connection_id != c.connection_id {
            return Err(invalid("heartbeat identity"));
        }
        live(&r)?;
        Ok(r)
    }
    pub async fn poll(&self, c: &Connection) -> Result<Batch> {
        live(c)?;
        let b: Batch = self
            .call(
                "executor.operations.poll",
                "ExecutionBatch",
                CallOptions {
                    params: params(&[("id", &c.executor_id)]),
                    query: params(&[("connectionId", &c.connection_id)]),
                    deadline: Some(expiry(&c.expires_at)?),
                    ..Default::default()
                },
            )
            .await?;
        if b.executor_id != c.executor_id || b.connection_id != c.connection_id {
            return Err(invalid("poll identity"));
        }
        let mut seen = HashSet::new();
        for op in &b.operations {
            validate_operation(op)?;
            if !seen.insert(&op.operation_id) {
                return Err(invalid("duplicate operation"));
            }
            self.check_identity(c, op)?;
        }
        Ok(b)
    }
    pub async fn initialize(
        &self,
        session: &str,
        platform: &Platform,
        tools: Option<Vec<String>>,
        closure: &str,
    ) -> Result<Capabilities> {
        let mut body = json!({"protocol":PROTOCOL,"sessionId":session,"platform":platform});
        if let Some(t) = tools {
            if t.iter().collect::<HashSet<_>>().len() != t.len() {
                return Err(invalid("duplicate tool"));
            }
            body["requestedTools"] = value(&t)?;
        }
        validate("SessionInitializeRequest", &body)?;
        let c: Capabilities = self
            .call(
                "execution.initialize",
                "SessionExecutionCapabilities",
                CallOptions {
                    params: params(&[("id", session)]),
                    body: Some(body),
                    closure_id: Some(closure.into()),
                    ..Default::default()
                },
            )
            .await?;
        if c.session_id != session || c.platform.as_ref() != Some(platform) {
            return Err(invalid("initialization identity"));
        }
        validate_capabilities(&c)?;
        Ok(c)
    }
    pub async fn execution_capabilities(&self, session: &str) -> Result<Capabilities> {
        let c: Capabilities = self
            .call(
                "execution.capabilities",
                "SessionExecutionCapabilities",
                CallOptions {
                    params: params(&[("id", session)]),
                    ..Default::default()
                },
            )
            .await?;
        if c.session_id != session {
            return Err(invalid("session identity"));
        }
        validate_capabilities(&c)?;
        Ok(c)
    }
    pub async fn bind(
        &self,
        session: &str,
        c: &Connection,
        workspace: &Workspace,
        revision: &str,
        closure: &str,
    ) -> Result<Capabilities> {
        live(c)?;
        let b = json!({"protocol":PROTOCOL,"sessionId":session,"executorId":c.executor_id,"connectionId":c.connection_id,"workspaceId":workspace.workspace_id,"expectedCapabilityRevision":revision});
        validate("ExecutionBindingRequest", &b)?;
        let out: Capabilities = self
            .call(
                "execution.binding.create",
                "SessionExecutionCapabilities",
                CallOptions {
                    params: params(&[("id", session)]),
                    body: Some(b),
                    closure_id: Some(closure.into()),
                    ..Default::default()
                },
            )
            .await?;
        let t = &out
            .binding
            .as_ref()
            .ok_or_else(|| invalid("binding missing"))?
            .target;
        if out.session_id != session
            || t.executor_id != c.executor_id
            || t.connection_id != c.connection_id
            || t.connection_revision != c.connection_revision
            || t.workspace_id != workspace.workspace_id
            || t.workspace_revision != workspace.revision
        {
            return Err(invalid("binding identity"));
        }
        validate_capabilities(&out)?;
        Ok(out)
    }
    pub async fn status(&self, session: &str, id: &str) -> Result<Status> {
        let s: Status = self
            .call(
                "execution.status",
                "ExecutionStatus",
                CallOptions {
                    params: params(&[("id", session), ("targetId", id)]),
                    ..Default::default()
                },
            )
            .await?;
        self.validate_status(&s)?;
        if s.operation.session_id != session || s.operation.operation_id != id {
            return Err(invalid("status identity"));
        }
        Ok(s)
    }
    /// Restricted executor tickets never fall back to a controller status endpoint.
    pub async fn executor_status(
        &self,
        session: &TerminalSessionReference,
        c: &Connection,
        op: &Operation,
    ) -> Result<Status> {
        live(c)?;
        validate_operation(op)?;
        self.check_identity(c, op)?;
        crate::api::validate_wire("terminal-services-v1", "SessionReference", &value(session)?)?;
        if session.session_id != op.session_id {
            return Err(invalid("terminal session identity"));
        }
        let r = self
            .api
            .call(
                "terminal.execution.state",
                CallOptions {
                    params: params(&[("id", &c.executor_id), ("targetId", &op.operation_id)]),
                    query: params(&[
                        ("sessionContract", &session.session_contract),
                        ("sessionId", &session.session_id),
                        ("requestDigest", &op.digest),
                        ("connectionId", &c.connection_id),
                    ]),
                    max_response_bytes: Some(CONTROL_BYTES),
                    deadline: Some(expiry(&c.expires_at)?),
                    ..Default::default()
                },
            )
            .await?;
        if r.status != 200 {
            return Err(invalid("terminal status HTTP"));
        }
        let v = r.json()?;
        crate::api::validate_wire("terminal-services-v1", "ExecutionState", &v)?;
        if v["session"] != value(session)? {
            return Err(invalid("terminal status session"));
        }
        let s: Status = serde_json::from_value(v["execution"].clone())?;
        self.validate_status(&s)?;
        if &s.operation != op {
            return Err(invalid("terminal status operation"));
        }
        Ok(s)
    }
    pub async fn submit(&self, op: &Operation, receipt: &Receipt) -> Result<Status> {
        if op.scope.application_scope_id != self.scope.application_scope_id
            || op.scope.end_user_id != self.scope.end_user_id
        {
            return Err(invalid("receipt scope"));
        }
        validate_receipt(op, receipt)?;
        let s: Status = self
            .call(
                "executor.receipt.submit",
                "ExecutionStatus",
                CallOptions {
                    params: params(&[("id", &receipt.executor_id)]),
                    body: Some(value(receipt)?),
                    ..Default::default()
                },
            )
            .await?;
        self.validate_status(&s)?;
        if &s.operation != op || s.receipt.as_ref() != Some(receipt) || s.status != receipt.status {
            return Err(invalid("receipt response identity"));
        }
        Ok(s)
    }
    pub(crate) fn check_identity(&self, c: &Connection, op: &Operation) -> Result<()> {
        let t = &op.binding.target;
        if op.scope != self.scope
            || t.executor_id != c.executor_id
            || t.connection_id != c.connection_id
            || t.connection_revision != c.connection_revision
        {
            return Err(invalid("operation identity or generation"));
        }
        Ok(())
    }
    pub(crate) fn validate_status(&self, s: &Status) -> Result<()> {
        validate("ExecutionStatus", s)?;
        validate_operation(&s.operation)?;
        if s.operation.scope.application_scope_id != self.scope.application_scope_id
            || s.operation.scope.end_user_id != self.scope.end_user_id
        {
            return Err(invalid("status scope"));
        }
        if s.status == "pending" || s.status == "unknown" && s.receipt.is_none() {
            if s.receipt.is_some() {
                return Err(invalid("pending receipt"));
            }
            return Ok(());
        }
        let r = s
            .receipt
            .as_ref()
            .ok_or_else(|| invalid("missing receipt"))?;
        if r.status != s.status {
            return Err(invalid("receipt status"));
        }
        validate_receipt(&s.operation, r)
    }
}
pub(crate) fn params(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).into(), (*v).into()))
        .collect()
}
pub(crate) fn validate_registration(r: &Registration) -> Result<()> {
    validate("ExecutorRegistrationRequest", r)?;
    if r.workspaces
        .iter()
        .map(|w| &w.workspace_id)
        .collect::<HashSet<_>>()
        .len()
        != r.workspaces.len()
        || r.tools
            .iter()
            .map(|t| &t.name)
            .collect::<HashSet<_>>()
            .len()
            != r.tools.len()
        || r.operations.iter().any(|o| o == "tool.invoke") == r.tools.is_empty()
    {
        return Err(invalid("registration duplicate or mismatch"));
    }
    Ok(())
}
fn validate_capabilities(c: &Capabilities) -> Result<()> {
    if c.effective_tools
        .iter()
        .map(|t| &t.name)
        .collect::<HashSet<_>>()
        .len()
        != c.effective_tools.len()
    {
        return Err(invalid("duplicate effective tool"));
    }
    Ok(())
}
pub fn operation_digest(op: &Operation) -> Result<String> {
    let mut v = value(op)?;
    v["digest"] = Value::String("0".repeat(64));
    crate::api::validate_wire(PROTOCOL, "ExecutionOperation", &v)?;
    v.as_object_mut()
        .ok_or_else(|| invalid("operation"))?
        .remove("digest");
    canonical::digest("tansr.sdk2.execution.v1", &v)
}
pub(crate) fn validate_operation(op: &Operation) -> Result<()> {
    if op.digest != operation_digest(op)? {
        return Err(invalid("operation digest"));
    }
    if op.request.operation == "tool.invoke" {
        if op.request.args["name"] == crate::memory_publication::TOOL_NAME
            || op.request.args["name"] == crate::terminal_persistence::TOOL_NAME
            || op.tool_name == "MemoryPublication"
        {
            if !crate::memory_publication::valid_profile(op)
                && !crate::terminal_persistence::valid_profile(op)
            {
                return Err(invalid("invalid reserved publication profile"));
            }
        } else if op.request.args["name"] != op.tool_name {
            return Err(invalid("reserved or substituted tool name"));
        }
        super::parse_tool_arguments(
            op.request.args["argsJson"]
                .as_str()
                .ok_or_else(|| invalid("arguments"))?,
        )?;
    }
    Ok(())
}
pub(crate) fn validate_receipt(op: &Operation, r: &Receipt) -> Result<()> {
    validate_operation(op)?;
    validate("ExecutionReceiptRequest", r)?;
    if r.operation_id != op.operation_id
        || r.digest != op.digest
        || r.executor_id != op.binding.target.executor_id
        || r.connection_id != op.binding.target.connection_id
    {
        return Err(invalid("receipt identity"));
    }
    if r.status != "completed" {
        if r.result.is_some() || r.error_code.is_none() {
            return Err(invalid("non-completed receipt"));
        }
        return Ok(());
    }
    let result = r.result.as_ref().ok_or_else(|| invalid("missing result"))?;
    if r.error_code.is_some()
        || result.operation != op.request.operation
        || result.operation != "tool.invoke"
    {
        return Err(invalid("unsupported result"));
    }
    super::tool::verify_result(
        result.args["resultJson"]
            .as_str()
            .ok_or_else(|| invalid("result JSON"))?,
    )
}
