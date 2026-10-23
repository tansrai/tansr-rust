//! Optional custody handoff. The host must supply the common trusted CAS
//! authority; the SDK never turns a copied directory into writer permission.
use super::{invalid, *};
use crate::{
    canonical,
    executor::{Authorizer, Operation, Tool, ToolContext, ToolError, ToolHandler},
};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
pub type AssertCurrent = Arc<dyn Fn() -> Result<()> + Send + Sync>;
pub type CommitAuthorizer = Arc<dyn Fn(&Operation) -> Result<()> + Send + Sync>;

pub const CONTRACT: &str = "persistence-custody-handoff-v1";
pub const TOOL_NAME: &str = "TansrPersistenceCustodyHandoffV1";
pub const TOOL_DIGEST: &str = "7f3b94cb801bf29be071674cdab0450855e70c55ab44bea27508c30dfbebb5e7";
pub const CONTROL_BYTES: usize = 32768;
pub const CHUNK_BYTES: usize = 12288;

pub fn validate(definition: &str, value: &Value) -> Result<()> {
    canonical::encode_limited(
        value,
        if definition == "TransferMetadata" {
            524288
        } else {
            CONTROL_BYTES
        },
    )?;
    crate::api::validate_wire(CONTRACT, definition, value)
}
pub fn digest(value: &Value) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(canonical::encode_limited(value, CONTROL_BYTES)?)
    ))
}
pub fn snapshot(identity: &Value, root: &Value) -> Result<String> {
    validate("Identity", identity)?;
    digest(&json!(["PST-CUSTODY-SNAPSHOT", identity, root]))
}
pub fn seed(identity: &Value, format: &str, root: &str) -> Result<String> {
    digest(&json!(["PST-CUSTODY-DIRECTORY", identity, format, root]))
}
pub fn record_digest(index: usize, prior: &str, record: &Value) -> Result<String> {
    digest(&json!([
        "PST-CUSTODY-RECORD",
        index.to_string(),
        prior,
        record
    ]))
}
pub fn writer_operation(request: &Value) -> Result<(String, String)> {
    Ok((digest(&json!(["PST-CUSTODY", request]))?, digest(request)?))
}
pub fn prepare_intent(request: &Value) -> Result<String> {
    digest(
        &json!({"identity":request["identity"],"source":request["source"],"destination":request["destination"],"expectedWriterFence":request["expectedWriterFence"],"expectedRoot":request["expectedRoot"]}),
    )
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MigrationBudget {
    pub bytes: usize,
    pub objects: usize,
}
impl Default for MigrationBudget {
    fn default() -> Self {
        Self {
            bytes: 1073741824,
            objects: 1048576,
        }
    }
}
impl MigrationBudget {
    pub fn check(&self, proof: &Value) -> Result<()> {
        validate("RootProof", proof)?;
        if self.bytes == 0
            || self.bytes > 1102569742
            || self.objects == 0
            || self.objects > 1310721
            || proof["bytes"]
                .as_u64()
                .is_none_or(|n| n > self.bytes as u64)
            || proof["objects"]
                .as_u64()
                .is_none_or(|n| n > self.objects as u64)
        {
            return Err(invalid("custody migration budget exceeded"));
        }
        Ok(())
    }
}

/// Deployment-owned shared control. It must durably CAS original keys and
/// fences across all sessions/locations. It must not be implemented in RAM.
#[async_trait]
pub trait Control: Send + Sync {
    async fn begin_handoff(&self, request: &Value) -> Result<String>;
    async fn record_prepared(&self, request: &Value, proof: &Value) -> Result<Value>;
    async fn query(&self, request: &Value) -> Result<Value>;
    async fn mark_verified(&self, request: &Value, proof: &Value) -> Result<Value>;
    async fn cutover(&self, request: &Value, proof: &Value) -> Result<Value>;
    async fn abort(&self, request: &Value) -> Result<Value>;
    /// writer-claim/query/finish returns the original nullable WriterClaim.
    async fn writer(&self, request: &Value) -> Result<Value>;
}
/// Durable inventory mechanics. The FileStore implementation retains its
/// original encrypted format and permanent transfer facts.
#[async_trait]
pub trait Port: Send + Sync {
    fn identity(&self) -> Value;
    /// Bind the original operation before any queued storage work. The callback
    /// is synchronous, must not reenter storage, and is checked at publication.
    fn for_operation(
        &self,
        owner: crate::memory_publication::Owner,
        current: AssertCurrent,
    ) -> Result<Arc<dyn Port>>;
    async fn freeze(&self, request: &Value, writer_fence: &str) -> Result<Value>;
    async fn page(&self, request: &Value) -> Result<Value>;
    async fn object(&self, request: &Value) -> Result<Value>;
    async fn import_page(&self, ticket: &Value, page: &Value) -> Result<()>;
    async fn import_object(&self, ticket: &Value, chunk: &Value) -> Result<()>;
    async fn verify(&self, ticket: &Value) -> Result<Value>;
    async fn activate(&self, ticket: &Value) -> Result<()>;
    async fn abort(&self, ticket: &Value) -> Result<()>;
}
#[derive(Clone)]
pub struct Client {
    api: ApiClient,
}
impl Client {
    pub fn new(api: ApiClient) -> Self {
        Self { api }
    }
    pub async fn capabilities(&self) -> Result<Value> {
        let v = self
            .read(
                "terminal.custody.capabilities",
                "CapabilitiesResponse",
                BTreeMap::new(),
                BTreeMap::from([("contract".into(), CONTRACT.into())]),
            )
            .await?;
        if v["installed"].as_bool() != Some(v["writerAuthority"] == "shared-cas") {
            return Err(invalid("custody authority declaration"));
        }
        Ok(v)
    }
    pub async fn query(&self, request: &Value) -> Result<Value> {
        validate("QueryRequest", request)?;
        let mut query = BTreeMap::from([
            ("contract".into(), CONTRACT.into()),
            ("intentSha256".into(), text(request, "intentSha256")?.into()),
        ]);
        for (key, value) in request["identity"]
            .as_object()
            .ok_or_else(|| invalid("custody identity"))?
        {
            query.insert(
                key.clone(),
                value
                    .as_str()
                    .ok_or_else(|| invalid("custody identity"))?
                    .into(),
            );
        }
        let v = self
            .read(
                "terminal.custody.query",
                "QueryResponse",
                BTreeMap::from([("id".into(), text(request, "operationId")?.into())]),
                query,
            )
            .await?;
        correlate(request, &v)?;
        Ok(v)
    }
    async fn read(
        &self,
        op: &str,
        definition: &str,
        params: BTreeMap<String, String>,
        query: BTreeMap<String, String>,
    ) -> Result<Value> {
        let r = self
            .api
            .call(
                op,
                CallOptions {
                    params,
                    query,
                    max_response_bytes: Some(CONTROL_BYTES),
                    ..Default::default()
                },
            )
            .await?;
        if r.status != 200 {
            return Err(invalid("custody HTTP status"));
        }
        let v = r.json()?;
        validate(definition, &v)?;
        Ok(v)
    }
}
pub fn correlate(request: &Value, response: &Value) -> Result<()> {
    validate("Response", response)?;
    for key in ["identity", "operationId", "intentSha256", "action"] {
        if request[key] != response[key] {
            return Err(invalid("custody response identity"));
        }
    }
    if response["preparing"] == true && !response["ticket"].is_null() {
        return Err(invalid("custody preparing has ticket"));
    }
    if let Some(ticket) = response.get("ticket").filter(|v| !v.is_null()) {
        check_ticket(request, ticket)?;
    }
    Ok(())
}
fn check_ticket(request: &Value, ticket: &Value) -> Result<()> {
    validate("Ticket", ticket)?;
    for key in ["identity", "operationId", "intentSha256"] {
        if request[key] != ticket[key] {
            return Err(invalid("custody ticket identity"));
        }
    }
    Ok(())
}
pub(crate) fn text<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v[key]
        .as_str()
        .ok_or_else(|| invalid("custody string field"))
}
pub fn chunk_bytes(chunk: &Value) -> Result<Vec<u8>> {
    validate("ObjectChunk", chunk)?;
    let b = STANDARD
        .decode(text(chunk, "base64")?)
        .map_err(|_| invalid("custody base64"))?;
    if STANDARD.encode(&b) != chunk["base64"]
        || Some(b.len() as u64) != chunk["byteLength"].as_u64()
        || format!("{:x}", Sha256::digest(&b)) != chunk["payloadDigest"]
    {
        return Err(invalid("custody object bytes"));
    }
    Ok(b)
}

/// Dedicated profile sharing the existing executor and MemoryPublication
/// permission. `authorize` must additionally permit administration of custody.
pub struct Host {
    port: Arc<dyn Port>,
    control: Arc<dyn Control>,
    authorize: Arc<dyn Authorizer>,
    commit_authorize: CommitAuthorizer,
    identity: Value,
}
impl Host {
    pub fn new(
        port: Arc<dyn Port>,
        control: Arc<dyn Control>,
        authorize: Arc<dyn Authorizer>,
        commit_authorize: CommitAuthorizer,
    ) -> Result<Self> {
        let identity = port.identity();
        validate("Identity", &identity)?;
        Ok(Self {
            port,
            control,
            authorize,
            commit_authorize,
            identity,
        })
    }
    pub fn into_tool(self) -> Tool {
        Tool::new(TOOL_DIGEST.into(), self)
    }
    async fn ticket(&self, request: &Value) -> Result<Value> {
        let found = self.control.query(request).await?;
        if found["preparing"] != false || found["ticket"].is_null() {
            return Err(invalid("custody ticket unavailable"));
        }
        let ticket = found["ticket"].clone();
        check_ticket(request, &ticket)?;
        Ok(ticket)
    }
    async fn checkpoint(&self, context: &ToolContext, op: &Operation) -> Result<()> {
        (self.commit_authorize)(op)?;
        if context.cancellation.is_cancelled()
            || self.port.identity() != self.identity
            || crate::executor::expiry(&op.expires_at)? <= std::time::SystemTime::now()
        {
            return Err(invalid("custody current context unavailable"));
        }
        tokio::select! {
            _ = context.cancellation.cancelled() => Err(crate::Error::Cancelled),
            result = self.authorize.authorize(op) => result,
        }
    }
    async fn execute(&self, r: &Value, context: &ToolContext, op: &Operation) -> Result<Value> {
        let original = op.clone();
        let cancelled = context.cancellation.clone();
        let commit_authorize = self.commit_authorize.clone();
        let current: AssertCurrent = Arc::new(move || {
            if cancelled.is_cancelled()
                || crate::executor::expiry(&original.expires_at)? <= std::time::SystemTime::now()
            {
                return Err(invalid("custody operation no longer current"));
            }
            commit_authorize(&original)
        });
        let port = self.port.for_operation(
            crate::memory_publication::Owner::from_operation(op),
            current,
        )?;
        // Recheck the actual operation around each durable/authority await.
        // Revocation after prepare must not authorize a later publication.
        macro_rules! checked {
            ($future:expr) => {{
                self.checkpoint(context, op).await?;
                let value = $future.await?;
                self.checkpoint(context, op).await?;
                value
            }};
        }
        let mut out = json!({"contract":CONTRACT,"identity":self.identity,"operationId":r["operationId"],"intentSha256":r["intentSha256"],"action":r["action"]});
        match text(r, "action")? {
            "prepare" => {
                if prepare_intent(r)? != r["intentSha256"] {
                    return Err(invalid("custody prepare intent"));
                }
                let fence = checked!(self.control.begin_handoff(r));
                let proof = checked!(port.freeze(r, &fence));
                out["ticket"] = checked!(self.control.record_prepared(r, &proof));
            }
            "export-page" => {
                checked!(self.ticket(r));
                let v = checked!(port.page(r));
                for key in ["proof", "page", "nextCursor"] {
                    out[key] = v[key].clone();
                }
            }
            "export-object" => {
                checked!(self.ticket(r));
                out["chunk"] = checked!(port.object(r));
            }
            "import-page" => {
                let t = checked!(self.ticket(r));
                if t["proof"] != r["proof"] {
                    return Err(invalid("custody proof mismatch"));
                }
                checked!(port.import_page(&t, &r["page"]));
                out["ticket"] = t;
                out["pageDigest"] = r["page"]["sha256"].clone();
            }
            "import-object" => {
                let t = checked!(self.ticket(r));
                checked!(port.import_object(&t, &r["chunk"]));
                out["ticket"] = t;
                out["object"] = r["chunk"]["object"].clone();
                out["nextOffset"] = json!(
                    r["chunk"]["offset"].as_u64().unwrap()
                        + r["chunk"]["byteLength"].as_u64().unwrap()
                );
            }
            "verify" => {
                let t = checked!(self.ticket(r));
                if t["proof"] != r["proof"] {
                    return Err(invalid("custody proof mismatch"));
                }
                let proof = checked!(port.verify(&t));
                out["ticket"] = checked!(self.control.mark_verified(r, &proof));
            }
            "cutover" => {
                let t = checked!(self.ticket(r));
                if t["proof"] != r["proof"]
                    || t["writerFence"] != r["expectedWriterFence"]
                    || t["source"]["custodyRevision"] != r["expectedCustodyRevision"]
                {
                    return Err(invalid("custody cutover precondition"));
                }
                let published = if t["phase"] == "published" {
                    t
                } else {
                    let proof = checked!(port.verify(&t));
                    checked!(self.control.cutover(r, &proof))
                };
                check_ticket(r, &published)?;
                if published["phase"] != "published" {
                    return Err(invalid("custody not published"));
                }
                checked!(port.activate(&published));
                out["ticket"] = published;
            }
            "query" => {
                let v = checked!(self.control.query(r));
                out["preparing"] = v["preparing"].clone();
                out["ticket"] = v["ticket"].clone();
            }
            "abort" => {
                let t = checked!(self.ticket(r));
                if t["writerFence"] != r["expectedWriterFence"]
                    || t["source"]["custodyRevision"] != r["expectedCustodyRevision"]
                {
                    return Err(invalid("custody abort precondition"));
                }
                let t = checked!(self.control.abort(r));
                checked!(port.abort(&t));
                out["ticket"] = t;
            }
            "writer-claim" | "writer-query" | "writer-finish" => {
                out["claim"] = checked!(self.control.writer(r))
            }
            _ => return Err(invalid("custody action")),
        }
        correlate(r, &out)?;
        Ok(out)
    }
}
pub(crate) fn valid_profile(op: &Operation) -> bool {
    op.tool_name == "MemoryPublication"
        && op.request.operation == "tool.invoke"
        && op.request.args["name"] == TOOL_NAME
        && op.request.args["definitionDigest"] == TOOL_DIGEST
        && op.request.args["argsJson"].as_str().is_some_and(|t| {
            canonical::parse_json(t.as_bytes(), CONTROL_BYTES)
                .is_ok_and(|v| validate("Request", &v).is_ok())
        })
}
#[async_trait]
impl ToolHandler for Host {
    fn is_persistence_custody_host(&self) -> bool {
        true
    }
    async fn invoke(
        &self,
        _context: ToolContext,
        _args: Value,
    ) -> std::result::Result<Value, ToolError> {
        Err(ToolError::Rejected(
            "custody requires original operation".into(),
        ))
    }
    async fn invoke_operation(
        &self,
        context: ToolContext,
        op: &Operation,
        args: Value,
    ) -> std::result::Result<Value, ToolError> {
        if !valid_profile(op)
            || self.port.identity() != self.identity
            || args["identity"] != self.identity
            || op.scope.application_scope_id != self.identity["applicationScopeId"]
            || op.scope.end_user_id != self.identity["endUserId"]
            || op.request.args["argsJson"]
                .as_str()
                .and_then(|s| canonical::parse_json(s.as_bytes(), CONTROL_BYTES).ok())
                .as_ref()
                != Some(&args)
            || crate::executor::operation_digest(op).ok().as_deref() != Some(&op.digest)
            || crate::executor::expiry(&op.expires_at)
                .map_or(true, |v| v <= std::time::SystemTime::now())
            || context.cancellation.is_cancelled()
        {
            return Err(ToolError::Rejected("custody profile/identity".into()));
        }
        self.authorize
            .authorize(op)
            .await
            .map_err(|_| ToolError::Rejected("custody authority".into()))?;
        let result = self.execute(&args, &context, op).await.map_err(|_| {
            ToolError::Unknown("custody result requires original-key reconciliation".into())
        })?;
        self.authorize
            .authorize(op)
            .await
            .map_err(|_| ToolError::Unknown("custody authority changed".into()))?;
        if context.cancellation.is_cancelled() || self.port.identity() != self.identity {
            return Err(ToolError::Unknown("custody context changed".into()));
        }
        let text = String::from_utf8(
            canonical::encode_limited(&result, CONTROL_BYTES)
                .map_err(|_| ToolError::Unknown("custody result limit".into()))?,
        )
        .map_err(|_| ToolError::Unknown("custody encoding".into()))?;
        Ok(json!({"status":"ok","content":[{"t":"text","text":text}]}))
    }
}
