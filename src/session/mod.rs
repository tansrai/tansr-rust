//! High-level Serve conversations. The explicit SDK1/offload family is retained
//! for the client's lifetime. There are no hidden reconnects, retries, turns or
//! background tasks; dropping an observation does not interrupt Serve.

mod control;
mod events;
mod types;

pub use events::{Outcome, OutcomeStatus, SessionEvent, SessionEventStream, TurnTracker};
pub use types::*;

use std::collections::BTreeMap;

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::api::{ApiClient, ApiResponse, CallOptions, Error, Result};

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const MEDIA_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone)]
pub struct SessionClient {
    api: ApiClient,
    cancellation: CancellationToken,
}

#[derive(Clone)]
pub struct Session {
    client: SessionClient,
    created: Created,
}

impl SessionClient {
    pub fn new(api: ApiClient) -> Result<Self> {
        if !matches!(api.family(), "sdk1" | "sdk2-offload-v1") {
            return Err(invalid(
                "an explicit SDK1 or offload session family is required",
            ));
        }
        Ok(Self {
            api,
            cancellation: CancellationToken::new(),
        })
    }

    /// Set the parent cancellation for reads and observation setup. Write calls
    /// additionally honor their own options. Cancellation is always local.
    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub fn api(&self) -> &ApiClient {
        &self.api
    }

    /// Performs read-only discovery before the single create request. A failure
    /// never falls back to another route/family or creates another session.
    pub async fn create(&self, options: CreateOptions) -> Result<Session> {
        validate_create(&options, self.api.family())?;
        self.discover_family(&options.write).await?;
        let body = serde_json::to_value(&options)?;
        let response = self
            .call(
                "session.create",
                CallOptions {
                    body: Some(body),
                    ..write_options(options.write)
                },
            )
            .await?;
        if !matches!(response.status, 200 | 201) {
            return Err(contract("creation must return 200 attach or 201 created"));
        }
        let raw = object_response(&response, response.status)?;
        let created: Created = serde_json::from_value(raw.clone())?;
        if created.session_id.is_empty()
            || created.last_seq > MAX_SAFE_INTEGER
            || options
                .resume
                .as_ref()
                .is_some_and(|r| r.session_id != created.session_id)
        {
            return Err(contract("created session identity or sequence is invalid"));
        }
        check_family(&raw, self.api.family())?;
        Ok(Session {
            client: self.clone(),
            created,
        })
    }

    /// Read-only attachment. A dormant session must explicitly be resumed.
    pub async fn attach(&self, id: &str) -> Result<Session> {
        nonempty(id, "session id")?;
        let mut session = Session {
            client: self.clone(),
            created: Created {
                session_id: id.into(),
                resumed: false,
                last_seq: 0,
            },
        };
        session.created.last_seq = session.meta().await?.last_seq;
        Ok(session)
    }

    pub async fn resume(&self, id: &str, write: WriteOptions) -> Result<Session> {
        self.create(CreateOptions {
            resume: Some(ResumeReference {
                session_id: id.into(),
            }),
            write,
            ..CreateOptions::default()
        })
        .await
    }

    pub async fn list(&self, offset: u64, limit: u64) -> Result<SessionList> {
        if limit == 0 || offset > MAX_SAFE_INTEGER || limit > MAX_SAFE_INTEGER {
            return Err(invalid(
                "list offset must be safe and limit must be positive",
            ));
        }
        let response = self
            .call(
                "session.list",
                CallOptions {
                    query: BTreeMap::from([
                        ("offset".into(), offset.to_string()),
                        ("limit".into(), limit.to_string()),
                    ]),
                    ..CallOptions::default()
                },
            )
            .await?;
        let raw = object_response(&response, 200)?;
        let total = safe_field(&raw, "total")?;
        let values = raw
            .get("sessions")
            .and_then(Value::as_array)
            .ok_or_else(|| contract("session list requires an array"))?;
        let sessions = values
            .iter()
            .map(|v| read_meta(v.clone(), self.api.family()))
            .collect::<Result<_>>()?;
        Ok(SessionList { sessions, total })
    }

    async fn discover_family(&self, options: &WriteOptions) -> Result<()> {
        let response = self
            .call(
                "session.capabilities",
                CallOptions {
                    query: BTreeMap::from([("protocol".into(), "sdk2-ext-v1".into())]),
                    cancellation: options.cancellation.clone(),
                    deadline: options.deadline,
                    ..CallOptions::default()
                },
            )
            .await?;
        let raw = object_response(&response, 200)?;
        let entries = raw
            .get("contracts")
            .and_then(Value::as_array)
            .ok_or_else(|| contract("missing session family contracts"))?;
        if raw["protocol"] != "sdk2-ext-v1" || entries.len() > 2 {
            return Err(contract("session family discovery is malformed"));
        }
        let mut seen = std::collections::BTreeSet::new();
        for entry in entries {
            let family = required_str(entry, "contract")?;
            let availability = required_str(entry, "availability")?;
            if !seen.insert(family.to_owned())
                || !matches!(
                    (family, availability),
                    ("sdk1", "legacy-complete") | ("sdk2-offload-v1", "source-required")
                )
            {
                return Err(contract(
                    "session discovery contains an invalid or duplicate family",
                ));
            }
        }
        if !seen.contains(self.api.family()) {
            return Err(invalid(
                "selected session family is unavailable; no write attempted",
            ));
        }
        Ok(())
    }

    async fn call(&self, operation: &str, options: CallOptions) -> Result<ApiResponse> {
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => Err(Error::Cancelled),
            result = self.api.call(operation, options) => result,
        }
    }
}

impl Session {
    pub fn id(&self) -> &str {
        &self.created.session_id
    }
    pub fn created(&self) -> &Created {
        &self.created
    }
    pub fn api(&self) -> &ApiClient {
        &self.client.api
    }

    /// Capability decisions are read anew for every write; they are not cached
    /// across authorization revisions. Generic transport validates the schema.
    pub async fn capabilities(&self) -> Result<CapabilityClosure> {
        self.capabilities_for(&CallOptions::default()).await
    }

    async fn capabilities_for(&self, options: &CallOptions) -> Result<CapabilityClosure> {
        let response = self
            .client
            .call(
                "discovery.session.capabilities",
                CallOptions {
                    params: self.params(),
                    cancellation: options.cancellation.clone(),
                    deadline: options.deadline,
                    ..CallOptions::default()
                },
            )
            .await?;
        let raw = object_response(&response, 200)?;
        let closure_id = required_str(&raw, "closureId")?.to_owned();
        if response.meta.closure_id.as_deref() != Some(closure_id.as_str()) {
            return Err(contract("closure response header differs from body"));
        }
        let operations: BTreeMap<String, String> = serde_json::from_value(
            raw.get("operations")
                .cloned()
                .ok_or_else(|| contract("missing closure operations"))?,
        )?;
        if operations
            .values()
            .any(|v| !matches!(v.as_str(), "enabled" | "disabled" | "unavailable"))
        {
            return Err(contract("invalid capability state"));
        }
        Ok(CapabilityClosure {
            closure_id,
            operations,
            raw,
        })
    }

    pub async fn meta(&self) -> Result<Meta> {
        self.meta_query(BTreeMap::new()).await
    }

    pub async fn application_prompt_meta(&self) -> Result<Meta> {
        self.meta_query(BTreeMap::from([(
            "include".into(),
            "applicationPrompt".into(),
        )]))
        .await
    }

    async fn meta_query(&self, query: BTreeMap<String, String>) -> Result<Meta> {
        let response = self.read("session.get", query).await?;
        let meta = read_meta(object_response(&response, 200)?, self.api().family())?;
        if meta.session_id != self.id() {
            return Err(contract("foreign session metadata"));
        }
        Ok(meta)
    }

    pub async fn send(&self, prompt: &str, options: WriteOptions) -> Result<Accepted> {
        if prompt.trim().is_empty() {
            return Err(invalid("prompt must not be blank"));
        }
        self.accepted(
            "session.message.send",
            None,
            json!({"prompt": prompt}),
            options,
            true,
        )
        .await
    }

    pub async fn send_blocks(&self, blocks: Vec<Block>, options: WriteOptions) -> Result<Accepted> {
        validate_blocks(&blocks, false)?;
        self.accepted(
            "session.message.send",
            None,
            json!({"blocks": blocks}),
            options,
            true,
        )
        .await
    }

    /// Explicit server-side interruption. Dropping a future/stream never calls
    /// this method, nor proves a write was not committed.
    pub async fn interrupt(&self, options: WriteOptions) -> Result<Accepted> {
        self.accepted("session.interrupt", None, json!({}), options, true)
            .await
    }

    /// Close also works for dormant/ended sessions and must not require a live
    /// capability subresource or send a closure header.
    pub async fn close(&self, options: WriteOptions) -> Result<Accepted> {
        let response = self
            .client
            .call(
                "session.close",
                CallOptions {
                    params: self.params(),
                    ..write_options(options)
                },
            )
            .await?;
        accepted_response(&response, self.id(), true)
    }

    pub async fn permission(
        &self,
        ticket_id: &str,
        digest: &str,
        verdict: &str,
        options: WriteOptions,
    ) -> Result<Accepted> {
        nonempty(ticket_id, "permission ticket")?;
        if digest.is_empty() || !matches!(verdict, "allow" | "deny") {
            return Err(invalid(
                "permission requires digest and an allow/deny verdict",
            ));
        }
        self.accepted(
            "session.permission.decide",
            Some(("ticketId", ticket_id)),
            json!({"digest": digest, "verdict": verdict}),
            options,
            false,
        )
        .await
    }

    pub async fn answer(
        &self,
        ticket_id: &str,
        answers: Vec<Answer>,
        options: WriteOptions,
    ) -> Result<Accepted> {
        nonempty(ticket_id, "question ticket")?;
        if answers.is_empty() || answers.iter().any(|a| a.question_id.is_empty()) {
            return Err(invalid(
                "answers require question IDs and selected option arrays",
            ));
        }
        self.accepted(
            "session.question.answer",
            Some(("ticketId", ticket_id)),
            json!({"answers": answers}),
            options,
            false,
        )
        .await
    }

    /// Legacy client-tool receipt only; SDK2 executions use `executor`.
    pub async fn tool_result(
        &self,
        call_id: &str,
        receipt: Value,
        options: WriteOptions,
    ) -> Result<Accepted> {
        nonempty(call_id, "tool call")?;
        if !receipt.is_object() {
            return Err(invalid("receipt must be an object"));
        }
        self.accepted(
            "session.tool.result",
            Some(("targetId", call_id)),
            receipt,
            options,
            false,
        )
        .await
    }

    /// `limit == 0` is a count-only query, never an archive acknowledgement.
    pub async fn history(&self, offset: u64, limit: u64) -> Result<Value> {
        if offset > MAX_SAFE_INTEGER || limit > MAX_SAFE_INTEGER {
            return Err(invalid("history range is unsafe"));
        }
        let response = self
            .read(
                "session.history.read",
                BTreeMap::from([
                    ("offset".into(), offset.to_string()),
                    ("limit".into(), limit.to_string()),
                ]),
            )
            .await?;
        object_response(&response, 200)
    }

    pub async fn input_capabilities(&self) -> Result<Value> {
        object_response(
            &self
                .read("session.input.capabilities", BTreeMap::new())
                .await?,
            200,
        )
    }

    pub async fn submit_input(&self, input: Input, options: WriteOptions) -> Result<Value> {
        validate_input(&input)?;
        if input.ack.as_deref() == Some("durable") {
            let response = self
                .client
                .call(
                    "session.input.capabilities",
                    CallOptions {
                        params: self.params(),
                        cancellation: options.cancellation.clone(),
                        deadline: options.deadline,
                        ..CallOptions::default()
                    },
                )
                .await?;
            let capabilities = object_response(&response, 200)?;
            if capabilities.get("durableAck").and_then(Value::as_bool) != Some(true) {
                return Err(invalid(
                    "durable input ACK is unavailable; no write attempted",
                ));
            }
        }
        let response = self
            .write(
                "session.input.submit",
                None,
                Some(serde_json::to_value(&input)?),
                options,
            )
            .await?;
        let raw = object_response(&response, 202)?;
        if raw["outcome"] != "accepted" {
            return Err(contract("202 input response is not an acceptance receipt"));
        }
        check_input_receipt(
            &raw["receipt"],
            self.id(),
            &input.input_id,
            &input.target,
            input.ack.as_deref(),
        )?;
        Ok(raw)
    }

    pub async fn input_status(&self, input_id: &str, target: InputTarget) -> Result<Value> {
        nonempty(input_id, "input ID")?;
        nonempty(&target.history_epoch, "history epoch")?;
        nonempty(&target.turn_id, "turn ID")?;
        let mut params = self.params();
        params.insert("targetId".into(), input_id.into());
        let response = self
            .client
            .call(
                "session.input.status",
                CallOptions {
                    params,
                    query: BTreeMap::from([
                        ("historyEpoch".into(), target.history_epoch.clone()),
                        ("turnId".into(), target.turn_id.clone()),
                    ]),
                    ..CallOptions::default()
                },
            )
            .await?;
        let raw = object_response(&response, 200)?;
        check_input_receipt(&raw["receipt"], self.id(), input_id, &target, None)?;
        Ok(raw)
    }

    fn params(&self) -> BTreeMap<String, String> {
        BTreeMap::from([("id".into(), self.id().into())])
    }

    async fn read(&self, operation: &str, query: BTreeMap<String, String>) -> Result<ApiResponse> {
        self.client
            .call(
                operation,
                CallOptions {
                    params: self.params(),
                    query,
                    ..CallOptions::default()
                },
            )
            .await
    }

    async fn write(
        &self,
        operation: &str,
        extra: Option<(&str, &str)>,
        body: Option<Value>,
        options: WriteOptions,
    ) -> Result<ApiResponse> {
        self.write_call(
            operation,
            extra,
            CallOptions {
                body,
                ..write_options(options)
            },
        )
        .await
    }

    async fn write_call(
        &self,
        operation: &str,
        extra: Option<(&str, &str)>,
        mut options: CallOptions,
    ) -> Result<ApiResponse> {
        // The caller's deadline covers discovery as well as the mutation. A
        // blocked preflight must not outlive an already-expired write budget.
        let closure = self.capabilities_for(&options).await?;
        if closure.operations.get(operation).map(String::as_str) != Some("enabled") {
            return Err(invalid(&format!(
                "operation {operation} is not enabled; no write attempted"
            )));
        }
        options.params = self.params();
        if let Some((key, value)) = extra {
            options.params.insert(key.into(), value.into());
        }
        options.closure_id = Some(closure.closure_id);
        if matches!(
            operation,
            "session.audio.speak" | "session.audio.transcribe"
        ) {
            options.max_response_bytes = Some(MEDIA_BYTES);
        }
        self.client.call(operation, options).await
    }

    async fn accepted(
        &self,
        operation: &str,
        extra: Option<(&str, &str)>,
        body: Value,
        options: WriteOptions,
        with_session: bool,
    ) -> Result<Accepted> {
        let response = self.write(operation, extra, Some(body), options).await?;
        accepted_response(&response, self.id(), with_session)
    }
}

fn write_options(options: WriteOptions) -> CallOptions {
    CallOptions {
        idempotency_key: options.idempotency_key,
        deadline: options.deadline,
        cancellation: options.cancellation,
        ..CallOptions::default()
    }
}

fn validate_create(options: &CreateOptions, family: &str) -> Result<()> {
    if options.resume.is_some() && options.fork.is_some() {
        return Err(invalid("resume and fork are mutually exclusive"));
    }
    if let Some(resume) = &options.resume {
        nonempty(&resume.session_id, "resume session")?;
    }
    if let Some(fork) = &options.fork {
        nonempty(&fork.session_id, "fork session")?;
        nonempty(&fork.checkpoint_id, "fork checkpoint")?;
    }
    if family == "sdk2-offload-v1" {
        if options.fork.is_some()
            || options.resume.is_none()
                && !options.request_id.as_deref().is_some_and(valid_request_id)
        {
            return Err(invalid(
                "offload create requires a retained requestId and does not support fork",
            ));
        }
    } else if options.request_id.is_some() {
        return Err(invalid(
            "requestId belongs to offload creation; use idempotency_key for SDK1",
        ));
    }
    if let Some(budget) = &options.budget {
        if budget.max_usd.is_some_and(|v| !v.is_finite() || v < 0.0)
            || budget.max_tokens.is_some_and(|v| v > MAX_SAFE_INTEGER)
        {
            return Err(invalid(
                "budget must contain finite nonnegative safe values",
            ));
        }
    }
    Ok(())
}

fn valid_request_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn validate_input(input: &Input) -> Result<()> {
    nonempty(&input.input_id, "input ID")?;
    nonempty(&input.target.history_epoch, "history epoch")?;
    nonempty(&input.target.turn_id, "turn ID")?;
    if [
        &input.input_id,
        &input.target.history_epoch,
        &input.target.turn_id,
    ]
    .iter()
    .any(|s| s.encode_utf16().count() > 128)
    {
        return Err(invalid("input identity exceeds 128 UTF-16 units"));
    }
    if input
        .ack
        .as_deref()
        .is_some_and(|a| !matches!(a, "memory" | "durable"))
    {
        return Err(invalid("input ACK must be memory or durable"));
    }
    match (&input.content.text, &input.content.blocks) {
        (Some(text), None) if !text.is_empty() && text.encode_utf16().count() <= 262_144 => Ok(()),
        (None, Some(blocks)) => validate_blocks(blocks, true),
        _ => Err(invalid("input requires exactly one nonempty text form")),
    }
}

fn validate_blocks(blocks: &[Block], text_only: bool) -> Result<()> {
    if blocks.is_empty() || blocks.len() > 64 {
        return Err(invalid("message requires 1 to 64 blocks"));
    }
    for block in blocks {
        let valid = match block {
            Block::Text { text } => !text.is_empty() && text.encode_utf16().count() <= 262_144,
            Block::Image { mime, data } => {
                !text_only
                    && !data.is_empty()
                    && matches!(
                        mime.as_str(),
                        "image/png" | "image/jpeg" | "image/gif" | "image/webp"
                    )
            }
        };
        if !valid {
            return Err(invalid("unsupported or empty message block"));
        }
    }
    Ok(())
}

fn check_input_receipt(
    raw: &Value,
    session_id: &str,
    input_id: &str,
    target: &InputTarget,
    requested_ack: Option<&str>,
) -> Result<()> {
    if raw["sessionId"] != session_id
        || raw["inputId"] != input_id
        || raw["turnId"] != target.turn_id
        || raw["historyEpoch"] != target.history_epoch
        || raw["source"] != "strict"
        || !matches!(
            raw.get("state").and_then(Value::as_str),
            Some("reserved" | "accepted" | "consumed" | "closed" | "cancelled")
        )
        || !matches!(
            raw.get("durability").and_then(Value::as_str),
            Some("memory" | "durable")
        )
        || requested_ack.is_some_and(|ack| raw["durability"] != ack)
    {
        return Err(contract(
            "input receipt changed identity, state or requested durability",
        ));
    }
    safe_field(raw, "ordinal")?;
    safe_field(raw, "revision")?;
    Ok(())
}

fn accepted_response(response: &ApiResponse, id: &str, with_session: bool) -> Result<Accepted> {
    let raw = object_response(response, if with_session { 202 } else { 200 })?;
    let accepted: Accepted = serde_json::from_value(raw)?;
    if !accepted.accepted || with_session && accepted.session_id.as_deref() != Some(id) {
        return Err(contract("invalid acceptance receipt"));
    }
    Ok(accepted)
}

fn read_meta(raw: Value, family: &str) -> Result<Meta> {
    let session_id = required_str(&raw, "sessionId")?.to_owned();
    let status = required_str(&raw, "status")?.to_owned();
    if !matches!(status.as_str(), "idle" | "running" | "ended") {
        return Err(contract("invalid session status"));
    }
    let live = raw
        .get("live")
        .and_then(Value::as_bool)
        .ok_or_else(|| contract("metadata must contain live"))?;
    let last_seq = safe_field(&raw, "lastSeq")?;
    check_family(&raw, family)?;
    Ok(Meta {
        session_id,
        status,
        live,
        last_seq,
        raw,
    })
}

fn check_family(raw: &Value, family: &str) -> Result<()> {
    if family == "sdk2-offload-v1"
        && (raw["contract"] != family || raw["availability"] != "source-required")
    {
        return Err(contract("offload family/source lifecycle is missing"));
    }
    Ok(())
}

fn object_response(response: &ApiResponse, status: u16) -> Result<Value> {
    if response.status != status || response.meta.content_type != "application/json" {
        return Err(contract("unexpected response status or content type"));
    }
    let value = response.json()?;
    if !value.is_object() {
        return Err(contract("response must be an object"));
    }
    Ok(value)
}

fn required_str<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| contract(&format!("missing or empty {field}")))
}

fn safe_field(value: &Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .filter(|v| *v <= MAX_SAFE_INTEGER)
        .ok_or_else(|| contract(&format!("missing or unsafe {field}")))
}

fn nonempty(value: &str, label: &str) -> Result<()> {
    if value.is_empty() {
        Err(invalid(&format!("{label} must not be empty")))
    } else {
        Ok(())
    }
}

fn invalid(message: &str) -> Error {
    Error::InvalidInput(format!("session: {message}"))
}
fn contract(message: &str) -> Error {
    Error::Contract(format!("session: {message}"))
}
