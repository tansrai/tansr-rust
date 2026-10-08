use super::*;
use crate::api::{ApiClient, CallOptions, Result};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone)]
pub struct ArchiveClient {
    pub(crate) api: ApiClient,
}
impl ArchiveClient {
    pub fn new(api: ApiClient) -> Self {
        Self { api }
    }
    /// Closing admits no new material work. The returned operation receipt
    /// does not prove drain completion; query the binding until it is closed.
    pub async fn close_binding(&self, input: &BindingCloseRequest) -> Result<MutationReceipt> {
        validate("BindingCloseRequest", input)?;
        let result: MutationReceipt = self
            .call(
                "archive.binding.close",
                "MutationReceipt",
                mutation_options(
                    &input.binding_id,
                    input,
                    &input.request,
                    Some(&input.expected_revision),
                )?,
                200,
            )
            .await?;
        if result.binding_id != input.binding_id
            || result.request != input.request
            || result.operation != "binding-close"
        {
            return Err(integrity());
        }
        Ok(result)
    }
    /// Observe the archive's independent log; processing these events never
    /// advances archive coverage or a conversation event cursor.
    pub async fn events(
        &self,
        binding: &Binding,
        last_event_id: Option<String>,
    ) -> Result<crate::api::EventStream> {
        use futures_util::StreamExt;
        verify_binding(binding)?;
        let mut options = read_options(&binding.binding_id);
        options.last_event_id = last_event_id;
        let mut stream = self.api.events("archive.events.observe", options).await?;
        let id = binding.binding_id.clone();
        let generations = serde_json::to_value(&binding.target.generations)?;
        Ok(Box::pin(async_stream::try_stream! {
            while let Some(event)=stream.next().await {
                let event=event?;validate("EventFrame",&event.raw)?;
                if event.raw.get("bindingId").and_then(|v|v.as_str())!=Some(id.as_str()) || event.raw.get("generations")!=Some(&generations) {Err(integrity())?;}
                yield event;
            }
        }))
    }
    pub(crate) async fn call<T: DeserializeOwned>(
        &self,
        operation: &str,
        definition: &str,
        options: CallOptions,
        status: u16,
    ) -> Result<T> {
        let result = self.api.call(operation, options).await?;
        if result.status != status {
            return Err(integrity());
        }
        let value = result.json()?;
        crate::api::validate_wire(PROTOCOL, definition, &value)?;
        Ok(serde_json::from_value(value)?)
    }
    pub async fn capabilities(&self) -> Result<Capabilities> {
        let value: Capabilities = self
            .call(
                "archive.capabilities",
                "CapabilitiesResponse",
                CallOptions::default(),
                200,
            )
            .await?;
        if value.limits.inflight_reserve_bytes > value.limits.pending_bytes
            || value.archive_ack_formats.len()
                != usize::from(has(&value.capabilities, "archive-transfer-v1"))
        {
            return Err(integrity());
        }
        verify_epoch(Some(&value.operation_epoch), value.limits.epoch_lifetime_ms)?;
        Ok(value)
    }
    pub async fn binding_target(&self, session_id: &str) -> Result<BindingTarget> {
        validate("LegacyId", &session_id)?;
        let value: BindingTarget = self
            .call(
                "archive.binding.target",
                "BindingTargetView",
                read_options(session_id),
                200,
            )
            .await?;
        if value.target.session_id != session_id
            || value.binding_id.is_none()
                && (value.revision != "0" || value.operation_epoch.is_some())
        {
            return Err(integrity());
        }
        verify_epoch(value.operation_epoch.as_ref(), 0)?;
        Ok(value)
    }
    /// Create once. On a lost response, query `creation_operation` using the
    /// original identity; never substitute a second binding request.
    pub async fn create(
        &self,
        session_id: &str,
        source_id: &str,
        request_id: &str,
    ) -> Result<Binding> {
        let input = self
            .prepare_create(session_id, source_id, request_id)
            .await?;
        self.create_binding(&input).await
    }
    /// Discover once and return an immutable request that the host can persist
    /// before calling `create_binding`. Reuse it after transport uncertainty.
    pub async fn prepare_create(
        &self,
        session_id: &str,
        source_id: &str,
        request_id: &str,
    ) -> Result<BindingCreateRequest> {
        let caps = self.capabilities().await?;
        if !has(&caps.capabilities, "archive-transfer-v1")
            || !has(&caps.archive_ack_formats, "split-receipts-v1")
        {
            return Err(integrity());
        }
        let target = self.binding_target(session_id).await?;
        if target.binding_id.is_some() {
            return Err(crate::api::Error::InvalidInput(
                "archive binding exists; reopen it explicitly".into(),
            ));
        }
        let input = BindingCreateRequest {
            protocol: PROTOCOL.into(),
            request: RequestIdentity {
                request_id: request_id.into(),
                operation_epoch: caps.operation_epoch.id,
            },
            target: target.target,
            expected_revision: target.revision,
            required_capabilities: vec!["archive-transfer-v1".into()],
            optional_capabilities: if has(&caps.capabilities, "context-materials-v1") {
                vec!["context-materials-v1".into()]
            } else {
                vec![]
            },
            archive: BindingArchive {
                strategy: "single-authorized-source".into(),
                source_id: source_id.into(),
                durability: "source-ack-with-durable-spool".into(),
                delivery: "required".into(),
                session_availability: "legacy-complete".into(),
                ack_format: "split-receipts-v1".into(),
            },
        };
        validate("BindingCreateRequest", &input)?;
        Ok(input)
    }
    pub async fn create_binding(&self, input: &BindingCreateRequest) -> Result<Binding> {
        validate("BindingCreateRequest", input)?;
        let requested: BTreeSet<_> = input
            .required_capabilities
            .iter()
            .chain(&input.optional_capabilities)
            .collect();
        if requested.len() != input.required_capabilities.len() + input.optional_capabilities.len()
        {
            return Err(integrity());
        }
        let value: Binding = self
            .call(
                "archive.binding.create",
                "BindingView",
                mutation_options("", input, &input.request, Some(&input.expected_revision))?,
                201,
            )
            .await?;
        verify_binding(&value)?;
        let decisions: BTreeSet<_> = value
            .accepted_capabilities
            .iter()
            .chain(value.rejected_capabilities.iter().map(|v| &v.capability))
            .collect();
        if value.target != input.target
            || value.source_id != input.archive.source_id
            || decisions != requested
            || input
                .required_capabilities
                .iter()
                .any(|v| !has(&value.accepted_capabilities, v))
        {
            return Err(integrity());
        }
        Ok(value)
    }
    pub async fn binding(&self, id: &str) -> Result<Binding> {
        validate("Id", &id)?;
        let value: Binding = self
            .call("archive.binding.get", "BindingView", read_options(id), 200)
            .await?;
        if value.binding_id != id {
            return Err(integrity());
        }
        verify_binding(&value)?;
        Ok(value)
    }
    pub async fn status(&self, id: &str) -> Result<Status> {
        validate("Id", &id)?;
        let value: Status = self
            .call("archive.status", "ArchiveStatus", read_options(id), 200)
            .await?;
        if value.binding_id != id {
            return Err(integrity());
        }
        for s in [
            &value.published_through_sequence,
            &value.releasable_through_sequence,
        ]
        .into_iter()
        .flatten()
        {
            if seq(s)? == 0 {
                return Err(integrity());
            }
        }
        if let Some(c) = &value.acknowledged_coverage {
            verify_coverage(c)?;
            if value
                .published_through_sequence
                .as_ref()
                .map(|s| seq(s))
                .transpose()?
                .is_none_or(|s| s < seq(&c.through_sequence).unwrap_or(u64::MAX))
                || value
                    .releasable_through_sequence
                    .as_ref()
                    .map(|s| seq(s))
                    .transpose()?
                    .is_some_and(|s| s > seq(&c.through_sequence).unwrap_or(0))
            {
                return Err(integrity());
            }
        } else if value.releasable_through_sequence.is_some() {
            return Err(integrity());
        }
        Ok(value)
    }
    pub async fn records(&self, binding: &Binding, after: Option<&str>) -> Result<Page> {
        verify_binding(binding)?;
        let mut options = read_options(&binding.binding_id);
        options.query = generation_query(&binding.target.generations);
        options
            .query
            .insert("limit".into(), binding.limits.page_records.to_string());
        options
            .query
            .insert("maxBytes".into(), binding.limits.page_bytes.to_string());
        if let Some(s) = after {
            validate("Sequence", &s)?;
            options.query.insert("afterSequence".into(), s.into());
        }
        options.max_response_bytes = Some(binding.limits.page_bytes);
        let value = self
            .call("archive.records.read", "ArchivePage", options, 200)
            .await?;
        verify_page(binding, after, &value)?;
        Ok(value)
    }
    pub async fn artifact(&self, binding: &Binding, reference: &ArtifactRef) -> Result<Vec<u8>> {
        verify_binding(binding)?;
        validate("ArtifactRef", reference)?;
        if reference.source_id != binding.source_id
            || reference.bytes > binding.limits.attachment_bytes
        {
            return Err(integrity());
        }
        let mut out = Vec::with_capacity(reference.bytes);
        while out.len() < reference.bytes {
            let maximum = binding.limits.chunk_bytes.min(reference.bytes - out.len());
            if maximum == 0 {
                return Err(integrity());
            }
            let mut options = read_options(&binding.binding_id);
            options
                .params
                .insert("targetId".into(), reference.artifact_id.clone());
            options.query = generation_query(&binding.target.generations);
            options.query.insert("offset".into(), out.len().to_string());
            options.query.insert("maxBytes".into(), maximum.to_string());
            options.max_response_bytes = Some(1 << 20);
            let c: ArtifactChunk = self
                .call("archive.artifact.read", "ArtifactChunk", options, 200)
                .await?;
            let bytes = STANDARD.decode(&c.base64).map_err(|_| integrity())?;
            if STANDARD.encode(&bytes) != c.base64
                || c.binding_id != binding.binding_id
                || c.artifact_id != reference.artifact_id
                || c.source_id != binding.source_id
                || c.generations != binding.target.generations
                || c.sha256 != reference.sha256
                || c.total_bytes != reference.bytes
                || c.offset != out.len()
                || c.bytes == 0
                || c.bytes > maximum
                || bytes.len() != c.bytes
                || hash(&bytes) != c.chunk_sha256
            {
                return Err(integrity());
            }
            out.extend(bytes);
        }
        if hash(&out) != reference.sha256 {
            return Err(integrity());
        }
        Ok(out)
    }
    pub async fn acknowledge(&self, ack: &Ack) -> Result<MutationReceipt> {
        validate("ArchiveAckRequest", ack)?;
        verify_coverage(&ack.coverage)?;
        if seq(&ack.coverage.through_sequence)? - seq(&ack.coverage.from_sequence)? >= 128 {
            return Err(integrity());
        }
        let value: MutationReceipt = self
            .call(
                "archive.ack.commit",
                "MutationReceipt",
                mutation_options(
                    &ack.binding_id,
                    ack,
                    &ack.request,
                    Some(&ack.expected_revision),
                )?,
                200,
            )
            .await?;
        if value.binding_id != ack.binding_id
            || value.request != ack.request
            || value.operation != "archive-ack"
        {
            return Err(integrity());
        }
        Ok(value)
    }
    pub async fn operation(
        &self,
        binding_id: &str,
        operation: &str,
        request: &RequestIdentity,
    ) -> Result<MutationReceipt> {
        self.query_operation(Some(binding_id), None, operation, request)
            .await
    }
    pub async fn creation_operation(
        &self,
        session_id: &str,
        request: &RequestIdentity,
    ) -> Result<MutationReceipt> {
        self.query_operation(None, Some(session_id), "binding-create", request)
            .await
    }
    async fn query_operation(
        &self,
        binding_id: Option<&str>,
        session_id: Option<&str>,
        operation: &str,
        request: &RequestIdentity,
    ) -> Result<MutationReceipt> {
        let mut input = json!({"protocol":PROTOCOL,"operation":operation,"request":request});
        let mut query = BTreeMap::from([
            ("protocol".into(), PROTOCOL.into()),
            ("operation".into(), operation.into()),
            ("operationEpoch".into(), request.operation_epoch.clone()),
            ("requestId".into(), request.request_id.clone()),
        ]);
        if let Some(id) = binding_id {
            input["bindingId"] = id.into();
            query.insert("bindingId".into(), id.into());
        }
        if let Some(id) = session_id {
            input["sessionId"] = id.into();
            query.insert("sessionId".into(), id.into());
        }
        validate("OperationStatusRequest", &input)?;
        let value: MutationReceipt = self
            .call(
                "archive.operation.query",
                "MutationReceipt",
                CallOptions {
                    query,
                    ..Default::default()
                },
                200,
            )
            .await?;
        if value.request != *request
            || value.operation != operation
            || binding_id.is_some_and(|id| value.binding_id != id)
        {
            return Err(integrity());
        }
        Ok(value)
    }
}
pub(crate) fn read_options(id: &str) -> CallOptions {
    CallOptions {
        params: BTreeMap::from([("id".into(), id.into())]),
        query: BTreeMap::from([("protocol".into(), PROTOCOL.into())]),
        ..Default::default()
    }
}
pub(crate) fn generation_query(g: &Generations) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("protocol".into(), PROTOCOL.into()),
        ("historyEpoch".into(), g.history_epoch.clone()),
        ("deletionGeneration".into(), g.deletion_generation.clone()),
        ("projectionRevision".into(), g.projection_revision.clone()),
    ])
}
pub(crate) fn mutation_options<T: Serialize>(
    id: &str,
    input: &T,
    request: &RequestIdentity,
    revision: Option<&str>,
) -> Result<CallOptions> {
    Ok(CallOptions {
        params: if id.is_empty() {
            BTreeMap::new()
        } else {
            BTreeMap::from([("id".into(), id.into())])
        },
        body: Some(serde_json::to_value(input)?),
        idempotency_key: Some(request.request_id.clone()),
        if_match: revision.map(|v| format!("\"{v}\"")),
        ..Default::default()
    })
}
pub(crate) fn verify_epoch(epoch: Option<&Epoch>, maximum: usize) -> Result<()> {
    if let Some(epoch) = epoch {
        let format = &time::format_description::well_known::Rfc3339;
        let issued =
            time::OffsetDateTime::parse(&epoch.issued_at, format).map_err(|_| integrity())?;
        let expires =
            time::OffsetDateTime::parse(&epoch.expires_at, format).map_err(|_| integrity())?;
        let delta = (expires - issued).whole_milliseconds();
        if delta <= 0 || maximum > 0 && delta > maximum as i128 {
            return Err(integrity());
        }
    }
    Ok(())
}
pub(crate) fn verify_binding(b: &Binding) -> Result<()> {
    validate("BindingView", b)?;
    verify_epoch(b.operation_epoch.as_ref(), b.limits.epoch_lifetime_ms)?;
    let all: BTreeSet<_> = b
        .accepted_capabilities
        .iter()
        .chain(b.rejected_capabilities.iter().map(|v| &v.capability))
        .collect();
    if b.limits.inflight_reserve_bytes > b.limits.pending_bytes
        || all.len() != b.accepted_capabilities.len() + b.rejected_capabilities.len()
        || has(&b.accepted_capabilities, "archive-transfer-v1")
            != (b.archive_ack_format.as_deref() == Some("split-receipts-v1"))
    {
        return Err(integrity());
    }
    Ok(())
}
pub(crate) fn verify_coverage(c: &Coverage) -> Result<()> {
    validate("Coverage", c)?;
    if seq(&c.from_sequence)? < 1 || seq(&c.through_sequence)? < seq(&c.from_sequence)? {
        return Err(integrity());
    }
    Ok(())
}
pub(crate) fn verify_record(r: &Record, maximum: usize) -> Result<()> {
    validate("ArchiveRecord", r)?;
    if canonical(r)?.len() > maximum {
        return Err(capacity());
    }
    let mut v = serde_json::to_value(r)?;
    v.as_object_mut()
        .ok_or_else(integrity)?
        .remove("recordDigest");
    if domain_hash("tansr.sdk2.record.v1", &canonical(&v)?)? != r.record_digest
        || r.source_event_range
            .as_ref()
            .is_some_and(|v| v.first_seq > v.last_seq)
    {
        return Err(integrity());
    }
    if let Some(p) = &r.projection {
        verify_coverage(&p.coverage)?;
    }
    Ok(())
}
pub(crate) fn verify_page(b: &Binding, after: Option<&str>, p: &Page) -> Result<()> {
    validate("ArchivePage", p)?;
    if p.binding_id != b.binding_id
        || p.generations != b.target.generations
        || p.records.len() > b.limits.page_records
    {
        return Err(integrity());
    }
    if canonical(p)?.len() > b.limits.page_bytes {
        return Err(capacity());
    }
    let mut previous = after.map(seq).transpose()?.unwrap_or(0);
    let mut previous_digest = None;
    let mut ids = BTreeSet::new();
    let mut refs = BTreeMap::new();
    for r in &p.records {
        if seq(&r.sequence)? != previous.checked_add(1).ok_or_else(integrity)?
            || !ids.insert(&r.record_id)
            || r.target.session_id != b.target.session_id
            || r.target.generations != b.target.generations
            || previous == 0 && r.predecessor_digest != "0".repeat(64)
            || previous_digest.is_some_and(|d: &String| r.predecessor_digest != *d)
        {
            return Err(integrity());
        }
        verify_record(r, b.limits.record_bytes)?;
        for reference in std::iter::once(&r.payload).chain(&r.attachments) {
            if reference.source_id != b.source_id
                || reference.bytes > b.limits.attachment_bytes
                || refs
                    .insert(&reference.artifact_id, reference)
                    .is_some_and(|old| old != reference)
            {
                return Err(integrity());
            }
        }
        previous = seq(&r.sequence)?;
        previous_digest = Some(&r.record_digest);
    }
    let next = p.records.last().map(|r| r.sequence.as_str()).or(after);
    if p.next_after_sequence.as_deref() != next {
        return Err(integrity());
    }
    match &p.published_through_sequence {
        None if !p.records.is_empty() || after.is_some() || !p.complete => return Err(integrity()),
        Some(v)
            if seq(v)? < 1
                || previous > seq(v)?
                || p.complete != (previous == seq(v)?)
                || !p.complete && p.records.is_empty() =>
        {
            return Err(integrity());
        }
        _ => {}
    }
    Ok(())
}
pub(crate) fn verify_receipt(
    identity: &Identity,
    ack: &Ack,
    receipt: &MutationReceipt,
) -> Result<()> {
    validate("MutationReceipt", receipt)?;
    if receipt.state != "completed"
        || receipt.operation != "archive-ack"
        || receipt.binding_id != identity.binding_id
        || receipt.binding_id != ack.binding_id
        || receipt.request != ack.request
        || seq(&receipt.revision)? <= seq(&ack.expected_revision)?
    {
        return Err(integrity());
    }
    let mut semantic = serde_json::to_value(ack)?;
    semantic
        .as_object_mut()
        .ok_or_else(integrity)?
        .remove("request");
    let framed = json!({"scope":[identity.application_scope_id,identity.end_user_id],"operation":"archive-ack","semantic":semantic});
    if domain_hash("tansr.sdk2.operation.v1", &canonical(&framed)?)? != receipt.semantic_digest {
        return Err(integrity());
    }
    Ok(())
}
