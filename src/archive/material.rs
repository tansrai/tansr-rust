use super::*;
use crate::api::{CallOptions, Error, Result};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, SystemTime},
};

/// The response is returned separately from submission so hosts can persist its
/// exact identity before the final mutation and reconcile an uncertain result.
#[derive(Clone, Debug)]
pub struct MaterialResult {
    pub response: MaterialResponse,
    pub receipt: MaterialReceipt,
}

fn verify_upload(
    value: &MaterialUploadStatus,
    binding: &str,
    request: &str,
    artifact: &str,
) -> Result<()> {
    validate("MaterialUploadStatus", value)?;
    if value.binding_id != binding
        || value.material_request_id != request
        || value.artifact.artifact_id != artifact
        || value.chunk_bytes == 0
    {
        return Err(integrity());
    }
    let mut previous = None;
    let mut bytes = 0usize;
    for offset in &value.received_offsets {
        if previous.is_some_and(|p| *offset <= p)
            || offset % value.chunk_bytes != 0
            || *offset >= value.artifact.bytes
        {
            return Err(integrity());
        }
        bytes = bytes
            .checked_add(value.chunk_bytes.min(value.artifact.bytes - offset))
            .ok_or_else(capacity)?;
        previous = Some(*offset);
    }
    if bytes != value.received_bytes
        || value.artifact.bytes.div_ceil(value.chunk_bytes) > 16
        || (value.state == "committed") != (value.received_bytes == value.artifact.bytes)
    {
        return Err(integrity());
    }
    Ok(())
}
impl ArchiveClient {
    pub async fn upload_material_chunk(
        &self,
        input: &MaterialUploadChunk,
    ) -> Result<MaterialUploadStatus> {
        self.upload_material_chunk_deadline(input, None).await
    }
    async fn upload_material_chunk_deadline(
        &self,
        input: &MaterialUploadChunk,
        deadline: Option<SystemTime>,
    ) -> Result<MaterialUploadStatus> {
        validate("MaterialUploadChunkRequest", input)?;
        let bytes = STANDARD.decode(&input.base64).map_err(|_| integrity())?;
        if STANDARD.encode(&bytes) != input.base64
            || bytes.len() != input.bytes
            || hash(&bytes) != input.chunk_sha256
        {
            return Err(integrity());
        }
        let options = CallOptions {
            params: BTreeMap::from([
                ("id".into(), input.binding_id.clone()),
                ("targetId".into(), input.material_request_id.clone()),
                ("uploadId".into(), input.artifact_id.clone()),
            ]),
            body: Some(serde_json::to_value(input)?),
            deadline,
            ..Default::default()
        };
        let value: MaterialUploadStatus = self
            .call(
                "material.upload.chunk",
                "MaterialUploadStatus",
                options,
                200,
            )
            .await?;
        verify_upload(
            &value,
            &input.binding_id,
            &input.material_request_id,
            &input.artifact_id,
        )?;
        if value.artifact.source_id != input.source_id
            || input.offset % value.chunk_bytes != 0
            || !value.received_offsets.contains(&input.offset)
            || input.offset >= value.artifact.bytes
            || input.bytes != value.chunk_bytes.min(value.artifact.bytes - input.offset)
            || input.offset == 0
                && input.bytes == value.artifact.bytes
                && value.artifact.sha256 != input.chunk_sha256
        {
            return Err(integrity());
        }
        Ok(value)
    }
    pub async fn material_upload_status(
        &self,
        binding: &str,
        request: &str,
        artifact: &str,
    ) -> Result<MaterialUploadStatus> {
        for id in [binding, request, artifact] {
            validate("Id", &id)?;
        }
        let mut options = client::read_options(binding);
        options.params.insert("targetId".into(), request.into());
        options.params.insert("uploadId".into(), artifact.into());
        let value: MaterialUploadStatus = self
            .call(
                "material.upload.status",
                "MaterialUploadStatus",
                options,
                200,
            )
            .await?;
        verify_upload(&value, binding, request, artifact)?;
        Ok(value)
    }
    /// `received` confirms ingress only. Query `material_status` for eventual
    /// `core-consumed` or another terminal outcome.
    pub async fn submit_materials(&self, input: &MaterialResponse) -> Result<MaterialReceipt> {
        validate("MaterialResponseRequest", input)?;
        let ids: BTreeSet<_> = input.results.iter().map(|r| &r.record_id).collect();
        if ids.len() != input.results.len() {
            return Err(integrity());
        }
        let value: MaterialReceipt = self
            .call(
                "material.response.submit",
                "MaterialReceipt",
                client::mutation_options(&input.binding_id, input, &input.request, None)?,
                202,
            )
            .await?;
        let accepted: BTreeSet<_> = value.accepted_record_ids.iter().collect();
        if value.binding_id != input.binding_id
            || value.material_request_id != input.material_request_id
            || value.state != "received"
            || value.revision == "0"
            || ids != accepted
            || accepted.len() != value.accepted_record_ids.len()
        {
            return Err(integrity());
        }
        Ok(value)
    }
    pub async fn material_status(&self, binding: &str, request: &str) -> Result<MaterialReceipt> {
        validate("Id", &binding)?;
        validate("Id", &request)?;
        let mut options = client::read_options(binding);
        options.params.insert("targetId".into(), request.into());
        let value: MaterialReceipt = self
            .call("material.status", "MaterialReceipt", options, 200)
            .await?;
        if value.binding_id != binding
            || value.material_request_id != request
            || value.state == "pending"
                && (value.revision != "0" || !value.accepted_record_ids.is_empty())
        {
            return Err(integrity());
        }
        Ok(value)
    }
    /// Upload exact selected bytes from a newly received request, then return
    /// an immutable response. This convenience observes `remaining_ttl_ms`
    /// once at this call; do not use it to replay a saved request. Durable jobs
    /// must use `prepare_materials_before` with their original absolute deadline
    /// and persist the response before `submit_materials`.
    pub async fn prepare_materials(
        &self,
        store: &dyn ArchiveStore,
        request: &MaterialRequest,
        identity: RequestIdentity,
    ) -> Result<MaterialResponse> {
        // This convenience is only for a freshly observed event. A durable
        // host job must keep its original deadline and use the explicit form.
        let deadline = SystemTime::now()
            .checked_add(Duration::from_millis(request.remaining_ttl_ms as u64))
            .ok_or_else(integrity)?;
        self.prepare_materials_before(store, request, identity, deadline)
            .await
    }
    /// Prepare or retry a durable material job without renewing its observed
    /// lifetime. Persist `original_deadline` alongside the exact request before
    /// the first attempt; reuse both after restart. Serve remains the authority
    /// for its own original deadline and final material state.
    pub async fn prepare_materials_before(
        &self,
        store: &dyn ArchiveStore,
        request: &MaterialRequest,
        identity: RequestIdentity,
        original_deadline: SystemTime,
    ) -> Result<MaterialResponse> {
        validate("MaterialRequest", request)?;
        validate("RequestIdentity", &identity)?;
        let expected = store.identity();
        let limits = store.limits().validate()?;
        if request.binding_id != expected.binding_id
            || request.source_id != expected.source_id
            || request.source_generation != expected.source_generation
            || request.target.session_id != expected.session_id
            || request.target.generations != expected.generations
            || request.chunk_bytes == 0
        {
            return Err(integrity());
        }
        let deadline = SystemTime::now()
            .checked_add(Duration::from_millis(request.remaining_ttl_ms as u64))
            .ok_or_else(integrity)?
            .min(original_deadline);
        let remaining = deadline.duration_since(SystemTime::now()).map_err(|_| {
            Error::InvalidInput("material original deadline expired; do not renew its TTL".into())
        })?;
        let task = async {
            store.check_access().await?;
            let ids: Vec<_> = request
                .requested_records
                .iter()
                .map(|r| r.record_id.clone())
                .collect();
            let saved = store.records_by_id(&ids).await?;
            if saved.len() != ids.len() {
                return Err(integrity());
            }
            let mut records = BTreeMap::new();
            let mut refs = BTreeMap::new();
            let mut asked_ids = BTreeSet::new();
            let mut total = 0usize;
            for record in &saved {
                client::verify_record(record, 262144)?;
                if records.insert(record.record_id.clone(), record).is_some() {
                    return Err(integrity());
                }
            }
            for asked in &request.requested_records {
                let record = records.get(&asked.record_id).ok_or_else(integrity)?;
                if !asked_ids.insert(&asked.record_id)
                    || record.record_digest != asked.digest
                    || record.payload != asked.payload
                    || record.attachments != asked.attachments
                    || record.target.session_id != request.target.session_id
                    || record.target.generations != request.target.generations
                {
                    return Err(integrity());
                }
                for reference in std::iter::once(&record.payload).chain(&record.attachments) {
                    if reference.source_id != expected.source_id
                        || reference.bytes.div_ceil(request.chunk_bytes) > 16
                    {
                        return Err(integrity());
                    }
                    if let Some(old) = refs.insert(reference.artifact_id.clone(), reference.clone())
                    {
                        if old != *reference {
                            return Err(integrity());
                        }
                    } else {
                        total = total.checked_add(reference.bytes).ok_or_else(capacity)?;
                    }
                }
            }
            if total > request.max_bytes
                || total > limits.max_batch_bytes
                || refs.len() > limits.max_artifacts
            {
                return Err(capacity());
            }
            let mut bodies = BTreeMap::new();
            for (id, reference) in &refs {
                let body = store.body(reference).await?;
                if body.len() != reference.bytes || hash(&body) != reference.sha256 {
                    return Err(integrity());
                }
                bodies.insert(id.clone(), body);
            }
            for record in &saved {
                if domain_hash(
                    "tansr.sdk2.payload.v1",
                    bodies
                        .get(&record.payload.artifact_id)
                        .ok_or_else(integrity)?,
                )? != record.payload_digest
                {
                    return Err(integrity());
                }
            }
            let mut uploads = BTreeMap::new();
            for (id, reference) in &refs {
                let body = bodies.get(id).ok_or_else(integrity)?;
                let mut last = None;
                for (index, chunk) in body.chunks(request.chunk_bytes).enumerate() {
                    store.check_access().await?;
                    let input = MaterialUploadChunk {
                        protocol: PROTOCOL.into(),
                        binding_id: request.binding_id.clone(),
                        material_request_id: request.material_request_id.clone(),
                        target: request.target.clone(),
                        source_id: request.source_id.clone(),
                        source_generation: request.source_generation.clone(),
                        artifact_id: id.clone(),
                        offset: index * request.chunk_bytes,
                        bytes: chunk.len(),
                        chunk_sha256: hash(chunk),
                        base64: STANDARD.encode(chunk),
                    };
                    let status = self
                        .upload_material_chunk_deadline(&input, Some(deadline))
                        .await?;
                    if status.artifact != *reference || status.chunk_bytes != request.chunk_bytes {
                        return Err(integrity());
                    }
                    last = Some(status);
                }
                let last = match last {
                    Some(s) if s.state == "committed" => s,
                    _ => {
                        self.material_upload_status(
                            &request.binding_id,
                            &request.material_request_id,
                            id,
                        )
                        .await?
                    }
                };
                if last.state != "committed" || last.artifact != *reference {
                    return Err(integrity());
                }
                uploads.insert(
                    id.clone(),
                    MaterialUploadRef {
                        upload_id: last.upload_id,
                    },
                );
            }
            let mut results = vec![];
            for asked in &request.requested_records {
                results.push(MaterialRecordResponse {
                    record_id: asked.record_id.clone(),
                    digest: asked.digest.clone(),
                    payload: uploads
                        .get(&asked.payload.artifact_id)
                        .ok_or_else(integrity)?
                        .clone(),
                    attachments: asked
                        .attachments
                        .iter()
                        .map(|a| uploads.get(&a.artifact_id).cloned().ok_or_else(integrity))
                        .collect::<Result<_>>()?,
                });
            }
            let response = MaterialResponse {
                protocol: PROTOCOL.into(),
                request: identity,
                binding_id: request.binding_id.clone(),
                material_request_id: request.material_request_id.clone(),
                target: request.target.clone(),
                source_id: request.source_id.clone(),
                source_generation: request.source_generation.clone(),
                results,
            };
            validate("MaterialResponseRequest", &response)?;
            store.check_access().await?;
            Ok(response)
        };
        tokio::time::timeout(remaining, task).await.map_err(|_| {
            Error::Unknown("material request TTL elapsed; query original upload identities".into())
        })?
    }
}
/// Convenience for a newly received request without crash-resumable submission.
/// For durable jobs, preserve the original absolute deadline, call
/// `prepare_materials_before`, persist that exact response, then use
/// `submit_materials`/`material_status` with the original identities after restart.
pub async fn respond_materials(
    client: &ArchiveClient,
    store: &dyn ArchiveStore,
    request: &MaterialRequest,
    identity: RequestIdentity,
) -> Result<MaterialResult> {
    let response = client.prepare_materials(store, request, identity).await?;
    store.check_access().await?;
    let receipt = client.submit_materials(&response).await?;
    Ok(MaterialResult { response, receipt })
}
