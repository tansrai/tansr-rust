//! Archive wire DTOs mirror the frozen sdk2-ext-v1 contract.
use serde::{Deserialize, Serialize};
pub const PROTOCOL: &str = "sdk2-ext-v1";
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BindingCloseRequest {
    pub protocol: String,
    pub request: RequestIdentity,
    pub binding_id: String,
    pub expected_revision: String,
    pub generations: Generations,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    #[serde(rename = "applicationScopeId")]
    pub application_scope_id: String,
    #[serde(rename = "endUserId")]
    pub end_user_id: String,
    #[serde(rename = "authorizationRevision")]
    pub authorization_revision: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RequestIdentity {
    #[serde(rename = "requestId")]
    pub request_id: String,
    #[serde(rename = "operationEpoch")]
    pub operation_epoch: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Generations {
    #[serde(rename = "historyEpoch")]
    pub history_epoch: String,
    #[serde(rename = "deletionGeneration")]
    pub deletion_generation: String,
    #[serde(rename = "projectionRevision")]
    pub projection_revision: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Target {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "generations")]
    pub generations: Generations,
    #[serde(rename = "sourceSnapshotDigest")]
    pub source_snapshot_digest: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Epoch {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "issuedAt")]
    pub issued_at: String,
    #[serde(rename = "expiresAt")]
    pub expires_at: String,
    #[serde(rename = "state")]
    pub state: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    #[serde(rename = "controlBytes")]
    pub control_bytes: usize,
    #[serde(rename = "recordBytes")]
    pub record_bytes: usize,
    #[serde(rename = "pageRecords")]
    pub page_records: usize,
    #[serde(rename = "pageBytes")]
    pub page_bytes: usize,
    #[serde(rename = "attachmentBytes")]
    pub attachment_bytes: usize,
    #[serde(rename = "chunkBytes")]
    pub chunk_bytes: usize,
    #[serde(rename = "materialConcurrent")]
    pub material_concurrent: usize,
    #[serde(rename = "materialQueue")]
    pub material_queue: usize,
    #[serde(rename = "materialCandidates")]
    pub material_candidates: usize,
    #[serde(rename = "materialBytes")]
    pub material_bytes: usize,
    #[serde(rename = "materialDeadlineMs")]
    pub material_deadline_ms: usize,
    #[serde(rename = "pendingRecords")]
    pub pending_records: usize,
    #[serde(rename = "pendingBytes")]
    pub pending_bytes: usize,
    #[serde(rename = "inflightReserveBytes")]
    pub inflight_reserve_bytes: usize,
    #[serde(rename = "offlineMs")]
    pub offline_ms: usize,
    #[serde(rename = "eventRetentionMs")]
    pub event_retention_ms: usize,
    #[serde(rename = "eventRetentionFrames")]
    pub event_retention_frames: usize,
    #[serde(rename = "eventRetentionBytes")]
    pub event_retention_bytes: usize,
    #[serde(rename = "terminalReceiptRetentionMs")]
    pub terminal_receipt_retention_ms: usize,
    #[serde(rename = "epochLifetimeMs")]
    pub epoch_lifetime_ms: usize,
    #[serde(rename = "materialChunkBytes")]
    pub material_chunk_bytes: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Capabilities {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "availability")]
    pub availability: String,
    #[serde(rename = "capabilities")]
    pub capabilities: Vec<String>,
    #[serde(rename = "limits")]
    pub limits: Limits,
    #[serde(rename = "operationEpoch")]
    pub operation_epoch: Epoch,
    #[serde(rename = "archiveAckFormats")]
    pub archive_ack_formats: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BindingTarget {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "target")]
    pub target: Target,
    #[serde(rename = "revision")]
    pub revision: String,
    #[serde(rename = "bindingId")]
    pub binding_id: Option<String>,
    #[serde(rename = "operationEpoch")]
    pub operation_epoch: Option<Epoch>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "bindingId")]
    pub binding_id: String,
    #[serde(rename = "scope")]
    pub scope: Scope,
    #[serde(rename = "target")]
    pub target: Target,
    #[serde(rename = "revision")]
    pub revision: String,
    #[serde(rename = "state")]
    pub state: String,
    #[serde(rename = "sourceId")]
    pub source_id: String,
    #[serde(rename = "acceptedCapabilities")]
    pub accepted_capabilities: Vec<String>,
    #[serde(rename = "rejectedCapabilities")]
    pub rejected_capabilities: Vec<RejectedCapability>,
    #[serde(rename = "availability")]
    pub availability: String,
    #[serde(rename = "operationEpoch")]
    pub operation_epoch: Option<Epoch>,
    #[serde(rename = "limits")]
    pub limits: Limits,
    #[serde(rename = "archiveAckFormat")]
    pub archive_ack_format: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RejectedCapability {
    #[serde(rename = "capability")]
    pub capability: String,
    #[serde(rename = "reason")]
    pub reason: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BindingCreateRequest {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "request")]
    pub request: RequestIdentity,
    #[serde(rename = "target")]
    pub target: Target,
    #[serde(rename = "expectedRevision")]
    pub expected_revision: String,
    #[serde(rename = "requiredCapabilities")]
    pub required_capabilities: Vec<String>,
    #[serde(rename = "optionalCapabilities")]
    pub optional_capabilities: Vec<String>,
    #[serde(rename = "archive")]
    pub archive: BindingArchive,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BindingArchive {
    #[serde(rename = "strategy")]
    pub strategy: String,
    #[serde(rename = "sourceId")]
    pub source_id: String,
    #[serde(rename = "durability")]
    pub durability: String,
    #[serde(rename = "delivery")]
    pub delivery: String,
    #[serde(rename = "sessionAvailability")]
    pub session_availability: String,
    #[serde(rename = "ackFormat")]
    pub ack_format: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    #[serde(rename = "artifactId")]
    pub artifact_id: String,
    #[serde(rename = "sourceId")]
    pub source_id: String,
    #[serde(rename = "bytes")]
    pub bytes: usize,
    #[serde(rename = "sha256")]
    pub sha256: String,
    #[serde(rename = "mediaType")]
    pub media_type: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Coverage {
    #[serde(rename = "fromSequence")]
    pub from_sequence: String,
    #[serde(rename = "throughSequence")]
    pub through_sequence: String,
    #[serde(rename = "headDigest")]
    pub head_digest: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Record {
    #[serde(rename = "recordId")]
    pub record_id: String,
    #[serde(rename = "sequence")]
    pub sequence: String,
    #[serde(rename = "target")]
    pub target: Target,
    #[serde(rename = "turnId")]
    pub turn_id: String,
    #[serde(rename = "recordKind")]
    pub record_kind: String,
    #[serde(rename = "turnState")]
    pub turn_state: String,
    #[serde(rename = "predecessorDigest")]
    pub predecessor_digest: String,
    #[serde(rename = "recordDigest")]
    pub record_digest: String,
    #[serde(rename = "payload")]
    pub payload: ArtifactRef,
    #[serde(rename = "attachments")]
    pub attachments: Vec<ArtifactRef>,
    #[serde(rename = "sourceEventRange", skip_serializing_if = "Option::is_none")]
    pub source_event_range: Option<EventRange>,
    #[serde(rename = "projection", skip_serializing_if = "Option::is_none")]
    pub projection: Option<Projection>,
    #[serde(rename = "payloadDigest")]
    pub payload_digest: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EventRange {
    #[serde(rename = "firstSeq")]
    pub first_seq: u64,
    #[serde(rename = "lastSeq")]
    pub last_seq: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Projection {
    #[serde(rename = "coverage")]
    pub coverage: Coverage,
    #[serde(rename = "assemblerVersion")]
    pub assembler_version: String,
    #[serde(rename = "sourceHeadDigest")]
    pub source_head_digest: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Page {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "bindingId")]
    pub binding_id: String,
    #[serde(rename = "generations")]
    pub generations: Generations,
    #[serde(rename = "records")]
    pub records: Vec<Record>,
    #[serde(rename = "nextAfterSequence")]
    pub next_after_sequence: Option<String>,
    #[serde(rename = "complete")]
    pub complete: bool,
    #[serde(rename = "publishedThroughSequence")]
    pub published_through_sequence: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactChunk {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "bindingId")]
    pub binding_id: String,
    #[serde(rename = "artifactId")]
    pub artifact_id: String,
    #[serde(rename = "sourceId")]
    pub source_id: String,
    #[serde(rename = "generations")]
    pub generations: Generations,
    #[serde(rename = "offset")]
    pub offset: usize,
    #[serde(rename = "bytes")]
    pub bytes: usize,
    #[serde(rename = "totalBytes")]
    pub total_bytes: usize,
    #[serde(rename = "sha256")]
    pub sha256: String,
    #[serde(rename = "chunkSha256")]
    pub chunk_sha256: String,
    #[serde(rename = "base64")]
    pub base64: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Status {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "bindingId")]
    pub binding_id: String,
    #[serde(rename = "revision")]
    pub revision: String,
    #[serde(rename = "generations")]
    pub generations: Generations,
    #[serde(rename = "sourceId")]
    pub source_id: String,
    #[serde(rename = "sourceGeneration")]
    pub source_generation: String,
    #[serde(rename = "publishedThroughSequence")]
    pub published_through_sequence: Option<String>,
    #[serde(rename = "acknowledgedCoverage")]
    pub acknowledged_coverage: Option<Coverage>,
    #[serde(rename = "releasableThroughSequence")]
    pub releasable_through_sequence: Option<String>,
    #[serde(rename = "pendingBytes")]
    pub pending_bytes: usize,
    #[serde(rename = "pendingRecords")]
    pub pending_records: usize,
    #[serde(rename = "sessionPersistence")]
    pub session_persistence: String,
    #[serde(rename = "state")]
    pub state: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactReceipt {
    #[serde(rename = "artifactId")]
    pub artifact_id: String,
    #[serde(rename = "sha256")]
    pub sha256: String,
    #[serde(rename = "state")]
    pub state: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Ack {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "request")]
    pub request: RequestIdentity,
    #[serde(rename = "bindingId")]
    pub binding_id: String,
    #[serde(rename = "expectedRevision")]
    pub expected_revision: String,
    #[serde(rename = "generations")]
    pub generations: Generations,
    #[serde(rename = "sourceId")]
    pub source_id: String,
    #[serde(rename = "sourceGeneration")]
    pub source_generation: String,
    #[serde(rename = "coverage")]
    pub coverage: Coverage,
    #[serde(rename = "attachments")]
    pub attachments: Vec<ArtifactReceipt>,
    #[serde(rename = "ackFormat")]
    pub ack_format: String,
    #[serde(rename = "payloads")]
    pub payloads: Vec<ArtifactReceipt>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MutationReceipt {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "request")]
    pub request: RequestIdentity,
    #[serde(rename = "bindingId")]
    pub binding_id: String,
    #[serde(rename = "operation")]
    pub operation: String,
    #[serde(rename = "semanticDigest")]
    pub semantic_digest: String,
    #[serde(rename = "state")]
    pub state: String,
    #[serde(rename = "revision")]
    pub revision: String,
    #[serde(rename = "outcomeRef")]
    pub outcome_ref: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    #[serde(rename = "applicationScopeId")]
    pub application_scope_id: String,
    #[serde(rename = "endUserId")]
    pub end_user_id: String,
    #[serde(rename = "bindingId")]
    pub binding_id: String,
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(rename = "generations")]
    pub generations: Generations,
    #[serde(rename = "sourceId")]
    pub source_id: String,
    #[serde(rename = "sourceGeneration")]
    pub source_generation: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Head {
    #[serde(rename = "sequence")]
    pub sequence: String,
    #[serde(rename = "recordDigest")]
    pub record_digest: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaterialRecord {
    #[serde(rename = "recordId")]
    pub record_id: String,
    #[serde(rename = "digest")]
    pub digest: String,
    #[serde(rename = "payload")]
    pub payload: ArtifactRef,
    #[serde(rename = "attachments")]
    pub attachments: Vec<ArtifactRef>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaterialRequest {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "bindingId")]
    pub binding_id: String,
    #[serde(rename = "materialRequestId")]
    pub material_request_id: String,
    #[serde(rename = "target")]
    pub target: Target,
    #[serde(rename = "sourceId")]
    pub source_id: String,
    #[serde(rename = "sourceGeneration")]
    pub source_generation: String,
    #[serde(rename = "requestedRecords")]
    pub requested_records: Vec<MaterialRecord>,
    #[serde(rename = "purpose")]
    pub purpose: String,
    #[serde(rename = "maxBytes")]
    pub max_bytes: usize,
    #[serde(rename = "remainingTtlMs")]
    pub remaining_ttl_ms: usize,
    #[serde(rename = "chunkBytes")]
    pub chunk_bytes: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaterialUploadRef {
    #[serde(rename = "uploadId")]
    pub upload_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaterialRecordResponse {
    #[serde(rename = "recordId")]
    pub record_id: String,
    #[serde(rename = "digest")]
    pub digest: String,
    #[serde(rename = "payload")]
    pub payload: MaterialUploadRef,
    #[serde(rename = "attachments")]
    pub attachments: Vec<MaterialUploadRef>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaterialResponse {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "request")]
    pub request: RequestIdentity,
    #[serde(rename = "bindingId")]
    pub binding_id: String,
    #[serde(rename = "materialRequestId")]
    pub material_request_id: String,
    #[serde(rename = "target")]
    pub target: Target,
    #[serde(rename = "sourceId")]
    pub source_id: String,
    #[serde(rename = "sourceGeneration")]
    pub source_generation: String,
    #[serde(rename = "results")]
    pub results: Vec<MaterialRecordResponse>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaterialReceipt {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "bindingId")]
    pub binding_id: String,
    #[serde(rename = "materialRequestId")]
    pub material_request_id: String,
    #[serde(rename = "state")]
    pub state: String,
    #[serde(rename = "revision")]
    pub revision: String,
    #[serde(rename = "acceptedRecordIds")]
    pub accepted_record_ids: Vec<String>,
    #[serde(rename = "reason", skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaterialUploadChunk {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "bindingId")]
    pub binding_id: String,
    #[serde(rename = "materialRequestId")]
    pub material_request_id: String,
    #[serde(rename = "target")]
    pub target: Target,
    #[serde(rename = "sourceId")]
    pub source_id: String,
    #[serde(rename = "sourceGeneration")]
    pub source_generation: String,
    #[serde(rename = "artifactId")]
    pub artifact_id: String,
    #[serde(rename = "offset")]
    pub offset: usize,
    #[serde(rename = "bytes")]
    pub bytes: usize,
    #[serde(rename = "chunkSha256")]
    pub chunk_sha256: String,
    #[serde(rename = "base64")]
    pub base64: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaterialUploadStatus {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "bindingId")]
    pub binding_id: String,
    #[serde(rename = "materialRequestId")]
    pub material_request_id: String,
    #[serde(rename = "uploadId")]
    pub upload_id: String,
    #[serde(rename = "artifact")]
    pub artifact: ArtifactRef,
    #[serde(rename = "state")]
    pub state: String,
    #[serde(rename = "chunkBytes")]
    pub chunk_bytes: usize,
    #[serde(rename = "receivedOffsets")]
    pub received_offsets: Vec<usize>,
    #[serde(rename = "receivedBytes")]
    pub received_bytes: usize,
    #[serde(rename = "remainingTtlMs")]
    pub remaining_ttl_ms: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AckRebaseRequest {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "bindingId")]
    pub binding_id: String,
    #[serde(rename = "previous")]
    pub previous: Ack,
    #[serde(rename = "request")]
    pub request: RequestIdentity,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AckRebaseReceipt {
    #[serde(rename = "protocol")]
    pub protocol: String,
    #[serde(rename = "bindingId")]
    pub binding_id: String,
    #[serde(rename = "previous")]
    pub previous: Ack,
    #[serde(rename = "request")]
    pub request: RequestIdentity,
    #[serde(rename = "next")]
    pub next: Ack,
    #[serde(rename = "receipt")]
    pub receipt: MutationReceipt,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RebaseEntry {
    #[serde(rename = "intent")]
    pub intent: AckRebaseRequest,
    #[serde(rename = "result")]
    pub result: Option<AckRebaseReceipt>,
    #[serde(rename = "originalReceipt")]
    pub original_receipt: Option<MutationReceipt>,
}
