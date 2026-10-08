use std::{collections::BTreeMap, time::SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

/// Retain these values when reconciling a write. The SDK never invents a key,
/// renews its deadline or automatically repeats a side effect.
#[derive(Clone, Debug, Default)]
pub struct WriteOptions {
    pub idempotency_key: Option<String>,
    pub deadline: Option<SystemTime>,
    pub cancellation: CancellationToken,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResumeReference {
    pub session_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForkReference {
    pub session_id: String,
    pub checkpoint_id: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Budget {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
}

/// Omit `prompt` to subscribe before starting the first turn. `request_id`
/// belongs to offload creation; SDK1 writes instead use `write.idempotency_key`.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget: Option<Budget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_tools: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities_profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume: Option<ResumeReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fork: Option<ForkReference>,
    #[serde(skip)]
    pub write: WriteOptions,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Created {
    pub session_id: String,
    /// True when Serve rebuilt a persisted session. Resuming an already-live
    /// handle attaches to it and returns false; this flag is not a success bit.
    pub resumed: bool,
    pub last_seq: u64,
}

/// Additive metadata, including explicitly requested application prompt data,
/// remains available in `raw`.
#[derive(Clone, Debug)]
pub struct Meta {
    pub session_id: String,
    pub status: String,
    pub live: bool,
    pub last_seq: u64,
    pub raw: Value,
}

#[derive(Clone, Debug)]
pub struct SessionList {
    pub sessions: Vec<Meta>,
    pub total: u64,
}

/// Acceptance is not turn completion or proof that a model consumed material.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Accepted {
    pub accepted: bool,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "lowercase", deny_unknown_fields)]
pub enum Block {
    Text { text: String },
    Image { mime: String, data: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Answer {
    pub question_id: String,
    pub selected_option_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub free_text: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InputTarget {
    pub history_epoch: String,
    pub turn_id: String,
}

/// Exactly one text form is required. Steering never silently starts a turn.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputContent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocks: Option<Vec<Block>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Input {
    pub input_id: String,
    pub target: InputTarget,
    pub content: InputContent,
    /// Omit for the server default, or explicitly select memory/durable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ack: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CapabilityClosure {
    pub closure_id: String,
    pub operations: BTreeMap<String, String>,
    pub raw: Value,
}

#[derive(Clone, Debug)]
pub struct Checkpoint {
    pub checkpoint_id: String,
    pub session_id: String,
    pub message_count: u64,
    pub raw: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CheckpointOption {
    Enabled(bool),
    Labeled {
        #[serde(skip_serializing_if = "Option::is_none")]
        label: Option<String>,
    },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CompactOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<CheckpointOption>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TranscriptionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub audio: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diarize: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SpeechRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub input: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voice: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed: Option<f64>,
}
