//! Bounded observation transport for one existing authorized execution.
//! It never starts tools. A negotiated feature alone does not establish an
//! operation's output eligibility; Runner checks its actual window first.
use super::{client::params, types::*};
use crate::{
    api::{ApiClient, CallOptions, Error, Result},
    canonical,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

const CONTRACT: &str = "terminal-services-v1";
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TerminalSessionReference {
    pub session_contract: String,
    pub session_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutputOperationReference {
    pub operation_id: String,
    pub request_digest: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutputLimits {
    pub max_control_bytes: usize,
    pub max_block_bytes: usize,
    pub max_batch_bytes: usize,
    pub max_pending_bytes: usize,
    pub max_retained_bytes: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutputBlock {
    pub seq: String,
    pub byte_offset: String,
    pub channel: String,
    pub encoding: String,
    pub byte_length: usize,
    pub payload_digest: String,
    pub base64: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutputSeal {
    pub last_seq: Option<String>,
    pub total_bytes: String,
    pub payload_digest: String,
    pub truncated: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OutputStatus {
    pub contract: String,
    pub operation: OutputOperationReference,
    pub state: String,
    pub accepted_through: Option<String>,
    pub durable_through: Option<String>,
    pub retained_from: Option<String>,
    pub next_byte_offset: Option<String>,
    pub seal: Option<OutputSeal>,
}
#[derive(Clone, Debug)]
pub struct OutputOptions {
    pub session: TerminalSessionReference,
    pub operation: OutputOperationReference,
    pub executor_id: String,
    pub connection_id: String,
    pub limits: OutputLimits,
    pub encoding: String,
    pub cancellation: CancellationToken,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputSnapshot {
    pub pending_bytes: usize,
    pub pending_blocks: usize,
    pub captured_bytes: u64,
    pub dropped_bytes: u64,
    pub truncated: bool,
    pub sealed: bool,
    pub failed: bool,
}
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct OutputBatch {
    contract: &'static str,
    session: TerminalSessionReference,
    operation: OutputOperationReference,
    executor_id: String,
    connection_id: String,
    blocks: Vec<OutputBlock>,
    seal: Option<OutputSeal>,
}
struct Pending {
    block: OutputBlock,
    cost: usize,
}
struct State {
    pending: VecDeque<Pending>,
    pending_bytes: usize,
    next: i64,
    offset: i64,
    ack: i64,
    ack_offset: i64,
    sent: i64,
    hash: Sha256,
    dropped: u64,
    truncated: bool,
    seal: Option<OutputSeal>,
    status: Option<OutputStatus>,
    failed: Option<String>,
    sealed: bool,
    worker_done: bool,
}
struct Shared {
    state: Mutex<State>,
    changed: Notify,
}
struct Owner {
    shared: Arc<Shared>,
    opts: OutputOptions,
    wake: mpsc::Sender<()>,
    cancel: CancellationToken,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
/// Capture never waits for network. Saturation truncates a continuous byte prefix;
/// later capture drains/discards. `finish` is the remote seal-ACK barrier.
#[derive(Clone)]
pub struct OutputWriter {
    owner: Arc<Owner>,
}
fn terminal_validate<T: Serialize>(name: &str, v: &T) -> Result<()> {
    crate::api::validate_wire(CONTRACT, name, &value(v)?)
}
fn sequence(v: &Option<String>) -> Result<i64> {
    match v {
        None => Ok(-1),
        Some(s) => {
            let n = s.parse::<i64>().map_err(|_| invalid("output sequence"))?;
            if n < 0 || n.to_string() != *s {
                return Err(invalid("output sequence"));
            }
            Ok(n)
        }
    }
}
impl OutputWriter {
    /// Must run inside the caller's Tokio runtime. The supplied limits must be
    /// copied from a negotiated execution-stream-v1 binding, never invented.
    pub fn new(api: ApiClient, opts: OutputOptions) -> Result<Self> {
        terminal_validate("SessionReference", &opts.session)?;
        terminal_validate("OperationReference", &opts.operation)?;
        terminal_validate("Limits", &opts.limits)?;
        terminal_validate("Id", &opts.executor_id)?;
        terminal_validate("Id", &opts.connection_id)?;
        if !matches!(opts.encoding.as_str(), "binary" | "utf-8") {
            return Err(invalid("output encoding"));
        }
        if opts.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                pending: VecDeque::new(),
                pending_bytes: 0,
                next: 0,
                offset: 0,
                ack: -1,
                ack_offset: 0,
                sent: -1,
                hash: Sha256::new(),
                dropped: 0,
                truncated: false,
                seal: None,
                status: None,
                failed: None,
                sealed: false,
                worker_done: false,
            }),
            changed: Notify::new(),
        });
        let cancel = opts.cancellation.child_token();
        let (wake, rx) = mpsc::channel(1);
        let worker_shared = shared.clone();
        let worker_opts = opts.clone();
        let worker_cancel = cancel.clone();
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| invalid("output requires caller Tokio runtime"))?;
        runtime.spawn(async move {
            worker(api, worker_opts, worker_shared, worker_cancel, rx).await;
        });
        Ok(Self {
            owner: Arc::new(Owner {
                shared,
                opts,
                wake,
                cancel,
            }),
        })
    }
    /// Returns retained bytes; a short count means truncation, not an upload ACK.
    pub async fn capture(&self, channel: &str, bytes: &[u8]) -> Result<usize> {
        if !matches!(channel, "stdout" | "stderr") {
            return Err(invalid("output channel"));
        }
        let mut s = self
            .owner
            .shared
            .state
            .lock()
            .map_err(|_| invalid("output lock poisoned"))?;
        if s.seal.is_some() {
            return Err(invalid("output capture closed"));
        }
        if s.truncated || s.failed.is_some() || self.owner.cancel.is_cancelled() {
            s.truncated = true;
            s.dropped = s.dropped.saturating_add(bytes.len() as u64);
            return Ok(0);
        }
        let max = self
            .owner
            .opts
            .limits
            .max_block_bytes
            .min(self.owner.opts.limits.max_batch_bytes);
        let mut retained = 0;
        while retained < bytes.len() {
            let remaining = self
                .owner
                .opts
                .limits
                .max_retained_bytes
                .saturating_sub(s.offset as usize);
            let n = max.min(bytes.len() - retained).min(remaining);
            if n == 0 {
                break;
            }
            if s.next == i64::MAX || s.offset.checked_add(n as i64).is_none() {
                break;
            }
            let part = &bytes[retained..retained + n];
            let block = OutputBlock {
                seq: s.next.to_string(),
                byte_offset: s.offset.to_string(),
                channel: channel.into(),
                encoding: self.owner.opts.encoding.clone(),
                byte_length: n,
                payload_digest: format!("{:x}", Sha256::digest(part)),
                base64: STANDARD.encode(part),
            };
            let cost = canonical::encode_limited(
                &value(&block)?,
                self.owner.opts.limits.max_control_bytes,
            )?
            .len();
            if cost
                > self
                    .owner
                    .opts
                    .limits
                    .max_pending_bytes
                    .saturating_sub(s.pending_bytes)
            {
                break;
            }
            // Include framing, not just raw bytes, in the single-request feasibility check.
            if canonical::encode_limited(
                &value(&batch(&self.owner.opts, vec![block.clone()], None))?,
                self.owner.opts.limits.max_control_bytes,
            )
            .is_err()
            {
                break;
            }
            s.hash.update(part);
            s.pending.push_back(Pending { block, cost });
            s.pending_bytes += cost;
            s.next += 1;
            s.offset += n as i64;
            retained += n;
        }
        if retained < bytes.len() {
            s.truncated = true;
            s.dropped = s.dropped.saturating_add((bytes.len() - retained) as u64);
        }
        drop(s);
        let _ = self.owner.wake.try_send(());
        Ok(retained)
    }
    pub fn snapshot(&self) -> Result<OutputSnapshot> {
        let s = self
            .owner
            .shared
            .state
            .lock()
            .map_err(|_| invalid("output lock poisoned"))?;
        Ok(OutputSnapshot {
            pending_bytes: s.pending_bytes,
            pending_blocks: s.pending.len(),
            captured_bytes: s.offset as u64,
            dropped_bytes: s.dropped,
            truncated: s.truncated,
            sealed: s.sealed,
            failed: s.failed.is_some() || self.owner.cancel.is_cancelled(),
        })
    }
    pub fn abort(&self) {
        self.owner.cancel.cancel();
        self.owner.shared.changed.notify_waiters();
    }
    /// Stop network work and await cleanup. This says nothing about remote tool cancellation.
    pub async fn shutdown(&self) -> Result<()> {
        self.abort();
        loop {
            let notified = self.owner.shared.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .owner
                .shared
                .state
                .lock()
                .map_err(|_| invalid("output lock poisoned"))?
                .worker_done
            {
                return Ok(());
            }
            notified.await;
        }
    }
    /// Freezes a prefix digest and waits until the exact seal is acknowledged.
    pub async fn finish(&self) -> Result<OutputStatus> {
        {
            let mut s = self
                .owner
                .shared
                .state
                .lock()
                .map_err(|_| invalid("output lock poisoned"))?;
            if s.seal.is_none() {
                s.seal = Some(OutputSeal {
                    last_seq: if s.next == 0 {
                        None
                    } else {
                        Some((s.next - 1).to_string())
                    },
                    total_bytes: s.offset.to_string(),
                    payload_digest: format!("{:x}", s.hash.clone().finalize()),
                    truncated: s.truncated,
                });
            }
        }
        let _ = self.owner.wake.try_send(());
        loop {
            let notified = self.owner.shared.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let s = self
                    .owner
                    .shared
                    .state
                    .lock()
                    .map_err(|_| invalid("output lock poisoned"))?;
                if let Some(e) = &s.failed {
                    return Err(Error::Unknown(e.clone()));
                }
                if s.sealed && s.worker_done {
                    return s
                        .status
                        .clone()
                        .ok_or_else(|| invalid("sealed status missing"));
                }
            }
            tokio::select! {_=self.owner.cancel.cancelled()=>return Err(Error::Cancelled),_=notified=>{}}
        }
    }
}
fn batch(o: &OutputOptions, blocks: Vec<OutputBlock>, seal: Option<OutputSeal>) -> OutputBatch {
    OutputBatch {
        contract: CONTRACT,
        session: o.session.clone(),
        operation: o.operation.clone(),
        executor_id: o.executor_id.clone(),
        connection_id: o.connection_id.clone(),
        blocks,
        seal,
    }
}
async fn worker(
    api: ApiClient,
    o: OutputOptions,
    shared: Arc<Shared>,
    cancel: CancellationToken,
    mut rx: mpsc::Receiver<()>,
) {
    loop {
        tokio::select! {_=cancel.cancelled()=>break,n=rx.recv()=>if n.is_none(){break;}}
        loop {
            let next = (|| -> Result<Option<OutputBatch>> {
                let mut s = shared
                    .state
                    .lock()
                    .map_err(|_| invalid("output lock poisoned"))?;
                if s.failed.is_some() || s.sealed {
                    return Ok(None);
                }
                if let Some(p) = s.pending.front() {
                    let b = p.block.clone();
                    s.sent = sequence(&Some(b.seq.clone()))?;
                    Ok(Some(batch(&o, vec![b], None)))
                } else if let Some(seal) = &s.seal {
                    Ok(Some(batch(&o, vec![], Some(seal.clone()))))
                } else {
                    Ok(None)
                }
            })();
            let result = match next {
                Ok(Some(b)) => upload(&api, &o, &shared, &cancel, &b).await,
                Ok(None) => break,
                Err(e) => Err(e),
            };
            if let Err(e) = result {
                if let Ok(mut s) = shared.state.lock() {
                    s.failed = Some(e.to_string());
                }
                shared.changed.notify_waiters();
                break;
            }
        }
        if shared
            .state
            .lock()
            .map(|s| s.sealed || s.failed.is_some())
            .unwrap_or(true)
        {
            break;
        }
    }
    if let Ok(mut s) = shared.state.lock() {
        s.worker_done = true;
    }
    shared.changed.notify_waiters();
}
async fn upload(
    api: &ApiClient,
    o: &OutputOptions,
    shared: &Shared,
    cancel: &CancellationToken,
    b: &OutputBatch,
) -> Result<()> {
    terminal_validate("OutputBatchRequest", b)?;
    let body = value(b)?;
    canonical::encode_limited(&body, o.limits.max_control_bytes)?;
    // A failed POST has unknown acceptance. Query that operation before at most
    // one replay of the identical batch. Authorization errors never trigger replay.
    for attempt in 0..2 {
        let r = api
            .call(
                "terminal.output.batch",
                CallOptions {
                    params: params(&[("id", &o.executor_id)]),
                    body: Some(body.clone()),
                    cancellation: cancel.clone(),
                    deadline: Some(SystemTime::now() + Duration::from_secs(30)),
                    max_response_bytes: Some(o.limits.max_control_bytes),
                    ..Default::default()
                },
            )
            .await;
        match r {
            Ok(r) => {
                if r.status != 200 || r.meta.content_type != "application/json" {
                    return Err(invalid("output status HTTP or media type"));
                }
                let status = decode_status(canonical::parse_strict(
                    &r.body,
                    o.limits.max_control_bytes,
                )?)?;
                accept(shared, o, &status)?;
                if acknowledged(shared, b)? {
                    return Ok(());
                }
            }
            Err(Error::Api(e)) if matches!(e.code.as_str(), "unauthorized" | "forbidden") => {
                return Err(Error::Api(e));
            }
            Err(Error::Cancelled) => return Err(Error::Cancelled),
            Err(_) => {}
        }
        let r = api
            .call(
                "terminal.output.status",
                CallOptions {
                    params: params(&[("id", &o.session.session_id)]),
                    query: params(&[
                        ("contract", CONTRACT),
                        ("sessionContract", &o.session.session_contract),
                        ("operationId", &o.operation.operation_id),
                        ("requestDigest", &o.operation.request_digest),
                    ]),
                    cancellation: cancel.clone(),
                    deadline: Some(SystemTime::now() + Duration::from_secs(30)),
                    max_response_bytes: Some(o.limits.max_control_bytes),
                    ..Default::default()
                },
            )
            .await?;
        if r.status != 200 || r.meta.content_type != "application/json" {
            return Err(invalid("output query HTTP or media type"));
        }
        let status = decode_status(canonical::parse_strict(
            &r.body,
            o.limits.max_control_bytes,
        )?)?;
        accept(shared, o, &status)?;
        if acknowledged(shared, b)? {
            return Ok(());
        }
        if attempt == 1 {
            return Err(Error::Unknown(
                "output ACK unknown; original pending bytes retained".into(),
            ));
        }
    }
    Err(Error::Unknown("output ACK unknown".into()))
}
fn acknowledged(shared: &Shared, b: &OutputBatch) -> Result<bool> {
    let s = shared
        .state
        .lock()
        .map_err(|_| invalid("output lock poisoned"))?;
    if let Some(last) = b.blocks.last() {
        Ok(s.ack >= sequence(&Some(last.seq.clone()))?)
    } else {
        Ok(b.seal.is_some() && s.sealed)
    }
}
pub(crate) async fn query_status(
    api: &ApiClient,
    session: &TerminalSessionReference,
    operation: &OutputOperationReference,
    max_bytes: usize,
    cancel: &CancellationToken,
) -> Result<OutputStatus> {
    let response = api
        .call(
            "terminal.output.status",
            CallOptions {
                params: params(&[("id", &session.session_id)]),
                query: params(&[
                    ("contract", CONTRACT),
                    ("sessionContract", &session.session_contract),
                    ("operationId", &operation.operation_id),
                    ("requestDigest", &operation.request_digest),
                ]),
                cancellation: cancel.clone(),
                deadline: Some(SystemTime::now() + Duration::from_secs(30)),
                max_response_bytes: Some(max_bytes),
                ..Default::default()
            },
        )
        .await?;
    if response.status != 200 || response.meta.content_type != "application/json" {
        return Err(invalid("output status HTTP or media type"));
    }
    let status = decode_status(canonical::parse_strict(&response.body, max_bytes)?)?;
    if &status.operation != operation {
        return Err(invalid("output operation identity"));
    }
    Ok(status)
}
fn decode_status(v: serde_json::Value) -> Result<OutputStatus> {
    terminal_validate("OutputStatus", &v)?;
    let s: OutputStatus = serde_json::from_value(v)?;
    validate_status(&s)?;
    Ok(s)
}
fn validate_status(s: &OutputStatus) -> Result<()> {
    let ack = sequence(&s.accepted_through)?;
    let durable = sequence(&s.durable_through)?;
    let retained = sequence(&s.retained_from)?;
    let offset = sequence(&s.next_byte_offset)?;
    if durable > ack || retained > ack {
        return Err(invalid("impossible output watermark"));
    }
    if offset < 0 {
        if s.state != "unavailable"
            || ack != -1
            || durable != -1
            || retained != -1
            || s.seal.is_some()
        {
            return Err(invalid("missing output offset"));
        }
    } else if ack == -1 && offset != 0 || ack != -1 && offset <= ack {
        return Err(invalid("output offset"));
    }
    if let Some(seal) = &s.seal {
        if sequence(&seal.last_seq)? != ack
            || s.next_byte_offset.as_ref() != Some(&seal.total_bytes)
        {
            return Err(invalid("output seal watermark"));
        }
        if ack == -1
            && (seal.total_bytes != "0"
                || seal.payload_digest != format!("{:x}", Sha256::digest([])))
        {
            return Err(invalid("empty output seal"));
        }
    }
    let valid = match s.state.as_str() {
        "complete" => s.seal.as_ref().is_some_and(|s| !s.truncated),
        "truncated" => s.seal.as_ref().is_some_and(|s| s.truncated),
        "available" => ack == -1 && s.seal.is_none(),
        "receiving" => ack != -1 && s.seal.is_none(),
        "gap" => ack != -1,
        "unavailable" => true,
        _ => false,
    };
    if !valid {
        return Err(invalid("output state"));
    }
    Ok(())
}
fn accept(shared: &Shared, o: &OutputOptions, status: &OutputStatus) -> Result<()> {
    validate_status(status)?;
    if status.operation != o.operation {
        return Err(invalid("output operation identity"));
    }
    if matches!(status.state.as_str(), "gap" | "unavailable") {
        return Err(Error::Unknown(
            "output watermark unavailable; do not restart from zero".into(),
        ));
    }
    let mut s = shared
        .state
        .lock()
        .map_err(|_| invalid("output lock poisoned"))?;
    let ack = sequence(&status.accepted_through)?;
    if ack < s.ack || ack > s.sent {
        return Err(invalid("output ACK outside sent range"));
    }
    let offset = if ack == s.ack {
        s.ack_offset
    } else {
        let b = &s
            .pending
            .iter()
            .find(|p| p.block.seq == ack.to_string())
            .ok_or_else(|| invalid("output ACK without pending block"))?
            .block;
        sequence(&Some(b.byte_offset.clone()))?
            .checked_add(b.byte_length as i64)
            .ok_or_else(|| invalid("output offset overflow"))?
    };
    if status.next_byte_offset.as_ref() != Some(&offset.to_string())
        || status.seal.is_some() && status.seal != s.seal
    {
        return Err(invalid("output ACK offset or seal mismatch"));
    }
    while s
        .pending
        .front()
        .is_some_and(|p| p.block.seq.parse::<i64>().is_ok_and(|n| n <= ack))
    {
        let p = s
            .pending
            .pop_front()
            .ok_or_else(|| invalid("output queue"))?;
        s.pending_bytes -= p.cost;
    }
    s.ack = ack;
    s.ack_offset = offset;
    s.sealed = status.seal.is_some();
    s.status = Some(status.clone());
    drop(s);
    shared.changed.notify_waiters();
    Ok(())
}
