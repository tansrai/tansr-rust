use super::{client::*, *};
use crate::api::{Error, Result};
use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use async_trait::async_trait;
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;
use zeroize::{Zeroize, Zeroizing};

const MAGIC: &[u8] = b"Tansr-Rust-Archive/1\n";
const FORMAT: &str = "tansr-rust-archive-v1";
pub(crate) const REBASE_RESERVE: usize = 528384;

/// Logical bounds. The built-in store rewrites one bounded encrypted snapshot;
/// applications with large archives should implement `ArchiveStore` instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StoreLimits {
    pub max_records: usize,
    pub max_artifacts: usize,
    pub max_stored_bytes: usize,
    pub max_batch_bytes: usize,
}
impl Default for StoreLimits {
    fn default() -> Self {
        Self {
            max_records: 4096,
            max_artifacts: 16384,
            max_stored_bytes: 64 << 20,
            max_batch_bytes: 8 << 20,
        }
    }
}
impl StoreLimits {
    pub(crate) fn validate(self) -> Result<Self> {
        if self.max_records == 0
            || self.max_records > 1_000_000
            || self.max_artifacts == 0
            || self.max_artifacts > 1_000_000
            || self.max_stored_bytes == 0
            || self.max_stored_bytes > 64 << 20
            || self.max_batch_bytes == 0
            || self.max_batch_bytes > self.max_stored_bytes
        {
            return Err(capacity());
        }
        Ok(self)
    }
}
pub type AccessCheck = Arc<dyn Fn(&Identity) -> Result<()> + Send + Sync>;
pub struct StoreOptions {
    pub path: PathBuf,
    /// Caller-provided key, never persisted. Prefer an OS key store in production.
    pub key: [u8; 32],
    pub identity: Identity,
    pub limits: StoreLimits,
    /// Must use live authenticated host state, not fields restored from this file.
    /// The callback runs inside the serialized storage lane and must not reenter
    /// this store or block on its futures.
    pub check_access: AccessCheck,
}

/// Application storage contract. `receive` must atomically and durably save
/// verified bytes and the immutable ACK before returning; `confirm` verifies
/// the original completed scope-framed receipt before advancing coverage.
#[async_trait]
pub trait ArchiveStore: Send + Sync {
    fn identity(&self) -> &Identity;
    fn limits(&self) -> StoreLimits;
    async fn check_access(&self) -> Result<()>;
    async fn head(&self) -> Result<Option<Head>>;
    async fn coverage(&self) -> Result<Option<Coverage>>;
    async fn pending(&self) -> Result<Option<Ack>>;
    async fn receive(
        &self,
        binding: &Binding,
        status: &Status,
        page: &Page,
        bodies: BTreeMap<String, Vec<u8>>,
        request: RequestIdentity,
    ) -> Result<Ack>;
    async fn confirm(&self, receipt: MutationReceipt) -> Result<()>;
    async fn records_by_id(&self, ids: &[String]) -> Result<Vec<Record>>;
    async fn body(&self, reference: &ArtifactRef) -> Result<Vec<u8>>;
    async fn pending_rebase(&self) -> Result<Option<AckRebaseRequest>> {
        Ok(None)
    }
    async fn prepare_rebase(&self, _request: RequestIdentity) -> Result<AckRebaseRequest> {
        Err(Error::InvalidInput(
            "store does not support explicit ACK recovery".into(),
        ))
    }
    async fn confirm_rebase(&self, _result: AckRebaseReceipt) -> Result<()> {
        Err(Error::InvalidInput(
            "store does not support explicit ACK recovery".into(),
        ))
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SavedArtifact {
    reference: ArtifactRef,
    #[serde(with = "encoded_body")]
    body: Vec<u8>,
}
mod encoded_body {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(body: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(body))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let text = String::deserialize(deserializer)?;
        let bytes = STANDARD.decode(&text).map_err(serde::de::Error::custom)?;
        if STANDARD.encode(&bytes) != text {
            return Err(serde::de::Error::custom("non-canonical base64"));
        }
        Ok(bytes)
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct State {
    format: String,
    identity: Identity,
    limits: StoreLimits,
    records: Vec<Record>,
    artifacts: BTreeMap<String, SavedArtifact>,
    pending: Option<Ack>,
    coverage: Option<Coverage>,
    last_receipt: Option<MutationReceipt>,
    rebases: Vec<RebaseEntry>,
}
struct Inner {
    path: PathBuf,
    _directory: File,
    _lock: File,
    key: Zeroizing<[u8; 32]>,
    state: State,
    access: AccessCheck,
    uncertain: bool,
    #[cfg(test)]
    fault: Option<(&'static str, bool)>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        for artifact in self.state.artifacts.values_mut() {
            artifact.body.zeroize();
        }
    }
}
/// The process lock lives for this value's lifetime. File I/O executes on a
/// bounded blocking lane; cancellation never advertises a disk commit as undone.
pub struct FileStore {
    inner: Arc<Mutex<Option<Inner>>>,
    lane: Arc<Semaphore>,
    identity: Identity,
    limits: StoreLimits,
}
impl FileStore {
    pub async fn open(options: StoreOptions) -> Result<Self> {
        let identity = options.identity.clone();
        let limits = options.limits.validate()?;
        let inner = tokio::task::spawn_blocking(move || Inner::open(options))
            .await
            .map_err(|_| Error::Io("archive open task failed".into()))??;
        Ok(Self {
            inner: Arc::new(Mutex::new(Some(inner))),
            lane: Arc::new(Semaphore::new(1)),
            identity,
            limits,
        })
    }
    async fn run<T: Send + 'static>(
        &self,
        action: impl FnOnce(&mut Inner) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = self
            .lane
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Io("archive closed".into()))?;
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut locked = inner
                .lock()
                .map_err(|_| Error::Io("archive lock poisoned".into()))?;
            let state = locked
                .as_mut()
                .ok_or_else(|| Error::Io("archive closed".into()))?;
            state.authorize()?;
            if state.uncertain {
                return Err(Error::Unknown(
                    "durable write uncertain; close and reopen archive".into(),
                ));
            }
            let out = action(state)?;
            state.authorize()?;
            Ok(out)
        })
        .await
        .map_err(|_| {
            Error::Unknown("archive transaction task failed; reopen before retry".into())
        })?
    }
    /// Wait for in-flight storage, release its process lock and forget the key.
    pub async fn close(&self) -> Result<()> {
        let permit = self
            .lane
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Io("archive closed".into()))?;
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            inner
                .lock()
                .map_err(|_| Error::Io("archive lock poisoned".into()))?
                .take();
            Ok(())
        })
        .await
        .map_err(|_| Error::Io("archive close task failed".into()))?
    }
}
impl Inner {
    fn authorize(&self) -> Result<()> {
        (self.access)(&self.state.identity)
    }
    fn open(mut o: StoreOptions) -> Result<Self> {
        let key = Zeroizing::new(o.key);
        o.key.zeroize();
        for id in [
            &o.identity.application_scope_id,
            &o.identity.binding_id,
            &o.identity.source_id,
            &o.identity.source_generation,
        ] {
            validate("Id", id)?;
        }
        for id in [&o.identity.end_user_id, &o.identity.session_id] {
            validate("LegacyId", id)?;
        }
        validate("Generations", &o.identity.generations)?;
        o.limits.validate()?;
        (o.check_access)(&o.identity)?;
        let directory = platform::verify_parent(&o.path)?;
        let name = o
            .path
            .file_name()
            .ok_or_else(integrity)?
            .to_str()
            .ok_or_else(integrity)?;
        let lock_path = o.path.with_file_name(format!("{name}.lock"));
        let lock = platform::open_at(&directory, &lock_path, true, false)?;
        fs2::FileExt::try_lock_exclusive(&lock)
            .map_err(|_| Error::Io("archive is already in use".into()))?;
        let mut inner = Self {
            path: o.path,
            _directory: directory,
            _lock: lock,
            key,
            state: State {
                format: FORMAT.into(),
                identity: o.identity,
                limits: o.limits,
                records: vec![],
                artifacts: BTreeMap::new(),
                pending: None,
                coverage: None,
                last_receipt: None,
                rebases: vec![],
            },
            access: o.check_access,
            uncertain: false,
            #[cfg(test)]
            fault: None,
        };
        if !inner.path.exists() {
            let initial = inner.state.clone();
            inner.save(initial)?;
            return Ok(inner);
        }
        let file = platform::open_at(&inner._directory, &inner.path, false, false)?;
        let cap = inner.state.limits.max_stored_bytes * 2 + (4 << 20);
        let mut blob = Vec::new();
        file.take((cap + 1) as u64).read_to_end(&mut blob)?;
        if blob.len() > cap || !blob.starts_with(MAGIC) || blob.len() < MAGIC.len() + 12 + 16 {
            return Err(integrity());
        }
        let cipher = Aes256Gcm::new_from_slice(inner.key.as_ref()).map_err(|_| integrity())?;
        let sealed = &blob[MAGIC.len()..];
        let plain = Zeroizing::new(
            cipher
                .decrypt(
                    Nonce::from_slice(&sealed[..12]),
                    Payload {
                        msg: &sealed[12..],
                        aad: MAGIC,
                    },
                )
                .map_err(|_| integrity())?,
        );
        let value = crate::canonical::parse_json(&plain, cap)?;
        let state: State = serde_json::from_value(value)?;
        inner.validate_state(&state)?;
        inner.state = state;
        inner.authorize()?;
        Ok(inner)
    }
    fn save(&mut self, next: State) -> Result<()> {
        self.authorize()?;
        self.validate_state(&next)?;
        platform::verify_parent_identity(&self._directory, &self.path)?;
        let plain = Zeroizing::new(serde_json::to_vec(&next)?);
        if plain.len() > next.limits.max_stored_bytes * 2 + (4 << 20) {
            return Err(capacity());
        }
        // The reopening parser also caps JSON depth/nodes. Never ACK a valid
        // logical archive whose serialized snapshot this SDK cannot reopen.
        validate_snapshot_encoding(&plain, next.limits.max_stored_bytes * 2 + (4 << 20))?;
        let cipher = Aes256Gcm::new_from_slice(self.key.as_ref()).map_err(|_| integrity())?;
        let mut nonce = [0u8; 12];
        OsRng.fill_bytes(&mut nonce);
        let encrypted = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: &plain,
                    aad: MAGIC,
                },
            )
            .map_err(|_| integrity())?;
        let mut suffix = [0u8; 12];
        OsRng.fill_bytes(&mut suffix);
        let name = self
            .path
            .file_name()
            .ok_or_else(integrity)?
            .to_str()
            .ok_or_else(integrity)?;
        let temp = self
            .path
            .with_file_name(format!(".{name}.{}.tmp", hash(&suffix)));
        let result = (|| {
            let mut f = platform::open_at(&self._directory, &temp, true, true)?;
            f.write_all(MAGIC)?;
            f.write_all(&nonce)?;
            #[cfg(test)]
            self.test_boundary("partial-write")?;
            f.write_all(&encrypted)?;
            #[cfg(test)]
            self.test_boundary("flush")?;
            f.flush()?;
            #[cfg(test)]
            self.test_boundary("file-sync")?;
            f.sync_all()?;
            drop(f);
            #[cfg(test)]
            self.test_boundary("before-replace")?;
            self.authorize()?;
            platform::reject_links(&self.path, true)?;
            if self.path.exists() {
                drop(platform::open_at(
                    &self._directory,
                    &self.path,
                    false,
                    false,
                )?);
            }
            if let Err(e) = platform::replace_at(&self._directory, &temp, &self.path) {
                self.uncertain = true;
                return Err(e);
            }
            #[cfg(test)]
            self.test_boundary("after-replace")?;
            Ok(())
        })();
        let _ = platform::remove_at(&self._directory, &temp);
        result?;
        self.state = next;
        Ok(())
    }
    #[cfg(test)]
    fn test_boundary(&mut self, name: &'static str) -> Result<()> {
        if let Some((point, crash)) = self.fault {
            if point == name {
                if crash {
                    // Only compiled into this crate's unit-test executable.
                    // Process exit intentionally skips all Rust destructors.
                    std::process::exit(73);
                }
                if point == "after-replace" {
                    self.uncertain = true;
                }
                return Err(Error::Io(format!(
                    "injected archive I/O failure at {point}"
                )));
            }
        }
        Ok(())
    }
    fn head(&self) -> Option<Head> {
        self.state.records.last().map(|r| Head {
            sequence: r.sequence.clone(),
            record_digest: r.record_digest.clone(),
        })
    }
    fn validate_state(&self, s: &State) -> Result<()> {
        if s.format != FORMAT
            || s.identity != self.state.identity
            || s.limits != self.state.limits
            || s.records.len() > s.limits.max_records
            || s.artifacts.len() > s.limits.max_artifacts
        {
            return Err(integrity());
        }
        let mut bytes = 0usize;
        let mut previous = "0".repeat(64);
        let mut ids = BTreeSet::new();
        let mut refs = BTreeSet::new();
        for (i, r) in s.records.iter().enumerate() {
            verify_record(r, 262144)?;
            if seq(&r.sequence)? != i as u64 + 1
                || r.predecessor_digest != previous
                || !ids.insert(&r.record_id)
                || r.target.session_id != s.identity.session_id
                || r.target.generations != s.identity.generations
            {
                return Err(integrity());
            }
            previous = r.record_digest.clone();
            bytes = bytes
                .checked_add(canonical(r)?.len())
                .ok_or_else(capacity)?;
            for reference in std::iter::once(&r.payload).chain(&r.attachments) {
                let saved = s
                    .artifacts
                    .get(&reference.artifact_id)
                    .ok_or_else(integrity)?;
                if saved.reference != *reference
                    || reference.source_id != s.identity.source_id
                    || saved.body.len() != reference.bytes
                    || hash(&saved.body) != reference.sha256
                {
                    return Err(integrity());
                }
                if refs.insert(&reference.artifact_id) {
                    bytes = bytes.checked_add(saved.body.len()).ok_or_else(capacity)?;
                }
            }
            if domain_hash(
                "tansr.sdk2.payload.v1",
                &s.artifacts
                    .get(&r.payload.artifact_id)
                    .ok_or_else(integrity)?
                    .body,
            )? != r.payload_digest
            {
                return Err(integrity());
            }
        }
        if refs.len() != s.artifacts.len() {
            return Err(integrity());
        }
        if let Some(c) = &s.coverage {
            verify_coverage(c)?;
            let receipt = s.last_receipt.as_ref().ok_or_else(integrity)?;
            validate("MutationReceipt", receipt)?;
            if s.records
                .get((seq(&c.through_sequence)? - 1) as usize)
                .is_none_or(|r| r.record_digest != c.head_digest)
                || receipt.binding_id != s.identity.binding_id
                || receipt.operation != "archive-ack"
                || receipt.state != "completed"
                || seq(&receipt.revision)? == 0
            {
                return Err(integrity());
            }
        } else if s.last_receipt.is_some() {
            return Err(integrity());
        }
        if let Some(ack) = &s.pending {
            validate("ArchiveAckRequest", ack)?;
            verify_coverage(&ack.coverage)?;
            if ack.binding_id != s.identity.binding_id
                || ack.source_id != s.identity.source_id
                || ack.source_generation != s.identity.source_generation
                || ack.generations != s.identity.generations
                || seq(&ack.coverage.through_sequence)? != s.records.len() as u64
                || ack.coverage.head_digest != previous
                || seq(&ack.coverage.from_sequence)?
                    != s.coverage
                        .as_ref()
                        .map(|c| seq(&c.through_sequence))
                        .transpose()?
                        .unwrap_or(0)
                        + 1
            {
                return Err(integrity());
            }
        } else if !s.records.is_empty()
            && s.coverage
                .as_ref()
                .map(|c| seq(&c.through_sequence))
                .transpose()?
                != Some(s.records.len() as u64)
        {
            return Err(integrity());
        }
        let mut identities = BTreeSet::new();
        let mut unresolved = 0;
        for row in &s.rebases {
            super::recovery::verify_rebase(&row.intent)?;
            let old = &row.intent.previous;
            for request in [&row.intent.request, &old.request] {
                if !identities.insert((&request.request_id, &request.operation_epoch)) {
                    return Err(integrity());
                }
            }
            if old.binding_id != s.identity.binding_id
                || old.source_id != s.identity.source_id
                || old.source_generation != s.identity.source_generation
                || old.generations != s.identity.generations
                || s.records
                    .get((seq(&old.coverage.through_sequence)? - 1) as usize)
                    .is_none_or(|r| r.record_digest != old.coverage.head_digest)
            {
                return Err(integrity());
            }
            bytes = bytes
                .checked_add(canonical(row)?.len())
                .ok_or_else(capacity)?;
            if let Some(result) = &row.result {
                if row.original_receipt.is_some() {
                    return Err(integrity());
                }
                super::recovery::verify_rebase_result(&row.intent, result)?;
                verify_receipt(&s.identity, &result.next, &result.receipt)?;
            } else if let Some(receipt) = &row.original_receipt {
                verify_receipt(&s.identity, old, receipt)?;
            } else {
                unresolved += 1;
                bytes = bytes.checked_add(REBASE_RESERVE).ok_or_else(capacity)?;
                if s.pending.as_ref() != Some(old) {
                    return Err(integrity());
                }
            }
            if (row.result.is_some() || row.original_receipt.is_some())
                && s.coverage
                    .as_ref()
                    .map(|c| seq(&c.through_sequence))
                    .transpose()?
                    .is_none_or(|n| n < seq(&old.coverage.through_sequence).unwrap_or(u64::MAX))
            {
                return Err(integrity());
            }
        }
        if unresolved > 1 {
            return Err(integrity());
        }
        if bytes > s.limits.max_stored_bytes {
            return Err(capacity());
        }
        Ok(())
    }
    fn reserved(&self, request: &RequestIdentity) -> bool {
        self.state
            .last_receipt
            .as_ref()
            .is_some_and(|r| &r.request == request)
            || self
                .state
                .rebases
                .iter()
                .any(|r| &r.intent.request == request || &r.intent.previous.request == request)
    }
}

fn validate_snapshot_encoding(bytes: &[u8], maximum: usize) -> Result<()> {
    crate::canonical::parse_json(bytes, maximum)
        .map(|_| ())
        .map_err(|_| capacity())
}

#[async_trait]
impl ArchiveStore for FileStore {
    fn identity(&self) -> &Identity {
        &self.identity
    }
    fn limits(&self) -> StoreLimits {
        self.limits
    }
    async fn check_access(&self) -> Result<()> {
        self.run(|_| Ok(())).await
    }
    async fn head(&self) -> Result<Option<Head>> {
        self.run(|s| Ok(s.head())).await
    }
    async fn coverage(&self) -> Result<Option<Coverage>> {
        self.run(|s| Ok(s.state.coverage.clone())).await
    }
    async fn pending(&self) -> Result<Option<Ack>> {
        self.run(|s| Ok(s.state.pending.clone())).await
    }
    async fn records_by_id(&self, ids: &[String]) -> Result<Vec<Record>> {
        if ids.is_empty() || ids.len() > 128 {
            return Err(capacity());
        }
        let ids = ids.to_vec();
        self.run(move |s| {
            let mut seen = BTreeSet::new();
            let mut out = vec![];
            for id in ids {
                validate("Id", &id)?;
                if !seen.insert(id.clone()) {
                    return Err(integrity());
                }
                out.push(
                    s.state
                        .records
                        .iter()
                        .find(|r| r.record_id == id)
                        .ok_or_else(|| Error::Io("requested archive record unavailable".into()))?
                        .clone(),
                );
            }
            Ok(out)
        })
        .await
    }
    async fn body(&self, reference: &ArtifactRef) -> Result<Vec<u8>> {
        validate("ArtifactRef", reference)?;
        let reference = reference.clone();
        self.run(move |s| {
            let body = s
                .state
                .artifacts
                .get(&reference.artifact_id)
                .ok_or_else(integrity)?;
            if body.reference != reference
                || reference.source_id != s.state.identity.source_id
                || body.body.len() != reference.bytes
                || hash(&body.body) != reference.sha256
            {
                return Err(integrity());
            }
            Ok(body.body.clone())
        })
        .await
    }
    async fn receive(
        &self,
        binding: &Binding,
        status: &Status,
        page: &Page,
        bodies: BTreeMap<String, Vec<u8>>,
        request: RequestIdentity,
    ) -> Result<Ack> {
        let binding = binding.clone();
        let status = status.clone();
        let page = page.clone();
        self.run(move |s| {
            if s.state.pending.is_some() {
                return Err(Error::InvalidInput(
                    "resolve durable pending ACK first".into(),
                ));
            }
            let id = Identity::from_binding(&binding, &status)?;
            if id != s.state.identity
                || binding
                    .operation_epoch
                    .as_ref()
                    .is_none_or(|e| e.id != request.operation_epoch)
                || !has(&binding.accepted_capabilities, "archive-transfer-v1")
                || binding.archive_ack_format.as_deref() != Some("split-receipts-v1")
            {
                return Err(integrity());
            }
            if status.published_through_sequence != page.published_through_sequence {
                return Err(Error::Unknown(
                    "archive publication changed while sampling; retry before receiving".into(),
                ));
            }
            validate("RequestIdentity", &request)?;
            if s.reserved(&request) {
                return Err(integrity());
            }
            let head = s.head();
            match (&head, &status.acknowledged_coverage) {
                (None, None) => {}
                (Some(h), Some(c))
                    if c.through_sequence == h.sequence && c.head_digest == h.record_digest => {}
                _ => return Err(integrity()),
            }
            verify_page(&binding, head.as_ref().map(|h| h.sequence.as_str()), &page)?;
            if page.records.first().is_none_or(|r| {
                r.predecessor_digest
                    != head
                        .as_ref()
                        .map(|h| h.record_digest.clone())
                        .unwrap_or_else(|| "0".repeat(64))
            }) {
                return Err(integrity());
            }
            let mut refs = BTreeMap::new();
            let mut payloads = vec![];
            let mut attachments = vec![];
            let mut pseen = BTreeSet::new();
            let mut aseen = BTreeSet::new();
            let mut bytes = 0usize;
            for r in &page.records {
                bytes = bytes
                    .checked_add(canonical(r)?.len())
                    .ok_or_else(capacity)?;
                for (i, reference) in std::iter::once(&r.payload)
                    .chain(&r.attachments)
                    .enumerate()
                {
                    if refs
                        .insert(reference.artifact_id.clone(), reference.clone())
                        .is_none()
                    {
                        bytes = bytes.checked_add(reference.bytes).ok_or_else(capacity)?;
                    }
                    let receipt = ArtifactReceipt {
                        artifact_id: reference.artifact_id.clone(),
                        sha256: reference.sha256.clone(),
                        state: "durably-stored".into(),
                    };
                    if i == 0 && pseen.insert(reference.artifact_id.clone()) {
                        payloads.push(receipt);
                    } else if i > 0 && aseen.insert(reference.artifact_id.clone()) {
                        attachments.push(receipt);
                    }
                }
            }
            if bytes > s.state.limits.max_batch_bytes || refs.len() != bodies.len() {
                return Err(capacity());
            }
            let mut next = s.state.clone();
            for (id, reference) in refs {
                let body = bodies.get(&id).ok_or_else(integrity)?;
                if body.len() != reference.bytes
                    || hash(body) != reference.sha256
                    || next
                        .artifacts
                        .get(&id)
                        .is_some_and(|a| a.reference != reference)
                {
                    return Err(integrity());
                }
                next.artifacts.insert(
                    id,
                    SavedArtifact {
                        reference,
                        body: body.clone(),
                    },
                );
            }
            let first = page.records.first().ok_or_else(integrity)?;
            let last = page.records.last().ok_or_else(integrity)?;
            let ack = Ack {
                protocol: PROTOCOL.into(),
                request,
                binding_id: id.binding_id,
                expected_revision: binding.revision,
                generations: id.generations,
                source_id: id.source_id,
                source_generation: id.source_generation,
                coverage: Coverage {
                    from_sequence: first.sequence.clone(),
                    through_sequence: last.sequence.clone(),
                    head_digest: last.record_digest.clone(),
                },
                attachments,
                ack_format: "split-receipts-v1".into(),
                payloads,
            };
            validate("ArchiveAckRequest", &ack)?;
            if canonical(&ack)?.len() > binding.limits.control_bytes {
                return Err(capacity());
            }
            next.records.extend(page.records);
            next.pending = Some(ack.clone());
            s.save(next)?;
            Ok(ack)
        })
        .await
    }
    async fn confirm(&self, receipt: MutationReceipt) -> Result<()> {
        self.run(move |s| {
            let Some(pending) = s.state.pending.clone() else {
                return if s.state.last_receipt.as_ref() == Some(&receipt) {
                    Ok(())
                } else {
                    Err(integrity())
                };
            };
            verify_receipt(&s.state.identity, &pending, &receipt)?;
            let mut next = s.state.clone();
            next.coverage = Some(pending.coverage.clone());
            next.pending = None;
            next.last_receipt = Some(receipt.clone());
            for row in &mut next.rebases {
                if row.result.is_none()
                    && row.original_receipt.is_none()
                    && row.intent.previous == pending
                {
                    row.original_receipt = Some(receipt.clone());
                }
            }
            s.save(next)
        })
        .await
    }
    async fn pending_rebase(&self) -> Result<Option<AckRebaseRequest>> {
        self.run(|s| {
            Ok(s.state
                .rebases
                .iter()
                .find(|r| r.result.is_none() && r.original_receipt.is_none())
                .map(|r| r.intent.clone()))
        })
        .await
    }
    async fn prepare_rebase(&self, request: RequestIdentity) -> Result<AckRebaseRequest> {
        self.run(move |s| {
            validate("RequestIdentity", &request)?;
            if let Some(row) = s
                .state
                .rebases
                .iter()
                .find(|r| r.result.is_none() && r.original_receipt.is_none())
            {
                return if row.intent.request == request {
                    Ok(row.intent.clone())
                } else {
                    Err(integrity())
                };
            }
            if s.reserved(&request) {
                return Err(integrity());
            }
            let previous = s.state.pending.clone().ok_or_else(integrity)?;
            let intent = AckRebaseRequest {
                protocol: PROTOCOL.into(),
                binding_id: s.state.identity.binding_id.clone(),
                previous,
                request,
            };
            super::recovery::verify_rebase(&intent)?;
            let mut next = s.state.clone();
            next.rebases.push(RebaseEntry {
                intent: intent.clone(),
                result: None,
                original_receipt: None,
            });
            s.save(next)?;
            Ok(intent)
        })
        .await
    }
    async fn confirm_rebase(&self, result: AckRebaseReceipt) -> Result<()> {
        self.run(move |s| {
            let index = s
                .state
                .rebases
                .iter()
                .position(|r| r.intent.request == result.request)
                .ok_or_else(integrity)?;
            let row = &s.state.rebases[index];
            super::recovery::verify_rebase_result(&row.intent, &result)?;
            verify_receipt(&s.state.identity, &result.next, &result.receipt)?;
            if let Some(saved) = &row.result {
                return if saved == &result {
                    Ok(())
                } else {
                    Err(integrity())
                };
            }
            if row.original_receipt.is_some()
                || s.state.pending.as_ref() != Some(&row.intent.previous)
            {
                return Err(integrity());
            }
            let mut next = s.state.clone();
            next.coverage = Some(result.next.coverage.clone());
            next.last_receipt = Some(result.receipt.clone());
            next.pending = None;
            next.rebases[index].result = Some(result);
            s.save(next)
        })
        .await
    }
}

#[cfg(test)]
mod codec_tests {
    use super::*;
    #[test]
    fn serialized_snapshot_must_fit_the_same_node_budget_as_reopen() {
        let small = serde_json::to_vec(&vec!["node"; 1024]).unwrap();
        assert!(validate_snapshot_encoding(&small, 4 << 20).is_ok());
        // Small in bytes, excessive in nodes: a byte-only save check misses it.
        let large = serde_json::to_vec(&vec!["node"; crate::canonical::MAX_NODES + 1]).unwrap();
        assert!(large.len() < 4 << 20);
        assert!(matches!(
            validate_snapshot_encoding(&large, 4 << 20),
            Err(Error::InvalidInput(_))
        ));
    }
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod fault_tests;
