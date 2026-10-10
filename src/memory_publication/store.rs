use super::*;
use crate::{archive::platform, storage_cipher::Cipher};
use base64::{Engine, engine::general_purpose::STANDARD};
use rand::{RngCore, rngs::OsRng};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    sync::Mutex,
};
use tokio::sync::Semaphore;
use zeroize::Zeroizing;
const FORMAT: &str = "tansr-rust-memory-publication-v1";
const SNAPSHOT_CAP: usize = 64 << 20;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Limits {
    pub max_transfers: usize,
    pub max_staging_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_transfers: 4096,
            max_staging_bytes: MAX_BODY * 2,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<()> {
        if self.max_transfers == 0
            || self.max_transfers > 16384
            || self.max_staging_bytes == 0
            || self.max_staging_bytes > MAX_BODY * 8
        {
            return Err(reject("capacity_exceeded"));
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug)]
pub enum OpenMode {
    Create,
    Reopen,
}
pub struct StoreOptions {
    pub path: PathBuf,
    pub mode: OpenMode,
    /// Supplied by the host/OS key facility, never written to disk. This private
    /// format is always encrypted and has no implicit plaintext fallback.
    pub key: [u8; 32],
    pub identity: Identity,
    pub limits: Limits,
    pub read_context: ReadContext,
    pub authorize_recovery: Option<AuthorizeRecovery>,
}
#[derive(Debug, PartialEq, Eq)]
pub struct Capacity {
    pub stored_transfers: usize,
    pub remaining_transfers: usize,
    pub staging_bytes: usize,
    pub remaining_staging_bytes: usize,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Transfer {
    owner: Owner,
    request: Value,
    status: String,
    received: usize,
    body: Option<String>,
    etag: Option<String>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Publication {
    etag: String,
    body: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct State {
    format: String,
    identity: Identity,
    limits: Limits,
    publication: Option<Publication>,
    transfers: BTreeMap<String, Transfer>,
}
struct Inner {
    path: PathBuf,
    directory: File,
    _lock: File,
    cipher: Cipher,
    aad: Vec<u8>,
    state: State,
    context: ReadContext,
    recovery: Option<AuthorizeRecovery>,
    uncertain: bool,
    fingerprint: Option<String>,
}
/// Bounded encrypted snapshots with a process lock and serialized disk lane.
/// Commit publishes the body and its permanent transfer result in one atomic
/// replacement. Capacity exhaustion never evicts completed transfer identities.
pub struct FileStore {
    inner: Arc<Mutex<Option<Inner>>>,
    lane: Arc<Semaphore>,
    identity: Identity,
}
impl FileStore {
    pub async fn open(options: StoreOptions) -> Result<Self> {
        let identity = options.identity.clone();
        let inner = tokio::task::spawn_blocking(move || Inner::open(options))
            .await
            .map_err(|_| integrity())??;
        Ok(Self {
            inner: Arc::new(Mutex::new(Some(inner))),
            lane: Arc::new(Semaphore::new(1)),
            identity,
        })
    }
    async fn run<T: Send + 'static>(
        &self,
        action: impl FnOnce(&mut Inner, &Owner) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = self
            .lane
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| integrity())?;
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut guard = inner.lock().map_err(|_| integrity())?;
            let inner = guard.as_mut().ok_or_else(|| reject("closed"))?;
            if inner.uncertain { return Err(Error::Unknown("publication reconciliation required; close and reopen".into())); }
            let before = inner.context()?;
            inner.fixed()?;
            let fingerprint = inner.fingerprint.clone();
            let result = action(inner, &before)?;
            if let Err(error) = inner.check(&before) {
                if inner.fingerprint != fingerprint { inner.uncertain = true; return Err(Error::Unknown("publication committed but current authority changed; reopen and query original transfer".into())); }
                return Err(error);
            }
            Ok(result)
        }).await.map_err(|_| Error::Unknown("publication task outcome unknown; reopen original store".into()))?
    }
    pub async fn capacity(&self) -> Result<Capacity> {
        self.run(|inner, _| inner.capacity()).await
    }
    /// Drain the accepted disk operation, release the OS lock and forget the key.
    pub async fn close(&self) -> Result<()> {
        let permit = self
            .lane
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| integrity())?;
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            inner.lock().map_err(|_| integrity())?.take();
            Ok(())
        })
        .await
        .map_err(|_| integrity())?
    }
    /// Copy a consistent snapshot to a fresh encrypted path/key. Preserve the
    /// source and all original transfer IDs; quiesce writers before switching
    /// the host's recovery anchor. An unsuccessful target is never adopted.
    /// A different key is required; independent copies must not fork key use.
    pub async fn copy_to(&self, path: PathBuf, key: [u8; 32]) -> Result<Self> {
        let (state, context, recovery) = self
            .run(move |inner, _| {
                if inner.cipher.same_key(&key) {
                    return Err(Error::InvalidInput(
                        "copy requires a new encryption key".into(),
                    ));
                }
                Ok((
                    inner.state.clone(),
                    inner.context.clone(),
                    inner.recovery.clone(),
                ))
            })
            .await?;
        let identity = state.identity.clone();
        let options = StoreOptions {
            path,
            mode: OpenMode::Create,
            key,
            identity: identity.clone(),
            limits: state.limits,
            read_context: context,
            authorize_recovery: recovery,
        };
        // The first target snapshot already includes every source witness. A
        // crash can never leave a valid empty target that looks migrated.
        let inner = tokio::task::spawn_blocking(move || Inner::open_import(options, Some(state)))
            .await
            .map_err(|_| integrity())??;
        Ok(Self {
            inner: Arc::new(Mutex::new(Some(inner))),
            lane: Arc::new(Semaphore::new(1)),
            identity,
        })
    }
}
#[async_trait]
impl MemoryPublicationStore for FileStore {
    fn identity(&self) -> &Identity {
        &self.identity
    }
    fn atomic_durable_publication(&self) -> bool {
        true
    }
    fn encrypted_at_rest(&self) -> bool {
        true
    }
    async fn execute(&self, input: Value, owner: Owner) -> Result<Value> {
        validate_request(&input, &self.identity)?;
        owner.validate()?;
        self.run(move |inner, before| inner.execute(input, owner, before))
            .await
    }
}
impl Inner {
    fn context(&self) -> Result<Owner> {
        let scope = (self.context)()?;
        scope.validate()?;
        if scope.scope.application_scope_id != self.state.identity.application_scope_id
            || scope.scope.end_user_id != self.state.identity.end_user_id
        {
            return Err(reject("access_denied"));
        }
        Ok(scope)
    }
    fn fixed(&self) -> Result<()> {
        platform::verify_parent_identity(&self.directory, &self.path)?;
        if let Some(expected) = &self.fingerprint {
            if hash(&self.read_blob()?) != *expected {
                return Err(integrity());
            }
        }
        Ok(())
    }
    fn check(&self, before: &Owner) -> Result<()> {
        if self.context()? != *before {
            return Err(reject("context_changed"));
        }
        self.fixed()
    }
    fn read_blob(&self) -> Result<Vec<u8>> {
        let f = platform::open_at(&self.directory, &self.path, false, false)?;
        let mut bytes = Vec::new();
        f.take((SNAPSHOT_CAP + 29) as u64).read_to_end(&mut bytes)?;
        if bytes.len() > SNAPSHOT_CAP + 28 {
            return Err(integrity());
        }
        Ok(bytes)
    }
    fn open(o: StoreOptions) -> Result<Self> {
        Self::open_import(o, None)
    }
    fn open_import(o: StoreOptions, imported: Option<State>) -> Result<Self> {
        o.limits.validate()?;
        validate_request(&request(&o.identity, "head"), &o.identity)?;
        let directory = platform::verify_parent(&o.path)?;
        let name = o
            .path
            .file_name()
            .and_then(|v| v.to_str())
            .ok_or_else(integrity)?;
        let lock = platform::open_at(
            &directory,
            &o.path.with_file_name(format!("{name}.lock")),
            true,
            false,
        )?;
        fs2::FileExt::try_lock_exclusive(&lock).map_err(|_| reject("store_in_use"))?;
        let aad = crate::canonical::encode(&json!([FORMAT, o.identity]))?;
        let mut inner = Self {
            path: o.path,
            directory,
            _lock: lock,
            cipher: Cipher::new(o.key),
            aad,
            state: State {
                format: FORMAT.into(),
                identity: o.identity,
                limits: o.limits,
                publication: None,
                transfers: BTreeMap::new(),
            },
            context: o.read_context,
            recovery: o.authorize_recovery,
            uncertain: false,
            fingerprint: None,
        };
        let before = inner.context()?;
        match o.mode {
            OpenMode::Create => {
                if inner.path.try_exists()? {
                    return Err(reject("target_exists"));
                }
                inner.save(imported.unwrap_or_else(|| inner.state.clone()), &before)?;
            }
            OpenMode::Reopen => {
                let blob = inner.read_blob()?;
                let plain = inner.cipher.open(&blob, &inner.aad)?;
                let state: State =
                    serde_json::from_value(crate::canonical::parse_json(&plain, SNAPSHOT_CAP)?)?;
                inner.validate(&state)?;
                inner.state = state;
                inner.fingerprint = Some(hash(&blob));
            }
        }
        inner.check(&before).map_err(|_| {
            Error::Unknown(
                "publication open authority changed; preserve original and reopen".into(),
            )
        })?;
        Ok(inner)
    }
    fn validate(&self, state: &State) -> Result<()> {
        if state.format != FORMAT
            || state.identity != self.state.identity
            || state.limits != self.state.limits
            || state.transfers.len() > state.limits.max_transfers
        {
            return Err(integrity());
        }
        if let Some(p) = &state.publication {
            let body = decode(&p.body)?;
            if body.is_empty()
                || body.len() > MAX_BODY
                || hash(&body) != p.etag
                || std::str::from_utf8(&body).is_err()
            {
                return Err(integrity());
            }
        }
        let mut staging = 0;
        for (id, t) in &state.transfers {
            validate_request(&t.request, &state.identity)?;
            t.owner.validate()?;
            if t.request["action"] != "begin"
                || t.request["transferId"] != *id
                || t.owner.scope.application_scope_id != state.identity.application_scope_id
                || t.owner.scope.end_user_id != state.identity.end_user_id
            {
                return Err(integrity());
            }
            let size = number(&t.request, "byteLength");
            if t.received > size {
                return Err(integrity());
            }
            match t.status.as_str() {
                "staging" => {
                    let body = decode(t.body.as_deref().ok_or_else(integrity)?)?;
                    if body.len() != t.received || t.etag.is_some() {
                        return Err(integrity());
                    }
                    staging += size;
                }
                "committed"
                    if t.received == size
                        && t.body.is_none()
                        && t.etag.as_deref() == t.request["sha256"].as_str() => {}
                "conflict" if t.received == size && t.body.is_none() && t.etag.is_none() => {}
                _ => return Err(integrity()),
            }
        }
        if staging > state.limits.max_staging_bytes {
            return Err(integrity());
        }
        Ok(())
    }
    fn capacity(&self) -> Result<Capacity> {
        let staging_bytes = self
            .state
            .transfers
            .values()
            .filter(|t| t.status == "staging")
            .map(|t| number(&t.request, "byteLength"))
            .sum();
        Ok(Capacity {
            stored_transfers: self.state.transfers.len(),
            remaining_transfers: self.state.limits.max_transfers - self.state.transfers.len(),
            staging_bytes,
            remaining_staging_bytes: self.state.limits.max_staging_bytes - staging_bytes,
        })
    }
    fn save(&mut self, next: State, before: &Owner) -> Result<()> {
        self.validate(&next)?;
        self.check(before)?;
        let plain = Zeroizing::new(serde_json::to_vec(&next)?);
        // Verify parser limits before promising a snapshot that can reopen.
        crate::canonical::parse_json(&plain, SNAPSHOT_CAP)?;
        let blob = self.cipher.seal(&plain, &self.aad)?;
        let mut nonce = [0; 12];
        OsRng.fill_bytes(&mut nonce);
        let name = self
            .path
            .file_name()
            .and_then(|v| v.to_str())
            .ok_or_else(integrity)?;
        let temporary = self
            .path
            .with_file_name(format!(".{name}.{}.tmp", hash(&nonce)));
        let result = (|| {
            let mut f = platform::open_at(&self.directory, &temporary, true, true)?;
            f.write_all(&blob)?;
            f.flush()?;
            f.sync_all()?;
            drop(f);
            self.check(before)?;
            platform::reject_links(&self.path, true)?;
            if self.fingerprint.is_none() && self.path.try_exists()? {
                return Err(reject("target_exists"));
            }
            if platform::replace_at(&self.directory, &temporary, &self.path).is_err() {
                self.uncertain = true;
                return Err(Error::Unknown(
                    "publication replacement outcome unknown; reopen original store".into(),
                ));
            }
            self.state = next;
            self.fingerprint = Some(hash(&blob));
            Ok(())
        })();
        let _ = platform::remove_at(&self.directory, &temporary);
        result
    }
    fn execute(&mut self, input: Value, owner: Owner, before: &Owner) -> Result<Value> {
        if owner != *before {
            return Err(reject("context_changed"));
        }
        let action = input["action"].as_str().ok_or_else(integrity)?;
        let mut response = request(&self.state.identity, action);
        match action {
            "head" => {
                response["publication"] = match &self.state.publication {
                    None => Value::Null,
                    Some(p) => {
                        json!({"etag":p.etag,"byteLength":decode(&p.body)?.len(),"sha256":p.etag})
                    }
                }
            }
            "read" => {
                let p = self
                    .state
                    .publication
                    .as_ref()
                    .filter(|p| input["etag"] == p.etag)
                    .ok_or_else(|| reject("revision_conflict"))?;
                let body = decode(&p.body)?;
                let offset = number(&input, "offset");
                if offset > body.len() {
                    return Err(reject("invalid_request"));
                }
                let end = (offset + number(&input, "length")).min(body.len());
                let bytes = &body[offset..end];
                for (k, v) in json!({"etag":p.etag,"offset":offset,"byteLength":bytes.len(),"base64":STANDARD.encode(bytes),"payloadDigest":hash(bytes),"nextOffset":end,"complete":end == body.len()}).as_object().ok_or_else(integrity)? { response[k] = v.clone(); }
            }
            _ => {
                let id = input["transferId"].as_str().ok_or_else(integrity)?;
                let row = self.state.transfers.get(id).cloned();
                if let Some(t) = &row {
                    if t.owner != owner {
                        let allowed = action == "query"
                            && self
                                .recovery
                                .as_ref()
                                .is_some_and(|f| f(&self.state.identity, &t.owner, &owner, id));
                        self.check(before)?;
                        if !allowed {
                            return Err(reject("request_conflict"));
                        }
                    }
                }
                let mut next = self.state.clone();
                let mut changed = false;
                match action {
                    "begin" => {
                        if let Some(t) = row {
                            if t.request != input {
                                return Err(reject("request_conflict"));
                            }
                        } else {
                            let capacity = self.capacity()?;
                            if capacity.remaining_transfers == 0
                                || capacity.remaining_staging_bytes < number(&input, "byteLength")
                            {
                                return Err(reject("capacity_exceeded"));
                            }
                            next.transfers.insert(
                                id.into(),
                                Transfer {
                                    owner,
                                    request: input.clone(),
                                    status: "staging".into(),
                                    received: 0,
                                    body: Some(String::new()),
                                    etag: None,
                                },
                            );
                            changed = true;
                        }
                    }
                    "chunk" if row.is_some() => {
                        let t = next.transfers.get_mut(id).ok_or_else(integrity)?;
                        let bytes = decode(input["base64"].as_str().ok_or_else(integrity)?)?;
                        if bytes.len() != number(&input, "byteLength")
                            || hash(&bytes) != input["payloadDigest"]
                        {
                            return Err(reject("integrity_mismatch"));
                        }
                        let offset = number(&input, "offset");
                        if t.status != "staging"
                            || offset + bytes.len() > number(&t.request, "byteLength")
                        {
                            return Err(reject("request_conflict"));
                        }
                        let mut body = decode(t.body.as_deref().ok_or_else(integrity)?)?;
                        if offset < t.received {
                            if offset + bytes.len() > t.received
                                || body[offset..offset + bytes.len()] != bytes
                            {
                                return Err(reject("request_conflict"));
                            }
                        } else {
                            if offset != t.received {
                                return Err(reject("request_conflict"));
                            }
                            body.extend(bytes);
                            t.received = body.len();
                            t.body = Some(STANDARD.encode(body));
                            changed = true;
                        }
                    }
                    "commit" if row.as_ref().is_some_and(|t| t.status == "staging") => {
                        let t = next.transfers.get_mut(id).ok_or_else(integrity)?;
                        let body = decode(t.body.as_deref().ok_or_else(integrity)?)?;
                        if t.received != number(&t.request, "byteLength")
                            || hash(&body) != t.request["sha256"]
                            || std::str::from_utf8(&body).is_err()
                        {
                            return Err(reject("integrity_mismatch"));
                        }
                        if serde_json::to_value(next.publication.as_ref().map(|p| &p.etag))?
                            != t.request["expectedEtag"]
                        {
                            t.status = "conflict".into();
                        } else {
                            let etag = hash(&body);
                            t.status = "committed".into();
                            t.etag = Some(etag.clone());
                            next.publication = Some(Publication {
                                etag,
                                body: STANDARD.encode(body),
                            });
                        }
                        t.body = None;
                        changed = true;
                    }
                    "commit" | "chunk" | "query" => {}
                    _ => return Err(reject("invalid_request")),
                }
                if changed {
                    self.save(next, before)?;
                }
                response["transfer"] = match self.state.transfers.get(id) {
                    None => {
                        json!({"transferId":id,"status":"unknown","receivedBytes":null,"etag":null})
                    }
                    Some(t) => {
                        json!({"transferId":id,"status":t.status,"receivedBytes":t.received,"etag":t.etag})
                    }
                };
            }
        }
        crate::api::validate_wire(CONTRACT, "MemoryPublicationResponse", &response)?;
        Ok(response)
    }
}
fn number(v: &Value, key: &str) -> usize {
    v[key].as_u64().unwrap_or(0) as usize
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn decode(text: &str) -> Result<Vec<u8>> {
    let bytes = STANDARD
        .decode(text)
        .map_err(|_| reject("integrity_mismatch"))?;
    if STANDARD.encode(&bytes) != text {
        return Err(reject("integrity_mismatch"));
    }
    Ok(bytes)
}
