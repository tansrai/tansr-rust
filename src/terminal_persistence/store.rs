use super::*;
use crate::{archive::platform, storage_cipher::Cipher};
use base64::{Engine, engine::general_purpose::STANDARD};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;
use zeroize::{Zeroize, Zeroizing};
const FORMAT: &str = "tansr-rust-terminal-persistence-v1";
const META_CAP: usize = 16 << 20;
const RESERVE: usize = 262_144;
const PHYSICAL_RESERVE: usize = 524_288;
const PHYSICAL_NODES: usize = 8192;

/// Honest bounds of this implementation. Metadata is rewritten atomically; it
/// is not a million-entry database. Immutable body/value objects are separate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Limits {
    pub active_transfers: usize,
    pub staging_bytes: usize,
    pub receipt_entries: usize,
    pub transfer_facts: usize,
    pub objects: usize,
    pub retained_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            active_transfers: 8,
            staging_bytes: 32 << 20,
            receipt_entries: 4096,
            transfer_facts: 1024,
            objects: 8192,
            retained_bytes: 128 << 20,
        }
    }
}
impl Limits {
    fn validate(self) -> Result<()> {
        let hard = Self::default();
        if self.active_transfers == 0
            || self.active_transfers > hard.active_transfers
            || self.staging_bytes == 0
            || self.staging_bytes > hard.staging_bytes
            || self.receipt_entries > hard.receipt_entries
            || self.transfer_facts == 0
            || self.transfer_facts > hard.transfer_facts
            || self.objects == 0
            || self.objects > hard.objects
            || self.retained_bytes == 0
            || self.retained_bytes > hard.retained_bytes
        {
            return Err(reject("capacity_exceeded"));
        }
        Ok(())
    }
}
pub struct StoreOptions {
    /// Absolute metadata filename under an already private directory. This is a
    /// new layout; neither open mode interprets a legacy publication snapshot.
    pub path: PathBuf,
    pub mode: OpenMode,
    pub key: [u8; 32],
    pub identity: Identity,
    pub limits: Limits,
    pub read_context: ReadContext,
    pub authorize_recovery: Option<AuthorizeRecovery>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EntryRow {
    ordinal: String,
    entry: Value,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Ticket {
    begin: Value,
    owner: Owner,
    base_root: Option<Value>,
    status: String,
    result: Option<Value>,
    rejection: Option<Value>,
    available: BTreeSet<String>,
    received: BTreeSet<String>,
    pages: BTreeMap<String, Value>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct State {
    format: String,
    identity: Identity,
    limits: Limits,
    root: Option<Value>,
    /// Authenticated local copy fence. No ordinary reopen or wire action clears it.
    #[serde(default)]
    read_only_copy: bool,
    primary: BTreeMap<String, EntryRow>,
    secondary: BTreeMap<String, String>,
    tickets: BTreeMap<String, Ticket>,
    objects: BTreeMap<String, usize>,
    /// A ready object may still be at this already-synced private filename.
    /// The pointer and Progress are published by the same metadata commit.
    pending_objects: BTreeMap<String, String>,
    /// Durable retirement witnesses precede physical removal. Their bytes and
    /// slots remain charged until removal and its confirming metadata commit.
    retiring: BTreeMap<String, usize>,
    orphaned: BTreeSet<String>,
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
/// A validated encrypted copy is not an authority transfer. The target remains
/// durably read-only, including after ordinary reopen and further copies.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CopyReceipt {
    pub path: PathBuf,
    pub source_sha256: String,
    pub destination_sha256: String,
    pub read_only: bool,
    pub cutover: CopyCutover,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CopyCutover {
    Pending,
}
/// One exclusive owner and disk lane. Immutable AEAD objects and one metadata
/// replacement jointly publish root, indexes and the permanent ticket result.
/// Physical GC shares the writer lock; historical results do not pin bodies.
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
            if inner.uncertain {
                return Err(Error::Unknown(
                    "persistence reconciliation required; reopen original store".into(),
                ));
            }
            let before = inner.context()?;
            inner.fixed()?;
            let fingerprint = inner.fingerprint.clone();
            let result = match action(inner, &before) {
                Ok(value) => value,
                Err(error) => {
                    if inner.uncertain || fingerprint != inner.fingerprint {
                        inner.uncertain = true;
                        return Err(Error::Unknown(
                            "persistence write outcome requires original-key reconciliation".into(),
                        ));
                    }
                    return Err(error);
                }
            };
            if let Err(error) = inner.check(&before) {
                if fingerprint != inner.fingerprint {
                    inner.uncertain = true;
                    return Err(Error::Unknown(
                        "persistence committed but authority changed; query original key".into(),
                    ));
                }
                return Err(error);
            }
            Ok(result)
        })
        .await
        .map_err(|_| {
            Error::Unknown("persistence task outcome unknown; reopen original store".into())
        })?
    }
    /// Drain accepted disk operations, then release the owner lock and key.
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
    /// Copy this complete V1 layout under a different key without changing source
    /// bytes. The target metadata file must be inside a NEW directory whose
    /// parent already exists and is private. One no-replace directory publication
    /// exposes the complete encrypted copy; errors retain source and staging.
    ///
    /// This local host operation holds the existing writer lane and authorization.
    /// It preserves roots, indexes, all original tickets and pending progress. It
    /// does not migrate legacy layouts or copy the separate execution journal.
    /// No common fence retires the source, so the target is authenticated read-only
    /// and cutover remains pending. There is deliberately no activation override.
    pub async fn copy_to(&self, path: PathBuf, mut key: [u8; 32]) -> Result<CopyReceipt> {
        let new_key = Zeroizing::new(key);
        key.zeroize();
        self.run(move |inner, before| inner.copy_to(path, new_key, before))
            .await
    }
    /// Cold-copy an original V1 medium without running normal reopen repairs.
    /// Requires Reopen options. Pending immutable files are authenticated in place,
    /// not renamed or deleted; unrecorded objects fail closed rather than being
    /// adopted by a source metadata write. No writable source handle is exposed.
    /// The same new-directory/key and read-only target rules as copy_to apply.
    pub async fn copy_from(
        options: StoreOptions,
        path: PathBuf,
        mut key: [u8; 32],
    ) -> Result<CopyReceipt> {
        let new_key = Zeroizing::new(key);
        key.zeroize();
        if !matches!(options.mode, OpenMode::Reopen) {
            return Err(reject("copy_requires_reopen"));
        }
        tokio::task::spawn_blocking(move || {
            let mut source = Inner::open_mode(options, false)?;
            let before = source.context()?;
            let receipt = source.copy_to(path, new_key, &before)?;
            source.check(&before).map_err(|_| Error::Unknown(
                "persistence copy published but authority changed; preserve target and both keys".into()
            ))?;
            Ok(receipt)
        })
        .await
        .map_err(|_| {
            Error::Unknown(
                "persistence cold-copy outcome unknown; retain all paths and keys".into(),
            )
        })?
    }
    /// Local trusted maintenance only. No remote list/delete action is exposed.
    pub async fn compact(&self) -> Result<()> {
        self.run(|inner, before| inner.compact(before)).await
    }
}
#[async_trait]
impl PersistenceStore for FileStore {
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
        let owner = (self.context)()?;
        owner.validate()?;
        if owner.scope.application_scope_id != self.state.identity.application_scope_id
            || owner.scope.end_user_id != self.state.identity.end_user_id
        {
            return Err(reject("access_denied"));
        }
        Ok(owner)
    }
    fn check(&self, before: &Owner) -> Result<()> {
        if self.context()? != *before {
            return Err(reject("context_changed"));
        }
        self.fixed()
    }
    fn fixed(&self) -> Result<()> {
        platform::verify_parent_identity(&self.directory, &self.path)?;
        if let Some(fingerprint) = &self.fingerprint {
            if hash(&self.read_file(&self.path, META_CAP)?) != *fingerprint {
                return Err(integrity());
            }
        }
        Ok(())
    }
    fn read_file(&self, path: &std::path::Path, cap: usize) -> Result<Vec<u8>> {
        let f = platform::open_at(&self.directory, path, false, false)?;
        let mut bytes = Vec::new();
        f.take((cap + 29) as u64).read_to_end(&mut bytes)?;
        if bytes.len() > cap + 28 {
            return Err(integrity());
        }
        Ok(bytes)
    }
    fn open(o: StoreOptions) -> Result<Self> {
        Self::open_mode(o, true)
    }
    fn open_mode(o: StoreOptions, reconcile: bool) -> Result<Self> {
        o.limits.validate()?;
        validate_request(&request(&o.identity, "head"), &o.identity)?;
        let directory = platform::verify_parent(&o.path)?;
        let name = o
            .path
            .file_name()
            .and_then(|x| x.to_str())
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
                root: None,
                read_only_copy: false,
                primary: BTreeMap::new(),
                secondary: BTreeMap::new(),
                tickets: BTreeMap::new(),
                objects: BTreeMap::new(),
                pending_objects: BTreeMap::new(),
                retiring: BTreeMap::new(),
                orphaned: BTreeSet::new(),
            },
            context: o.read_context,
            recovery: o.authorize_recovery,
            uncertain: false,
            fingerprint: None,
        };
        let before = inner.context()?;
        match o.mode {
            OpenMode::Create => {
                if inner.path.try_exists()? || !inner.inventory()?.is_empty() {
                    return Err(reject("target_exists"));
                }
                inner.save(inner.state.clone(), &before)?;
            }
            OpenMode::Reopen => {
                let blob = inner.read_file(&inner.path, META_CAP)?;
                let plain = inner.cipher.open(&blob, &inner.aad)?;
                let state: State =
                    serde_json::from_value(crate::canonical::parse_json(&plain, META_CAP)?)?;
                if state.format != FORMAT
                    || state.identity != inner.state.identity
                    || state.limits != inner.state.limits
                {
                    return Err(integrity());
                }
                inner.state = state;
                inner.fingerprint = Some(hash(&blob));
                inner.audit()?;
                // Objects written before a lost metadata commit are not reuse
                // authority. Authenticate and charge them before accepting work.
                let inventory = inner.inventory()?;
                let mut next = inner.state.clone();
                let mut changed = false;
                for (key, len) in inventory {
                    if let Some(expected) =
                        next.objects.get(&key).or_else(|| next.retiring.get(&key))
                    {
                        if *expected != len {
                            return Err(integrity());
                        }
                    } else {
                        next.orphaned.insert(key.clone());
                        next.objects.insert(key, len);
                        changed = true;
                    }
                }
                if changed {
                    if !reconcile {
                        return Err(reject("unrecorded_objects_preserve_source"));
                    }
                    inner.save(next, &before)?;
                }
            }
        }
        if inner.state.read_only_copy {
            if !inner.state.pending_objects.is_empty() {
                return Err(integrity());
            }
        } else if reconcile {
            inner.settle_objects(&before)?;
        }
        inner.check(&before)?;
        Ok(inner)
    }
    fn copy_fingerprints(&self) -> Result<BTreeMap<PathBuf, String>> {
        self.audit()?;
        let inventory = self.inventory()?;
        if inventory
            .keys()
            .any(|k| !self.state.objects.contains_key(k) && !self.state.retiring.contains_key(k))
        {
            return Err(integrity());
        }
        let mut files = BTreeMap::new();
        files.insert(
            self.path.clone(),
            hash(&self.read_file(&self.path, META_CAP)?),
        );
        for key in self.state.objects.keys().chain(self.state.retiring.keys()) {
            let path = self.object_path(key)?;
            if path.try_exists()? {
                files.insert(path.clone(), hash(&self.read_file(&path, BLOCK_BYTES)?));
            }
        }
        for name in self.state.pending_objects.values() {
            let path = self.pending_path(name)?;
            if path.try_exists()? {
                files.insert(path.clone(), hash(&self.read_file(&path, BLOCK_BYTES)?));
            }
        }
        Ok(files)
    }
    fn copy_to(
        &mut self,
        path: PathBuf,
        key: Zeroizing<[u8; 32]>,
        before: &Owner,
    ) -> Result<CopyReceipt> {
        if self.cipher.same_key(&key) {
            return Err(reject("new_key_required"));
        }
        if !path.is_absolute()
            || path.components().any(|c| {
                matches!(
                    c,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
        {
            return Err(reject("fresh_absolute_target_required"));
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(integrity)?;
        let target_directory = path.parent().ok_or_else(integrity)?;
        let parent = platform::verify_parent(target_directory)?;
        if target_directory.try_exists()? {
            return Err(reject("target_exists"));
        }
        let parent_path = target_directory
            .parent()
            .ok_or_else(integrity)?
            .canonicalize()?;
        let target_directory =
            parent_path.join(target_directory.file_name().ok_or_else(integrity)?);
        let target_path = target_directory.join(name);
        self.check(before)?;
        let source_files = self.copy_fingerprints()?;
        let mut nonce = [0u8; 16];
        OsRng.fill_bytes(&mut nonce);
        let staging = parent_path.join(format!(
            ".persistence-copy-{:032x}",
            u128::from_be_bytes(nonce)
        ));
        platform::create_private_directory_new(&staging)?;
        let staged_path = staging.join(name);
        let directory = platform::verify_parent(&staged_path)?;
        let lock = platform::open_at(
            &directory,
            &staging.join(format!("{name}.lock")),
            true,
            true,
        )?;
        fs2::FileExt::try_lock_exclusive(&lock).map_err(|_| reject("store_in_use"))?;
        let mut state = self.state.clone();
        // Physical pending renames become canonical immutable files; accepted
        // transfer progress and original begin/result facts stay byte-equivalent.
        state.pending_objects.clear();
        state.read_only_copy = true;
        let mut staging_state = state.clone();
        staging_state.read_only_copy = false;
        let mut target = Self {
            path: staged_path,
            directory,
            _lock: lock,
            cipher: Cipher::new(*key),
            aad: self.aad.clone(),
            state: staging_state,
            context: self.context.clone(),
            recovery: self.recovery.clone(),
            uncertain: false,
            fingerprint: None,
        };
        for (object, len) in state.objects.iter().chain(state.retiring.iter()) {
            self.check(before)?;
            if !state.objects.contains_key(object) && !self.object_path(object)?.try_exists()? {
                continue;
            }
            let plain = self.object(object)?;
            if plain.len() != *len {
                return Err(integrity());
            }
            let sealed = target.cipher.seal(&plain, &target.object_aad(object)?)?;
            let mut file =
                platform::open_at(&target.directory, &target.object_path(object)?, true, true)?;
            file.write_all(&sealed)?;
            file.sync_all()?;
            self.check(before)?;
        }
        target.audit()?;
        target.save(state, before)?;
        target.audit()?;
        let destination_sha256 = target.fingerprint.clone().ok_or_else(integrity)?;
        #[cfg(unix)]
        target.directory.sync_all()?;
        drop(target);
        self.check(before)?;
        if source_files != self.copy_fingerprints()? {
            return Err(integrity());
        }
        // Windows requires all staged handles closed. The OS primitive refuses
        // replacement if another actor created the target during the copy.
        platform::publish_directory_at(&parent, &staging, &target_directory).map_err(|_| {
            Error::Unknown("persistence copy publication uncertain; retain source, target, staging and both keys".into())
        })?;
        let checked = (|| {
            let target = Self::open(StoreOptions {
                path: target_path.clone(),
                mode: OpenMode::Reopen,
                key: *key,
                identity: self.state.identity.clone(),
                limits: self.state.limits,
                read_context: self.context.clone(),
                authorize_recovery: self.recovery.clone(),
            })?;
            if !target.state.read_only_copy
                || target.fingerprint.as_ref() != Some(&destination_sha256)
            {
                return Err(integrity());
            }
            self.check(before)?;
            if source_files != self.copy_fingerprints()? {
                return Err(integrity());
            }
            Ok(())
        })();
        checked.map_err(|_| Error::Unknown("persistence copy published; preserve both paths and keys for read-only reconciliation".into()))?;
        Ok(CopyReceipt {
            path: target_path,
            source_sha256: self.fingerprint.clone().ok_or_else(integrity)?,
            destination_sha256,
            read_only: true,
            cutover: CopyCutover::Pending,
        })
    }
    fn object_path(&self, key: &str) -> Result<PathBuf> {
        let (kind, digest) = split_key(key)?;
        Ok(self.path.with_file_name(format!(
            "{}.object.{kind}.{digest}",
            self.path
                .file_name()
                .and_then(|x| x.to_str())
                .ok_or_else(integrity)?
        )))
    }
    fn object_aad(&self, key: &str) -> Result<Vec<u8>> {
        crate::canonical::encode(&json!([FORMAT, self.state.identity, "object", key]))
    }
    fn object(&self, key: &str) -> Result<Zeroizing<Vec<u8>>> {
        let canonical = self.object_path(key)?;
        let path = if canonical.try_exists()? {
            canonical
        } else if let Some(name) = self.state.pending_objects.get(key) {
            self.pending_path(name)?
        } else {
            canonical
        };
        let bytes = self.read_file(&path, BLOCK_BYTES)?;
        let plain = self.cipher.open(&bytes, &self.object_aad(key)?)?;
        if plain.is_empty() || plain.len() > BLOCK_BYTES || hash(&plain) != split_key(key)?.1 {
            return Err(integrity());
        }
        if self
            .state
            .objects
            .get(key)
            .is_some_and(|len| *len != plain.len())
        {
            return Err(integrity());
        }
        Ok(plain)
    }
    fn inventory(&self) -> Result<BTreeMap<String, usize>> {
        let prefix = format!(
            "{}.object.",
            self.path
                .file_name()
                .and_then(|x| x.to_str())
                .ok_or_else(integrity)?
        );
        let mut found = BTreeMap::new();
        for entry in std::fs::read_dir(self.path.parent().ok_or_else(integrity)?)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_str().ok_or_else(integrity)?;
            if let Some(suffix) = name.strip_prefix(&prefix) {
                let (kind, digest) = suffix.rsplit_once('.').ok_or_else(integrity)?;
                let key = format!("{kind}:{digest}");
                split_key(&key)?;
                let bytes = self.object(&key)?;
                found.insert(key, bytes.len());
                if found.len() > self.state.limits.objects {
                    return Err(reject("capacity_exceeded"));
                }
            }
        }
        Ok(found)
    }
    fn pending_path(&self, name: &str) -> Result<PathBuf> {
        let prefix = format!(
            ".{}.",
            self.path
                .file_name()
                .and_then(|x| x.to_str())
                .ok_or_else(integrity)?
        );
        let suffix = name
            .strip_prefix(&prefix)
            .and_then(|x| x.strip_suffix(".tmp"))
            .ok_or_else(integrity)?;
        if suffix.len() != 64
            || !suffix
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(integrity());
        }
        Ok(self.path.with_file_name(name))
    }
    fn write_object(&mut self, key: &str, bytes: &[u8], before: &Owner) -> Result<Option<PathBuf>> {
        self.check(before)?;
        let path = self.object_path(key)?;
        if path.try_exists()? || self.state.pending_objects.contains_key(key) {
            if self.object(key)?.as_slice() != bytes {
                return Err(integrity());
            }
            return Ok(None);
        }
        let blob = self.cipher.seal(bytes, &self.object_aad(key)?)?;
        let temporary = self.temporary()?;
        let result = (|| {
            let mut f = platform::open_at(&self.directory, &temporary, true, true)?;
            f.write_all(&blob)?;
            f.sync_all()?;
            drop(f);
            self.check(before)?;
            Ok(Some(temporary.clone()))
        })();
        if result.is_err() {
            let _ = platform::remove_at(&self.directory, &temporary);
        }
        result
    }
    fn settle_objects(&mut self, before: &Owner) -> Result<()> {
        if self.state.pending_objects.is_empty() {
            return Ok(());
        }
        for (key, name) in &self.state.pending_objects {
            self.check(before)?;
            let temporary = self.pending_path(name)?;
            let target = self.object_path(key)?;
            let expected = self.object(key)?;
            if target.try_exists()? {
                if temporary.try_exists()? {
                    let sealed = self.read_file(&temporary, BLOCK_BYTES)?;
                    if self
                        .cipher
                        .open(&sealed, &self.object_aad(key)?)?
                        .as_slice()
                        != expected.as_slice()
                    {
                        return Err(integrity());
                    }
                    self.check(before)?;
                    platform::remove_at(&self.directory, &temporary)?;
                }
            } else {
                self.check(before)?;
                self.uncertain = true;
                platform::replace_at(&self.directory, &temporary, &target)
                    .map_err(|_| integrity())?;
            }
        }
        let mut next = self.state.clone();
        next.pending_objects.clear();
        self.save(next, before)
    }
    fn temporary(&self) -> Result<PathBuf> {
        let mut nonce = [0u8; 12];
        OsRng.fill_bytes(&mut nonce);
        Ok(self.path.with_file_name(format!(
            ".{}.{}.tmp",
            self.path
                .file_name()
                .and_then(|x| x.to_str())
                .ok_or_else(integrity)?,
            hash(&nonce)
        )))
    }
    fn save(&mut self, next: State, before: &Owner) -> Result<()> {
        if self.state.read_only_copy || (next.read_only_copy && self.fingerprint.is_some()) {
            return Err(reject("copy_read_only"));
        }
        self.check(before)?;
        let plain = Zeroizing::new(serde_json::to_vec(&next)?);
        // Additional physical metadata bound, independent of wire logical quota.
        crate::canonical::parse_json(&plain, META_CAP).map_err(|_| reject("capacity_exceeded"))?;
        let blob = self.cipher.seal(&plain, &self.aad)?;
        let temporary = self.temporary()?;
        let result = (|| {
            let mut f = platform::open_at(&self.directory, &temporary, true, true)?;
            f.write_all(&blob)?;
            f.sync_all()?;
            drop(f);
            self.check(before)?;
            platform::reject_links(&self.path, true)?;
            if self.fingerprint.is_none() && self.path.try_exists()? {
                return Err(reject("target_exists"));
            }
            if platform::replace_at(&self.directory, &temporary, &self.path).is_err() {
                self.uncertain = true;
                return Err(integrity());
            }
            self.state = next;
            self.fingerprint = Some(hash(&blob));
            self.uncertain = false;
            Ok(())
        })();
        let _ = platform::remove_at(&self.directory, &temporary);
        result
    }
    fn plan(&self, t: &Ticket) -> Result<Plan> {
        let body = &t.begin["body"];
        let index = &t.begin["index"];
        let mut plan = Plan {
            objects: BTreeMap::new(),
            body: vec![None; num(body, "blockCount")],
            entries: vec![None; num(index, "entryCount")],
            pages: Vec::new(),
            complete: true,
        };
        for (kind, hashes, per_page) in [
            ("body-page", array(&body["pageHashes"])?, 64usize),
            ("index-page", array(&index["pageHashes"])?, 32usize),
        ] {
            for (position, digest) in hashes.iter().enumerate() {
                let key = object_key(kind, string(digest)?);
                if !t.available.contains(&key) {
                    plan.pages.push(false);
                    plan.complete = false;
                    continue;
                }
                // Pages are strictly validated on admission and authenticated
                // again during reopen. Progress never reparses the same schema
                // once per block; commit still verifies the physical objects.
                let page = t.pages.get(&key).ok_or_else(integrity)?;
                add_ref(
                    &mut plan.objects,
                    &key,
                    *self.state.objects.get(&key).ok_or_else(integrity)?,
                )?;
                plan.pages.push(true);
                if kind == "body-page" {
                    for (j, r) in array(&page["refs"])?.iter().enumerate() {
                        add_ref(
                            &mut plan.objects,
                            &object_key("body-block", string(&r["sha256"])?),
                            num(r, "byteLength"),
                        )?;
                        plan.body[position * per_page + j] = Some(r.clone());
                    }
                } else {
                    for (j, e) in array(&page["entries"])?.iter().enumerate() {
                        add_ref(
                            &mut plan.objects,
                            &object_key("receipt-value", string(&e["value"]["sha256"])?),
                            num(&e["value"], "byteLength"),
                        )?;
                        plan.entries[position * per_page + j] = Some(e.clone());
                    }
                }
            }
        }
        if plan.complete {
            if plan.objects.len() != num(&t.begin["declared"], "objects")
                || plan.objects.values().sum::<usize>() != num(&t.begin["declared"], "bytes")
            {
                return Err(reject("invalid_request"));
            }
            let mut previous = None;
            let mut secondary = BTreeSet::new();
            for entry in &plan.entries {
                let entry = entry.as_ref().ok_or_else(integrity)?;
                let primary = string(&entry["primaryKey"])?;
                if previous.is_some_and(|p| p >= primary)
                    || !secondary.insert(string(&entry["secondaryKey"])?)
                {
                    return Err(reject("request_conflict"));
                }
                previous = Some(primary);
            }
        }
        Ok(plan)
    }
    fn transfer(&self, t: &Ticket) -> Result<Value> {
        let progress = if t.status == "staging" {
            let p = self.plan(t)?;
            let body = p
                .body
                .iter()
                .map(|r| {
                    r.as_ref().is_some_and(|r| {
                        t.available.contains(&object_key(
                            "body-block",
                            r["sha256"].as_str().unwrap_or(""),
                        ))
                    })
                })
                .collect::<Vec<_>>();
            let values = p
                .entries
                .iter()
                .map(|e| {
                    e.as_ref().is_some_and(|e| {
                        t.available.contains(&object_key(
                            "receipt-value",
                            e["value"]["sha256"].as_str().unwrap_or(""),
                        ))
                    })
                })
                .collect::<Vec<_>>();
            let received = t
                .received
                .iter()
                .map(|k| self.state.objects.get(k).copied().ok_or_else(integrity))
                .collect::<Result<Vec<_>>>()?
                .iter()
                .sum::<usize>();
            json!({"pagesReady":bits(&p.pages),"bodyReady":bits(&body),"valuesReady":bits(&values),"receivedBytes":received})
        } else {
            Value::Null
        };
        Ok(
            json!({"transferId":t.begin["transferId"],"intentSha256":t.begin["intentSha256"],"status":t.status,"progress":progress,"result":t.result,"rejection":t.rejection}),
        )
    }
    fn capacity(&self) -> Result<Value> {
        let mut retained = 0;
        let mut reserved = 0;
        let mut reserved_objects = 0;
        let mut reserved_entries = 0;
        let mut active = 0;
        let mut staging = 0;
        for (key, len) in self.state.objects.iter().chain(&self.state.retiring) {
            retained += len + object_metadata(key, *len)?;
        }
        for row in self.state.primary.values() {
            retained += encoded_len(&json!({"ordinal":row.ordinal,"entry":row.entry}))?;
        }
        if let Some(root) = &self.state.root {
            retained += encoded_len(root)?;
        }
        for t in self.state.tickets.values() {
            let transfer = self.transfer(t)?;
            let meta = encoded_len(
                &json!({"begin":t.begin,"owner":t.owner,"baseRoot":t.base_root,"transfer":transfer}),
            )?;
            retained += meta;
            if t.status == "staging" {
                active += 1;
                let mut ready_raw = 0;
                let mut reused_raw = 0;
                let mut ready_meta = 0;
                for k in &t.available {
                    let len = *self.state.objects.get(k).ok_or_else(integrity)?;
                    ready_raw += len;
                    ready_meta += object_metadata(k, len)?;
                    if !t.received.contains(k) {
                        reused_raw += len;
                    }
                }
                let declared = &t.begin["declared"];
                if t.available.len() > num(declared, "objects")
                    || ready_raw > num(declared, "bytes")
                    || meta + ready_meta > RESERVE
                {
                    return Err(reject("invalid_request"));
                }
                reserved += num(declared, "bytes") - ready_raw + RESERVE - meta - ready_meta;
                reserved_objects += num(declared, "objects") - t.available.len();
                reserved_entries += num(&t.begin["index"], "addedCount");
                staging += num(declared, "bytes") - reused_raw;
            }
        }
        Ok(
            json!({"limits":self.state.limits,"used":{"activeTransfers":active,"stagingBytes":staging,"receiptEntries":self.state.primary.len(),"transferFacts":self.state.tickets.len(),"objects":self.state.objects.len()+self.state.retiring.len(),"retainedBytes":retained,"reservedBytes":reserved,"reservedObjects":reserved_objects,"reservedReceiptEntries":reserved_entries}}),
        )
    }
    fn within_capacity(&self) -> Result<()> {
        let c = self.capacity()?;
        let u = &c["used"];
        let l = self.state.limits;
        if num(u, "activeTransfers") > l.active_transfers
            || num(u, "stagingBytes") > l.staging_bytes
            || num(u, "receiptEntries") + num(u, "reservedReceiptEntries") > l.receipt_entries
            || num(u, "transferFacts") > l.transfer_facts
            || num(u, "objects") + num(u, "reservedObjects") > l.objects
            || num(u, "retainedBytes") + num(u, "reservedBytes") > l.retained_bytes
        {
            return Err(reject("capacity_exceeded"));
        }
        // Project each accepted active ticket at its complete worst-case size,
        // instead of reserving that size again on top of its received pages.
        // The global object map is budgeted at its full slot cap so shared
        // objects cannot be subtracted twice by concurrent tickets. Terminal
        // entry/index/root growth fits the credit released by that ticket.
        let state_value = serde_json::to_value(&self.state)?;
        let mut physical_bytes = serde_json::to_vec(&state_value)?.len();
        let mut physical_nodes = node_count(&state_value);
        for t in self
            .state
            .tickets
            .values()
            .filter(|t| t.status == "staging")
        {
            let value = serde_json::to_value(t)?;
            let bytes = serde_json::to_vec(&value)?.len();
            let nodes = node_count(&value);
            physical_bytes += PHYSICAL_RESERVE
                .checked_sub(bytes)
                .ok_or_else(|| reject("capacity_exceeded"))?;
            physical_nodes += PHYSICAL_NODES
                .checked_sub(nodes)
                .ok_or_else(|| reject("capacity_exceeded"))?;
        }
        // An object-map member has a bounded ASCII kind/hash key and a raw
        // length <= 12288. 128 bytes also covers its comma and JSON syntax.
        let object_slots = self.state.objects.len() + self.state.retiring.len();
        let object_bytes = serde_json::to_vec(&self.state.objects)?.len()
            + serde_json::to_vec(&self.state.retiring)?.len()
            - 4;
        physical_bytes = physical_bytes - object_bytes + l.objects * 128;
        physical_nodes = physical_nodes - object_slots + l.objects;
        // At most one disk-lane publication can await its final rename.
        if self.state.pending_objects.len() > 1 {
            return Err(integrity());
        }
        physical_bytes += 512usize
            .checked_sub(serde_json::to_vec(&self.state.pending_objects)?.len())
            .ok_or_else(integrity)?;
        physical_nodes += 4usize
            .checked_sub(node_count(&serde_json::to_value(
                &self.state.pending_objects,
            )?))
            .ok_or_else(integrity)?;
        if physical_bytes > META_CAP || physical_nodes > crate::canonical::MAX_NODES {
            return Err(reject("capacity_exceeded"));
        }
        Ok(())
    }
    fn expected_matches(&self, expected: &Value) -> bool {
        match &self.state.root {
            None => expected.is_null(),
            Some(r) => {
                *expected
                    == json!({"commitRoot":r["commitRoot"],"generation":r["generation"],"bodyEtag":r["body"]["sha256"],"indexRoot":r["index"]["root"],"indexCount":r["index"]["count"]})
            }
        }
    }
    fn root_for(&self, input: &Value) -> Result<&Value> {
        self.state
            .root
            .as_ref()
            .filter(|r| r["commitRoot"] == input["commitRoot"])
            .ok_or_else(|| reject("revision_conflict"))
    }
    fn body_refs(&self, root: &Value) -> Result<Vec<Value>> {
        let mut refs = Vec::new();
        for (i, h) in array(&root["body"]["pageHashes"])?.iter().enumerate() {
            let bytes = self.object(&object_key("body-page", string(h)?))?;
            let page = validate_page("body-page", i, &bytes, &json!({"body":root["body"]}))?;
            refs.extend(array(&page["refs"])?.iter().cloned());
        }
        Ok(refs)
    }
    fn execute(&mut self, input: Value, owner: Owner, before: &Owner) -> Result<Value> {
        if owner != *before {
            return Err(reject("context_changed"));
        }
        let action = string(&input["action"])?;
        if self.state.read_only_copy && matches!(action, "begin" | "put" | "commit") {
            return Err(reject("copy_read_only"));
        }
        let mut response = request(&self.state.identity, action);
        match action {
            "head" => {
                response["root"] = self.state.root.clone().unwrap_or(Value::Null);
                response["capacity"] = self.capacity()?;
            }
            "read" => {
                let root = self.root_for(&input)?;
                response["part"] = input["part"].clone();
                response["commitRoot"] = input["commitRoot"].clone();
                let bytes = if input["part"] == "body-page" {
                    let page = num(&input, "pageIndex");
                    let h = array(&root["body"]["pageHashes"])?
                        .get(page)
                        .ok_or_else(|| reject("invalid_request"))?;
                    let bytes = self.object(&object_key("body-page", string(h)?))?;
                    validate_page("body-page", page, &bytes, &json!({"body":root["body"]}))?;
                    response["pageIndex"] = input["pageIndex"].clone();
                    bytes.to_vec()
                } else {
                    let length = num(&root["body"], "byteLength");
                    let start = num(&input, "offset");
                    if start > length {
                        return Err(reject("invalid_request"));
                    }
                    let end = (start + num(&input, "length")).min(length);
                    let refs = self.body_refs(root)?;
                    let mut bytes = Vec::with_capacity(end - start);
                    if start < end {
                        for idx in start / BLOCK_BYTES..=(end - 1) / BLOCK_BYTES {
                            let r = refs.get(idx).ok_or_else(integrity)?;
                            let b =
                                self.object(&object_key("body-block", string(&r["sha256"])?))?;
                            if b.len() != num(r, "byteLength") {
                                return Err(integrity());
                            }
                            let lo = start.saturating_sub(idx * BLOCK_BYTES);
                            let hi = (end - idx * BLOCK_BYTES).min(b.len());
                            bytes.extend_from_slice(&b[lo..hi]);
                        }
                    }
                    response["bodyEtag"] = root["body"]["sha256"].clone();
                    response["offset"] = input["offset"].clone();
                    response["nextOffset"] = json!(end);
                    response["complete"] = json!(end == length);
                    bytes
                };
                response["byteLength"] = json!(bytes.len());
                response["base64"] = json!(STANDARD.encode(&bytes));
                response["payloadDigest"] = json!(hash(&bytes));
            }
            "lookup" => {
                let root = self.root_for(&input)?;
                let key = &input["key"];
                let digest = string(&key["digest"])?;
                let primary = if key["kind"] == "primary" {
                    Some(digest)
                } else {
                    self.state.secondary.get(digest).map(String::as_str)
                };
                let row = primary.and_then(|k| self.state.primary.get(k));
                let entry = if let Some(row) = row {
                    let mut entry = row.entry.clone();
                    let bytes = self.object(&object_key(
                        "receipt-value",
                        string(&entry["value"]["sha256"])?,
                    ))?;
                    if bytes.len() != num(&entry["value"], "byteLength") {
                        return Err(integrity());
                    }
                    entry["base64"] = json!(STANDARD.encode(&bytes));
                    entry
                } else {
                    Value::Null
                };
                response["commitRoot"] = root["commitRoot"].clone();
                response["indexRoot"] = root["index"]["root"].clone();
                response["indexCount"] = root["index"]["count"].clone();
                response["entry"] = entry;
            }
            _ => {
                let id = string(&input["transferId"])?.to_owned();
                if let Some(t) = self.state.tickets.get(&id) {
                    if t.begin["intentSha256"] != input["intentSha256"] {
                        return Err(reject("request_conflict"));
                    }
                    if t.owner != owner {
                        let permitted = action == "query"
                            && self
                                .recovery
                                .as_ref()
                                .is_some_and(|f| f(&self.state.identity, &t.owner, &owner, &id));
                        self.check(before)?;
                        if !permitted {
                            return Err(reject("request_conflict"));
                        }
                    }
                }
                match action {
                    "begin" => self.begin(&input, owner, before)?,
                    "put" => self.put(&input, before)?,
                    "commit" => self.commit(&id, before)?,
                    "query" => {}
                    _ => return Err(reject("invalid_request")),
                }
                response["transfer"] = match self.state.tickets.get(&id) {
                    Some(t) => self.transfer(t)?,
                    None => {
                        json!({"transferId":id,"intentSha256":input["intentSha256"],"status":"unknown","progress":null,"result":null,"rejection":null})
                    }
                };
            }
        }
        crate::api::validate_wire(CONTRACT, "Response", &response)?;
        crate::canonical::encode_limited(&response, 32768)?;
        Ok(response)
    }
    fn begin(&mut self, input: &Value, owner: Owner, before: &Owner) -> Result<()> {
        let id = string(&input["transferId"])?;
        if let Some(t) = self.state.tickets.get(id) {
            if t.begin != *input {
                return Err(reject("request_conflict"));
            }
            return Ok(());
        }
        validate_begin(input)?;
        self.compact(before)?;
        let mut t = Ticket {
            begin: input.clone(),
            owner,
            base_root: self.state.root.clone(),
            status: "staging".into(),
            result: None,
            rejection: None,
            available: BTreeSet::new(),
            received: BTreeSet::new(),
            pages: BTreeMap::new(),
        };
        let mut next = self.state.clone();
        if !self.expected_matches(&input["expected"]) {
            reject_ticket(&mut t, "revision_conflict", next.root.clone());
        } else if let Some(base) = &t.base_root {
            for (i, h) in array(&input["body"]["pageHashes"])?.iter().enumerate() {
                if array(&base["body"]["pageHashes"])?.get(i) == Some(h) {
                    let key = object_key("body-page", string(h)?);
                    let bytes = self.object(&key)?;
                    let page = validate_page("body-page", i, &bytes, input)?;
                    t.pages.insert(key.clone(), page.clone());
                    t.available.insert(key);
                    for r in array(&page["refs"])? {
                        let k = object_key("body-block", string(&r["sha256"])?);
                        let b = self.object(&k)?;
                        if b.len() != num(r, "byteLength") {
                            return Err(integrity());
                        }
                        t.available.insert(k);
                    }
                }
            }
        }
        next.tickets.insert(id.to_owned(), t);
        let previous = std::mem::replace(&mut self.state, next);
        let check = self.within_capacity();
        let next = std::mem::replace(&mut self.state, previous);
        check?;
        self.save(next, before)
    }
    fn put(&mut self, input: &Value, before: &Owner) -> Result<()> {
        let id = string(&input["transferId"])?;
        let Some(mut t) = self.state.tickets.get(id).cloned() else {
            return Ok(());
        };
        if t.status != "staging" {
            return Ok(());
        }
        let kind = string(&input["kind"])?;
        let key = object_key(kind, string(&input["sha256"])?);
        let bytes = decode(string(&input["base64"])?)?;
        if bytes.len() != num(input, "byteLength") || hash(&bytes) != input["sha256"] {
            return Err(reject("integrity_mismatch"));
        }
        if kind.ends_with("-page") {
            let plan_key = if kind == "body-page" { "body" } else { "index" };
            let positions = array(&t.begin[plan_key]["pageHashes"])?
                .iter()
                .enumerate()
                .filter(|(_, h)| **h == input["sha256"])
                .map(|(i, _)| i)
                .collect::<Vec<_>>();
            if positions.is_empty() {
                return Err(reject("invalid_request"));
            }
            for i in positions {
                validate_page(kind, i, &bytes, &t.begin)?;
            }
        } else {
            let plan = self.plan(&t)?;
            if plan.objects.get(&key) != Some(&bytes.len()) {
                return Err(reject("invalid_request"));
            }
        }
        if t.available.contains(&key) {
            if self.object(&key)?.as_slice() != bytes {
                return Err(integrity());
            }
            return Ok(());
        }
        let temporary = self.write_object(&key, &bytes, before)?;
        let mut next = self.state.clone();
        if let Some(path) = &temporary {
            next.pending_objects.insert(
                key.clone(),
                path.file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(integrity)?
                    .to_owned(),
            );
        }
        next.objects.insert(key.clone(), bytes.len());
        next.orphaned.remove(&key);
        t.available.insert(key.clone());
        t.received.insert(key.clone());
        if kind.ends_with("-page") {
            t.pages
                .insert(key, crate::canonical::parse_json(&bytes, BLOCK_BYTES)?);
        }
        // Reuse only the protected base's refs and exact existing index values.
        if let Some(base) = t.base_root.as_ref().filter(|_| kind == "body-page") {
            let refs = self.body_refs(base)?;
            let known = refs
                .iter()
                .map(|r| {
                    (
                        object_key("body-block", r["sha256"].as_str().unwrap_or("")),
                        num(r, "byteLength"),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            if kind == "body-page" {
                let page = crate::canonical::parse_json(&bytes, BLOCK_BYTES)?;
                for r in array(&page["refs"])? {
                    let k = object_key("body-block", string(&r["sha256"])?);
                    if known.get(&k) == Some(&num(r, "byteLength")) {
                        let b = self.object(&k)?;
                        if b.len() != num(r, "byteLength") {
                            return Err(integrity());
                        }
                        t.available.insert(k);
                    }
                }
            }
        }
        if kind == "index-page" {
            let page = crate::canonical::parse_json(&bytes, BLOCK_BYTES)?;
            for e in array(&page["entries"])? {
                if matches!(self.entry_existing(e), Ok(true)) {
                    let k = object_key("receipt-value", string(&e["value"]["sha256"])?);
                    let b = self.object(&k)?;
                    if b.len() != num(&e["value"], "byteLength") {
                        return Err(integrity());
                    }
                    t.available.insert(k);
                }
            }
        }
        next.tickets.insert(id.into(), t);
        let previous = std::mem::replace(&mut self.state, next);
        let validation = (|| {
            let t = self.state.tickets.get(id).ok_or_else(integrity)?;
            let p = self.plan(t)?;
            if p.complete {
                let added = p.entries.iter().try_fold(0usize, |n, e| {
                    Ok::<usize, Error>(
                        n + usize::from(!self.entry_existing(e.as_ref().ok_or_else(integrity)?)?),
                    )
                })?;
                if added != num(&t.begin["index"], "addedCount") {
                    return Err(reject("request_conflict"));
                }
            }
            self.within_capacity()
        })();
        let mut next = std::mem::replace(&mut self.state, previous);
        if let Err(error) = validation {
            if let Some(code) = deterministic(&error) {
                let observed = next.root.clone();
                reject_ticket(
                    next.tickets.get_mut(id).ok_or_else(integrity)?,
                    code,
                    observed,
                );
            } else {
                self.uncertain = true;
                return Err(error);
            }
        }
        let result = self.save(next, before);
        if result.is_err() && !self.uncertain {
            if let Some(path) = temporary {
                let _ = platform::remove_at(&self.directory, &path);
            }
        }
        result?;
        self.settle_objects(before)
    }
    fn entry_existing(&self, e: &Value) -> Result<bool> {
        let primary = string(&e["primaryKey"])?;
        let secondary = string(&e["secondaryKey"])?;
        match (
            self.state.primary.get(primary),
            self.state.secondary.get(secondary),
        ) {
            (None, None) => Ok(false),
            (Some(row), Some(p)) if p == primary && row.entry == *e => Ok(true),
            _ => Err(reject("request_conflict")),
        }
    }
    fn commit(&mut self, id: &str, before: &Owner) -> Result<()> {
        let Some(mut t) = self.state.tickets.get(id).cloned() else {
            return Ok(());
        };
        if t.status != "staging" {
            return Ok(());
        }
        let mut next = self.state.clone();
        let result = (|| {
            if !self.expected_matches(&t.begin["expected"]) {
                return Err(reject("revision_conflict"));
            }
            let p = self.plan(&t)?;
            if !p.complete || p.objects.keys().any(|k| !t.available.contains(k)) {
                return Err(reject("incomplete"));
            }
            for (key, len) in &p.objects {
                if self.object(key)?.len() != *len {
                    return Err(integrity());
                }
            }
            let mut digest = Sha256::new();
            let mut length = 0;
            for r in p.body {
                let r = r.ok_or_else(integrity)?;
                let b = self.object(&object_key("body-block", string(&r["sha256"])?))?;
                if b.len() != num(&r, "byteLength") {
                    return Err(integrity());
                }
                length += b.len();
                digest.update(&b);
            }
            if length != num(&t.begin["body"], "byteLength")
                || format!("{:x}", digest.finalize()) != t.begin["body"]["sha256"]
            {
                return Err(reject("integrity_mismatch"));
            }
            let mut index_root = match &next.root {
                Some(r) => string(&r["index"]["root"])?.to_owned(),
                None => canonical_hash(&json!(["TPV1-INDEX", next.identity]))?,
            };
            let old = next.primary.len();
            for e in p.entries {
                let e = e.ok_or_else(integrity)?;
                if !self.entry_existing(&e)? {
                    let ordinal = (next.primary.len() + 1).to_string();
                    index_root = canonical_hash(&json!(["TPV1-ENTRY", index_root, ordinal, e]))?;
                    next.secondary.insert(
                        string(&e["secondaryKey"])?.into(),
                        string(&e["primaryKey"])?.into(),
                    );
                    next.primary.insert(
                        string(&e["primaryKey"])?.into(),
                        EntryRow { ordinal, entry: e },
                    );
                }
            }
            if next.primary.len() - old != num(&t.begin["index"], "addedCount") {
                return Err(reject("request_conflict"));
            }
            let generation = next
                .root
                .as_ref()
                .map(|r| {
                    string(&r["generation"]).and_then(|s| s.parse::<u64>().map_err(|_| integrity()))
                })
                .transpose()?
                .unwrap_or(0)
                .checked_add(1)
                .filter(|v| *v <= i64::MAX as u64)
                .ok_or_else(|| reject("invalid_request"))?
                .to_string();
            let mut root = json!({"generation":generation,"body":t.begin["body"],"index":{"root":index_root,"count":next.primary.len().to_string()}});
            root["commitRoot"] = json!(canonical_hash(&json!(["TPV1-ROOT", next.identity, root]))?);
            Ok(root)
        })();
        match result {
            Ok(root) => {
                t.status = "committed".into();
                t.result = Some(root.clone());
                t.base_root = None;
                t.available.clear();
                t.received.clear();
                t.pages.clear();
                next.root = Some(root);
            }
            Err(error) => {
                if let Some(code) = deterministic(&error) {
                    next = self.state.clone();
                    reject_ticket(&mut t, code, next.root.clone());
                } else {
                    return Err(error);
                }
            }
        }
        next.tickets.insert(id.into(), t);
        self.check(before)?;
        self.save(next, before)?;
        // Already committed results remain queryable if retirement fails.
        self.compact(before)
    }
    fn referenced(&self) -> Result<BTreeSet<String>> {
        let mut keep = BTreeSet::new();
        for root in self.state.root.iter().chain(
            self.state
                .tickets
                .values()
                .filter(|t| t.status == "staging")
                .filter_map(|t| t.base_root.as_ref()),
        ) {
            for h in array(&root["body"]["pageHashes"])? {
                keep.insert(object_key("body-page", string(h)?));
            }
            for r in self.body_refs(root)? {
                keep.insert(object_key("body-block", string(&r["sha256"])?));
            }
        }
        for t in self
            .state
            .tickets
            .values()
            .filter(|t| t.status == "staging")
        {
            keep.extend(t.available.iter().cloned());
        }
        for e in self.state.primary.values() {
            keep.insert(object_key(
                "receipt-value",
                string(&e.entry["value"]["sha256"])?,
            ));
        }
        keep.extend(self.state.orphaned.iter().cloned());
        Ok(keep)
    }
    fn compact(&mut self, before: &Owner) -> Result<()> {
        if self.state.read_only_copy {
            return Err(reject("copy_read_only"));
        }
        self.check(before)?;
        self.settle_objects(before)?;
        let keep = self.referenced()?;
        let mut next = self.state.clone();
        for (key, len) in &self.state.objects {
            if !keep.contains(key) {
                next.objects.remove(key);
                next.retiring.insert(key.clone(), *len);
            }
        }
        if next.retiring.is_empty() {
            return Ok(());
        }
        if next.retiring.keys().any(|k| keep.contains(k)) {
            return Err(integrity());
        }
        if next.objects != self.state.objects {
            self.save(next, before)?;
        }
        // The persisted witness and exclusive owner cover every deletion. A
        // crash here keeps charged witnesses; reopen can retry, never resurrect.
        for key in self.state.retiring.keys() {
            self.check(before)?;
            let path = self.object_path(key)?;
            if path.try_exists()? {
                self.object(key)?;
                platform::verify_parent_identity(&self.directory, &path)?;
                platform::remove_at(&self.directory, &path)?;
            }
        }
        let mut next = self.state.clone();
        next.retiring.clear();
        self.save(next, before)
    }
    fn audit(&self) -> Result<()> {
        self.state.limits.validate()?;
        if self
            .state
            .objects
            .keys()
            .any(|k| self.state.retiring.contains_key(k))
            || self
                .state
                .orphaned
                .iter()
                .any(|k| !self.state.objects.contains_key(k))
        {
            return Err(integrity());
        }
        for (key, name) in &self.state.pending_objects {
            self.pending_path(name)?;
            if !self.state.objects.contains_key(key) || self.state.orphaned.contains(key) {
                return Err(integrity());
            }
            self.object(key)?;
        }
        let mut rows = self.state.primary.values().collect::<Vec<_>>();
        rows.sort_by_key(|r| r.ordinal.parse::<u64>().unwrap_or(u64::MAX));
        let mut index_root = canonical_hash(&json!(["TPV1-INDEX", self.state.identity]))?;
        let mut secondary = BTreeMap::new();
        for (i, row) in rows.iter().enumerate() {
            crate::api::validate_wire(CONTRACT, "Entry", &row.entry)?;
            if row.ordinal != (i + 1).to_string()
                || self
                    .state
                    .primary
                    .get(string(&row.entry["primaryKey"])?)
                    .is_none_or(|r| r.ordinal != row.ordinal)
            {
                return Err(integrity());
            }
            if secondary
                .insert(
                    string(&row.entry["secondaryKey"])?.to_owned(),
                    string(&row.entry["primaryKey"])?.to_owned(),
                )
                .is_some()
            {
                return Err(integrity());
            }
            index_root =
                canonical_hash(&json!(["TPV1-ENTRY", index_root, row.ordinal, row.entry]))?;
        }
        if secondary != self.state.secondary {
            return Err(integrity());
        }
        if let Some(root) = &self.state.root {
            verify_root(root, &self.state.identity)?;
            if root["index"]["root"] != index_root
                || root["index"]["count"]
                    .as_str()
                    .and_then(|s| s.parse::<usize>().ok())
                    != Some(self.state.primary.len())
            {
                return Err(integrity());
            }
            let mut digest = Sha256::new();
            let mut length = 0;
            for r in self.body_refs(root)? {
                let b = self.object(&object_key("body-block", string(&r["sha256"])?))?;
                if b.len() != num(&r, "byteLength") {
                    return Err(integrity());
                }
                length += b.len();
                digest.update(&b);
            }
            if length != num(&root["body"], "byteLength")
                || format!("{:x}", digest.finalize()) != root["body"]["sha256"]
            {
                return Err(integrity());
            }
        } else if !self.state.primary.is_empty() {
            return Err(integrity());
        }
        for (id, t) in &self.state.tickets {
            validate_request(&t.begin, &self.state.identity)?;
            validate_begin(&t.begin)?;
            t.owner.validate()?;
            if t.begin["transferId"] != *id
                || t.owner.scope.application_scope_id != self.state.identity.application_scope_id
                || t.owner.scope.end_user_id != self.state.identity.end_user_id
                || !t.received.is_subset(&t.available)
            {
                return Err(integrity());
            }
            if t.status == "staging" {
                if t.result.is_some() || t.rejection.is_some() {
                    return Err(integrity());
                }
                if let Some(base) = &t.base_root {
                    verify_root(base, &self.state.identity)?;
                }
                for (key, page) in &t.pages {
                    let (kind, _) = split_key(key)?;
                    validate_page(
                        kind,
                        num(page, "index"),
                        &crate::canonical::encode(page)?,
                        &t.begin,
                    )?;
                    if !t.available.contains(key)
                        || self.object(key)?.as_slice() != crate::canonical::encode(page)?
                    {
                        return Err(integrity());
                    }
                }
                let plan = self.plan(t)?;
                if t.available.iter().any(|k| !plan.objects.contains_key(k)) {
                    return Err(integrity());
                }
            } else {
                if t.base_root.is_some()
                    || !t.available.is_empty()
                    || !t.received.is_empty()
                    || !t.pages.is_empty()
                {
                    return Err(integrity());
                }
                if t.status == "committed" {
                    verify_root(
                        t.result.as_ref().ok_or_else(integrity)?,
                        &self.state.identity,
                    )?;
                }
            }
            crate::api::validate_wire(CONTRACT, "Transfer", &self.transfer(t)?)?;
        }
        for key in self.referenced()? {
            if !self.state.objects.contains_key(&key) {
                return Err(integrity());
            }
            self.object(&key)?;
        }
        self.within_capacity()
    }
}
struct Plan {
    objects: BTreeMap<String, usize>,
    body: Vec<Option<Value>>,
    entries: Vec<Option<Value>>,
    pages: Vec<bool>,
    complete: bool,
}
fn num(v: &Value, key: &str) -> usize {
    v[key].as_u64().unwrap_or(0) as usize
}
fn string(v: &Value) -> Result<&str> {
    v.as_str().ok_or_else(integrity)
}
fn array(v: &Value) -> Result<&Vec<Value>> {
    v.as_array().ok_or_else(integrity)
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn canonical_hash(v: &Value) -> Result<String> {
    Ok(hash(&crate::canonical::encode(v)?))
}
fn encoded_len(v: &Value) -> Result<usize> {
    Ok(crate::canonical::encode(v)?.len())
}
fn decode(s: &str) -> Result<Vec<u8>> {
    let b = STANDARD.decode(s).map_err(|_| reject("invalid_request"))?;
    if STANDARD.encode(&b) != s {
        return Err(reject("invalid_request"));
    }
    Ok(b)
}
fn object_key(kind: &str, digest: &str) -> String {
    format!("{kind}:{digest}")
}
fn split_key(key: &str) -> Result<(&str, &str)> {
    let (kind, digest) = key.split_once(':').ok_or_else(integrity)?;
    if !matches!(
        kind,
        "body-page" | "index-page" | "body-block" | "receipt-value"
    ) || digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(integrity());
    }
    Ok((kind, digest))
}
fn object_metadata(key: &str, len: usize) -> Result<usize> {
    let (kind, digest) = split_key(key)?;
    encoded_len(&json!({"kind":kind,"sha256":digest,"byteLength":len}))
}
fn bits(values: &[bool]) -> String {
    let mut bytes = vec![0u8; values.len().div_ceil(8)];
    for (i, v) in values.iter().enumerate() {
        if *v {
            bytes[i / 8] |= 1 << (i % 8);
        }
    }
    STANDARD.encode(bytes)
}
fn add_ref(objects: &mut BTreeMap<String, usize>, key: &str, len: usize) -> Result<()> {
    if objects
        .insert(key.into(), len)
        .is_some_and(|old| old != len)
    {
        return Err(reject("invalid_request"));
    }
    Ok(())
}
fn deterministic(error: &Error) -> Option<&str> {
    match error {
        Error::InvalidInput(m) => m.strip_prefix("terminal persistence: ").filter(|c| {
            matches!(
                *c,
                "revision_conflict" | "request_conflict" | "invalid_request" | "integrity_mismatch"
            )
        }),
        _ => None,
    }
}
fn reject_ticket(t: &mut Ticket, code: &str, root: Option<Value>) {
    t.status = "rejected".into();
    t.result = None;
    t.rejection = Some(json!({"code":code,"observedRoot":root}));
    t.base_root = None;
    t.available.clear();
    t.received.clear();
    t.pages.clear();
}
fn validate_begin(input: &Value) -> Result<()> {
    let mut intent = input.clone();
    intent
        .as_object_mut()
        .ok_or_else(integrity)?
        .remove("intentSha256");
    if canonical_hash(&intent)? != input["intentSha256"] {
        return Err(reject("request_conflict"));
    }
    let b = &input["body"];
    let i = &input["index"];
    let blocks = num(b, "byteLength").div_ceil(BLOCK_BYTES);
    if blocks != num(b, "blockCount")
        || array(&b["pageHashes"])?.len() != blocks.div_ceil(64)
        || array(&i["pageHashes"])?.len() != num(i, "entryCount").div_ceil(32)
        || num(i, "addedCount") > num(i, "entryCount")
        || blocks == 0 && b["sha256"] != hash(&[])
    {
        return Err(reject("invalid_request"));
    }
    Ok(())
}
fn validate_page(kind: &str, index: usize, bytes: &[u8], begin: &Value) -> Result<Value> {
    let p =
        crate::canonical::parse_json(bytes, BLOCK_BYTES).map_err(|_| reject("invalid_request"))?;
    let field = if kind == "body-page" {
        "refs"
    } else {
        "entries"
    };
    let keys = p.as_object().ok_or_else(|| reject("invalid_request"))?;
    if keys.len() != 4
        || !keys.contains_key(field)
        || p["version"] != 1
        || p["kind"] != kind
        || p["index"] != index
        || crate::canonical::encode(&p)? != bytes
    {
        return Err(reject("invalid_request"));
    }
    let values = p[field]
        .as_array()
        .ok_or_else(|| reject("invalid_request"))?;
    let (per, total) = if kind == "body-page" {
        (64, num(&begin["body"], "blockCount"))
    } else {
        (32, num(&begin["index"], "entryCount"))
    };
    if index * per >= total || values.len() != (total - index * per).min(per) {
        return Err(reject("invalid_request"));
    }
    let mut previous_primary = None;
    let mut secondary_keys = BTreeSet::new();
    for (n, v) in values.iter().enumerate() {
        crate::api::validate_wire(
            CONTRACT,
            if kind == "body-page" { "Ref" } else { "Entry" },
            v,
        )
        .map_err(|_| reject("invalid_request"))?;
        if kind == "index-page" {
            let primary = string(&v["primaryKey"])?;
            if previous_primary.is_some_and(|previous| previous >= primary)
                || !secondary_keys.insert(string(&v["secondaryKey"])?)
            {
                return Err(reject("invalid_request"));
            }
            previous_primary = Some(primary);
        }
        if kind == "body-page"
            && num(v, "byteLength")
                != (num(&begin["body"], "byteLength") - (index * per + n) * BLOCK_BYTES)
                    .min(BLOCK_BYTES)
        {
            return Err(reject("invalid_request"));
        }
    }
    Ok(p)
}
pub(super) fn verify_root(root: &Value, identity: &Identity) -> Result<()> {
    if root["generation"] == "0" {
        return Err(integrity());
    }
    crate::api::validate_wire(CONTRACT, "Root", root)?;
    let mut r = root.clone();
    r.as_object_mut()
        .ok_or_else(integrity)?
        .remove("commitRoot");
    if canonical_hash(&json!(["TPV1-ROOT", identity, r]))? != root["commitRoot"] {
        return Err(integrity());
    }
    Ok(())
}

fn node_count(value: &Value) -> usize {
    match value {
        Value::Array(a) => 1 + a.iter().map(node_count).sum::<usize>(),
        Value::Object(o) => 1 + o.values().map(node_count).sum::<usize>(),
        _ => 1,
    }
}
