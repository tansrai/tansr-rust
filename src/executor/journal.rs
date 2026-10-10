mod migration;

use super::{
    client::{validate_operation, validate_receipt},
    types::*,
};
use crate::{
    api::{Error, Result},
    canonical,
};
use async_trait::async_trait;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

/// Immutable claims and receipts in a host-owned directory. Entries are never
/// age-evicted. A damaged/partial entry is unknown, never permission to retry IO.
/// open retains the legacy plaintext format; open_encrypted protects every
/// claim and receipt, including publication read results. The host owns keys.
pub struct FileJournal {
    root: PathBuf,
    directory: File,
    gate: Mutex<()>,
    cipher: Option<crate::storage_cipher::Cipher>,
}
impl FileJournal {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_key(directory.as_ref(), None, false)
    }
    /// Explicit encrypted format. Existing plaintext journals are rejected;
    /// use copy_to with a fresh target and key instead of resetting claims.
    pub fn open_encrypted(directory: impl AsRef<Path>, key: [u8; 32]) -> Result<Self> {
        Self::open_with_key(directory.as_ref(), Some(key), false)
    }
    /// Reopen the original encrypted journal only. A missing directory, lock or
    /// encryption marker fails without creating an empty recovery replacement.
    pub fn reopen_encrypted(directory: impl AsRef<Path>, key: [u8; 32]) -> Result<Self> {
        Self::open_with_key(directory.as_ref(), Some(key), true)
    }
    fn open_with_key(root: &Path, key: Option<[u8; 32]>, reopen: bool) -> Result<Self> {
        if !root.is_absolute() {
            return Err(invalid("journal needs absolute path"));
        }
        if !reopen {
            crate::archive::platform::create_private_directory(root)?;
        }
        let directory = crate::archive::platform::verify_parent(&root.join("journal.guard"))?;
        let journal = Self {
            root: root.to_path_buf(),
            directory,
            gate: Mutex::new(()),
            cipher: key.map(crate::storage_cipher::Cipher::new),
        };
        // Serialize format initialization across processes. A marker is durable
        // before any sensitive claim/receipt can be accepted.
        let lock = crate::archive::platform::open_at(
            &journal.directory,
            &root.join("journal.format.lock"),
            !reopen,
            false,
        )?;
        fs2::FileExt::lock_exclusive(&lock)?;
        let marker = "journal.encryption";
        let encrypted = journal.root.join(marker).try_exists()?;
        if reopen && !encrypted {
            return Err(invalid(
                "original encrypted journal is missing; preserve recovery anchors",
            ));
        }
        match (journal.cipher.is_some(), encrypted) {
            (false, true) => return Err(invalid("encrypted journal key required")),
            (false, false) => {
                journal.create(
                    "journal.plaintext",
                    &json!({"format":"tansr-rust-journal-plaintext-v1"}),
                )?;
            }
            (true, false) => {
                if std::fs::read_dir(root)?.any(|entry| {
                    entry.map_or(true, |entry| entry.file_name() != "journal.format.lock")
                }) {
                    return Err(invalid(
                        "existing journal must not be silently encrypted or reset",
                    ));
                }
                journal.create(marker, &json!({"format":"tansr-rust-journal-encrypted-v1"}))?;
            }
            _ => {}
        }
        if journal.cipher.is_some()
            && journal.read(marker)? != json!({"format":"tansr-rust-journal-encrypted-v1"})
        {
            return Err(invalid("journal encryption format"));
        }
        Ok(journal)
    }
    // A separate handle per transaction releases the OS lock on drop, including
    // errors. The in-process mutex avoids same-handle lock ownership ambiguity.
    fn transaction_lock(&self) -> Result<File> {
        self.check()?;
        let lock = crate::archive::platform::open_at(
            &self.directory,
            &self.root.join("journal.format.lock"),
            true,
            false,
        )?;
        fs2::FileExt::lock_exclusive(&lock)?;
        self.check()?;
        Ok(lock)
    }
    fn check(&self) -> Result<()> {
        crate::archive::platform::verify_parent_identity(
            &self.directory,
            &self.root.join("journal.guard"),
        )
    }
    fn read_raw(&self, name: &str, limit: usize) -> Result<zeroize::Zeroizing<Vec<u8>>> {
        self.check()?;
        let path = self.root.join(name);
        let mut f = crate::archive::platform::open_at(&self.directory, &path, false, false)?;
        let meta = f.metadata()?;
        if !meta.is_file() || meta.file_type().is_symlink() || meta.len() > limit as u64 {
            return Err(Error::Unknown("journal entry invalid".into()));
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            if meta.file_attributes() & 0x400 != 0 {
                return Err(invalid("journal reparse entry"));
            }
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if meta.nlink() != 1 || meta.mode() & 0o077 != 0 {
                return Err(invalid("journal entry not private"));
            }
        }
        let mut bytes = zeroize::Zeroizing::new(Vec::new());
        Read::by_ref(&mut f)
            .take((limit + 1) as u64)
            .read_to_end(&mut bytes)?;
        self.check()?;
        if bytes.len() > limit {
            return Err(Error::Unknown("journal entry exceeds size limit".into()));
        }
        Ok(bytes)
    }
    fn read(&self, name: &str) -> Result<serde_json::Value> {
        let bytes = self.read_raw(name, CONTROL_BYTES + 28)?;
        let plain = match &self.cipher {
            Some(cipher) => cipher.open(&bytes, &journal_aad(name))?,
            None => bytes,
        };
        canonical::parse_strict(&plain, CONTROL_BYTES)
            .map_err(|_| Error::Unknown("journal entry corrupt or incomplete".into()))
    }
    fn create(&self, name: &str, v: &serde_json::Value) -> Result<bool> {
        self.check()?;
        let plain = zeroize::Zeroizing::new(canonical::encode_limited(v, CONTROL_BYTES)?);
        self.create_raw(name, &plain)
    }
    fn create_raw(&self, name: &str, plain: &[u8]) -> Result<bool> {
        self.check()?;
        let bytes = zeroize::Zeroizing::new(match &self.cipher {
            Some(cipher) => cipher.seal(plain, &journal_aad(name))?,
            None => plain.to_vec(),
        });
        let path = self.root.join(name);
        let mut f = match crate::archive::platform::open_at(&self.directory, &path, true, true) {
            Ok(f) => f,
            Err(e) => {
                self.check()?;
                if path.try_exists()? {
                    return Ok(false);
                }
                return Err(e);
            }
        };
        // Never remove a partly written fact, even if sync fails.
        f.write_all(&bytes)?;
        f.sync_all()?;
        drop(f);
        #[cfg(unix)]
        self.directory.sync_all()?;
        self.check()?;
        Ok(true)
    }
}
fn journal_aad(name: &str) -> Vec<u8> {
    format!("tansr-rust-journal-encrypted-v1\0{name}").into_bytes()
}
fn key(op: &Operation) -> Result<String> {
    validate_operation(op)?;
    let bytes = canonical::encode(&json!([
        op.scope.application_scope_id,
        op.scope.end_user_id,
        op.binding.target.executor_id,
        op.operation_id
    ]))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
#[async_trait]
impl Journal for FileJournal {
    fn encrypted_at_rest(&self) -> bool {
        self.cipher.is_some()
    }
    async fn claim(&self, op: &Operation) -> Result<ClaimResult> {
        let k = key(op)?;
        let _guard = self
            .gate
            .lock()
            .map_err(|_| invalid("journal lock poisoned"))?;
        let _transaction = self.transaction_lock()?;
        // A surviving receipt without its claim is not permission to execute IO.
        if !self.root.join(format!("{k}.claim")).try_exists()?
            && self.root.join(format!("{k}.receipt")).try_exists()?
        {
            return Err(Error::Unknown(
                "journal receipt has no claim; preserve original".into(),
            ));
        }
        if self.create(&format!("{k}.claim"), &json!({"digest":op.digest}))? {
            return Ok(ClaimResult::Claimed);
        }
        let c = self.read(&format!("{k}.claim"))?;
        if c != json!({"digest":op.digest}) {
            return Err(invalid("journal digest conflict"));
        }
        let path = self.root.join(format!("{k}.receipt"));
        self.check()?;
        if !path.try_exists()? {
            self.check()?;
            return Ok(ClaimResult::Pending);
        }
        let r: Receipt = serde_json::from_value(self.read(&format!("{k}.receipt"))?)?;
        validate_receipt(op, &r)?;
        Ok(ClaimResult::Receipt(Box::new(r)))
    }
    async fn complete(&self, op: &Operation, r: &Receipt) -> Result<()> {
        let k = key(op)?;
        validate_receipt(op, r)?;
        let _guard = self
            .gate
            .lock()
            .map_err(|_| invalid("journal lock poisoned"))?;
        let _transaction = self.transaction_lock()?;
        if self.read(&format!("{k}.claim"))? != json!({"digest":op.digest}) {
            return Err(invalid("journal digest conflict"));
        }
        let v = value(r)?;
        if !self.create(&format!("{k}.receipt"), &v)? && self.read(&format!("{k}.receipt"))? != v {
            return Err(invalid("journal receipt conflict"));
        }
        Ok(())
    }
}
