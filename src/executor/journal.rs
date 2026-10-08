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
/// This is a plaintext journal; the host owns backup/encryption policy.
pub struct FileJournal {
    root: PathBuf,
    directory: File,
    gate: Mutex<()>,
}
impl FileJournal {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self> {
        let root = directory.as_ref();
        if !root.is_absolute() {
            return Err(invalid("journal needs absolute path"));
        }
        crate::archive::platform::create_private_directory(root)?;
        let directory = crate::archive::platform::verify_parent(&root.join("journal.guard"))?;
        Ok(Self {
            root: root.to_path_buf(),
            directory,
            gate: Mutex::new(()),
        })
    }
    fn check(&self) -> Result<()> {
        crate::archive::platform::verify_parent_identity(
            &self.directory,
            &self.root.join("journal.guard"),
        )
    }
    fn read(&self, name: &str) -> Result<serde_json::Value> {
        self.check()?;
        let path = self.root.join(name);
        let mut f = crate::archive::platform::open_at(&self.directory, &path, false, false)?;
        let meta = f.metadata()?;
        if !meta.is_file() || meta.file_type().is_symlink() || meta.len() > CONTROL_BYTES as u64 {
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
        let mut bytes = Vec::new();
        Read::by_ref(&mut f)
            .take(CONTROL_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        self.check()?;
        canonical::parse_strict(&bytes, CONTROL_BYTES)
            .map_err(|_| Error::Unknown("journal entry corrupt or incomplete".into()))
    }
    fn create(&self, name: &str, v: &serde_json::Value) -> Result<bool> {
        self.check()?;
        let bytes = canonical::encode_limited(v, CONTROL_BYTES)?;
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
    async fn claim(&self, op: &Operation) -> Result<ClaimResult> {
        let k = key(op)?;
        let _guard = self
            .gate
            .lock()
            .map_err(|_| invalid("journal lock poisoned"))?;
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
