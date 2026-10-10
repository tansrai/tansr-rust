//! Explicit encrypted copies; source files and recovery keys are never removed.
use super::*;
use crate::archive::platform;
use rand::{RngCore, rngs::OsRng};
use std::collections::BTreeMap;
use zeroize::{Zeroize, Zeroizing};

// A corrupt authenticated envelope needs a small amount of wrapping space.
// Oversize or unrecognized files fail the entire copy, never get skipped.
const COPY_ENTRY_BYTES: usize = CONTROL_BYTES + 128;
const COPY_ENTRIES: usize = 100_000;
const UNKNOWN_PREFIX: &[u8] = b"\xfftansr-journal-unknown-v1\0";
type Inventory = BTreeMap<String, [u8; 32]>;

impl FileJournal {
    /// Copy every immutable claim/receipt to a fresh encrypted directory.
    ///
    /// This blocking operation supports legacy plaintext migration and encrypted
    /// key rotation. A new key is mandatory for encrypted sources. The source is
    /// retained byte-for-byte, including damaged facts; unreadable ciphertext is
    /// wrapped as an encrypted, permanently unknown fact under its original key.
    /// Unsupported names, links and size limits fail without publishing a target.
    ///
    /// The target must not exist. All staged files are synced before one atomic
    /// no-replace directory rename. A crash before publication leaves only an
    /// encrypted `.journal-copy-*` staging directory; never adopt that directory.
    /// An error after publication can leave a complete target: reopen the original
    /// target with the new key and reconcile, without overwriting or resetting it.
    ///
    /// Stop all host writers before copying publication and journal, validate both
    /// targets, then explicitly switch both paths/keys. There is no transaction
    /// across those two stores. This lock coordinates current SDK journal writers;
    /// older SDKs and external writers must also be stopped by the host.
    pub fn copy_to(&self, directory: impl AsRef<Path>, key: [u8; 32]) -> Result<Self> {
        self.copy_to_impl(directory.as_ref(), key, |_| Ok(()))
    }

    fn copy_to_impl(
        &self,
        target: &Path,
        mut key: [u8; 32],
        before_publish: impl FnOnce(&Path) -> Result<()>,
    ) -> Result<Self> {
        let new_key = Zeroizing::new(key);
        key.zeroize();
        if self.cipher.as_ref().is_some_and(|c| c.same_key(&new_key)) {
            return Err(invalid("journal copy requires a new encryption key"));
        }
        if !target.is_absolute() || target.file_name().is_none() {
            return Err(invalid("journal copy needs a fresh absolute target"));
        }
        let parent = platform::verify_parent(target)?;
        let target = target
            .parent()
            .ok_or_else(|| invalid("journal target parent"))?
            .canonicalize()?
            .join(target.file_name().unwrap());
        if target.starts_with(self.root.canonicalize()?) || target.try_exists()? {
            return Err(invalid(
                "journal copy target must be new and outside the source",
            ));
        }
        let _gate = self
            .gate
            .lock()
            .map_err(|_| invalid("journal lock poisoned"))?;
        let _transaction = self.transaction_lock()?;
        let inventory = self.inventory()?;
        let mut nonce = [0u8; 16];
        OsRng.fill_bytes(&mut nonce);
        let staging =
            target.with_file_name(format!(".journal-copy-{:032x}", u128::from_be_bytes(nonce)));
        platform::create_private_directory_new(&staging)?;
        let staged = Self::open_encrypted(&staging, *new_key)?;
        for name in inventory.keys().filter(|name| is_fact(name)) {
            let raw = self.read_raw(name, COPY_ENTRY_BYTES)?;
            if Sha256::digest(&raw).as_slice() != inventory[name] {
                return Err(Error::Unknown(
                    "journal changed during copy; preserve source".into(),
                ));
            }
            let plain = match &self.cipher {
                None => raw,
                Some(cipher) => match cipher.open(&raw, &journal_aad(name)) {
                    Ok(plain) => plain,
                    Err(_) => {
                        // Invalid UTF-8 can never parse as a valid claim/receipt.
                        // Retain the unreadable bytes inside the new encryption.
                        let mut unknown = Zeroizing::new(UNKNOWN_PREFIX.to_vec());
                        unknown.extend_from_slice(&raw);
                        unknown
                    }
                },
            };
            if plain.len() + 28 > COPY_ENTRY_BYTES {
                return Err(Error::Unknown(
                    "journal unknown fact exceeds migration limit".into(),
                ));
            }
            if !staged.create_raw(name, &plain)? {
                return Err(invalid("journal staging collision"));
            }
        }
        #[cfg(unix)]
        staged.directory.sync_all()?;
        // Windows denies renaming directories while these guarded handles live.
        drop(staged);
        before_publish(&staging)?;
        if inventory != self.inventory()? {
            return Err(Error::Unknown(
                "journal changed during copy; preserve source".into(),
            ));
        }
        platform::publish_directory_at(&parent, &staging, &target).map_err(|_| {
            Error::Unknown(
                "journal target publication uncertain; preserve both paths and keys".into(),
            )
        })?;
        Self::open_encrypted(target, *new_key)
    }

    fn inventory(&self) -> Result<Inventory> {
        self.check()?;
        let marker = if self.cipher.is_some() {
            "journal.encryption"
        } else {
            "journal.plaintext"
        };
        let format = if self.cipher.is_some() {
            "tansr-rust-journal-encrypted-v1"
        } else {
            "tansr-rust-journal-plaintext-v1"
        };
        if self.read(marker)? != json!({"format":format}) {
            return Err(invalid("journal migration format mismatch"));
        }
        let mut inventory = Inventory::new();
        for entry in std::fs::read_dir(&self.root)? {
            let name = entry?
                .file_name()
                .into_string()
                .map_err(|_| invalid("journal entry name"))?;
            if name == "journal.format.lock" {
                continue;
            }
            if name != marker && !is_fact(&name) {
                return Err(invalid("unrecognized journal entry; preserve source"));
            }
            if inventory.len() >= COPY_ENTRIES {
                return Err(invalid(
                    "journal migration entry limit reached; preserve source",
                ));
            }
            let bytes = self.read_raw(&name, COPY_ENTRY_BYTES)?;
            inventory.insert(name, Sha256::digest(&bytes).into());
        }
        self.check()?;
        Ok(inventory)
    }
}
fn is_fact(name: &str) -> bool {
    name.strip_suffix(".claim")
        .or_else(|| name.strip_suffix(".receipt"))
        .is_some_and(|key| {
            key.len() == 64
                && key
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn private_root(dir: &tempfile::TempDir) -> PathBuf {
        let root = dir.path().canonicalize().unwrap().join("private");
        platform::create_private_directory(&root).unwrap();
        root
    }
    fn source(root: &Path) -> FileJournal {
        let journal = FileJournal::open(root.join("source")).unwrap();
        journal
            .create_raw(
                &format!("{}.claim", "a".repeat(64)),
                b"partial-private-fact",
            )
            .unwrap();
        journal
    }
    #[test]
    fn copy_failure_and_target_race_never_publish_empty_or_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let root = private_root(&dir);
        let source = source(&root);
        let before = source.inventory().unwrap();
        let target = root.join("target");
        assert!(
            source
                .copy_to_impl(&target, [9; 32], |staging| {
                    assert!(!target.exists());
                    let staged = FileJournal::open_encrypted(staging, [9; 32]).unwrap();
                    assert_eq!(staged.inventory().unwrap().len(), before.len());
                    Err(Error::Io("injected before publication".into()))
                })
                .is_err()
        );
        assert!(!target.exists());
        assert_eq!(source.inventory().unwrap(), before);
        assert!(
            source
                .copy_to_impl(&target, [9; 32], |_| {
                    platform::create_private_directory(&target)?;
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(std::fs::read_dir(&target).unwrap().count(), 0);
        assert_eq!(source.inventory().unwrap(), before);
    }
    #[test]
    fn changed_source_unknown_names_and_oversize_facts_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let root = private_root(&dir);
        let journal = source(&root);
        let target = root.join("target");
        assert!(
            journal
                .copy_to_impl(&target, [9; 32], |_| {
                    journal.create_raw(&format!("{}.receipt", "b".repeat(64)), b"partial")?;
                    Ok(())
                })
                .is_err()
        );
        assert!(!target.exists());
        journal
            .create_raw("unrecognized", b"must-not-be-skipped")
            .unwrap();
        assert!(journal.copy_to(&target, [9; 32]).is_err());
        assert!(!target.exists());
        let dir = tempfile::tempdir().unwrap();
        let root = private_root(&dir);
        let journal = source(&root);
        let target = root.join("target");
        let name = format!("{}.receipt", "b".repeat(64));
        journal
            .create_raw(&name, &vec![0; COPY_ENTRY_BYTES + 1])
            .unwrap();
        assert!(journal.copy_to(&target, [9; 32]).is_err());
        assert!(!target.exists());
        assert_eq!(
            std::fs::metadata(journal.root.join(name)).unwrap().len(),
            (COPY_ENTRY_BYTES + 1) as u64
        );
    }
    #[test]
    fn source_transactions_share_the_migration_lock() {
        let dir = tempfile::tempdir().unwrap();
        let root = private_root(&dir);
        let source = source(&root);
        let lock = source.transaction_lock().unwrap();
        let path = source.root.clone();
        let (send, recv) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            send.send("ready").unwrap();
            let journal = FileJournal::open(path).unwrap();
            let _lock = journal.transaction_lock().unwrap();
            send.send("locked").unwrap();
        });
        assert_eq!(
            recv.recv_timeout(std::time::Duration::from_secs(5))
                .unwrap(),
            "ready"
        );
        assert!(
            recv.recv_timeout(std::time::Duration::from_millis(100))
                .is_err()
        );
        drop(lock);
        assert_eq!(
            recv.recv_timeout(std::time::Duration::from_secs(5))
                .unwrap(),
            "locked"
        );
        worker.join().unwrap();
    }
    #[test]
    fn migration_child_exit_before_publish() {
        let Some(root) = std::env::var_os("TANSR_RUST_JOURNAL_COPY_CHILD") else {
            return;
        };
        let root = PathBuf::from(root);
        let source = FileJournal::open(root.join("source")).unwrap();
        let _ = source.copy_to_impl(&root.join("target"), [9; 32], |_| std::process::exit(84));
        panic!("fault hook not reached");
    }
    #[test]
    fn process_death_before_publish_preserves_source_and_retry_identity() {
        let dir = tempfile::tempdir().unwrap();
        let root = private_root(&dir);
        let journal = source(&root);
        let before = journal.inventory().unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "executor::journal::migration::tests::migration_child_exit_before_publish",
                "--nocapture",
            ])
            .env("TANSR_RUST_JOURNAL_COPY_CHILD", &root)
            .output()
            .unwrap();
        assert_eq!(
            child.status.code(),
            Some(84),
            "{}",
            String::from_utf8_lossy(&child.stderr)
        );
        assert!(!root.join("target").exists());
        assert_eq!(journal.inventory().unwrap(), before);
        let copied = journal.copy_to(root.join("target"), [9; 32]).unwrap();
        assert!(copied.read(&format!("{}.claim", "a".repeat(64))).is_err());
    }
}
