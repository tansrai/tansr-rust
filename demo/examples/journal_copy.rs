//! Offline explicit migration/key rotation. Source and target remain separate.
use std::path::PathBuf;
use tansr_sdk::{Error, Result, executor::FileJournal};
use tansr_sdk_demo::{Args, archive_key_from_file, report};

fn main() -> std::process::ExitCode {
    report("journal-copy", run())
}
fn run() -> Result<()> {
    let mut args = Args::parse()?;
    if args.help() {
        println!(
            "journal_copy --source ABSOLUTE_DIRECTORY --source-format plaintext|encrypted --target ABSOLUTE_NEW_DIRECTORY --target-key-file KEY_FILE [--source-key-file KEY_FILE]\nStop all writers first. Keys are 64-digit hex files; encrypted copies require a different new key. Source is preserved. This offline command copies all claims/receipts, including permanent unknowns; it never switches host configuration. If publication is also copied, validate both targets before switching both paths and keys. Neither store is reset after a failure. Retained plaintext source/backup copies remain plaintext."
        );
        return Ok(());
    }
    let source = PathBuf::from(args.required("--source")?);
    let target = PathBuf::from(args.required("--target")?);
    let format = args.required("--source-format")?;
    let target_key = PathBuf::from(args.required("--target-key-file")?);
    let source_key = args.take("--source-key-file").map(PathBuf::from);
    args.finish()?;
    if !source.is_absolute() || !source.is_dir() || std::fs::read_dir(&source)?.next().is_none() {
        return Err(Error::InvalidInput(
            "source must be an existing nonempty journal directory".into(),
        ));
    }
    // Read all selected keys before opening any storage. Never initialize a
    // missing encrypted source or infer plaintext after a decryption failure.
    let target_key = archive_key_from_file(&target_key)?;
    let journal = match (format.as_str(), source_key) {
        ("plaintext", None) => FileJournal::open(&source)?,
        ("encrypted", Some(path)) if source.join("journal.encryption").is_file() => {
            FileJournal::open_encrypted(&source, archive_key_from_file(&path)?)?
        }
        _ => {
            return Err(Error::InvalidInput(
                "explicit source format/key mismatch".into(),
            ));
        }
    };
    let copied = journal.copy_to(target, target_key)?;
    drop(copied);
    println!(
        "Complete encrypted journal published; original claims, receipts and unknown identities retained. Source remains. Validate publication and journal together before explicitly changing host paths and keys."
    );
    Ok(())
}
