//! Explicit single-source archive lifecycle. This CLI does not compose context.
use futures_util::StreamExt;
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tansr_sdk::{
    api::{Error, Result},
    archive::{
        ArchiveClient, ArchiveStore, BindingCreateRequest, FileStore, Identity, MaterialRequest,
        MaterialResponse, RequestIdentity, StoreLimits, StoreOptions, create_private_directory,
        recover_pending, sync_once,
    },
};
use tansr_sdk_demo::{Args, archive_key, base_url, client, family, report, request_id, safe_text};
use tokio_util::sync::CancellationToken;

const HELP: &str = "tansr-archive --mode MODE [--base ORIGIN] [--family sdk1|sdk2-offload-v1]\nprepare-create: --session ID --source ID --request-id STABLE_ID --intent ABSOLUTE_PRIVATE_FILE\ncreate / creation-status: --intent ORIGINAL_FILE\nsync: --binding ID --file ABSOLUTE_PRIVATE_FILE [--max-pages 64]\nrecover: --binding ID --file ORIGINAL_FILE --request-id STABLE_RECOVERY_ID\nmaterials: --binding ID --file ORIGINAL_FILE --request-id STABLE_RESPONSE_ID --intent NEW_PRIVATE_FILE\nmaterial-submit / material-status: --intent ORIGINAL_MATERIAL_RESPONSE_FILE\nTANSR_TOKEN_FILE is required. sync/recover/materials additionally require TANSR_ARCHIVE_KEY_FILE (64 hex digits). Existing intents, archives and keys are never replaced on errors.";

#[tokio::main]
async fn main() -> std::process::ExitCode {
    report("tansr-archive", run().await)
}

async fn run() -> Result<()> {
    let mut args = Args::parse()?;
    if args.help() {
        println!("{HELP}");
        return Ok(());
    }
    let base = base_url(&mut args);
    let family = family(&mut args)?;
    let mode = args.value("--mode", "sync");
    let archive = ArchiveClient::new(client(&base, &family)?);
    let cancelled = CancellationToken::new();
    let work = async {
        match mode.as_str() {
            "prepare-create" => {
                let session = args.required("--session")?;
                let source = args.required("--source")?;
                let request = args.required("--request-id")?;
                let path = absolute(args.required("--intent")?)?;
                args.finish()?;
                let intent = archive.prepare_create(&session, &source, &request).await?;
                let bytes = serde_json::to_vec(&intent)?;
                tokio::task::spawn_blocking(move || save_intent(&path, &bytes))
                    .await
                    .map_err(|_| Error::Io("intent writer stopped".into()))??;
                println!(
                    "binding intent durably saved; no binding was created. Run --mode create with the same --intent file."
                );
                Ok(())
            }
            "create" | "creation-status" => {
                let path = absolute(args.required("--intent")?)?;
                args.finish()?;
                let intent: BindingCreateRequest = serde_json::from_value(
                    tokio::task::spawn_blocking(move || read_intent(&path))
                        .await
                        .map_err(|_| Error::Io("intent reader stopped".into()))??,
                )?;
                if mode == "create" {
                    let binding = archive.create_binding(&intent).await?;
                    println!("binding: {}", safe_text(&binding.binding_id));
                } else {
                    let receipt = archive
                        .creation_operation(&intent.target.session_id, &intent.request)
                        .await?;
                    println!(
                        "creation state: {}; binding: {}",
                        safe_text(&receipt.state),
                        safe_text(&receipt.binding_id)
                    );
                    if receipt.state != "completed" {
                        return Err(Error::Unknown(
                            "binding creation is not confirmed complete".into(),
                        ));
                    }
                }
                Ok(())
            }
            "material-submit" | "material-status" => {
                let path = absolute(args.required("--intent")?)?;
                args.finish()?;
                let response: MaterialResponse = serde_json::from_value(
                    tokio::task::spawn_blocking(move || read_intent(&path))
                        .await
                        .map_err(|_| Error::Io("intent reader stopped".into()))??,
                )?;
                let receipt = if mode == "material-submit" {
                    archive.submit_materials(&response).await?
                } else {
                    archive
                        .material_status(&response.binding_id, &response.material_request_id)
                        .await?
                };
                println!(
                    "material state: {} (received is not core-consumed)",
                    safe_text(&receipt.state)
                );
                if mode == "material-status" && receipt.state != "core-consumed" {
                    return Err(Error::Unknown(
                        "material consumption is not confirmed".into(),
                    ));
                }
                Ok(())
            }
            "sync" | "recover" | "materials" => {
                let binding_id = args.required("--binding")?;
                let path = absolute(args.required("--file")?)?;
                let count = args
                    .value("--max-pages", "64")
                    .parse::<usize>()
                    .map_err(|_| Error::InvalidInput("max-pages must be an integer".into()))?;
                if !(1..=1024).contains(&count) {
                    return Err(Error::InvalidInput("max-pages must be 1..1024".into()));
                }
                let recovery = if mode != "sync" {
                    Some(args.required("--request-id")?)
                } else {
                    None
                };
                let intent = if mode == "materials" {
                    Some(absolute(args.required("--intent")?)?)
                } else {
                    None
                };
                args.finish()?;
                if mode != "sync" && !path.is_file() {
                    return Err(Error::InvalidInput(
                        "recovery/materials require the existing archive file and original key"
                            .into(),
                    ));
                }
                let key = archive_key()?;
                let binding = archive.binding(&binding_id).await?;
                let status = archive.status(&binding_id).await?;
                let identity = Identity::from_binding(&binding, &status)?;
                let expected = identity.clone();
                let access_cancel = cancelled.clone();
                let parent = path
                    .parent()
                    .ok_or_else(|| Error::InvalidInput("archive path needs a parent".into()))?;
                create_private_directory(parent)?;
                let store = FileStore::open(StoreOptions {
                    path,
                    key,
                    identity,
                    limits: StoreLimits::default(),
                    check_access: Arc::new(move |current| {
                        if access_cancel.is_cancelled() {
                            return Err(Error::Cancelled);
                        }
                        if *current != expected {
                            return Err(Error::Contract("archive ownership changed".into()));
                        }
                        Ok(())
                    }),
                })
                .await?;
                let result = run_store(
                    &archive,
                    &store,
                    &mode,
                    recovery.as_deref(),
                    intent.as_deref(),
                    count,
                )
                .await;
                let closed = store.close().await;
                result.and(closed)
            }
            _ => Err(Error::InvalidInput(
                "unknown archive mode; see --help".into(),
            )),
        }
    };
    tokio::pin!(work);
    tokio::select! {
        result = &mut work => result,
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(Error::from)?;
            cancelled.cancel();
            Err(Error::Cancelled)
        }
        _ = tokio::time::sleep(Duration::from_secs(300)) => {
            cancelled.cancel();
            Err(Error::Unknown("archive run deadline expired; preserve original intent/file/key".into()))
        }
    }
}

async fn run_store(
    client: &ArchiveClient,
    store: &FileStore,
    mode: &str,
    recovery: Option<&str>,
    intent: Option<&Path>,
    count: usize,
) -> Result<()> {
    if mode == "recover" {
        let id = recovery.ok_or_else(|| Error::InvalidInput("recovery ID missing".into()))?;
        let result = recover_pending(client, store, id).await?;
        if !result.recovered
            || result
                .receipt
                .as_ref()
                .is_none_or(|r| r.state != "completed")
        {
            return Err(Error::Unknown(
                "pending ACK is not confirmed complete".into(),
            ));
        }
        println!(
            "pending ACK confirmed; this recovery-only run did not synchronize all remaining pages"
        );
        return Ok(());
    }
    if mode == "materials" {
        return supply_materials(
            client,
            store,
            recovery
                .ok_or_else(|| Error::InvalidInput("material response identity missing".into()))?,
            intent.ok_or_else(|| Error::InvalidInput("material intent path missing".into()))?,
        )
        .await;
    }
    for page in 0..count {
        let result = sync_once(client, store, &request_id()).await?;
        println!(
            "page {}: verified records={}, complete={}, recovered-pending-ack={}",
            page + 1,
            result.records,
            result.complete,
            result.recovered
        );
        if result.complete {
            println!(
                "archive synchronized; coverage is independent of the SSE cursor and material consumption"
            );
            return Ok(());
        }
    }
    Err(Error::InvalidInput(
        "page limit reached; continue with the same binding, archive file and key".into(),
    ))
}

fn absolute(value: String) -> Result<PathBuf> {
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(Error::InvalidInput(
            "an absolute private path is required".into(),
        ));
    }
    Ok(path)
}

fn save_intent(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::InvalidInput("intent needs a parent".into()))?;
    create_private_directory(parent)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x80000000);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    #[cfg(unix)]
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn read_intent(path: &Path) -> Result<serde_json::Value> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::InvalidInput("intent needs a parent".into()))?;
    create_private_directory(parent)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(Error::InvalidInput(
            "intent must be a regular unlinked file".into(),
        ));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x400 != 0 {
            return Err(Error::InvalidInput(
                "intent must not be a reparse point".into(),
            ));
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 || metadata.mode() & 0o077 != 0 {
            return Err(Error::InvalidInput(
                "intent file must be private and unlinked".into(),
            ));
        }
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(262145)
        .read_to_end(&mut bytes)?;
    tansr_sdk::canonical::parse_json(&bytes, 262144)
}

async fn supply_materials(
    client: &ArchiveClient,
    store: &FileStore,
    request_id: &str,
    intent: &Path,
) -> Result<()> {
    // A fresh subscription re-notifies still-valid requests with their remaining
    // TTL; loading an old serialized MaterialRequest would restart a false TTL.
    let binding_id = &store.identity().binding_id;
    let binding = client.binding(binding_id).await?;
    let mut events = client.events(&binding, None).await?;
    println!(
        "waiting for one live material request; existing archive records will be supplied exactly as requested"
    );
    while let Some(event) = events.next().await {
        let event = event?;
        if event.r#type.as_deref() != Some("material.request") {
            continue;
        }
        let request: MaterialRequest = serde_json::from_value(event.raw["payload"].clone())?;
        let deadline = SystemTime::now()
            .checked_add(Duration::from_millis(request.remaining_ttl_ms as u64))
            .ok_or_else(|| Error::InvalidInput("material deadline overflow".into()))?;
        let binding = client.binding(binding_id).await?;
        let epoch = binding
            .operation_epoch
            .ok_or_else(|| Error::Contract("binding has no current operation epoch".into()))?;
        let response = client
            .prepare_materials_before(
                store,
                &request,
                RequestIdentity {
                    request_id: request_id.into(),
                    operation_epoch: epoch.id,
                },
                deadline,
            )
            .await?;
        let path = intent.to_path_buf();
        let bytes = serde_json::to_vec(&response)?;
        tokio::task::spawn_blocking(move || save_intent(&path, &bytes))
            .await
            .map_err(|_| Error::Io("material intent writer stopped".into()))??;
        let receipt = client.submit_materials(&response).await?;
        println!(
            "material state: {}. Original response saved; use material-status to observe core consumption.",
            safe_text(&receipt.state)
        );
        return Ok(());
    }
    Err(Error::Unknown(
        "material stream ended before a live request".into(),
    ))
}
