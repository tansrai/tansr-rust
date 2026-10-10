//! Attach an already authorized reserved publication binding. This example never
//! creates a memory domain, selects memories, or exposes a model-facing tool.
use async_trait::async_trait;
use std::{collections::BTreeMap, io::Read, path::PathBuf, sync::Arc, time::Duration};
use tansr_sdk::{
    CancellationToken, Error, Result,
    executor::{
        Authorizer, Client, Connection, FileJournal, Operation, Platform, Registration, Runner,
        RunnerOptions, ToolDefinition, Workspace,
    },
    terminal_persistence::{
        FileStore, Host, Identity, Limits, OpenMode, Owner, StoreOptions, TOOL_DIGEST, TOOL_NAME,
    },
};
use tansr_sdk_demo::{Args, archive_key, archive_key_from_file, base_url, client, family, report};
struct Policy {
    owner: Owner,
    cancelled: CancellationToken,
    config_path: PathBuf,
    original_config: serde_json::Value,
}
impl Policy {
    fn current(&self) -> Result<Owner> {
        if self.cancelled.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if read_config(&self.config_path)? != self.original_config {
            return Err(Error::InvalidInput(
                "trusted publication authority changed; preserve original anchors".into(),
            ));
        }
        Ok(self.owner.clone())
    }
}
#[async_trait]
impl Authorizer for Policy {
    async fn authorize(&self, op: &Operation) -> Result<()> {
        if self.current()? != Owner::from_operation(op) || op.tool_name != "MemoryPublication" {
            return Err(Error::InvalidInput("publication authority denied".into()));
        }
        Ok(())
    }
}
fn read_config(path: &std::path::Path) -> Result<serde_json::Value> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(32769)
        .read_to_end(&mut bytes)?;
    tansr_sdk::canonical::parse_json(&bytes, 32768)
}
#[tokio::main]
async fn main() -> std::process::ExitCode {
    report("terminal-persistence", run().await)
}
async fn run() -> Result<()> {
    let mut args = Args::parse()?;
    if args.help() {
        println!(
            "terminal_persistence --config TRUSTED_HOST_JSON --store ABSOLUTE_FILE --journal ABSOLUTE_DIRECTORY --mode create|reopen [--base ORIGIN] [--family sdk1|sdk2-offload-v1] [--stop-file ABSOLUTE_PATH] [--recover-operation ORIGINAL_OPERATION_ID]\nRequires TANSR_TOKEN_FILE and TANSR_ARCHIVE_KEY_FILE. Config contains identity, owner and an already registered live connection. Registration must include only TansrTerminalPersistenceV1 with its frozen digest. Ctrl+C or an explicitly supplied stop-file drains storage. Reopen requires the original encrypted journal; missing media never initializes an empty replacement. The trusted config is reread on every local authorization check; a changed file stops this owner. Offline copy: --config TRUSTED_HOST_JSON --store ORIGINAL_FILE --mode reopen --copy-to NEW_DIRECTORY/METADATA_FILE --copy-key-file NEW_KEY_FILE; omit --journal, no token or Serve connection is used. The target directory must be new under a private parent. Complete encrypted V1 facts are copied; the separate execution journal is not. A persistent authenticated read-only fence keeps cutover pending; ordinary reopen cannot activate it. Preserve both paths and keys on errors. Recovery queries the original operation from Serve and reuses its exact journal key; it prints only operation/digest/transfer/status anchors. This is a host integration example, not a login or memory business implementation."
        );
        return Ok(());
    }
    let base = base_url(&mut args);
    let family = family(&mut args)?;
    let config = PathBuf::from(args.required("--config")?);
    let path = PathBuf::from(args.required("--store")?);
    let journal = args.take("--journal").map(PathBuf::from);
    let copy_to = args.take("--copy-to").map(PathBuf::from);
    let copy_key_file = args.take("--copy-key-file").map(PathBuf::from);
    let mode = match args.required("--mode")?.as_str() {
        "create" => OpenMode::Create,
        "reopen" => OpenMode::Reopen,
        _ => return Err(Error::InvalidInput("mode must be create or reopen".into())),
    };
    let stop_file = args.take("--stop-file").map(PathBuf::from);
    let recovery_operation = args.take("--recover-operation");
    if recovery_operation.is_some() && !matches!(mode, OpenMode::Reopen) {
        return Err(Error::InvalidInput(
            "operation recovery requires reopen of both original stores".into(),
        ));
    }
    if stop_file.as_ref().is_some_and(|path| !path.is_absolute()) {
        return Err(Error::InvalidInput(
            "stop-file requires an absolute host path".into(),
        ));
    }
    if copy_to.is_some() != copy_key_file.is_some()
        || (copy_to.is_some()
            && (!matches!(mode, OpenMode::Reopen)
                || journal.is_some()
                || stop_file.is_some()
                || recovery_operation.is_some()))
    {
        return Err(Error::InvalidInput("copy requires reopen, --copy-to and --copy-key-file; it does not attach a journal or runner".into()));
    }
    if copy_to.is_none() && journal.is_none() {
        return Err(Error::InvalidInput(
            "live attachment requires --journal".into(),
        ));
    }
    args.finish()?;
    let config_path = config;
    let config = read_config(&config_path)?;
    let identity: Identity = serde_json::from_value(config["identity"].clone())?;
    let owner: Owner = serde_json::from_value(config["owner"].clone())?;
    let connection_value = config["connection"].clone();
    let cancelled = CancellationToken::new();
    let policy = Arc::new(Policy {
        owner: owner.clone(),
        cancelled: cancelled.clone(),
        config_path,
        original_config: config,
    });
    let source = policy.clone();
    let key = archive_key()?;
    if let Some(target) = copy_to {
        let new_key = archive_key_from_file(
            &copy_key_file.ok_or_else(|| Error::InvalidInput("copy key missing".into()))?,
        )?;
        let receipt = FileStore::copy_from(
            StoreOptions {
                path,
                mode,
                key,
                identity,
                limits: Limits::default(),
                read_context: Arc::new(move || source.current()),
                authorize_recovery: None,
            },
            target,
            new_key,
        )
        .await?;
        println!("{}", serde_json::to_string(&receipt)?);
        return Ok(());
    }
    let journal = journal.ok_or_else(|| Error::InvalidInput("journal missing".into()))?;
    let connection: Connection = serde_json::from_value(connection_value)?;
    // Check the authenticated original binding before opening either recovery medium.
    let api = client(&base, &family)?;
    let execution = match Client::new(api.clone(), owner.scope.clone()) {
        Ok(client) => client,
        Err(error) => {
            api.shutdown().await;
            return Err(error);
        }
    };
    let preflight = execution.execution_capabilities(&owner.session_id).await;
    if !matches!(&preflight, Ok(capabilities) if capabilities.binding.as_ref() == Some(&owner.binding))
    {
        api.shutdown().await;
        return Err(preflight.err().unwrap_or_else(|| {
            Error::InvalidInput("original publication binding changed".into())
        }));
    }
    let journal = match mode {
        OpenMode::Create => {
            let exists = match journal.try_exists() {
                Ok(exists) => exists,
                Err(error) => {
                    api.shutdown().await;
                    return Err(error.into());
                }
            };
            if exists {
                api.shutdown().await;
                return Err(Error::InvalidInput(
                    "create requires a new journal path; reopen original anchors".into(),
                ));
            }
            FileJournal::open_encrypted(journal, key)
        }
        OpenMode::Reopen => FileJournal::reopen_encrypted(journal, key),
    };
    let journal = match journal {
        Ok(journal) => Arc::new(journal),
        Err(error) => {
            api.shutdown().await;
            return Err(error);
        }
    };
    let store = FileStore::open(StoreOptions {
        path,
        mode,
        key,
        identity,
        limits: Limits::default(),
        read_context: Arc::new(move || source.current()),
        authorize_recovery: None,
    })
    .await;
    let store = match store {
        Ok(store) => Arc::new(store),
        Err(error) => {
            api.shutdown().await;
            return Err(error);
        }
    };
    let result = async {
        let host = Host::new(store.clone(), policy.clone())?;
        let registration = Registration {
            protocol: "sdk2-ext-v1".into(),
            executor_id: owner.binding.target.executor_id.clone(),
            platform: Platform::current(),
            workspaces: vec![Workspace {
                workspace_id: owner.binding.target.workspace_id.clone(),
                revision: owner.binding.target.workspace_revision.clone(),
            }],
            operations: vec!["tool.invoke".into()],
            tools: vec![ToolDefinition {
                name: TOOL_NAME.into(),
                definition_digest: TOOL_DIGEST.into(),
            }],
            interpreter: None,
        };
        let runner = Runner::with_connection(
            RunnerOptions {
                client: execution.clone(),
                registration,
                journal,
                tools: BTreeMap::from([(TOOL_NAME.into(), host.into_tool())]),
                authorize: policy,
                poll_interval: Duration::from_millis(250),
                terminal: None,
                require_output: false,
                restricted_status: false,
            },
            connection,
        )?;
        if let Some(operation_id) = recovery_operation {
            let original = execution.status(&owner.session_id, &operation_id).await?.operation;
            let receipt = runner.execute(original.clone(), cancelled.clone()).await?;
            if let Err(error) = execution.submit(&original, &receipt).await {
                let reconciled = execution.status(&owner.session_id, &operation_id).await?;
                if reconciled.operation != original || reconciled.receipt.as_ref() != Some(&receipt) {
                    return Err(error);
                }
            }
            let request = original.request.args["argsJson"].as_str()
                .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok());
            println!("{}", serde_json::json!({"operationId":original.operation_id,"digest":original.digest,
                "transferId":request.as_ref().and_then(|v|v.get("transferId")),"status":receipt.status}));
            return Ok(());
        }
        println!("Encrypted V1 root, body blocks, opaque indexes, permanent transfer witnesses and execution receipts attached; original recovery anchors retained.");
        let signal = cancelled.clone();
        let interrupt = tokio::spawn(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = async {
                    loop {
                        if let Some(path) = &stop_file {
                            // Missing means keep running; all other lookup failures stop closed.
                            if path.try_exists().unwrap_or(true) { break; }
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                } => {},
            }
            signal.cancel();
        });
        let result = runner.run(cancelled.clone()).await;
        interrupt.abort();
        let _ = interrupt.await;
        result
    }.await;
    let stopped_by_host = cancelled.is_cancelled();
    cancelled.cancel();
    let closed = store.close().await;
    api.shutdown().await;
    match result {
        Err(Error::Cancelled) if stopped_by_host => closed,
        other => {
            closed?;
            other
        }
    }
}
