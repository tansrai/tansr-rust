mod support;
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tansr_sdk::{
    ApiClient, CallOptions, CancellationToken, Error, Result,
    executor::*,
    memory_publication::{
        CONTRACT, FileStore, Host, Identity, Limits, MemoryPublicationStore, OpenMode, Owner,
        StoreOptions, TOOL_DIGEST, TOOL_NAME,
    },
    session::*,
};

struct ReleaseOnDrop(Arc<(Mutex<bool>, Condvar)>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        let (lock, wake) = &*self.0;
        *lock.lock().unwrap() = true;
        wake.notify_all();
    }
}
struct Policy(Arc<Mutex<Owner>>);
#[async_trait]
impl Authorizer for Policy {
    async fn authorize(&self, operation: &Operation) -> Result<()> {
        if *self.0.lock().unwrap() != Owner::from_operation(operation)
            || operation.tool_name != "MemoryPublication"
        {
            return Err(Error::InvalidInput(
                "current publication owner denied".into(),
            ));
        }
        Ok(())
    }
}
struct Harness {
    api: ApiClient,
    client: Client,
    session: Session,
    connection: Connection,
    registration: Registration,
    owner: Owner,
    current: Arc<Mutex<Owner>>,
    identity: Identity,
    directory: tempfile::TempDir,
    root: PathBuf,
}
impl Harness {
    async fn new(fixture: &support::Serve) -> Self {
        let api = fixture.client("sdk1");
        let session = SessionClient::new(api.clone())
            .unwrap()
            .create(CreateOptions {
                tools: Some(vec!["SearchMemory".into()]),
                ..Default::default()
            })
            .await
            .unwrap();
        let scope = Scope {
            application_scope_id: fixture.info["applicationScopeId"].as_str().unwrap().into(),
            end_user_id: fixture.info["endUserId"].as_str().unwrap().into(),
            authorization_revision: fixture.info["authorizationRevision"]
                .as_str()
                .unwrap()
                .into(),
        };
        let client = Client::new(api.clone(), scope.clone()).unwrap();
        let workspace = Workspace {
            workspace_id: "rust-memory".into(),
            revision: "1".into(),
        };
        let registration = Registration {
            protocol: PROTOCOL.into(),
            executor_id: fixture.info["executorId"].as_str().unwrap().into(),
            platform: Platform::current(),
            workspaces: vec![workspace.clone()],
            operations: vec!["tool.invoke".into()],
            tools: vec![ToolDefinition {
                name: TOOL_NAME.into(),
                definition_digest: TOOL_DIGEST.into(),
            }],
            interpreter: None,
        };
        let connection = client.register(&registration).await.unwrap();
        let closure = session.capabilities().await.unwrap();
        let initialized = client
            .initialize(
                session.id(),
                &registration.platform,
                None,
                &closure.closure_id,
            )
            .await
            .unwrap();
        let closure = session.capabilities().await.unwrap();
        let binding = client
            .bind(
                session.id(),
                &connection,
                &workspace,
                &initialized.capability_revision,
                &closure.closure_id,
            )
            .await
            .unwrap()
            .binding
            .unwrap();
        let owner = Owner {
            scope,
            session_id: session.id().into(),
            binding,
        };
        let body = json!({"contract":CONTRACT,"requestId":"rust-publication-binding","session":{"sessionContract":"sdk1","sessionId":session.id()},"executionBinding":owner.binding,"required":["memory-lifecycle-v1"],"optional":[]});
        let response = api
            .call(
                "terminal.binding.create",
                CallOptions {
                    body: Some(body),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .json()
            .unwrap();
        tansr_sdk::api::validate_wire(CONTRACT, "BindingResponse", &response).unwrap();
        assert_eq!(
            response["executionBinding"],
            serde_json::to_value(&owner.binding).unwrap()
        );
        assert_eq!(
            response["scope"],
            serde_json::to_value(&owner.scope).unwrap()
        );
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap().join("private");
        tansr_sdk::archive::create_private_directory(&root).unwrap();
        Self {
            api,
            client,
            session,
            connection,
            registration,
            current: Arc::new(Mutex::new(owner.clone())),
            owner,
            identity: Identity {
                application_scope_id:
                    fixture.info["publicationIdentity"]["scope"]["applicationScopeId"]
                        .as_str()
                        .unwrap()
                        .into(),
                end_user_id: fixture.info["publicationIdentity"]["scope"]["endUserId"]
                    .as_str()
                    .unwrap()
                    .into(),
                source_id: fixture.info["publicationIdentity"]["sourceId"]
                    .as_str()
                    .unwrap()
                    .into(),
                source_generation: fixture.info["publicationIdentity"]["sourceGeneration"]
                    .as_str()
                    .unwrap()
                    .into(),
                domain_key: fixture.info["publicationIdentity"]["domainKey"]
                    .as_str()
                    .unwrap()
                    .into(),
            },
            directory,
            root,
        }
    }
    fn options(&self, mode: OpenMode) -> StoreOptions {
        let current = self.current.clone();
        StoreOptions {
            path: self.root.join("publication.bin"),
            mode,
            key: [51; 32],
            identity: self.identity.clone(),
            limits: Limits::default(),
            read_context: Arc::new(move || Ok(current.lock().unwrap().clone())),
            authorize_recovery: None,
        }
    }
    fn journal(&self) -> Arc<FileJournal> {
        Arc::new(FileJournal::open_encrypted(self.root.join("journal"), [52; 32]).unwrap())
    }
    fn runner(
        &self,
        store: Arc<dyn MemoryPublicationStore>,
        journal: Arc<FileJournal>,
        api: Option<ApiClient>,
    ) -> Arc<Runner> {
        let policy = Arc::new(Policy(self.current.clone()));
        let host = Host::new(store, policy.clone()).unwrap();
        Arc::new(
            Runner::with_connection(
                RunnerOptions {
                    client: api
                        .map(|api| Client::new(api, self.owner.scope.clone()).unwrap())
                        .unwrap_or_else(|| self.client.clone()),
                    registration: self.registration.clone(),
                    journal,
                    tools: BTreeMap::from([(TOOL_NAME.into(), host.into_tool())]),
                    authorize: policy,
                    poll_interval: Duration::from_millis(20),
                    terminal: None,
                    require_output: false,
                    restricted_status: false,
                },
                self.connection.clone(),
            )
            .unwrap(),
        )
    }
    fn request(&self, action: &str) -> Value {
        json!({"contract":CONTRACT,"action":action,"sourceId":self.identity.source_id,"sourceGeneration":self.identity.source_generation,"domainKey":self.identity.domain_key})
    }
    fn memory(&self) -> tokio::task::JoinHandle<Result<Value>> {
        let api = self.api.clone();
        let session = self.session.id().to_owned();
        tokio::spawn(async move {
            let response = api
                .call(
                    "terminal.memory.read",
                    CallOptions {
                        params: BTreeMap::from([("id".into(), session)]),
                        query: BTreeMap::from([
                            ("contract".into(), CONTRACT.into()),
                            ("sessionContract".into(), "sdk1".into()),
                        ]),
                        ..Default::default()
                    },
                )
                .await?
                .json()?;
            tansr_sdk::api::validate_wire(CONTRACT, "MemoryStateResponse", &response)?;
            Ok(response)
        })
    }
    async fn next(&self) -> Operation {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let batch = self.client.poll(&self.connection).await.unwrap();
                if let Some(op) = batch.operations.into_iter().next() {
                    return op;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("real publication dispatcher did not produce an operation")
    }
    fn entries(&self) -> usize {
        std::fs::read_dir(self.root.join("journal"))
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|v| v == "claim" || v == "receipt")
            })
            .count()
    }
    async fn close(self) {
        self.session.close(Default::default()).await.unwrap();
        self.api.shutdown().await;
        assert!(self.directory.path().exists());
    }
}
struct Observed {
    store: Arc<FileStore>,
    requests: Mutex<Vec<Value>>,
    lose_commit: AtomicBool,
}
#[async_trait]
impl MemoryPublicationStore for Observed {
    fn identity(&self) -> &Identity {
        self.store.identity()
    }
    fn atomic_durable_publication(&self) -> bool {
        true
    }
    fn encrypted_at_rest(&self) -> bool {
        true
    }
    async fn execute(&self, request: Value, owner: Owner) -> Result<Value> {
        self.requests.lock().unwrap().push(request.clone());
        let response = self.store.execute(request.clone(), owner).await?;
        if request["action"] == "commit" && self.lose_commit.swap(false, Ordering::SeqCst) {
            return Err(Error::Unknown("controlled lost local commit return".into()));
        }
        Ok(response)
    }
}
async fn drive(
    h: &Harness,
    runner: &Runner,
    memory: &mut tokio::task::JoinHandle<Result<Value>>,
) -> Vec<(Operation, Receipt)> {
    tokio::time::timeout(Duration::from_secs(35), async {
        let mut facts = Vec::new();
        loop {
            if memory.is_finished() {
                return facts;
            }
            let batch = h.client.poll(&h.connection).await.unwrap();
            for op in batch.operations {
                let receipt = runner
                    .execute(op.clone(), CancellationToken::new())
                    .await
                    .unwrap();
                let status = h.client.submit(&op, &receipt).await.unwrap();
                assert_eq!(status.operation, op);
                assert_eq!(status.receipt.as_ref(), Some(&receipt));
                facts.push((op, receipt));
                assert!(facts.len() <= 32);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("publication pipeline timed out")
}
#[tokio::test]
#[ignore = "requires frozen packaged publication Serve host"]
async fn real_serve_publication_original_identity_receipts_and_cold_reopen() {
    let f = support::Serve::start("publication").await;
    let h = Harness::new(&f).await;
    let store = Arc::new(FileStore::open(h.options(OpenMode::Create)).await.unwrap());
    let observed = Arc::new(Observed {
        store: store.clone(),
        requests: Mutex::new(Vec::new()),
        lose_commit: AtomicBool::new(false),
    });
    let journal = h.journal();
    let runner = h.runner(observed.clone(), journal.clone(), None);
    let mut memory = h.memory();
    let facts = drive(&h, &runner, &mut memory).await;
    let state = memory.await.unwrap().unwrap();
    assert_eq!(state["memory"]["available"], true);
    assert!(facts.len() >= 4);
    assert!(facts.iter().all(|(_, r)| r.status == "completed"));
    let head = store
        .execute(h.request("head"), h.owner.clone())
        .await
        .unwrap();
    assert!(!head["publication"].is_null());
    let action_list: Vec<_> = observed
        .requests
        .lock()
        .unwrap()
        .iter()
        .map(|r| r["action"].as_str().unwrap().to_owned())
        .collect();
    assert!(
        action_list.iter().any(|a| a == "begin")
            && action_list.iter().any(|a| a == "chunk")
            && action_list.iter().any(|a| a == "commit")
    );
    drop(runner);
    drop(journal);
    store.close().await.unwrap();
    let reopened = Arc::new(FileStore::open(h.options(OpenMode::Reopen)).await.unwrap());
    let journal =
        Arc::new(FileJournal::reopen_encrypted(h.root.join("journal"), [52; 32]).unwrap());
    for (op, receipt) in &facts {
        assert!(matches!(journal.claim(op).await.unwrap(),ClaimResult::Receipt(r) if *r==*receipt));
    }
    assert_eq!(
        reopened
            .execute(h.request("head"), h.owner.clone())
            .await
            .unwrap(),
        head
    );
    let encrypted = std::fs::read(h.root.join("publication.bin")).unwrap();
    assert!(!String::from_utf8_lossy(&encrypted).contains(&h.identity.domain_key));
    reopened.close().await.unwrap();
    drop(journal);
    h.close().await;
    f.stop().await;
}
#[tokio::test]
#[ignore = "requires frozen packaged publication Serve host"]
async fn real_serve_publication_commit_unknown_replays_original_key_after_reopen() {
    let f = support::Serve::start("publication").await;
    let h = Harness::new(&f).await;
    let store = Arc::new(FileStore::open(h.options(OpenMode::Create)).await.unwrap());
    let observed = Arc::new(Observed {
        store: store.clone(),
        requests: Mutex::new(Vec::new()),
        lose_commit: AtomicBool::new(true),
    });
    let journal = Arc::new(FileJournal::open_encrypted(h.root.join("journal"), [51; 32]).unwrap());
    let runner = h.runner(observed.clone(), journal.clone(), None);
    let mut memory = h.memory();
    let facts = drive(&h, &runner, &mut memory).await;
    let _uncertain_response = memory.await.unwrap();
    let (op, receipt) =
        facts
            .iter()
            .find(|(op, _)| {
                serde_json::from_str::<Value>(op.request.args["argsJson"].as_str().unwrap())
                    .unwrap()["action"]
                    == "commit"
            })
            .expect("actual Serve commit")
            .clone();
    assert_eq!(receipt.status, "unknown");
    let request: Value =
        serde_json::from_str(op.request.args["argsJson"].as_str().unwrap()).unwrap();
    let before = observed.requests.lock().unwrap().len();
    assert_eq!(
        runner
            .execute(op.clone(), CancellationToken::new())
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(observed.requests.lock().unwrap().len(), before);
    drop(runner);
    drop(journal);
    store.close().await.unwrap();
    let reopened = Arc::new(FileStore::open(h.options(OpenMode::Reopen)).await.unwrap());
    let restored_journal =
        Arc::new(FileJournal::reopen_encrypted(h.root.join("journal"), [51; 32]).unwrap());
    let restored = h.runner(reopened.clone(), restored_journal.clone(), None);
    assert_eq!(
        restored
            .execute(op.clone(), CancellationToken::new())
            .await
            .unwrap(),
        receipt
    );
    let mut query = h.request("query");
    query["transferId"] = request["transferId"].clone();
    let recovered = reopened.execute(query, h.owner.clone()).await.unwrap();
    assert_eq!(recovered["transfer"]["status"], "committed");
    assert_eq!(
        h.client
            .status(h.session.id(), &op.operation_id)
            .await
            .unwrap()
            .receipt,
        Some(receipt)
    );
    drop(restored);
    drop(restored_journal);
    reopened.close().await.unwrap();
    private_file(
        &h.root.join("host.json"),
        &serde_json::to_vec(
            &json!({"identity":h.identity,"owner":h.owner,"connection":h.connection}),
        )
        .unwrap(),
    );
    private_file(
        &h.root.join("token.txt"),
        f.info["token"].as_str().unwrap().as_bytes(),
    );
    private_file(&h.root.join("key.txt"), "33".repeat(32).as_bytes());
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        demo_command(&h, &f, "reopen", "unused-stop", "journal")
            .arg("--recover-operation")
            .arg(&op.operation_id)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        result.status.success(),
        "original-key demo recovery failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let recovery: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(recovery["operationId"], op.operation_id);
    assert_eq!(recovery["digest"], op.digest);
    assert_eq!(recovery["transferId"], request["transferId"]);
    assert_eq!(recovery["status"], "unknown");
    h.close().await;
    f.stop().await;
}
#[tokio::test]
#[ignore = "requires frozen packaged publication Serve host with auth controls"]
async fn real_serve_publication_wrong_scope_and_revocation_never_claim() {
    let mut f = support::Serve::start("publication").await;
    let h = Harness::new(&f).await;
    let store = Arc::new(FileStore::open(h.options(OpenMode::Create)).await.unwrap());
    let journal = h.journal();
    let runner = h.runner(store.clone(), journal.clone(), None);
    let memory = h.memory();
    let op = h.next().await;
    h.current.lock().unwrap().scope.end_user_id = "foreign-user".into();
    assert!(
        runner
            .execute(op.clone(), CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(h.entries(), 0);
    *h.current.lock().unwrap() = h.owner.clone();
    let other = ApiClient::builder(f.info["baseURL"].as_str().unwrap())
        .token(f.info["otherToken"].as_str().unwrap())
        .session_family("sdk1")
        .build()
        .unwrap();
    let denied = h.runner(store.clone(), journal.clone(), Some(other.clone()));
    assert!(
        denied
            .execute(op.clone(), CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(h.entries(), 0);
    drop(denied);
    other.shutdown().await;
    f.send_control(
        &json!({"command":"set-auth","requestId":"rust-publication-revoke","allowed":false}),
    )
    .await;
    assert!(runner.execute(op, CancellationToken::new()).await.is_err());
    assert_eq!(h.entries(), 0);
    f.send_control(
        &json!({"command":"set-auth","requestId":"rust-publication-restore","allowed":true}),
    )
    .await;
    memory.abort();
    let _ = memory.await;
    drop(runner);
    drop(journal);
    store.close().await.unwrap();
    h.close().await;
    f.stop().await;
}
#[tokio::test]
#[ignore = "requires frozen packaged publication Serve host"]
async fn real_serve_publication_inflight_cancel_drains_store_and_keeps_original_unknown() {
    let f = support::Serve::start("publication").await;
    let h = Harness::new(&f).await;
    let requested = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(tokio::sync::Notify::new());
    let barrier = Arc::new((Mutex::new(false), Condvar::new()));
    let _release_on_panic = ReleaseOnDrop(barrier.clone());
    let mut options = h.options(OpenMode::Create);
    let current = h.current.clone();
    let blocked = requested.clone();
    let signal = entered.clone();
    let release = barrier.clone();
    options.read_context = Arc::new(move || {
        if blocked.swap(false, Ordering::SeqCst) {
            signal.notify_one();
            let (lock, wake) = &*release;
            let mut done = lock.lock().unwrap();
            while !*done {
                done = wake.wait(done).unwrap();
            }
        }
        Ok(current.lock().unwrap().clone())
    });
    let store = Arc::new(FileStore::open(options).await.unwrap());
    let journal = h.journal();
    let runner = h.runner(store.clone(), journal.clone(), None);
    let memory = h.memory();
    let op = h.next().await;
    requested.store(true, Ordering::SeqCst);
    let cancel = CancellationToken::new();
    let execution = runner.clone();
    let operation = op.clone();
    let token = cancel.clone();
    let work = tokio::spawn(async move { execution.execute(operation, token).await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    cancel.cancel();
    // Runner gives cooperative handlers a five-second cleanup window before unknown.
    let receipt = tokio::time::timeout(Duration::from_secs(10), work)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(receipt.status, "unknown");
    let pending = store.clone();
    let closing = tokio::spawn(async move { pending.close().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !closing.is_finished(),
        "close must drain accepted disk work"
    );
    {
        let (lock, wake) = &*barrier;
        *lock.lock().unwrap() = true;
        wake.notify_all();
    }
    tokio::time::timeout(Duration::from_secs(5), closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(runner);
    drop(journal);
    let reopened = FileStore::open(h.options(OpenMode::Reopen)).await.unwrap();
    let journal = FileJournal::reopen_encrypted(h.root.join("journal"), [52; 32]).unwrap();
    assert!(matches!(journal.claim(&op).await.unwrap(),ClaimResult::Receipt(r) if *r==receipt));
    h.client.submit(&op, &receipt).await.unwrap();
    memory.abort();
    let _ = memory.await;
    reopened.close().await.unwrap();
    drop(journal);
    h.close().await;
    f.stop().await;
}

fn private_file(path: &std::path::Path, bytes: &[u8]) {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}
fn demo_command(
    h: &Harness,
    f: &support::Serve,
    mode: &str,
    stop: &str,
    journal: &str,
) -> tokio::process::Command {
    let binary = std::env::var_os("TANSR_RUST_MEMORY_DEMO").expect("build the memory_publication example and set TANSR_RUST_MEMORY_DEMO to its absolute binary path");
    assert!(std::path::Path::new(&binary).is_absolute());
    let mut command = tokio::process::Command::new(binary);
    command
        .args([
            "--base",
            f.info["baseURL"].as_str().unwrap(),
            "--mode",
            mode,
        ])
        .arg("--config")
        .arg(h.root.join("host.json"))
        .arg("--store")
        .arg(h.root.join("publication.bin"))
        .arg("--journal")
        .arg(h.root.join(journal))
        .arg("--stop-file")
        .arg(h.root.join(stop))
        .env("TANSR_TOKEN_FILE", h.root.join("token.txt"))
        .env("TANSR_ARCHIVE_KEY_FILE", h.root.join("key.txt"))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    command
}
#[tokio::test]
#[ignore = "requires frozen packaged publication Serve host and compiled memory_publication example"]
async fn real_serve_publication_demo_cold_process_reopen_and_missing_journal_fail_closed() {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let f = support::Serve::start("publication").await;
    let h = Harness::new(&f).await;
    private_file(
        &h.root.join("host.json"),
        &serde_json::to_vec(
            &json!({"identity":h.identity,"owner":h.owner,"connection":h.connection}),
        )
        .unwrap(),
    );
    private_file(
        &h.root.join("token.txt"),
        f.info["token"].as_str().unwrap().as_bytes(),
    );
    private_file(&h.root.join("key.txt"), "33".repeat(32).as_bytes());
    let mut original_head = None;
    for (mode, stop) in [("create", "stop-first"), ("reopen", "stop-reopened")] {
        let mut child = demo_command(&h, &f, mode, stop, "demo-journal")
            .spawn()
            .unwrap();
        let mut stdout = BufReader::new(child.stdout.take().unwrap()).lines();
        let line = tokio::time::timeout(Duration::from_secs(15), stdout.next_line())
            .await
            .unwrap()
            .unwrap()
            .expect("compiled demo ready");
        assert!(line.contains("original recovery anchors retained"));
        let memory = tokio::time::timeout(Duration::from_secs(35), h.memory())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(memory["memory"]["available"], true);
        private_file(&h.root.join(stop), b"stop");
        let result = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        assert!(
            result.status.success(),
            "demo failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let store = FileStore::open(h.options(OpenMode::Reopen)).await.unwrap();
        let head = store
            .execute(h.request("head"), h.owner.clone())
            .await
            .unwrap();
        assert!(!head["publication"].is_null());
        if let Some(original) = &original_head {
            assert_eq!(&head, original);
        } else {
            original_head = Some(head);
        }
        store.close().await.unwrap();
        let journal = FileJournal::reopen_encrypted(h.root.join("demo-journal"), [51; 32]).unwrap();
        assert!(
            std::fs::read_dir(h.root.join("demo-journal"))
                .unwrap()
                .any(|e| e
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|v| v == "receipt"))
        );
        drop(journal);
    }
    let before = std::fs::read(h.root.join("publication.bin")).unwrap();
    let rejected = tokio::time::timeout(
        Duration::from_secs(10),
        demo_command(&h, &f, "reopen", "stop-missing", "missing-journal").output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!rejected.status.success());
    assert!(!h.root.join("missing-journal").exists());
    assert_eq!(
        std::fs::read(h.root.join("publication.bin")).unwrap(),
        before
    );
    h.close().await;
    f.stop().await;
}
