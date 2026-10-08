//! Explicit real Serve acceptance of the three compiled Rust commands.
//! The private fixture substitutes only a synthetic model, never the SDK or
//! Serve HTTP/executor/archive implementation. No production token is read.
#[path = "../../tests/support/mod.rs"]
mod support;

use futures_util::StreamExt;
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tansr_sdk::{
    CallOptions, CancellationToken,
    archive::{
        ArchiveClient, ArchiveStore, Coverage, FileStore, Identity, RequestIdentity, StoreLimits,
        StoreOptions, create_private_directory,
    },
    executor::{Batch, Client as ExecutorClient, Operation, Receipt, Scope},
    session::{CreateOptions, OutcomeStatus, Session, SessionClient, TurnTracker, WriteOptions},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdout, Command},
};

const CHAT: &str = env!("CARGO_BIN_EXE_tansr-chat");
const TOOLS: &str = env!("CARGO_BIN_EXE_tansr-tools");
const ARCHIVE: &str = env!("CARGO_BIN_EXE_tansr-archive");

struct Files {
    _root: tempfile::TempDir,
    private: PathBuf,
    token: PathBuf,
    key: PathBuf,
}
impl Files {
    fn new(serve: &support::Serve) -> Self {
        let root = tempfile::tempdir().expect("temporary demo directory");
        let private = root.path().canonicalize().unwrap().join("private");
        // Use the same Unix/Windows ownership rules as the product. Do not
        // relax ACLs just to let a test or a child process open the files.
        create_private_directory(&private).expect("private demo directory");
        let token = private.join("token.txt");
        let key = private.join("archive-key.txt");
        private_file(&token, serve.info["token"].as_str().unwrap().as_bytes());
        private_file(&key, "71".repeat(32).as_bytes());
        Self {
            _root: root,
            private,
            token,
            key,
        }
    }

    fn command(&self, bin: &str, serve: &support::Serve, family: &str) -> Command {
        let mut command = Command::new(bin);
        command
            .arg("--base")
            .arg(serve.info["baseURL"].as_str().unwrap())
            .arg("--family")
            .arg(family)
            .env("TANSR_TOKEN_FILE", &self.token)
            .env("TANSR_ARCHIVE_KEY_FILE", &self.key)
            .current_dir(&self.private)
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        command
    }
}

fn private_file(path: &Path, bytes: &[u8]) {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    // Windows inherits the SDK-created private directory's protected ACL.
    let mut file = options.open(path).expect("create private synthetic file");
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
    #[cfg(unix)]
    std::fs::File::open(path.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
}

#[tokio::test]
async fn compiled_demos_help_and_invalid_flags_never_echo_supplied_secrets() {
    for binary in [CHAT, TOOLS, ARCHIVE] {
        let help = Command::new(binary)
            .arg("--help")
            .env_remove("TANSR_TOKEN_FILE")
            .kill_on_drop(true)
            .output()
            .await
            .unwrap();
        assert!(help.status.success());
        assert!(String::from_utf8_lossy(&help.stdout).contains("TANSR_TOKEN_FILE"));
        let invalid = Command::new(binary)
            .args([
                "--base",
                "fake-token-secret",
                "--base",
                "http://127.0.0.1:8787",
            ])
            .env_remove("TANSR_TOKEN_FILE")
            .kill_on_drop(true)
            .output()
            .await
            .unwrap();
        assert!(!invalid.status.success());
        assert!(!String::from_utf8_lossy(&invalid.stdout).contains("fake-token-secret"));
        assert!(!String::from_utf8_lossy(&invalid.stderr).contains("fake-token-secret"));
    }
}

struct Demo {
    child: Child,
    lines: Lines<BufReader<ChildStdout>>,
    transcript: String,
}
impl Demo {
    fn start(command: &mut Command) -> Self {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("start compiled Rust demo");
        let lines = BufReader::new(child.stdout.take().unwrap()).lines();
        Self {
            child,
            lines,
            transcript: String::new(),
        }
    }

    async fn until(&mut self, marker: &str) -> String {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let line = self
                    .lines
                    .next_line()
                    .await
                    .expect("demo output")
                    .expect("demo exited before expected output");
                assert!(self.transcript.len() + line.len() < 1_048_576);
                self.transcript.push_str(&line);
                self.transcript.push('\n');
                if line.contains(marker) {
                    return line;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("demo timed out before {marker}"))
    }

    async fn input(&mut self, line: &str) {
        let stdin = self.child.stdin.as_mut().expect("interactive demo stdin");
        stdin.write_all(line.as_bytes()).await.unwrap();
        stdin.write_all(b"\n").await.unwrap();
        stdin.flush().await.unwrap();
    }

    async fn finish(mut self) {
        drop(self.child.stdin.take());
        let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait())
            .await
            .expect("demo exit timeout")
            .unwrap();
        assert!(status.success(), "demo exit was not successful: {status}");
    }

    async fn finish_with_status(mut self, success: bool) -> String {
        drop(self.child.stdin.take());
        tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(line) = self.lines.next_line().await.unwrap() {
                assert!(self.transcript.len() + line.len() < 1_048_576);
                self.transcript.push_str(&line);
                self.transcript.push('\n');
            }
            let status = self.child.wait().await.unwrap();
            assert_eq!(
                status.success(),
                success,
                "unexpected demo status: {status}"
            );
        })
        .await
        .expect("demo did not exit and release its output");
        self.transcript
    }

    async fn cleanup(mut self) {
        // Test cleanup after a confirmed business terminal is not evidence of
        // the product's graceful Ctrl+C or remote interruption behavior.
        self.child.start_kill().unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.child.wait())
            .await
            .expect("demo cleanup timeout")
            .unwrap();
    }
}

async fn wait_idle(session: &Session) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let meta = session.meta().await.unwrap();
            if meta.status == "idle" && meta.live {
                return;
            }
            assert_eq!(meta.status, "running", "session did not remain live");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Serve turn did not settle to idle");
}

#[tokio::test]
#[ignore = "requires TANSR_RUST_SERVE_FIXTURE verified private real Serve fixture"]
async fn real_chat_process_two_turns_and_local_exit() {
    let serve = support::Serve::start("session").await;
    let files = Files::new(&serve);
    let api = serve.client("sdk1");
    let sessions = SessionClient::new(api.clone()).unwrap();
    let mut command = files.command(CHAT, &serve, "sdk1");
    command.args(["--timeout", "90"]);
    let mut demo = Demo::start(&mut command);
    let line = demo.until("session: ").await;
    let session_id = line.strip_prefix("session: ").unwrap();
    let session = sessions.attach(session_id).await.unwrap();
    for message in ["RUST-DEMO-FIRST", "RUST-DEMO-SECOND"] {
        wait_idle(&session).await;
        demo.input(message).await;
        demo.until("[turn completed]").await;
    }
    assert_eq!(demo.transcript.matches("[turn completed]").count(), 2);
    assert_eq!(demo.transcript.matches("go-real-serve-answer").count(), 2);
    wait_idle(&session).await;
    let history = session.history(0, 100).await.unwrap().to_string();
    assert!(history.contains("RUST-DEMO-FIRST"));
    assert!(history.contains("RUST-DEMO-SECOND"));
    demo.input("/quit").await;
    demo.finish().await;
    assert_eq!(session.meta().await.unwrap().status, "idle");
    api.shutdown().await;
    serve.stop().await;
}

#[tokio::test]
#[ignore = "requires TANSR_RUST_SERVE_FIXTURE verified private real Serve fixture"]
async fn real_chat_process_local_quit_does_not_interrupt_but_explicit_interrupt_does() {
    let serve = support::Serve::start("session").await;
    let files = Files::new(&serve);
    let api = serve.client("sdk1");
    let mut command = files.command(CHAT, &serve, "sdk1");
    let mut demo = Demo::start(&mut command);
    let created = demo.until("session: ").await;
    let session = SessionClient::new(api.clone())
        .unwrap()
        .attach(created.strip_prefix("session: ").unwrap())
        .await
        .unwrap();
    demo.input("GO-BLOCK").await;
    demo.until("[event: turn.started]").await;
    demo.input("/quit").await;
    let output = demo.finish_with_status(true).await;
    assert!(output.contains("local observation stopped; Serve turn is still unconfirmed"));
    assert!(!output.contains("[turn completed]"));
    assert_eq!(session.meta().await.unwrap().status, "running");

    let mut command = files.command(CHAT, &serve, "sdk1");
    command.args(["--resume", session.id()]);
    let mut resumed = Demo::start(&mut command);
    resumed.until("session: ").await;
    resumed.input("/interrupt").await;
    let output = resumed.finish_with_status(false).await;
    assert!(output.contains("interruption requested; waiting for the Serve terminal event"));
    assert!(output.contains("[event: turn.aborted]"));
    assert!(!output.contains("[turn completed]"));
    wait_idle(&session).await;
    api.shutdown().await;
    serve.stop().await;
}

#[tokio::test]
#[ignore = "requires TANSR_RUST_SERVE_FIXTURE verified private real Serve fixture"]
async fn real_tools_process_business_receipt_without_output() {
    real_tools(false).await;
}

#[tokio::test]
#[ignore = "requires verified private Serve fixture with business output support"]
async fn real_tools_process_first_chunk_before_completion_and_final_seal() {
    real_tools(true).await;
}

async fn real_tools(with_output: bool) {
    let serve = support::Serve::start("execution-demo").await;
    let files = Files::new(&serve);
    let journal = files.private.join("journal");
    let mut command = files.command(TOOLS, &serve, "sdk1");
    command
        .arg("--journal")
        .arg(&journal)
        .args(["--executor", "go-executor"])
        .arg("--application")
        .arg(serve.info["applicationScopeId"].as_str().unwrap())
        .arg("--user")
        .arg(serve.info["endUserId"].as_str().unwrap())
        .arg("--authorization-revision")
        .arg(serve.info["authorizationRevision"].as_str().unwrap());
    if with_output {
        command.arg("--require-output");
    }
    let mut demo = Demo::start(&mut command);
    let line = demo.until("session: ").await;
    let session_id = line.strip_prefix("session: ").unwrap();
    demo.until("ready: ").await;
    let api = serve.client("sdk1");
    let executor = ExecutorClient::new(
        api.clone(),
        Scope {
            application_scope_id: serve.info["applicationScopeId"].as_str().unwrap().into(),
            end_user_id: serve.info["endUserId"].as_str().unwrap().into(),
            authorization_revision: serve.info["authorizationRevision"].as_str().unwrap().into(),
        },
    )
    .unwrap();
    let session = SessionClient::new(api.clone())
        .unwrap()
        .attach(session_id)
        .await
        .unwrap();
    let floor = session.meta().await.unwrap().last_seq;
    let mut tracker = TurnTracker::new(floor).unwrap();
    let mut events = session
        .events(Some(&floor.to_string()), CancellationToken::new())
        .await
        .unwrap();
    let mut chat_command = files.command(CHAT, &serve, "sdk1");
    chat_command.args(["--resume", session.id()]);
    let mut chat = Demo::start(&mut chat_command);
    chat.until("session: ").await;
    chat.input("GO-TOOL").await;
    let output_observer = if with_output {
        let client = executor.clone();
        let session_id = session.id().to_owned();
        Some(tokio::spawn(async move {
            let bound = client.execution_capabilities(&session_id).await.unwrap();
            let target = bound.binding.unwrap().target;
            // This is the same read-only poll exposed to the registered executor;
            // it observes the actual pending operation and never executes it.
            let operation = tokio::time::timeout(Duration::from_secs(15), async {
                loop {
                    let response = client
                        .api()
                        .call(
                            "executor.operations.poll",
                            CallOptions {
                                params: BTreeMap::from([("id".into(), target.executor_id.clone())]),
                                query: BTreeMap::from([(
                                    "connectionId".into(),
                                    target.connection_id.clone(),
                                )]),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();
                    let batch: Batch = serde_json::from_value(response.json().unwrap()).unwrap();
                    if let Some(operation) = batch.operations.into_iter().next() {
                        break operation;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("demo did not dispatch an observable operation");
            tokio::time::timeout(Duration::from_secs(15), async {
                loop {
                    let value = client
                        .api()
                        .call("terminal.output.status", output_query(&operation))
                        .await
                        .unwrap()
                        .json()
                        .unwrap();
                    if value["acceptedThrough"].is_string() {
                        assert_eq!(
                            value["state"], "receiving",
                            "first chunk must arrive before the handler returns"
                        );
                        assert_eq!(value["nextByteOffset"], "21");
                        assert_eq!(
                            client
                                .status(&session_id, &operation.operation_id)
                                .await
                                .unwrap()
                                .status,
                            "pending"
                        );
                        return operation;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("first output chunk was not observed during the handler")
        }))
    } else {
        None
    };
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let event = events.next().await.expect("business stream EOF").unwrap();
            if event.kind() == "server.permission.request" {
                // Explicitly authorized fixture action for this one synthetic
                // order query. The demo and SDK never automatically approve.
                let ticket = event.raw()["requestId"].as_str().unwrap();
                let prompt = chat.until("[permission ").await;
                assert!(prompt.contains(ticket));
                assert_eq!(session.meta().await.unwrap().status, "running");
                chat.input(&format!("/allow {ticket}")).await;
            }
            if let Some(outcome) = tracker.observe(&event) {
                assert_eq!(outcome.status, OutcomeStatus::Completed);
                break;
            }
        }
    })
    .await
    .expect("business executor did not finish");
    events.shutdown().await;
    // The compiled chat sent this turn and handled its actual approval ticket.
    chat.until("[turn completed]").await;
    chat.input("/quit").await;
    chat.finish().await;
    wait_idle(&session).await;
    let history = session.history(0, 100).await.unwrap().to_string();
    assert!(history.contains("awaiting shipment"));
    assert!(history.contains("go-tool-complete"));
    let receipts: Vec<PathBuf> = std::fs::read_dir(&journal)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "receipt"))
        .collect();
    assert_eq!(
        receipts.len(),
        1,
        "business action needs one durable receipt"
    );
    let receipt: Receipt = serde_json::from_slice(&std::fs::read(&receipts[0]).unwrap()).unwrap();
    assert_eq!(receipt.status, "completed");
    assert_eq!(receipt.executor_id, "go-executor");
    let status = executor
        .status(session.id(), &receipt.operation_id)
        .await
        .unwrap();
    assert_eq!(status.status, "completed");
    assert_eq!(status.receipt.as_ref(), Some(&receipt));
    if let Some(observer) = output_observer {
        let operation = observer.await.unwrap();
        assert_eq!(operation.operation_id, receipt.operation_id);
        let output = api
            .call("terminal.output.status", output_query(&operation))
            .await
            .unwrap()
            .json()
            .unwrap();
        assert_eq!(output["state"], "complete");
        assert_eq!(output["seal"]["totalBytes"], "44");
        assert_eq!(output["seal"]["truncated"], false);
        assert_eq!(
            output["seal"]["payloadDigest"],
            "46c03665e76f71d4754d889debce48289f418cdcabc65fa26f41255500daa80f"
        );
    }
    demo.cleanup().await;
    api.shutdown().await;
    serve.stop().await;
}

fn output_query(operation: &Operation) -> CallOptions {
    CallOptions {
        params: BTreeMap::from([("id".into(), operation.session_id.clone())]),
        query: BTreeMap::from([
            ("contract".into(), "terminal-services-v1".into()),
            ("sessionContract".into(), "sdk1".into()),
            ("operationId".into(), operation.operation_id.clone()),
            ("requestDigest".into(), operation.digest.clone()),
        ]),
        ..Default::default()
    }
}

fn write(id: &str) -> WriteOptions {
    WriteOptions {
        idempotency_key: Some(id.into()),
        ..Default::default()
    }
}

async fn archive_turn(session: &Session, request_id: &str) {
    let floor = session.meta().await.unwrap().last_seq;
    let mut tracker = TurnTracker::new(floor).unwrap();
    let mut events = session
        .events(Some(&floor.to_string()), CancellationToken::new())
        .await
        .unwrap();
    session
        .send("RUST-DEMO-ARCHIVE", write(request_id))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let event = events.next().await.expect("archive stream EOF").unwrap();
            if let Some(outcome) = tracker.observe(&event) {
                assert_eq!(outcome.status, OutcomeStatus::Completed);
                break;
            }
        }
    })
    .await
    .expect("archive turn deadline");
    events.shutdown().await;
    wait_idle(session).await;
}

async fn open_store(path: &Path, identity: &Identity) -> FileStore {
    let expected = identity.clone();
    FileStore::open(StoreOptions {
        path: path.into(),
        key: [0x71; 32],
        identity: identity.clone(),
        limits: StoreLimits::default(),
        check_access: Arc::new(move |actual| {
            if actual == &expected {
                Ok(())
            } else {
                Err(tansr_sdk::Error::InvalidInput("test owner changed".into()))
            }
        }),
    })
    .await
    .unwrap()
}

async fn stored_coverage(path: &Path, identity: &Identity) -> Coverage {
    let store = open_store(path, identity).await;
    assert!(store.head().await.unwrap().is_some());
    assert!(store.pending().await.unwrap().is_none());
    let coverage = store
        .coverage()
        .await
        .unwrap()
        .expect("durable ACK coverage");
    store.close().await.unwrap();
    coverage
}

#[tokio::test]
#[ignore = "requires verified private Serve fixture with archive-manual mode"]
async fn real_archive_process_prepared_creation_and_original_receipt_query() {
    let serve = support::Serve::start("archive-manual").await;
    let files = Files::new(&serve);
    let api = serve.client("sdk1");
    let archive = ArchiveClient::new(api.clone());
    let session_id = serve.info["sessionId"].as_str().unwrap();
    let source_id = serve.info["sourceId"].as_str().unwrap();
    assert!(
        archive
            .binding_target(session_id)
            .await
            .unwrap()
            .binding_id
            .is_none()
    );
    let path = files.private.join("binding-intent.json");
    let mut command = files.command(ARCHIVE, &serve, "sdk1");
    command
        .args([
            "--mode",
            "prepare-create",
            "--session",
            session_id,
            "--source",
            source_id,
            "--request-id",
            "rust-demo-original-binding",
        ])
        .arg("--intent")
        .arg(&path);
    let output = Demo::start(&mut command).finish_with_status(true).await;
    assert!(output.contains("no binding was created"));
    assert!(
        archive
            .binding_target(session_id)
            .await
            .unwrap()
            .binding_id
            .is_none()
    );
    let original = std::fs::read(&path).unwrap();
    let request: tansr_sdk::archive::BindingCreateRequest =
        serde_json::from_slice(&original).unwrap();
    assert_eq!(request.request.request_id, "rust-demo-original-binding");
    assert!(!request.request.operation_epoch.is_empty());
    let mut command = files.command(ARCHIVE, &serve, "sdk1");
    command
        .args(["--mode", "create"])
        .arg("--intent")
        .arg(&path);
    let output = Demo::start(&mut command).finish_with_status(true).await;
    let binding_id = archive
        .binding_target(session_id)
        .await
        .unwrap()
        .binding_id
        .unwrap();
    assert!(output.contains(&format!("binding: {binding_id}")));
    let mut command = files.command(ARCHIVE, &serve, "sdk1");
    command
        .args(["--mode", "creation-status"])
        .arg("--intent")
        .arg(&path);
    let output = Demo::start(&mut command).finish_with_status(true).await;
    assert!(output.contains("creation state: completed"));
    assert!(output.contains(&binding_id));
    assert_eq!(std::fs::read(&path).unwrap(), original);
    api.shutdown().await;
    serve.stop().await;
}

#[tokio::test]
#[ignore = "requires TANSR_RUST_SERVE_FIXTURE verified private real Serve fixture"]
async fn real_archive_process_both_families_sync_materials_recovery_and_reopen() {
    for family in ["sdk1", "sdk2-offload-v1"] {
        let mut serve = support::Serve::start(if family == "sdk1" {
            "archive"
        } else {
            "archive-offload"
        })
        .await;
        let files = Files::new(&serve);
        let api = serve.client(family);
        let session = SessionClient::new(api.clone())
            .unwrap()
            .create(CreateOptions {
                request_id: (family == "sdk2-offload-v1")
                    .then(|| "rust-demo-archive-create".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        archive_turn(&session, "rust-demo-archive-first").await;
        let archive = ArchiveClient::new(api.clone());
        let binding_id = archive
            .binding_target(session.id())
            .await
            .unwrap()
            .binding_id
            .expect("fixture provisions a real Source and binding");
        let original_binding = archive.binding(&binding_id).await.unwrap();
        let original_page = archive.records(&original_binding, None).await.unwrap();
        let path = files.private.join("archive.bin");
        let mut previous = None;
        for _ in 0..2 {
            let mut command = files.command(ARCHIVE, &serve, family);
            command
                .args(["--mode", "sync", "--binding", &binding_id])
                .arg("--file")
                .arg(&path)
                .stdin(Stdio::null())
                .stdout(Stdio::piped());
            let output = tokio::time::timeout(Duration::from_secs(30), command.output())
                .await
                .expect("archive demo timeout")
                .unwrap();
            assert!(
                output.status.success(),
                "archive demo failed: {}",
                output.status
            );
            let stdout = String::from_utf8(output.stdout).unwrap();
            assert!(stdout.contains("complete=true"));
            assert!(stdout.contains("archive synchronized"));
            let identity = Identity::from_binding(
                &archive.binding(&binding_id).await.unwrap(),
                &archive.status(&binding_id).await.unwrap(),
            )
            .unwrap();
            let current = stored_coverage(&path, &identity).await;
            if let Some(expected) = previous {
                assert_eq!(current, expected, "reopening must preserve ACK coverage");
            }
            previous = Some(current);
        }
        let ciphertext = std::fs::read(&path).unwrap();
        assert!(
            !ciphertext
                .windows(b"RUST-DEMO-ARCHIVE".len())
                .any(|bytes| bytes == b"RUST-DEMO-ARCHIVE")
        );
        let record_ids: Vec<_> = original_page
            .records
            .iter()
            .map(|r| r.record_id.clone())
            .collect();
        let subject = serde_json::json!({"endUserId":original_binding.scope.end_user_id,"sessionId":session.id()});
        let response_path = files.private.join("material-response.json");
        let mut command = files.command(ARCHIVE, &serve, family);
        command
            .args([
                "--mode",
                "materials",
                "--binding",
                &binding_id,
                "--request-id",
                "rust-demo-material-response",
            ])
            .arg("--file")
            .arg(&path)
            .arg("--intent")
            .arg(&response_path);
        let mut materials = Demo::start(&mut command);
        materials
            .until("waiting for one live material request")
            .await;
        serve.send_control(&serde_json::json!({
            "requestId":"rust-demo-request-material", "command":"request-materials", "subject":subject,
            "request":{"materialRequestId":"rust-demo-recall","recordIds":record_ids,"purpose":"context-recall"}
        })).await;
        let output = materials.finish_with_status(true).await;
        assert!(output.contains("material state: received"));
        let original_response = std::fs::read(&response_path).unwrap();
        let response: tansr_sdk::archive::MaterialResponse =
            serde_json::from_slice(&original_response).unwrap();
        assert_eq!(response.request.request_id, "rust-demo-material-response");
        assert_eq!(response.material_request_id, "rust-demo-recall");

        let mut command = files.command(ARCHIVE, &serve, family);
        command
            .args(["--mode", "material-status"])
            .arg("--intent")
            .arg(&response_path);
        let output = Demo::start(&mut command).finish_with_status(false).await;
        assert!(
            output.contains("material state: received"),
            "received must not claim consumed"
        );
        serve.send_control(&serde_json::json!({
            "requestId":"rust-demo-enqueue-material", "command":"enqueue-materials", "subject":subject,
            "request":{"materialRequestId":"rust-demo-recall","leaseId":"rust-demo-material-lease"}
        })).await;
        archive_turn(&session, "rust-demo-material-consumer-turn").await;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let status = archive
                    .material_status(&binding_id, &response.material_request_id)
                    .await
                    .unwrap();
                if status.state == "core-consumed" {
                    break;
                }
                assert!(matches!(status.state.as_str(), "received" | "verified"));
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Serve did not confirm consuming the original material response");
        let mut command = files.command(ARCHIVE, &serve, family);
        command
            .args(["--mode", "material-status"])
            .arg("--intent")
            .arg(&response_path);
        let output = Demo::start(&mut command).finish_with_status(true).await;
        assert!(output.contains("material state: core-consumed"));
        assert_eq!(std::fs::read(&response_path).unwrap(), original_response);

        // Prepare a genuine durable ACK for the newly published page, then
        // advance the session to produce a confirmed stale binding revision.
        let binding = archive.binding(&binding_id).await.unwrap();
        let status = archive.status(&binding_id).await.unwrap();
        let identity = Identity::from_binding(&binding, &status).unwrap();
        let store = open_store(&path, &identity).await;
        let head = store.head().await.unwrap().unwrap();
        let page = archive
            .records(&binding, Some(&head.sequence))
            .await
            .unwrap();
        assert!(!page.records.is_empty());
        let mut bodies = BTreeMap::new();
        for record in &page.records {
            for reference in std::iter::once(&record.payload).chain(&record.attachments) {
                bodies.insert(
                    reference.artifact_id.clone(),
                    archive.artifact(&binding, reference).await.unwrap(),
                );
            }
        }
        let pending = store
            .receive(
                &binding,
                &status,
                &page,
                bodies,
                RequestIdentity {
                    request_id: "rust-demo-original-pending".into(),
                    operation_epoch: binding.operation_epoch.as_ref().unwrap().id.clone(),
                },
            )
            .await
            .unwrap();
        store.close().await.unwrap();
        archive_turn(&session, "rust-demo-stale-revision-turn").await;
        assert_ne!(
            archive.binding(&binding_id).await.unwrap().revision,
            pending.expected_revision
        );
        let mut command = files.command(ARCHIVE, &serve, family);
        command
            .args([
                "--mode",
                "recover",
                "--binding",
                &binding_id,
                "--request-id",
                "rust-demo-explicit-recovery",
            ])
            .arg("--file")
            .arg(&path);
        let output = Demo::start(&mut command).finish_with_status(true).await;
        assert!(output.contains("pending ACK confirmed"));
        assert!(!output.contains("archive synchronized"));
        assert_eq!(stored_coverage(&path, &identity).await, pending.coverage);
        let mut command = files.command(ARCHIVE, &serve, family);
        command
            .args(["--mode", "sync", "--binding", &binding_id])
            .arg("--file")
            .arg(&path);
        let output = Demo::start(&mut command).finish_with_status(true).await;
        assert!(output.contains("archive synchronized"));
        api.shutdown().await;
        serve.stop().await;
    }
}
