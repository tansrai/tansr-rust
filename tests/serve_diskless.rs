//! Opt-in real Serve routing with synthetic backend/model. The private host is
//! supplied by the workspace; this public test never distributes its source.
use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tansr_sdk::{
    ApiClient, CancellationToken, Result,
    archive::ArchiveClient,
    executor::*,
    serve_persistence,
    session::{SessionClient, WriteOptions},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};

struct Host {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    serial: usize,
}
impl Host {
    async fn start() -> Self {
        let root = std::env::var("TANSR_DISKLESS_SERVE_ROOT")
            .expect("explicit real Serve source root required");
        let mut child = Command::new("node")
            .current_dir(root)
            .args([
                "--import",
                "./scripts/inject-globals.mjs",
                "--import",
                "tsx",
                "scripts/persistence/cross-client-host.ts",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        let mut host = Self {
            child,
            input,
            output,
            serial: 0,
        };
        assert_eq!(host.line().await["kind"], "ready");
        host
    }
    async fn line(&mut self) -> Value {
        let mut line = String::new();
        let count = tokio::time::timeout(Duration::from_secs(45), self.output.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        assert!(count > 0, "Serve host exited before reply");
        serde_json::from_str(&line).unwrap_or_else(|e| panic!("host JSON: {e}: {line}"))
    }
    async fn call(&mut self, mut body: Value) -> Value {
        self.serial += 1;
        let id = self.serial.to_string();
        body["id"] = json!(id);
        let mut bytes = serde_json::to_vec(&body).unwrap();
        bytes.push(b'\n');
        self.input.write_all(&bytes).await.unwrap();
        self.input.flush().await.unwrap();
        let reply = self.line().await;
        assert_eq!(reply["id"], id);
        assert_eq!(reply["ok"], true, "{reply}");
        reply["value"].clone()
    }
    async fn finish(mut self) {
        let state = self.call(json!({"command":"status"})).await;
        for mode in state["modes"].as_array().unwrap() {
            assert_eq!(mode["errors"], json!([]));
        }
        println!(
            "real-Serve final observations: {}",
            serde_json::to_string(&state).unwrap()
        );
        assert_eq!(self.call(json!({"command":"quit"})).await["closed"], true);
        self.input.shutdown().await.unwrap();
        drop(self.input);
        assert!(
            tokio::time::timeout(Duration::from_secs(30), self.child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }
}

struct GrantOnly {
    scope: Scope,
    session: String,
    executor: String,
}
#[async_trait]
impl Authorizer for GrantOnly {
    async fn authorize(&self, op: &Operation) -> Result<()> {
        if op.scope != self.scope
            || op.session_id != self.session
            || op.binding.target.executor_id != self.executor
            || op.tool_name != "BusinessLookup"
        {
            return Err(tansr_sdk::Error::Contract("synthetic scope denied".into()));
        }
        Ok(())
    }
}
struct Business(Arc<AtomicUsize>);
#[async_trait]
impl ToolHandler for Business {
    async fn invoke(
        &self,
        _context: ToolContext,
        args: Value,
    ) -> std::result::Result<Value, ToolError> {
        assert_eq!(args, json!({}));
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(json!({"status":"ok","content":[{"t":"text","text":"synthetic business fact"}]}))
    }
}
fn text<'a>(v: &'a Value, field: &str) -> &'a str {
    v[field].as_str().unwrap()
}
async fn idle(session: &tansr_sdk::session::Session) {
    for _ in 0..150 {
        if session.meta().await.unwrap().raw["status"] == "idle" {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("synthetic turn did not settle: {:?}", session.meta().await);
}

#[tokio::test]
#[ignore = "requires explicit shared real Serve source root; no production services"]
async fn real_serve_dual_modes_original_input_archive_and_cloud_runner() {
    let mut host = Host::start().await;
    for mode in ["cloud", "terminal"] {
        let seed = host
            .call(json!({"command":"seed","mode":mode,"client":"rust","toolMode":"business"}))
            .await;
        let api = ApiClient::builder(text(&seed, "baseUrl"))
            .token(text(&seed, "token"))
            .session_family("sdk1")
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        let session_id = text(&seed, "sessionId");
        let placement = serve_persistence::Client::new(api.clone());
        let found = placement.discover(session_id).await.unwrap();
        assert_eq!(found.mode, mode);
        assert!(!found.local_disk);
        assert_eq!(found.handoff.execution_journal, mode == "cloud");
        let archive = ArchiveClient::new(api.clone());
        let binding = archive.installed_binding(session_id).await.unwrap();
        assert_eq!(binding.availability, "source-managed");
        assert_eq!(binding.persistence.as_ref().unwrap().mode, mode);
        let status = archive.status(&binding.binding_id).await.unwrap();
        tansr_sdk::archive::Identity::from_binding(&binding, &status).unwrap();
        let mut events = archive.events(&binding, None).await.unwrap();
        let event = tokio::time::timeout(Duration::from_secs(5), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(event.raw["bindingId"], binding.binding_id);
        drop(events);

        let session = SessionClient::new(api.clone())
            .unwrap()
            .attach(session_id)
            .await
            .unwrap();
        let scope = Scope {
            application_scope_id: text(&seed, "applicationScopeId").into(),
            end_user_id: text(&seed, "user").into(),
            authorization_revision: text(&seed, "authorizationRevision").into(),
        };
        let client = Client::new(api.clone(), scope.clone()).unwrap();
        let platform = Platform::current();
        let initialized = client
            .initialize(
                session_id,
                &platform,
                Some(vec!["BusinessLookup".into()]),
                &session.capabilities().await.unwrap().closure_id,
            )
            .await
            .unwrap();
        let workspace = Workspace {
            workspace_id: "work".into(),
            revision: "1".into(),
        };
        let registration = Registration {
            protocol: PROTOCOL.into(),
            executor_id: text(&seed, "executorId").into(),
            platform,
            workspaces: vec![workspace.clone()],
            operations: vec!["tool.invoke".into()],
            tools: vec![ToolDefinition {
                name: "BusinessLookup".into(),
                definition_digest: text(&seed, "definitionDigest").into(),
            }],
            interpreter: None,
        };
        let connection = client.register(&registration).await.unwrap();
        client
            .bind(
                session_id,
                &connection,
                &workspace,
                &initialized.capability_revision,
                &session.capabilities().await.unwrap().closure_id,
            )
            .await
            .unwrap();
        let key = format!("rust-{mode}-original-message");
        // The product has accepted the request before the proxy loses its response.
        host.call(json!({"command":"drop-response","mode":mode,"client":"rust","method":"POST","path":format!("/api/sessions/{session_id}/messages")})).await;
        let lost = session
            .send_with_request_id("synthetic original message", &key, WriteOptions::default())
            .await
            .expect_err("dropped response must remain an error");
        assert!(
            matches!(lost, tansr_sdk::Error::Transport(_)),
            "unexpected pre-mutation error: {lost:?}"
        );
        let receipt = placement.input(session_id, &key).await.unwrap();
        if receipt.is_none() {
            panic!(
                "input receipt absent: {}",
                host.call(json!({"command":"status"})).await
            );
        }
        let receipt = receipt.unwrap();
        assert_eq!(receipt.request_id, key);
        assert_eq!(
            placement.input(session_id, &key).await.unwrap().unwrap(),
            receipt
        );
        let rows = placement.inputs(session_id, None, 10).await.unwrap();
        assert_eq!(
            rows.receipts.iter().filter(|r| r.request_id == key).count(),
            1
        );
        idle(&session).await;

        let temporary = tempfile::tempdir().unwrap();
        let journal_path = temporary.path().canonicalize().unwrap().join("journal");
        let cloud = CloudJournal::new(api.clone(), connection.executor_id.clone(), 8).unwrap();
        let journal: Arc<dyn Journal> = if mode == "cloud" {
            cloud.clone()
        } else {
            Arc::new(FileJournal::open_encrypted(&journal_path, [41; 32]).unwrap())
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let make_runner = |journal: Arc<dyn Journal>| {
            Runner::with_connection(
                RunnerOptions {
                    client: client.clone(),
                    registration: registration.clone(),
                    journal,
                    tools: BTreeMap::from([(
                        "BusinessLookup".into(),
                        Tool::new(
                            text(&seed, "definitionDigest").into(),
                            Business(calls.clone()),
                        ),
                    )]),
                    authorize: Arc::new(GrantOnly {
                        scope: scope.clone(),
                        session: session_id.into(),
                        executor: connection.executor_id.clone(),
                    }),
                    poll_interval: Duration::from_millis(20),
                    terminal: None,
                    require_output: false,
                    restricted_status: false,
                },
                connection.clone(),
            )
            .unwrap()
        };
        let runner = make_runner(journal.clone());
        let dispatched = host.call(json!({"command":"dispatch","mode":mode,"client":"rust","connectionId":connection.connection_id,"requestId":format!("rust-{mode}-business")})).await;
        let op: Operation = serde_json::from_value(dispatched["operation"].clone()).unwrap();
        assert_eq!(
            client.poll(&connection).await.unwrap().operations,
            vec![op.clone()]
        );
        let receipt = runner
            .execute(op.clone(), CancellationToken::new())
            .await
            .unwrap_or_else(|error| panic!("{mode} execution failed: {error}"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(receipt.status, "completed");
        client.submit(&op, &receipt).await.unwrap();
        idle(&session).await;
        drop(runner);
        drop(journal);
        let restored: Arc<dyn Journal> = if mode == "cloud" {
            CloudJournal::new(api.clone(), connection.executor_id.clone(), 8).unwrap()
        } else {
            Arc::new(FileJournal::reopen_encrypted(&journal_path, [41; 32]).unwrap())
        };
        let restored_runner = make_runner(restored);
        assert_eq!(
            restored_runner
                .execute(op.clone(), CancellationToken::new())
                .await
                .unwrap(),
            receipt
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        if mode == "cloud" {
            assert!(
                !journal_path.exists(),
                "cloud must not create local journal"
            );
            assert_eq!(
                cloud
                    .read(&connection.connection_id, &op.operation_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .receipt,
                Some(receipt)
            );
            assert_eq!(
                cloud
                    .list(&connection.connection_id, Some(session_id), None, 10)
                    .await
                    .unwrap()
                    .0
                    .len(),
                1
            );
        } else {
            assert!(
                cloud.claim(&op).await.is_err(),
                "terminal may not silently switch to cloud authority"
            );
        }
        api.shutdown().await;
    }
    host.finish().await;
}
