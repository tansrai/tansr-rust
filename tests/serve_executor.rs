mod support;
use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tansr_sdk::{ApiClient, CallOptions, CancellationToken, Result, executor::*, session::*};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
};

struct ReceiptProxy {
    base: String,
    lost: Arc<AtomicUsize>,
    reconciled: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for ReceiptProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn receipt_proxy(target: String) -> ReceiptProxy {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let lost = Arc::new(AtomicUsize::new(0));
    let reconciled = Arc::new(AtomicUsize::new(0));
    let loss = lost.clone();
    let queries = reconciled.clone();
    let task = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                connection=listener.accept()=>{
                    let (mut socket,_)=connection.unwrap();
                    let target=target.clone();let loss=loss.clone();let queries=queries.clone();
                    tasks.spawn(async move {
                        let mut bytes=Vec::new();
                        let boundary=loop {let mut part=[0;4096];let n=socket.read(&mut part).await.unwrap();if n==0{return;}bytes.extend_from_slice(&part[..n]);if let Some(i)=bytes.windows(4).position(|v|v==b"\r\n\r\n"){break i+4;}assert!(bytes.len()<65536);};
                        let headers=String::from_utf8(bytes[..boundary].to_vec()).unwrap();
                        let mut lines=headers.lines();let mut line=lines.next().unwrap().split_whitespace();
                        let method=line.next().unwrap();let path=line.next().unwrap();
                        let mut request=reqwest::Client::new().request(reqwest::Method::from_bytes(method.as_bytes()).unwrap(),format!("{target}{path}"));
                        let mut length=0;
                        for line in lines {if let Some((name,value))=line.split_once(':') {
                            if name.eq_ignore_ascii_case("content-length"){length=value.trim().parse().unwrap();}
                            else if !name.eq_ignore_ascii_case("host") && !name.eq_ignore_ascii_case("connection") {request=request.header(name,value.trim());}
                        }}
                        while bytes.len()<boundary+length {let mut part=[0;4096];let n=socket.read(&mut part).await.unwrap();if n==0{return;}bytes.extend_from_slice(&part[..n]);}
                        let response=request.body(bytes[boundary..boundary+length].to_vec()).send().await.unwrap();
                        let status=response.status().as_u16();let response_headers=response.headers().clone();let body=response.bytes().await.unwrap();
                        if method=="POST" && path.contains("/receipts") && status==200 && loss.fetch_add(1,Ordering::SeqCst)==0 {return;}
                        if method=="GET" && path.contains("/executions/") && loss.load(Ordering::SeqCst)>0 {queries.fetch_add(1,Ordering::SeqCst);}
                        let mut head=format!("HTTP/1.1 {status} Forwarded\r\ncontent-length: {}\r\nconnection: close\r\n",body.len());
                        for (name,value) in &response_headers {if name.as_str().starts_with("tansr-") || name==reqwest::header::CONTENT_TYPE {head.push_str(&format!("{name}: {}\r\n",value.to_str().unwrap()));}}
                        head.push_str("\r\n");let _=socket.write_all(head.as_bytes()).await;let _=socket.write_all(&body).await;
                    });
                },
                Some(done)=tasks.join_next(),if !tasks.is_empty()=>{done.unwrap();}
            }
        }
    });
    ReceiptProxy {
        base,
        lost,
        reconciled,
        task,
    }
}

struct Host {
    session: String,
    scope: Scope,
}
#[async_trait]
impl Authorizer for Host {
    async fn authorize(&self, op: &Operation) -> Result<()> {
        if op.session_id != self.session
            || op.scope != self.scope
            || op.binding.target.workspace_id != "rust-business-workspace"
        {
            return Err(tansr_sdk::Error::InvalidInput(
                "local permission denied".into(),
            ));
        }
        Ok(())
    }
}
struct Business {
    executions: Arc<AtomicUsize>,
    output_observed: Option<Arc<Notify>>,
    handler_finished: Arc<AtomicUsize>,
}
#[async_trait]
impl ToolHandler for Business {
    async fn invoke(
        &self,
        context: ToolContext,
        _args: Value,
    ) -> std::result::Result<Value, ToolError> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        if let Some(observed) = &self.output_observed {
            let writer = context.output.as_ref().expect("negotiated handler output");
            assert_eq!(
                writer.capture("stdout", b"rust-first-chunk").await.unwrap(),
                16
            );
            observed.notified().await;
            assert_eq!(
                writer
                    .capture("stderr", b"rust-second-chunk")
                    .await
                    .unwrap(),
                17
            );
        }
        self.handler_finished.store(1, Ordering::SeqCst);
        Ok(json!({"status":"ok","content":[{"t":"text","text":"go-terminal-fact"}]}))
    }
}

/// Real Serve dispatcher/model fixture, not a Rust HTTP response mock. Paid models are not used.
#[tokio::test]
#[ignore = "requires the verified private real Serve fixture"]
async fn real_serve_first_binding_dispatch_receipt_and_durable_replay() {
    exercise_real_executor(false, false, 0).await;
}

#[tokio::test]
#[ignore = "requires the verified real Serve fixture with ordinary business output support"]
async fn real_serve_output_first_chunk_before_handler_completion_and_exact_seal() {
    exercise_real_executor(true, false, 0).await;
}

#[tokio::test]
#[ignore = "requires the verified private real Serve fixture"]
async fn real_serve_receipt_loss_reconciles_original_fact_without_handler_reexecution() {
    exercise_real_executor(false, true, 0).await;
}

#[tokio::test]
#[ignore = "requires verified real Serve with policy and auth test controls"]
async fn real_serve_revoked_tool_authority_never_invokes_pending_business_handler() {
    exercise_real_executor(false, false, 1).await;
}
#[tokio::test]
#[ignore = "requires verified real Serve with two synthetic principals"]
async fn real_serve_other_principal_restricted_status_cannot_execute_or_elevate() {
    exercise_real_executor(true, false, 2).await;
}
#[tokio::test]
#[ignore = "requires verified real Serve with auth test control"]
async fn real_serve_revoked_login_token_cannot_execute_pending_business_handler() {
    exercise_real_executor(false, false, 3).await;
}

struct PermissionHarness {
    session: Session,
    executor: Client,
    connection: Connection,
    runner: Arc<Runner>,
    executions: Arc<AtomicUsize>,
    events: SessionEventStream,
    _journal: tempfile::TempDir,
}

async fn permission_harness(fixture: &support::Serve) -> PermissionHarness {
    let api = fixture.client("sdk1");
    let session = SessionClient::new(api.clone())
        .unwrap()
        .create(CreateOptions {
            client_tools: Some(vec![fixture.info["declaration"].clone()]),
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
    let executor = Client::new(api, scope.clone()).unwrap();
    let platform = Platform::current();
    let workspace = Workspace {
        workspace_id: "rust-business-workspace".into(),
        revision: "1".into(),
    };
    let definition = fixture.info["definitionDigest"]
        .as_str()
        .unwrap()
        .to_owned();
    let registration = Registration {
        protocol: PROTOCOL.into(),
        executor_id: "go-executor".into(),
        platform: platform.clone(),
        workspaces: vec![workspace.clone()],
        operations: vec!["tool.invoke".into()],
        tools: vec![ToolDefinition {
            name: "BusinessLookup".into(),
            definition_digest: definition.clone(),
        }],
        interpreter: None,
    };
    let connection = executor.register(&registration).await.unwrap();
    let initialized = executor
        .initialize(
            session.id(),
            &platform,
            Some(vec!["BusinessLookup".into()]),
            &session.capabilities().await.unwrap().closure_id,
        )
        .await
        .unwrap();
    executor
        .bind(
            session.id(),
            &connection,
            &workspace,
            &initialized.capability_revision,
            &session.capabilities().await.unwrap().closure_id,
        )
        .await
        .unwrap();
    let journal = tempfile::tempdir().unwrap();
    let executions = Arc::new(AtomicUsize::new(0));
    let runner = Arc::new(
        Runner::with_connection(
            RunnerOptions {
                client: executor.clone(),
                registration,
                journal: Arc::new(FileJournal::open(journal.path().join("private")).unwrap()),
                tools: BTreeMap::from([(
                    "BusinessLookup".into(),
                    Tool::new(
                        definition,
                        Business {
                            executions: executions.clone(),
                            output_observed: None,
                            handler_finished: Arc::new(AtomicUsize::new(0)),
                        },
                    ),
                )]),
                authorize: Arc::new(Host {
                    session: session.id().into(),
                    scope,
                }),
                poll_interval: Duration::from_millis(20),
                terminal: None,
                require_output: false,
                restricted_status: false,
            },
            connection.clone(),
        )
        .unwrap(),
    );
    let events = session
        .events(
            Some(&session.created().last_seq.to_string()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    PermissionHarness {
        session,
        executor,
        connection,
        runner,
        executions,
        events,
        _journal: journal,
    }
}

struct PermissionWorker {
    cancel: CancellationToken,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
}
impl PermissionWorker {
    fn start(runner: Arc<Runner>) -> Self {
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let task = tokio::spawn(async move { runner.run(token).await });
        Self {
            cancel,
            task: Some(task),
        }
    }
    async fn stop(mut self) -> Result<()> {
        self.cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
    }
}
impl Drop for PermissionWorker {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn next_permission(events: &mut SessionEventStream) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(event) = events.next().await {
            let event = event.unwrap();
            assert_ne!(
                event.kind(),
                "server.question.request",
                "AskUser is not a tool permission test"
            );
            if event.kind() == "tool.permission.decided" {
                assert_ne!(event.raw()["decision"], "allow");
            }
            if event.kind() == "server.permission.request" {
                assert_eq!(event.raw()["name"], "BusinessLookup");
                return event.raw().clone();
            }
            assert!(
                event.turn_outcome().is_none(),
                "turn ended before the real tool permission gate"
            );
        }
        panic!("EOF before permission request")
    })
    .await
    .unwrap()
}

fn permission_rejected<T: std::fmt::Debug>(result: Result<T>, allowed: &[u16]) {
    match result {
        Err(tansr_sdk::Error::Api(error)) => assert!(
            allowed.contains(&error.status),
            "unexpected permission rejection: {error:?}"
        ),
        Err(tansr_sdk::Error::Domain { status, .. }) => assert!(
            allowed.contains(&status),
            "unexpected permission domain status: {status}"
        ),
        other => panic!("expected a real HTTP permission rejection, got {other:?}"),
    }
}

async fn no_permission_execution(harness: &PermissionHarness) {
    assert!(
        harness
            .executor
            .poll(&harness.connection)
            .await
            .unwrap()
            .operations
            .is_empty(),
        "invalid permission must not admit an executor operation"
    );
    assert_eq!(harness.executions.load(Ordering::SeqCst), 0);
    assert_eq!(
        std::fs::read_dir(harness._journal.path().join("private"))
            .unwrap()
            .count(),
        0,
        "invalid permission must not create a durable execution claim"
    );
}

/// RUST-A14: real tool permission tickets; question answers are a separate gate.
#[tokio::test]
#[ignore = "requires verified real Serve with permission timeout, two principals and auth control"]
async fn real_serve_permission_wrong_digest_expired_ticket_subject_and_token_never_approve() {
    let mut fixture = support::Serve::start("execution").await;
    tokio::time::timeout(Duration::from_secs(40), async {
        let mut expired = permission_harness(&fixture).await;
        let worker = PermissionWorker::start(expired.runner.clone());
        expired.session.send("GO-TOOL", WriteOptions { idempotency_key: Some("rust-permission-expiry-turn".into()), ..Default::default() }).await.unwrap();
        let old_ticket = next_permission(&mut expired.events).await;
        assert_eq!(old_ticket["ttlMs"], fixture.info["permissionTimeoutMs"]);
        let old_id = old_ticket["requestId"].as_str().unwrap();
        let old_digest = old_ticket["digest"].as_str().unwrap();
        tokio::time::timeout(Duration::from_secs(6), async {
            while let Some(event) = expired.events.next().await {
                let event = event.unwrap();
                if event.kind() == "tool.permission.decided" { assert_ne!(event.raw()["decision"], "allow"); }
                if event.kind() == "server.permission.closed" && event.raw()["requestId"] == old_id { return; }
            }
            panic!("expired permission did not close");
        }).await.unwrap();
        permission_rejected(expired.session.permission(old_id, old_digest, "allow", WriteOptions { idempotency_key: Some("rust-permission-expired-answer".into()), ..Default::default() }).await, &[409,410]);
        no_permission_execution(&expired).await;
        assert!(matches!(worker.stop().await, Err(tansr_sdk::Error::Cancelled)));
        expired.session.close(Default::default()).await.unwrap();

        let mut current = permission_harness(&fixture).await;
        let worker = PermissionWorker::start(current.runner.clone());
        current.session.send("GO-TOOL", WriteOptions { idempotency_key: Some("rust-permission-current-turn".into()), ..Default::default() }).await.unwrap();
        let ticket = next_permission(&mut current.events).await;
        let id = ticket["requestId"].as_str().unwrap();
        let digest = ticket["digest"].as_str().unwrap();
        assert_ne!(id, old_id);
        let other = ApiClient::builder(fixture.info["baseURL"].as_str().unwrap()).token(fixture.info["otherToken"].as_str().unwrap()).session_family("sdk1").build().unwrap();
        let wrong_digest = "0".repeat(64);
        assert_ne!(wrong_digest, digest);
        let (wrong, old, foreign) = tokio::join!(
            current.session.permission(id, &wrong_digest, "allow", WriteOptions { idempotency_key: Some("rust-permission-wrong-digest".into()), ..Default::default() }),
            current.session.permission(old_id, old_digest, "allow", WriteOptions { idempotency_key: Some("rust-permission-old-ticket".into()), ..Default::default() }),
            other.call("session.permission.decide", CallOptions { params: BTreeMap::from([("id".into(), current.session.id().into()), ("ticketId".into(), id.into())]), body: Some(json!({"digest":digest,"verdict":"allow"})), idempotency_key: Some("rust-permission-other-subject".into()), ..Default::default() })
        );
        permission_rejected(wrong, &[409]);
        permission_rejected(old, &[404,409,410]);
        permission_rejected(foreign, &[403,404]);
        no_permission_execution(&current).await;

        fixture.send_control(&json!({"command":"set-auth","requestId":"rust-permission-revoke-login","allowed":false})).await;
        permission_rejected(current.session.permission(id, digest, "allow", WriteOptions { idempotency_key: Some("rust-permission-revoked-token".into()), ..Default::default() }).await, &[401,403]);
        assert_eq!(current.executions.load(Ordering::SeqCst), 0);
        let stopped = worker.stop().await;
        if !matches!(stopped, Err(tansr_sdk::Error::Cancelled)) { permission_rejected(stopped, &[401,403]); }
        fixture.send_control(&json!({"command":"set-auth","requestId":"rust-permission-restore-login","allowed":true})).await;
        no_permission_execution(&current).await;
        let worker = PermissionWorker::start(current.runner.clone());
        let accepted = current.session.permission(id, digest, "allow", WriteOptions { idempotency_key: Some("rust-permission-valid-current".into()), ..Default::default() }).await.unwrap();
        assert!(accepted.accepted);
        let mut approvals = 0;
        tokio::time::timeout(Duration::from_secs(8), async {
            while let Some(event) = current.events.next().await {
                let event = event.unwrap();
                if event.kind() == "tool.permission.decided" && event.raw()["decision"] == "allow" { approvals += 1; }
                if event.kind() == "server.permission.request" { panic!("the current valid permission did not release its exact call"); }
                if let Some(outcome) = event.turn_outcome() { assert_eq!(outcome.status, OutcomeStatus::Completed); return; }
            }
            panic!("EOF before approved tool completed");
        }).await.unwrap();
        assert_eq!(approvals, 1, "only the final current ticket may be recorded as approval");
        assert_eq!(current.executions.load(Ordering::SeqCst), 1);
        assert!(matches!(worker.stop().await, Err(tansr_sdk::Error::Cancelled)));
        assert!(current.session.history(0,20).await.unwrap().to_string().contains("go-tool-complete"));
        current.session.close(Default::default()).await.unwrap();
    }).await.expect("permission negative matrix timed out");
    fixture.stop().await;
}

async fn exercise_real_executor(stream_output: bool, lost_receipt: bool, negative: u8) {
    let mut f = support::Serve::start("execution").await;
    tokio::time::timeout(Duration::from_secs(45), async {
        let api = f.client("sdk1");
        let s = SessionClient::new(api.clone())
            .unwrap()
            .create(CreateOptions {
                client_tools: Some(vec![f.info["declaration"].clone()]),
                ..Default::default()
            })
            .await
            .unwrap();
        let scope = Scope {
            application_scope_id: f.info["applicationScopeId"].as_str().unwrap().into(),
            end_user_id: f.info["endUserId"].as_str().unwrap().into(),
            authorization_revision: f.info["authorizationRevision"].as_str().unwrap().into(),
        };
        let x = Client::new(api, scope.clone()).unwrap();
        let proxy=if lost_receipt {Some(receipt_proxy(f.info["baseURL"].as_str().unwrap().trim_end_matches('/').to_owned()).await)}else{None};
        let runner_client=if negative==2 {Client::new(ApiClient::builder(f.info["baseURL"].as_str().unwrap()).token(f.info["otherToken"].as_str().unwrap()).session_family("sdk1").build().unwrap(),scope.clone()).unwrap()}
        else if let Some(p)=&proxy {Client::new(ApiClient::builder(&p.base).token(f.info["token"].as_str().unwrap()).session_family("sdk1").build().unwrap(),scope.clone()).unwrap()}else{x.clone()};
        let platform = Platform::current();
        let workspace = Workspace {
            workspace_id: "rust-business-workspace".into(),
            revision: "1".into(),
        };
        let definition = f.info["definitionDigest"].as_str().unwrap().to_string();
        let dir = tempfile::tempdir().unwrap();
        let executions = Arc::new(AtomicUsize::new(0));
        let registration = Registration {
            protocol: PROTOCOL.into(),
            executor_id: "go-executor".into(),
            platform: platform.clone(),
            workspaces: vec![workspace.clone()],
            operations: vec!["tool.invoke".into()],
            tools: vec![ToolDefinition {
                name: "BusinessLookup".into(),
                definition_digest: definition.clone(),
            }],
            interpreter: None,
        };
        let connection = x.register(&registration).await.unwrap();
        let closure = s.capabilities().await.unwrap();
        let cap = x
            .initialize(
                s.id(),
                &platform,
                Some(vec!["BusinessLookup".into()]),
                &closure.closure_id,
            )
            .await
            .unwrap();
        let closure = s.capabilities().await.unwrap();
        let bound = x
            .bind(
                s.id(),
                &connection,
                &workspace,
                &cap.capability_revision,
                &closure.closure_id,
            )
            .await
            .unwrap();
        assert!(bound.binding.is_some());
        let terminal = if stream_output {
            Some(
                x.negotiate_output(
                    &TerminalSessionReference {
                        session_contract: "sdk1".into(),
                        session_id: s.id().into(),
                    },
                    bound.binding.as_ref().unwrap(),
                    "rust-output-binding",
                )
                .await
                .unwrap(),
            )
        } else {
            None
        };
        let output_observed = stream_output.then(|| Arc::new(Notify::new()));
        let handler_finished = Arc::new(AtomicUsize::new(0));
        let runner = Runner::with_connection(
            RunnerOptions {
                client: runner_client,
                registration,
                journal: Arc::new(FileJournal::open(dir.path().join("private")).unwrap()),
                tools: BTreeMap::from([(
                    "BusinessLookup".into(),
                    Tool::new(
                        definition,
                        Business {
                            executions: executions.clone(),
                            output_observed: output_observed.clone(),
                            handler_finished: handler_finished.clone(),
                        },
                    ),
                )]),
                authorize: Arc::new(Host {
                    session: s.id().into(),
                    scope,
                }),
                poll_interval: Duration::from_millis(20),
                terminal,
                require_output: stream_output,
                restricted_status: stream_output,
            },
            connection.clone(),
        )
        .unwrap();
        let mut events = s
            .events(
                Some(&s.created().last_seq.to_string()),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let event_session = s.clone();
        let observer = tokio::spawn(async move {
            while let Some(event) = events.next().await {
                let e = event.unwrap();
                if e.kind() == "server.permission.request" {
                    event_session
                        .permission(
                            e.raw()["requestId"].as_str().unwrap(),
                            e.raw()["digest"].as_str().unwrap(),
                            "allow",
                            WriteOptions {
                                idempotency_key: Some("rust-executor-approval".into()),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();
                }
                if let Some(outcome) = e.turn_outcome() {
                    assert_eq!(outcome.status, OutcomeStatus::Completed);
                    return;
                }
            }
            panic!("EOF before completed turn");
        });
        s.send(
            "GO-TOOL",
            WriteOptions {
                idempotency_key: Some("rust-executor-message".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let op = loop {
            let mut b = x.poll(&connection).await.unwrap();
            assert!(b.operations.len() <= 1);
            if let Some(op) = b.operations.pop() {
                break op;
            }
            assert!(!observer.is_finished(), "turn ended without dispatch");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(op.tool_name, "BusinessLookup");
        if negative!=0 {
            if negative==1 {f.send_control(&json!({"command":"set-policy","requestId":"rust-executor-revoke-policy","allowed":false,"authorizationRevision":"2"})).await;}
            if negative==3 {f.send_control(&json!({"command":"set-auth","requestId":"rust-executor-revoke-auth","allowed":false})).await;}
            assert!(runner.execute(op,CancellationToken::new()).await.is_err());
            assert_eq!(executions.load(Ordering::SeqCst),0,"revoked or other principal must not enter the handler");
            assert_eq!(std::fs::read_dir(dir.path().join("private")).unwrap().count(),0,"remote authority must be checked before claim");
            observer.abort();let _=observer.await;
            return;
        }
        let output_monitor = output_observed.map(|observed| {
            let api = x.api().clone();
            let operation = op.clone();
            let done = handler_finished.clone();
            tokio::spawn(async move {
                loop {
                    let response = api
                        .call("terminal.output.status", output_query(&operation))
                        .await
                        .unwrap()
                        .json()
                        .unwrap();
                    if response["acceptedThrough"].is_string() {
                        assert_eq!(
                            done.load(Ordering::SeqCst),
                            0,
                            "Serve first ACK must precede handler completion"
                        );
                        assert_eq!(response["nextByteOffset"], "16");
                        assert_eq!(response["state"], "receiving");
                        observed.notify_one();
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
        });
        let receipt = if let Some(proxy)=&proxy {
            let runner=Arc::new(runner);
            let run=runner.clone();let cancel=CancellationToken::new();let run_cancel=cancel.clone();
            let task=tokio::spawn(async move{run.run(run_cancel).await});
            while proxy.reconciled.load(Ordering::SeqCst)==0 {assert!(!task.is_finished(),"runner stopped before real receipt reconciliation");tokio::time::sleep(Duration::from_millis(10)).await;}
            // Wait for reconciliation to be consumed, rather than racing cancellation
            // against the HTTP read of the original accepted receipt.
            tokio::time::sleep(Duration::from_millis(30)).await;
            cancel.cancel();assert!(matches!(task.await.unwrap(),Err(tansr_sdk::Error::Cancelled)));
            assert_eq!(proxy.lost.load(Ordering::SeqCst),1,"accepted receipt must not be blindly resubmitted");
            let status=x.status(&op.session_id,&op.operation_id).await.unwrap();
            assert_eq!(status.status,"completed");
            let receipt=status.receipt.unwrap();
            assert_eq!(runner.execute(op.clone(),CancellationToken::new()).await.unwrap(),receipt);
            receipt
        } else { let receipt=runner
            .execute(op.clone(), CancellationToken::new())
            .await
            .unwrap();
            assert_eq!(runner.execute(op.clone(),CancellationToken::new()).await.unwrap(),receipt);
            receipt
        };
        assert_eq!(receipt.status, "completed");
        if let Some(monitor) = output_monitor {
            monitor.await.unwrap();
            let status = x
                .api()
                .call("terminal.output.status", output_query(&op))
                .await
                .unwrap()
                .json()
                .unwrap();
            assert_eq!(status["state"], "complete");
            assert_eq!(status["seal"]["totalBytes"], "33");
            use sha2::{Digest, Sha256};
            assert_eq!(
                status["seal"]["payloadDigest"],
                format!("{:x}", Sha256::digest(b"rust-first-chunkrust-second-chunk"))
            );
        }
        // The lost-response branch also replays after Serve has the final fact.
        assert_eq!(executions.load(Ordering::SeqCst), 1);
        for _ in 0..2 {
            assert_eq!(x.submit(&op, &receipt).await.unwrap().status, "completed");
        }
        observer.await.unwrap();
        assert!(
            s.history(0, 20)
                .await
                .unwrap()
                .to_string()
                .contains("go-tool-complete")
        );
        s.close(Default::default()).await.unwrap();
    })
    .await
    .expect("executor real Serve test timed out");
    f.stop().await;
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
