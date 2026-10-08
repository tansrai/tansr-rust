use async_trait::async_trait;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tansr_sdk::{ApiClient, CancellationToken, Result, executor::*};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Default)]
struct RemoteState {
    operation: Option<Operation>,
    substitute: Option<Operation>,
    receipt: Option<Receipt>,
    requests: Vec<String>,
    deny: bool,
    renewal_failure: bool,
    mutate_after_status: Option<(usize, u8)>,
    statuses: usize,
    stall_status: bool,
    output_failure: bool,
    output_seal: Option<Value>,
}
struct Remote {
    api: ApiClient,
    state: Arc<Mutex<RemoteState>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Remote {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Remote {
    fn new() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let listener = TcpListener::from_std(listener).unwrap();
        let state = Arc::new(Mutex::new(RemoteState::default()));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut bytes = Vec::new();
                let boundary = loop {
                    let mut part = [0; 4096];
                    let Ok(n) = socket.read(&mut part).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&part[..n]);
                    if let Some(i) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        break i + 4;
                    }
                    assert!(bytes.len() < 65536);
                };
                let headers = String::from_utf8(bytes[..boundary].to_vec()).unwrap();
                let route = headers.lines().next().unwrap().to_string();
                let len = headers
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|s| s.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                while bytes.len() < boundary + len {
                    let mut part = [0; 4096];
                    let n = socket.read(&mut part).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&part[..n]);
                }
                let (code, domain, response) = {
                    let mut s = shared.lock().unwrap();
                    s.requests.push(route.clone());
                    let terminal = route.contains("/terminal/");
                    let is_status = route.contains("/executions/") || terminal;
                    if is_status {
                        s.statuses += 1;
                    }
                    if s.deny || s.renewal_failure && route.contains("/heartbeats") {
                        (
                            403,
                            if terminal { "terminal" } else { "execution" },
                            json!({"error":{"code":"forbidden","message":"synthetic denial","retryAction":"do_not_retry"}}),
                        )
                    } else if route.contains("/tool-output-status")
                        || route.contains("/output-batches")
                    {
                        if route.contains("/output-batches") && s.output_failure {
                            (
                                503,
                                "terminal",
                                json!({"error":{"code":"source_unavailable","message":"synthetic output outage","retryAction":"do_not_retry"}}),
                            )
                        } else {
                            if route.contains("/output-batches") {
                                s.output_seal = Some(
                                    serde_json::from_slice::<Value>(
                                        &bytes[boundary..boundary + len],
                                    )
                                    .unwrap()["seal"]
                                        .clone(),
                                );
                            }
                            let op = s.operation.as_ref().unwrap();
                            (
                                200,
                                "terminal",
                                json!({"contract":"terminal-services-v1","operation":{"operationId":op.operation_id,"requestDigest":op.digest},"state":if s.output_seal.is_some(){"complete"}else{"available"},"acceptedThrough":null,"durableThrough":null,"retainedFrom":null,"nextByteOffset":"0","seal":s.output_seal}),
                            )
                        }
                    } else if route.contains("/heartbeats") {
                        (
                            200,
                            "execution",
                            serde_json::to_value(connection()).unwrap(),
                        )
                    } else if route.contains("/operations?") {
                        let ops = if s.receipt.is_none() {
                            s.operation.clone().into_iter().collect::<Vec<_>>()
                        } else {
                            Vec::new()
                        };
                        (
                            200,
                            "execution",
                            json!({"protocol":PROTOCOL,"executorId":"device","connectionId":"connection","operations":ops}),
                        )
                    } else {
                        if route.contains("/receipts") {
                            s.receipt = Some(
                                serde_json::from_slice(&bytes[boundary..boundary + len]).unwrap(),
                            );
                        }
                        let mut op = s
                            .substitute
                            .clone()
                            .or_else(|| s.operation.clone())
                            .expect("expected operation installed before IO");
                        if let Some((after, kind)) = s.mutate_after_status {
                            if s.statuses >= after {
                                match kind {
                                    0 => op.scope.end_user_id = "other-user".into(),
                                    1 => op.binding.binding_id = "other-binding".into(),
                                    _ => op.scope.authorization_revision = "2".into(),
                                };
                                op.digest = operation_digest(&op).unwrap();
                            }
                        }
                        let status = Status {
                            protocol: PROTOCOL.into(),
                            operation: op,
                            status: s
                                .receipt
                                .as_ref()
                                .map(|r| r.status.clone())
                                .unwrap_or("pending".into()),
                            receipt: s.receipt.clone(),
                        };
                        if terminal {
                            (
                                200,
                                "terminal",
                                json!({"contract":"terminal-services-v1","session":{"sessionContract":"sdk1","sessionId":"session"},"execution":status}),
                            )
                        } else {
                            (200, "execution", serde_json::to_value(status).unwrap())
                        }
                    }
                };
                let stall = {
                    let s = shared.lock().unwrap();
                    s.stall_status && s.statuses > 1 && route.contains("/executions/")
                };
                if stall {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
                let body = serde_json::to_vec(&response).unwrap();
                let head = format!(
                    "HTTP/1.1 {code} Test\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\ntansr-contract: unified-v1\r\ntansr-manifest-revision: 7\r\ntansr-schema-hash: none\r\ntansr-domain: {domain}\r\n\r\n",
                    body.len()
                );
                if socket.write_all(head.as_bytes()).await.is_ok() {
                    let _ = socket.write_all(&body).await;
                }
            }
        });
        Self {
            api: ApiClient::builder(base)
                .token("fixture")
                .session_family("sdk1")
                .build()
                .unwrap(),
            state,
            task,
        }
    }
}
struct Harness {
    runner: Runner,
    remote: Remote,
}
impl Harness {
    async fn execute(&self, op: Operation, cancel: CancellationToken) -> Result<Receipt> {
        self.remote.state.lock().unwrap().operation = Some(op.clone());
        self.runner.execute(op, cancel).await
    }
}

fn scope() -> Scope {
    Scope {
        application_scope_id: "app".into(),
        end_user_id: "user".into(),
        authorization_revision: "1".into(),
    }
}
fn deadline() -> String {
    (time::OffsetDateTime::now_utc() + time::Duration::minutes(2))
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}
fn connection() -> Connection {
    Connection {
        protocol: PROTOCOL.into(),
        executor_id: "device".into(),
        connection_id: "connection".into(),
        connection_revision: "1".into(),
        expires_at: deadline(),
        heartbeat_after_ms: 1000,
    }
}
fn registration() -> Registration {
    Registration {
        protocol: PROTOCOL.into(),
        executor_id: "device".into(),
        platform: Platform::current(),
        workspaces: vec![Workspace {
            workspace_id: "workspace".into(),
            revision: "1".into(),
        }],
        operations: vec!["tool.invoke".into()],
        tools: vec![ToolDefinition {
            name: "Lookup".into(),
            definition_digest: "a".repeat(64),
        }],
        interpreter: None,
    }
}
fn operation() -> Operation {
    let mut op = Operation {
        protocol: PROTOCOL.into(),
        operation_id: "op".into(),
        session_id: "session".into(),
        scope: scope(),
        binding: Binding {
            binding_id: "binding".into(),
            revision: "1".into(),
            target: Target {
                executor_id: "device".into(),
                connection_id: "connection".into(),
                connection_revision: "1".into(),
                workspace_id: "workspace".into(),
                workspace_revision: "1".into(),
                interpreter: None,
            },
        },
        tool_name: "Lookup".into(),
        request: Resource {
            operation: "tool.invoke".into(),
            args: json!({"name":"Lookup","definitionDigest":"a".repeat(64),"argsJson":"{\"value\":-1.5}"}),
        },
        digest: String::new(),
        expires_at: deadline(),
    };
    op.digest = operation_digest(&op).unwrap();
    op
}
fn api() -> ApiClient {
    ApiClient::builder("http://127.0.0.1:1")
        .token("fixture")
        .session_family("sdk1")
        .build()
        .unwrap()
}
struct Auth {
    calls: AtomicUsize,
    reject_at: usize,
}
#[async_trait]
impl Authorizer for Auth {
    async fn authorize(&self, op: &Operation) -> Result<()> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if op.session_id != "session" || op.binding.binding_id != "binding" || n >= self.reject_at {
            return Err(tansr_sdk::Error::InvalidInput(
                "local authorization denied".into(),
            ));
        }
        Ok(())
    }
}
struct Handler {
    calls: Arc<AtomicUsize>,
    mode: u8,
}
#[async_trait]
impl ToolHandler for Handler {
    async fn invoke(
        &self,
        context: ToolContext,
        args: Value,
    ) -> std::result::Result<Value, ToolError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(args["value"], json!(-1.5));
        match self.mode {
            0 => Ok(json!({"status":"ok","content":[{"t":"text","text":"client-only fact"}]})),
            1 => Err(ToolError::Unknown("effect could occur".into())),
            2 => Err(ToolError::Rejected("order_not_found".into())),
            3 => panic!("synthetic handler panic"),
            4 => Ok(json!({"status":"error","message":"order unavailable"})),
            5 => Ok(json!({"invalid":true})),
            _ => {
                context.cancellation.cancelled().await;
                Err(ToolError::Unknown("cancelled".into()))
            }
        }
    }
}
fn runner(
    journal: Arc<dyn Journal>,
    calls: Arc<AtomicUsize>,
    mode: u8,
    auth: Arc<dyn Authorizer>,
) -> Harness {
    let remote = Remote::new();
    let runner = Runner::with_connection(
        RunnerOptions {
            client: Client::new(remote.api.clone(), scope()).unwrap(),
            registration: registration(),
            journal,
            tools: BTreeMap::from([(
                "Lookup".into(),
                Tool::new("a".repeat(64), Handler { calls, mode }),
            )]),
            authorize: auth,
            poll_interval: Duration::from_millis(10),
            terminal: None,
            require_output: false,
            restricted_status: false,
        },
        connection(),
    )
    .unwrap();
    Harness { runner, remote }
}
fn auth() -> Arc<dyn Authorizer> {
    Arc::new(Auth {
        calls: AtomicUsize::new(0),
        reject_at: usize::MAX,
    })
}
fn journal(dir: &tempfile::TempDir) -> Arc<FileJournal> {
    Arc::new(FileJournal::open(dir.path().join("private")).unwrap())
}

#[test]
fn legacy_definition_digest_matches_node() {
    let decl = json!({"name":"DemoOrderStatus","description":"Read the status of sample order DEMO-001; this is demonstration data.","parameters":{"orderId":{"type":"string","description":"Sample order ID: DEMO-001"}},"readOnly":true});
    assert_eq!(
        definition_digest(&decl).unwrap(),
        "fb0ae6b3fd7dbfca35be5f54694fa5d70bf9587234d5d559188fefaa86da65fd"
    );
    let indices = json!({"name":"Indexes","description":"<>& 中文 \u{2028} \u{2029}","parameters":{"10":{"type":"string"},"2":{"type":"number"},"01":{"type":"boolean"}}});
    assert_eq!(
        definition_digest(&indices).unwrap(),
        "05536eedfce5c077e587e5a2e2fc48e77f075cea6c3cb8f3139095f1e7af6491"
    );
    let mut bad = decl.clone();
    bad["parameters"] = json!({"__proto__":{"type":"string"}});
    assert!(definition_digest(&bad).is_err());
    bad = decl.clone();
    bad["unknown"] = json!(true);
    assert!(definition_digest(&bad).is_err());
    bad = decl.clone();
    bad["description"] = json!("😀".repeat(1025));
    assert!(definition_digest(&bad).is_err());
    bad = decl.clone();
    bad["parameters"] = json!({"中文":{"type":"string"}});
    assert!(definition_digest(&bad).is_err());
    bad = decl.clone();
    bad["readOnly"] = json!(false);
    assert_ne!(
        definition_digest(&bad).unwrap(),
        definition_digest(&decl).unwrap()
    );
}
#[test]
fn ordinary_business_json_is_distinct_from_control() {
    for good in [
        r#"{"n":-1.25,"nested":{"名称":true}}"#,
        r#"{"n":1e3}"#,
        r#"{"s":"\ud83d\ude00"}"#,
    ] {
        assert!(parse_tool_arguments(good).is_ok(), "{good}");
    }
    for bad in [
        r#"{"s":"\ud800"}"#,
        r#"{"s":"\udc00"}"#,
        r#"{"x":1,"x":2}"#,
        "{} {}",
        "[]",
        r#"{"n":1e9999}"#,
    ] {
        assert!(parse_tool_arguments(bad).is_err(), "{bad}");
    }
}

#[test]
fn requiring_business_output_rejects_before_register_claim_or_handler() {
    let dir = tempfile::tempdir().unwrap();
    let store = journal(&dir);
    let count = Arc::new(AtomicUsize::new(0));
    let result = Runner::new(RunnerOptions {
        client: Client::new(api(), scope()).unwrap(),
        registration: registration(),
        journal: store,
        tools: BTreeMap::from([(
            "Lookup".into(),
            Tool::new(
                "a".repeat(64),
                Handler {
                    calls: count.clone(),
                    mode: 0,
                },
            ),
        )]),
        authorize: auth(),
        poll_interval: Duration::from_millis(10),
        terminal: None,
        require_output: true,
        restricted_status: false,
    });
    assert!(
        matches!(result,Err(tansr_sdk::Error::Contract(message)) if message.contains("negotiated terminal binding"))
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(
        std::fs::read_dir(dir.path().join("private"))
            .unwrap()
            .count(),
        0
    );
}

#[tokio::test]
async fn durable_replay_calls_handler_once_and_conflicting_digest_fails() {
    let dir = tempfile::tempdir().unwrap();
    let j = journal(&dir);
    let count = Arc::new(AtomicUsize::new(0));
    let r = runner(j.clone(), count.clone(), 0, auth());
    let op = operation();
    let first = r
        .execute(op.clone(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(first.status, "completed");
    assert_eq!(
        r.execute(op.clone(), CancellationToken::new())
            .await
            .unwrap(),
        first
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    drop(r);
    drop(j);
    let reopened = journal(&dir);
    let r = runner(reopened, count.clone(), 0, auth());
    assert_eq!(
        r.execute(op.clone(), CancellationToken::new())
            .await
            .unwrap(),
        first
    );
    let mut changed = op;
    changed.request.args["argsJson"] = json!("{\"value\":1}");
    changed.digest = operation_digest(&changed).unwrap();
    assert!(r.execute(changed, CancellationToken::new()).await.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn durable_pending_is_unknown_and_never_reexecuted() {
    let dir = tempfile::tempdir().unwrap();
    let j = journal(&dir);
    let op = operation();
    assert!(matches!(j.claim(&op).await.unwrap(), ClaimResult::Claimed));
    drop(j);
    let count = Arc::new(AtomicUsize::new(0));
    let r = runner(journal(&dir), count.clone(), 0, auth());
    let receipt = r.execute(op, CancellationToken::new()).await.unwrap();
    assert_eq!(receipt.status, "unknown");
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn result_classes_do_not_fabricate_certain_failures() {
    for (mode, expected) in [
        (1, "unknown"),
        (2, "failed"),
        (3, "unknown"),
        (4, "completed"),
        (5, "unknown"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let r = runner(journal(&dir), count.clone(), mode, auth());
        let receipt = r
            .execute(operation(), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(receipt.status, expected, "mode {mode}");
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn authorization_rechecked_after_claim_before_handler() {
    let dir = tempfile::tempdir().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let r = runner(
        journal(&dir),
        count.clone(),
        0,
        Arc::new(Auth {
            calls: AtomicUsize::new(0),
            reject_at: 2,
        }),
    );
    let receipt = r
        .execute(operation(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(receipt.status, "failed");
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn identity_digest_and_expired_lease_never_reach_handler() {
    let dir = tempfile::tempdir().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let r = runner(journal(&dir), count.clone(), 0, auth());
    for index in 0..9 {
        let mut op = operation();
        match index {
            0 => op.scope.application_scope_id = "other".into(),
            1 => op.scope.end_user_id = "other".into(),
            2 => op.scope.authorization_revision = "2".into(),
            3 => op.binding.target.connection_revision = "2".into(),
            4 => op.binding.target.workspace_revision = "2".into(),
            5 => op.binding.binding_id = "other".into(),
            6 => op.request.args["definitionDigest"] = json!("b".repeat(64)),
            7 => op.digest = "f".repeat(64),
            _ => op.expires_at = "2020-01-01T00:00:00Z".into(),
        }
        if index != 7 {
            op.digest = operation_digest(&op).unwrap();
        }
        assert!(
            r.execute(op, CancellationToken::new()).await.is_err(),
            "case {index}"
        );
    }
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(
        r.remote.state.lock().unwrap().requests.is_empty(),
        "local invalid inputs fail before IO or claim"
    );
}

#[tokio::test]
async fn forged_remote_status_and_denied_credentials_cannot_create_claims() {
    for kind in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let r = runner(journal(&dir), calls.clone(), 0, auth());
        let original = operation();
        let mut other = original.clone();
        match kind {
            0 => other.scope.end_user_id = "other".into(),
            1 => other.binding.binding_id = "other".into(),
            2 => other.scope.authorization_revision = "2".into(),
            3 => other.request.args["argsJson"] = json!("{\"value\":2}"),
            _ => r.remote.state.lock().unwrap().deny = true,
        }
        other.digest = operation_digest(&other).unwrap();
        r.remote.state.lock().unwrap().substitute = Some(other);
        assert!(
            r.execute(original, CancellationToken::new()).await.is_err(),
            "case {kind}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            std::fs::read_dir(dir.path().join("private"))
                .unwrap()
                .count(),
            0
        );
    }
}

#[tokio::test]
async fn restricted_status_denial_has_no_controller_fallback_and_zero_handler() {
    let remote = Remote::new();
    remote.state.lock().unwrap().deny = true;
    let dir = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let r = Runner::with_connection(
        RunnerOptions {
            client: Client::new(remote.api.clone(), scope()).unwrap(),
            registration: registration(),
            journal: journal(&dir),
            tools: BTreeMap::from([(
                "Lookup".into(),
                Tool::new(
                    "a".repeat(64),
                    Handler {
                        calls: calls.clone(),
                        mode: 0,
                    },
                ),
            )]),
            authorize: auth(),
            poll_interval: Duration::from_millis(10),
            restricted_status: true,
            require_output: false,
            terminal: Some(TerminalOptions {
                session_contract: "sdk1".into(),
                limits: OutputLimits {
                    max_control_bytes: 4096,
                    max_block_bytes: 16,
                    max_batch_bytes: 1024,
                    max_pending_bytes: 4096,
                    max_retained_bytes: 8192,
                },
            }),
        },
        connection(),
    )
    .unwrap();
    assert!(
        r.execute(operation(), CancellationToken::new())
            .await
            .is_err()
    );
    let requests = &remote.state.lock().unwrap().requests;
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("GET /api/terminal/executors/device/operations/op?"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        std::fs::read_dir(dir.path().join("private"))
            .unwrap()
            .count(),
        0
    );
}

#[tokio::test]
async fn failed_output_seal_preserves_business_fact_and_replay_never_reruns_handler() {
    for run_loop in [false, true] {
        let remote = Remote::new();
        let op = operation();
        {
            let mut state = remote.state.lock().unwrap();
            state.operation = Some(op.clone());
            state.output_failure = true;
        }
        let dir = tempfile::tempdir().unwrap();
        let journal = journal(&dir);
        let calls = Arc::new(AtomicUsize::new(0));
        let runner = Runner::with_connection(
            RunnerOptions {
                client: Client::new(remote.api.clone(), scope()).unwrap(),
                registration: registration(),
                journal: journal.clone(),
                tools: BTreeMap::from([(
                    "Lookup".into(),
                    Tool::new(
                        "a".repeat(64),
                        Handler {
                            calls: calls.clone(),
                            mode: 0,
                        },
                    ),
                )]),
                authorize: auth(),
                poll_interval: Duration::from_millis(10),
                restricted_status: false,
                require_output: true,
                terminal: Some(TerminalOptions {
                    session_contract: "sdk1".into(),
                    limits: OutputLimits {
                        max_control_bytes: 4096,
                        max_block_bytes: 16,
                        max_batch_bytes: 1024,
                        max_pending_bytes: 4096,
                        max_retained_bytes: 8192,
                    },
                }),
            },
            connection(),
        )
        .unwrap();
        if run_loop {
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(3), runner.run(CancellationToken::new()))
                    .await
                    .unwrap(),
                Err(tansr_sdk::Error::OutputIncomplete { .. })
            ));
            assert_eq!(
                remote
                    .state
                    .lock()
                    .unwrap()
                    .receipt
                    .as_ref()
                    .unwrap()
                    .status,
                "completed"
            );
        } else {
            let outcome = runner
                .execute_with_output(op.clone(), CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(outcome.receipt.status, "completed");
            assert!(!outcome.output_confirmed());
        }
        let ClaimResult::Receipt(receipt) = journal.claim(&op).await.unwrap() else {
            panic!("known fact must be durable");
        };
        assert_eq!(receipt.status, "completed");
        assert!(
            receipt.result.unwrap().args["resultJson"]
                .as_str()
                .unwrap()
                .contains("client-only fact")
        );
        assert!(matches!(
            runner.execute(op, CancellationToken::new()).await,
            Err(tansr_sdk::Error::OutputIncomplete { .. })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn in_flight_renewal_principal_binding_and_permission_changes_cancel_without_reexecution() {
    for kind in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let journal = journal(&dir);
        let remote = Remote::new();
        let op = operation();
        {
            let mut state = remote.state.lock().unwrap();
            state.operation = Some(op.clone());
            state.renewal_failure = kind == 0;
            if (1..=3).contains(&kind) {
                state.mutate_after_status = Some((2, kind - 1));
            }
        }
        let connection = connection();
        let runner = Runner::with_connection(
            RunnerOptions {
                client: Client::new(remote.api.clone(), scope()).unwrap(),
                registration: registration(),
                journal: journal.clone(),
                tools: BTreeMap::from([(
                    "Lookup".into(),
                    Tool::new(
                        "a".repeat(64),
                        Handler {
                            calls: calls.clone(),
                            mode: 6,
                        },
                    ),
                )]),
                authorize: if kind == 4 {
                    Arc::new(Auth {
                        calls: AtomicUsize::new(0),
                        reject_at: 3,
                    })
                } else {
                    auth()
                },
                poll_interval: Duration::from_millis(10),
                terminal: None,
                require_output: false,
                restricted_status: false,
            },
            connection,
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        let state = remote.state.clone();
        let stop = tokio::spawn(async move {
            loop {
                if state.lock().unwrap().receipt.is_some() {
                    trigger.cancel();
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        let result = tokio::time::timeout(Duration::from_secs(3), runner.run(cancel)).await;
        stop.abort();
        assert!(result.unwrap().is_err(), "case {kind}");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "case {kind}");
        let ClaimResult::Receipt(receipt) = journal.claim(&op).await.unwrap() else {
            panic!("missing durable receipt, case {kind}");
        };
        assert_eq!(receipt.status, "unknown");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn cancellation_preserves_durable_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let r = runner(journal(&dir), count.clone(), 6, auth());
    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    let task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        trigger.cancel();
    });
    let receipt = r.execute(operation(), cancel).await.unwrap();
    task.await.unwrap();
    assert_eq!(receipt.status, "unknown");
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelled_queued_execution_does_not_wait_for_another_handler() {
    let dir = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let runner = Arc::new(runner(journal(&dir), calls.clone(), 6, auth()));
    let first_cancel = CancellationToken::new();
    let first = runner.clone();
    let child = first_cancel.clone();
    let task = tokio::spawn(async move { first.execute(operation(), child).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let queued_cancel = CancellationToken::new();
    let queued = queued_cancel.clone();
    let trigger = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        queued.cancel();
    });
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_millis(500),
            runner.runner.execute(operation(), queued_cancel)
        )
        .await
        .unwrap(),
        Err(tansr_sdk::Error::Cancelled)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    first_cancel.cancel();
    assert_eq!(task.await.unwrap().unwrap().status, "unknown");
    trigger.await.unwrap();
}

#[tokio::test]
async fn direct_execute_cannot_borrow_another_runs_renewal() {
    let dir = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let remote = Remote::new();
    let op = operation();
    remote.state.lock().unwrap().operation = Some(op.clone());
    remote.state.lock().unwrap().stall_status = true;
    let mut c = connection();
    c.expires_at = (time::OffsetDateTime::now_utc() + time::Duration::milliseconds(150))
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
    let r = Runner::with_connection(
        RunnerOptions {
            client: Client::new(remote.api.clone(), scope()).unwrap(),
            registration: registration(),
            journal: journal(&dir),
            tools: BTreeMap::from([(
                "Lookup".into(),
                Tool::new(
                    "a".repeat(64),
                    Handler {
                        calls: calls.clone(),
                        mode: 6,
                    },
                ),
            )]),
            authorize: auth(),
            poll_interval: Duration::from_millis(10),
            terminal: None,
            require_output: false,
            restricted_status: false,
        },
        c,
    )
    .unwrap();
    let receipt = tokio::time::timeout(
        Duration::from_secs(2),
        r.execute(op, CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(receipt.status, "unknown");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn corrupted_claim_cannot_be_deleted_and_reexecuted() {
    let dir = tempfile::tempdir().unwrap();
    let j = journal(&dir);
    let op = operation();
    assert!(matches!(j.claim(&op).await.unwrap(), ClaimResult::Claimed));
    let claim = std::fs::read_dir(dir.path().join("private"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "claim"))
        .unwrap();
    std::fs::write(&claim, b"partial").unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let r = runner(j, count.clone(), 0, auth());
    assert!(r.execute(op, CancellationToken::new()).await.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read(claim).unwrap(), b"partial");
}

#[test]
fn journal_rejects_relative_and_symlink_directory() {
    assert!(FileJournal::open("relative-path").is_err());
    #[cfg(unix)]
    {
        let d = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(d.path(), d.path().join("linked")).unwrap();
        assert!(FileJournal::open(d.path().join("linked")).is_err());
    }
}

#[cfg(unix)]
#[tokio::test]
async fn renamed_journal_directory_cannot_redirect_claims_or_receipts() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let original = directory.path().join("private");
    let moved = directory.path().join("moved");
    let journal = FileJournal::open(&original).unwrap();
    let operation = operation();
    assert!(matches!(
        journal.claim(&operation).await.unwrap(),
        ClaimResult::Claimed
    ));
    std::fs::rename(&original, &moved).unwrap();
    std::fs::create_dir(&original).unwrap();
    std::fs::set_permissions(&original, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(journal.claim(&operation).await.is_err());
    let mut another = operation.clone();
    another.operation_id = "new-operation".into();
    another.digest = operation_digest(&another).unwrap();
    assert!(journal.claim(&another).await.is_err());
    let receipt = Receipt {
        protocol: PROTOCOL.into(),
        executor_id: operation.binding.target.executor_id.clone(),
        connection_id: operation.binding.target.connection_id.clone(),
        operation_id: operation.operation_id.clone(),
        digest: operation.digest.clone(),
        status: "unknown".into(),
        result: None,
        error_code: Some("execution_outcome_unknown".into()),
    };
    assert!(journal.complete(&operation, &receipt).await.is_err());
    assert_eq!(std::fs::read_dir(&original).unwrap().count(), 0);
    assert_eq!(std::fs::read_dir(&moved).unwrap().count(), 1);
}

#[test]
fn executor_child_claim() {
    let Some(file) = std::env::var_os("TANSR_RUST_EXECUTOR_CHILD_INPUT") else {
        return;
    };
    let file = std::path::PathBuf::from(file);
    let op: Operation = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
    let journal = FileJournal::open(file.parent().unwrap().join("private")).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(matches!(
        runtime.block_on(journal.claim(&op)).unwrap(),
        ClaimResult::Claimed
    ));
    // Abrupt process termination skips Rust destructors; Claim itself must have synced.
    std::process::exit(0);
}

#[tokio::test]
async fn subprocess_claim_survives_abrupt_exit_without_reexecution() {
    let dir = tempfile::tempdir().unwrap();
    let op = operation();
    let input = dir.path().join("operation.json");
    std::fs::write(&input, serde_json::to_vec(&op).unwrap()).unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "executor_child_claim", "--nocapture"])
        .env("TANSR_RUST_EXECUTOR_CHILD_INPUT", &input)
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "{}",
        String::from_utf8_lossy(&child.stderr)
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let runner = runner(journal(&dir), calls.clone(), 0, auth());
    assert_eq!(
        runner
            .execute(op, CancellationToken::new())
            .await
            .unwrap()
            .status,
        "unknown"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
