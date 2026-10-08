//! Real archive host, HTTP/SSE, local durable store and kernel material handoff.
//! Synthetic models are the only substituted upstream; no paid requests occur.
mod support;
use futures_util::StreamExt;
use serde_json::json;
use std::{collections::BTreeMap, io::Write, path::Path, sync::Arc, time::Duration};
use tansr_sdk::{
    CancellationToken, Error,
    archive::*,
    session::{CreateOptions, OutcomeStatus, Session, SessionClient, TurnTracker, WriteOptions},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn turn(session: &Session, request_id: &str) -> String {
    let mut tracker = TurnTracker::new(session.meta().await.unwrap().last_seq).unwrap();
    let mut stream = session
        .events(
            Some(&session.meta().await.unwrap().last_seq.to_string()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    session
        .send(
            "RUST-ARCHIVE synthetic retained material",
            WriteOptions {
                idempotency_key: Some(request_id.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut text = String::new();
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let event = stream
                .next()
                .await
                .expect("turn stream ended before terminal")
                .unwrap();
            if event.kind() == "msg.text.delta" {
                text.push_str(event.raw()["text"].as_str().unwrap_or(""));
            }
            if let Some(outcome) = tracker.observe(&event) {
                assert_eq!(outcome.status, OutcomeStatus::Completed);
                break;
            }
        }
    })
    .await
    .expect("archive turn timeout");
    stream.shutdown().await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let meta = session.meta().await.unwrap();
            if meta.status == "idle" {
                break;
            }
            assert_eq!(meta.status, "running");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("archive control finish-run did not settle");
    text
}
fn options(path: &Path, identity: &Identity) -> StoreOptions {
    let expected = identity.clone();
    StoreOptions {
        path: path.into(),
        key: [0x71; 32],
        identity: identity.clone(),
        limits: StoreLimits::default(),
        check_access: Arc::new(move |actual| {
            if actual != &expected
                || actual.application_scope_id != "go-app"
                || actual.end_user_id != "go-user"
            {
                return Err(Error::InvalidInput(
                    "synthetic archive principal mismatch".into(),
                ));
            }
            Ok(())
        }),
    }
}
async fn page_with_bodies(
    client: &ArchiveClient,
    binding: &Binding,
    after: Option<&str>,
) -> (Page, BTreeMap<String, Vec<u8>>) {
    let page = client.records(binding, after).await.unwrap();
    assert!(!page.records.is_empty(), "actual turn must publish records");
    let mut bodies = BTreeMap::new();
    for record in &page.records {
        for reference in std::iter::once(&record.payload).chain(&record.attachments) {
            if !bodies.contains_key(&reference.artifact_id) {
                bodies.insert(
                    reference.artifact_id.clone(),
                    client.artifact(binding, reference).await.unwrap(),
                );
            }
        }
    }
    (page, bodies)
}
fn persist_material_response(path: &Path, response: &MaterialResponse) {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).unwrap();
    file.write_all(&serde_json::to_vec(response).unwrap())
        .unwrap();
    file.sync_all().unwrap();
    drop(file);
    #[cfg(unix)]
    std::fs::File::open(path.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
}

/// Forward one real mutation, drain the real Serve success, then close the
/// downstream socket without any response bytes. No receipt is fabricated.
async fn lose_committed_response(
    base: &str,
) -> (String, tokio::task::JoinHandle<serde_json::Value>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let base = base.to_owned();
    let task = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(15), async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (header_end, length) = loop {
                let mut chunk = [0u8; 4096];
                let n = socket.read(&mut chunk).await.unwrap();
                assert_ne!(n, 0, "client closed before mutation headers");
                bytes.extend_from_slice(&chunk[..n]);
                assert!(bytes.len() <= 300_000);
                if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&bytes[..end]).unwrap();
                    let size = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .expect("SDK mutation must have bounded body length");
                    assert!(size <= 263_168);
                    break (end + 4, size);
                }
            };
            while bytes.len() < header_end + length {
                let mut chunk = [0u8; 4096];
                let n = socket.read(&mut chunk).await.unwrap();
                assert_ne!(n, 0, "client closed before mutation body");
                bytes.extend_from_slice(&chunk[..n]);
            }
            assert_eq!(bytes.len(), header_end + length);
            let header = std::str::from_utf8(&bytes[..header_end]).unwrap();
            let mut lines = header.lines();
            let mut start = lines.next().unwrap().split_whitespace();
            let method = reqwest::Method::from_bytes(start.next().unwrap().as_bytes()).unwrap();
            let path = start.next().unwrap();
            assert!(path.starts_with("/api/") && !path.contains("://"));
            let client = reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap();
            let mut request = client.request(method, format!("{base}{path}"));
            for line in lines.filter(|line| !line.is_empty()) {
                let (name, value) = line.split_once(':').unwrap();
                if !name.eq_ignore_ascii_case("host") && !name.eq_ignore_ascii_case("connection") {
                    request = request.header(name, value.trim());
                }
            }
            let original = serde_json::from_slice(&bytes[header_end..]).unwrap();
            let response = request
                .body(bytes[header_end..].to_vec())
                .send()
                .await
                .unwrap();
            let status = response.status();
            let receipt = response.bytes().await.unwrap();
            assert!(
                status.is_success(),
                "real Serve rejected dropped-response mutation: {status} {}",
                String::from_utf8_lossy(&receipt)
            );
            socket.shutdown().await.unwrap();
            original
        })
        .await
        .expect("response-loss forwarder timed out")
    });
    (endpoint, task)
}

async fn crash_worker(input_path: &str) -> ! {
    let input: serde_json::Value =
        serde_json::from_slice(&std::fs::read(input_path).unwrap()).unwrap();
    let api = tansr_sdk::ClientBuilder::new(input["endpoint"].as_str().unwrap())
        .session_family(input["family"].as_str().unwrap())
        .token("go-integration-token")
        .build()
        .unwrap();
    let client = ArchiveClient::new(api);
    if input["mode"] == "binding-create" {
        let intent: BindingCreateRequest =
            serde_json::from_slice(&std::fs::read(input["intent"].as_str().unwrap()).unwrap())
                .unwrap();
        assert!(matches!(
            client.create_binding(&intent).await,
            Err(Error::Transport(_))
        ));
        std::process::exit(72);
    }
    let identity: Identity = serde_json::from_value(input["identity"].clone()).unwrap();
    let store = FileStore::open(options(
        Path::new(input["path"].as_str().unwrap()),
        &identity,
    ))
    .await
    .unwrap();
    let result = if input["mode"] == "material" {
        let response_path = Path::new(input["path"].as_str().unwrap())
            .parent()
            .unwrap()
            .join("material-response.json");
        let response: MaterialResponse =
            serde_json::from_slice(&std::fs::read(response_path).unwrap()).unwrap();
        client.submit_materials(&response).await.map(|_| ())
    } else if input["mode"] == "ack" {
        client
            .acknowledge(&store.pending().await.unwrap().unwrap())
            .await
            .map(|_| ())
    } else {
        client
            .rebase_acknowledgement(&store.pending_rebase().await.unwrap().unwrap())
            .await
            .map(|_| ())
    };
    assert!(
        matches!(result, Err(Error::Transport(_))),
        "expected actual response EOF: {result:?}"
    );
    // Deliberately bypass FileStore::close and all destructors. The parent
    // reopens from disk after this real OS process and its lock are gone.
    std::process::exit(72)
}

async fn crash_after_lost_response(
    test_name: &str,
    mode: &str,
    family: &str,
    base: &str,
    path: &Path,
    identity: &Identity,
) -> serde_json::Value {
    let (endpoint, forwarder) = lose_committed_response(base).await;
    let input =
        json!({"endpoint":endpoint,"path":path,"identity":identity,"family":family,"mode":mode});
    let input_path = path.with_extension(format!("{mode}-child.json"));
    let mut open = std::fs::OpenOptions::new();
    open.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        open.mode(0o600);
    }
    let mut file = open.open(&input_path).unwrap();
    file.write_all(&serde_json::to_vec(&input).unwrap())
        .unwrap();
    file.sync_all().unwrap();
    drop(file);
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test_name, "--ignored", "--nocapture"])
        .env("TANSR_RUST_ARCHIVE_CRASH_INPUT", &input_path)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("archive child timeout")
        .unwrap();
    assert_eq!(
        status.code(),
        Some(72),
        "archive child did not stop at known unknown-result boundary"
    );
    forwarder.await.unwrap()
}

#[tokio::test]
#[ignore = "requires the verified private real Serve fixture; run xtask integration --require-serve"]
async fn real_serve_archive_both_families_original_ack_reopen_and_material_consumption() {
    if let Ok(input) = std::env::var("TANSR_RUST_ARCHIVE_CRASH_INPUT") {
        crash_worker(&input).await;
    }
    for family in ["sdk1", "sdk2-offload-v1"] {
        let mut serve = support::Serve::start(if family == "sdk1" {
            "archive"
        } else {
            "archive-offload"
        })
        .await;
        tokio::time::timeout(Duration::from_secs(60),async{
            let api=serve.client(family);let sessions=SessionClient::new(api.clone()).unwrap();
            let session=sessions.create(CreateOptions{request_id:(family=="sdk2-offload-v1").then(||"rust-archive-create".into()),..Default::default()}).await.unwrap();
            let client=ArchiveClient::new(api);
            let target=client.binding_target(session.id()).await.unwrap();
            let binding_id=target.binding_id.expect("host must install a real archive binding");
            assert!(turn(&session,"rust-archive-first-turn").await.contains("go-archive-answer"));
            let binding=client.binding(&binding_id).await.unwrap();let status=client.status(&binding_id).await.unwrap();
            let identity=Identity::from_binding(&binding,&status).unwrap();
            let directory=tempfile::tempdir().unwrap();let private=directory.path().canonicalize().unwrap().join("private");create_private_directory(&private).unwrap();let path=private.join("archive.bin");
            let store=FileStore::open(options(&path,&identity)).await.unwrap();
            let(page,bodies)=page_with_bodies(&client,&binding,None).await;
            let ack=store.receive(&binding,&status,&page,bodies.clone(),RequestIdentity{request_id:"rust-original-ack".into(),operation_epoch:binding.operation_epoch.as_ref().unwrap().id.clone()}).await.unwrap();
            assert!(store.coverage().await.unwrap().is_none());
            store.close().await.unwrap();
            let sent=crash_after_lost_response("real_serve_archive_both_families_original_ack_reopen_and_material_consumption","ack",family,serve.info["baseURL"].as_str().unwrap(),&path,&identity).await;
            assert_eq!(sent,serde_json::to_value(&ack).unwrap());
            let store=FileStore::open(options(&path,&identity)).await.unwrap();
            assert_eq!(store.pending().await.unwrap(),Some(ack.clone()));
            let recovered=sync_once(&client,&store,"must-not-replace-original").await.unwrap();
            assert!(recovered.recovered);assert_eq!(recovered.receipt.unwrap().request,ack.request);
            let coverage=store.coverage().await.unwrap().unwrap();assert_eq!(coverage,ack.coverage);
            assert_eq!(client.status(&binding_id).await.unwrap().acknowledged_coverage,Some(coverage.clone()));
            for record in &page.records{assert_eq!(store.body(&record.payload).await.unwrap(),bodies[&record.payload.artifact_id]);}
            assert!(bodies.values().any(|bytes|String::from_utf8_lossy(bytes).contains("go-archive-answer")));
            store.close().await.unwrap();
            let store=FileStore::open(options(&path,&identity)).await.unwrap();assert_eq!(store.coverage().await.unwrap(),Some(coverage));
            let resumed=sessions.resume(session.id(),WriteOptions::default()).await.unwrap();assert_eq!(resumed.id(),session.id());
            // Attaching a still-live runtime keeps its identity but is not a
            // reconstruction from persistent session storage.
            assert!(!resumed.created().resumed);assert!(resumed.created().last_seq>0);
            let current=client.binding(&binding_id).await.unwrap();let mut events=client.events(&current,None).await.unwrap();
            let record_ids:Vec<_>=page.records.iter().map(|r|r.record_id.clone()).collect();
            let subject=json!({"endUserId":"go-user","sessionId":session.id()});
            serve.send_control(&json!({"requestId":"rust-issue-material","command":"request-materials","subject":subject,"request":{"materialRequestId":"rust-recall-original","recordIds":record_ids,"purpose":"context-recall"}})).await;
            let material:MaterialRequest=tokio::time::timeout(Duration::from_secs(10),async{
                loop{
                    let event=events.next().await.expect("archive stream ended before requested material").unwrap();
                    if event.raw["eventType"]=="material.request"{break serde_json::from_value(event.raw["payload"].clone()).unwrap();}
                }
            }).await.expect("fixture did not emit real material request");
            assert_eq!(material.material_request_id,"rust-recall-original");assert_eq!(material.source_id,identity.source_id);assert_eq!(material.source_generation,identity.source_generation);
            let response=client.prepare_materials(&store,&material,RequestIdentity{request_id:"rust-original-material-response".into(),operation_epoch:current.operation_epoch.as_ref().unwrap().id.clone()}).await.unwrap();
            let response_path=private.join("material-response.json");persist_material_response(&response_path,&response);
            let exact:MaterialResponse=serde_json::from_slice(&std::fs::read(&response_path).unwrap()).unwrap();assert_eq!(exact,response);
            store.close().await.unwrap();
            let sent=crash_after_lost_response("real_serve_archive_both_families_original_ack_reopen_and_material_consumption","material",family,serve.info["baseURL"].as_str().unwrap(),&path,&identity).await;
            assert_eq!(sent,serde_json::to_value(&exact).unwrap());
            let store=FileStore::open(options(&path,&identity)).await.unwrap();
            assert_eq!(client.material_status(&binding_id,&material.material_request_id).await.unwrap().state,"received");
            let receipt=client.submit_materials(&exact).await.unwrap();assert_eq!(receipt.state,"received");
            serve.send_control(&json!({"requestId":"rust-enqueue-material","command":"enqueue-materials","subject":subject,"request":{"materialRequestId":material.material_request_id,"leaseId":"rust-material-consumer"}})).await;
            let text=turn(&resumed,"rust-archive-material-turn").await;assert!(!text.is_empty());
            tokio::time::timeout(Duration::from_secs(10),async{
                loop{let status=client.material_status(&binding_id,&material.material_request_id).await.unwrap();if status.state=="core-consumed"{break;}assert!(matches!(status.state.as_str(),"received"|"verified"),"unexpected material outcome {}",status.state);tokio::time::sleep(Duration::from_millis(10)).await;}
            }).await.expect("received material was never core-consumed");
            drop(events);
            let after=sync_once(&client,&store,"rust-after-material-ack").await.unwrap();assert!(after.records>0);assert!(after.receipt.is_some());
            store.close().await.unwrap();resumed.close(WriteOptions::default()).await.unwrap();
            eprintln!("RUST archive {family}: create/resume, verified local bytes, original ACK replay, received -> core-consumed");
        }).await.unwrap_or_else(|_|panic!("real {family} archive test timeout"));
        serve.stop().await;
    }
}

#[tokio::test]
#[ignore = "requires the verified private real Serve fixture; run xtask integration --require-serve"]
async fn real_serve_original_ack_completion_wins_a_prepared_rebase_without_double_coverage() {
    for family in ["sdk1", "sdk2-offload-v1"] {
        let serve = support::Serve::start(if family == "sdk1" {
            "archive"
        } else {
            "archive-offload"
        })
        .await;
        tokio::time::timeout(Duration::from_secs(35), async {
            let api = serve.client(family);
            let sessions = SessionClient::new(api.clone()).unwrap();
            let session = sessions
                .create(CreateOptions {
                    request_id: (family == "sdk2-offload-v1").then(|| "rust-race-create".into()),
                    ..Default::default()
                })
                .await
                .unwrap();
            turn(&session, "race-first").await;
            let client = ArchiveClient::new(api);
            let binding_id = client
                .binding_target(session.id())
                .await
                .unwrap()
                .binding_id
                .unwrap();
            let binding = client.binding(&binding_id).await.unwrap();
            let status = client.status(&binding_id).await.unwrap();
            let identity = Identity::from_binding(&binding, &status).unwrap();
            let temp = tempfile::tempdir().unwrap();
            let private = temp.path().canonicalize().unwrap().join("private");
            create_private_directory(&private).unwrap();
            let path = private.join("race.bin");
            let store = FileStore::open(options(&path, &identity)).await.unwrap();
            let (page, bodies) = page_with_bodies(&client, &binding, None).await;
            let original = store
                .receive(
                    &binding,
                    &status,
                    &page,
                    bodies,
                    RequestIdentity {
                        request_id: "race-original".into(),
                        operation_epoch: binding.operation_epoch.as_ref().unwrap().id.clone(),
                    },
                )
                .await
                .unwrap();
            // This models an operator-prepared durable recovery intent racing
            // a request already in flight; the original receipt is authoritative.
            let intent = store
                .prepare_rebase(RequestIdentity {
                    request_id: "race-recovery".into(),
                    operation_epoch: original.request.operation_epoch.clone(),
                })
                .await
                .unwrap();
            let committed = client.acknowledge(&original).await.unwrap();
            assert_eq!(committed.state, "completed");
            store.close().await.unwrap();
            let store = FileStore::open(options(&path, &identity)).await.unwrap();
            assert_eq!(store.pending_rebase().await.unwrap(), Some(intent));
            let recovered = recover_pending(&client, &store, "must-not-create-another")
                .await
                .unwrap();
            assert_eq!(recovered.receipt.unwrap(), committed);
            assert_eq!(store.coverage().await.unwrap(), Some(original.coverage));
            assert!(store.pending_rebase().await.unwrap().is_none());
            assert!(store.pending().await.unwrap().is_none());
            store.close().await.unwrap();
            session.close(WriteOptions::default()).await.unwrap();
        })
        .await
        .expect("real original/rebase race timeout");
        serve.stop().await;
    }
}

#[tokio::test]
#[ignore = "requires the verified private real Serve fixture; run xtask integration --require-serve"]
async fn real_serve_explicit_binding_first_response_loss_queries_original_creation_after_restart() {
    const NAME: &str =
        "real_serve_explicit_binding_first_response_loss_queries_original_creation_after_restart";
    if let Ok(input) = std::env::var("TANSR_RUST_ARCHIVE_CRASH_INPUT") {
        crash_worker(&input).await;
    }
    // Archive extension operations have the same frozen contract for both
    // client selectors. The fixture's pre-existing session is a real SDK1
    // driver; offload runtime persistence is covered by the two existing tests.
    for family in ["sdk1", "sdk2-offload-v1"] {
        let serve = support::Serve::start("archive-manual").await;
        tokio::time::timeout(Duration::from_secs(35),async {
            let client=ArchiveClient::new(serve.client(family));let session_id=serve.info["sessionId"].as_str().unwrap();let source_id=serve.info["sourceId"].as_str().unwrap();
            assert!(client.binding_target(session_id).await.unwrap().binding_id.is_none());
            let intent=client.prepare_create(session_id,source_id,"rust-first-binding").await.unwrap();
            let temp=tempfile::tempdir().unwrap();let private=temp.path().canonicalize().unwrap().join("private");create_private_directory(&private).unwrap();let intent_path=private.join("binding-intent.json");let original=serde_json::to_vec(&intent).unwrap();
            let mut open=std::fs::OpenOptions::new();open.write(true).create_new(true);
            #[cfg(unix)] {use std::os::unix::fs::OpenOptionsExt;open.mode(0o600);}
            let mut file=open.open(&intent_path).unwrap();file.write_all(&original).unwrap();file.sync_all().unwrap();drop(file);
            let(endpoint,forwarder)=lose_committed_response(serve.info["baseURL"].as_str().unwrap()).await;
            let input=json!({"endpoint":endpoint,"family":family,"mode":"binding-create","intent":intent_path});let input_path=private.join("child.json");let mut file=open.open(&input_path).unwrap();file.write_all(&serde_json::to_vec(&input).unwrap()).unwrap();file.sync_all().unwrap();drop(file);
            let mut child=tokio::process::Command::new(std::env::current_exe().unwrap()).args(["--exact",NAME,"--ignored","--nocapture"]).env("TANSR_RUST_ARCHIVE_CRASH_INPUT",&input_path).kill_on_drop(true).spawn().unwrap();let exit=tokio::time::timeout(Duration::from_secs(20),child.wait()).await.unwrap().unwrap();assert_eq!(exit.code(),Some(72));assert_eq!(forwarder.await.unwrap(),serde_json::to_value(&intent).unwrap());
            let recovered:BindingCreateRequest=serde_json::from_slice(&std::fs::read(&intent_path).unwrap()).unwrap();assert_eq!(std::fs::read(&intent_path).unwrap(),original);
            let receipt=client.creation_operation(session_id,&recovered.request).await.unwrap();assert_eq!(receipt.state,"completed");assert_eq!(receipt.request,recovered.request);
            let binding=client.binding(&receipt.binding_id).await.unwrap();assert_eq!(binding.source_id,source_id);assert_eq!(binding.target,recovered.target);assert_eq!(client.binding_target(session_id).await.unwrap().binding_id.as_deref(),Some(receipt.binding_id.as_str()));
            let status=client.status(&binding.binding_id).await.unwrap();Identity::from_binding(&binding,&status).unwrap();
            let replay=client.create_binding(&recovered).await.unwrap();assert_eq!(replay.binding_id,binding.binding_id);assert!(client.prepare_create(session_id,source_id,"must-not-create-second").await.is_err());
        }).await.expect("explicit original binding creation recovery timeout");
        serve.stop().await;
    }
}

#[tokio::test]
#[ignore = "requires the verified private real Serve fixture; run xtask integration --require-serve"]
async fn real_serve_archive_both_families_explicit_stale_rebase_preserves_original() {
    if let Ok(input) = std::env::var("TANSR_RUST_ARCHIVE_CRASH_INPUT") {
        crash_worker(&input).await;
    }
    for family in ["sdk1", "sdk2-offload-v1"] {
        let serve = support::Serve::start(if family == "sdk1" {
            "archive"
        } else {
            "archive-offload"
        })
        .await;
        tokio::time::timeout(Duration::from_secs(55),async{
            let api=serve.client(family);let sessions=SessionClient::new(api.clone()).unwrap();
            let session=sessions.create(CreateOptions{request_id:(family=="sdk2-offload-v1").then(||"rust-rebase-create".into()),..Default::default()}).await.unwrap();
            let client=ArchiveClient::new(api);turn(&session,"rust-rebase-first-turn").await;
            let binding_id=client.binding_target(session.id()).await.unwrap().binding_id.unwrap();
            let binding=client.binding(&binding_id).await.unwrap();let status=client.status(&binding_id).await.unwrap();let identity=Identity::from_binding(&binding,&status).unwrap();
            let temp=tempfile::tempdir().unwrap();let private=temp.path().canonicalize().unwrap().join("private");create_private_directory(&private).unwrap();let path=private.join("archive.bin");
            let store=FileStore::open(options(&path,&identity)).await.unwrap();let(page,bodies)=page_with_bodies(&client,&binding,None).await;
            let previous=store.receive(&binding,&status,&page,bodies.clone(),RequestIdentity{request_id:"rust-stale-original".into(),operation_epoch:binding.operation_epoch.as_ref().unwrap().id.clone()}).await.unwrap();
            turn(&session,"rust-rebase-second-turn").await;
            let changed=client.binding(&binding_id).await.unwrap();assert_ne!(changed.revision,previous.expected_revision);
            match sync_once(&client,&store,"must-not-auto-rebase").await{
                Err(Error::Api(e))=>{assert_eq!(e.code,"precondition_failed");assert_eq!(e.detail["reason"],"if_match_stale");},other=>panic!("expected definitive stale ACK, got {other:?}")
            }
            assert_eq!(store.pending().await.unwrap(),Some(previous.clone()));assert!(store.coverage().await.unwrap().is_none());
            // Persist the explicit recovery identity, then lose the actual
            // committed HTTP response and terminate its client process.
            let intent=store.prepare_rebase(RequestIdentity{request_id:"rust-fixed-recovery".into(),operation_epoch:previous.request.operation_epoch.clone()}).await.unwrap();
            store.close().await.unwrap();
            let sent=crash_after_lost_response("real_serve_archive_both_families_explicit_stale_rebase_preserves_original","rebase",family,serve.info["baseURL"].as_str().unwrap(),&path,&identity).await;
            assert_eq!(sent,serde_json::to_value(&intent).unwrap());
            let store=FileStore::open(options(&path,&identity)).await.unwrap();assert_eq!(store.pending_rebase().await.unwrap(),Some(intent.clone()));
            let recovered=recover_pending(&client,&store,"must-not-replace-recovery").await.unwrap();assert!(recovered.recovered);assert_eq!(recovered.receipt.unwrap().request,intent.request);
            assert_eq!(store.coverage().await.unwrap(),Some(previous.coverage.clone()));assert!(store.pending().await.unwrap().is_none());assert!(store.pending_rebase().await.unwrap().is_none());
            assert_eq!(client.status(&binding_id).await.unwrap().acknowledged_coverage,Some(previous.coverage));
            for record in &page.records{assert_eq!(store.body(&record.payload).await.unwrap(),bodies[&record.payload.artifact_id]);}
            let second=sync_once(&client,&store,"rust-after-rebase-ack").await.unwrap();assert!(second.records>0);store.close().await.unwrap();session.close(WriteOptions::default()).await.unwrap();
            eprintln!("RUST archive {family}: stale original retained, durable explicit rebase, reopen same recovery identity");
        }).await.unwrap_or_else(|_|panic!("real {family} rebase test timeout"));
        serve.stop().await;
    }
}
