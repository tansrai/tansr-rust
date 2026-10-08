//! Public-client protocol fault injection, separate from real Serve acceptance.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use futures_util::StreamExt;
use serde_json::{Map, Value, json};
use tansr_sdk::{
    api::{ApiClient, Error},
    session::*,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug)]
struct Request {
    method: String,
    target: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

struct Reply {
    status: u16,
    domain: &'static str,
    content_type: &'static str,
    body: Vec<u8>,
    headers: Vec<(&'static str, String)>,
}

impl Reply {
    fn json(status: u16, domain: &'static str, body: Value) -> Self {
        Self {
            status,
            domain,
            content_type: "application/json",
            body: serde_json::to_vec(&body).unwrap(),
            headers: vec![],
        }
    }
    fn closure(state: &str) -> Self {
        let schema: Value =
            serde_json::from_slice(include_bytes!("../contract/unified-v1.schema.json")).unwrap();
        let defs = &schema["definitions"]["CapabilityClosure"]["properties"];
        let mut operations = Map::new();
        for name in defs["operations"]["required"].as_array().unwrap() {
            operations.insert(name.as_str().unwrap().into(), json!(state));
        }
        let mut domains = Map::new();
        for name in defs["domains"]["required"].as_array().unwrap() {
            domains.insert(
                name.as_str().unwrap().into(),
                json!({"installed":true,"revision":null}),
            );
        }
        let id = "a".repeat(64);
        let mut reply = Self::json(
            200,
            "discovery",
            json!({"contract":"unified-v1", "closureId":id,
            "authorizationRevision":null,"domains":domains,"operations":operations}),
        );
        reply.headers.push(("tansr-closure-id", id));
        reply
    }
}

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<Request>>>,
    task: JoinHandle<()>,
}
impl Server {
    async fn start(
        handler: impl Fn(&Request, usize) -> Option<Reply> + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let handler = Arc::new(handler);
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let captured = captured.clone();
                let handler = handler.clone();
                tokio::spawn(async move {
                    let mut bytes = Vec::new();
                    let mut chunk = [0; 8192];
                    let header_end = loop {
                        let size = socket.read(&mut chunk).await.unwrap_or(0);
                        if size == 0 {
                            return;
                        }
                        bytes.extend_from_slice(&chunk[..size]);
                        if let Some(i) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                            break i + 4;
                        }
                        if bytes.len() > 64 * 1024 {
                            return;
                        }
                    };
                    let text = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
                    let mut lines = text.lines();
                    let mut first = lines.next().unwrap().split_whitespace();
                    let method = first.next().unwrap().into();
                    let target = first.next().unwrap().into();
                    let headers: BTreeMap<String, String> = lines
                        .filter_map(|line| line.split_once(':'))
                        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().into()))
                        .collect();
                    let length: usize = headers
                        .get("content-length")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                    while bytes.len() < header_end + length {
                        let size = socket.read(&mut chunk).await.unwrap_or(0);
                        if size == 0 {
                            return;
                        }
                        bytes.extend_from_slice(&chunk[..size]);
                    }
                    let request = Request {
                        method,
                        target,
                        headers,
                        body: bytes[header_end..header_end + length].to_vec(),
                    };
                    let index = {
                        let mut all = captured.lock().unwrap();
                        all.push(request.clone());
                        all.len() - 1
                    };
                    if let Some(reply) = handler(&request, index) {
                        let mut head = format!(
                            "HTTP/1.1 {} OK\r\nConnection: close\r\nContent-Length: {}\r\nContent-Type: {}\r\ntansr-contract: unified-v1\r\ntansr-manifest-revision: 7\r\ntansr-domain: {}\r\ntansr-schema-hash: none\r\n",
                            reply.status,
                            reply.body.len(),
                            reply.content_type,
                            reply.domain
                        );
                        for (key, value) in reply.headers {
                            head.push_str(&format!("{key}: {value}\r\n"));
                        }
                        head.push_str("\r\n");
                        if socket.write_all(head.as_bytes()).await.is_ok() {
                            let _ = socket.write_all(&reply.body).await;
                        }
                    }
                });
            }
        });
        Self {
            url,
            requests,
            task,
        }
    }
    fn client(&self, family: &str) -> SessionClient {
        SessionClient::new(
            ApiClient::builder(&self.url)
                .token("fixture")
                .session_family(family)
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap(),
        )
        .unwrap()
    }
    fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn discovery() -> Reply {
    Reply::json(
        200,
        "session",
        json!({"protocol":"sdk2-ext-v1","contracts":[
        {"contract":"sdk1","availability":"legacy-complete"},
        {"contract":"sdk2-offload-v1","availability":"source-required"}]}),
    )
}
fn meta(family: &str) -> Reply {
    let mut raw =
        json!({"sessionId":"s1","live":true,"lastSeq":0,"status":"idle","title":"retained"});
    if family == "sdk2-offload-v1" {
        raw["contract"] = json!(family);
        raw["availability"] = json!("source-required");
    }
    Reply::json(200, "session", raw)
}
fn write() -> WriteOptions {
    WriteOptions {
        idempotency_key: Some("retained-write".into()),
        ..WriteOptions::default()
    }
}
fn receipt() -> Value {
    json!({"sessionId":"s1","inputId":"input-1","turnId":"turn-1","historyEpoch":"epoch-1",
        "durability":"memory","ordinal":1,"revision":1,"source":"strict","state":"accepted"})
}

#[tokio::test]
async fn explicit_families_create_201_resume_200_and_invalid_offload_never_writes() {
    let server = Server::start(|request, _| {
        if request.target.starts_with("/api/capabilities/sessions") {
            return Some(discovery());
        }
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        let resumed = body.get("resume").is_some();
        let mut raw = json!({"sessionId":"s1","lastSeq":0,"resumed":false});
        if request.headers["tansr-session-family"] == "sdk2-offload-v1" {
            raw["contract"] = json!("sdk2-offload-v1");
            raw["availability"] = json!("source-required");
        }
        Some(Reply::json(if resumed { 200 } else { 201 }, "session", raw))
    })
    .await;
    let sdk1 = server.client("sdk1");
    let first = sdk1.create(CreateOptions::default()).await.unwrap();
    assert!(!first.created().resumed);
    assert!(!sdk1.resume("s1", write()).await.unwrap().created().resumed);
    let offload = server.client("sdk2-offload-v1");
    let before = server.requests().len();
    assert!(offload.create(CreateOptions::default()).await.is_err());
    assert_eq!(server.requests().len(), before);
    let remote = offload
        .create(CreateOptions {
            request_id: Some("stable_request".into()),
            ..CreateOptions::default()
        })
        .await
        .unwrap();
    assert_eq!(remote.id(), "s1");
    assert!(offload.resume("s1", write()).await.is_ok());
    let requests = server.requests();
    assert_eq!(requests.iter().filter(|r| r.method == "POST").count(), 4);
    assert!(
        requests
            .iter()
            .filter(|r| r.method == "GET")
            .all(|r| r.target.contains("protocol=sdk2-ext-v1"))
    );
}

#[tokio::test]
async fn malformed_or_foreign_create_is_not_success_or_automatic_recreation() {
    for body in [
        json!({"resumed":false,"lastSeq":0}),
        json!({"sessionId":"s1","resumed":false}),
        json!({"sessionId":"foreign","resumed":true,"lastSeq":0}),
    ] {
        let server = Server::start(move |r, _| {
            Some(if r.method == "GET" {
                discovery()
            } else {
                Reply::json(200, "session", body.clone())
            })
        })
        .await;
        assert!(server.client("sdk1").resume("s1", write()).await.is_err());
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|r| r.method == "POST")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn refreshes_closure_for_each_write_and_never_replays_unknown_side_effects() {
    let server = Server::start(|r, index| {
        if r.target == "/api/sessions/s1" {
            return Some(meta("sdk1"));
        }
        if r.target.ends_with("/capabilities") {
            return Some(Reply::closure(if index == 1 {
                "disabled"
            } else {
                "enabled"
            }));
        }
        None // Request reached the server, response was lost.
    })
    .await;
    let session = server.client("sdk1").attach("s1").await.unwrap();
    assert!(matches!(
        session.send("one", write()).await,
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        session.send("two", write()).await,
        Err(Error::Transport(_))
    ));
    let requests = server.requests();
    let writes: Vec<_> = requests.iter().filter(|r| r.method == "POST").collect();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].headers["idempotency-key"], "retained-write");
    assert_eq!(writes[0].headers["tansr-closure-id"], "a".repeat(64));
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.target.ends_with("/capabilities"))
            .count(),
        2
    );
}

#[tokio::test]
async fn dormant_close_has_no_live_closure_and_history_zero_is_count_only() {
    let server = Server::start(|r, _| {
        if r.target == "/api/sessions/s1" && r.method == "GET" {
            return Some(Reply::json(
                200,
                "session",
                json!({"sessionId":"s1","status":"ended","live":false,"lastSeq":7}),
            ));
        }
        if r.target.starts_with("/api/sessions/s1/history") {
            return Some(Reply::json(
                200,
                "session",
                json!({"messages":[],"total":7}),
            ));
        }
        Some(Reply::json(
            202,
            "session",
            json!({"sessionId":"s1","accepted":true}),
        ))
    })
    .await;
    let session = server.client("sdk1").attach("s1").await.unwrap();
    assert_eq!(session.history(0, 0).await.unwrap()["total"], 7);
    assert!(session.close(write()).await.unwrap().accepted);
    let requests = server.requests();
    assert!(requests[1].target.contains("limit=0"));
    assert_eq!(requests.len(), 3);
    assert!(!requests[2].headers.contains_key("tansr-closure-id"));
}

#[tokio::test]
async fn checkpoint_import_keeps_original_bytes_and_compaction_preserves_failure() {
    let original = b"{\n  \"synthetic\":\"raw bytes\"\n}\n";
    let server = Server::start(move |r, _| {
        if r.target == "/api/sessions/s1" {
            return Some(meta("sdk1"));
        }
        if r.target.ends_with("/capabilities") {
            return Some(Reply::closure("enabled"));
        }
        if r.target.ends_with("/export") {
            return Some(Reply {
                status: 200,
                domain: "session",
                content_type: "application/octet-stream",
                body: original.to_vec(),
                headers: vec![],
            });
        }
        if r.target.contains("/import") {
            return Some(Reply::json(
                201,
                "session",
                json!({"checkpointId":"c2","sessionId":"s1","messageCount":5}),
            ));
        }
        if r.target.ends_with("/compact") {
            return Some(Reply::json(
                200,
                "session",
                json!({"status":"failed","reason":"synthetic","checkpointId":"before"}),
            ));
        }
        Some(Reply::json(
            200,
            "session",
            json!({"status":"restored","checkpointId":"other","fromMessages":5,"toMessages":2}),
        ))
    })
    .await;
    let session = server.client("sdk1").attach("s1").await.unwrap();
    let data = session.export_checkpoint("c1").await.unwrap();
    assert_eq!(data, original);
    assert_eq!(
        session
            .import_checkpoint(data, "本地快照", write())
            .await
            .unwrap()
            .checkpoint_id,
        "c2"
    );
    let raw = session
        .compact(CompactOptions::default(), write())
        .await
        .unwrap();
    assert_eq!(raw["status"], "failed");
    assert_eq!(raw["checkpointId"], "before");
    assert!(session.restore("c1", true, write()).await.is_err());
    let requests = server.requests();
    let import = requests
        .iter()
        .find(|r| r.target.contains("/import"))
        .unwrap();
    assert_eq!(import.body, original);
    assert_eq!(import.headers["content-type"], "application/octet-stream");
    assert!(
        import
            .target
            .contains("label=%E6%9C%AC%E5%9C%B0%E5%BF%AB%E7%85%A7")
    );
}

#[tokio::test]
async fn permission_question_and_input_keep_identity_and_durable_never_downgrades() {
    let server = Server::start(|r, _| {
        if r.target == "/api/sessions/s1" {
            return Some(meta("sdk1"));
        }
        if r.target.ends_with("/input-capabilities") {
            return Some(Reply::json(200, "session", json!({"durableAck":false})));
        }
        if r.target.ends_with("/capabilities") {
            return Some(Reply::closure("enabled"));
        }
        if r.target.ends_with("/inputs") {
            return Some(Reply::json(
                202,
                "session",
                json!({"outcome":"accepted","receipt":receipt()}),
            ));
        }
        if r.target.contains("/inputs/") {
            return Some(Reply::json(200, "session", json!({"receipt":receipt()})));
        }
        Some(Reply::json(200, "session", json!({"accepted":true})))
    })
    .await;
    let session = server.client("sdk1").attach("s1").await.unwrap();
    session
        .permission("permission-1", "original-digest", "deny", write())
        .await
        .unwrap();
    session
        .answer(
            "question-1",
            vec![Answer {
                question_id: "q1".into(),
                selected_option_ids: vec![],
                free_text: Some("manual answer".into()),
            }],
            write(),
        )
        .await
        .unwrap();
    let input = Input {
        input_id: "input-1".into(),
        target: InputTarget {
            history_epoch: "epoch-1".into(),
            turn_id: "turn-1".into(),
        },
        content: InputContent {
            text: Some("additional instruction".into()),
            blocks: None,
        },
        ack: Some("memory".into()),
    };
    assert_eq!(
        session.submit_input(input.clone(), write()).await.unwrap()["receipt"]["state"],
        "accepted"
    );
    assert_eq!(
        session
            .input_status("input-1", input.target.clone())
            .await
            .unwrap()["receipt"]["inputId"],
        "input-1"
    );
    let mut durable = input;
    durable.ack = Some("durable".into());
    assert!(matches!(
        session.submit_input(durable, write()).await,
        Err(Error::InvalidInput(_))
    ));
    let requests = server.requests();
    let writes: Vec<_> = requests.iter().filter(|r| r.method == "POST").collect();
    assert_eq!(writes.len(), 3);
    let permission: Value = serde_json::from_slice(&writes[0].body).unwrap();
    assert_eq!(permission["digest"], "original-digest");
    assert_eq!(permission["verdict"], "deny");
    let answers: Value = serde_json::from_slice(&writes[1].body).unwrap();
    assert_eq!(answers["answers"][0]["selectedOptionIds"], json!([]));
    let submitted: Value = serde_json::from_slice(&writes[2].body).unwrap();
    assert_eq!(submitted["target"]["turnId"], "turn-1");
    assert_eq!(submitted["ack"], "memory");
}

#[tokio::test]
async fn foreign_input_receipt_is_never_accepted() {
    let server = Server::start(|r, _| {
        if r.target == "/api/sessions/s1" {
            return Some(meta("sdk1"));
        }
        if r.target.ends_with("/capabilities") {
            return Some(Reply::closure("enabled"));
        }
        let mut received = receipt();
        received["turnId"] = json!("foreign");
        Some(Reply::json(
            202,
            "session",
            json!({"outcome":"accepted","receipt":received}),
        ))
    })
    .await;
    let session = server.client("sdk1").attach("s1").await.unwrap();
    let input = Input {
        input_id: "input-1".into(),
        target: InputTarget {
            history_epoch: "epoch-1".into(),
            turn_id: "turn-1".into(),
        },
        content: InputContent {
            text: Some("hello".into()),
            blocks: None,
        },
        ack: None,
    };
    assert!(matches!(
        session.submit_input(input, write()).await,
        Err(Error::Contract(_))
    ));
}

#[tokio::test]
async fn invalid_user_inputs_fail_locally_without_write_or_discovery() {
    let server = Server::start(|_, _| Some(meta("sdk1"))).await;
    let session = server.client("sdk1").attach("s1").await.unwrap();
    assert!(session.send("  ", write()).await.is_err());
    assert!(
        session
            .send_blocks(
                vec![Block::Image {
                    mime: "audio/wav".into(),
                    data: "abc".into()
                }],
                write()
            )
            .await
            .is_err()
    );
    assert!(
        session
            .permission("ticket", "", "allow", write())
            .await
            .is_err()
    );
    assert!(session.answer("ticket", vec![], write()).await.is_err());
    assert!(
        session
            .transcribe(TranscriptionRequest::default(), write())
            .await
            .is_err()
    );
    assert!(
        session
            .speak(
                SpeechRequest {
                    input: "hello".into(),
                    speed: Some(f64::NAN),
                    ..SpeechRequest::default()
                },
                write()
            )
            .await
            .is_err()
    );
    assert!(session.checkpoint(&"🙂".repeat(61), write()).await.is_err());
    assert!(
        session
            .events(Some("01"), CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn speech_preserves_large_audio_and_deferred_platform_result() {
    let server = Server::start(|r, _| {
        if r.target == "/api/sessions/s1" {
            return Some(meta("sdk1"));
        }
        if r.target.ends_with("/capabilities") {
            return Some(Reply::closure("enabled"));
        }
        Some(Reply::json(
            200,
            "session",
            json!({"errorCode":"platform-deferred","taskId":"task-1",
            "audio":{"b64":"A".repeat(2*1024*1024+1),"format":"wav","mime":"audio/wav"}}),
        ))
    })
    .await;
    let session = server.client("sdk1").attach("s1").await.unwrap();
    let raw = session
        .speak(
            SpeechRequest {
                input: "hello".into(),
                ..SpeechRequest::default()
            },
            write(),
        )
        .await
        .unwrap();
    assert_eq!(raw["errorCode"], "platform-deferred");
    assert_eq!(
        raw["audio"]["b64"].as_str().unwrap().len(),
        2 * 1024 * 1024 + 1
    );
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.method == "POST")
            .count(),
        1
    );
}

#[tokio::test]
async fn stream_cancel_drop_and_close_release_socket_without_interrupting_serve() {
    for mode in ["cancel", "drop", "close"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut meta_socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let length = meta_socket.read(&mut request).await.unwrap();
            assert!(length > 0, "metadata request arrived");
            let meta = br#"{"sessionId":"s1","status":"idle","live":true,"lastSeq":0}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\ntansr-contract: unified-v1\r\ntansr-manifest-revision: 7\r\ntansr-domain: session\r\ntansr-schema-hash: none\r\n\r\n",
                meta.len()
            );
            meta_socket.write_all(response.as_bytes()).await.unwrap();
            meta_socket.write_all(meta).await.unwrap();
            drop(meta_socket);
            let (mut stream_socket, _) = listener.accept().await.unwrap();
            let length = stream_socket.read(&mut request).await.unwrap();
            let route = String::from_utf8_lossy(&request[..length]).to_string();
            stream_socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\ntansr-contract: unified-v1\r\ntansr-manifest-revision: 7\r\ntansr-domain: session\r\ntansr-schema-hash: none\r\ntansr-event-envelope: unified-v1\r\n\r\n").await.unwrap();
            let length = stream_socket.read(&mut request).await.unwrap_or(0);
            (route, length)
        });
        let client = SessionClient::new(
            ApiClient::builder(format!("http://{address}"))
                .token("fixture")
                .session_family("sdk1")
                .build()
                .unwrap(),
        )
        .unwrap();
        let session = client.attach("s1").await.unwrap();
        let cancellation = CancellationToken::new();
        let mut events = session.events(None, cancellation.clone()).await.unwrap();
        match mode {
            "cancel" => {
                cancellation.cancel();
                assert!(matches!(
                    events.next().await.unwrap(),
                    Err(Error::Cancelled)
                ));
                events.shutdown().await;
            }
            "drop" => drop(events),
            _ => {
                events.close();
                events.close();
                events.shutdown().await;
            }
        }
        let (route, length) = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(route.starts_with("GET /api/sessions/s1/events"));
        assert!(!route.contains("interrupt"));
        assert_eq!(length, 0);
    }
}

#[tokio::test]
async fn unsupported_capability_states_do_not_mutate_and_durable_receipt_cannot_downgrade() {
    for state in ["disabled", "unavailable", "invented-future-state"] {
        let server = Server::start(move |r, _| {
            Some(if r.target.ends_with("/capabilities") {
                Reply::closure(state)
            } else {
                meta("sdk1")
            })
        })
        .await;
        let session = server.client("sdk1").attach("s1").await.unwrap();
        assert!(
            session
                .send("must not grant permission", write())
                .await
                .is_err()
        );
        assert!(
            session
                .submit_input(
                    Input {
                        input_id: "disabled-input".into(),
                        target: InputTarget {
                            history_epoch: "epoch".into(),
                            turn_id: "turn".into()
                        },
                        content: InputContent {
                            text: Some("must not become another turn".into()),
                            blocks: None
                        },
                        ack: None,
                    },
                    write()
                )
                .await
                .is_err()
        );
        assert!(server.requests().iter().all(|r| r.method == "GET"));
    }
    let server = Server::start(|r, _| {
        Some(if r.target.ends_with("/input-capabilities") {
            Reply::json(200, "session", json!({"durableAck":true}))
        } else if r.target.ends_with("/capabilities") {
            Reply::closure("enabled")
        } else if r.target.ends_with("/inputs") {
            Reply::json(
                202,
                "session",
                json!({"outcome":"accepted","receipt":receipt()}),
            )
        } else {
            meta("sdk1")
        })
    })
    .await;
    let session = server.client("sdk1").attach("s1").await.unwrap();
    assert!(matches!(
        session
            .submit_input(
                Input {
                    input_id: "input-1".into(),
                    target: InputTarget {
                        history_epoch: "epoch-1".into(),
                        turn_id: "turn-1".into()
                    },
                    content: InputContent {
                        text: Some("must remain durable".into()),
                        blocks: None
                    },
                    ack: Some("durable".into()),
                },
                write()
            )
            .await,
        Err(Error::Contract(_))
    ));
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.method == "POST")
            .count(),
        1,
        "mismatched receipt must not trigger a replacement write"
    );
}

#[tokio::test]
async fn expired_write_deadline_includes_family_closure_and_durable_preflights() {
    let server = Server::start(|_, _| Some(meta("sdk1"))).await;
    let client = server.client("sdk1");
    let session = client.attach("s1").await.unwrap();
    let expired = || WriteOptions {
        deadline: Some(SystemTime::now() - Duration::from_secs(1)),
        ..Default::default()
    };
    assert!(
        client
            .create(CreateOptions {
                write: expired(),
                ..Default::default()
            })
            .await
            .is_err()
    );
    assert!(session.send("must not discover", expired()).await.is_err());
    assert!(
        session
            .submit_input(
                Input {
                    input_id: "expired".into(),
                    target: InputTarget {
                        history_epoch: "epoch".into(),
                        turn_id: "turn".into()
                    },
                    content: InputContent {
                        text: Some("synthetic".into()),
                        blocks: None
                    },
                    ack: Some("durable".into()),
                },
                expired()
            )
            .await
            .is_err()
    );
    assert_eq!(
        server.requests().len(),
        1,
        "expired calls performed no preflight or mutation"
    );
}

/// The peer keeps the response incomplete until the SDK drops the socket.
/// Each mode covers a distinct owning future: response headers, body and SSE
/// setup. A JoinHandle is explicitly aborted and awaited where one is used.
#[tokio::test]
async fn dropped_read_and_connect_futures_release_their_actual_sockets() {
    for partial_body in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (received, ready) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 4096];
            let n = socket.read(&mut bytes).await.unwrap();
            assert!(String::from_utf8_lossy(&bytes[..n]).starts_with("GET /api/sessions/s1 "));
            if partial_body {
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 999\r\ntansr-contract: unified-v1\r\ntansr-manifest-revision: 7\r\ntansr-domain: session\r\ntansr-schema-hash: none\r\n\r\n{").await.unwrap();
            }
            received.send(()).unwrap();
            socket.read(&mut bytes).await.unwrap_or(0)
        });
        let client = SessionClient::new(
            ApiClient::builder(endpoint)
                .token("fixture")
                .session_family("sdk1")
                .build()
                .unwrap(),
        )
        .unwrap();
        let mut request = Box::pin(client.attach("s1"));
        tokio::select! {
            result = &mut request => panic!("blocked request unexpectedly ended: {}", result.is_ok()),
            result = ready => result.unwrap(),
        }
        drop(request);
        drop(client);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), peer)
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn cancelling_shared_client_concurrently_is_idempotent_and_releases_waiters() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (received, ready) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 4096];
        assert!(socket.read(&mut bytes).await.unwrap() > 0);
        received.send(()).unwrap();
        socket.read(&mut bytes).await.unwrap_or(0)
    });
    let api = ApiClient::builder(endpoint)
        .token("fixture")
        .session_family("sdk1")
        .build()
        .unwrap();
    let client = SessionClient::new(api.clone()).unwrap();
    let request = tokio::spawn(async move { client.attach("s1").await.map(|_| ()) });
    ready.await.unwrap();
    let clone = api.clone();
    tokio::join!(api.shutdown(), clone.shutdown());
    api.shutdown().await;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), request)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::Cancelled)
    ));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), peer)
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[test]
fn runtime_shutdown_aborts_owned_request_and_releases_the_socket() {
    use std::io::Read;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (received, ready) = std::sync::mpsc::channel();
    let peer = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = [0; 4096];
        assert!(socket.read(&mut bytes).unwrap() > 0);
        received.send(()).unwrap();
        match socket.read(&mut bytes) {
            Ok(n) => n,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::UnexpectedEof
                ) =>
            {
                0
            }
            Err(error) => panic!("socket remained open after runtime shutdown: {error}"),
        }
    });
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let api = ApiClient::builder(endpoint)
        .token("fixture")
        .session_family("sdk1")
        .build()
        .unwrap();
    let pending = runtime.spawn(async move {
        SessionClient::new(api)
            .unwrap()
            .attach("s1")
            .await
            .map(|_| ())
    });
    ready.recv_timeout(Duration::from_secs(3)).unwrap();
    // Runtime shutdown owns cancellation. Dropping a JoinHandle alone would
    // detach the task and is deliberately not used as the cleanup mechanism.
    runtime.shutdown_timeout(Duration::from_secs(1));
    drop(pending);
    assert_eq!(peer.join().unwrap(), 0);
}
