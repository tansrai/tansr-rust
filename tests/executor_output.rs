use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tansr_sdk::{ApiClient, CancellationToken, executor::*};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

struct Observed {
    bytes: Vec<u8>,
    blocks: Vec<Value>,
    seal: Option<Value>,
}
struct Fixture {
    base: String,
    state: Arc<Mutex<Observed>>,
    posts: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn fixture(lose_first: bool, forge: bool) -> Fixture {
    fixture_with_pause(lose_first, forge, None).await
}
async fn fixture_with_pause(
    lose_first: bool,
    forge: bool,
    pause: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
) -> Fixture {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let state = Arc::new(Mutex::new(Observed {
        bytes: vec![],
        blocks: vec![],
        seal: None,
    }));
    let posts = Arc::new(AtomicUsize::new(0));
    let shared = state.clone();
    let counter = posts.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut bytes = vec![];
            let boundary = loop {
                let mut chunk = [0u8; 2048];
                let n = socket.read(&mut chunk).await.unwrap();
                if n == 0 {
                    return;
                }
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    break i + 4;
                }
            };
            let headers = String::from_utf8(bytes[..boundary].to_vec()).unwrap();
            let length = headers
                .lines()
                .find_map(|l| {
                    l.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(str::trim)
                        .and_then(|s| s.parse::<usize>().ok())
                })
                .unwrap_or(0);
            while bytes.len() < boundary + length {
                let mut chunk = [0u8; 4096];
                let n = socket.read(&mut chunk).await.unwrap();
                if n == 0 {
                    return;
                }
                bytes.extend_from_slice(&chunk[..n]);
            }
            let post = headers.starts_with("POST ");
            let v: Value = if post {
                serde_json::from_slice(&bytes[boundary..boundary + length]).unwrap()
            } else {
                Value::Null
            };
            if post {
                let mut s = shared.lock().unwrap();
                for block in v["blocks"].as_array().unwrap() {
                    assert_eq!(block["seq"], s.blocks.len().to_string());
                    assert_eq!(block["byteOffset"], s.bytes.len().to_string());
                    let part = STANDARD.decode(block["base64"].as_str().unwrap()).unwrap();
                    assert_eq!(
                        block["payloadDigest"],
                        format!("{:x}", Sha256::digest(&part))
                    );
                    s.bytes.extend(part);
                    s.blocks.push(block.clone());
                }
                if !v["seal"].is_null() {
                    s.seal = Some(v["seal"].clone());
                }
            }
            if post && counter.fetch_add(1, Ordering::SeqCst) == 0 {
                if let Some((seen, release)) = &pause {
                    seen.notify_one();
                    release.notified().await;
                }
                if lose_first {
                    drop(socket);
                    continue;
                }
            }
            let response = {
                let s = shared.lock().unwrap();
                let seq = s
                    .blocks
                    .last()
                    .map(|b| b["seq"].clone())
                    .unwrap_or(Value::Null);
                json!({"contract":"terminal-services-v1","operation":{"operationId":"op","requestDigest":"a".repeat(64)},"state":if let Some(seal)=&s.seal{if seal["truncated"]==true{"truncated"}else{"complete"}}else if s.blocks.is_empty(){"available"}else{"receiving"},"acceptedThrough":seq,"durableThrough":seq,"retainedFrom":if s.blocks.is_empty(){Value::Null}else{json!("0")},"nextByteOffset":if forge{(s.bytes.len()+1).to_string()}else{s.bytes.len().to_string()},"seal":s.seal})
            };
            let body = serde_json::to_vec(&response).unwrap();
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\ntansr-contract: unified-v1\r\ntansr-manifest-revision: 7\r\ntansr-schema-hash: none\r\ntansr-domain: terminal\r\n\r\n",
                body.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
        }
    });
    Fixture {
        base,
        state,
        posts,
        task,
    }
}

#[tokio::test]
async fn concurrent_channels_keep_one_sequence_and_exact_shared_seal() {
    let f = fixture(false, false).await;
    let writer = writer(&f, 65536, 16);
    let mut tasks = Vec::new();
    for n in 0..32u8 {
        let writer = writer.clone();
        tasks.push(tokio::spawn(async move {
            assert_eq!(
                writer
                    .capture(if n % 2 == 0 { "stdout" } else { "stderr" }, &[n; 16])
                    .await
                    .unwrap(),
                16
            );
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), writer.finish())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.seal.as_ref().unwrap().total_bytes, "512");
    let s = f.state.lock().unwrap();
    assert_eq!(s.blocks.len(), 32);
    let mut seen = [false; 32];
    for part in s.bytes.chunks_exact(16) {
        assert!(part.iter().all(|v| *v == part[0]));
        assert!(!seen[part[0] as usize]);
        seen[part[0] as usize] = true;
    }
    assert!(seen.into_iter().all(|v| v));
    assert_eq!(
        status.seal.unwrap().payload_digest,
        format!("{:x}", Sha256::digest(&s.bytes))
    );
}

#[tokio::test]
async fn slow_ack_keeps_inflight_bytes_bounded_and_never_blocks_producer() {
    let seen = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let f = fixture_with_pause(false, false, Some((seen.clone(), release.clone()))).await;
    let writer = writer(&f, 520, 16);
    writer.capture("stdout", b"first-in-flight!").await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), seen.notified())
        .await
        .unwrap();
    assert!(
        writer.snapshot().unwrap().pending_bytes > 0,
        "inflight is charged until ACK"
    );
    let retained = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        writer.capture("stderr", &[b'x'; 8192]),
    )
    .await
    .unwrap()
    .unwrap();
    let snapshot = writer.snapshot().unwrap();
    assert!(snapshot.pending_bytes <= 520 && retained < 8192);
    assert_eq!(
        f.posts.load(Ordering::SeqCst),
        1,
        "slow peer cannot create parallel unbounded posts"
    );
    release.notify_one();
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), writer.finish())
        .await
        .unwrap()
        .unwrap();
    assert!(status.seal.unwrap().truncated);
    assert_eq!(f.state.lock().unwrap().bytes.len(), 16 + retained);
}

#[tokio::test]
async fn cancellation_stops_inflight_upload_and_preserves_unacknowledged_prefix() {
    let seen = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let f = fixture_with_pause(false, false, Some((seen.clone(), release))).await;
    let writer = writer(&f, 4096, 16);
    writer.capture("stdout", b"uncertain").await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), seen.notified())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), writer.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(writer.finish().await.is_err());
    let state = writer.snapshot().unwrap();
    assert!(state.failed && !state.sealed && state.pending_bytes > 0);
    assert!(writer.capture("stderr", b"late").await.is_err());
    assert_eq!(f.posts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn total_capture_never_exceeds_negotiated_retained_budget() {
    let f = fixture(false, false).await;
    let w = writer(&f, 131072, 1024);
    assert_eq!(w.capture("stdout", &[b'x'; 9000]).await.unwrap(), 8192);
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), w.finish())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.seal.as_ref().unwrap().total_bytes, "8192");
    assert!(status.seal.unwrap().truncated);
}
fn writer(f: &Fixture, pending: usize, block: usize) -> OutputWriter {
    let api = ApiClient::builder(&f.base)
        .token("fixture")
        .session_family("sdk1")
        .build()
        .unwrap();
    OutputWriter::new(
        api,
        OutputOptions {
            session: TerminalSessionReference {
                session_contract: "sdk1".into(),
                session_id: "session".into(),
            },
            operation: OutputOperationReference {
                operation_id: "op".into(),
                request_digest: "a".repeat(64),
            },
            executor_id: "device".into(),
            connection_id: "connection".into(),
            limits: OutputLimits {
                max_control_bytes: 4096,
                max_block_bytes: block,
                max_batch_bytes: 1024,
                max_pending_bytes: pending,
                max_retained_bytes: 8192,
            },
            encoding: "binary".into(),
            cancellation: CancellationToken::new(),
        },
    )
    .unwrap()
}

#[tokio::test]
async fn original_bytes_order_and_seal_wait_for_ack() {
    let f = fixture(false, false).await;
    let w = writer(&f, 4096, 2);
    let first = [0xf0, 0x9f, 0x98];
    assert_eq!(w.capture("stdout", &first).await.unwrap(), 3);
    assert_eq!(w.capture("stderr", &[0x80, b'!']).await.unwrap(), 2);
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), w.finish())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.state, "complete");
    {
        let s = f.state.lock().unwrap();
        assert_eq!(s.bytes, [0xf0, 0x9f, 0x98, 0x80, b'!']);
        assert_eq!(
            s.seal.as_ref().unwrap()["payloadDigest"],
            format!("{:x}", Sha256::digest(&s.bytes))
        );
    }
    assert_eq!(w.snapshot().unwrap().pending_bytes, 0);
    assert!(w.capture("stdout", b"late").await.is_err());
}

#[tokio::test]
async fn bounded_prefix_truncates_but_capture_keeps_draining() {
    let f = fixture(false, false).await;
    let w = writer(&f, 260, 16);
    let retained = w.capture("stdout", &[b'x'; 4096]).await.unwrap();
    assert!(retained > 0 && retained < 4096);
    assert_eq!(w.capture("stderr", b"drained").await.unwrap(), 0);
    let snapshot = w.snapshot().unwrap();
    assert!(snapshot.pending_bytes <= 260);
    assert_eq!(snapshot.dropped_bytes, (4096 - retained + 7) as u64);
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), w.finish())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.state, "truncated");
    assert_eq!(f.state.lock().unwrap().bytes.len(), retained);
}

#[tokio::test]
async fn lost_post_response_queries_same_operation_without_duplicate_capture() {
    let f = fixture(true, false).await;
    let w = writer(&f, 4096, 16);
    w.capture("stdout", b"once").await.unwrap();
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), w.finish())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.state, "complete");
    assert_eq!(f.state.lock().unwrap().bytes, b"once");
    assert_eq!(f.posts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn impossible_ack_retains_pending_and_fails_finish() {
    let f = fixture(false, true).await;
    let w = writer(&f, 4096, 16);
    w.capture("stdout", b"once").await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), w.finish())
            .await
            .unwrap()
            .is_err()
    );
    let snapshot = w.snapshot().unwrap();
    assert!(snapshot.failed);
    assert!(snapshot.pending_bytes > 0);
    assert!(!snapshot.sealed);
}

#[tokio::test]
async fn empty_output_has_explicit_empty_seal() {
    let f = fixture(false, false).await;
    let w = writer(&f, 4096, 16);
    let status = tokio::time::timeout(std::time::Duration::from_secs(5), w.finish())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.seal.unwrap().last_seq, None);
    assert_eq!(f.posts.load(Ordering::SeqCst), 1);
}
