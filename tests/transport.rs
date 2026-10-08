//! Transport adversaries use real local sockets and synthetic credentials only.
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};
use tansr_sdk::{ApiClient, CallOptions, Error};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
    task::{JoinHandle, JoinSet},
};

const TOKEN: &str = "synthetic-private-token-not-for-logs";
const HASH: &str = "sha256:b60e77ffcbf08d985a993dbdbd4cf610f12f7c7e70f090aee5ff8d523f70bb57";

#[derive(Debug)]
struct Request {
    line: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

#[derive(Clone)]
struct Reply {
    bytes: Vec<u8>,
    fragment: usize,
    hold: bool,
}

struct Server {
    url: String,
    requests: mpsc::UnboundedReceiver<Request>,
    count: Arc<AtomicUsize>,
    disconnected: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn next_request(&mut self) -> Request {
        tokio::time::timeout(Duration::from_secs(3), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn wait_disconnected(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self.disconnected.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("SDK did not release the active connection");
    }
}

async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<Request> {
    let mut bytes = Vec::new();
    let header_end = loop {
        if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break end + 4;
        }
        if bytes.len() > 32 * 1024 {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let mut chunk = [0; 1024];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        bytes.extend_from_slice(&chunk[..n]);
    };
    let text = std::str::from_utf8(&bytes[..header_end]).map_err(|_| io::ErrorKind::InvalidData)?;
    let mut lines = text.split("\r\n");
    let line = lines.next().unwrap().to_owned();
    let headers: BTreeMap<_, _> = lines
        .filter_map(|s| s.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    let length = headers
        .get("content-length")
        .map_or(Ok(0), |v| v.parse::<usize>())
        .map_err(|_| io::ErrorKind::InvalidData)?;
    if length > 64 * 1024 {
        return Err(io::ErrorKind::InvalidData.into());
    }
    while bytes.len() - header_end < length {
        let mut chunk = [0; 1024];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
    Ok(Request {
        line,
        headers,
        body: bytes[header_end..header_end + length].to_vec(),
    })
}

async fn connection<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    reply: Reply,
    send: mpsc::UnboundedSender<Request>,
    count: Arc<AtomicUsize>,
    disconnected: Arc<AtomicUsize>,
) {
    let Ok(request) = read_request(&mut stream).await else {
        return;
    };
    count.fetch_add(1, Ordering::SeqCst);
    let _ = send.send(request);
    for fragment in reply.bytes.chunks(reply.fragment.max(1)) {
        if stream.write_all(fragment).await.is_err() {
            return;
        }
        tokio::task::yield_now().await;
    }
    if reply.hold {
        let mut byte = [0];
        while let Ok(n) = stream.read(&mut byte).await {
            if n == 0 {
                break;
            }
        }
        disconnected.fetch_add(1, Ordering::SeqCst);
    }
    let _ = stream.shutdown().await;
}

async fn serve(reply: Reply) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (send, requests) = mpsc::unbounded_channel();
    let count = Arc::new(AtomicUsize::new(0));
    let disconnected = Arc::new(AtomicUsize::new(0));
    let received = count.clone();
    let closed = disconnected.clone();
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else { break; };
                    connections.spawn(connection(stream, reply.clone(), send.clone(), received.clone(), closed.clone()));
                }
                _ = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });
    Server {
        url,
        requests,
        count,
        disconnected,
        task,
    }
}

fn headers(domain: &str) -> String {
    format!(
        "tansr-contract: unified-v1\r\ntansr-manifest-revision: 7\r\ntansr-schema-hash: {HASH}\r\ntansr-domain: {domain}\r\n"
    )
}

fn response(status: u16, domain: &str, body: &[u8]) -> Reply {
    let head = format!(
        "HTTP/1.1 {status} Test\r\n{}content-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        headers(domain),
        body.len()
    );
    let mut bytes = head.into_bytes();
    bytes.extend_from_slice(body);
    Reply {
        bytes,
        fragment: 4096,
        hold: false,
    }
}

fn client(server: &Server) -> ApiClient {
    ApiClient::builder(&server.url)
        .token(TOKEN)
        .session_family("sdk1")
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap()
}

fn parameter(id: &str) -> CallOptions {
    CallOptions {
        params: BTreeMap::from([("id".into(), id.into())]),
        ..Default::default()
    }
}

fn event_body() -> Value {
    json!({"contract":"unified-v1","eventId":"12","domain":"session","type":"msg.text.delta",
        "cursorSet":{"eventCursor":"12","archiveCoverage":null,"outputWatermark":null,"materialConsumed":null,"ackReceipt":null},
        "terminalStatus":null,"raw":{"type":"msg.text.delta","sessionId":"s-1","seq":12,"text":"汉😀"}})
}

fn stream_reply(data: &str, complete: bool) -> Reply {
    let mut bytes = format!("HTTP/1.1 200 OK\r\n{}content-type: text/event-stream\r\ntansr-event-envelope: unified-v1\r\ntransfer-encoding: chunked\r\n\r\n", headers("session")).into_bytes();
    if !data.is_empty() {
        bytes.extend_from_slice(format!("{:x}\r\n{data}\r\n", data.len()).as_bytes());
    }
    if complete {
        bytes.extend_from_slice(b"0\r\n\r\n");
    }
    Reply {
        bytes,
        fragment: 7,
        hold: !complete,
    }
}

#[tokio::test]
async fn real_http_routes_headers_and_business_json() {
    let mut server = serve(response(202, "session", b"{\"accepted\":true}")).await;
    let client = client(&server);
    let mut opts = parameter("会话%2F+!~");
    opts.body = Some(json!({"text":"synthetic business text", "ratio":-1.25}));
    opts.idempotency_key = Some("message-1".into());
    opts.closure_id = Some("a".repeat(64));
    opts.deadline = Some(SystemTime::now() + Duration::from_secs(10));
    let original = opts.body.clone().unwrap();
    assert_eq!(
        client
            .call("session.message.send", opts)
            .await
            .unwrap()
            .status,
        202
    );
    let request = server.next_request().await;
    assert_eq!(
        request.line,
        "POST /api/sessions/%E4%BC%9A%E8%AF%9D%252F%2B!~/messages HTTP/1.1"
    );
    assert_eq!(request.headers["authorization"], format!("Bearer {TOKEN}"));
    assert_eq!(request.headers["tansr-session-family"], "sdk1");
    assert_eq!(request.headers["tansr-closure-id"], "a".repeat(64));
    assert_eq!(request.headers["idempotency-key"], "message-1");
    assert!(request.headers["deadline"].ends_with('Z'));
    assert_eq!(
        serde_json::from_slice::<Value>(&request.body).unwrap(),
        original
    );
}

#[tokio::test]
async fn same_and_cross_origin_redirects_are_never_followed() {
    for same_origin in [true, false] {
        let destination = serve(response(200, "session", b"{}")).await;
        let location = if same_origin {
            "/api/redirected".into()
        } else {
            format!("{}/api/redirected", destination.url)
        };
        let bytes = format!("HTTP/1.1 307 Temporary Redirect\r\nlocation: {location}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").into_bytes();
        let server = serve(Reply {
            bytes,
            fragment: 4096,
            hold: false,
        })
        .await;
        let error = client(&server)
            .call("session.list", CallOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Contract(_)));
        assert!(!error.to_string().contains(TOKEN));
        assert_eq!(server.count.load(Ordering::SeqCst), 1);
        assert_eq!(destination.count.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn missing_wrong_duplicate_headers_html_and_duplicate_json_fail_closed() {
    let good = response(200, "session", b"{}");
    let text = String::from_utf8(good.bytes).unwrap();
    let variants = [
        text.replace("tansr-contract: unified-v1\r\n", ""),
        text.replace("unified-v1", "legacy-v0"),
        text.replace("tansr-manifest-revision: 7", "tansr-manifest-revision: 07"),
        text.replace(
            "tansr-contract: unified-v1",
            "tansr-contract: unified-v1\r\ntansr-contract: unified-v1",
        ),
        text.replace("content-type: application/json", "content-type: text/html"),
    ];
    for bytes in variants {
        let server = serve(Reply {
            bytes: bytes.into_bytes(),
            fragment: 4096,
            hold: false,
        })
        .await;
        let error = client(&server)
            .call("session.list", CallOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Contract(_)));
        assert!(!format!("{error:?}").contains(TOKEN));
    }
    let secret = "synthetic-private-body-not-for-logs";
    let body = format!("{{\"a\":\"{secret}\",\"a\":2}}");
    let server = serve(response(200, "session", body.as_bytes())).await;
    let error = client(&server)
        .call("session.list", CallOptions::default())
        .await
        .unwrap_err();
    assert!(!error.to_string().contains(secret));
    assert!(matches!(error, Error::Contract(_)));
}

#[tokio::test]
async fn local_request_guards_do_not_send() {
    let server = serve(response(200, "session", b"{}")).await;
    let client = client(&server);
    let cases = [
        (
            "session.list",
            CallOptions {
                idempotency_key: Some("key".into()),
                ..Default::default()
            },
        ),
        (
            "session.list",
            CallOptions {
                if_match: Some("\"3\"".into()),
                ..Default::default()
            },
        ),
        (
            "session.list",
            CallOptions {
                deadline: Some(SystemTime::UNIX_EPOCH),
                ..Default::default()
            },
        ),
        (
            "session.list",
            CallOptions {
                query: BTreeMap::from([("unknown".into(), "1".into())]),
                ..Default::default()
            },
        ),
        ("session.get", parameter("../escape")),
        ("session.get", parameter("a/b")),
        ("session.get", parameter("..")),
        ("session.get", parameter("a\\b")),
        ("session.get", parameter("a\n")),
        (
            "session.message.send",
            CallOptions {
                if_match: Some("3".into()),
                ..parameter("s")
            },
        ),
        (
            "session.message.send",
            CallOptions {
                idempotency_key: Some("has space".into()),
                ..parameter("s")
            },
        ),
    ];
    for (operation, options) in cases {
        assert!(
            client.call(operation, options).await.is_err(),
            "accepted {operation}"
        );
    }
    for etag in ["W/\"3\"", "*", "\"3\",\"4\"", "03", "-1"] {
        let opts = CallOptions {
            if_match: Some(etag.into()),
            ..parameter("b-1")
        };
        assert!(client.call("archive.binding.close", opts).await.is_err());
    }
    let cancelled = CallOptions::default();
    cancelled.cancellation.cancel();
    assert!(matches!(
        client.call("session.list", cancelled).await,
        Err(Error::Cancelled)
    ));
    client.shutdown().await;
    assert!(matches!(
        client.call("session.list", CallOptions::default()).await,
        Err(Error::Cancelled)
    ));
    assert_eq!(server.count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn conditional_headers_are_sent_without_rewriting_original_body() {
    let receipt = json!({"protocol":"sdk2-ext-v1","request":{"requestId":"original","operationEpoch":"epoch-1"},"bindingId":"b-1","operation":"binding-close","semanticDigest":"a".repeat(64),"state":"completed","revision":"4","outcomeRef":"outcome-1"});
    let mut server = serve(response(
        200,
        "archive",
        &serde_json::to_vec(&receipt).unwrap(),
    ))
    .await;
    let client = client(&server);
    let body = json!({"protocol":"sdk2-ext-v1","request":{"operationEpoch":"epoch-1"},"bindingId":"b-1","generations":{"historyEpoch":"h-1","deletionGeneration":"0","projectionRevision":"1"}});
    let options = CallOptions {
        body: Some(body.clone()),
        if_match: Some("3".into()),
        idempotency_key: Some("original".into()),
        deadline: Some(SystemTime::now() + Duration::from_secs(10)),
        ..parameter("b-1")
    };
    client.call("archive.binding.close", options).await.unwrap();
    let request = server.next_request().await;
    assert_eq!(request.headers["if-match"], "\"3\"");
    assert_eq!(request.headers["idempotency-key"], "original");
    assert_eq!(
        serde_json::from_slice::<Value>(&request.body).unwrap(),
        body,
        "Serve performs the three-header mapping; SDK keeps the original body"
    );
    let conflicting = json!({"protocol":"sdk2-ext-v1","request":{"requestId":"different","operationEpoch":"epoch-1"},"bindingId":"b-1","expectedRevision":"3","generations":{"historyEpoch":"h-1","deletionGeneration":"0","projectionRevision":"1"}});
    assert!(
        client
            .call(
                "archive.binding.close",
                CallOptions {
                    body: Some(conflicting),
                    idempotency_key: Some("original".into()),
                    ..parameter("b-1")
                }
            )
            .await
            .is_err()
    );
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancellation_releases_a_response_that_never_finishes() {
    let bytes = format!(
        "HTTP/1.1 200 OK\r\n{}content-type: application/json\r\ncontent-length: 100\r\n\r\n",
        headers("session")
    )
    .into_bytes();
    let mut server = serve(Reply {
        bytes,
        fragment: 4096,
        hold: true,
    })
    .await;
    let client = client(&server);
    let options = CallOptions::default();
    let cancel = options.cancellation.clone();
    let task = tokio::spawn(async move { client.call("session.list", options).await });
    server.next_request().await;
    cancel.cancel();
    let result = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(Error::Cancelled)));
    server.wait_disconnected().await;
}

#[tokio::test]
async fn sse_fragments_cr_delivery_and_drop_release_real_connection() {
    let data = format!(
        "\u{feff}: synthetic\rid: 12\revent: msg.text.delta\rdata: {}\r\r",
        event_body()
    );
    let mut server = serve(stream_reply(&data, false)).await;
    let client = client(&server);
    let options = CallOptions {
        last_event_id: Some("11".into()),
        ..parameter("s-1")
    };
    let mut stream = client
        .events("session.events.observe", options)
        .await
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(event.event_id.as_deref(), Some("12"));
    assert_eq!(event.raw["text"], "汉😀");
    let request = server.next_request().await;
    assert_eq!(request.headers["last-event-id"], "11");
    assert_eq!(request.headers["tansr-event-envelope"], "unified-v1");
    drop(stream);
    server.wait_disconnected().await;
}

#[tokio::test]
async fn sse_truncation_bad_envelope_and_wrong_cursor_are_errors() {
    let mut extra = event_body();
    extra["payload"] = json!({});
    for data in [
        format!("id: 12\ndata: {}\n", event_body()),
        format!("id: 13\ndata: {}\n\n", event_body()),
        format!("id: 12\ndata: {extra}\n\n"),
        "data: <html>proxy</html>\n\n".into(),
    ] {
        let server = serve(stream_reply(&data, true)).await;
        let client = client(&server);
        let mut stream = client
            .events("session.events.observe", parameter("s-1"))
            .await
            .unwrap();
        assert!(stream.next().await.unwrap().is_err());
        assert!(stream.next().await.is_none());
    }
    let server = serve(stream_reply("", true)).await;
    let client = client(&server);
    let mut stream = client
        .events("session.events.observe", parameter("s-1"))
        .await
        .unwrap();
    assert!(
        stream.next().await.is_none(),
        "clean EOF is not a terminal event"
    );
}

#[tokio::test]
async fn no_implicit_retry_and_explicit_replay_respects_original_deadline() {
    let body = json!({"contract":"unified-v1","traceId":"trace-1","requestId":null,"code":"upstream_unavailable","status":503,"retryAction":"same-request","retryAfterMs":1,"message":"temporary"});
    let server = serve(response(
        503,
        "session",
        &serde_json::to_vec(&body).unwrap(),
    ))
    .await;
    let client = client(&server);
    let result = client.call("session.list", CallOptions::default()).await;
    assert!(
        matches!(result, Err(Error::Api(_))),
        "valid optional-detail error: {result:?}"
    );
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
    let body = json!({"contract":"unified-v1","traceId":"trace-2","requestId":"original","code":"upstream_unavailable","status":503,"retryAction":"same-request","retryAfterMs":2000,"message":"temporary"});
    let delayed = serve(response(
        503,
        "session",
        &serde_json::to_vec(&body).unwrap(),
    ))
    .await;
    let replay_client = ApiClient::builder(&delayed.url)
        .token("fixture")
        .session_family("sdk1")
        .build()
        .unwrap();
    let options = CallOptions {
        idempotency_key: Some("original".into()),
        deadline: Some(SystemTime::now() + Duration::from_millis(300)),
        ..parameter("s-1")
    };
    let Err(Error::Api(previous)) = replay_client
        .call("session.interrupt", options.clone())
        .await
    else {
        panic!("expected original retry advice")
    };
    assert!(
        replay_client
            .retry_same_request("session.interrupt", options, &previous)
            .await
            .is_err()
    );
    assert_eq!(
        delayed.count.load(Ordering::SeqCst),
        1,
        "expired delayed replay must never reach Serve"
    );
}

async fn tls_server(expired: bool) -> (Server, String) {
    use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
    if expired {
        params.not_before = time::OffsetDateTime::from_unix_timestamp(1_577_836_800).unwrap();
        params.not_after = time::OffsetDateTime::from_unix_timestamp(1_609_459_200).unwrap();
    }
    let certificate = params.self_signed(&key).unwrap();
    let pem = certificate.pem();
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.der().clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        )
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "https://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    let (send, requests) = mpsc::unbounded_channel();
    let count = Arc::new(AtomicUsize::new(0));
    let disconnected = Arc::new(AtomicUsize::new(0));
    let received = count.clone();
    let closed = disconnected.clone();
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else { break; };
                    let acceptor = acceptor.clone();
                    let send = send.clone(); let count = received.clone(); let disconnected = closed.clone();
                    connections.spawn(async move {
                        if let Ok(Ok(stream)) = tokio::time::timeout(Duration::from_secs(5), acceptor.accept(stream)).await {
                            connection(stream, response(200, "session", b"{}"), send, count, disconnected).await;
                        }
                    });
                }
                _ = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });
    (
        Server {
            url,
            requests,
            count,
            disconnected,
            task,
        },
        pem,
    )
}

#[tokio::test]
async fn private_ca_is_explicit_and_does_not_disable_hostname_or_time_validation() {
    let (server, pem) = tls_server(false).await;
    let untrusted = client(&server)
        .call("session.list", CallOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(untrusted, Error::Transport(_)));
    assert!(!format!("{untrusted:?}").contains(TOKEN));
    assert_eq!(server.count.load(Ordering::SeqCst), 0);
    let trusted = ApiClient::builder(&server.url)
        .session_family("sdk1")
        .token(TOKEN)
        .root_certificate_pem(pem.as_bytes())
        .unwrap()
        .build()
        .unwrap();
    trusted
        .call("session.list", CallOptions::default())
        .await
        .unwrap();
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
    let wrong_host = server.url.replace("localhost", "127.0.0.1");
    let wrong = ApiClient::builder(wrong_host)
        .session_family("sdk1")
        .token(TOKEN)
        .root_certificate_pem(pem.as_bytes())
        .unwrap()
        .build()
        .unwrap();
    assert!(matches!(
        wrong.call("session.list", CallOptions::default()).await,
        Err(Error::Transport(_))
    ));
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
    let (expired, pem) = tls_server(true).await;
    let expired_client = ApiClient::builder(&expired.url)
        .session_family("sdk1")
        .token(TOKEN)
        .root_certificate_pem(pem.as_bytes())
        .unwrap()
        .build()
        .unwrap();
    assert!(matches!(
        expired_client
            .call("session.list", CallOptions::default())
            .await,
        Err(Error::Transport(_))
    ));
    assert_eq!(expired.count.load(Ordering::SeqCst), 0);
}
