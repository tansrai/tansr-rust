use serde_json::json;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant, SystemTime},
};
use tansr_sdk::{CallOptions, CancellationToken, ClientBuilder, Error};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn read_request(socket: &mut TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let mut buffer = [0u8; 4096];
        let read = socket.read(&mut buffer).await.unwrap();
        assert!(read > 0);
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(position) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..position]);
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|v| v.trim().parse().unwrap())
                })
                .unwrap_or(0);
            if bytes.len() >= position + 4 + length {
                return bytes;
            }
        }
    }
}

#[tokio::test]
async fn replay_requires_same_client_operation_body_and_deadline_and_honors_retry_after() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut captured = Vec::new();
        for index in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            captured.push(read_request(&mut socket).await);
            let (status, body) = if index == 0 {
                (503, json!({"contract":"unified-v1","traceId":"retry-test","requestId":"original","code":"upstream_unavailable","status":503,"retryAction":"same-request","message":"temporary"}).to_string())
            } else {
                (202, json!({"accepted":true}).to_string())
            };
            let response = format!(
                "HTTP/1.1 {status} Fixture\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\ntansr-contract: unified-v1\r\ntansr-manifest-revision: 7\r\ntansr-domain: session\r\ntansr-schema-hash: none\r\nRetry-After: 0.02\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        captured
    });
    let client = ClientBuilder::new(&address)
        .token("synthetic")
        .session_family("sdk1")
        .build()
        .unwrap();
    let options = CallOptions {
        params: BTreeMap::from([("id".into(), "session-one".into())]),
        body: Some(json!({"prompt":"one"})),
        idempotency_key: Some("original".into()),
        deadline: Some(SystemTime::now() + Duration::from_secs(10)),
        ..Default::default()
    };
    let error = client
        .call("session.message.send", options.clone())
        .await
        .unwrap_err();
    let Error::Api(previous) = error else {
        panic!("expected typed remote error")
    };
    assert_eq!(previous.retry_after_ms, Some(20));
    assert!(previous.detail.is_null());
    let mut changed = options.clone();
    changed.body = Some(json!({"prompt":"changed"}));
    assert!(
        client
            .retry_same_request("session.message.send", changed, &previous)
            .await
            .is_err()
    );
    let mut changed = options.clone();
    changed.deadline = Some(SystemTime::now() + Duration::from_secs(20));
    assert!(
        client
            .retry_same_request("session.message.send", changed, &previous)
            .await
            .is_err()
    );
    let other = ClientBuilder::new(&address)
        .token("synthetic-other")
        .session_family("sdk1")
        .build()
        .unwrap();
    assert!(
        other
            .retry_same_request("session.message.send", options.clone(), &previous)
            .await
            .is_err()
    );
    let mut contradictory = (*previous).clone();
    contradictory.detail = json!({"domainCode":"commit_unknown"});
    assert!(
        client
            .retry_same_request("session.message.send", options.clone(), &contradictory)
            .await
            .is_err()
    );
    let start = Instant::now();
    let response = client
        .retry_same_request("session.message.send", options, &previous)
        .await
        .unwrap();
    assert!(start.elapsed() >= Duration::from_millis(20));
    assert_eq!(response.status, 202);
    let captured = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(captured[0], captured[1]);
}

#[tokio::test]
async fn cancellation_during_retry_after_wait_never_sends_a_second_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let original = read_request(&mut socket).await;
        assert!(String::from_utf8_lossy(&original).contains("original-cancel"));
        let body = json!({"contract":"unified-v1","traceId":"retry-cancel-test","requestId":"original-cancel","code":"upstream_unavailable","status":503,"retryAction":"same-request","message":"temporary"}).to_string();
        socket.write_all(format!(
            "HTTP/1.1 503 Fixture\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\ntansr-contract: unified-v1\r\ntansr-manifest-revision: 7\r\ntansr-domain: session\r\ntansr-schema-hash: none\r\nRetry-After: 30\r\n\r\n{body}",
            body.len()
        ).as_bytes()).await.unwrap();
        listener
    });
    let client = ClientBuilder::new(&address)
        .token("synthetic")
        .session_family("sdk1")
        .build()
        .unwrap();
    let cancellation = CancellationToken::new();
    let options = CallOptions {
        params: BTreeMap::from([("id".into(), "session-one".into())]),
        body: Some(json!({"prompt":"one"})),
        idempotency_key: Some("original-cancel".into()),
        // The original deadline outlives Retry-After. Only cancellation, not a
        // shortened deadline or an invalid replay, may terminate this wait.
        deadline: Some(SystemTime::now() + Duration::from_secs(120)),
        cancellation: cancellation.clone(),
        ..Default::default()
    };
    let error = client
        .call("session.message.send", options.clone())
        .await
        .unwrap_err();
    let Error::Api(previous) = error else {
        panic!("expected the actual retryable response")
    };
    assert_eq!(previous.retry_after_ms, Some(30_000));
    let listener = server.await.unwrap();
    let retry = client.retry_same_request("session.message.send", options, &previous);
    tokio::pin!(retry);
    tokio::select! {
        result = &mut retry => panic!("retry finished before cancellation: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(25)) => {},
    }
    cancellation.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), &mut retry).await,
        Ok(Err(Error::Cancelled))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err(),
        "cancelled retry made a second HTTP connection"
    );
}
