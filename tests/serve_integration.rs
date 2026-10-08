//! 显式真实宿主测试：普通单元池列明 ignored；xtask integration 必须执行而不能跳过。
mod support;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::time::Duration;
use tansr_sdk::{
    ApiClient, CancellationToken, Error,
    api::{ErrorCode, RetryAction},
    session::{
        Answer, Block, CheckpointOption, CompactOptions, CreateOptions, Input, InputContent,
        InputTarget, OutcomeStatus, Session, SessionClient, SessionEventStream, SpeechRequest,
        TranscriptionRequest, TurnTracker, WriteOptions,
    },
};

async fn wait_idle(session: &Session) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let meta = session.meta().await.unwrap();
            if meta.status == "idle" {
                return;
            }
            assert_eq!(meta.status, "running");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("kernel did not settle after its terminal event");
}

async fn wait_kind(stream: &mut SessionEventStream, kind: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let event = stream
                .next()
                .await
                .expect("EOF before expected event")
                .unwrap();
            if event.kind() == kind {
                return event.raw().clone();
            }
            assert!(
                event.turn_outcome().is_none(),
                "turn ended before {kind}: {:?}",
                event.turn_outcome()
            );
        }
    })
    .await
    .expect("expected event did not arrive")
}

async fn complete_turn(session: &Session, prompt: &str) {
    let floor = session.meta().await.unwrap().last_seq;
    let mut tracker = TurnTracker::new(floor).unwrap();
    let mut stream = session
        .events(Some(&floor.to_string()), CancellationToken::new())
        .await
        .unwrap();
    session.send(prompt, WriteOptions::default()).await.unwrap();
    finish_turn(&mut stream, &mut tracker, OutcomeStatus::Completed).await;
    stream.shutdown().await;
    wait_idle(session).await;
}

async fn finish_turn(
    stream: &mut SessionEventStream,
    tracker: &mut TurnTracker,
    status: OutcomeStatus,
) {
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let event = stream
                .next()
                .await
                .expect("EOF does not complete the turn")
                .unwrap();
            if let Some(outcome) = tracker.observe(&event) {
                assert_eq!(outcome.status, status);
                return;
            }
        }
    })
    .await
    .expect("turn did not produce a matching terminal");
}

fn reject_status(error: Error, expected: &[u16]) {
    let status = match &error {
        Error::Api(error) => error.status,
        Error::Domain { status, .. } => *status,
        _ => panic!("expected Serve rejection, got {error:?}"),
    };
    assert!(
        expected.contains(&status),
        "unexpected Serve rejection: {error:?}"
    );
}

fn reject_unmapped_input(error: Error, domain_code: &str, domain_status: u16) {
    let Error::Api(error) = error else {
        panic!("expected frozen unified rejection, got {error:?}");
    };
    // These original input codes are absent from the frozen error matrix.
    // Preserve the fallback, not an invented client-side classification;
    // internal_error must use HTTP 500 and retain the original domain status.
    assert_eq!(error.code, ErrorCode::InternalError);
    assert_eq!(error.status, 500);
    assert_eq!(error.retry_action, RetryAction::None);
    assert_eq!(error.detail["domainCode"], domain_code);
    assert_eq!(error.detail["domainStatus"], domain_status);
}

fn other_client(serve: &support::Serve) -> SessionClient {
    SessionClient::new(
        ApiClient::builder(serve.info["baseURL"].as_str().unwrap())
            .token("go-other-token")
            .session_family("sdk1")
            .build()
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires verified private Serve fixture; run xtask integration --require-serve"]
async fn real_serve_multiple_turns_history_resume_and_local_cancel() {
    let serve = support::Serve::start("session").await;
    let client = SessionClient::new(serve.client("sdk1")).unwrap();
    let session = client.create(CreateOptions::default()).await.unwrap();
    let resumed = client
        .resume(session.id(), WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(resumed.id(), session.id());
    // An already-live handle attaches with HTTP 200 and resumed:false. Only a
    // persisted session reconstructed by Serve has resumed:true.
    assert!(!resumed.created().resumed);
    let mut cursor = None;
    for prompt in ["RUST-FIRST", "RUST-SECOND"] {
        let meta = session.meta().await.unwrap();
        let mut tracker = TurnTracker::new(meta.last_seq).unwrap();
        let cancel = CancellationToken::new();
        let mut stream = session
            .events(cursor.as_deref(), cancel.clone())
            .await
            .unwrap();
        assert!(
            session
                .send(prompt, WriteOptions::default())
                .await
                .unwrap()
                .accepted
        );
        let mut text = String::new();
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let event = stream
                    .next()
                    .await
                    .expect("no turn terminal before EOF")
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
        .expect("turn timeout");
        assert!(text.contains("go-real-serve-answer"));
        cursor = stream.last_event_id().map(str::to_owned);
        stream.shutdown().await;
    }
    let attached = client.attach(session.id()).await.unwrap();
    assert_eq!(attached.id(), session.id());
    assert!(attached.meta().await.unwrap().last_seq > 0);
    let count = attached.history(0, 0).await.unwrap();
    assert!(count["messages"].as_array().unwrap().is_empty());
    assert!(count["total"].as_u64().unwrap() >= 4);
    let history = attached.history(0, 20).await.unwrap();
    assert_eq!(history["total"], count["total"]);
    assert!(history.to_string().contains("go-real-serve-answer"));
    assert_eq!(history["sessionId"], session.id());
    let cancellation = CancellationToken::new();
    let mut stream = attached
        .events(cursor.as_deref(), cancellation.clone())
        .await
        .unwrap();
    cancellation.cancel();
    assert!(matches!(
        stream.next().await,
        Some(Err(tansr_sdk::Error::Cancelled)) | None
    ));
    drop(stream);
    assert_eq!(attached.id(), session.id());
    session.close(WriteOptions::default()).await.unwrap();
    serve.stop().await;
}

#[tokio::test]
#[ignore = "requires verified private Serve fixture; run xtask integration --require-serve"]
async fn real_serve_explicit_interrupt_is_not_eof_success() {
    let serve = support::Serve::start("session").await;
    let client = SessionClient::new(serve.client("sdk1")).unwrap();
    let session = client.create(CreateOptions::default()).await.unwrap();
    let mut tracker = TurnTracker::new(session.meta().await.unwrap().last_seq).unwrap();
    let mut stream = session
        .events(None, CancellationToken::new())
        .await
        .unwrap();
    session
        .send("GO-BLOCK", WriteOptions::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let event = stream
                .next()
                .await
                .expect("stream ended before block")
                .unwrap();
            assert!(tracker.observe(&event).is_none());
            if event.kind() == "msg.text.delta" {
                break;
            }
        }
    })
    .await
    .expect("blocking turn startup");
    // The exact live target comes from Serve, not from locally guessed IDs.
    let capabilities = session.input_capabilities().await.unwrap();
    let target: InputTarget = serde_json::from_value(capabilities["target"].clone()).unwrap();
    assert_eq!(capabilities["durableAck"], false);
    // Disconnect and resume the running conversation. Replaying its old start
    // must recover the current identity without mistaking an old terminal for
    // this turn; the explicit trusted-ID tracker reaches the same result.
    let floor = session.meta().await.unwrap().last_seq;
    stream.shutdown().await;
    let resumed = client
        .resume(session.id(), WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(resumed.id(), session.id());
    stream = resumed
        .events(Some("0"), CancellationToken::new())
        .await
        .unwrap();
    tracker = TurnTracker::from_replay(floor).unwrap();
    let mut identity_tracker = TurnTracker::resume(floor, target.turn_id.clone()).unwrap();
    let input = Input {
        input_id: "rust-insert-original".into(),
        target: target.clone(),
        content: InputContent {
            text: Some("additional synthetic instruction".into()),
            blocks: None,
        },
        ack: Some("memory".into()),
    };
    let accepted = session
        .submit_input(input.clone(), WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(accepted["outcome"], "accepted");
    assert_eq!(accepted["receipt"]["durability"], "memory");
    assert_eq!(accepted["receipt"]["turnId"], target.turn_id);
    let status = session
        .input_status(&input.input_id, target.clone())
        .await
        .unwrap();
    assert_eq!(status["receipt"]["inputId"], input.input_id);
    let mut durable = input;
    durable.ack = Some("durable".into());
    assert!(matches!(
        session.submit_input(durable, WriteOptions::default()).await,
        Err(Error::InvalidInput(_))
    ));
    assert!(
        session
            .interrupt(WriteOptions::default())
            .await
            .unwrap()
            .accepted
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let event = stream.next().await.expect("missing abort").unwrap();
            let identity_outcome = identity_tracker.observe(&event);
            if let Some(outcome) = tracker.observe(&event) {
                assert_eq!(outcome.status, OutcomeStatus::Aborted);
                assert_eq!(identity_outcome.unwrap().status, OutcomeStatus::Aborted);
                break;
            }
        }
    })
    .await
    .expect("interrupt terminal timeout");
    stream.shutdown().await;
    // Query uses the same target even after the turn closes; it does not create
    // another turn or substitute a new input identity.
    let final_receipt = session
        .input_status("rust-insert-original", target)
        .await
        .unwrap();
    assert!(matches!(
        final_receipt["receipt"]["state"].as_str(),
        Some("closed" | "cancelled" | "consumed")
    ));
    session.close(WriteOptions::default()).await.unwrap();
    serve.stop().await;
}

#[tokio::test]
#[ignore = "requires verified private Serve fixture; run xtask integration --require-serve"]
async fn real_serve_snapshot_roundtrip_compaction_and_media_routes() {
    let serve = support::Serve::start("session").await;
    let client = SessionClient::new(serve.client("sdk1")).unwrap();
    let session = client.create(CreateOptions::default()).await.unwrap();
    // An empty compaction must remain a rejection, even with checkpoints wired.
    let compact = session
        .compact(CompactOptions::default(), WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(compact["status"], "rejected");
    assert!(matches!(
        compact["reason"].as_str(),
        Some("empty_history" | "not_configured")
    ));

    let mut tracker = TurnTracker::new(session.meta().await.unwrap().last_seq).unwrap();
    let mut stream = session
        .events(None, CancellationToken::new())
        .await
        .unwrap();
    session
        .send_blocks(
            vec![Block::Text {
                text: "RUST-SNAPSHOT-BLOCK".into(),
            }, Block::Image {
                mime: "image/png".into(),
                // One synthetic transparent pixel; no external content fetch.
                data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+y9k8AAAAASUVORK5CYII=".into(),
            }],
            WriteOptions::default(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let event = stream
                .next()
                .await
                .expect("missing block turn terminal")
                .unwrap();
            if let Some(outcome) = tracker.observe(&event) {
                assert_eq!(outcome.status, OutcomeStatus::Completed);
                break;
            }
        }
    })
    .await
    .expect("block turn timeout");
    stream.shutdown().await;
    wait_idle(&session).await;
    assert!(
        session
            .history(0, 20)
            .await
            .unwrap()
            .to_string()
            .contains("image/png")
    );
    let before = session.history(0, 0).await.unwrap();
    let checkpoint = session
        .checkpoint("原始快照", WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(checkpoint.message_count, before["total"].as_u64().unwrap());
    assert!(
        session
            .checkpoints()
            .await
            .unwrap()
            .iter()
            .any(|item| item.checkpoint_id == checkpoint.checkpoint_id)
    );
    let original = session
        .export_checkpoint(&checkpoint.checkpoint_id)
        .await
        .unwrap();
    let exported: serde_json::Value = serde_json::from_slice(&original).unwrap();
    assert!(
        exported["checkpoint"]["messages"]
            .to_string()
            .contains("go-real-serve-answer")
    );
    // Pass the original export byte vector directly, not reserialized JSON.
    let imported = session
        .import_checkpoint(original, "导入副本", WriteOptions::default())
        .await
        .unwrap();
    assert_ne!(imported.checkpoint_id, checkpoint.checkpoint_id);
    assert_eq!(imported.message_count, checkpoint.message_count);
    let reexported: serde_json::Value = serde_json::from_slice(
        &session
            .export_checkpoint(&imported.checkpoint_id)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        reexported["checkpoint"]["messages"],
        exported["checkpoint"]["messages"]
    );
    assert_eq!(
        session.history(0, 0).await.unwrap()["total"],
        before["total"],
        "import must not rewrite the running session history"
    );
    let restored = session
        .restore(&imported.checkpoint_id, false, WriteOptions::default())
        .await
        .unwrap();
    assert_eq!(restored["status"], "restored");
    assert_eq!(restored["checkpointId"], imported.checkpoint_id);
    assert_eq!(restored["toMessages"], before["total"]);
    session
        .delete_checkpoint(&imported.checkpoint_id, WriteOptions::default())
        .await
        .unwrap();
    assert!(
        !session
            .checkpoints()
            .await
            .unwrap()
            .iter()
            .any(|item| item.checkpoint_id == imported.checkpoint_id)
    );
    assert!(
        session
            .export_checkpoint(&imported.checkpoint_id)
            .await
            .is_err()
    );
    assert!(
        session
            .restore("missing", false, WriteOptions::default())
            .await
            .is_err()
    );
    assert!(
        session
            .import_checkpoint(b"invalid-snapshot".to_vec(), "", WriteOptions::default())
            .await
            .is_err()
    );

    let after_snapshot = session.history(0, 0).await.unwrap();
    let transcript = session
        .transcribe(
            TranscriptionRequest {
                audio: "data:audio/wav;base64,UklGRi1ydXN0LXN5bnRoZXRpYy1hdWRpbw==".into(),
                language: Some("en".into()),
                ..TranscriptionRequest::default()
            },
            WriteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(transcript["text"], "rust synthetic transcript");
    let speech = session
        .speak(
            SpeechRequest {
                input: "one".into(),
                format: Some("wav".into()),
                ..SpeechRequest::default()
            },
            WriteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(speech["audio"]["mime"], "audio/wav");
    assert_eq!(
        speech["audio"]["b64"],
        "UklGRi1ydXN0LXN5bnRoZXRpYy1hdWRpbw=="
    );
    assert_eq!(
        session.history(0, 0).await.unwrap()["total"],
        after_snapshot["total"],
        "media requests must not silently send new user messages"
    );
    let compact = session
        .compact(
            CompactOptions {
                checkpoint: Some(CheckpointOption::Enabled(true)),
                ..CompactOptions::default()
            },
            WriteOptions::default(),
        )
        .await
        .unwrap();
    assert!(matches!(
        compact["status"].as_str(),
        Some("compacted" | "rejected" | "failed")
    ));
    if compact["status"] != "compacted" {
        assert!(
            compact["reason"]
                .as_str()
                .is_some_and(|reason| !reason.is_empty()),
            "non-success compaction must retain its reason"
        );
    }
    session.close(WriteOptions::default()).await.unwrap();
    serve.stop().await;
}

#[tokio::test]
#[ignore = "requires verified private Serve fixture; run xtask integration --require-serve"]
async fn real_serve_persisted_resume_concurrent_close_gap_and_owner_isolation() {
    let serve = support::Serve::start("session").await;
    let client = SessionClient::new(serve.client("sdk1")).unwrap();
    let session = client.create(CreateOptions::default()).await.unwrap();
    complete_turn(&session, "RUST-PERSISTED original history").await;
    let history = session.history(0, 20).await.unwrap();
    let checkpoint = session
        .checkpoint("owner-only", WriteOptions::default())
        .await
        .unwrap();
    let other = other_client(&serve);
    assert!(other.attach(session.id()).await.is_err());
    assert!(
        other
            .resume(session.id(), WriteOptions::default())
            .await
            .is_err()
    );
    let export = other
        .api()
        .call(
            "session.checkpoint.export",
            tansr_sdk::CallOptions {
                params: std::collections::BTreeMap::from([
                    ("id".into(), session.id().into()),
                    ("targetId".into(), checkpoint.checkpoint_id),
                ]),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    reject_status(export, &[403]);
    assert_eq!(
        other.list(0, 20).await.unwrap().total,
        0,
        "rejected resume must not create a replacement"
    );

    let floor = session.meta().await.unwrap().last_seq;
    let mut ending = session
        .events(Some(&floor.to_string()), CancellationToken::new())
        .await
        .unwrap();
    let (one, two) = tokio::join!(
        session.close(WriteOptions::default()),
        session.close(WriteOptions::default())
    );
    assert!(one.unwrap().accepted && two.unwrap().accepted);
    wait_kind(&mut ending, "session.ended").await;
    ending.shutdown().await;
    // Two cold resumes share one reconstruction, unlike the already-live 200
    // attach covered above. The session ID and stored history remain exact.
    let (first, second) = tokio::join!(
        client.resume(session.id(), WriteOptions::default()),
        client.resume(session.id(), WriteOptions::default())
    );
    let (first, second) = (first.unwrap(), second.unwrap());
    assert_eq!(first.id(), session.id());
    assert_eq!(second.id(), session.id());
    assert_ne!(first.created().resumed, second.created().resumed);
    assert_eq!(
        first.history(0, 20).await.unwrap()["messages"],
        history["messages"]
    );
    assert_eq!(client.list(0, 20).await.unwrap().total, 1);

    let mut gap = first
        .events(Some("9007199254740991"), CancellationToken::new())
        .await
        .unwrap();
    let notice = tokio::time::timeout(Duration::from_secs(10), gap.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(notice.kind(), "server.replay.gap");
    assert_eq!(notice.raw()["reason"], "ahead_of_log");
    assert!(notice.envelope.event_id.is_none());
    let mut tracker = TurnTracker::from_replay(floor).unwrap();
    assert!(tracker.observe(&notice).is_none());
    assert!(tracker.needs_reconciliation());
    gap.shutdown().await;
    // Reconcile explicitly through metadata/history, then open a new tracker.
    assert_eq!(
        first.history(0, 0).await.unwrap()["total"],
        history["total"]
    );
    complete_turn(&first, "RUST-PERSISTED second turn after reconstruction").await;
    let retained_history = first.history(0, 30).await.unwrap();
    let mut sealing = first
        .events(
            Some(&first.meta().await.unwrap().last_seq.to_string()),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    first.close(WriteOptions::default()).await.unwrap();
    wait_kind(&mut sealing, "session.ended").await;
    // This watermark has actually been delivered and handled, including the
    // final seal. Persist application state independently of the HTTP parser.
    let local = tempfile::tempdir().unwrap();
    let resume_file = local.path().join("resume.json");
    std::fs::write(
        &resume_file,
        serde_json::to_vec(&json!({
            "sessionId": first.id(), "processedCursor": sealing.last_event_id().unwrap(),
        }))
        .unwrap(),
    )
    .unwrap();
    sealing.shutdown().await;

    // A separate Node process reopens the same physical store. None of the
    // old Session/ApiClient handles, in-memory registry or listener is reused.
    let serve = serve.restart("session").await;
    let recovered_state: Value =
        serde_json::from_slice(&std::fs::read(resume_file).unwrap()).unwrap();
    let id = recovered_state["sessionId"].as_str().unwrap();
    let processed = recovered_state["processedCursor"].as_str().unwrap();
    let processed_sequence = processed.parse::<u64>().unwrap();
    let fresh_client = SessionClient::new(serve.client("sdk1")).unwrap();
    let dormant = fresh_client.attach(id).await.unwrap();
    assert!(
        !dormant.meta().await.unwrap().live,
        "new process must expose stored, not live, metadata"
    );
    let fresh = fresh_client
        .resume(id, WriteOptions::default())
        .await
        .unwrap();
    assert!(fresh.created().resumed);
    assert_eq!(fresh.id(), id);
    assert!(
        fresh.created().last_seq > processed_sequence,
        "fresh process must not reset the original event sequence"
    );
    assert_eq!(
        fresh.history(0, 30).await.unwrap()["messages"],
        retained_history["messages"]
    );
    let mut replay = fresh
        .events(Some(processed), CancellationToken::new())
        .await
        .unwrap();
    let resumed_event = tokio::time::timeout(Duration::from_secs(10), replay.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_ne!(
        resumed_event.kind(),
        "server.replay.gap",
        "the sealed processed cursor must resume continuously"
    );
    assert!(
        resumed_event
            .envelope
            .event_id
            .as_deref()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > processed_sequence
    );
    replay.shutdown().await;
    complete_turn(
        &fresh,
        "RUST-PERSISTED third turn after independent process restart",
    )
    .await;
    assert!(
        fresh.history(0, 0).await.unwrap()["total"]
            .as_u64()
            .unwrap()
            > retained_history["total"].as_u64().unwrap()
    );
    assert_eq!(fresh_client.list(0, 20).await.unwrap().total, 1);
    fresh.close(WriteOptions::default()).await.unwrap();
    serve.stop().await;
}

#[tokio::test]
#[ignore = "requires verified private Serve fixture; run xtask integration --require-serve"]
async fn real_serve_same_turn_duplicates_conflicts_consumption_and_epoch_reset() {
    let mut serve = support::Serve::start("session").await;
    let session = SessionClient::new(serve.client("sdk1"))
        .unwrap()
        .create(CreateOptions::default())
        .await
        .unwrap();
    complete_turn(&session, "RUST-BEFORE-INPUT").await;
    let checkpoint = session
        .checkpoint("pre-input-generation", WriteOptions::default())
        .await
        .unwrap();
    let floor = session.meta().await.unwrap().last_seq;
    let mut stream = session
        .events(Some(&floor.to_string()), CancellationToken::new())
        .await
        .unwrap();
    session
        .send("GO-BLOCK", WriteOptions::default())
        .await
        .unwrap();
    wait_kind(&mut stream, "msg.text.delta").await;
    let target: InputTarget =
        serde_json::from_value(session.input_capabilities().await.unwrap()["target"].clone())
            .unwrap();
    let mut tracker = TurnTracker::resume(floor, target.turn_id.clone()).unwrap();
    let input = Input {
        input_id: "rust-concurrent-original".into(),
        target: target.clone(),
        content: InputContent {
            text: Some("RUST-INSERTED actual consumption".into()),
            blocks: None,
        },
        ack: Some("memory".into()),
    };
    let (first, duplicate) = tokio::join!(
        session.submit_input(input.clone(), WriteOptions::default()),
        session.submit_input(input.clone(), WriteOptions::default())
    );
    let first = first.unwrap();
    assert_eq!(first, duplicate.unwrap());
    assert_eq!(
        first["receipt"]["state"], "accepted",
        "acceptance is not model consumption"
    );
    let mut conflicting = input.clone();
    conflicting.content.text = Some("altered intent".into());
    reject_unmapped_input(
        session
            .submit_input(conflicting, WriteOptions::default())
            .await
            .unwrap_err(),
        "input_conflict",
        409,
    );
    let mut wrong_turn = input.clone();
    wrong_turn.input_id = "wrong-turn".into();
    wrong_turn.target.turn_id = "not-the-active-turn".into();
    reject_unmapped_input(
        session
            .submit_input(wrong_turn, WriteOptions::default())
            .await
            .unwrap_err(),
        "turn_mismatch",
        409,
    );
    let mut non_text = input.clone();
    non_text.input_id = "image-insertion".into();
    non_text.content = InputContent {
        text: None,
        blocks: Some(vec![Block::Image {
            mime: "image/png".into(),
            data: "AA==".into(),
        }]),
    };
    assert!(matches!(
        session
            .submit_input(non_text, WriteOptions::default())
            .await,
        Err(Error::InvalidInput(_))
    ));
    serve
        .send_control(&json!({"command":"release-model","requestId":"release-input"}))
        .await;
    finish_turn(&mut stream, &mut tracker, OutcomeStatus::Completed).await;
    stream.shutdown().await;
    wait_idle(&session).await;
    let consumed = session
        .input_status(&input.input_id, target.clone())
        .await
        .unwrap();
    assert_eq!(consumed["receipt"]["state"], "consumed");
    assert!(
        session
            .history(0, 30)
            .await
            .unwrap()
            .to_string()
            .contains("RUST-INSERTED actual consumption")
    );
    assert_eq!(
        session
            .submit_input(input.clone(), WriteOptions::default())
            .await
            .unwrap()["receipt"],
        consumed["receipt"],
        "same intent retains the original receipt after its turn ended"
    );
    let mut late = input.clone();
    late.input_id = "late-new-input".into();
    reject_unmapped_input(
        session
            .submit_input(late, WriteOptions::default())
            .await
            .unwrap_err(),
        "turn_closed",
        409,
    );
    let count_before = session.history(0, 0).await.unwrap()["total"].clone();
    session
        .restore(&checkpoint.checkpoint_id, false, WriteOptions::default())
        .await
        .unwrap();
    reject_unmapped_input(
        session
            .input_status(&input.input_id, target.clone())
            .await
            .unwrap_err(),
        "input_not_found",
        404,
    );
    reject_unmapped_input(
        session
            .submit_input(input, WriteOptions::default())
            .await
            .unwrap_err(),
        "epoch_mismatch",
        409,
    );
    assert!(
        session.history(0, 0).await.unwrap()["total"]
            .as_u64()
            .unwrap()
            < count_before.as_u64().unwrap()
    );
    assert_eq!(
        session.meta().await.unwrap().status,
        "idle",
        "rejected old-generation input must never start a turn"
    );
    session.close(WriteOptions::default()).await.unwrap();
    serve.stop().await;
}

#[tokio::test]
#[ignore = "requires verified private Serve fixture; run xtask integration --require-serve"]
async fn real_serve_questions_original_ticket_wrong_answers_principal_and_revocation() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let mut serve = support::Serve::start("session").await;
    let switched = Arc::new(AtomicBool::new(false));
    let provider_flag = switched.clone();
    let api = ApiClient::builder(serve.info["baseURL"].as_str().unwrap())
        .session_family("sdk1")
        .token_provider(Arc::new(move || {
            let token = if provider_flag.load(Ordering::SeqCst) {
                "go-other-token"
            } else {
                "go-integration-token"
            };
            Box::pin(async move { Ok(token.to_owned()) })
        }))
        .build()
        .unwrap();
    let client = SessionClient::new(api).unwrap();
    let session = client.create(CreateOptions::default()).await.unwrap();
    let floor = session.meta().await.unwrap().last_seq;
    let mut tracker = TurnTracker::new(floor).unwrap();
    let mut stream = session
        .events(Some(&floor.to_string()), CancellationToken::new())
        .await
        .unwrap();
    session
        .send(
            "GO-QUESTION model text says approved, but only the user may answer",
            WriteOptions::default(),
        )
        .await
        .unwrap();
    let request = wait_kind(&mut stream, "server.question.request").await;
    let ticket = request["requestId"].as_str().unwrap();
    let question = &request["questions"][0];
    let answer = Answer {
        question_id: question["id"].as_str().unwrap().into(),
        selected_option_ids: vec![question["options"][0]["id"].as_str().unwrap().into()],
        free_text: None,
    };
    let mut wrong_answer = answer.clone();
    wrong_answer.selected_option_ids = vec!["invented-option".into()];
    reject_status(
        session
            .answer(ticket, vec![wrong_answer], WriteOptions::default())
            .await
            .unwrap_err(),
        &[400],
    );
    reject_status(
        session
            .answer(
                "wrong-ticket",
                vec![answer.clone()],
                WriteOptions::default(),
            )
            .await
            .unwrap_err(),
        &[404],
    );
    assert_eq!(session.meta().await.unwrap().status, "running");
    // Even the same client object cannot reuse its old closure after a token
    // provider switches principal. The request is rejected before ticket use.
    switched.store(true, Ordering::SeqCst);
    let error = session
        .answer(ticket, vec![answer.clone()], WriteOptions::default())
        .await
        .unwrap_err();
    let Error::Api(error) = error else {
        panic!("expected hidden foreign session, got {error:?}");
    };
    // Discovery hides a foreign session before the original ticket is used.
    assert_eq!(error.code, ErrorCode::NotFound);
    assert_eq!(error.status, 404);
    assert_eq!(error.retry_action, RetryAction::None);
    switched.store(false, Ordering::SeqCst);
    serve
        .send_control(
            &json!({"command":"set-auth","requestId":"revoke-pending-question","allowed":false}),
        )
        .await;
    reject_status(
        session
            .answer(ticket, vec![answer.clone()], WriteOptions::default())
            .await
            .unwrap_err(),
        &[401],
    );
    serve
        .send_control(
            &json!({"command":"set-auth","requestId":"restore-question-auth","allowed":true}),
        )
        .await;
    assert!(
        session
            .answer(ticket, vec![answer.clone()], WriteOptions::default())
            .await
            .unwrap()
            .accepted
    );
    // The tracker missed turn.started while inspecting the ticket, so recover
    // its authoritative running turn identity through the original replay.
    stream.shutdown().await;
    stream = session
        .events(Some(&floor.to_string()), CancellationToken::new())
        .await
        .unwrap();
    finish_turn(&mut stream, &mut tracker, OutcomeStatus::Completed).await;
    stream.shutdown().await;
    wait_idle(&session).await;
    reject_status(
        session
            .answer(ticket, vec![answer.clone()], WriteOptions::default())
            .await
            .unwrap_err(),
        &[409],
    );
    let second = client.create(CreateOptions::default()).await.unwrap();
    reject_status(
        second
            .answer(ticket, vec![answer], WriteOptions::default())
            .await
            .unwrap_err(),
        &[404],
    );
    session.close(WriteOptions::default()).await.unwrap();
    second.close(WriteOptions::default()).await.unwrap();
    serve.stop().await;
}

#[tokio::test]
#[ignore = "requires verified private Serve fixture; run xtask integration --require-serve"]
async fn real_serve_disconnected_question_expires_instead_of_becoming_approval() {
    let serve = support::Serve::start("session").await;
    let session = SessionClient::new(serve.client("sdk1"))
        .unwrap()
        .create(CreateOptions::default())
        .await
        .unwrap();
    let mut stream = session
        .events(None, CancellationToken::new())
        .await
        .unwrap();
    session
        .send("GO-QUESTION", WriteOptions::default())
        .await
        .unwrap();
    let request = wait_kind(&mut stream, "server.question.request").await;
    stream.shutdown().await;
    tokio::time::sleep(Duration::from_millis(750)).await;
    let question = &request["questions"][0];
    let answer = Answer {
        question_id: question["id"].as_str().unwrap().into(),
        selected_option_ids: vec![question["options"][0]["id"].as_str().unwrap().into()],
        free_text: None,
    };
    reject_status(
        session
            .answer(
                request["requestId"].as_str().unwrap(),
                vec![answer],
                WriteOptions::default(),
            )
            .await
            .unwrap_err(),
        &[410],
    );
    let history = session.history(0, 20).await.unwrap();
    assert!(!history.to_string().contains("invented-option"));
    session.close(WriteOptions::default()).await.unwrap();
    serve.stop().await;
}

#[tokio::test]
#[ignore = "requires verified private Serve fixture; run xtask integration --require-serve"]
async fn real_serve_media_disabled_does_not_grant_permissions_from_request_configuration() {
    let serve = support::Serve::start("session-media-disabled").await;
    let session = SessionClient::new(serve.client("sdk1"))
        .unwrap()
        .create(CreateOptions::default())
        .await
        .unwrap();
    complete_turn(&session, "text remains available").await;
    let before = session.history(0, 0).await.unwrap()["total"].clone();
    let transcription = session
        .transcribe(
            TranscriptionRequest {
                audio: "data:audio/wav;base64,UklGRg==".into(),
                ..Default::default()
            },
            WriteOptions::default(),
        )
        .await;
    assert!(
        transcription.is_err(),
        "disabled ASR returned synthetic success"
    );
    let speech = session
        .speak(
            SpeechRequest {
                input: "synthetic speech".into(),
                ..Default::default()
            },
            WriteOptions::default(),
        )
        .await;
    assert!(speech.is_err(), "disabled TTS returned synthetic success");
    assert_eq!(
        session.history(0, 0).await.unwrap()["total"],
        before,
        "speech failures must not insert messages"
    );
    let floor = session.meta().await.unwrap().last_seq;
    let mut tracker = TurnTracker::new(floor).unwrap();
    let mut stream = session
        .events(Some(&floor.to_string()), CancellationToken::new())
        .await
        .unwrap();
    // A syntactically valid image may be accepted by the session route before
    // the provider capability gate rejects it. Its 202 is not task success.
    assert!(session.send_blocks(vec![Block::Image {
        mime: "image/png".into(),
        data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+y9k8AAAAASUVORK5CYII=".into(),
    }], WriteOptions::default()).await.unwrap().accepted);
    finish_turn(&mut stream, &mut tracker, OutcomeStatus::Failed).await;
    stream.shutdown().await;
    session.close(WriteOptions::default()).await.unwrap();
    serve.stop().await;
}

/// Forward every request to the actual private Serve. Only the successful
/// mutation response is discarded; the test never fabricates acceptance.
async fn mutation_response_loss_proxy(
    base: &str,
    suffix: &'static str,
    cancel: Option<CancellationToken>,
) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let base = base.to_owned();
    let task = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(20), async move {
            let upstream = reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap();
            let mut paths = Vec::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let (end, length) = loop {
                    let mut chunk = [0; 4096];
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert_ne!(n, 0);
                    bytes.extend_from_slice(&chunk[..n]);
                    assert!(bytes.len() <= 65536);
                    if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                        let head = std::str::from_utf8(&bytes[..end]).unwrap();
                        let length = head
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        break (end + 4, length);
                    }
                };
                while bytes.len() < end + length {
                    let mut chunk = [0; 4096];
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert_ne!(n, 0);
                    bytes.extend_from_slice(&chunk[..n]);
                }
                let headers = std::str::from_utf8(&bytes[..end]).unwrap();
                let mut lines = headers.lines();
                let start = lines.next().unwrap();
                let mut request_line = start.split_whitespace();
                let method =
                    reqwest::Method::from_bytes(request_line.next().unwrap().as_bytes()).unwrap();
                let path = request_line.next().unwrap();
                assert!(path.starts_with("/api/") && !path.contains("://"));
                paths.push(start.to_owned());
                let discard = path.ends_with(suffix);
                let mut request = upstream.request(method, format!("{base}{path}"));
                for line in lines.filter(|line| !line.is_empty()) {
                    let (name, value) = line.split_once(':').unwrap();
                    if !name.eq_ignore_ascii_case("host")
                        && !name.eq_ignore_ascii_case("connection")
                    {
                        request = request.header(name, value.trim());
                    }
                }
                let response = request.body(bytes[end..].to_vec()).send().await.unwrap();
                let status = response.status();
                let headers = response.headers().clone();
                let body = response.bytes().await.unwrap();
                assert!(
                    status.is_success(),
                    "real route rejected request: {status} {}",
                    String::from_utf8_lossy(&body)
                );
                if discard {
                    assert_eq!(
                        serde_json::from_slice::<Value>(&body).unwrap()["accepted"],
                        true
                    );
                    if let Some(cancel) = &cancel {
                        cancel.cancel();
                    }
                    socket.shutdown().await.unwrap();
                    return paths;
                }
                let mut head = format!(
                    "HTTP/1.1 {} OK\r\nConnection: close\r\nContent-Length: {}\r\n",
                    status.as_u16(),
                    body.len()
                );
                for (name, value) in &headers {
                    if !matches!(
                        name.as_str(),
                        "connection" | "content-length" | "transfer-encoding"
                    ) {
                        head.push_str(&format!("{name}: {}\r\n", value.to_str().unwrap()));
                    }
                }
                head.push_str("\r\n");
                socket.write_all(head.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            }
        })
        .await
        .expect("interrupt loss proxy timed out")
    });
    (endpoint, task)
}

#[tokio::test]
#[ignore = "requires verified private Serve fixture; run xtask integration --require-serve"]
async fn real_serve_local_drop_and_lost_interrupt_response_require_original_turn_reconciliation() {
    let serve = support::Serve::start("session").await;
    let session = SessionClient::new(serve.client("sdk1"))
        .unwrap()
        .create(CreateOptions::default())
        .await
        .unwrap();
    let mut observing = session
        .events(None, CancellationToken::new())
        .await
        .unwrap();
    session
        .send("GO-BLOCK", WriteOptions::default())
        .await
        .unwrap();
    wait_kind(&mut observing, "msg.text.delta").await;
    let target: InputTarget =
        serde_json::from_value(session.input_capabilities().await.unwrap()["target"].clone())
            .unwrap();
    let floor = session.meta().await.unwrap().last_seq;
    drop(observing);
    assert_eq!(
        session.meta().await.unwrap().status,
        "running",
        "dropping an observation must not interrupt Serve"
    );
    let mut stream = session
        .events(Some(&floor.to_string()), CancellationToken::new())
        .await
        .unwrap();
    let mut tracker = TurnTracker::resume(floor, target.turn_id).unwrap();
    let (proxy, forwarded) =
        mutation_response_loss_proxy(serve.info["baseURL"].as_str().unwrap(), "/interrupt", None)
            .await;
    let losing_client = SessionClient::new(
        ApiClient::builder(proxy)
            .token("go-integration-token")
            .session_family("sdk1")
            .build()
            .unwrap(),
    )
    .unwrap();
    let same = losing_client.attach(session.id()).await.unwrap();
    let error = same
        .interrupt(WriteOptions {
            idempotency_key: Some("original-interrupt-request".into()),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Transport(_)),
        "lost response must retain unknown outcome: {error:?}"
    );
    let paths = forwarded.await.unwrap();
    assert_eq!(
        paths.iter().filter(|p| p.starts_with("POST ")).count(),
        1,
        "no hidden retry or recreation"
    );
    assert!(
        paths
            .iter()
            .any(|p| p.contains(&format!("/sessions/{}/interrupt", session.id())))
    );
    finish_turn(&mut stream, &mut tracker, OutcomeStatus::Aborted).await;
    stream.shutdown().await;
    wait_idle(&session).await;
    assert_eq!(same.id(), session.id());
    session.close(WriteOptions::default()).await.unwrap();
    serve.stop().await;
}

#[tokio::test]
#[ignore = "requires verified private Serve fixture; run xtask integration --require-serve"]
async fn real_serve_send_cancel_race_preserves_committed_turn_without_retry() {
    let serve = support::Serve::start("session").await;
    let session = SessionClient::new(serve.client("sdk1"))
        .unwrap()
        .create(CreateOptions::default())
        .await
        .unwrap();
    let mut stream = session
        .events(None, CancellationToken::new())
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    let (proxy, forwarded) = mutation_response_loss_proxy(
        serve.info["baseURL"].as_str().unwrap(),
        "/messages",
        Some(cancel.clone()),
    )
    .await;
    let client = SessionClient::new(
        ApiClient::builder(proxy)
            .token("go-integration-token")
            .session_family("sdk1")
            .build()
            .unwrap(),
    )
    .unwrap();
    let original = client.attach(session.id()).await.unwrap();
    let error = original
        .send(
            "GO-BLOCK",
            WriteOptions {
                idempotency_key: Some("original-send-id".into()),
                cancellation: cancel,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Cancelled | Error::Transport(_)),
        "committed request cannot be reported as rolled back: {error:?}"
    );
    assert_eq!(
        forwarded
            .await
            .unwrap()
            .iter()
            .filter(|p| p.starts_with("POST "))
            .count(),
        1
    );
    wait_kind(&mut stream, "msg.text.delta").await;
    assert_eq!(session.meta().await.unwrap().status, "running");
    let target: InputTarget =
        serde_json::from_value(session.input_capabilities().await.unwrap()["target"].clone())
            .unwrap();
    let mut tracker =
        TurnTracker::resume(session.meta().await.unwrap().last_seq, target.turn_id).unwrap();
    session.interrupt(WriteOptions::default()).await.unwrap();
    finish_turn(&mut stream, &mut tracker, OutcomeStatus::Aborted).await;
    stream.shutdown().await;
    session.close(WriteOptions::default()).await.unwrap();
    serve.stop().await;
}

#[tokio::test]
#[ignore = "requires verified private Serve fixture; run xtask integration --require-serve"]
async fn real_serve_evicted_cursor_requires_explicit_reconciliation() {
    let serve = support::Serve::start("session-gap").await;
    let session = SessionClient::new(serve.client("sdk1"))
        .unwrap()
        .create(CreateOptions::default())
        .await
        .unwrap();
    complete_turn(&session, "first real turn evicts its oldest events").await;
    let floor = session.meta().await.unwrap().last_seq;
    let mut tracker = TurnTracker::from_replay(floor).unwrap();
    let mut replay = session
        .events(Some("0"), CancellationToken::new())
        .await
        .unwrap();
    let gap = tokio::time::timeout(Duration::from_secs(10), replay.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(gap.kind(), "server.replay.gap");
    assert_eq!(gap.raw()["reason"], "evicted");
    assert!(tracker.observe(&gap).is_none());
    assert!(tracker.needs_reconciliation());
    let retained = tokio::time::timeout(Duration::from_secs(10), replay.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        tracker.observe(&retained).is_none(),
        "retained old terminal must not repair a gapped tracker"
    );
    replay.shutdown().await;
    assert!(
        session.history(0, 0).await.unwrap()["total"]
            .as_u64()
            .unwrap()
            >= 2
    );
    complete_turn(&session, "explicitly reconciled second turn").await;
    session.close(WriteOptions::default()).await.unwrap();
    serve.stop().await;
}
