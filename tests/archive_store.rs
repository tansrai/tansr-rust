use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tansr_sdk::{api::Error, archive::*, canonical};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn fixture() -> (Binding, Status, Page, BTreeMap<String, Vec<u8>>) {
    let target = json!({"sessionId":"opaque-session 中文","generations":{"historyEpoch":"h-1","deletionGeneration":"0","projectionRevision":"9007199254740993"},"sourceSnapshotDigest":"0".repeat(64)});
    let binding:Binding=serde_json::from_value(json!({"protocol":PROTOCOL,"bindingId":"binding-1","scope":{"applicationScopeId":"app-1","endUserId":"user-1","authorizationRevision":"1"},"target":target,"revision":"1","state":"active","sourceId":"source-1","acceptedCapabilities":["archive-transfer-v1","context-materials-v1"],"rejectedCapabilities":[],"availability":"legacy-complete","operationEpoch":{"id":"epoch-1","issuedAt":"2030-01-01T00:00:00.000Z","expiresAt":"2030-01-01T00:01:00.000Z","state":"active"},"archiveAckFormat":"split-receipts-v1","limits":{"controlBytes":262144,"recordBytes":262144,"pageRecords":128,"pageBytes":1048576,"attachmentBytes":33554432,"chunkBytes":1024,"materialConcurrent":2,"materialQueue":16,"materialCandidates":32,"materialBytes":1048576,"materialDeadlineMs":30000,"pendingRecords":4096,"pendingBytes":67108864,"inflightReserveBytes":1048576,"offlineMs":1000,"eventRetentionMs":1000,"eventRetentionFrames":1,"eventRetentionBytes":1024,"terminalReceiptRetentionMs":60000,"epochLifetimeMs":60000,"materialChunkBytes":65536}})).unwrap();
    let body = br#"{"role":"user","text":"synthetic-secret-body"}"#.to_vec();
    let reference = json!({"artifactId":"artifact-1","sourceId":"source-1","bytes":body.len(),"sha256":sha(&body),"mediaType":"application/json"});
    let mut record = json!({"recordId":"record-1","sequence":"1","target":target,"turnId":"turn-1","recordKind":"turn","turnState":"completed","predecessorDigest":"0".repeat(64),"payload":reference,"attachments":[],"payloadDigest":canonical::digest_bytes("tansr.sdk2.payload.v1",&body).unwrap()});
    let digest = canonical::digest("tansr.sdk2.record.v1", &record).unwrap();
    record["recordDigest"] = digest.into();
    let status:Status=serde_json::from_value(json!({"protocol":PROTOCOL,"bindingId":"binding-1","revision":"1","generations":target["generations"],"sourceId":"source-1","sourceGeneration":"source-generation-1","publishedThroughSequence":"1","acknowledgedCoverage":null,"releasableThroughSequence":null,"pendingBytes":body.len(),"pendingRecords":1,"sessionPersistence":"unchanged","state":"active"})).unwrap();
    let page:Page=serde_json::from_value(json!({"protocol":PROTOCOL,"bindingId":"binding-1","generations":target["generations"],"records":[record],"nextAfterSequence":"1","complete":true,"publishedThroughSequence":"1"})).unwrap();
    (
        binding,
        status,
        page,
        BTreeMap::from([("artifact-1".into(), body)]),
    )
}
fn receipt(identity: &Identity, ack: &Ack) -> MutationReceipt {
    let mut semantic = serde_json::to_value(ack).unwrap();
    semantic.as_object_mut().unwrap().remove("request");
    MutationReceipt{protocol:PROTOCOL.into(),request:ack.request.clone(),binding_id:ack.binding_id.clone(),operation:"archive-ack".into(),semantic_digest:canonical::digest("tansr.sdk2.operation.v1",&json!({"scope":[identity.application_scope_id,identity.end_user_id],"operation":"archive-ack","semantic":semantic})).unwrap(),state:"completed".into(),revision:(ack.expected_revision.parse::<u64>().unwrap()+1).to_string(),outcome_ref:"receipt-1".into()}
}
fn attachment_fixture() -> (Binding, Status, Page, BTreeMap<String, Vec<u8>>) {
    let (binding, mut status, mut page, mut bodies) = fixture();
    let bytes = (0..2048).map(|n| (n % 256) as u8).collect::<Vec<_>>();
    page.records[0].attachments.push(ArtifactRef {
        artifact_id: "attachment-1".into(),
        source_id: binding.source_id.clone(),
        bytes: bytes.len(),
        sha256: sha(&bytes),
        media_type: "application/octet-stream".into(),
    });
    let mut value = serde_json::to_value(&page.records[0]).unwrap();
    value.as_object_mut().unwrap().remove("recordDigest");
    page.records[0].record_digest = canonical::digest("tansr.sdk2.record.v1", &value).unwrap();
    status.pending_bytes += bytes.len();
    bodies.insert("attachment-1".into(), bytes);
    (binding, status, page, bodies)
}
fn options(path: &Path, id: Identity) -> StoreOptions {
    StoreOptions {
        path: path.into(),
        key: [7; 32],
        identity: id,
        limits: StoreLimits::default(),
        check_access: Arc::new(|_| Ok(())),
    }
}
fn path(temp: &tempfile::TempDir) -> std::path::PathBuf {
    let root = temp.path().canonicalize().unwrap().join("private");
    create_private_directory(&root).unwrap();
    root.join("archive.bin")
}
#[tokio::test]
async fn archive_encrypted_pending_ack_reopens_and_confirms_exact_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let path = path(&temp);
    let (b, s, p, bodies) = fixture();
    let id = Identity::from_binding(&b, &s).unwrap();
    let store = FileStore::open(options(&path, id.clone())).await.unwrap();
    let ack = store
        .receive(
            &b,
            &s,
            &p,
            bodies.clone(),
            RequestIdentity {
                request_id: "request-1".into(),
                operation_epoch: "epoch-1".into(),
            },
        )
        .await
        .unwrap();
    assert!(store.coverage().await.unwrap().is_none());
    let blob = std::fs::read(&path).unwrap();
    assert!(!blob.windows(11).any(|b| b == b"secret-body"));
    assert!(FileStore::open(options(&path, id.clone())).await.is_err());
    store.close().await.unwrap();
    let store = FileStore::open(options(&path, id.clone())).await.unwrap();
    assert_eq!(store.pending().await.unwrap(), Some(ack.clone()));
    let mut bad = receipt(&id, &ack);
    bad.state = "accepted".into();
    assert!(store.confirm(bad).await.is_err());
    let mut bad = receipt(&id, &ack);
    bad.semantic_digest = "f".repeat(64);
    assert!(store.confirm(bad).await.is_err());
    let good = receipt(&id, &ack);
    store.confirm(good.clone()).await.unwrap();
    store.confirm(good).await.unwrap();
    assert_eq!(store.coverage().await.unwrap(), Some(ack.coverage));
    assert_eq!(
        store.body(&p.records[0].payload).await.unwrap(),
        bodies["artifact-1"]
    );
    store.close().await.unwrap();
    let mut wrong = options(&path, id);
    wrong.key = [8; 32];
    assert!(FileStore::open(wrong).await.is_err());
    assert_eq!(
        std::fs::read(&path).unwrap().len(),
        std::fs::metadata(&path).unwrap().len() as usize
    );
}
#[tokio::test]
async fn archive_integrity_capacity_revocation_and_snapshot_failure_leave_old_store() {
    for case in [
        "body", "record", "chain", "source", "sequence", "extra", "identity", "snapshot",
        "capacity",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = path(&temp);
        let (b, mut s, mut p, mut bodies) = fixture();
        let id = Identity::from_binding(&b, &s).unwrap();
        let mut o = options(&path, id);
        if case == "capacity" {
            o.limits.max_batch_bytes = 1;
        }
        let store = FileStore::open(o).await.unwrap();
        let before = std::fs::read(&path).unwrap();
        match case {
            "body" => bodies.get_mut("artifact-1").unwrap()[0] = b'x',
            "record" => p.records[0].record_digest = "f".repeat(64),
            "chain" => p.records[0].predecessor_digest = "f".repeat(64),
            "source" => p.records[0].payload.source_id = "other".into(),
            "sequence" => p.records[0].sequence = "2".into(),
            "extra" => {
                bodies.insert("extra".into(), vec![1]);
            }
            "identity" => s.source_generation = "other".into(),
            "snapshot" => s.published_through_sequence = Some("2".into()),
            _ => {}
        }
        assert!(
            store
                .receive(
                    &b,
                    &s,
                    &p,
                    bodies,
                    RequestIdentity {
                        request_id: "request-1".into(),
                        operation_epoch: "epoch-1".into()
                    }
                )
                .await
                .is_err(),
            "{case}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), before, "{case}");
        assert!(store.pending().await.unwrap().is_none());
        store.close().await.unwrap();
    }
    let temp = tempfile::tempdir().unwrap();
    let path = path(&temp);
    let (b, s, _, _) = fixture();
    let id = Identity::from_binding(&b, &s).unwrap();
    let allowed = Arc::new(AtomicBool::new(true));
    let flag = allowed.clone();
    let mut o = options(&path, id);
    o.check_access = Arc::new(move |_| {
        if flag.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(Error::InvalidInput("revoked".into()))
        }
    });
    let store = FileStore::open(o).await.unwrap();
    allowed.store(false, Ordering::SeqCst);
    assert!(store.head().await.is_err());
    store.close().await.unwrap();
}
#[tokio::test]
async fn archive_rebase_intent_survives_restart_and_never_reuses_id() {
    let temp = tempfile::tempdir().unwrap();
    let path = path(&temp);
    let (b, s, p, bodies) = fixture();
    let id = Identity::from_binding(&b, &s).unwrap();
    let store = FileStore::open(options(&path, id.clone())).await.unwrap();
    let ack = store
        .receive(
            &b,
            &s,
            &p,
            bodies,
            RequestIdentity {
                request_id: "original".into(),
                operation_epoch: "epoch-1".into(),
            },
        )
        .await
        .unwrap();
    assert!(store.prepare_rebase(ack.request.clone()).await.is_err());
    assert!(
        store
            .prepare_rebase(RequestIdentity {
                request_id: "different-epoch".into(),
                operation_epoch: "epoch-2".into()
            })
            .await
            .is_err()
    );
    let key = RequestIdentity {
        request_id: "recovery".into(),
        operation_epoch: "epoch-1".into(),
    };
    let intent = store.prepare_rebase(key.clone()).await.unwrap();
    assert!(
        store
            .prepare_rebase(RequestIdentity {
                request_id: "replacement-recovery".into(),
                operation_epoch: "epoch-1".into()
            })
            .await
            .is_err()
    );
    assert_eq!(store.pending().await.unwrap(), Some(ack.clone()));
    store.close().await.unwrap();
    let store = FileStore::open(options(&path, id.clone())).await.unwrap();
    assert_eq!(store.pending_rebase().await.unwrap(), Some(intent.clone()));
    let mut next = ack.clone();
    next.request = key.clone();
    next.expected_revision = "2".into();
    let result = AckRebaseReceipt {
        protocol: PROTOCOL.into(),
        binding_id: ack.binding_id.clone(),
        previous: ack.clone(),
        request: key,
        next: next.clone(),
        receipt: receipt(&id, &next),
    };
    let mut bad = result.clone();
    bad.next.coverage.head_digest = "f".repeat(64);
    assert!(store.confirm_rebase(bad).await.is_err());
    store.confirm_rebase(result.clone()).await.unwrap();
    store.confirm_rebase(result).await.unwrap();
    assert!(store.pending().await.unwrap().is_none());
    assert!(store.pending_rebase().await.unwrap().is_none());
    assert!(store.prepare_rebase(ack.request).await.is_err());
    store.close().await.unwrap();
}
#[test]
fn archive_nullable_fields_cannot_be_omitted_on_wire() {
    let (_, status, _, _) = fixture();
    let mut value = serde_json::to_value(status).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .remove("acknowledgedCoverage");
    assert!(tansr_sdk::api::validate_wire(PROTOCOL, "ArchiveStatus", &value).is_err());
}

#[tokio::test]
async fn archive_material_preflight_rejects_foreign_or_incomplete_requests_before_upload() {
    let temp = tempfile::tempdir().unwrap();
    let path = path(&temp);
    let (b, s, p, bodies) = attachment_fixture();
    let id = Identity::from_binding(&b, &s).unwrap();
    let store = FileStore::open(options(&path, id)).await.unwrap();
    store
        .receive(
            &b,
            &s,
            &p,
            bodies,
            RequestIdentity {
                request_id: "receive".into(),
                operation_epoch: "epoch-1".into(),
            },
        )
        .await
        .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let api =
        tansr_sdk::api::ApiClient::builder(format!("http://{}", listener.local_addr().unwrap()))
            .token("fixture")
            .session_family("sdk1")
            .build()
            .unwrap();
    let client = ArchiveClient::new(api);
    let record = &p.records[0];
    let request = MaterialRequest {
        protocol: PROTOCOL.into(),
        binding_id: b.binding_id.clone(),
        material_request_id: "material-1".into(),
        target: b.target.clone(),
        source_id: b.source_id.clone(),
        source_generation: s.source_generation.clone(),
        requested_records: vec![MaterialRecord {
            record_id: record.record_id.clone(),
            digest: record.record_digest.clone(),
            payload: record.payload.clone(),
            attachments: record.attachments.clone(),
        }],
        purpose: "context-recall".into(),
        max_bytes: 1048576,
        remaining_ttl_ms: 30000,
        chunk_bytes: 65536,
    };
    for case in [
        "source",
        "generation",
        "digest",
        "missing-record",
        "artifact",
        "missing-attachment",
        "quota",
        "upload-slots",
        "expired",
    ] {
        let mut invalid = request.clone();
        match case {
            "source" => invalid.source_id = "another-source".into(),
            "generation" => invalid.source_generation = "another-generation".into(),
            "digest" => invalid.requested_records[0].digest = "f".repeat(64),
            "missing-record" => invalid.requested_records[0].record_id = "missing-record".into(),
            "missing-attachment" => invalid.requested_records[0].attachments.clear(),
            "quota" => invalid.max_bytes = 1024,
            "upload-slots" => invalid.chunk_bytes = 1,
            "expired" => invalid.remaining_ttl_ms = 0,
            _ => invalid.requested_records[0].payload.sha256 = "f".repeat(64),
        }
        assert!(
            matches!(
                client
                    .prepare_materials(
                        &store,
                        &invalid,
                        RequestIdentity {
                            request_id: "supply".into(),
                            operation_epoch: "epoch-1".into()
                        }
                    )
                    .await,
                Err(Error::Contract(_) | Error::Io(_) | Error::InvalidInput(_))
            ),
            "{case}"
        );
        assert!(matches!(listener.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock));
    }
    let elapsed = std::time::SystemTime::now() - std::time::Duration::from_secs(1);
    assert!(matches!(
        client
            .prepare_materials_before(
                &store,
                &request,
                RequestIdentity {
                    request_id: "persisted-material".into(),
                    operation_epoch: "epoch-1".into()
                },
                elapsed
            )
            .await,
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(listener.accept(),Err(e) if e.kind()==std::io::ErrorKind::WouldBlock));
    store.close().await.unwrap();
}

#[tokio::test]
async fn archive_exact_binary_attachments_and_json_encoding_are_not_interchangeable() {
    for case in [
        "good",
        "missing",
        "replaced",
        "reencoded",
        "reordered",
        "source",
        "reference",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = path(&temp);
        let (binding, status, mut page, mut bodies) = attachment_fixture();
        let id = Identity::from_binding(&binding, &status).unwrap();
        let store = FileStore::open(options(&path, id)).await.unwrap();
        let before = std::fs::read(&path).unwrap();
        match case {
            "missing" => {
                bodies.remove("attachment-1");
            }
            "replaced" => {
                bodies.get_mut("attachment-1").unwrap()[17] ^= 1;
            }
            "reencoded" => {
                let body = bodies.get_mut("artifact-1").unwrap();
                let json: serde_json::Value = serde_json::from_slice(body).unwrap();
                *body = serde_json::to_vec_pretty(&json).unwrap();
            }
            "reordered" => {
                let mut duplicate = page.records[0].clone();
                duplicate.sequence = "2".into();
                page.records.insert(0, duplicate);
            }
            "source" => page.records[0].attachments[0].source_id = "foreign-source".into(),
            "reference" => page.records[0].attachments[0].sha256 = "f".repeat(64),
            _ => {}
        }
        let result = store
            .receive(
                &binding,
                &status,
                &page,
                bodies.clone(),
                RequestIdentity {
                    request_id: "exact".into(),
                    operation_epoch: "epoch-1".into(),
                },
            )
            .await;
        if case == "good" {
            let ack = result.unwrap();
            assert_eq!(ack.attachments.len(), 1);
            assert_eq!(ack.payloads.len(), 1);
            assert_eq!(
                store.body(&page.records[0].attachments[0]).await.unwrap(),
                bodies["attachment-1"]
            );
        } else {
            assert!(result.is_err(), "{case}");
            assert!(store.pending().await.unwrap().is_none());
            assert_eq!(std::fs::read(&path).unwrap(), before, "{case}");
        }
        store.close().await.unwrap();
    }
}

async fn controlled_archive_response(
    status: u16,
    body: serde_json::Value,
    delay: std::time::Duration,
) -> (ArchiveClient, tokio::task::JoinHandle<String>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = ArchiveClient::new(
        tansr_sdk::ApiClient::builder(format!("http://{}", listener.local_addr().unwrap()))
            .session_family("sdk1")
            .token("synthetic")
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .unwrap(),
    );
    let task = tokio::spawn(async move {
        let (mut socket, _) =
            tokio::time::timeout(std::time::Duration::from_secs(3), listener.accept())
                .await
                .unwrap()
                .unwrap();
        let mut bytes = Vec::new();
        let (end, length) = loop {
            let mut chunk = [0; 4096];
            let n = socket.read(&mut chunk).await.unwrap();
            assert_ne!(n, 0);
            bytes.extend_from_slice(&chunk[..n]);
            assert!(bytes.len() < 300_000);
            if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let header = std::str::from_utf8(&bytes[..end]).unwrap();
                let length = header
                    .lines()
                    .find_map(|line| {
                        let (k, v) = line.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse::<usize>().unwrap())
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
        let request = String::from_utf8(bytes).unwrap();
        tokio::time::sleep(delay).await;
        let body = serde_json::to_vec(&body).unwrap();
        let header = format!(
            "HTTP/1.1 {status} Synthetic\r\ncontent-type: application/json\r\ncontent-length: {}\r\ntansr-contract: unified-v1\r\ntansr-manifest-revision: 7\r\ntansr-schema-hash: sha256:b60e77ffcbf08d985a993dbdbd4cf610f12f7c7e70f090aee5ff8d523f70bb57\r\ntansr-domain: archive\r\nconnection: close\r\n\r\n",
            body.len()
        );
        let _ = socket.write_all(header.as_bytes()).await;
        let _ = socket.write_all(&body).await;
        let _ = socket.shutdown().await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "unexpected retry/rebase HTTP request"
        );
        request
    });
    (client, task)
}

#[tokio::test]
async fn archive_recovery_does_not_rebase_ordinary_errors_or_revoked_access() {
    for (status, code, action, detail) in [
        (
            412,
            "precondition_failed",
            "refresh",
            json!({"reason":"if_match_body_mismatch","domainCode":"revision_conflict","header":"if-match"}),
        ),
        (
            412,
            "precondition_failed",
            "refresh",
            json!({"reason":"if_match_stale","domainCode":"request_id_conflict"}),
        ),
        (
            409,
            "conflict",
            "same-request",
            json!({"busy":true,"domainCode":"binding_conflict"}),
        ),
        (
            403,
            "forbidden",
            "none",
            json!({"domainCode":"forbidden","revoked":true}),
        ),
        (410, "gone", "none", json!({"domainCode":"request_expired"})),
        (
            503,
            "upstream_unavailable",
            "same-request",
            json!({"domainCode":"epoch_unavailable"}),
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = path(&temp);
        let (b, s, p, bodies) = fixture();
        let id = Identity::from_binding(&b, &s).unwrap();
        let store = FileStore::open(options(&path, id)).await.unwrap();
        let ack = store
            .receive(
                &b,
                &s,
                &p,
                bodies,
                RequestIdentity {
                    request_id: "original-error-test".into(),
                    operation_epoch: "epoch-1".into(),
                },
            )
            .await
            .unwrap();
        let before = std::fs::read(&path).unwrap();
        // Frozen UnifiedError binds precondition_failed to refresh/rediscover.
        // A valid refresh hint alone still must not authorize ACK rebase.
        let body = json!({"contract":"unified-v1","traceId":"synthetic","requestId":ack.request.request_id,"code":code,"status":status,"retryAction":action,"message":"synthetic error","detail":detail});
        tansr_sdk::api::schema::validate("UnifiedError", &body)
            .expect("controlled error must satisfy the complete frozen schema");
        let (client, task) =
            controlled_archive_response(status, body, std::time::Duration::ZERO).await;
        match recover_pending(&client, &store, "must-not-be-created").await {
            Err(Error::Api(e)) => assert_eq!(e.status, status),
            other => panic!("expected decoded {code}, got {other:?}"),
        }
        assert_eq!(store.pending().await.unwrap(), Some(ack));
        assert!(store.pending_rebase().await.unwrap().is_none());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(task.await.unwrap().starts_with("POST /api/archive/"));
        store.close().await.unwrap();
    }
    let temp = tempfile::tempdir().unwrap();
    let path = path(&temp);
    let (b, s, p, bodies) = fixture();
    let id = Identity::from_binding(&b, &s).unwrap();
    let live = Arc::new(AtomicBool::new(true));
    let flag = live.clone();
    let mut config = options(&path, id);
    config.check_access = Arc::new(move |_| {
        if flag.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(Error::Io("revoked".into()))
        }
    });
    let store = FileStore::open(config).await.unwrap();
    store
        .receive(
            &b,
            &s,
            &p,
            bodies,
            RequestIdentity {
                request_id: "revoked-before-HTTP".into(),
                operation_epoch: "epoch-1".into(),
            },
        )
        .await
        .unwrap();
    live.store(false, Ordering::SeqCst);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let client = ArchiveClient::new(
        tansr_sdk::ApiClient::builder(format!("http://{}", listener.local_addr().unwrap()))
            .session_family("sdk1")
            .token("synthetic")
            .build()
            .unwrap(),
    );
    assert!(recover_pending(&client, &store, "forbidden").await.is_err());
    assert!(matches!(listener.accept(),Err(e)if e.kind()==std::io::ErrorKind::WouldBlock));
    store.close().await.unwrap();
}

#[tokio::test]
async fn archive_material_timeout_releases_http_and_does_not_renew_original_deadline() {
    let temp = tempfile::tempdir().unwrap();
    let path = path(&temp);
    let (b, s, p, bodies) = fixture();
    let store = FileStore::open(options(&path, Identity::from_binding(&b, &s).unwrap()))
        .await
        .unwrap();
    store
        .receive(
            &b,
            &s,
            &p,
            bodies,
            RequestIdentity {
                request_id: "receive-timeout".into(),
                operation_epoch: "epoch-1".into(),
            },
        )
        .await
        .unwrap();
    let record = &p.records[0];
    let request = MaterialRequest {
        protocol: PROTOCOL.into(),
        binding_id: b.binding_id.clone(),
        material_request_id: "material-timeout".into(),
        target: b.target.clone(),
        source_id: b.source_id.clone(),
        source_generation: s.source_generation.clone(),
        requested_records: vec![MaterialRecord {
            record_id: record.record_id.clone(),
            digest: record.record_digest.clone(),
            payload: record.payload.clone(),
            attachments: record.attachments.clone(),
        }],
        purpose: "context-recall".into(),
        max_bytes: 1048576,
        remaining_ttl_ms: 30000,
        chunk_bytes: 65536,
    };
    let (client, task) =
        controlled_archive_response(200, json!({}), std::time::Duration::from_secs(3)).await;
    let deadline = std::time::SystemTime::now() + std::time::Duration::from_secs(2);
    let identity = RequestIdentity {
        request_id: "material-original".into(),
        operation_epoch: "epoch-1".into(),
    };
    assert!(matches!(
        client
            .prepare_materials_before(&store, &request, identity.clone(), deadline)
            .await,
        Err(Error::Unknown(_) | Error::Transport(_))
    ));
    assert!(matches!(
        client
            .prepare_materials_before(&store, &request, identity, deadline)
            .await,
        Err(Error::InvalidInput(_))
    ));
    assert!(task.await.unwrap().contains("material-timeout"));
    assert!(store.coverage().await.unwrap().is_none());
    store.close().await.unwrap();
}

#[tokio::test]
async fn archive_cross_process_second_writer_cannot_acquire_live_store() {
    const TEST: &str = "archive_cross_process_second_writer_cannot_acquire_live_store";
    if let Ok(path) = std::env::var("TANSR_RUST_ARCHIVE_LOCK_CHILD") {
        let (b, s, _, _) = fixture();
        let id = Identity::from_binding(&b, &s).unwrap();
        let result = FileStore::open(options(Path::new(&path), id)).await;
        assert!(matches!(result, Err(Error::Io(_))));
        std::process::exit(74);
    }
    let temp = tempfile::tempdir().unwrap();
    let path = path(&temp);
    let (b, s, p, bodies) = fixture();
    let id = Identity::from_binding(&b, &s).unwrap();
    let store = FileStore::open(options(&path, id.clone())).await.unwrap();
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST, "--nocapture"])
        .env("TANSR_RUST_ARCHIVE_LOCK_CHILD", &path)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let status = tokio::time::timeout(std::time::Duration::from_secs(15), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.code(), Some(74));
    store
        .receive(
            &b,
            &s,
            &p,
            bodies,
            RequestIdentity {
                request_id: "only-writer".into(),
                operation_epoch: "epoch-1".into(),
            },
        )
        .await
        .unwrap();
    store.close().await.unwrap();
    let reopened = FileStore::open(options(&path, id)).await.unwrap();
    assert_eq!(
        reopened
            .pending()
            .await
            .unwrap()
            .unwrap()
            .request
            .request_id,
        "only-writer"
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn archive_live_authorization_is_checked_after_read_and_before_commit() {
    for deny_at in [3usize, 4] {
        let temp = tempfile::tempdir().unwrap();
        let path = path(&temp);
        let (b, s, p, bodies) = fixture();
        let id = Identity::from_binding(&b, &s).unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        let denial = Arc::new(AtomicUsize::new(usize::MAX));
        let checks = counter.clone();
        let limit = denial.clone();
        let mut config = options(&path, id.clone());
        config.check_access = Arc::new(move |_| {
            if checks.fetch_add(1, Ordering::SeqCst) + 1 >= limit.load(Ordering::SeqCst) {
                Err(Error::Io("host principal revoked".into()))
            } else {
                Ok(())
            }
        });
        let store = FileStore::open(config).await.unwrap();
        let old = std::fs::read(&path).unwrap();
        counter.store(0, Ordering::SeqCst);
        denial.store(deny_at, Ordering::SeqCst);
        assert!(
            store
                .receive(
                    &b,
                    &s,
                    &p,
                    bodies.clone(),
                    RequestIdentity {
                        request_id: "revoked-commit".into(),
                        operation_epoch: "epoch-1".into()
                    }
                )
                .await
                .is_err()
        );
        store.close().await.unwrap();
        let store = FileStore::open(options(&path, id)).await.unwrap();
        assert!(store.coverage().await.unwrap().is_none());
        assert_eq!(store.pending().await.unwrap().is_some(), deny_at == 4);
        if deny_at == 3 {
            assert_eq!(std::fs::read(&path).unwrap(), old);
        }
        store.close().await.unwrap();
    }
    let temp = tempfile::tempdir().unwrap();
    let path = path(&temp);
    let (b, s, p, bodies) = fixture();
    let id = Identity::from_binding(&b, &s).unwrap();
    let counter = Arc::new(AtomicUsize::new(0));
    let denial = Arc::new(AtomicUsize::new(usize::MAX));
    let checks = counter.clone();
    let limit = denial.clone();
    let mut config = options(&path, id);
    config.check_access = Arc::new(move |_| {
        if checks.fetch_add(1, Ordering::SeqCst) + 1 >= limit.load(Ordering::SeqCst) {
            Err(Error::Io("read revoked".into()))
        } else {
            Ok(())
        }
    });
    let store = FileStore::open(config).await.unwrap();
    store
        .receive(
            &b,
            &s,
            &p,
            bodies,
            RequestIdentity {
                request_id: "read-revoke".into(),
                operation_epoch: "epoch-1".into(),
            },
        )
        .await
        .unwrap();
    counter.store(0, Ordering::SeqCst);
    denial.store(2, Ordering::SeqCst);
    assert!(
        store.body(&p.records[0].payload).await.is_err(),
        "no bytes escape when authorization changes during read"
    );
    store.close().await.unwrap();
}

#[tokio::test]
async fn archive_hard_links_and_changed_source_do_not_reopen_as_authorized() {
    let temp = tempfile::tempdir().unwrap();
    let path = path(&temp);
    let (b, s, _, _) = fixture();
    let id = Identity::from_binding(&b, &s).unwrap();
    let store = FileStore::open(options(&path, id.clone())).await.unwrap();
    store.close().await.unwrap();
    let original = std::fs::read(&path).unwrap();
    let mut wrong = id.clone();
    wrong.source_id = "different-source".into();
    assert!(FileStore::open(options(&path, wrong)).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), original);
    let alias = path.with_extension("alias");
    std::fs::hard_link(&path, &alias).unwrap();
    assert!(FileStore::open(options(&path, id.clone())).await.is_err());
    assert_eq!(std::fs::read(&alias).unwrap(), original);
    std::fs::remove_file(alias).unwrap();
    let store = FileStore::open(options(&path, id)).await.unwrap();
    store.close().await.unwrap();
}

#[tokio::test]
async fn archive_capacity_limits_and_recovery_reserve_fail_before_mutating_intent() {
    for case in ["objects", "records", "batch", "stored", "recovery"] {
        let temp = tempfile::tempdir().unwrap();
        let path = path(&temp);
        let (b, mut s, mut p, bodies) = attachment_fixture();
        let id = Identity::from_binding(&b, &s).unwrap();
        let mut config = options(&path, id);
        match case {
            "objects" => config.limits.max_artifacts = 1,
            "records" => {
                config.limits.max_records = 1;
                let mut second = p.records[0].clone();
                second.sequence = "2".into();
                second.record_id = "record-2".into();
                second.predecessor_digest = p.records[0].record_digest.clone();
                let mut value = serde_json::to_value(&second).unwrap();
                value.as_object_mut().unwrap().remove("recordDigest");
                second.record_digest = canonical::digest("tansr.sdk2.record.v1", &value).unwrap();
                p.records.push(second);
                p.next_after_sequence = Some("2".into());
                p.published_through_sequence = Some("2".into());
                s.published_through_sequence = Some("2".into());
                s.pending_records = 2;
            }
            "batch" => config.limits.max_batch_bytes = 1,
            "stored" => {
                config.limits.max_stored_bytes = 1024;
                config.limits.max_batch_bytes = 1024;
            }
            "recovery" => {
                config.limits.max_stored_bytes = 4096;
                config.limits.max_batch_bytes = 4096;
            }
            _ => unreachable!(),
        }
        let store = FileStore::open(config).await.unwrap();
        let original = std::fs::read(&path).unwrap();
        let result = store
            .receive(
                &b,
                &s,
                &p,
                bodies,
                RequestIdentity {
                    request_id: "capacity-original".into(),
                    operation_epoch: "epoch-1".into(),
                },
            )
            .await;
        if case == "recovery" {
            let ack = result.unwrap();
            let saved = std::fs::read(&path).unwrap();
            assert!(
                store
                    .prepare_rebase(RequestIdentity {
                        request_id: "never-prepared".into(),
                        operation_epoch: "epoch-1".into()
                    })
                    .await
                    .is_err()
            );
            assert_eq!(std::fs::read(&path).unwrap(), saved);
            assert_eq!(store.pending().await.unwrap(), Some(ack));
            assert!(store.pending_rebase().await.unwrap().is_none());
        } else {
            assert!(result.is_err(), "{case}");
            assert_eq!(std::fs::read(&path).unwrap(), original);
            assert!(store.pending().await.unwrap().is_none());
        }
        store.close().await.unwrap();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn archive_unix_permissions_and_symbolic_links_are_rejected_without_chmod() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let temp = tempfile::tempdir().unwrap();
    let path = path(&temp);
    let (b, s, _, _) = fixture();
    let id = Identity::from_binding(&b, &s).unwrap();
    let store = FileStore::open(options(&path, id.clone())).await.unwrap();
    store.close().await.unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(FileStore::open(options(&path, id.clone())).await.is_err());
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o644
    );
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::set_permissions(
        path.parent().unwrap(),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    assert!(FileStore::open(options(&path, id.clone())).await.is_err());
    std::fs::set_permissions(
        path.parent().unwrap(),
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    let alias = path.with_extension("link");
    symlink(&path, &alias).unwrap();
    assert!(FileStore::open(options(&alias, id.clone())).await.is_err());
    let parent_alias = path.parent().unwrap().with_file_name("link-parent");
    symlink(path.parent().unwrap(), &parent_alias).unwrap();
    assert!(
        FileStore::open(options(&parent_alias.join("archive.bin"), id))
            .await
            .is_err()
    );
}

#[cfg(windows)]
#[tokio::test]
async fn archive_windows_everyone_ace_is_rejected_without_repairing_acl() {
    let temp = tempfile::tempdir().unwrap();
    let path = path(&temp);
    let (b, s, _, _) = fixture();
    let id = Identity::from_binding(&b, &s).unwrap();
    let store = FileStore::open(options(&path, id.clone())).await.unwrap();
    store.close().await.unwrap();
    // SID spelling is independent of the Windows display language. This only
    // changes this synthetic temp file, inside a still-private parent directory.
    let result = std::process::Command::new("icacls.exe")
        .arg(&path)
        .args(["/grant", "*S-1-1-0:(R)"])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "could not construct actual invalid ACL"
    );
    let before = std::process::Command::new("icacls.exe")
        .arg(&path)
        .output()
        .unwrap();
    assert!(FileStore::open(options(&path, id)).await.is_err());
    let after = std::process::Command::new("icacls.exe")
        .arg(&path)
        .output()
        .unwrap();
    assert_eq!(
        before.stdout, after.stdout,
        "SDK must reject, not silently repair host ACL"
    );
}

#[cfg(windows)]
#[tokio::test]
async fn archive_windows_junction_cannot_redirect_storage_into_another_private_directory() {
    use std::os::windows::fs::MetadataExt;
    struct Junction(std::path::PathBuf);
    impl Drop for Junction {
        fn drop(&mut self) {
            if std::fs::symlink_metadata(&self.0)
                .is_ok_and(|meta| meta.file_attributes() & 0x400 != 0)
            {
                // RemoveDirectory removes this junction itself, never its target.
                let _ = std::fs::remove_dir(&self.0);
            }
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let private = root.join("client");
    let target = root.join("target");
    create_private_directory(&private).unwrap();
    create_private_directory(&target).unwrap();
    let file = target.join("archive.bin");
    let (b, s, p, bodies) = fixture();
    let id = Identity::from_binding(&b, &s).unwrap();
    let store = FileStore::open(options(&file, id.clone())).await.unwrap();
    store
        .receive(
            &b,
            &s,
            &p,
            bodies,
            RequestIdentity {
                request_id: "junction-target-original".into(),
                operation_epoch: "epoch-1".into(),
            },
        )
        .await
        .unwrap();
    store.close().await.unwrap();
    let original = std::fs::read(&file).unwrap();
    let link = private.join("junction");
    assert!(link.starts_with(&root) && target.starts_with(&root));
    // PowerShell's junction creation does not require symbolic-link privilege.
    // Its paths are environment values, not interpolated script fragments.
    let shell_path = |path: &Path| {
        path.to_str()
            .unwrap()
            .strip_prefix(r"\\?\")
            .unwrap_or(path.to_str().unwrap())
            .to_owned()
    };
    let created=std::process::Command::new("powershell.exe")
        .args(["-NoProfile","-NonInteractive","-Command","$ErrorActionPreference = 'Stop'; New-Item -ItemType Junction -Path $env:TANSR_TEST_JUNCTION_LINK -Target $env:TANSR_TEST_JUNCTION_TARGET | Out-Null"])
        .env("TANSR_TEST_JUNCTION_LINK",shell_path(&link)).env("TANSR_TEST_JUNCTION_TARGET",shell_path(&target)).output().unwrap();
    assert!(
        created.status.success(),
        "could not construct actual junction: {}",
        String::from_utf8_lossy(&created.stderr)
    );
    let guard = Junction(link.clone());
    assert_ne!(
        std::fs::symlink_metadata(&link).unwrap().file_attributes() & 0x400,
        0
    );
    assert_eq!(
        std::fs::read(link.join("archive.bin")).unwrap(),
        original,
        "junction must actually resolve to target before testing rejection"
    );
    assert!(
        FileStore::open(options(&link.join("archive.bin"), id.clone()))
            .await
            .is_err()
    );
    assert!(create_private_directory(&link).is_err());
    assert_eq!(std::fs::read(&file).unwrap(), original);
    std::fs::remove_dir(&link).unwrap();
    drop(guard);
    assert!(!link.exists());
    assert_eq!(
        std::fs::read(&file).unwrap(),
        original,
        "link removal must preserve target"
    );
    let reopened = FileStore::open(options(&file, id)).await.unwrap();
    assert_eq!(
        reopened
            .pending()
            .await
            .unwrap()
            .unwrap()
            .request
            .request_id,
        "junction-target-original"
    );
    reopened.close().await.unwrap();
}

#[test]
fn runtime_shutdown_drops_owned_archive_lock_and_journal_directory_handle() {
    use std::time::Duration;
    use tansr_sdk::executor::FileJournal;
    let temp = tempfile::tempdir().unwrap();
    let path = path(&temp);
    let journal_path = path.parent().unwrap().join("journal");
    let (b, s, p, bodies) = fixture();
    let id = Identity::from_binding(&b, &s).unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let owned_path = path.clone();
    let owned_journal = journal_path.clone();
    let owner_id = id.clone();
    runtime.spawn(async move {
        let store = Arc::new(
            FileStore::open(options(&owned_path, owner_id))
                .await
                .unwrap(),
        );
        store
            .receive(
                &b,
                &s,
                &p,
                bodies,
                RequestIdentity {
                    request_id: "runtime-owned-pending".into(),
                    operation_epoch: "epoch-1".into(),
                },
            )
            .await
            .unwrap();
        let journal = Arc::new(FileJournal::open(&owned_journal).unwrap());
        tx.send((Arc::downgrade(&store), Arc::downgrade(&journal)))
            .unwrap();
        std::future::pending::<()>().await;
        // These values deliberately stay owned by a pending runtime task.
        drop((store, journal));
    });
    let (store_weak, journal_weak) = rx.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(store_weak.upgrade().is_some() && journal_weak.upgrade().is_some());
    assert!(
        runtime
            .block_on(FileStore::open(options(&path, id.clone())))
            .is_err(),
        "live task still owns the archive process lock"
    );
    #[cfg(windows)]
    assert!(
        std::fs::rename(
            &journal_path,
            journal_path.with_file_name("journal-before-shutdown")
        )
        .is_err(),
        "live Windows journal directory handle must be held"
    );
    runtime.shutdown_timeout(Duration::from_secs(5));
    assert!(
        store_weak.upgrade().is_none() && journal_weak.upgrade().is_none(),
        "shutdown must drop pending task-owned SDK resources"
    );
    let moved = journal_path.with_file_name("journal-after-shutdown");
    std::fs::rename(&journal_path, &moved).unwrap();
    let journal = FileJournal::open(&moved).unwrap();
    drop(journal);
    let next = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    next.block_on(async {
        let reopened = FileStore::open(options(&path, id)).await.unwrap();
        assert_eq!(
            reopened
                .pending()
                .await
                .unwrap()
                .unwrap()
                .request
                .request_id,
            "runtime-owned-pending"
        );
        assert!(reopened.coverage().await.unwrap().is_none());
        reopened.close().await.unwrap();
    });
    next.shutdown_timeout(Duration::from_secs(5));
}

#[cfg(unix)]
#[tokio::test]
async fn archive_replaced_parent_cannot_redirect_a_locked_store_write() {
    let temp = tempfile::tempdir().unwrap();
    let path = path(&temp);
    let (b, s, p, bodies) = fixture();
    let id = Identity::from_binding(&b, &s).unwrap();
    let store = FileStore::open(options(&path, id)).await.unwrap();
    let original = path.parent().unwrap();
    let moved = original.with_file_name("moved");
    std::fs::rename(original, &moved).unwrap();
    create_private_directory(original).unwrap();
    let unchanged = std::fs::read(moved.join("archive.bin")).unwrap();
    assert!(
        store
            .receive(
                &b,
                &s,
                &p,
                bodies,
                RequestIdentity {
                    request_id: "no-redirect".into(),
                    operation_epoch: "epoch-1".into()
                }
            )
            .await
            .is_err()
    );
    assert!(!path.exists());
    assert_eq!(std::fs::read(moved.join("archive.bin")).unwrap(), unchanged);
    store.close().await.unwrap();
}
