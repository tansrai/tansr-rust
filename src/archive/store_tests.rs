//! Per-store test-only fault points. No environment switch or fault API exists
//! in the distributed library. OS exits test process recovery, not power loss.
use super::*;
use serde_json::json;
use std::{io::Read, process::Stdio};

fn data() -> (Binding, Status, Page, BTreeMap<String, Vec<u8>>) {
    let target = json!({"sessionId":"fault-session","generations":{"historyEpoch":"h-1","deletionGeneration":"0","projectionRevision":"1"},"sourceSnapshotDigest":"0".repeat(64)});
    let binding = serde_json::from_value(json!({"protocol":PROTOCOL,"bindingId":"binding-1","scope":{"applicationScopeId":"app-1","endUserId":"user-1","authorizationRevision":"1"},"target":target,"revision":"1","state":"active","sourceId":"source-1","acceptedCapabilities":["archive-transfer-v1","context-materials-v1"],"rejectedCapabilities":[],"availability":"legacy-complete","operationEpoch":{"id":"epoch-1","issuedAt":"2030-01-01T00:00:00.000Z","expiresAt":"2030-01-01T00:01:00.000Z","state":"active"},"archiveAckFormat":"split-receipts-v1","limits":{"controlBytes":262144,"recordBytes":262144,"pageRecords":128,"pageBytes":1048576,"attachmentBytes":33554432,"chunkBytes":1024,"materialConcurrent":2,"materialQueue":16,"materialCandidates":32,"materialBytes":1048576,"materialDeadlineMs":30000,"pendingRecords":4096,"pendingBytes":67108864,"inflightReserveBytes":1048576,"offlineMs":1000,"eventRetentionMs":1000,"eventRetentionFrames":1,"eventRetentionBytes":1024,"terminalReceiptRetentionMs":60000,"epochLifetimeMs":60000,"materialChunkBytes":65536}})).unwrap();
    let body = b"{\"role\":\"user\",\"text\":\"fault-only synthetic bytes\"}".to_vec();
    let reference = json!({"artifactId":"artifact-1","sourceId":"source-1","bytes":body.len(),"sha256":hash(&body),"mediaType":"application/json"});
    let mut record = json!({"recordId":"record-1","sequence":"1","target":target,"turnId":"turn-1","recordKind":"turn","turnState":"completed","predecessorDigest":"0".repeat(64),"payload":reference,"attachments":[],"payloadDigest":domain_hash("tansr.sdk2.payload.v1",&body).unwrap()});
    record["recordDigest"] = crate::canonical::digest("tansr.sdk2.record.v1", &record)
        .unwrap()
        .into();
    let status = serde_json::from_value(json!({"protocol":PROTOCOL,"bindingId":"binding-1","revision":"1","generations":target["generations"],"sourceId":"source-1","sourceGeneration":"source-generation-1","publishedThroughSequence":"1","acknowledgedCoverage":null,"releasableThroughSequence":null,"pendingBytes":body.len(),"pendingRecords":1,"sessionPersistence":"unchanged","state":"active"})).unwrap();
    let page = serde_json::from_value(json!({"protocol":PROTOCOL,"bindingId":"binding-1","generations":target["generations"],"records":[record],"nextAfterSequence":"1","complete":true,"publishedThroughSequence":"1"})).unwrap();
    (
        binding,
        status,
        page,
        BTreeMap::from([("artifact-1".into(), body)]),
    )
}
fn settings(path: &std::path::Path) -> StoreOptions {
    let (binding, status, _, _) = data();
    StoreOptions {
        path: path.into(),
        key: [7; 32],
        identity: Identity::from_binding(&binding, &status).unwrap(),
        limits: StoreLimits::default(),
        check_access: Arc::new(|_| Ok(())),
    }
}
async fn receive(store: &FileStore) -> Result<Ack> {
    let (binding, status, page, bodies) = data();
    store
        .receive(
            &binding,
            &status,
            &page,
            bodies,
            RequestIdentity {
                request_id: "original".into(),
                operation_epoch: "epoch-1".into(),
            },
        )
        .await
}
fn private_path(temp: &tempfile::TempDir) -> PathBuf {
    let private = temp.path().canonicalize().unwrap().join("private");
    create_private_directory(&private).unwrap();
    private.join("archive.bin")
}
const POINTS: [&str; 5] = [
    "partial-write",
    "flush",
    "file-sync",
    "before-replace",
    "after-replace",
];

#[tokio::test]
async fn io_failure_never_returns_an_ack_and_reopens_a_complete_state() {
    for point in POINTS {
        let temp = tempfile::tempdir().unwrap();
        let path = private_path(&temp);
        let store = FileStore::open(settings(&path)).await.unwrap();
        let old = std::fs::read(&path).unwrap();
        store.inner.lock().unwrap().as_mut().unwrap().fault = Some((point, false));
        // receive must return Err, so a host cannot acquire an ACK to send.
        assert!(
            matches!(receive(&store).await, Err(Error::Io(_))),
            "{point}"
        );
        if point == "after-replace" {
            assert!(matches!(store.pending().await, Err(Error::Unknown(_))));
        } else {
            assert_eq!(std::fs::read(&path).unwrap(), old, "{point}");
            assert!(store.pending().await.unwrap().is_none());
        }
        store.close().await.unwrap();
        let reopened = FileStore::open(settings(&path)).await.unwrap();
        assert!(
            reopened.coverage().await.unwrap().is_none(),
            "{point}: no receipt, no coverage"
        );
        let pending = reopened.pending().await.unwrap();
        assert_eq!(pending.is_some(), point == "after-replace", "{point}");
        if let Some(ack) = pending {
            assert_eq!(ack.request.request_id, "original");
            assert_eq!(
                reopened.body(&data().2.records[0].payload).await.unwrap(),
                data().3["artifact-1"]
            );
        }
        reopened.close().await.unwrap();
        assert!(
            !std::fs::read_dir(path.parent().unwrap())
                .unwrap()
                .any(|item| item
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".tmp")),
            "failed write temp must be removed: {point}"
        );
    }
}

#[tokio::test]
async fn process_exit_at_snapshot_boundaries_releases_lock_and_preserves_old_or_new() {
    if let Ok(raw) = std::env::var("TANSR_ARCHIVE_UNIT_CRASH") {
        let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let path = PathBuf::from(value["path"].as_str().unwrap());
        let point = POINTS
            .iter()
            .copied()
            .find(|point| Some(*point) == value["point"].as_str())
            .unwrap();
        let store = FileStore::open(settings(&path)).await.unwrap();
        store.inner.lock().unwrap().as_mut().unwrap().fault = Some((point, true));
        let _ = receive(&store).await;
        panic!("crash point did not execute");
    }
    for point in POINTS {
        let temp = tempfile::tempdir().unwrap();
        let path = private_path(&temp);
        let store = FileStore::open(settings(&path)).await.unwrap();
        let old = std::fs::read(&path).unwrap();
        store.close().await.unwrap();
        let mut child=tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","archive::store::fault_tests::process_exit_at_snapshot_boundaries_releases_lock_and_preserves_old_or_new","--nocapture"])
            .env("TANSR_ARCHIVE_UNIT_CRASH",json!({"path":path,"point":point}).to_string())
            .stdin(Stdio::null()).kill_on_drop(true).spawn().unwrap();
        let exit = tokio::time::timeout(std::time::Duration::from_secs(15), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exit.code(), Some(73), "{point}");
        let reopened = FileStore::open(settings(&path)).await.unwrap();
        assert!(reopened.coverage().await.unwrap().is_none());
        assert_eq!(
            reopened.pending().await.unwrap().is_some(),
            point == "after-replace",
            "{point}"
        );
        if point != "after-replace" {
            assert_eq!(std::fs::read(&path).unwrap(), old, "{point}");
        }
        // A torn/orphan temp is never treated as a committed snapshot.
        reopened.close().await.unwrap();
    }
}

#[tokio::test]
async fn ciphertext_corruption_truncation_nonce_and_header_fail_without_modification() {
    let temp = tempfile::tempdir().unwrap();
    let path = private_path(&temp);
    let store = FileStore::open(settings(&path)).await.unwrap();
    receive(&store).await.unwrap();
    store.close().await.unwrap();
    let original = std::fs::read(&path).unwrap();
    let mut samples = vec![
        Vec::new(),
        original[..MAGIC.len() - 1].to_vec(),
        original[..MAGIC.len() + 12].to_vec(),
        original[..original.len() - 1].to_vec(),
    ];
    for at in [0, MAGIC.len(), MAGIC.len() + 12, original.len() - 1] {
        let mut b = original.clone();
        b[at] ^= 1;
        samples.push(b);
    }
    for bytes in samples {
        std::fs::write(&path, &bytes).unwrap();
        assert!(FileStore::open(settings(&path)).await.is_err());
        let mut got = Vec::new();
        File::open(&path).unwrap().read_to_end(&mut got).unwrap();
        assert_eq!(got, bytes);
    }
    std::fs::write(&path, &original).unwrap();
    let restored = FileStore::open(settings(&path)).await.unwrap();
    assert!(restored.pending().await.unwrap().is_some());
    restored.close().await.unwrap();
}
