use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};
use tansr_sdk::{
    Error,
    archive::create_private_directory,
    executor::{Binding, Scope},
    memory_publication::*,
};
fn owner() -> Owner {
    let binding: Binding = serde_json::from_value(json!({"bindingId":"binding","revision":"1","target":{"executorId":"device","connectionId":"connection","connectionRevision":"1","workspaceId":"workspace","workspaceRevision":"1"}})).unwrap();
    Owner {
        scope: Scope {
            application_scope_id: "app".into(),
            end_user_id: "user".into(),
            authorization_revision: "1".into(),
        },
        session_id: "session".into(),
        binding,
    }
}
fn identity() -> Identity {
    Identity {
        application_scope_id: "app".into(),
        end_user_id: "user".into(),
        source_id: "source".into(),
        source_generation: "1".into(),
        domain_key: "domain".into(),
    }
}
fn options(path: &Path, mode: OpenMode) -> StoreOptions {
    StoreOptions {
        path: path.into(),
        mode,
        key: [7; 32],
        identity: identity(),
        limits: Limits::default(),
        read_context: Arc::new(|| Ok(owner())),
        authorize_recovery: None,
    }
}
fn path(dir: &tempfile::TempDir) -> std::path::PathBuf {
    let root = std::fs::canonicalize(dir.path()).unwrap().join("private");
    create_private_directory(&root).unwrap();
    root.join("publication")
}
fn req(action: &str, fields: Value) -> Value {
    let mut r = json!({"contract":CONTRACT,"sourceId":"source","sourceGeneration":"1","domainKey":"domain","action":action});
    for (k, v) in fields.as_object().unwrap() {
        r[k] = v.clone();
    }
    r
}
fn hash(b: &[u8]) -> String {
    format!("{:x}", Sha256::digest(b))
}
fn begin(id: &str, body: &[u8], expected: Option<&str>) -> Value {
    req(
        "begin",
        json!({"transferId":id,"expectedEtag":expected,"byteLength":body.len(),"sha256":hash(body)}),
    )
}
fn chunk(id: &str, body: &[u8], offset: usize) -> Value {
    req(
        "chunk",
        json!({"transferId":id,"offset":offset,"byteLength":body.len(),"base64":STANDARD.encode(body),"payloadDigest":hash(body)}),
    )
}
fn transfer(action: &str, id: &str) -> Value {
    req(action, json!({"transferId":id}))
}
async fn publish(s: &FileStore, id: &str, body: &[u8], expected: Option<&str>) -> Value {
    s.execute(begin(id, body, expected), owner()).await.unwrap();
    for (i, part) in body.chunks(12288).enumerate() {
        s.execute(chunk(id, part, i * 12288), owner())
            .await
            .unwrap();
    }
    s.execute(transfer("commit", id), owner()).await.unwrap()
}
#[tokio::test]
async fn six_actions_reopen_lost_result_and_encrypted_body() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let s = FileStore::open(options(&path, OpenMode::Create))
        .await
        .unwrap();
    assert!(s.execute(req("head", json!({})), owner()).await.unwrap()["publication"].is_null());
    let body = "sensitive 😀 memory body".as_bytes();
    let committed = publish(&s, "first", body, None).await;
    assert_eq!(committed["transfer"]["status"], "committed");
    assert_eq!(
        s.execute(transfer("commit", "first"), owner())
            .await
            .unwrap(),
        committed
    );
    s.close().await.unwrap();
    let disk = std::fs::read(&path).unwrap();
    assert!(!disk.windows(body.len()).any(|b| b == body));
    assert!(!String::from_utf8_lossy(&disk).contains("first"));
    let s = FileStore::open(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    let q = s
        .execute(transfer("query", "first"), owner())
        .await
        .unwrap();
    assert_eq!(q["transfer"], committed["transfer"]);
    let h = s.execute(req("head", json!({})), owner()).await.unwrap();
    assert_eq!(h["publication"]["sha256"], hash(body));
    let r = s
        .execute(
            req("read", json!({"etag":hash(body),"offset":0,"length":12288})),
            owner(),
        )
        .await
        .unwrap();
    assert_eq!(
        STANDARD.decode(r["base64"].as_str().unwrap()).unwrap(),
        body
    );
    assert_eq!(r["complete"], true);
    assert_eq!(
        s.execute(begin("first", body, None), owner())
            .await
            .unwrap()["transfer"],
        q["transfer"]
    );
    assert_eq!(
        s.execute(transfer("commit", "missing"), owner())
            .await
            .unwrap()["transfer"]["status"],
        "unknown"
    );
    s.close().await.unwrap();
    assert!(s.capacity().await.is_err());
}
#[tokio::test]
async fn partial_chunks_reopen_and_reject_gaps_changes_and_bad_digest() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let s = FileStore::open(options(&path, OpenMode::Create))
        .await
        .unwrap();
    s.execute(begin("t", b"abcdef", None), owner())
        .await
        .unwrap();
    s.execute(chunk("t", b"abc", 0), owner()).await.unwrap();
    s.close().await.unwrap();
    let s = FileStore::open(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    assert_eq!(
        s.execute(chunk("t", b"abc", 0), owner()).await.unwrap()["transfer"]["receivedBytes"],
        3
    );
    assert!(s.execute(chunk("t", b"xyz", 0), owner()).await.is_err());
    assert!(s.execute(chunk("t", b"f", 5), owner()).await.is_err());
    assert!(
        s.execute(begin("t", b"other!", None), owner())
            .await
            .is_err()
    );
    let mut bad = chunk("t", b"def", 3);
    bad["payloadDigest"] = json!("0".repeat(64));
    assert!(s.execute(bad, owner()).await.is_err());
    assert!(s.execute(transfer("commit", "t"), owner()).await.is_err());
    s.execute(chunk("t", b"def", 3), owner()).await.unwrap();
    assert_eq!(
        s.execute(transfer("commit", "t"), owner()).await.unwrap()["transfer"]["status"],
        "committed"
    );
}
#[tokio::test]
async fn cas_conflict_and_capacity_preserve_permanent_witnesses() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let mut o = options(&path, OpenMode::Create);
    o.limits = Limits {
        max_transfers: 2,
        max_staging_bytes: 4,
    };
    let s = FileStore::open(o).await.unwrap();
    s.execute(begin("a", b"abc", None), owner()).await.unwrap();
    assert!(s.execute(begin("b", b"xyz", None), owner()).await.is_err());
    s.execute(chunk("a", b"abc", 0), owner()).await.unwrap();
    s.execute(transfer("commit", "a"), owner()).await.unwrap();
    assert_eq!(
        publish(&s, "b", b"xyz", None).await["transfer"]["status"],
        "conflict"
    );
    assert_eq!(
        s.execute(transfer("commit", "b"), owner()).await.unwrap()["transfer"]["status"],
        "conflict"
    );
    assert_eq!(
        s.capacity().await.unwrap(),
        Capacity {
            stored_transfers: 2,
            remaining_transfers: 0,
            staging_bytes: 0,
            remaining_staging_bytes: 4
        }
    );
    assert!(
        s.execute(begin("c", b"n", Some(&hash(b"abc"))), owner())
            .await
            .is_err()
    );
    assert_eq!(
        s.execute(req("head", json!({})), owner()).await.unwrap()["publication"]["etag"],
        hash(b"abc")
    );
}
#[tokio::test]
async fn live_owner_scope_binding_and_source_fences() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let context = Arc::new(Mutex::new(owner()));
    let c = context.clone();
    let mut o = options(&path, OpenMode::Create);
    o.read_context = Arc::new(move || Ok(c.lock().unwrap().clone()));
    let s = FileStore::open(o).await.unwrap();
    s.execute(begin("t", b"x", None), owner()).await.unwrap();
    let mut other = owner();
    other.binding.target.connection_revision = "2".into();
    assert!(s.execute(transfer("query", "t"), other).await.is_err());
    context.lock().unwrap().binding.revision = "2".into();
    assert!(s.execute(chunk("t", b"x", 0), owner()).await.is_err());
    *context.lock().unwrap() = owner();
    let mut r = req("head", json!({}));
    r["sourceGeneration"] = json!("2");
    assert!(s.execute(r, owner()).await.is_err());
    context.lock().unwrap().scope.authorization_revision = "2".into();
    assert!(s.execute(req("head", json!({})), owner()).await.is_err());
    context.lock().unwrap().scope.end_user_id = "other".into();
    assert!(s.capacity().await.is_err());
}
#[tokio::test]
async fn recovery_query_only_never_transfers_write_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let context = Arc::new(Mutex::new(owner()));
    let c = context.clone();
    let mut o = options(&path, OpenMode::Create);
    o.read_context = Arc::new(move || Ok(c.lock().unwrap().clone()));
    o.authorize_recovery = Some(Arc::new(|_, old, new, _| {
        old.binding.target.connection_id == "connection"
            && new.binding.target.connection_id == "new"
    }));
    let s = FileStore::open(o).await.unwrap();
    publish(&s, "t", b"x", None).await;
    let mut other = owner();
    other.binding.target.connection_id = "new".into();
    *context.lock().unwrap() = other.clone();
    assert_eq!(
        s.execute(transfer("query", "t"), other.clone())
            .await
            .unwrap()["transfer"]["status"],
        "committed"
    );
    assert!(s.execute(transfer("commit", "t"), other).await.is_err());
}
#[tokio::test]
async fn key_identity_corruption_lock_and_modes_preserve_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    assert!(
        FileStore::open(options(&path, OpenMode::Reopen))
            .await
            .is_err()
    );
    let s = FileStore::open(options(&path, OpenMode::Create))
        .await
        .unwrap();
    publish(&s, "t", b"secret", None).await;
    assert!(
        FileStore::open(options(&path, OpenMode::Reopen))
            .await
            .is_err()
    );
    s.close().await.unwrap();
    let original = std::fs::read(&path).unwrap();
    assert!(
        FileStore::open(options(&path, OpenMode::Create))
            .await
            .is_err()
    );
    let mut o = options(&path, OpenMode::Reopen);
    o.key = [8; 32];
    assert!(FileStore::open(o).await.is_err());
    let mut o = options(&path, OpenMode::Reopen);
    o.identity.domain_key = "wrong".into();
    assert!(FileStore::open(o).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), original);
    let mut corrupt = original;
    let n = corrupt.len();
    corrupt[n - 1] ^= 1;
    std::fs::write(&path, &corrupt).unwrap();
    assert!(
        FileStore::open(options(&path, OpenMode::Reopen))
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), corrupt);
}
#[tokio::test]
async fn copy_rotates_key_and_preserves_staging_and_terminal_ids() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let s = FileStore::open(options(&path, OpenMode::Create))
        .await
        .unwrap();
    publish(&s, "done", b"secret", None).await;
    s.execute(begin("pending", b"later", Some(&hash(b"secret"))), owner())
        .await
        .unwrap();
    s.execute(chunk("pending", b"la", 0), owner())
        .await
        .unwrap();
    let target = path.with_file_name("rotated");
    assert!(s.copy_to(target.clone(), [7; 32]).await.is_err());
    assert!(!target.exists());
    let copied = s.copy_to(target.clone(), [9; 32]).await.unwrap();
    assert_eq!(
        copied
            .execute(transfer("query", "pending"), owner())
            .await
            .unwrap()["transfer"]["receivedBytes"],
        2
    );
    assert_eq!(
        copied
            .execute(transfer("query", "done"), owner())
            .await
            .unwrap()["transfer"]["status"],
        "committed"
    );
    copied.close().await.unwrap();
    assert!(
        FileStore::open(options(&target, OpenMode::Reopen))
            .await
            .is_err()
    );
    let mut o = options(&target, OpenMode::Reopen);
    o.key = [9; 32];
    let copied = FileStore::open(o).await.unwrap();
    assert_eq!(copied.capacity().await.unwrap().stored_transfers, 2);
    assert_eq!(s.capacity().await.unwrap().stored_transfers, 2);
}
#[tokio::test]
async fn invalid_utf8_and_external_replacement_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let s = FileStore::open(options(&path, OpenMode::Create))
        .await
        .unwrap();
    s.execute(begin("t", &[255], None), owner()).await.unwrap();
    s.execute(chunk("t", &[255], 0), owner()).await.unwrap();
    assert!(s.execute(transfer("commit", "t"), owner()).await.is_err());
    let mut corrupt = std::fs::read(&path).unwrap();
    corrupt[0] ^= 1;
    std::fs::write(&path, &corrupt).unwrap();
    assert!(matches!(
        s.execute(req("head", json!({})), owner()).await,
        Err(Error::Unknown(_))
    ));
    assert_eq!(std::fs::read(&path).unwrap(), corrupt);
}

#[tokio::test]
async fn authority_loss_before_and_after_replace_preserves_known_or_unknown_outcome() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for fail_at in [3, 4] {
        let dir = tempfile::tempdir().unwrap();
        let path = path(&dir);
        let calls = Arc::new(AtomicUsize::new(0));
        let limit = Arc::new(AtomicUsize::new(usize::MAX));
        let c = calls.clone();
        let l = limit.clone();
        let mut o = options(&path, OpenMode::Create);
        o.read_context = Arc::new(move || {
            let n = c.fetch_add(1, Ordering::SeqCst) + 1;
            let mut current = owner();
            if n >= l.load(Ordering::SeqCst) {
                current.binding.revision = "2".into();
            }
            Ok(current)
        });
        let s = FileStore::open(o).await.unwrap();
        s.execute(begin("t", b"body", None), owner()).await.unwrap();
        s.execute(chunk("t", b"body", 0), owner()).await.unwrap();
        calls.store(0, Ordering::SeqCst);
        limit.store(fail_at, Ordering::SeqCst);
        let result = s.execute(transfer("commit", "t"), owner()).await;
        assert!(result.is_err());
        if fail_at == 4 {
            assert!(matches!(result, Err(Error::Unknown(_))));
        }
        s.close().await.unwrap();
        let s = FileStore::open(options(&path, OpenMode::Reopen))
            .await
            .unwrap();
        let status = s.execute(transfer("query", "t"), owner()).await.unwrap();
        assert_eq!(
            status["transfer"]["status"],
            if fail_at == 4 { "committed" } else { "staging" }
        );
    }
}
#[test]
fn publication_child_commit() {
    let Some(path) = std::env::var_os("TANSR_PST_RUST_CHILD_STORE") else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let s = FileStore::open(options(Path::new(&path), OpenMode::Create))
            .await
            .unwrap();
        publish(&s, "process-transfer", b"process-secret", None).await;
        std::process::exit(73);
    });
}
#[tokio::test]
async fn subprocess_exit_releases_lock_and_preserves_committed_transfer() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "publication_child_commit", "--nocapture"])
        .env("TANSR_PST_RUST_CHILD_STORE", &path)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    let s = FileStore::open(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    assert_eq!(
        s.execute(transfer("query", "process-transfer"), owner())
            .await
            .unwrap()["transfer"]["status"],
        "committed"
    );
    assert_eq!(
        s.execute(req("head", json!({})), owner()).await.unwrap()["publication"]["etag"],
        hash(b"process-secret")
    );
}

#[tokio::test]
async fn truncated_publication_and_temporary_files_never_expose_body() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let body = b"PST-Rust-A05-sensitive-publication-body";
    let scans = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = scans.clone();
    let root = path.parent().unwrap().to_path_buf();
    let mut o = options(&path, OpenMode::Create);
    o.read_context = Arc::new(move || {
        for entry in std::fs::read_dir(&root).unwrap() {
            let entry = entry.unwrap();
            if entry.path().extension().is_some_and(|ext| ext == "tmp") {
                let bytes = std::fs::read(entry.path()).unwrap();
                assert!(!bytes.windows(body.len()).any(|w| w == body));
                let encoded = STANDARD.encode(body);
                assert!(
                    !bytes
                        .windows(encoded.len())
                        .any(|w| w == encoded.as_bytes())
                );
                assert!(!bytes.windows(32).any(|w| w == [7; 32]));
                seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        Ok(owner())
    });
    let s = FileStore::open(o).await.unwrap();
    publish(&s, "original", body, None).await;
    s.close().await.unwrap();
    assert!(scans.load(std::sync::atomic::Ordering::SeqCst) >= 4);
    let original = std::fs::read(&path).unwrap();
    for length in [0, 12, original.len() - 1] {
        let bad = &original[..length];
        std::fs::write(&path, bad).unwrap();
        assert!(
            FileStore::open(options(&path, OpenMode::Reopen))
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&path).unwrap(), bad);
    }
    std::fs::write(&path, original).unwrap();
    let s = FileStore::open(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    assert_eq!(
        s.execute(transfer("query", "original"), owner())
            .await
            .unwrap()["transfer"]["status"],
        "committed"
    );
    s.close().await.unwrap();
}
