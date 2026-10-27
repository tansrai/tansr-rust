use super::*;
use crate::{
    archive::create_private_directory,
    executor::{Binding, Scope},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};
fn owner() -> Owner {
    Owner {scope:Scope{application_scope_id:"app".into(),end_user_id:"user".into(),authorization_revision:"1".into()},session_id:"session".into(),binding:serde_json::from_value::<Binding>(json!({"bindingId":"binding","revision":"1","target":{"executorId":"device","connectionId":"connection","connectionRevision":"1","workspaceId":"workspace","workspaceRevision":"1"}})).unwrap()}
}
#[test]
fn indexed_source_shared_patricia_vectors_and_corruption() {
    let v: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/authentication-tree-vectors.json"
    ))
    .unwrap();
    let cache = RefCell::new(BTreeMap::<String, Vec<u8>>::new());
    for step in v["steps"].as_array().unwrap() {
        let before = cache.borrow().len();
        let root = tree::put(
            step["root"].as_str(),
            step["key"].as_str().unwrap(),
            step["value"].as_str().unwrap(),
            |h| cache.borrow().get(h).cloned().ok_or_else(integrity),
            |h, b| {
                cache.borrow_mut().insert(h, b);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(root, step["expectedRoot"]);
        if step["expectedWrites"] == 0 {
            assert_eq!(cache.borrow().len(), before);
        }
        assert_eq!(
            tree::get(Some(&root), step["key"].as_str().unwrap(), |h| cache
                .borrow()
                .get(h)
                .cloned()
                .ok_or_else(integrity))
            .unwrap()
            .as_deref(),
            step["value"].as_str()
        );
    }
    for step in v["absence"].as_array().unwrap() {
        assert_eq!(
            tree::get(step["root"].as_str(), step["key"].as_str().unwrap(), |h| {
                cache.borrow().get(h).cloned().ok_or_else(integrity)
            })
            .unwrap(),
            None
        );
    }
    for (h, b) in v["nodes"].as_object().unwrap() {
        assert_eq!(&cache.borrow()[h], b.as_str().unwrap().as_bytes());
    }
    let root = v["steps"].as_array().unwrap().last().unwrap()["expectedRoot"]
        .as_str()
        .unwrap();
    assert!(
        tree::get(Some(root), v["keys"][0].as_str().unwrap(), |_| Err(
            integrity()
        ))
        .is_err()
    );
    assert!(
        tree::get(Some(root), v["keys"][0].as_str().unwrap(), |_| Ok(
            b"{}".to_vec()
        ))
        .is_err()
    );
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
    let p = std::fs::canonicalize(dir.path()).unwrap().join("private");
    create_private_directory(&p).unwrap();
    p.join("persistence")
}
fn hash(b: &[u8]) -> String {
    format!("{:x}", Sha256::digest(b))
}
fn canonical(v: &Value) -> Vec<u8> {
    crate::canonical::encode(v).unwrap()
}
fn req(action: &str, fields: Value) -> Value {
    let mut r = json!({"contract":CONTRACT,"action":action,"sourceId":"source","sourceGeneration":"1","domainKey":"domain"});
    for (k, v) in fields.as_object().unwrap() {
        r[k] = v.clone();
    }
    r
}
struct Plan {
    begin: Value,
    objects: BTreeMap<(String, String), Vec<u8>>,
}
fn plan(
    id: &str,
    body: &[u8],
    entries: &[Value],
    values: &[Vec<u8>],
    root: Option<&Value>,
) -> Plan {
    let mut objects = BTreeMap::new();
    let mut refs = Vec::new();
    for block in body.chunks(BLOCK_BYTES) {
        let h = hash(block);
        objects.insert(("body-block".into(), h.clone()), block.to_vec());
        refs.push(json!({"sha256":h,"byteLength":block.len()}));
    }
    let mut body_pages = Vec::new();
    for (i, refs) in refs.chunks(64).enumerate() {
        let b = canonical(&json!({"version":1,"kind":"body-page","index":i,"refs":refs}));
        let h = hash(&b);
        body_pages.push(h.clone());
        objects.insert(("body-page".into(), h), b);
    }
    let mut entries = entries.to_vec();
    entries.sort_by_key(|e| e["primaryKey"].as_str().unwrap().to_owned());
    let mut index_pages = Vec::new();
    for (i, entries) in entries.chunks(32).enumerate() {
        let b = canonical(&json!({"version":1,"kind":"index-page","index":i,"entries":entries}));
        let h = hash(&b);
        index_pages.push(h.clone());
        objects.insert(("index-page".into(), h), b);
    }
    for b in values {
        objects.insert(("receipt-value".into(), hash(b)), b.clone());
    }
    let expected=root.map(|r|json!({"commitRoot":r["commitRoot"],"generation":r["generation"],"bodyEtag":r["body"]["sha256"],"indexRoot":r["index"]["root"],"indexCount":r["index"]["count"]}));
    let mut begin = req(
        "begin",
        json!({"transferId":id,"expected":expected,"body":{"byteLength":body.len(),"sha256":hash(body),"blockCount":body.len().div_ceil(BLOCK_BYTES),"pageHashes":body_pages},"index":{"entryCount":entries.len(),"addedCount":entries.len(),"pageHashes":index_pages},"declared":{"objects":objects.len(),"bytes":objects.values().map(Vec::len).sum::<usize>()}}),
    );
    begin["intentSha256"] = json!(hash(&canonical(&begin)));
    Plan { begin, objects }
}
fn finish(p: &Plan, action: &str) -> Value {
    let mut request = req(
        action,
        json!({"transferId":p.begin["transferId"],"intentSha256":p.begin["intentSha256"]}),
    );
    for field in ["sourceId", "sourceGeneration", "domainKey"] {
        request[field] = p.begin[field].clone();
    }
    request
}
fn put(p: &Plan, kind: &str, digest: &str, b: &[u8]) -> Value {
    let mut r = finish(p, "put");
    r["kind"] = json!(kind);
    r["sha256"] = json!(digest);
    r["byteLength"] = json!(b.len());
    r["base64"] = json!(STANDARD.encode(b));
    r
}
async fn execute(s: &FileStore, v: Value) -> Value {
    let label = format!("{} {}", v["action"], v["kind"]);
    s.execute(v, owner())
        .await
        .unwrap_or_else(|e| panic!("{label}: {e:?}"))
}
async fn publish(s: &FileStore, p: &Plan) -> Value {
    execute(s, p.begin.clone()).await;
    for kind in ["body-page", "index-page", "body-block", "receipt-value"] {
        for ((k, h), b) in &p.objects {
            if k == kind {
                execute(s, put(p, k, h, b)).await;
            }
        }
    }
    execute(s, finish(p, "commit")).await["transfer"]["result"].clone()
}
fn entries(start: usize, count: usize) -> (Vec<Value>, Vec<Vec<u8>>) {
    let values = (start..start + count)
        .map(|n| canonical(&json!({"opaque":format!("sensitive-receipt-{n}")})))
        .collect::<Vec<_>>();
    let entries=values.iter().enumerate().map(|(i,v)|json!({"primaryKey":hash(format!("primary-{}",i+start).as_bytes()),"secondaryKey":hash(format!("secondary-{}",i+start).as_bytes()),"value":{"sha256":hash(v),"byteLength":v.len()}})).collect();
    (entries, values)
}
fn io(s: &FileStore) -> (usize, usize) {
    let guard = s.inner.lock().unwrap();
    guard.as_ref().unwrap().io.get()
}
fn reset_io(s: &FileStore) {
    let guard = s.inner.lock().unwrap();
    guard.as_ref().unwrap().io.set((0, 0));
}
#[tokio::test]
async fn indexed_source_active_proof_cost_and_freshness() {
    let mut measured = Vec::new();
    for blocks in [16, 32] {
        let dir = tempfile::tempdir().unwrap();
        let path = path(&dir);
        let s = FileStore::open_indexed(options(&path, OpenMode::Create))
            .await
            .unwrap();
        let mut body = vec![11; blocks * BLOCK_BYTES];
        for (i, chunk) in body.chunks_mut(BLOCK_BYTES).enumerate() {
            chunk[..4].copy_from_slice(&(i as u32).to_le_bytes());
        }
        let p = plan("active-proof-cost", &body, &[], &[], None);
        reset_io(&s);
        let started = std::time::Instant::now();
        execute(&s, p.begin.clone()).await;
        for kind in ["body-page", "body-block"] {
            for ((k, h), b) in &p.objects {
                if k == kind {
                    execute(&s, put(&p, k, h, b)).await;
                }
            }
        }
        let publication_io = io(&s);
        let publication_elapsed = started.elapsed();
        reset_io(&s);
        let started = std::time::Instant::now();
        let query = execute(&s, finish(&p, "query")).await;
        assert_eq!(query["transfer"]["status"], "staging");
        let query_io = io(&s);
        let query_elapsed = started.elapsed();
        println!(
            "active-proof blocks={blocks} body_bytes={} objects={} begin_put_io={publication_io:?} begin_put_ms={:.3} query_io={query_io:?} query_ms={:.3}",
            body.len(),
            p.objects.len(),
            publication_elapsed.as_secs_f64() * 1000.0,
            query_elapsed.as_secs_f64() * 1000.0
        );
        // A new call must reread the authenticated path, even on the same store.
        let root_path = {
            let guard = s.inner.lock().unwrap();
            let inner = guard.as_ref().unwrap();
            inner
                .index_path(
                    "node",
                    inner
                        .indexed
                        .as_ref()
                        .unwrap()
                        .head
                        .tree
                        .as_deref()
                        .unwrap(),
                )
                .unwrap()
        };
        let saved = std::fs::read(&root_path).unwrap();
        let mut damaged = saved.clone();
        *damaged.last_mut().unwrap() ^= 1;
        std::fs::write(&root_path, damaged).unwrap();
        assert!(s.execute(finish(&p, "query"), owner()).await.is_err());
        std::fs::write(&root_path, saved).unwrap();
        let root = execute(&s, finish(&p, "commit")).await["transfer"]["result"].clone();
        assert!(root.is_object());
        s.close().await.unwrap();
        let s = FileStore::open_indexed(options(&path, OpenMode::Reopen))
            .await
            .unwrap();
        assert_eq!(
            execute(&s, finish(&p, "query")).await["transfer"]["result"],
            root
        );
        s.close().await.unwrap();
        measured.push((query_io.0, p.objects.len()));
    }
    // Each current object needs its row and a path in a tree of 2*N-1 nodes;
    // the two independent header reads still guard the operation boundaries.
    for (reads, objects) in measured {
        assert!(
            reads <= 3 * objects + 1,
            "repeated active proof reads: {reads} for {objects} objects"
        );
    }
}
#[tokio::test]
async fn indexed_source_four_mib_complete_and_bounded_range() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let s = FileStore::open_indexed(options(&path, OpenMode::Create))
        .await
        .unwrap();
    let mut body = vec![11; 4 << 20];
    for (i, chunk) in body.chunks_mut(BLOCK_BYTES).enumerate() {
        chunk[..4].copy_from_slice(&(i as u32).to_le_bytes());
    }
    let p = plan("full-original-limit", &body, &[], &[], None);
    let root = publish(&s, &p).await;
    assert!(root.is_object());
    s.close().await.unwrap();
    let s = FileStore::open_indexed(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    let cold = io(&s);
    reset_io(&s);
    let value=execute(&s,req("read",json!({"commitRoot":root["commitRoot"],"part":"body","offset":BLOCK_BYTES-1,"length":2}))).await;
    assert_eq!(
        STANDARD.decode(value["base64"].as_str().unwrap()).unwrap(),
        body[BLOCK_BYTES - 1..BLOCK_BYTES + 1]
    );
    let range = io(&s);
    println!("four-mib full publication: cold={cold:?} two-block range={range:?}");
    assert!(cold.1 < 16_384);
    assert!(range.1 < 65_536);
    s.close().await.unwrap();
}
#[tokio::test]
async fn indexed_source_selected_missing_fact_is_not_absence() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let s = FileStore::open_indexed(options(&path, OpenMode::Create))
        .await
        .unwrap();
    let (e, v) = entries(0, 1);
    let p = plan("original", b"data", &e, &v, None);
    let root = publish(&s, &p).await;
    let selected = {
        let guard = s.inner.lock().unwrap();
        let inner = guard.as_ref().unwrap();
        let value = inner.index_row("ticket", "original").unwrap().unwrap();
        inner
            .index_path(
                "row",
                &hash(&envelope("ticket", "original", &value).unwrap()),
            )
            .unwrap()
    };
    s.close().await.unwrap();
    let saved = std::fs::read(&selected).unwrap();
    std::fs::remove_file(&selected).unwrap();
    let s = FileStore::open_indexed(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    assert_eq!(execute(&s, req("head", json!({}))).await["root"], root);
    let before = std::fs::read(&path).unwrap();
    assert!(s.execute(finish(&p, "query"), owner()).await.is_err());
    assert!(s.execute(p.begin.clone(), owner()).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(!selected.exists());
    s.close().await.unwrap();
    std::fs::write(&selected, saved).unwrap();
    let s = FileStore::open_indexed(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    assert_eq!(
        execute(&s, finish(&p, "query")).await["transfer"]["result"],
        root
    );
    s.close().await.unwrap();
}
#[tokio::test]
async fn indexed_source_budget_identity_and_four_mib_admission() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let s = FileStore::open_indexed(options(&path, OpenMode::Create))
        .await
        .unwrap();
    let mut bytes = vec![0; 4 << 20];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = ((i * 31 + i / 4096) % 251) as u8;
    }
    let p = plan("four-mib", &bytes, &[], &[], None);
    let r = execute(&s, p.begin.clone()).await;
    assert_eq!(r["transfer"]["status"], "staging");
    s.close().await.unwrap();
    let mut wrong = options(&path, OpenMode::Reopen);
    wrong.identity.domain_key = "foreign".into();
    assert!(FileStore::open_indexed(wrong).await.is_err());
    let s = FileStore::open_indexed(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    assert_eq!(
        execute(&s, finish(&p, "query")).await["transfer"]["status"],
        "staging"
    );
    s.run(|inner, before| {
        inner.indexed.as_mut().unwrap().burn.bytes = PHYSICAL_CAP - 1;
        inner.index_write_burn(before)
    })
    .await
    .unwrap();
    let original = std::fs::read(&path).unwrap();
    let ((k, h), v) = p
        .objects
        .iter()
        .find(|((k, _), _)| k == "body-page")
        .unwrap();
    assert!(s.execute(put(&p, k, h, v), owner()).await.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), original);
    s.close().await.unwrap();
    let s = FileStore::open_indexed(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    assert_eq!(
        execute(&s, finish(&p, "query")).await["transfer"]["status"],
        "staging"
    );
    s.close().await.unwrap();
}
#[tokio::test]
async fn indexed_source_range_only_reads_selected_blocks_and_layouts_reject() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let old = FileStore::open(options(&path, OpenMode::Create))
        .await
        .unwrap();
    old.close().await.unwrap();
    let snapshot = std::fs::read(&path).unwrap();
    assert!(
        FileStore::open_indexed(options(&path, OpenMode::Reopen))
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), snapshot);
    let dir2 = tempfile::tempdir().unwrap();
    let path2 = super_path(&dir2);
    let s = FileStore::open_indexed(options(&path2, OpenMode::Create))
        .await
        .unwrap();
    let mut body = vec![3; 2 * BLOCK_BYTES];
    body[BLOCK_BYTES..].fill(7);
    let p = plan("range", &body, &[], &[], None);
    let root = publish(&s, &p).await;
    s.close().await.unwrap();
    let second = path2.with_file_name(format!(
        "persistence.object.body-block.{}",
        hash(&body[BLOCK_BYTES..])
    ));
    let original = std::fs::read(&second).unwrap();
    std::fs::remove_file(&second).unwrap();
    let s = FileStore::open_indexed(options(&path2, OpenMode::Reopen))
        .await
        .unwrap();
    reset_io(&s);
    let result = execute(
        &s,
        req(
            "read",
            json!({"commitRoot":root["commitRoot"],"part":"body","offset":1,"length":2}),
        ),
    )
    .await;
    assert_eq!(
        STANDARD.decode(result["base64"].as_str().unwrap()).unwrap(),
        vec![3, 3]
    );
    let measured = io(&s);
    println!("selected-range bytes={} files={}", measured.1, measured.0);
    assert!(measured.1 < 32_768);
    assert!(s.execute(req("read",json!({"commitRoot":root["commitRoot"],"part":"body","offset":BLOCK_BYTES,"length":2})),owner()).await.is_err());
    s.close().await.unwrap();
    std::fs::write(second, original).unwrap();
}
#[tokio::test]
async fn indexed_source_scale_same_keys_and_first_write() {
    for count in [1, 64] {
        let dir = tempfile::tempdir().unwrap();
        let path = path(&dir);
        let s = FileStore::open_indexed(options(&path, OpenMode::Create))
            .await
            .unwrap();
        let (mut e, mut v) = entries(0, count);
        for (n, bytes) in v.iter_mut().enumerate() {
            *bytes = format!("{n}:{}", "x".repeat(8190)).into_bytes();
            e[n]["value"] = json!({"sha256":hash(bytes),"byteLength":bytes.len()});
        }
        let p = plan("original", b"current working set", &e, &v, None);
        let root = publish(&s, &p).await;
        assert_eq!(root["index"]["count"], count.to_string());
        s.close().await.unwrap();
        assert!(
            FileStore::open(options(&path, OpenMode::Reopen))
                .await
                .is_err()
        );
        let s = FileStore::open_indexed(options(&path, OpenMode::Reopen))
            .await
            .unwrap();
        let cold = io(&s);
        reset_io(&s);
        let head = execute(&s, req("head", json!({}))).await;
        assert_eq!(head["root"], root);
        let query = execute(&s, finish(&p, "query")).await;
        assert_eq!(query["transfer"]["result"], root);
        for kind in ["primary", "secondary"] {
            let lookup=execute(&s,req("lookup",json!({"commitRoot":root["commitRoot"],"key":{"kind":kind,"digest":e[0][format!("{kind}Key")]}}))).await;
            assert_eq!(lookup["entry"]["primaryKey"], e[0]["primaryKey"]);
        }
        let read = io(&s);
        reset_io(&s);
        let p2 = plan("next", b"new working set", &[], &[], Some(&root));
        let next = publish(&s, &p2).await;
        assert_eq!(next["index"]["count"], count.to_string());
        let write = io(&s);
        println!(
            "R={count} unrelated-value-bytes={} cold={cold:?} first-head-query-dual-lookup={read:?} first-write={write:?}",
            v.iter().map(Vec::len).sum::<usize>()
        );
        assert!(cold.1 < 16_384);
        assert!(read.1 < 100_000);
        assert!(write.1 < 500_000);
        s.run(|inner, _| inner.index_complete()).await.unwrap();
        s.close().await.unwrap();
    }
}
#[tokio::test]
async fn indexed_source_lost_commit_and_object_reply_reopen_original_keys_with_unchanged_index() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let armed = Arc::new(AtomicBool::new(false));
    let baseline = Arc::new(Mutex::new(Vec::new()));
    let mut o = options(&path, OpenMode::Create);
    let (a, b, p) = (armed.clone(), baseline.clone(), path.clone());
    o.read_context = Arc::new(move || {
        if a.load(Ordering::SeqCst) && std::fs::read(&p).unwrap_or_default() != *b.lock().unwrap() {
            return Err(crate::Error::Cancelled);
        }
        Ok(owner())
    });
    let s = FileStore::open_indexed(o).await.unwrap();
    let (e, v) = entries(0, 1);
    let plan = plan("lost-commit", b"body", &e, &v, None);
    execute(&s, plan.begin.clone()).await;
    for kind in ["body-page", "index-page", "body-block", "receipt-value"] {
        for ((k, h), v) in &plan.objects {
            if k == kind {
                execute(&s, put(&plan, k, h, v)).await;
            }
        }
    }
    *baseline.lock().unwrap() = std::fs::read(&path).unwrap();
    armed.store(true, Ordering::SeqCst);
    assert!(matches!(
        s.execute(finish(&plan, "commit"), owner()).await,
        Err(crate::Error::Unknown(_))
    ));
    armed.store(false, Ordering::SeqCst);
    s.close().await.unwrap();
    let s = FileStore::open_indexed(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    let receipt = execute(&s, finish(&plan, "query")).await;
    assert_eq!(receipt["transfer"]["status"], "committed");
    assert_eq!(receipt["transfer"]["result"]["index"]["count"], "1");
    assert_eq!(
        execute(&s, finish(&plan, "commit")).await["transfer"],
        receipt["transfer"]
    );
    s.close().await.unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = super_path(&dir);
    let p = plan_with_no_entries("lost-object", b"object-loss");
    let object = p
        .objects
        .iter()
        .find(|((k, _), _)| k == "body-block")
        .unwrap();
    let object_path = path.with_file_name(format!("persistence.object.body-block.{}", object.0.1));
    let armed = Arc::new(AtomicBool::new(false));
    let a = armed.clone();
    let q = object_path.clone();
    let mut o = options(&path, OpenMode::Create);
    o.read_context = Arc::new(move || {
        if a.load(Ordering::SeqCst) && q.exists() {
            return Err(crate::Error::Cancelled);
        }
        Ok(owner())
    });
    let s = FileStore::open_indexed(o).await.unwrap();
    execute(&s, p.begin.clone()).await;
    for ((k, h), v) in &p.objects {
        if k == "body-page" {
            execute(&s, put(&p, k, h, v)).await;
        }
    }
    armed.store(true, Ordering::SeqCst);
    assert!(matches!(
        s.execute(put(&p, &object.0.0, &object.0.1, object.1), owner())
            .await,
        Err(crate::Error::Unknown(_))
    ));
    armed.store(false, Ordering::SeqCst);
    s.close().await.unwrap();
    let s = FileStore::open_indexed(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    let q = execute(&s, finish(&p, "query")).await;
    assert_eq!(q["transfer"]["progress"]["bodyReady"], "AQ==");
    s.compact().await.unwrap();
    assert!(
        object_path.exists(),
        "durably received object must remain charged/protected after lost reply"
    );
    execute(&s, put(&p, &object.0.0, &object.0.1, object.1)).await;
    assert_eq!(
        execute(&s, finish(&p, "commit")).await["transfer"]["status"],
        "committed"
    );
    s.close().await.unwrap();
}
// Helpers avoid local variable names shadowing the fixture constructors.
fn super_path(dir: &tempfile::TempDir) -> std::path::PathBuf {
    path(dir)
}
fn plan_with_no_entries(id: &str, body: &[u8]) -> Plan {
    plan(id, body, &[], &[], None)
}

fn copy_source_bytes(path: &Path) -> BTreeMap<String, Vec<u8>> {
    let name = path.file_name().unwrap().to_str().unwrap();
    std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|result| result.unwrap())
        .filter_map(|entry| {
            let n = entry.file_name().into_string().unwrap();
            if n == name
                || (n.starts_with(&format!("{name}.")) && n != format!("{name}.lock"))
                || n.starts_with(&format!(".{name}."))
            {
                Some((n, std::fs::read(entry.path()).unwrap()))
            } else {
                None
            }
        })
        .collect()
}
#[tokio::test]
async fn indexed_source_copy_preserves_root_both_indexes_original_pending_and_durable_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let s = FileStore::open_indexed(options(&path, OpenMode::Create))
        .await
        .unwrap();
    let (e, v) = entries(0, 2);
    let first = plan("copy-first", b"old opaque body", &e, &v, None);
    let root1 = publish(&s, &first).await;
    let second = plan(
        "copy-second",
        b"current opaque body",
        &[],
        &[],
        Some(&root1),
    );
    let root2 = publish(&s, &second).await;
    let pending = plan(
        "copy-pending",
        b"unfinished next body",
        &[],
        &[],
        Some(&root2),
    );
    execute(&s, pending.begin.clone()).await;
    let ((k, h), b) = pending
        .objects
        .iter()
        .find(|((k, _), _)| k == "body-page")
        .unwrap();
    let pending_put = put(&pending, k, h, b);
    execute(&s, pending_put.clone()).await;
    let query_before = execute(&s, finish(&pending, "query")).await;
    let head_before = execute(&s, req("head", json!({}))).await;
    let original = copy_source_bytes(&path);
    let target = path.parent().unwrap().join("rotated").join("store");
    let receipt = s.copy_to(target.clone(), [9; 32]).await.unwrap();
    assert!(receipt.read_only);
    assert_eq!(receipt.cutover, CopyCutover::Pending);
    assert_eq!(receipt.source_sha256, hash(&original["persistence"]));
    assert_eq!(
        receipt.destination_sha256,
        hash(&std::fs::read(&target).unwrap())
    );
    assert_eq!(original, copy_source_bytes(&path));
    for wrong in [false, true] {
        let mut o = options(&target, OpenMode::Reopen);
        o.key = if wrong { [9; 32] } else { [7; 32] };
        if wrong {
            o.identity.domain_key = "another-domain".into();
        }
        assert!(FileStore::open_indexed(o).await.is_err());
    }
    for _ in 0..2 {
        let mut o = options(&target, OpenMode::Reopen);
        o.key = [9; 32];
        let copied = FileStore::open_indexed(o).await.unwrap();
        assert_eq!(execute(&copied, req("head", json!({}))).await, head_before);
        assert_eq!(
            execute(&copied, finish(&pending, "query")).await,
            query_before
        );
        assert_eq!(
            execute(&copied, finish(&first, "query")).await["transfer"]["result"],
            root1
        );
        for entry in &e {
            for kind in ["primary", "secondary"] {
                let field = format!("{kind}Key");
                let input = req(
                    "lookup",
                    json!({"commitRoot":root2["commitRoot"],"key":{"kind":kind,"digest":entry[&field]}}),
                );
                assert_eq!(
                    execute(&copied, input.clone()).await,
                    execute(&s, input).await
                );
            }
        }
        let input = req(
            "read",
            json!({"part":"body","commitRoot":root2["commitRoot"],"offset":0,"length":12288}),
        );
        assert_eq!(
            execute(&copied, input.clone()).await,
            execute(&s, input).await
        );
        for mutation in [
            first.begin.clone(),
            pending_put.clone(),
            finish(&first, "commit"),
        ] {
            assert!(
                copied
                    .execute(mutation, owner())
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("copy_read_only")
            );
        }
        assert!(
            copied
                .compact()
                .await
                .unwrap_err()
                .to_string()
                .contains("copy_read_only")
        );
        copied.close().await.unwrap();
    }
    let mut create = options(&target, OpenMode::Create);
    create.key = [9; 32];
    assert!(FileStore::open_indexed(create).await.is_err());
    assert_eq!(
        receipt.destination_sha256,
        hash(&std::fs::read(&target).unwrap())
    );
    assert_eq!(original, copy_source_bytes(&path));
    // No source fence is invented: the legitimate original can still settle its
    // original ticket, while the independent copy stays permanently read-only.
    for ((k, h), b) in &pending.objects {
        execute(&s, put(&pending, k, h, b)).await;
    }
    execute(&s, finish(&pending, "commit")).await;
    s.close().await.unwrap();
}
