use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};
use tansr_sdk::{
    archive::create_private_directory,
    executor::{Binding, Scope},
    terminal_persistence::*,
};
fn owner() -> Owner {
    Owner {scope:Scope{application_scope_id:"app".into(),end_user_id:"user".into(),authorization_revision:"1".into()},session_id:"session".into(),binding:serde_json::from_value::<Binding>(json!({"bindingId":"binding","revision":"1","target":{"executorId":"device","connectionId":"connection","connectionRevision":"1","workspaceId":"workspace","workspaceRevision":"1"}})).unwrap()}
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
    tansr_sdk::canonical::encode(v).unwrap()
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
    s.execute(v, owner()).await.unwrap()
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
#[test]
fn frozen_v1_schema_and_golden_leave_legacy_contract_unchanged() {
    let g: Value = serde_json::from_str(include_str!(
        "../src/terminal_persistence/assets/terminal-persistence-v1.golden.json"
    ))
    .unwrap();
    assert_eq!(
        g["schemaSha256"],
        hash(include_bytes!(
            "../src/terminal_persistence/assets/terminal-persistence-v1.schema.json"
        ))
    );
    assert_eq!(g["profile"]["definitionSha256"], TOOL_DIGEST);
    for v in g["positive"].as_array().unwrap() {
        tansr_sdk::api::validate_wire(CONTRACT, v["definition"].as_str().unwrap(), &v["value"])
            .unwrap_or_else(|e| panic!("{}: {e}", v["id"]));
    }
    for v in g["negative"].as_array().unwrap() {
        assert!(
            tansr_sdk::api::validate_wire(CONTRACT, v["definition"].as_str().unwrap(), &v["value"])
                .is_err(),
            "{}",
            v["id"]
        );
    }
}
#[tokio::test]
async fn atomic_root_indexes_original_transfer_reopen_and_opaque_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let s = FileStore::open(options(&path, OpenMode::Create))
        .await
        .unwrap();
    let (e, v) = entries(0, 2);
    let p = plan("one", b"opaque publication", &e, &v, None);
    let root = publish(&s, &p).await;
    assert_eq!(root["index"]["count"], "2");
    assert_ne!(root["commitRoot"], root["body"]["sha256"]);
    let unknown = plan("unknown", b"x", &[], &[], Some(&root));
    assert_eq!(
        execute(&s, finish(&unknown, "query")).await["transfer"]["status"],
        "unknown"
    );
    s.close().await.unwrap();
    let s = FileStore::open(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    assert_eq!(
        execute(&s, finish(&p, "query")).await["transfer"]["result"],
        root
    );
    for entry in &e {
        for kind in ["primary", "secondary"] {
            let field = format!("{kind}Key");
            let result=execute(&s,req("lookup",json!({"commitRoot":root["commitRoot"],"key":{"kind":kind,"digest":entry[&field]}}))).await;
            assert_eq!(result["entry"]["primaryKey"], entry["primaryKey"]);
            assert_eq!(
                hash(
                    &STANDARD
                        .decode(result["entry"]["base64"].as_str().unwrap())
                        .unwrap()
                ),
                entry["value"]["sha256"]
            );
        }
    }
    let p2 = plan("two", b"next", &[], &[], Some(&root));
    let second = publish(&s, &p2).await;
    assert_eq!(
        execute(&s, finish(&p, "query")).await["transfer"]["result"],
        root
    );
    assert!(
        s.execute(
            req(
                "read",
                json!({"part":"body","commitRoot":root["commitRoot"],"offset":0,"length":1})
            ),
            owner()
        )
        .await
        .is_err()
    );
    let result = execute(
        &s,
        req(
            "read",
            json!({"part":"body","commitRoot":second["commitRoot"],"offset":0,"length":12288}),
        ),
    )
    .await;
    assert_eq!(
        STANDARD.decode(result["base64"].as_str().unwrap()).unwrap(),
        b"next"
    );
    assert_eq!(
        execute(&s, finish(&p, "commit")).await["transfer"]["result"],
        root
    );
    s.close().await.unwrap();
    for f in std::fs::read_dir(path.parent().unwrap()).unwrap() {
        let b = std::fs::read(f.unwrap().path()).unwrap();
        for needle in [
            b"opaque publication".as_slice(),
            b"sensitive-receipt-0",
            &[7u8; 32],
        ] {
            assert!(!b.windows(needle.len()).any(|x| x == needle));
        }
    }
}
#[tokio::test]
async fn conflict_capacity_partial_reopen_and_current_owner_checks() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let current = Arc::new(Mutex::new(owner()));
    let mut o = options(&path, OpenMode::Create);
    let c = current.clone();
    o.read_context = Arc::new(move || Ok(c.lock().unwrap().clone()));
    let s = FileStore::open(o).await.unwrap();
    let p = plan("first", b"a", &[], &[], None);
    execute(&s, p.begin.clone()).await;
    assert!(
        FileStore::open(options(&path, OpenMode::Reopen))
            .await
            .is_err()
    );
    let page = p
        .objects
        .iter()
        .find(|((k, _), _)| k == "body-page")
        .unwrap();
    execute(&s, put(&p, &page.0.0, &page.0.1, page.1)).await;
    let before = execute(&s, finish(&p, "query")).await;
    s.close().await.unwrap();
    let s = FileStore::open(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    assert_eq!(execute(&s, finish(&p, "query")).await, before);
    let mut changed = p.begin.clone();
    changed["intentSha256"] = json!("f".repeat(64));
    assert!(s.execute(changed, owner()).await.is_err());
    let root = publish(&s, &p).await;
    let stale = plan("stale", b"b", &[], &[], None);
    let result = execute(&s, stale.begin.clone()).await;
    assert_eq!(result["transfer"]["status"], "rejected");
    assert_eq!(result["transfer"]["rejection"]["code"], "revision_conflict");
    let mut other = owner();
    other.scope.end_user_id = "other".into();
    assert!(s.execute(req("head", json!({})), other).await.is_err());
    assert_eq!(execute(&s, req("head", json!({}))).await["root"], root);
    s.close().await.unwrap();
    let mut wrong = options(&path, OpenMode::Reopen);
    wrong.key = [8; 32];
    assert!(FileStore::open(wrong).await.is_err());
    let original = std::fs::read(&path).unwrap();
    let mut truncated = original.clone();
    truncated.pop();
    std::fs::write(&path, &truncated).unwrap();
    assert!(
        FileStore::open(options(&path, OpenMode::Reopen))
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), truncated);
    std::fs::write(&path, original).unwrap();
}
#[tokio::test]
async fn batches_257_513_and_four_mib_single_block_delta_keep_permanent_keys() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let mut s = FileStore::open(options(&path, OpenMode::Create))
        .await
        .unwrap();
    let mut body = vec![b'x'; MAX_BODY];
    for (i, b) in body.chunks_mut(BLOCK_BYTES).enumerate() {
        let stamp = format!("{i:08}");
        b[..8].copy_from_slice(stamp.as_bytes());
    }
    let (e1, v1) = entries(0, 256);
    let first = plan("batch-256", &body, &e1, &v1, None);
    let started = std::time::Instant::now();
    let mut root = publish(&s, &first).await;
    eprintln!(
        "v1 4MiB + 256 entries committed after {:?}",
        started.elapsed()
    );
    assert_eq!(root["index"]["count"], "256");
    let (e2, v2) = entries(256, 1);
    let second = plan("batch-257", &body, &e2, &v2, Some(&root));
    root = publish(&s, &second).await;
    assert_eq!(root["index"]["count"], "257");
    s.close().await.unwrap();
    s = FileStore::open(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    let (e3, v3) = entries(257, 256);
    let third = plan("batch-513", &body, &e3, &v3, Some(&root));
    root = publish(&s, &third).await;
    assert_eq!(root["index"]["count"], "513");
    let before = execute(&s, req("head", json!({}))).await;
    body[2 * BLOCK_BYTES + 90] = b'y';
    let delta = plan("one-block-delta", &body, &[], &[], Some(&root));
    let initial = execute(&s, delta.begin.clone()).await;
    let page_bits = STANDARD
        .decode(
            initial["transfer"]["progress"]["pagesReady"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(page_bits[0] & 63, 62);
    let mut sent = 0;
    for ((kind, h), b) in &delta.objects {
        if kind == "body-page" && !first.objects.contains_key(&(kind.clone(), h.clone())) {
            execute(&s, put(&delta, kind, h, b)).await;
            sent += b.len();
        }
    }
    let progress = execute(&s, finish(&delta, "query")).await;
    let ready = STANDARD
        .decode(
            progress["transfer"]["progress"]["bodyReady"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(ready[0] & 4, 0);
    assert_eq!(ready.iter().map(|v| v.count_ones()).sum::<u32>(), 341);
    for ((kind, h), b) in &delta.objects {
        if kind == "body-block" && !first.objects.contains_key(&(kind.clone(), h.clone())) {
            execute(&s, put(&delta, kind, h, b)).await;
            sent += b.len();
        }
    }
    eprintln!(
        "v1 513 entries + one-block delta: fullBytes={},deltaBytes={},elapsed={:?}",
        body.len(),
        sent,
        started.elapsed()
    );
    assert!(sent < 25_000, "delta uploaded {sent}, full {}", body.len());
    let done = execute(&s, finish(&delta, "commit")).await;
    assert_eq!(done["transfer"]["status"], "committed");
    assert_eq!(done["transfer"]["result"]["index"]["count"], "513");
    let after = execute(&s, req("head", json!({}))).await;
    assert_eq!(
        before["capacity"]["used"]["objects"],
        after["capacity"]["used"]["objects"]
    );
    for p in [&first, &second, &third] {
        assert_eq!(
            execute(&s, finish(p, "query")).await["transfer"]["status"],
            "committed"
        );
    }
    s.close().await.unwrap();
    let s = FileStore::open(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    assert_eq!(
        execute(&s, req("head", json!({}))).await["root"],
        done["transfer"]["result"]
    );
    s.close().await.unwrap();
}

#[tokio::test]
async fn lost_commit_and_object_reply_reopen_original_keys_with_unchanged_index() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let armed = Arc::new(AtomicBool::new(false));
    let baseline = Arc::new(Mutex::new(Vec::new()));
    let mut o = options(&path, OpenMode::Create);
    let (a, b, p) = (armed.clone(), baseline.clone(), path.clone());
    o.read_context = Arc::new(move || {
        if a.load(Ordering::SeqCst) && std::fs::read(&p).unwrap_or_default() != *b.lock().unwrap() {
            return Err(tansr_sdk::Error::Cancelled);
        }
        Ok(owner())
    });
    let s = FileStore::open(o).await.unwrap();
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
        Err(tansr_sdk::Error::Unknown(_))
    ));
    armed.store(false, Ordering::SeqCst);
    s.close().await.unwrap();
    let s = FileStore::open(options(&path, OpenMode::Reopen))
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
            return Err(tansr_sdk::Error::Cancelled);
        }
        Ok(owner())
    });
    let s = FileStore::open(o).await.unwrap();
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
        Err(tansr_sdk::Error::Unknown(_))
    ));
    armed.store(false, Ordering::SeqCst);
    s.close().await.unwrap();
    let s = FileStore::open(options(&path, OpenMode::Reopen))
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

#[tokio::test]
async fn permanent_capacity_query_only_recovery_and_fixed_plan_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let mut o = options(&path, OpenMode::Create);
    o.limits.transfer_facts = 1;
    let s = FileStore::open(o).await.unwrap();
    let p = plan_with_no_entries("permanent", b"a");
    let root = publish(&s, &p).await;
    let next = plan("capacity", b"b", &[], &[], Some(&root));
    assert!(s.execute(next.begin, owner()).await.is_err());
    assert_eq!(
        execute(&s, finish(&p, "query")).await["transfer"]["status"],
        "committed"
    );
    s.close().await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = super_path(&dir);
    let s = FileStore::open(options(&path, OpenMode::Create))
        .await
        .unwrap();
    let p = plan_with_no_entries("owner", b"a");
    execute(&s, p.begin.clone()).await;
    s.close().await.unwrap();
    let mut current = owner();
    current.binding.target.connection_revision = "2".into();
    let c = current.clone();
    let mut o = options(&path, OpenMode::Reopen);
    o.read_context = Arc::new(move || Ok(c.clone()));
    o.authorize_recovery = Some(Arc::new(|_, _, _, _| true));
    let s = FileStore::open(o).await.unwrap();
    assert_eq!(
        s.execute(finish(&p, "query"), current.clone())
            .await
            .unwrap()["transfer"]["status"],
        "staging"
    );
    assert!(s.execute(finish(&p, "commit"), current).await.is_err());
    s.close().await.unwrap();
}

#[tokio::test]
async fn host_checks_original_operation_capability_and_after_call_authority() {
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tansr_sdk::{
        CancellationToken,
        executor::{
            Authorizer, Operation, Resource, ToolContext, ToolError, ToolHandler, operation_digest,
        },
    };
    struct Policy(Arc<AtomicBool>);
    #[async_trait]
    impl Authorizer for Policy {
        async fn authorize(&self, op: &Operation) -> tansr_sdk::Result<()> {
            if !self.0.load(Ordering::SeqCst) || Owner::from_operation(op) != owner() {
                Err(tansr_sdk::Error::Cancelled)
            } else {
                Ok(())
            }
        }
    }
    struct Adapter {
        identity: Identity,
        calls: AtomicUsize,
        allowed: Arc<AtomicBool>,
        flip: bool,
    }
    #[async_trait]
    impl PersistenceStore for Adapter {
        fn identity(&self) -> &Identity {
            &self.identity
        }
        fn encrypted_at_rest(&self) -> bool {
            true
        }
        fn atomic_durable_publication(&self) -> bool {
            true
        }
        async fn execute(&self, r: Value, _: Owner) -> tansr_sdk::Result<Value> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.flip {
                self.allowed.store(false, Ordering::SeqCst);
            }
            let mut v = r;
            v["root"] = Value::Null;
            v["capacity"] = json!({"limits":Limits::default(),"used":{"activeTransfers":0,"stagingBytes":0,"receiptEntries":0,"transferFacts":0,"objects":0,"retainedBytes":0,"reservedBytes":0,"reservedObjects":0,"reservedReceiptEntries":0}});
            Ok(v)
        }
    }
    let args = req("head", json!({}));
    let own = owner();
    let mut op = Operation {
        protocol: "sdk2-ext-v1".into(),
        operation_id: "op".into(),
        session_id: own.session_id,
        scope: own.scope,
        binding: own.binding,
        tool_name: "MemoryPublication".into(),
        request: Resource {
            operation: "tool.invoke".into(),
            args: json!({"name":TOOL_NAME,"definitionDigest":TOOL_DIGEST,"argsJson":String::from_utf8(canonical(&args)).unwrap()}),
        },
        digest: String::new(),
        expires_at: "2099-01-01T00:00:00Z".into(),
    };
    op.digest = operation_digest(&op).unwrap();
    let allowed = Arc::new(AtomicBool::new(true));
    let adapter = Arc::new(Adapter {
        identity: identity(),
        calls: AtomicUsize::new(0),
        allowed: allowed.clone(),
        flip: false,
    });
    let host = Host::new(adapter.clone(), Arc::new(Policy(allowed.clone()))).unwrap();
    let ctx = || ToolContext {
        cancellation: CancellationToken::new(),
        output: None,
    };
    assert!(
        host.invoke_operation(ctx(), &op, args.clone())
            .await
            .is_ok()
    );
    let mut substituted = op.clone();
    substituted.request.args["name"] = json!("BusinessTool");
    substituted.digest = operation_digest(&substituted).unwrap();
    assert!(matches!(
        host.invoke_operation(ctx(), &substituted, args.clone())
            .await,
        Err(ToolError::Rejected(_))
    ));
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    let adapter = Arc::new(Adapter {
        identity: identity(),
        calls: AtomicUsize::new(0),
        allowed: allowed.clone(),
        flip: true,
    });
    let host = Host::new(adapter, Arc::new(Policy(allowed))).unwrap();
    assert!(matches!(
        host.invoke_operation(ctx(), &op, args).await,
        Err(ToolError::Unknown(_))
    ));
}

#[tokio::test]
async fn normative_golden_commit_and_actual_opaque_body_recipe() {
    let g: Value = serde_json::from_str(include_str!(
        "../src/terminal_persistence/assets/terminal-persistence-v1.golden.json"
    ))
    .unwrap();
    for v in g["semantic"]["hashVectors"].as_array().unwrap() {
        assert_eq!(
            canonical(&v["preimage"]),
            v["canonicalUtf8"].as_str().unwrap().as_bytes()
        );
        assert_eq!(hash(&canonical(&v["preimage"])), v["sha256"]);
    }
    let mut recipe = (0..MAX_BODY).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    let expected = &g["semantic"]["deltaRecipe"];
    assert_eq!(hash(&recipe), expected["sha256"]);
    let p = plan_with_no_entries("recipe", &recipe);
    assert_eq!(p.begin["body"]["pageHashes"], expected["pageHashes"]);
    for byte in &mut recipe[BLOCK_BYTES..2 * BLOCK_BYTES] {
        *byte ^= 1;
    }
    assert_eq!(hash(&recipe), expected["change"]["sha256"]);
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let mut o = options(&path, OpenMode::Create);
    o.identity = serde_json::from_value(g["semantic"]["identity"].clone()).unwrap();
    let mut own = owner();
    own.scope.application_scope_id = o.identity.application_scope_id.clone();
    own.scope.end_user_id = o.identity.end_user_id.clone();
    let c = own.clone();
    o.read_context = Arc::new(move || Ok(c.clone()));
    let s = FileStore::open(o).await.unwrap();
    let begin = g["semantic"]["begin"].clone();
    s.execute(begin.clone(), own.clone()).await.unwrap();
    let mut small = plan_with_no_entries("unused", b"hello");
    small.begin = begin;
    for kind in ["body-page", "body-block"] {
        for ((k, h), b) in &small.objects {
            if k == kind {
                s.execute(put(&small, k, h, b), own.clone()).await.unwrap();
            }
        }
    }
    assert_eq!(
        s.execute(finish(&small, "commit"), own).await.unwrap()["transfer"],
        g["semantic"]["committed"]
    );
    s.close().await.unwrap();
}

#[tokio::test]
async fn interrupted_retirement_never_restores_old_body_or_drops_permanent_result() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let old_object = path.with_file_name(format!("persistence.object.body-block.{}", hash(b"old")));
    let armed = Arc::new(AtomicBool::new(false));
    let a = armed.clone();
    let old = old_object.clone();
    let mut o = options(&path, OpenMode::Create);
    o.read_context = Arc::new(move || {
        if a.load(Ordering::SeqCst) && !old.exists() {
            return Err(tansr_sdk::Error::Cancelled);
        }
        Ok(owner())
    });
    let s = FileStore::open(o).await.unwrap();
    let first = plan_with_no_entries("old-root", b"old");
    let root = publish(&s, &first).await;
    let second = plan("new-root", b"new", &[], &[], Some(&root));
    execute(&s, second.begin.clone()).await;
    for kind in ["body-page", "body-block"] {
        for ((k, h), b) in &second.objects {
            if k == kind {
                execute(&s, put(&second, k, h, b)).await;
            }
        }
    }
    armed.store(true, Ordering::SeqCst);
    assert!(matches!(
        s.execute(finish(&second, "commit"), owner()).await,
        Err(tansr_sdk::Error::Unknown(_))
    ));
    assert!(!old_object.exists());
    armed.store(false, Ordering::SeqCst);
    s.close().await.unwrap();
    let s = FileStore::open(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    assert_eq!(
        execute(&s, finish(&first, "query")).await["transfer"]["result"],
        root
    );
    let final_root = execute(&s, finish(&second, "query")).await["transfer"]["result"].clone();
    assert_eq!(final_root["body"]["sha256"], hash(b"new"));
    s.compact().await.unwrap();
    assert!(!old_object.exists());
    assert_eq!(
        execute(&s, req("head", json!({}))).await["root"],
        final_root
    );
    s.close().await.unwrap();
}

#[tokio::test]
async fn conflicting_permanent_keys_and_active_base_gc_are_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let s = FileStore::open(options(&path, OpenMode::Create))
        .await
        .unwrap();
    let (initial, values) = entries(0, 1);
    let original = plan("permanent-original", b"old-base", &initial, &values, None);
    let root = publish(&s, &original).await;
    let pending = plan("pending-base", b"unreceived", &[], &[], Some(&root));
    execute(&s, pending.begin.clone()).await;
    let old_path = path.with_file_name(format!(
        "persistence.object.body-block.{}",
        hash(b"old-base")
    ));
    let latest = plan("latest", b"new-base", &[], &[], Some(&root));
    let latest_root = publish(&s, &latest).await;
    assert!(old_path.exists(), "active expected root remains protected");
    let rejected = execute(&s, finish(&pending, "commit")).await;
    assert_eq!(
        rejected["transfer"]["rejection"]["code"],
        "revision_conflict"
    );
    assert!(!old_path.exists(), "retirement follows durable rejection");
    for field in ["primaryKey", "secondaryKey"] {
        let (mut conflicting, new_values) = entries(1, 1);
        conflicting[0][field] = initial[0][field].clone();
        let p = plan(
            field,
            b"new-base",
            &conflicting,
            &new_values,
            Some(&latest_root),
        );
        execute(&s, p.begin.clone()).await;
        for ((k, h), bytes) in &p.objects {
            if k == "index-page" {
                execute(&s, put(&p, k, h, bytes)).await;
            }
        }
        let transfer = execute(&s, finish(&p, "query")).await["transfer"].clone();
        assert_eq!(transfer["status"], "rejected");
        assert_eq!(transfer["rejection"]["code"], "request_conflict");
        assert_eq!(
            execute(&s, finish(&p, "commit")).await["transfer"],
            transfer
        );
    }
    s.close().await.unwrap();
    let s = FileStore::open(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    let head = execute(&s, req("head", json!({}))).await;
    assert_eq!(head["root"], latest_root);
    assert_eq!(head["capacity"]["used"]["receiptEntries"], 1);
    let result = execute(&s, req("lookup", json!({"commitRoot":latest_root["commitRoot"],"key":{"kind":"secondary","digest":initial[0]["secondaryKey"]}}))).await;
    assert_eq!(
        STANDARD
            .decode(result["entry"]["base64"].as_str().unwrap())
            .unwrap(),
        values[0]
    );
    s.close().await.unwrap();
}

#[tokio::test]
async fn immutable_object_tamper_and_wrong_aad_reject_without_replacing_media() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let s = FileStore::open(options(&path, OpenMode::Create))
        .await
        .unwrap();
    let p = plan_with_no_entries("cipher-original", &[0xff, 0x00, 0x81]);
    let root = publish(&s, &p).await;
    s.close().await.unwrap();
    let block_path = path.with_file_name(format!(
        "persistence.object.body-block.{}",
        hash(&[0xff, 0, 0x81])
    ));
    let page = p.objects.keys().find(|(k, _)| k == "body-page").unwrap();
    let page_path = path.with_file_name(format!("persistence.object.body-page.{}", page.1));
    let original = std::fs::read(&block_path).unwrap();
    let wrong_aad = std::fs::read(&page_path).unwrap();
    let mut corrupted = original.clone();
    *corrupted.last_mut().unwrap() ^= 1;
    for invalid in [
        corrupted,
        original[..original.len() - 1].to_vec(),
        wrong_aad,
    ] {
        std::fs::write(&block_path, &invalid).unwrap();
        assert!(
            FileStore::open(options(&path, OpenMode::Reopen))
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&block_path).unwrap(), invalid);
    }
    std::fs::write(&block_path, original).unwrap();
    let s = FileStore::open(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    assert_eq!(
        execute(&s, finish(&p, "query")).await["transfer"]["result"],
        root
    );
    s.close().await.unwrap();
}

#[tokio::test]
async fn lost_object_reply_does_not_double_charge_the_reserved_last_slot() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let body = b"last-slot";
    let object = path.with_file_name(format!("persistence.object.body-block.{}", hash(body)));
    let armed = Arc::new(AtomicBool::new(false));
    let a = armed.clone();
    let q = object.clone();
    let mut o = options(&path, OpenMode::Create);
    o.limits.objects = 2;
    o.read_context = Arc::new(move || {
        if a.load(Ordering::SeqCst) && q.exists() {
            return Err(tansr_sdk::Error::Cancelled);
        }
        Ok(owner())
    });
    let s = FileStore::open(o).await.unwrap();
    let p = plan_with_no_entries("tight-last-slot", body);
    execute(&s, p.begin.clone()).await;
    for ((k, h), b) in &p.objects {
        if k == "body-page" {
            execute(&s, put(&p, k, h, b)).await;
        }
    }
    armed.store(true, Ordering::SeqCst);
    assert!(
        s.execute(put(&p, "body-block", &hash(body), body), owner())
            .await
            .is_err()
    );
    armed.store(false, Ordering::SeqCst);
    s.close().await.unwrap();
    let mut o = options(&path, OpenMode::Reopen);
    o.limits.objects = 2;
    let s = FileStore::open(o).await.unwrap();
    let capacity = execute(&s, req("head", json!({}))).await["capacity"].clone();
    assert!(
        capacity["used"]["objects"].as_u64().unwrap()
            + capacity["used"]["reservedObjects"].as_u64().unwrap()
            <= 2,
        "accepted last slot must not be charged twice: {capacity}"
    );
    execute(&s, put(&p, "body-block", &hash(body), body)).await;
    assert_eq!(
        execute(&s, finish(&p, "commit")).await["transfer"]["status"],
        "committed"
    );
    s.close().await.unwrap();
}

#[tokio::test]
async fn lost_progress_commit_before_rename_reopens_the_original_encrypted_temp() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let body = b"sensitive-original-staged-body";
    let target = path.with_file_name(format!("persistence.object.body-block.{}", hash(body)));
    let armed = Arc::new(AtomicBool::new(false));
    let fingerprint = Arc::new(Mutex::new(Vec::new()));
    let a = armed.clone();
    let original_path = path.clone();
    let original_target = target.clone();
    let f = fingerprint.clone();
    let mut o = options(&path, OpenMode::Create);
    o.limits.objects = 2;
    o.read_context = Arc::new(move || {
        if a.load(Ordering::SeqCst) && std::fs::read(&original_path)? != *f.lock().unwrap() {
            assert!(
                !original_target.exists(),
                "fault must precede canonical rename"
            );
            for file in std::fs::read_dir(original_path.parent().unwrap())? {
                let bytes = std::fs::read(file?.path())?;
                assert!(!bytes.windows(body.len()).any(|w| w == body));
            }
            return Err(tansr_sdk::Error::Cancelled);
        }
        Ok(owner())
    });
    let s = FileStore::open(o).await.unwrap();
    let p = plan_with_no_entries("pending-rename", body);
    execute(&s, p.begin.clone()).await;
    for ((k, h), b) in &p.objects {
        if k == "body-page" {
            execute(&s, put(&p, k, h, b)).await;
        }
    }
    *fingerprint.lock().unwrap() = std::fs::read(&path).unwrap();
    armed.store(true, Ordering::SeqCst);
    assert!(matches!(
        s.execute(put(&p, "body-block", &hash(body), body), owner())
            .await,
        Err(tansr_sdk::Error::Unknown(_))
    ));
    armed.store(false, Ordering::SeqCst);
    assert!(!target.exists());
    s.close().await.unwrap();
    let mut o = options(&path, OpenMode::Reopen);
    o.limits.objects = 2;
    let s = FileStore::open(o).await.unwrap();
    assert!(target.exists());
    assert_eq!(
        execute(&s, finish(&p, "query")).await["transfer"]["progress"]["bodyReady"],
        "AQ=="
    );
    assert_eq!(
        execute(&s, finish(&p, "commit")).await["transfer"]["status"],
        "committed"
    );
    s.close().await.unwrap();
}

#[tokio::test]
async fn each_index_page_checks_sorting_before_marking_partial_plan_ready() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let s = FileStore::open(options(&path, OpenMode::Create))
        .await
        .unwrap();
    let (entries, values) = entries(0, 33);
    let mut p = plan("first-page-invalid", b"", &entries, &values, None);
    let original_hash = p.begin["index"]["pageHashes"][0]
        .as_str()
        .unwrap()
        .to_owned();
    let old = p
        .objects
        .remove(&("index-page".into(), original_hash))
        .unwrap();
    let mut page: Value = serde_json::from_slice(&old).unwrap();
    page["entries"].as_array_mut().unwrap().swap(0, 1);
    let bytes = canonical(&page);
    let digest = hash(&bytes);
    p.begin["index"]["pageHashes"][0] = json!(digest);
    p.begin.as_object_mut().unwrap().remove("intentSha256");
    p.begin["intentSha256"] = json!(hash(&canonical(&p.begin)));
    execute(&s, p.begin.clone()).await;
    assert!(
        s.execute(put(&p, "index-page", &digest, &bytes), owner())
            .await
            .is_err(),
        "one unsorted page must not become ready while waiting for the second page"
    );
    assert_eq!(
        execute(&s, finish(&p, "query")).await["transfer"]["progress"]["pagesReady"],
        "AA=="
    );
    s.close().await.unwrap();
}

fn copy_source_bytes(path: &Path) -> BTreeMap<String, Vec<u8>> {
    let name = path.file_name().unwrap().to_str().unwrap();
    std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(Result::unwrap)
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
async fn copy_preserves_root_both_indexes_original_pending_and_durable_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let s = FileStore::open(options(&path, OpenMode::Create))
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
        assert!(FileStore::open(o).await.is_err());
    }
    for _ in 0..2 {
        let mut o = options(&target, OpenMode::Reopen);
        o.key = [9; 32];
        let copied = FileStore::open(o).await.unwrap();
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
    assert!(FileStore::open(create).await.is_err());
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
    if let Some(destination) = std::env::var_os("PST_KEEP_COPY_SOURCE") {
        let destination = std::path::PathBuf::from(destination);
        assert!(!destination.exists());
        create_private_directory(&destination).unwrap();
        for (name, bytes) in copy_source_bytes(&path) {
            std::fs::write(destination.join(name), bytes).unwrap();
        }
        std::fs::write(
            destination.join("host.json"),
            serde_json::to_vec(&json!({"identity":identity(),"owner":owner()})).unwrap(),
        )
        .unwrap();
        std::fs::write(destination.join("source-key.hex"), "07".repeat(32)).unwrap();
        std::fs::write(destination.join("new-key.hex"), "0a".repeat(32)).unwrap();
    }
}
#[tokio::test]
async fn copy_rejects_same_key_path_existing_target_and_cannot_activate_second_copy() {
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let s = FileStore::open(options(&path, OpenMode::Create))
        .await
        .unwrap();
    let p = plan("copy-live", b"still private", &[], &[], None);
    publish(&s, &p).await;
    let original = copy_source_bytes(&path);
    let target = path.parent().unwrap().join("copy").join("store");
    assert!(s.copy_to(target.clone(), [7; 32]).await.is_err());
    assert!(!target.parent().unwrap().exists());
    assert!(s.copy_to(path.clone(), [9; 32]).await.is_err());
    assert!(
        s.copy_to(std::path::PathBuf::from("relative/store"), [9; 32])
            .await
            .is_err()
    );
    let first = s.copy_to(target.clone(), [9; 32]).await.unwrap();
    let sealed = copy_source_bytes(&target);
    assert!(s.copy_to(target.clone(), [11; 32]).await.is_err());
    assert_eq!(sealed, copy_source_bytes(&target));
    let mut o = options(&target, OpenMode::Reopen);
    o.key = [9; 32];
    let copied = FileStore::open(o).await.unwrap();
    let target2 = path.parent().unwrap().join("copy-two").join("store");
    let second = copied.copy_to(target2.clone(), [10; 32]).await.unwrap();
    assert_eq!(first.destination_sha256, second.source_sha256);
    assert!(second.read_only);
    assert_eq!(second.cutover, CopyCutover::Pending);
    copied.close().await.unwrap();
    let mut o = options(&target2, OpenMode::Reopen);
    o.key = [10; 32];
    let copied2 = FileStore::open(o).await.unwrap();
    assert_eq!(
        execute(&copied2, finish(&p, "query")).await["transfer"]["status"],
        "committed"
    );
    assert!(
        copied2
            .execute(p.begin.clone(), owner())
            .await
            .unwrap_err()
            .to_string()
            .contains("copy_read_only")
    );
    copied2.close().await.unwrap();
    assert_eq!(original, copy_source_bytes(&path));
    assert_eq!(sealed, copy_source_bytes(&target));
    s.close().await.unwrap();
}
#[tokio::test]
async fn copy_authority_loss_leaves_source_and_encrypted_staging_without_target() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let trigger = Arc::new(AtomicBool::new(false));
    let flag = trigger.clone();
    let root = path.parent().unwrap().to_path_buf();
    let watched = root.clone();
    let mut o = options(&path, OpenMode::Create);
    o.read_context = Arc::new(move || {
        if flag.load(Ordering::SeqCst)
            && std::fs::read_dir(&watched).unwrap().any(|e| {
                e.unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".persistence-copy-")
            })
        {
            return Err(tansr_sdk::Error::InvalidInput(
                "host revoked during copy".into(),
            ));
        }
        Ok(owner())
    });
    let s = FileStore::open(o).await.unwrap();
    let p = plan(
        "copy-interrupted",
        b"original recovery body",
        &[],
        &[],
        None,
    );
    publish(&s, &p).await;
    let original = copy_source_bytes(&path);
    let target = root.join("never-published").join("store");
    trigger.store(true, Ordering::SeqCst);
    assert!(s.copy_to(target.clone(), [9; 32]).await.is_err());
    assert!(!target.parent().unwrap().exists());
    assert!(std::fs::read_dir(&root).unwrap().any(|e| {
        e.unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".persistence-copy-")
    }));
    assert_eq!(original, copy_source_bytes(&path));
    trigger.store(false, Ordering::SeqCst);
    assert_eq!(
        execute(&s, finish(&p, "query")).await["transfer"]["status"],
        "committed"
    );
    s.close().await.unwrap();
    let reopened = FileStore::open(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    assert_eq!(
        execute(&reopened, finish(&p, "query")).await["transfer"]["status"],
        "committed"
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn copy_target_race_and_post_publication_loss_preserve_original_and_read_only_copy() {
    use std::sync::atomic::{AtomicU8, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let root = path.parent().unwrap().to_path_buf();
    let watched = root.clone();
    let race = root.join("raced");
    let race_watch = race.clone();
    let published = root.join("published").join("store");
    let published_watch = published.clone();
    let mode = Arc::new(AtomicU8::new(0));
    let trigger = mode.clone();
    let mut o = options(&path, OpenMode::Create);
    o.read_context = Arc::new(move || {
        if trigger.load(Ordering::SeqCst) == 1
            && std::fs::read_dir(&watched).unwrap().any(|e| {
                let e = e.unwrap();
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".persistence-copy-")
                    && e.path().join("store").exists()
            })
            && trigger.swap(0, Ordering::SeqCst) == 1
        {
            create_private_directory(&race_watch).unwrap();
            std::fs::write(race_watch.join("unrelated"), b"do not replace").unwrap();
        }
        if trigger.load(Ordering::SeqCst) == 2 && published_watch.exists() {
            return Err(tansr_sdk::Error::InvalidInput(
                "host revoked after publication".into(),
            ));
        }
        Ok(owner())
    });
    let s = FileStore::open(o).await.unwrap();
    let p = plan("copy-race", b"original must survive", &[], &[], None);
    publish(&s, &p).await;
    let original = copy_source_bytes(&path);
    mode.store(1, Ordering::SeqCst);
    assert!(matches!(
        s.copy_to(race.join("store"), [9; 32]).await,
        Err(tansr_sdk::Error::Unknown(_))
    ));
    assert_eq!(
        std::fs::read(race.join("unrelated")).unwrap(),
        b"do not replace"
    );
    assert!(!race.join("store").exists());
    assert_eq!(original, copy_source_bytes(&path));
    mode.store(2, Ordering::SeqCst);
    assert!(matches!(
        s.copy_to(published.clone(), [10; 32]).await,
        Err(tansr_sdk::Error::Unknown(_))
    ));
    assert!(published.exists());
    mode.store(0, Ordering::SeqCst);
    let mut o = options(&published, OpenMode::Reopen);
    o.key = [10; 32];
    let target = FileStore::open(o).await.unwrap();
    assert_eq!(
        execute(&target, finish(&p, "query")).await["transfer"]["status"],
        "committed"
    );
    assert!(
        target
            .execute(finish(&p, "commit"), owner())
            .await
            .unwrap_err()
            .to_string()
            .contains("copy_read_only")
    );
    target.close().await.unwrap();
    assert_eq!(original, copy_source_bytes(&path));
    s.close().await.unwrap();
}

#[tokio::test]
async fn copy_from_pending_temp_does_not_reconcile_or_rewrite_source_even_on_rejection() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let path = path(&dir);
    let body = b"private pending body kept with original key";
    let canonical = path.with_file_name(format!("persistence.object.body-block.{}", hash(body)));
    let armed = Arc::new(AtomicBool::new(false));
    let flag = armed.clone();
    let fingerprint = Arc::new(Mutex::new(Vec::new()));
    let original = fingerprint.clone();
    let watched = path.clone();
    let mut o = options(&path, OpenMode::Create);
    o.read_context = Arc::new(move || {
        if flag.load(Ordering::SeqCst) && std::fs::read(&watched)? != *original.lock().unwrap() {
            return Err(tansr_sdk::Error::Cancelled);
        }
        Ok(owner())
    });
    let s = FileStore::open(o).await.unwrap();
    let p = plan("cold-copy-pending", body, &[], &[], None);
    execute(&s, p.begin.clone()).await;
    for ((k, h), b) in &p.objects {
        if k == "body-page" {
            execute(&s, put(&p, k, h, b)).await;
        }
    }
    *fingerprint.lock().unwrap() = std::fs::read(&path).unwrap();
    armed.store(true, Ordering::SeqCst);
    assert!(matches!(
        s.execute(put(&p, "body-block", &hash(body), body), owner())
            .await,
        Err(tansr_sdk::Error::Unknown(_))
    ));
    s.close().await.unwrap();
    assert!(!canonical.exists());
    let original = copy_source_bytes(&path);
    assert!(
        original
            .keys()
            .any(|n| n.starts_with(".persistence.") && n.ends_with(".tmp"))
    );
    let target = path.parent().unwrap().join("cold-copy").join("store");
    assert!(
        FileStore::copy_from(options(&path, OpenMode::Create), target.clone(), [9; 32])
            .await
            .is_err()
    );
    assert!(
        FileStore::copy_from(options(&path, OpenMode::Reopen), target.clone(), [7; 32])
            .await
            .is_err()
    );
    assert!(
        FileStore::copy_from(options(&path, OpenMode::Reopen), path.clone(), [9; 32])
            .await
            .is_err()
    );
    let mut wrong = options(&path, OpenMode::Reopen);
    wrong.key = [2; 32];
    assert!(
        FileStore::copy_from(wrong, target.clone(), [9; 32])
            .await
            .is_err()
    );
    assert_eq!(original, copy_source_bytes(&path));
    assert!(!canonical.exists());
    let copied = FileStore::copy_from(options(&path, OpenMode::Reopen), target.clone(), [9; 32])
        .await
        .unwrap();
    assert!(copied.read_only);
    assert_eq!(original, copy_source_bytes(&path));
    assert!(!canonical.exists());
    assert!(
        FileStore::copy_from(options(&path, OpenMode::Reopen), target.clone(), [10; 32])
            .await
            .is_err()
    );
    assert_eq!(original, copy_source_bytes(&path));
    let mut o = options(&target, OpenMode::Reopen);
    o.key = [9; 32];
    let target = FileStore::open(o).await.unwrap();
    assert_eq!(
        execute(&target, finish(&p, "query")).await["transfer"]["progress"]["bodyReady"],
        "AQ=="
    );
    assert!(
        target
            .execute(finish(&p, "commit"), owner())
            .await
            .unwrap_err()
            .to_string()
            .contains("copy_read_only")
    );
    target.close().await.unwrap();
    if let Some(destination) = std::env::var_os("PST_KEEP_PENDING_COPY_SOURCE") {
        let destination = std::path::PathBuf::from(destination);
        assert!(!destination.exists());
        create_private_directory(&destination).unwrap();
        for (name, bytes) in &original {
            std::fs::write(destination.join(name), bytes).unwrap();
        }
        std::fs::write(
            destination.join("host.json"),
            serde_json::to_vec(&json!({"identity":identity(),"owner":owner()})).unwrap(),
        )
        .unwrap();
        std::fs::write(destination.join("source-key.hex"), "07".repeat(32)).unwrap();
        std::fs::write(destination.join("new-key.hex"), "0a".repeat(32)).unwrap();
    }
    // An explicit ordinary recovery still has the original V1 behavior.
    let s = FileStore::open(options(&path, OpenMode::Reopen))
        .await
        .unwrap();
    assert!(canonical.exists());
    assert_eq!(
        execute(&s, finish(&p, "commit")).await["transfer"]["status"],
        "committed"
    );
    s.close().await.unwrap();
}

#[tokio::test]
async fn host_durable_rejection_is_error_query_stays_readable_and_uncertainty_stays_unknown() {
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tansr_sdk::{
        CancellationToken,
        executor::{
            Authorizer, Operation, Resource, ToolContext, ToolError, ToolHandler, operation_digest,
        },
    };
    struct Policy(Arc<AtomicBool>);
    #[async_trait]
    impl Authorizer for Policy {
        async fn authorize(&self, op: &Operation) -> tansr_sdk::Result<()> {
            if self.0.load(Ordering::SeqCst) && Owner::from_operation(op) == owner() {
                Ok(())
            } else {
                Err(tansr_sdk::Error::Cancelled)
            }
        }
    }
    struct ObservedStore {
        inner: Arc<FileStore>,
        allowed: Arc<AtomicBool>,
        cancellation: CancellationToken,
        fault: u8,
    }
    #[async_trait]
    impl PersistenceStore for ObservedStore {
        fn identity(&self) -> &Identity {
            self.inner.identity()
        }
        fn encrypted_at_rest(&self) -> bool {
            true
        }
        fn atomic_durable_publication(&self) -> bool {
            true
        }
        async fn execute(&self, request: Value, owner: Owner) -> tansr_sdk::Result<Value> {
            let mut response = self.inner.execute(request, owner).await?;
            match self.fault {
                1 => self.allowed.store(false, Ordering::SeqCst),
                2 => self.cancellation.cancel(),
                3 => response["transfer"]["rejection"]["code"] = json!("unrecognized_code"),
                4 => response["transfer"]["transferId"] = json!("substituted-transfer"),
                5 => response["unrecognizedField"] = json!(true),
                _ => (),
            }
            Ok(response)
        }
    }
    fn operation(args: &Value) -> Operation {
        let own = owner();
        let mut op = Operation {
            protocol: "sdk2-ext-v1".into(),
            operation_id: "durable-rejection".into(),
            session_id: own.session_id,
            scope: own.scope,
            binding: own.binding,
            tool_name: "MemoryPublication".into(),
            request: Resource {
                operation: "tool.invoke".into(),
                args: json!({"name":TOOL_NAME,"definitionDigest":TOOL_DIGEST,"argsJson":String::from_utf8(canonical(args)).unwrap()}),
            },
            digest: String::new(),
            expires_at: "2099-01-01T00:00:00Z".into(),
        };
        op.digest = operation_digest(&op).unwrap();
        op
    }
    let dir = tempfile::tempdir().unwrap();
    let file = path(&dir);
    let store = Arc::new(
        FileStore::open(options(&file, OpenMode::Create))
            .await
            .unwrap(),
    );
    let original = plan_with_no_entries("original", b"original-body");
    let root = publish(&store, &original).await;
    let rejected = plan_with_no_entries("rejected", b"unpublished-body");
    let allowed = Arc::new(AtomicBool::new(true));
    let host = Host::new(store.clone(), Arc::new(Policy(allowed.clone()))).unwrap();
    let ((kind, digest), bytes) = rejected.objects.first_key_value().unwrap();
    for args in [
        rejected.begin.clone(),
        put(&rejected, kind, digest, bytes),
        finish(&rejected, "commit"),
    ] {
        let result = host
            .invoke_operation(
                ToolContext {
                    cancellation: CancellationToken::new(),
                    output: None,
                },
                &operation(&args),
                args,
            )
            .await
            .unwrap();
        assert_eq!(
            result,
            json!({"status":"error","message":"revision_conflict"})
        );
    }
    let query = finish(&rejected, "query");
    let result = host
        .invoke_operation(
            ToolContext {
                cancellation: CancellationToken::new(),
                output: None,
            },
            &operation(&query),
            query.clone(),
        )
        .await
        .unwrap();
    assert_eq!(result["status"], "ok");
    let original_fact: Value =
        serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(original_fact["transfer"]["status"], "rejected");
    assert_eq!(
        original_fact["transfer"]["rejection"]["code"],
        "revision_conflict"
    );
    for fault in 1..=5 {
        allowed.store(true, Ordering::SeqCst);
        let cancellation = CancellationToken::new();
        let adapter = Arc::new(ObservedStore {
            inner: store.clone(),
            allowed: allowed.clone(),
            cancellation: cancellation.clone(),
            fault,
        });
        let host = Host::new(adapter, Arc::new(Policy(allowed.clone()))).unwrap();
        let args = rejected.begin.clone();
        assert!(
            matches!(
                host.invoke_operation(
                    ToolContext {
                        cancellation,
                        output: None
                    },
                    &operation(&args),
                    args
                )
                .await,
                Err(ToolError::Unknown(_))
            ),
            "fault={fault} must not become a definite rejection"
        );
    }
    assert_eq!(execute(&store, query.clone()).await, original_fact);
    assert_eq!(execute(&store, req("head", json!({}))).await["root"], root);
    store.close().await.unwrap();
    let reopened = FileStore::open(options(&file, OpenMode::Reopen))
        .await
        .unwrap();
    assert_eq!(execute(&reopened, query).await, original_fact);
    assert_eq!(
        execute(&reopened, finish(&original, "query")).await["transfer"]["result"],
        root
    );
    reopened.close().await.unwrap();
}
