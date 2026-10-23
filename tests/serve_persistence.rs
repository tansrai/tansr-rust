//! Focused PST-SERVE-02 consumers. No model calls or external services.
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tansr_sdk::{
    Error, Result,
    archive::create_private_directory,
    serve_persistence::{
        Persistence,
        custody::{self, Control, MigrationBudget, Port},
    },
    terminal_persistence::{
        CustodyPort, FencedStore, FileStore, Identity, Limits, OpenMode, Owner, PersistenceStore,
        StoreOptions,
    },
};

fn fixture() -> Value {
    let value: Value =
        serde_json::from_str(include_str!("fixtures/persistence-custody-node.json")).unwrap();
    assert_eq!(value["pages"].as_array().unwrap().len(), 13);
    assert_eq!(value["chunks"].as_array().unwrap().len(), 49);
    assert_eq!(value["receipts"].as_array().unwrap().len(), 40);
    value
}
fn options(path: &std::path::Path, mode: OpenMode, f: &Value) -> StoreOptions {
    let owner: Owner = serde_json::from_value(f["owner"].clone()).unwrap();
    StoreOptions {
        path: path.into(),
        mode,
        key: [41; 32],
        identity: serde_json::from_value::<Identity>(f["identity"].clone()).unwrap(),
        limits: Limits::default(),
        read_context: Arc::new(move || Ok(owner.clone())),
        authorize_recovery: None,
    }
}
fn private(dir: &tempfile::TempDir) -> std::path::PathBuf {
    let path = std::fs::canonicalize(dir.path()).unwrap().join("private");
    create_private_directory(&path).unwrap();
    path.join("store")
}
fn request(f: &Value, action: &str) -> Value {
    let mut r = f["common"].clone();
    r["action"] = json!(action);
    r
}

#[test]
fn additive_contracts_golden_and_budgets_are_strict() {
    let g: Value = serde_json::from_str(include_str!(
        "../src/serve_persistence/assets/persistence-custody-handoff-v1.golden.json"
    ))
    .unwrap();
    for case in g["positive"].as_array().unwrap() {
        custody::validate(case["definition"].as_str().unwrap(), &case["value"])
            .unwrap_or_else(|e| panic!("{}: {e}", case["id"]));
    }
    for case in g["negative"].as_array().unwrap() {
        assert!(
            custody::validate(case["definition"].as_str().unwrap(), &case["value"]).is_err(),
            "{}",
            case["id"]
        );
    }
    let f = fixture();
    let proof = &f["ticket"]["proof"];
    MigrationBudget::default().check(proof).unwrap();
    assert!(
        MigrationBudget {
            bytes: 1,
            objects: 1
        }
        .check(proof)
        .is_err()
    );
    assert!(
        MigrationBudget {
            bytes: 1102569743,
            objects: 1
        }
        .check(proof)
        .is_err()
    );
    assert_eq!(
        custody::prepare_intent(&f["prepare"]).unwrap(),
        f["prepare"]["intentSha256"]
    );
    let mut value = json!({"contract":"serve-persistence-v1","sessionId":"session","mode":"cloud","sourceId":"source","sourceGeneration":"1","coverage":["history","memory","executions","input-receipts"],"handoff":{"archive":"cloud-immutable-publish","trustedControl":true,"executionJournal":true,"inputReceipts":true},"localDisk":false});
    serde_json::from_value::<Persistence>(value.clone())
        .unwrap()
        .validate()
        .unwrap();
    value["mode"] = json!("terminal");
    assert!(
        serde_json::from_value::<Persistence>(value.clone())
            .unwrap()
            .validate()
            .is_err()
    );
    value["mode"] = json!("cloud");
    value["localDisk"] = json!(true);
    assert!(
        serde_json::from_value::<Persistence>(value)
            .unwrap()
            .validate()
            .is_err()
    );
}

struct Authority {
    ticket: Mutex<Value>,
    claims: Mutex<BTreeMap<String, Value>>,
}
impl Authority {
    fn new(f: &Value) -> Self {
        Self {
            ticket: Mutex::new(f["ticket"].clone()),
            claims: Mutex::new(BTreeMap::new()),
        }
    }
}
#[async_trait]
impl Control for Authority {
    async fn begin_handoff(&self, _r: &Value) -> Result<String> {
        Ok(self.ticket.lock().unwrap()["writerFence"]
            .as_str()
            .unwrap()
            .into())
    }
    async fn record_prepared(&self, _r: &Value, p: &Value) -> Result<Value> {
        let t = self.ticket.lock().unwrap();
        if t["proof"] != *p {
            return Err(Error::Contract("proof mismatch".into()));
        }
        Ok(t.clone())
    }
    async fn query(&self, _r: &Value) -> Result<Value> {
        Ok(json!({"preparing":false,"ticket":self.ticket.lock().unwrap().clone()}))
    }
    async fn mark_verified(&self, _r: &Value, p: &Value) -> Result<Value> {
        let mut t = self.ticket.lock().unwrap();
        assert_eq!(t["proof"], *p);
        t["phase"] = json!("verified");
        Ok(t.clone())
    }
    async fn cutover(&self, _r: &Value, p: &Value) -> Result<Value> {
        let mut t = self.ticket.lock().unwrap();
        assert_eq!(t["proof"], *p);
        assert_eq!(t["phase"], "verified");
        t["phase"] = json!("published");
        t["selectedCustody"] = t["destination"].clone();
        t["selectedCustody"]["custodyRevision"] = json!("2");
        Ok(t.clone())
    }
    async fn abort(&self, _r: &Value) -> Result<Value> {
        Err(Error::Contract("not authorized".into()))
    }
    async fn writer(&self, r: &Value) -> Result<Value> {
        let t = self.ticket.lock().unwrap();
        if t["phase"] != "published" || r["custody"] != t["selectedCustody"] {
            return Err(Error::Contract("old writer fenced".into()));
        }
        let key = r["operationId"].as_str().unwrap();
        let mut claims = self.claims.lock().unwrap();
        if r["action"] == "writer-claim" {
            assert_eq!(r["expectedWriterFence"], t["writerFence"]);
            let claim=claims.entry(key.into()).or_insert_with(||json!({"operationId":key,"intentSha256":r["intentSha256"],"claimId":format!("claim-{}",&key[..12]),"writerFence":t["writerFence"],"selectedCustody":t["selectedCustody"],"status":"active","responseDigest":null}));
            assert_eq!(claim["intentSha256"], r["intentSha256"]);
            Ok(claim.clone())
        } else {
            let claim = claims.get_mut(key).unwrap();
            assert_eq!(claim["claimId"], r["claimId"]);
            claim["status"] = json!("settled");
            claim["responseDigest"] = r["settlement"]["responseDigest"].clone();
            Ok(claim.clone())
        }
    }
}

#[tokio::test]
async fn node_inventory_cold_import_keeps_permanent_facts_and_new_writer() {
    let f = fixture();
    let dir = tempfile::tempdir().unwrap();
    let path = private(&dir);
    let owner: Owner = serde_json::from_value(f["owner"].clone()).unwrap();
    let store = Arc::new(
        FileStore::open(options(&path, OpenMode::Create, &f))
            .await
            .unwrap(),
    );
    let port = CustodyPort::new(store.clone(), MigrationBudget::default()).unwrap();
    let ticket = &f["ticket"];
    // Whole fixed directory, including permanent receipts and staging transfer.
    for page in f["pages"].as_array().unwrap() {
        port.import_page(ticket, page).await.unwrap();
    }
    for chunk in f["chunks"].as_array().unwrap() {
        port.import_object(ticket, chunk).await.unwrap();
    }
    port.import_page(ticket, &f["pages"][0]).await.unwrap();
    port.import_object(ticket, &f["chunks"][0]).await.unwrap();
    assert_eq!(port.verify(ticket).await.unwrap(), ticket["proof"]);
    assert!(
        store
            .execute(f["pending"]["commit"].clone(), owner.clone())
            .await
            .is_err()
    );
    store.close().await.unwrap();
    drop(port);
    drop(store);
    let store = Arc::new(
        FileStore::open(options(&path, OpenMode::Reopen, &f))
            .await
            .unwrap(),
    );
    let port = CustodyPort::new(
        store.clone(),
        MigrationBudget {
            bytes: 1,
            objects: 1,
        },
    )
    .unwrap();
    // Budget is fixed at first acceptance, not silently changed on reopen.
    assert_eq!(port.verify(ticket).await.unwrap(), ticket["proof"]);
    let authority = Arc::new(Authority::new(&f));
    authority
        .mark_verified(&f["prepare"], &ticket["proof"])
        .await
        .unwrap();
    let published = authority
        .cutover(&f["prepare"], &ticket["proof"])
        .await
        .unwrap();
    port.activate(&published).await.unwrap();
    let old = FencedStore::new(
        store.clone(),
        authority.clone(),
        ticket["source"].clone(),
        "1".into(),
    )
    .unwrap();
    assert!(
        old.execute(f["pending"]["commit"].clone(), owner.clone())
            .await
            .is_err()
    );
    let current = FencedStore::new(
        store.clone(),
        authority.clone(),
        published["selectedCustody"].clone(),
        "1".into(),
    )
    .unwrap();
    let head = store
        .execute(request(&f, "head"), owner.clone())
        .await
        .unwrap();
    assert_eq!(head["root"], f["root"]);
    assert_eq!(
        store
            .execute(f["original"]["query"].clone(), owner.clone())
            .await
            .unwrap()["transfer"]["result"],
        f["root"]
    );
    assert_eq!(
        store
            .execute(f["pending"]["query"].clone(), owner.clone())
            .await
            .unwrap()["transfer"]["status"],
        "staging"
    );
    for receipt in f["receipts"].as_array().unwrap() {
        for kind in ["primary", "secondary"] {
            let mut r = request(&f, "lookup");
            r["commitRoot"] = f["root"]["commitRoot"].clone();
            r["key"] = json!({"kind":kind,"digest":receipt[format!("{kind}Key")]});
            let got = store.execute(r, owner.clone()).await.unwrap();
            assert_eq!(got["entry"]["base64"], receipt["base64"]);
        }
    }
    for put in f["pending"]["puts"].as_array().unwrap() {
        current.execute(put.clone(), owner.clone()).await.unwrap();
    }
    let committed = current
        .execute(f["pending"]["commit"].clone(), owner.clone())
        .await
        .unwrap();
    assert_eq!(committed["transfer"]["status"], "committed");
    let new_root = committed["transfer"]["result"].clone();
    assert_ne!(new_root, f["root"]);
    assert_eq!(
        current
            .execute(f["pending"]["commit"].clone(), owner.clone())
            .await
            .unwrap(),
        committed
    );
    // Recovered original published plan cannot reinstall the pre-write root.
    port.activate(&published).await.unwrap();
    assert_eq!(
        store
            .execute(request(&f, "head"), owner.clone())
            .await
            .unwrap()["root"],
        new_root
    );
    assert_eq!(
        store
            .execute(f["original"]["query"].clone(), owner.clone())
            .await
            .unwrap()["transfer"]["result"],
        f["root"]
    );
    store.close().await.unwrap();
    drop(current);
    drop(old);
    drop(port);
    drop(store);
    let reopened = FileStore::open(options(&path, OpenMode::Reopen, &f))
        .await
        .unwrap();
    assert_eq!(
        reopened
            .execute(request(&f, "head"), owner.clone())
            .await
            .unwrap()["root"],
        new_root
    );
    assert!(
        reopened
            .execute(f["pending"]["commit"].clone(), owner.clone())
            .await
            .is_err()
    );
    assert_eq!(
        reopened
            .execute(f["pending"]["query"].clone(), owner)
            .await
            .unwrap()["transfer"],
        committed["transfer"]
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn custody_import_rejects_gaps_conflicts_and_over_budget_without_publication() {
    let f = fixture();
    let dir = tempfile::tempdir().unwrap();
    let path = private(&dir);
    let store = Arc::new(
        FileStore::open(options(&path, OpenMode::Create, &f))
            .await
            .unwrap(),
    );
    let small = CustodyPort::new(
        store.clone(),
        MigrationBudget {
            bytes: 1,
            objects: 1,
        },
    )
    .unwrap();
    assert!(
        small
            .import_page(&f["ticket"], &f["pages"][0])
            .await
            .is_err()
    );
    let port = CustodyPort::new(store.clone(), MigrationBudget::default()).unwrap();
    assert!(
        port.import_page(&f["ticket"], &f["pages"][1])
            .await
            .is_err()
    );
    port.import_page(&f["ticket"], &f["pages"][0])
        .await
        .unwrap();
    let mut wrong = f["pages"][0].clone();
    wrong["records"][0]["root"]["generation"] = json!("9");
    assert!(port.import_page(&f["ticket"], &wrong).await.is_err());
    assert!(port.verify(&f["ticket"]).await.is_err());
    assert!(port.activate(&f["published"]).await.is_err());
    let owner: Owner = serde_json::from_value(f["owner"].clone()).unwrap();
    assert!(store.execute(request(&f, "head"), owner).await.unwrap()["root"].is_null());
    store.close().await.unwrap();
}

#[tokio::test]
async fn settled_writer_fact_cannot_reconstruct_missing_local_effect() {
    let f = fixture();
    let dir = tempfile::tempdir().unwrap();
    let path = private(&dir);
    let store = Arc::new(
        FileStore::open(options(&path, OpenMode::Create, &f))
            .await
            .unwrap(),
    );
    let authority = Arc::new(Authority::new(&f));
    *authority.ticket.lock().unwrap() = f["published"].clone();
    let input = f["pending"]["begin"].clone();
    let (id, digest) = custody::writer_operation(&input).unwrap();
    authority.claims.lock().unwrap().insert(id.clone(),json!({"operationId":id,"intentSha256":digest,"claimId":"prior-claim","writerFence":"1","selectedCustody":f["published"]["selectedCustody"],"status":"settled","responseDigest":"0".repeat(64)}));
    let fenced = FencedStore::new(
        store.clone(),
        authority,
        f["published"]["selectedCustody"].clone(),
        "1".into(),
    )
    .unwrap();
    let owner: Owner = serde_json::from_value(f["owner"].clone()).unwrap();
    assert!(fenced.execute(input, owner.clone()).await.is_err());
    let head = store.execute(request(&f, "head"), owner).await.unwrap();
    assert!(head["root"].is_null());
    assert_eq!(head["capacity"]["used"]["transferFacts"], 0);
    store.close().await.unwrap();
}

#[tokio::test]
async fn native_export_roundtrip_retains_original_domain_and_staging_ticket() {
    let f = fixture();
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(
        FileStore::open(options(&private(&dir), OpenMode::Create, &f))
            .await
            .unwrap(),
    );
    let owner: Owner = serde_json::from_value(f["owner"].clone()).unwrap();
    source
        .execute(f["original"]["begin"].clone(), owner.clone())
        .await
        .unwrap();
    for put in f["original"]["puts"].as_array().unwrap() {
        source.execute(put.clone(), owner.clone()).await.unwrap();
    }
    source
        .execute(f["original"]["commit"].clone(), owner.clone())
        .await
        .unwrap();
    source
        .execute(f["pending"]["begin"].clone(), owner.clone())
        .await
        .unwrap();
    let export = CustodyPort::new(source.clone(), MigrationBudget::default()).unwrap();
    let proof = export.freeze(&f["prepare"], "1").await.unwrap();
    assert_eq!(proof["originalCommitRoot"], f["root"]["commitRoot"]);
    assert_eq!(proof["indexCount"], "40");
    assert_eq!(export.freeze(&f["prepare"], "1").await.unwrap(), proof);
    assert!(
        source
            .execute(f["pending"]["puts"][0].clone(), owner.clone())
            .await
            .is_err()
    );
    let dir2 = tempfile::tempdir().unwrap();
    let target = Arc::new(
        FileStore::open(options(&private(&dir2), OpenMode::Create, &f))
            .await
            .unwrap(),
    );
    let import = CustodyPort::new(target.clone(), MigrationBudget::default()).unwrap();
    let mut ticket = f["ticket"].clone();
    ticket["proof"] = proof.clone();
    let mut request = json!({"contract":custody::CONTRACT,"identity":f["identity"],"operationId":f["prepare"]["operationId"],"intentSha256":f["prepare"]["intentSha256"],"action":"export-page","limit":7,"cursor":null});
    let mut transfers = 0usize;
    loop {
        let page = export.page(&request).await.unwrap();
        import.import_page(&ticket, &page["page"]).await.unwrap();
        for record in page["page"]["records"].as_array().unwrap() {
            if record["kind"] == "transfer" {
                transfers += 1;
            }
            if record["kind"] != "object" {
                continue;
            }
            let mut object = json!({"contract":custody::CONTRACT,"identity":f["identity"],"operationId":ticket["operationId"],"intentSha256":ticket["intentSha256"],"action":"export-object","object":record["object"],"offset":0,"length":12288});
            while object["offset"].as_u64().unwrap()
                < record["object"]["byteLength"].as_u64().unwrap()
            {
                let chunk = export.object(&object).await.unwrap();
                import.import_object(&ticket, &chunk).await.unwrap();
                object["offset"] = json!(
                    object["offset"].as_u64().unwrap() + chunk["byteLength"].as_u64().unwrap()
                );
            }
        }
        if page["nextCursor"].is_null() {
            break;
        }
        request["cursor"] = page["nextCursor"].clone();
    }
    assert_eq!(transfers, 2);
    assert_eq!(import.verify(&ticket).await.unwrap(), proof);
    let mut published = f["published"].clone();
    published["proof"] = proof;
    import.activate(&published).await.unwrap();
    assert_eq!(
        target
            .execute(f["original"]["query"].clone(), owner.clone())
            .await
            .unwrap()["transfer"]["result"],
        f["root"]
    );
    assert_eq!(
        target
            .execute(f["pending"]["query"].clone(), owner)
            .await
            .unwrap()["transfer"]["status"],
        "staging"
    );
    source.close().await.unwrap();
    target.close().await.unwrap();
}
