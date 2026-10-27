//! The existing encrypted FileStore's inventory adapter. Authority is supplied
//! separately; a disk snapshot and a published-looking JSON ticket cannot grant it.
use super::*;
use crate::serve_persistence::custody::{self as wire, Control, MigrationBudget, Port};

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct CustodyState {
    selected: Value,
    writer_fence: String,
    frozen: bool,
    migrations: BTreeMap<String, Migration>,
}
impl CustodyState {
    pub(super) fn indexed_fence(&self) -> Self {
        Self {
            selected: self.selected.clone(),
            writer_fence: self.writer_fence.clone(),
            frozen: self.frozen,
            migrations: BTreeMap::new(),
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
struct Migration {
    intent: String,
    proof: Value,
    records: Vec<Value>,
    source: bool,
    activated: bool,
    budget: MigrationBudget,
    special: BTreeMap<String, String>,
    received: BTreeMap<String, usize>,
}
pub struct CustodyPort {
    store: Arc<FileStore>,
    budget: MigrationBudget,
    access: Option<(Owner, wire::AssertCurrent)>,
}
impl CustodyPort {
    pub fn new(store: Arc<FileStore>, budget: MigrationBudget) -> Result<Arc<Self>> {
        if budget.bytes == 0
            || budget.bytes > 1102569742
            || budget.objects == 0
            || budget.objects > 1310721
        {
            return Err(reject("custody_capacity_exceeded"));
        }
        Ok(Arc::new(Self {
            store,
            budget,
            access: None,
        }))
    }
    async fn run<T: Send + 'static>(
        &self,
        action: impl FnOnce(&mut Inner, &Owner) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let access = self.access.clone();
        self.store
            .run(move |inner, before| {
                let prior = inner.commit_guard.take();
                if let Some((expected, guard)) = access {
                    if expected != *before {
                        inner.commit_guard = prior;
                        return Err(reject("context_changed"));
                    }
                    inner.commit_guard = Some(guard);
                }
                let result = (|| {
                    inner.check(before)?;
                    inner.index_complete()?;
                    let result = action(inner, before)?;
                    inner.check(before)?;
                    Ok(result)
                })();
                inner.commit_guard = prior;
                result
            })
            .await
    }
}
fn id(identity: &Identity) -> Value {
    serde_json::to_value(identity).expect("identity serialization")
}
fn ref_key(r: &Value) -> Result<String> {
    wire::validate("ObjectRef", r)?;
    Ok(format!("{}:{}", string(&r["kind"])?, string(&r["sha256"])?))
}
fn proof(identity: &Identity, records: &[Value]) -> Result<Value> {
    let root = records.first().ok_or_else(integrity)?;
    if root["kind"] != "root" {
        return Err(reject("unsupported_origin_format"));
    }
    let fixed = wire::snapshot(&id(identity), root)?;
    let mut digest = wire::seed(&id(identity), CONTRACT, &fixed)?;
    let mut bytes = 0usize;
    let mut objects = 0usize;
    for (index, record) in records.iter().enumerate() {
        wire::validate("Record", record)?;
        digest = wire::record_digest(index, &digest, record)?;
        if record["kind"] == "object" {
            objects += 1;
            bytes = bytes
                .checked_add(num(&record["object"], "byteLength"))
                .ok_or_else(integrity)?;
        }
    }
    let r = &root["root"];
    Ok(
        json!({"originFormat":CONTRACT,"fixedRoot":fixed,"originalCommitRoot":r.get("commitRoot"),"indexRoot":r.get("index").and_then(|i|i.get("root")),"indexCount":r.get("index").and_then(|i|i.get("count")).cloned().unwrap_or(json!("0")),"inventoryDigest":digest,"records":records.len(),"objects":objects,"bytes":bytes}),
    )
}
fn original(ticket: &Value) -> Result<(String, String)> {
    wire::validate("Ticket", ticket)?;
    Ok((
        string(&ticket["operationId"])?.into(),
        string(&ticket["intentSha256"])?.into(),
    ))
}
impl Inner {
    fn portable_transfer(&self, t: &Ticket) -> Result<Value> {
        let refs = |keys: &BTreeSet<String>| -> Result<Vec<Value>> {
            keys.iter().map(|key| {
                let (kind, sha) = split_key(key)?;
                Ok(json!({"kind":kind,"sha256":sha,"byteLength":self.state.objects.get(key).ok_or_else(integrity)?}))
            }).collect()
        };
        let received = refs(&t.received)?;
        let reused = refs(&t.available.difference(&t.received).cloned().collect())?;
        let transfer = self.transfer(t)?;
        let metadata = encoded_len(
            &json!({"begin":t.begin,"owner":t.owner,"baseRoot":t.base_root,"transfer":transfer}),
        )?;
        let (mut raw, mut meta, mut reused_raw) = (0usize, 0usize, 0usize);
        for key in &t.available {
            let len = *self.state.objects.get(key).ok_or_else(integrity)?;
            raw += len;
            meta += object_metadata(key, len)?;
            if !t.received.contains(key) {
                reused_raw += len;
            }
        }
        let active = t.status == "staging";
        if active
            && (raw > num(&t.begin["declared"], "bytes")
                || t.available.len() > num(&t.begin["declared"], "objects")
                || metadata + meta > RESERVE)
        {
            return Err(integrity());
        }
        let value = json!({
            "begin":t.begin,"owner":t.owner,"baseRoot":t.base_root,"transfer":transfer,"received":received,"reused":reused,
            "reservedBytes":if active {num(&t.begin["declared"],"bytes")-raw+RESERVE-metadata-meta} else {0},
            "reservedObjects":if active {num(&t.begin["declared"],"objects")-t.available.len()} else {0},
            "reservedEntries":if active {num(&t.begin["index"],"addedCount")} else {0},
            "stagingBytes":if active {num(&t.begin["declared"],"bytes")-reused_raw} else {0}
        });
        wire::validate("TransferMetadata", &value)?;
        Ok(value)
    }
    fn migration(&self, key: &str, intent: &str) -> Result<&Migration> {
        self.state
            .custody
            .as_ref()
            .and_then(|c| c.migrations.get(key))
            .filter(|m| m.intent == intent)
            .ok_or_else(|| reject("custody_unknown"))
    }
    fn custody_path(&self, key: &str, object: &Value) -> Result<PathBuf> {
        wire::validate("ObjectRef", object)?;
        Ok(self.path.with_file_name(format!(
            "{}.custody.{}.{}.{}",
            self.path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(integrity)?,
            hash(key.as_bytes()),
            string(&object["kind"])?,
            string(&object["sha256"])?
        )))
    }
    fn custody_aad(&self, key: &str, object: &Value) -> Result<Vec<u8>> {
        crate::canonical::encode(&json!([
            "PST-CUSTODY-OBJECT",
            self.state.identity,
            key,
            object
        ]))
    }
    fn custody_read(&self, key: &str, object: &Value) -> Result<Vec<u8>> {
        let path = self.custody_path(key, object)?;
        if !path.try_exists()? {
            return Ok(vec![]);
        }
        let data = self
            .cipher
            .open(
                &self.read_file(&path, 524288)?,
                &self.custody_aad(key, object)?,
            )?
            .to_vec();
        if data.len() > num(object, "byteLength") {
            return Err(integrity());
        }
        Ok(data)
    }
    fn custody_write(
        &mut self,
        key: &str,
        object: &Value,
        data: &[u8],
        before: &Owner,
    ) -> Result<()> {
        self.check(before)?;
        let path = self.custody_path(key, object)?;
        let blob = self.cipher.seal(data, &self.custody_aad(key, object)?)?;
        let temporary = self.temporary()?;
        let mut f = platform::open_at(&self.directory, &temporary, true, true)?;
        f.write_all(&blob)?;
        f.sync_all()?;
        drop(f);
        self.check(before)?;
        self.uncertain = true;
        platform::replace_at(&self.directory, &temporary, &path).map_err(|_| integrity())?;
        self.uncertain = false;
        Ok(())
    }
    fn source_object(&self, m: &Migration, reference: &Value) -> Result<Vec<u8>> {
        let key = ref_key(reference)?;
        if !m
            .records
            .iter()
            .any(|r| r["kind"] == "object" && r["object"] == *reference)
        {
            return Err(integrity());
        }
        let bytes = if let Some(value) = m.special.get(&key) {
            decode(value)?
        } else {
            self.object(&key)?.to_vec()
        };
        if bytes.len() != num(reference, "byteLength") || hash(&bytes) != reference["sha256"] {
            return Err(integrity());
        }
        Ok(bytes)
    }
    fn stage(&self, ticket: &Value, budget: MigrationBudget) -> Result<CustodyState> {
        let (key, intent) = original(ticket)?;
        if ticket["identity"] != id(&self.state.identity)
            || ticket["proof"]["originFormat"] != CONTRACT
        {
            return Err(reject("stale_generation"));
        }
        if let Some(c) = &self.state.custody {
            if let Some(m) = c.migrations.get(&key) {
                if m.intent != intent || m.proof != ticket["proof"] || m.source {
                    return Err(reject("request_conflict"));
                }
                m.budget.check(&m.proof)?;
                return Ok(c.clone());
            }
        }
        budget.check(&ticket["proof"])?;
        if let Some(c) = &self.state.custody {
            // Source/target history remains queryable, but the terminal adapter
            // does not admit an unbounded number of unfinished imports.
            let mut bytes = num(&ticket["proof"], "bytes");
            let mut objects = num(&ticket["proof"], "objects");
            for m in c.migrations.values().filter(|m| !m.source && !m.activated) {
                bytes = bytes
                    .checked_add(num(&m.proof, "bytes"))
                    .ok_or_else(integrity)?;
                objects = objects
                    .checked_add(num(&m.proof, "objects"))
                    .ok_or_else(integrity)?;
            }
            if c.migrations.len() >= self.state.limits.transfer_facts
                || bytes > budget.bytes
                || objects > budget.objects
            {
                return Err(reject("custody_capacity_exceeded"));
            }
        }
        if self.state.root.is_some()
            || !self.state.tickets.is_empty()
            || !self.state.primary.is_empty()
            || !self.state.objects.is_empty()
        {
            return Err(reject("custody_target_not_empty"));
        }
        let mut c = self.state.custody.clone().unwrap_or(CustodyState {
            selected: ticket["source"].clone(),
            writer_fence: string(&ticket["writerFence"])?.into(),
            frozen: true,
            migrations: BTreeMap::new(),
        });
        c.migrations.insert(
            key,
            Migration {
                intent,
                proof: ticket["proof"].clone(),
                records: vec![],
                source: false,
                activated: false,
                budget,
                special: BTreeMap::new(),
                received: BTreeMap::new(),
            },
        );
        Ok(c)
    }
}
#[async_trait]
impl Port for CustodyPort {
    fn identity(&self) -> Value {
        id(&self.store.identity)
    }
    fn for_operation(&self, owner: Owner, current: wire::AssertCurrent) -> Result<Arc<dyn Port>> {
        owner.validate()?;
        Ok(Arc::new(Self {
            store: self.store.clone(),
            budget: self.budget,
            access: Some((owner, current)),
        }))
    }
    async fn freeze(&self, request: &Value, writer_fence: &str) -> Result<Value> {
        wire::validate("PrepareRequest", request)?;
        if request["identity"] != self.identity()
            || wire::prepare_intent(request)? != request["intentSha256"]
        {
            return Err(reject("invalid_request"));
        }
        let r = request.clone();
        let fence = writer_fence.to_string();
        let budget = self.budget;
        self.run(move |inner, before| {
            let key = string(&r["operationId"])?;
            let intent = string(&r["intentSha256"])?;
            if let Some(c) = &inner.state.custody {
                if let Some(m) = c.migrations.get(key) {
                    if m.intent != intent || !m.source {
                        return Err(reject("request_conflict"));
                    }
                    return Ok(m.proof.clone());
                }
                if c.frozen || c.selected != r["source"] {
                    return Err(reject("custody_writer_fenced"));
                }
            }
            inner.settle_objects(before)?;
            inner.audit()?;
            if !inner.state.retiring.is_empty() || !inner.state.orphaned.is_empty() {
                return Err(reject("custody_unsettled_storage"));
            }
            let mut special = BTreeMap::new();
            let mut objects = BTreeMap::new();
            let capacity = inner.capacity()?;
            let control = json!({
                "format": "terminal-persistence-control-v1",
                "capacity": capacity["used"], "limits": capacity["limits"]
            });
            wire::validate("ControlState", &control)?;
            let bytes = crate::canonical::encode(&control)?;
            let control_ref = json!({
                "kind": "custody-control", "sha256": hash(&bytes), "byteLength": bytes.len()
            });
            special.insert(ref_key(&control_ref)?, STANDARD.encode(bytes));
            objects.insert(ref_key(&control_ref)?, control_ref.clone());
            let mut records = vec![json!({"kind":"root", "root":inner.state.root, "controlRef":control_ref})];
            let mut rows = inner.state.primary.values().collect::<Vec<_>>();
            rows.sort_by_key(|r| r.ordinal.parse::<u64>().unwrap_or(u64::MAX));
            for row in rows {
                records.push(json!({"kind":"receipt-index", "ordinal":row.ordinal, "entry":row.entry}));
            }
            for (key, t) in &inner.state.tickets {
                let portable = inner.portable_transfer(t)?;
                let bytes = crate::canonical::encode_limited(&portable, 524288)?;
                let reference = json!({"kind":"custody-transfer", "sha256":hash(&bytes), "byteLength":bytes.len()});
                special.insert(ref_key(&reference)?, STANDARD.encode(bytes));
                objects.insert(ref_key(&reference)?, reference.clone());
                records.push(json!({
                    "kind":"transfer", "transferId":key, "intentSha256":t.begin["intentSha256"],
                    "status":t.status, "metadataRef":reference
                }));
            }
            for (key, len) in &inner.state.objects {
                let (kind, sha) = split_key(key)?;
                objects.insert(key.clone(), json!({"kind":kind, "sha256":sha, "byteLength":len}));
            }
            for reference in objects.values() {
                records.push(json!({"kind":"object", "object":reference}));
            }
            let p = proof(&inner.state.identity, &records)?;
            budget.check(&p)?;
            if r["expectedRoot"] != p["originalCommitRoot"] {
                return Err(reject("revision_conflict"));
            }
            let mut next = inner.state.clone();
            let c = next.custody.get_or_insert(CustodyState {
                selected: r["source"].clone(), writer_fence: fence.clone(),
                frozen: true, migrations: BTreeMap::new(),
            });
            c.frozen = true;
            c.writer_fence = fence;
            c.migrations.insert(key.into(), Migration {
                intent: intent.into(), proof: p.clone(), records, source: true,
                activated: false, budget, special, received: BTreeMap::new(),
            });
            inner.save(next, before)?;
            Ok(p)
        }).await
    }
    async fn page(&self, request: &Value) -> Result<Value> {
        wire::validate("ExportPageRequest", request)?;
        let r = request.clone();
        self.run(move |inner, _| {
            let key = string(&r["operationId"])?;
            let m = inner.migration(key, string(&r["intentSha256"])?)?;
            if !m.source {
                return Err(integrity());
            }
            let mut index = 0;
            let mut prior = wire::seed(&id(&inner.state.identity), CONTRACT, string(&m.proof["fixedRoot"])?)?;
            if !r["cursor"].is_null() {
                let c = &r["cursor"];
                for field in ["operationId", "intentSha256"] {
                    if c[field] != r[field] {
                        return Err(integrity());
                    }
                }
                if c["fixedRoot"] != m.proof["fixedRoot"] || c["inventoryDigest"] != m.proof["inventoryDigest"] {
                    return Err(integrity());
                }
                index = string(&c["index"])?.parse::<usize>().map_err(|_| integrity())?;
                if index > m.records.len() {
                    return Err(integrity());
                }
                for (i, record) in m.records[..index].iter().enumerate() {
                    prior = wire::record_digest(i, &prior, record)?;
                }
                if c["previousDigest"] != prior {
                    return Err(integrity());
                }
            }
            let start = index;
            let previous = prior.clone();
            let mut records = vec![];
            let maximum = num(&r,"limit");
            while index < m.records.len() && records.len() < maximum {
                let d = wire::record_digest(index, &prior, &m.records[index])?;
                let mut proposed = records.clone();
                proposed.push(m.records[index].clone());
                let encoded = crate::canonical::encode(&json!({
                    "proof":m.proof, "page":{
                        "index":start.to_string(), "previousDigest":previous, "sha256":d,
                        "records":proposed, "complete":false
                    }
                }))?;
                if encoded.len() > 28000 {
                    break;
                }
                records = proposed;
                prior = d;
                index += 1;
            }
            if records.is_empty() {
                return Err(reject("invalid_cursor"));
            }
            let complete = index == m.records.len();
            let cursor = if complete { Value::Null } else { json!({
                "operationId":key, "intentSha256":m.intent, "fixedRoot":m.proof["fixedRoot"],
                "inventoryDigest":m.proof["inventoryDigest"], "index":index.to_string(), "previousDigest":prior
            }) };
            Ok(json!({"proof":m.proof,"page":{
                "index":start.to_string(),"previousDigest":previous,"sha256":prior,
                "records":records,"complete":complete
            },"nextCursor":cursor}))
        }).await
    }
    async fn object(&self, request: &Value) -> Result<Value> {
        wire::validate("ExportObjectRequest", request)?;
        let r = request.clone();
        self.run(move |inner, _| {
            let m = inner.migration(string(&r["operationId"])?, string(&r["intentSha256"])?)?;
            let data = inner.source_object(m, &r["object"])?;
            let start = num(&r, "offset");
            let end = start.saturating_add(num(&r, "length")).min(data.len());
            if start >= data.len() || end <= start {
                return Err(reject("invalid_request"));
            }
            let part = &data[start..end];
            Ok(
                json!({"object":r["object"], "offset":start, "byteLength":part.len(),
                "base64":STANDARD.encode(part), "payloadDigest":hash(part)}),
            )
        })
        .await
    }
    async fn import_page(&self, ticket: &Value, page: &Value) -> Result<()> {
        wire::validate("Page", page)?;
        let t = ticket.clone();
        let p = page.clone();
        let budget = self.budget;
        self.run(move |inner, before| {
            let (key, _) = original(&t)?;
            let mut custody = inner.stage(&t, budget)?;
            let m = custody.migrations.get_mut(&key).ok_or_else(integrity)?;
            let index = string(&p["index"])?
                .parse::<usize>()
                .map_err(|_| integrity())?;
            let records = array(&p["records"])?;
            if index > m.records.len() {
                return Err(reject("custody_page_gap"));
            }
            let mut prior = wire::seed(
                &id(&inner.state.identity),
                CONTRACT,
                string(&m.proof["fixedRoot"])?,
            )?;
            for (i, r) in m.records[..index].iter().enumerate() {
                prior = wire::record_digest(i, &prior, r)?;
            }
            if prior != p["previousDigest"] {
                return Err(integrity());
            }
            for (offset, r) in records.iter().enumerate() {
                let i = index + offset;
                if i < m.records.len() {
                    if m.records[i] != *r {
                        return Err(reject("request_conflict"));
                    }
                } else {
                    m.records.push(r.clone());
                }
                prior = wire::record_digest(i, &prior, r)?;
            }
            if prior != p["sha256"] || m.records.len() > num(&m.proof, "records") {
                return Err(integrity());
            }
            // Enforce the accepted budget before accepting bytes, not only
            // after receiving the final directory page.
            let mut objects = 0usize;
            let mut bytes = 0usize;
            let mut seen = BTreeSet::new();
            for record in &m.records {
                if record["kind"] == "object" {
                    if !seen.insert(ref_key(&record["object"])?) {
                        return Err(integrity());
                    }
                    objects += 1;
                    bytes = bytes
                        .checked_add(num(&record["object"], "byteLength"))
                        .ok_or_else(integrity)?;
                }
            }
            if objects > num(&m.proof, "objects") || bytes > num(&m.proof, "bytes") {
                return Err(reject("custody_capacity_exceeded"));
            }
            if p["complete"] == true
                && (m.records.len() != num(&m.proof, "records")
                    || proof(&inner.state.identity, &m.records)? != m.proof)
            {
                return Err(integrity());
            }
            let mut next = inner.state.clone();
            next.custody = Some(custody);
            inner.save(next, before)
        })
        .await
    }
    async fn import_object(&self, ticket: &Value, chunk: &Value) -> Result<()> {
        let bytes = wire::chunk_bytes(chunk)?;
        let t = ticket.clone();
        let chunk = chunk.clone();
        self.run(move |inner, before| {
            let (key, intent) = original(&t)?;
            let m = inner.migration(&key, &intent)?;
            if m.source || m.activated || m.proof != t["proof"] {
                return Err(reject("request_conflict"));
            }
            if !m
                .records
                .iter()
                .any(|r| r["kind"] == "object" && r["object"] == chunk["object"])
            {
                return Err(integrity());
            }
            let reference = &chunk["object"];
            let k = ref_key(reference)?;
            let offset = num(&chunk, "offset");
            let mut data = inner.custody_read(&key, reference)?;
            if offset > data.len() || offset + bytes.len() > num(reference, "byteLength") {
                return Err(reject("custody_chunk_gap"));
            }
            if offset < data.len() {
                if data.get(offset..offset + bytes.len()) != Some(bytes.as_slice()) {
                    return Err(reject("request_conflict"));
                }
            } else {
                data.extend_from_slice(&bytes);
            }
            if data.len() == num(reference, "byteLength") && hash(&data) != reference["sha256"] {
                return Err(integrity());
            }
            inner.custody_write(&key, reference, &data, before)?;
            let mut next = inner.state.clone();
            next.custody
                .as_mut()
                .ok_or_else(integrity)?
                .migrations
                .get_mut(&key)
                .ok_or_else(integrity)?
                .received
                .insert(k, data.len());
            inner.save(next, before)
        })
        .await
    }
    async fn verify(&self, ticket: &Value) -> Result<Value> {
        let t = ticket.clone();
        self.run(move |inner, before| {
            let (key, intent) = original(&t)?;
            let m = inner.migration(&key, &intent)?.clone();
            if m.proof != t["proof"] {
                return Err(integrity());
            }
            m.budget.check(&m.proof)?;
            if m.activated {
                return Ok(m.proof.clone());
            }
            if proof(&inner.state.identity, &m.records)? != m.proof {
                return Err(integrity());
            }
            let next = inner.custody_state(&key, &m, before)?;
            let old = std::mem::replace(&mut inner.state, next);
            let checked = inner.audit();
            inner.state = old;
            checked?;
            Ok(m.proof.clone())
        })
        .await
    }
    async fn activate(&self, ticket: &Value) -> Result<()> {
        let t = ticket.clone();
        self.run(move |inner, before| {
            let (key, intent) = original(&t)?;
            let m = inner.migration(&key, &intent)?.clone();
            if t["phase"] != "published" || m.source || m.proof != t["proof"] {
                return Err(reject("custody_not_published"));
            }
            if m.activated {
                return Ok(());
            }
            let mut next = inner.custody_state(&key, &m, before)?;
            let c = next.custody.as_mut().ok_or_else(integrity)?;
            c.selected = t["selectedCustody"].clone();
            c.writer_fence = string(&t["writerFence"])?.into();
            c.frozen = false;
            c.migrations.get_mut(&key).ok_or_else(integrity)?.activated = true;
            let old = std::mem::replace(&mut inner.state, next.clone());
            let checked = inner.audit();
            inner.state = old;
            checked?;
            inner.save(next, before)
        })
        .await
    }
    async fn abort(&self, ticket: &Value) -> Result<()> {
        let t = ticket.clone();
        self.run(move |inner, before| {
            let (key, intent) = original(&t)?;
            let m = inner.migration(&key, &intent)?;
            if t["phase"] != "aborted" || m.activated {
                return Err(reject("custody_not_aborted"));
            }
            let mut next = inner.state.clone();
            if m.source {
                let c = next.custody.as_mut().ok_or_else(integrity)?;
                c.frozen = false;
                c.selected = t["selectedCustody"].clone();
                c.writer_fence = string(&t["writerFence"])?.into();
            }
            inner.save(next, before)
        })
        .await
    }
}

impl Inner {
    fn custody_state(&mut self, key: &str, m: &Migration, before: &Owner) -> Result<State> {
        if m.source || proof(&self.state.identity, &m.records)? != m.proof {
            return Err(integrity());
        }
        let mut next = self.state.clone();
        next.root = None;
        next.primary.clear();
        next.secondary.clear();
        next.tickets.clear();
        next.objects.clear();
        next.pending_objects.clear();
        next.retiring.clear();
        next.orphaned.clear();
        let mut map = BTreeMap::new();
        let mut control = None;
        let mut previous_kind = 0;
        let mut prior_transfer = String::new();
        let mut prior_object = String::new();
        for r in &m.records {
            match string(&r["kind"])? {
                "root" => {
                    if previous_kind != 0 || control.is_some() {
                        return Err(integrity());
                    }
                    next.root = (!r["root"].is_null()).then(|| r["root"].clone());
                    control = Some(r["controlRef"].clone());
                    previous_kind = 1;
                }
                "receipt-index" => {
                    if previous_kind > 2 || previous_kind == 0 {
                        return Err(integrity());
                    }
                    previous_kind = 2;
                    let e = &r["entry"];
                    let expected_ordinal = (next.primary.len() + 1).to_string();
                    if r["ordinal"].as_str() != Some(expected_ordinal.as_str())
                        || next
                            .secondary
                            .insert(
                                string(&e["secondaryKey"])?.into(),
                                string(&e["primaryKey"])?.into(),
                            )
                            .is_some()
                        || next
                            .primary
                            .insert(
                                string(&e["primaryKey"])?.into(),
                                EntryRow {
                                    ordinal: string(&r["ordinal"])?.into(),
                                    entry: e.clone(),
                                },
                            )
                            .is_some()
                    {
                        return Err(integrity());
                    }
                }
                "transfer" => {
                    if previous_kind > 3 || previous_kind == 0 {
                        return Err(integrity());
                    }
                    previous_kind = 3;
                    let transfer = string(&r["transferId"])?;
                    if transfer <= prior_transfer.as_str() {
                        return Err(integrity());
                    }
                    prior_transfer = transfer.into();
                }
                "object" => {
                    if previous_kind == 0 {
                        return Err(integrity());
                    }
                    previous_kind = 4;
                    let reference = &r["object"];
                    let object = ref_key(reference)?;
                    if object <= prior_object {
                        return Err(integrity());
                    }
                    prior_object = object.clone();
                    let data = self.custody_read(key, reference)?;
                    if data.len() != num(reference, "byteLength")
                        || hash(&data) != reference["sha256"]
                        || m.received.get(&object) != Some(&data.len())
                    {
                        return Err(integrity());
                    }
                    map.insert(object, (reference.clone(), data));
                }
                _ => return Err(reject("unsupported_origin_format")),
            }
        }
        let (control_ref, control_data) = map
            .get(&ref_key(&control.ok_or_else(integrity)?)?)
            .ok_or_else(integrity)?;
        if control_ref["kind"] != "custody-control" {
            return Err(integrity());
        }
        let control = crate::canonical::parse_json(control_data, 32768)?;
        wire::validate("ControlState", &control)?;
        // Source quota is a fact, never permission to enlarge this target's limits.
        for r in &m.records {
            if r["kind"] == "transfer" {
                let (_, data) = map
                    .get(&ref_key(&r["metadataRef"])?)
                    .ok_or_else(integrity)?;
                let portable = crate::canonical::parse_json(data, 524288)?;
                wire::validate("TransferMetadata", &portable)?;
                if portable["transfer"]["transferId"] != r["transferId"]
                    || portable["transfer"]["intentSha256"] != r["intentSha256"]
                    || portable["transfer"]["status"] != r["status"]
                {
                    return Err(integrity());
                }
                let mut received = BTreeSet::new();
                let mut available = BTreeSet::new();
                let mut pages = BTreeMap::new();
                for field in ["received", "reused"] {
                    let mut previous = String::new();
                    for reference in array(&portable[field])? {
                        let key = ref_key(reference)?;
                        if key <= previous || !available.insert(key.clone()) {
                            return Err(integrity());
                        }
                        previous = key.clone();
                        let (_, bytes) = map.get(&key).ok_or_else(integrity)?;
                        if bytes.len() != num(reference, "byteLength") {
                            return Err(integrity());
                        }
                        if field == "received" {
                            received.insert(key.clone());
                        }
                        if matches!(reference["kind"].as_str(), Some("body-page" | "index-page")) {
                            pages.insert(key, crate::canonical::parse_json(bytes, BLOCK_BYTES)?);
                        }
                    }
                }
                let t = Ticket {
                    begin: portable["begin"].clone(),
                    owner: serde_json::from_value(portable["owner"].clone())?,
                    base_root: (!portable["baseRoot"].is_null())
                        .then(|| portable["baseRoot"].clone()),
                    status: string(&r["status"])?.into(),
                    result: (!portable["transfer"]["result"].is_null())
                        .then(|| portable["transfer"]["result"].clone()),
                    rejection: (!portable["transfer"]["rejection"].is_null())
                        .then(|| portable["transfer"]["rejection"].clone()),
                    available,
                    received,
                    pages,
                };
                next.tickets.insert(string(&r["transferId"])?.into(), t);
            }
        }
        for (key, (reference, bytes)) in &map {
            if !string(&reference["kind"])?.starts_with("custody-") {
                split_key(key)?;
                next.objects.insert(key.clone(), bytes.len());
                let path = self.object_path(key)?;
                if path.try_exists()? {
                    if self.object(key)?.as_slice() != bytes {
                        return Err(integrity());
                    }
                } else {
                    // Immutable staging objects are not writer publication. Root and
                    // all original terminal facts are published by one metadata CAS.
                    self.check(before)?;
                    let sealed = self.cipher.seal(bytes, &self.object_aad(key)?)?;
                    self.index_burn(sealed.len(), 1, before)?;
                    let mut file = platform::open_at(&self.directory, &path, true, true)?;
                    file.write_all(&sealed)?;
                    file.sync_all()?;
                }
            }
        }
        // Compare every original capacity fact after rebuilding the native representation.
        let old = std::mem::replace(&mut self.state, next.clone());
        let projected = (|| {
            for record in &m.records {
                if record["kind"] != "transfer" {
                    continue;
                }
                let ticket = self
                    .state
                    .tickets
                    .get(string(&record["transferId"])?)
                    .ok_or_else(integrity)?;
                let (_, bytes) = map
                    .get(&ref_key(&record["metadataRef"])?)
                    .ok_or_else(integrity)?;
                let original = crate::canonical::parse_json(bytes, 524288)?;
                if self.portable_transfer(ticket)? != original {
                    return Err(integrity());
                }
            }
            self.capacity()
        })();
        self.state = old;
        let projected = projected?;
        if projected["used"] != control["capacity"] {
            return Err(integrity());
        }
        Ok(next)
    }
}
/// Apply original PST requests under the deployment's shared writer authority.
/// Reads remain available for recovery; writes never bypass custody after reopen.
pub struct FencedStore {
    store: Arc<FileStore>,
    control: Arc<dyn Control>,
    custody: Value,
    writer_fence: String,
}
impl FencedStore {
    pub fn new(
        store: Arc<FileStore>,
        control: Arc<dyn Control>,
        custody: Value,
        writer_fence: String,
    ) -> Result<Arc<Self>> {
        wire::validate("Custody", &custody)?;
        wire::validate("Sequence", &json!(writer_fence))?;
        Ok(Arc::new(Self {
            store,
            control,
            custody,
            writer_fence,
        }))
    }
}
#[async_trait]
impl PersistenceStore for FencedStore {
    fn identity(&self) -> &Identity {
        self.store.identity()
    }
    fn atomic_durable_publication(&self) -> bool {
        true
    }
    fn encrypted_at_rest(&self) -> bool {
        true
    }
    async fn execute(&self, request: Value, owner: Owner) -> Result<Value> {
        validate_request(&request, self.identity())?;
        owner.validate()?;
        if !matches!(request["action"].as_str(), Some("begin" | "put" | "commit")) {
            return self.store.execute(request, owner).await;
        }
        let (operation, intent) = wire::writer_operation(&request)?;
        let mut claim = json!({"contract":wire::CONTRACT,"identity":id(self.identity()),"operationId":operation,"intentSha256":intent,"action":"writer-claim","custody":self.custody,"expectedWriterFence":self.writer_fence});
        let authority = self.control.writer(&claim).await?;
        wire::validate("WriterClaim", &authority)?;
        if authority["operationId"] != operation
            || authority["intentSha256"] != intent
            || authority["selectedCustody"] != self.custody
            || authority["writerFence"] != self.writer_fence
            || authority["status"] == "unknown"
        {
            return Err(reject("custody_writer_fenced"));
        }
        let custody = self.custody.clone();
        let fence = self.writer_fence.clone();
        let replay_only = authority["status"] == "settled";
        let response = self
            .store
            .run(move |inner, before| {
                if owner != *before {
                    return Err(reject("context_changed"));
                }
                inner.index_prepare(&request)?;
                if let Some(current) = &inner.state.custody {
                    if current.frozen
                        || current.selected != custody
                        || current.writer_fence != fence
                    {
                        return Err(reject("custody_writer_fenced"));
                    }
                }
                if replay_only {
                    // A settled authority receipt is not permission to rebuild
                    // missing local effects. Read original durable transfer only.
                    let action = request["action"].clone();
                    let mut query = request;
                    query["action"] = json!("query");
                    let mut result = inner.execute(query, owner, before)?;
                    result["action"] = action;
                    return Ok(result);
                }
                if inner.state.custody.is_none() {
                    // Once shared custody is selected, reopening the raw store
                    // must never restore a path around the writer authority.
                    let mut next = inner.state.clone();
                    next.custody = Some(CustodyState {
                        selected: custody,
                        writer_fence: fence,
                        frozen: false,
                        migrations: BTreeMap::new(),
                    });
                    inner.save(next, before)?;
                }
                inner.execute(request, owner, before)
            })
            .await?;
        let digest = wire::digest(&response)?;
        if authority["status"] == "settled" {
            if authority["responseDigest"] != digest {
                return Err(integrity());
            }
            return Ok(response);
        }
        claim["action"] = json!("writer-finish");
        claim
            .as_object_mut()
            .ok_or_else(integrity)?
            .remove("expectedWriterFence");
        claim["claimId"] = authority["claimId"].clone();
        claim["writerFence"] = authority["writerFence"].clone();
        claim["settlement"] = json!({"state":"committed","responseDigest":digest});
        let settled = self.control.writer(&claim).await?;
        wire::validate("WriterClaim", &settled)?;
        if settled["status"] != "settled"
            || settled["operationId"] != operation
            || settled["intentSha256"] != intent
            || settled["writerFence"] != self.writer_fence
            || settled["responseDigest"] != digest
            || settled["claimId"] != authority["claimId"]
            || settled["selectedCustody"] != self.custody
        {
            return Err(integrity());
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn custody_queued_import_and_activate_recheck_original_authority() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/persistence-custody-node.json"
        ))
        .unwrap();
        let owner: Owner = serde_json::from_value(fixture["owner"].clone()).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().canonicalize().unwrap().join("private");
        crate::archive::create_private_directory(&path).unwrap();
        let current_owner = Arc::new(Mutex::new(owner.clone()));
        let read_owner = current_owner.clone();
        let store = Arc::new(
            FileStore::open(StoreOptions {
                path: path.join("store"),
                mode: OpenMode::Create,
                key: [41; 32],
                identity: serde_json::from_value(fixture["identity"].clone()).unwrap(),
                limits: Limits::default(),
                read_context: Arc::new(move || Ok(read_owner.lock().unwrap().clone())),
                authorize_recovery: None,
            })
            .await
            .unwrap(),
        );
        let port = CustodyPort::new(store.clone(), MigrationBudget::default()).unwrap();
        let allowed = Arc::new(AtomicBool::new(true));
        let check = allowed.clone();
        let scoped = port
            .for_operation(
                owner.clone(),
                Arc::new(move || {
                    if check.load(Ordering::SeqCst) {
                        Ok(())
                    } else {
                        Err(reject("management_revoked"))
                    }
                }),
            )
            .unwrap();

        let lane = store.lane.clone().acquire_owned().await.unwrap();
        let queued = scoped.clone();
        let ticket = fixture["ticket"].clone();
        let page = fixture["pages"][0].clone();
        let waiting = tokio::spawn(async move { queued.import_page(&ticket, &page).await });
        tokio::task::yield_now().await;
        allowed.store(false, Ordering::SeqCst);
        drop(lane);
        assert!(waiting.await.unwrap().is_err());
        assert!(
            store
                .inner
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .state
                .custody
                .is_none()
        );
        // A new owner in the same lane is also not the original operation.
        allowed.store(true, Ordering::SeqCst);
        current_owner.lock().unwrap().scope.authorization_revision = "2".into();
        assert!(
            scoped
                .import_page(&fixture["ticket"], &fixture["pages"][0])
                .await
                .is_err()
        );
        *current_owner.lock().unwrap() = owner;
        for page in fixture["pages"].as_array().unwrap() {
            port.import_page(&fixture["ticket"], page).await.unwrap();
        }
        for chunk in fixture["chunks"].as_array().unwrap() {
            port.import_object(&fixture["ticket"], chunk).await.unwrap();
        }
        port.verify(&fixture["ticket"]).await.unwrap();
        let lane = store.lane.clone().acquire_owned().await.unwrap();
        let queued = scoped.clone();
        let published = fixture["published"].clone();
        let waiting = tokio::spawn(async move { queued.activate(&published).await });
        tokio::task::yield_now().await;
        allowed.store(false, Ordering::SeqCst);
        drop(lane);
        assert!(waiting.await.unwrap().is_err());
        {
            let inner = store.inner.lock().unwrap();
            assert!(inner.as_ref().unwrap().state.root.is_none());
            assert!(
                !inner
                    .as_ref()
                    .unwrap()
                    .state
                    .custody
                    .as_ref()
                    .unwrap()
                    .migrations
                    .values()
                    .any(|m| m.activated)
            );
        }
        store.close().await.unwrap();
    }
}
