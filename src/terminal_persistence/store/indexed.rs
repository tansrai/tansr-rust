//! Explicit indexed medium. `State` is used only as the original reducer's
//! private working cache; its aggregate authority is this authenticated head.
//! Complete State validation is reserved for explicit maintenance hydration.
use super::authentication_tree as tree;
use super::*;
use std::cell::RefCell;
#[cfg(test)]
#[path = "indexed_tests.rs"]
mod tests;

pub(super) const INDEX_FORMAT: &str = "tansr-rust-terminal-persistence-indexed-v2";
const PHYSICAL_CAP: usize = 128 << 20;
const KEY_CAP: usize = 1_000_000;
const ROW_CAP: usize = META_CAP;
// Only an individual active-workset preparation shares immutable node bytes.
// 1024 entries retain at most 512 KiB of plaintext, plus bounded map overhead.
const PROOF_NODE_CACHE_CAP: usize = 1024;
type ProofNodes = RefCell<BTreeMap<String, Zeroizing<Vec<u8>>>>;

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Measure {
    count: usize,
    members: usize,
    nodes: usize,
}
impl Measure {
    fn set(value: &BTreeSet<String>) -> Result<Self> {
        Ok(Self {
            count: value.len(),
            members: value
                .iter()
                .map(serde_json::to_vec)
                .collect::<std::result::Result<Vec<_>, _>>()?
                .iter()
                .map(Vec::len)
                .sum(),
            nodes: value.len(),
        })
    }
    fn of(value: &Value) -> Result<Self> {
        let map = value.as_object().ok_or_else(integrity)?;
        let mut result = Self::default();
        for (key, value) in map {
            result.count += 1;
            result.members += serde_json::to_vec(key)?.len() + 1 + serde_json::to_vec(value)?.len();
            result.nodes += node_count(value);
        }
        Ok(result)
    }
    fn bytes(&self) -> usize {
        2 + self.members + self.count.saturating_sub(1)
    }
    fn changed(&self, old: &Self, next: &Self) -> Result<Self> {
        let delta = |total: usize, a: usize, b: usize| {
            total
                .checked_sub(a)
                .and_then(|n| n.checked_add(b))
                .ok_or_else(integrity)
        };
        Ok(Self {
            count: delta(self.count, old.count, next.count)?,
            members: delta(self.members, old.members, next.members)?,
            nodes: delta(self.nodes, old.nodes, next.nodes)?,
        })
    }
}
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Totals {
    primary: Measure,
    secondary: Measure,
    tickets: Measure,
    objects: Measure,
    object_bytes: usize,
    entry_bytes: usize,
    ticket_bytes: usize,
    custody_bytes: usize,
    custody_nodes: usize,
    orphan: Measure,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Head {
    format: String,
    identity: Identity,
    limits: Limits,
    epoch: String,
    revision: u64,
    root: Option<Value>,
    read_only_copy: bool,
    custody: Option<custody::CustodyState>,
    active: BTreeMap<String, Ticket>,
    pending_objects: BTreeMap<String, String>,
    retiring: BTreeMap<String, usize>,
    retirement_candidates: BTreeSet<String>,
    tree: Option<String>,
    leaf_count: usize,
    totals: Totals,
    // Durable physical allocation is a high-water budget, not business facts.
    burned_bytes: usize,
    burned_keys: usize,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Burn {
    format: String,
    identity: Identity,
    epoch: String,
    bytes: usize,
    keys: usize,
}
pub(super) struct Indexed {
    head: Head,
    baseline: State,
    complete: bool,
    selected: BTreeSet<(String, String)>,
    burn: Burn,
    clear_candidates: bool,
}
fn state(head: &Head) -> State {
    State {
        format: FORMAT.into(),
        identity: head.identity.clone(),
        limits: head.limits,
        root: head.root.clone(),
        read_only_copy: head.read_only_copy,
        custody: head.custody.clone(),
        primary: BTreeMap::new(),
        secondary: BTreeMap::new(),
        tickets: head.active.clone(),
        objects: BTreeMap::new(),
        pending_objects: head.pending_objects.clone(),
        retiring: head.retiring.clone(),
        orphaned: BTreeSet::new(),
    }
}
fn rows(s: &State) -> Result<BTreeMap<(String, String), Value>> {
    let mut rows = BTreeMap::new();
    for (kind, value) in [
        ("primary", serde_json::to_value(&s.primary)?),
        ("secondary", serde_json::to_value(&s.secondary)?),
        ("ticket", serde_json::to_value(&s.tickets)?),
        ("object", serde_json::to_value(&s.objects)?),
    ] {
        for (k, v) in value.as_object().ok_or_else(integrity)? {
            // Complete active responsibility is in the authenticated head. Only
            // settled tickets belong to permanent rows.
            if kind == "ticket" && v["status"] == "staging" {
                continue;
            }
            rows.insert((kind.into(), k.clone()), v.clone());
        }
    }
    for key in &s.orphaned {
        rows.insert(("orphan".into(), key.clone()), json!(true));
    }
    Ok(rows)
}
fn key(kind: &str, id: &str) -> Result<String> {
    canonical_hash(&json!([kind, id]))
}
fn envelope(kind: &str, id: &str, value: &Value) -> Result<Vec<u8>> {
    crate::canonical::encode_limited(&json!({"kind":kind,"key":id,"value":value}), ROW_CAP)
}
impl Indexed {
    pub(super) fn empty(identity: Identity, limits: Limits) -> Self {
        let mut nonce = [0; 32];
        OsRng.fill_bytes(&mut nonce);
        let epoch = hash(&nonce);
        let head = Head {
            format: INDEX_FORMAT.into(),
            identity: identity.clone(),
            limits,
            epoch: epoch.clone(),
            revision: 0,
            root: None,
            read_only_copy: false,
            custody: None,
            active: BTreeMap::new(),
            pending_objects: BTreeMap::new(),
            retiring: BTreeMap::new(),
            retirement_candidates: BTreeSet::new(),
            tree: None,
            leaf_count: 0,
            totals: Totals {
                custody_bytes: 4,
                custody_nodes: 1,
                ..Totals::default()
            },
            burned_bytes: 0,
            burned_keys: 0,
        };
        Self {
            baseline: state(&head),
            head,
            complete: false,
            selected: BTreeSet::new(),
            burn: Burn {
                format: INDEX_FORMAT.into(),
                identity,
                epoch,
                bytes: 0,
                keys: 0,
            },
            clear_candidates: false,
        }
    }
}
impl Inner {
    pub(super) fn index_initialize_copy(&mut self, before: &Owner) -> Result<()> {
        if self.indexed.is_some() {
            self.index_write_burn(before)?;
        }
        Ok(())
    }
    pub(super) fn index_copy_fingerprints(
        &self,
        files: &mut BTreeMap<PathBuf, String>,
    ) -> Result<()> {
        let Some(i) = &self.indexed else {
            return Ok(());
        };
        let collected = RefCell::new(BTreeMap::new());
        tree::visit(
            i.head.tree.as_deref(),
            |id| {
                let path = self.index_path("node", id)?;
                let bytes = self.read_file(&path, 512)?;
                collected.borrow_mut().insert(path, hash(&bytes));
                let plain = self.cipher.open(&bytes, &self.index_aad("node", id)?)?;
                Ok(plain.to_vec())
            },
            |_, id| {
                let path = self.index_path("row", id)?;
                let bytes = self.read_file(&path, ROW_CAP)?;
                collected.borrow_mut().insert(path, hash(&bytes));
                let plain = self.cipher.open(&bytes, &self.index_aad("row", id)?)?;
                if hash(&plain) != id {
                    return Err(integrity());
                }
                Ok(())
            },
        )?;
        files.extend(collected.into_inner());
        let p = self.index_path("burn", "")?;
        files.insert(p.clone(), hash(&self.read_file(&p, 32768)?));
        Ok(())
    }
    pub(super) fn open_indexed(o: StoreOptions, reconcile: bool) -> Result<Self> {
        o.limits.validate()?;
        validate_request(&request(&o.identity, "head"), &o.identity)?;
        let directory = platform::verify_parent(&o.path)?;
        let name = o
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(integrity)?;
        let lock = platform::open_at(
            &directory,
            &o.path.with_file_name(format!("{name}.lock")),
            true,
            false,
        )?;
        fs2::FileExt::try_lock_exclusive(&lock).map_err(|_| reject("store_in_use"))?;
        let i = Indexed::empty(o.identity.clone(), o.limits);
        let mut inner = Self {
            path: o.path,
            directory,
            _lock: lock,
            cipher: Cipher::new(o.key),
            aad: crate::canonical::encode(&json!([INDEX_FORMAT, o.identity]))?,
            state: state(&i.head),
            context: o.read_context,
            recovery: o.authorize_recovery,
            commit_guard: None,
            uncertain: false,
            fingerprint: None,
            indexed: Some(i),
            #[cfg(test)]
            io: Default::default(),
        };
        let before = inner.context()?;
        match o.mode {
            OpenMode::Create => {
                // New media only. This one-time admission check never runs on
                // normal reopen; it does not authenticate or adopt old objects.
                let prefix = format!(
                    "{}.",
                    inner
                        .path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .ok_or_else(integrity)?
                );
                if inner.path.try_exists()?
                    || std::fs::read_dir(inner.path.parent().ok_or_else(integrity)?)?.any(|e| {
                        e.map_or(true, |e| {
                            let name = e.file_name();
                            let s = name.to_string_lossy();
                            s.starts_with(&prefix) && s != format!("{prefix}lock")
                        })
                    })
                {
                    return Err(reject("target_exists"));
                }
                inner.index_write_burn(&before)?;
                inner.index_save(inner.state.clone(), &before)?;
            }
            OpenMode::Reopen => {
                let blob = inner.read_file(&inner.path, META_CAP)?;
                let plain = inner.cipher.open(&blob, &inner.aad)?;
                let h: Head =
                    serde_json::from_value(crate::canonical::parse_json(&plain, META_CAP)?)?;
                if h.format != INDEX_FORMAT
                    || h.identity != inner.state.identity
                    || h.limits != inner.state.limits
                    || !tree::digest(&h.epoch)
                    || h.tree.as_deref().is_some_and(|v| !tree::digest(v))
                    || h.active.len() > h.limits.active_transfers
                    || h.pending_objects.len() > 1
                    || h.burned_bytes > PHYSICAL_CAP
                    || h.burned_keys > KEY_CAP
                    || h.leaf_count > KEY_CAP
                    || h.tree.is_none() != (h.leaf_count == 0)
                {
                    return Err(integrity());
                }
                for (m, max) in [
                    (&h.totals.primary, h.limits.receipt_entries),
                    (&h.totals.secondary, h.limits.receipt_entries),
                    (&h.totals.tickets, h.limits.transfer_facts),
                    (&h.totals.objects, h.limits.objects),
                ] {
                    if m.count > max
                        || m.members > META_CAP
                        || m.nodes > crate::canonical::MAX_NODES
                    {
                        return Err(integrity());
                    }
                }
                if h.totals.primary.count != h.totals.secondary.count
                    || h.totals.tickets.count < h.active.len()
                    || [
                        h.totals.object_bytes,
                        h.totals.entry_bytes,
                        h.totals.ticket_bytes,
                    ]
                    .iter()
                    .any(|n| *n > h.limits.retained_bytes)
                    || h.totals.custody_bytes > META_CAP
                    || h.totals.orphan.members > META_CAP
                    || h.totals.orphan.count > h.limits.objects
                    || h.totals.custody_nodes > crate::canonical::MAX_NODES
                    || h.totals.orphan.nodes > crate::canonical::MAX_NODES
                    || h.retirement_candidates.len() > h.limits.objects
                {
                    return Err(integrity());
                }
                if serde_json::to_value(&h.custody)?
                    != serde_json::to_value(
                        h.custody.as_ref().map(custody::CustodyState::indexed_fence),
                    )?
                {
                    return Err(integrity());
                }
                if let Some(root) = &h.root {
                    verify_root(root, &h.identity)?;
                    if root["index"]["count"] != json!(h.totals.primary.count.to_string()) {
                        return Err(integrity());
                    }
                } else if h.totals.primary.count != 0 {
                    return Err(integrity());
                }
                let burn: Burn = serde_json::from_value(crate::canonical::parse_json(
                    &inner.index_read("burn", "")?,
                    32768,
                )?)?;
                if burn.format != INDEX_FORMAT
                    || burn.identity != h.identity
                    || burn.epoch != h.epoch
                    || burn.bytes < h.burned_bytes
                    || burn.keys < h.burned_keys
                    || burn.bytes > PHYSICAL_CAP
                    || burn.keys > KEY_CAP
                {
                    return Err(integrity());
                }
                inner.state = state(&h);
                inner.fingerprint = Some(hash(&blob));
                inner.indexed = Some(Indexed {
                    baseline: inner.state.clone(),
                    head: h,
                    complete: false,
                    selected: BTreeSet::new(),
                    burn,
                    clear_candidates: false,
                });
                inner.index_prepare(&request(&inner.state.identity, "head"))?;
                for (id, t) in &inner.state.tickets {
                    validate_request(&t.begin, &inner.state.identity)?;
                    validate_begin(&t.begin)?;
                    t.owner.validate()?;
                    if t.status != "staging"
                        || t.begin["transferId"] != *id
                        || t.result.is_some()
                        || t.rejection.is_some()
                        || !t.received.is_subset(&t.available)
                        || t.owner.scope.application_scope_id
                            != inner.state.identity.application_scope_id
                        || t.owner.scope.end_user_id != inner.state.identity.end_user_id
                    {
                        return Err(integrity());
                    }
                    if let Some(r) = &t.base_root {
                        verify_root(r, &inner.state.identity)?;
                    }
                    for (k, p) in &t.pages {
                        let (kind, _) = split_key(k)?;
                        let bytes = crate::canonical::encode(p)?;
                        validate_page(kind, num(p, "index"), &bytes, &t.begin)?;
                        if hash(&bytes) != split_key(k)?.1 || !t.available.contains(k) {
                            return Err(integrity());
                        }
                    }
                    inner.plan(t)?;
                }
                inner.within_capacity()?;
            }
        }
        if inner.state.read_only_copy {
            if !inner.state.pending_objects.is_empty() {
                return Err(integrity());
            }
        } else if reconcile {
            inner.settle_objects(&before)?;
        }
        inner.check(&before)?;
        inner.index_reset()?;
        Ok(inner)
    }
    fn index_write_burn(&mut self, before: &Owner) -> Result<()> {
        self.check(before)?;
        let burn = &self.indexed.as_ref().ok_or_else(integrity)?.burn;
        let bytes = crate::canonical::encode(&serde_json::to_value(burn)?)?;
        let blob = self.cipher.seal(&bytes, &self.index_aad("burn", "")?)?;
        let temp = self.temporary()?;
        let mut f = platform::open_at(&self.directory, &temp, true, true)?;
        f.write_all(&blob)?;
        f.sync_all()?;
        drop(f);
        self.check(before)?;
        if platform::replace_at(&self.directory, &temp, &self.index_path("burn", "")?).is_err() {
            self.uncertain = true;
            return Err(integrity());
        }
        Ok(())
    }
    pub(super) fn index_burn(&mut self, bytes: usize, keys: usize, before: &Owner) -> Result<()> {
        let Some(i) = self.indexed.as_mut() else {
            return Ok(());
        };
        let b = i
            .burn
            .bytes
            .checked_add(bytes)
            .filter(|n| *n <= PHYSICAL_CAP)
            .ok_or_else(|| reject("capacity_exceeded"))?;
        let k = i
            .burn
            .keys
            .checked_add(keys)
            .filter(|n| *n <= KEY_CAP)
            .ok_or_else(|| reject("capacity_exceeded"))?;
        i.burn.bytes = b;
        i.burn.keys = k;
        // Burn is durable before any immutable write. A lost head never loses
        // physical orphan responsibility, and reopening never scans a directory.
        self.index_write_burn(before)
    }
    fn index_write_blob(&self, part: &str, id: &str, bytes: &[u8]) -> Result<()> {
        let path = self.index_path(part, id)?;
        if path.try_exists()? {
            if self.index_read(part, id)? != bytes {
                return Err(integrity());
            }
            return Ok(());
        }
        let sealed = self.cipher.seal(bytes, &self.index_aad(part, id)?)?;
        let mut f = platform::open_at(&self.directory, &path, true, true)?;
        f.write_all(&sealed)?;
        f.sync_all()?;
        Ok(())
    }
    fn completion_reserve(&self, next: &State, leaves: usize) -> Result<usize> {
        let mut result = 0usize;
        let mut max_leaves = leaves;
        for t in next.tickets.values().filter(|t| t.status == "staging") {
            max_leaves += num(&t.begin["declared"], "objects")
                .checked_sub(t.available.len())
                .ok_or_else(integrity)?
                + 3 * num(&t.begin["index"], "addedCount")
                + 1;
        }
        let full_tree_nodes = 2 * max_leaves.max(1) - 1;
        let root_objects = next
            .root
            .as_ref()
            .map(|r| {
                num(&r["body"], "blockCount")
                    + r["body"]["pageHashes"].as_array().map_or(0, Vec::len)
            })
            .unwrap_or(0);
        for t in next.tickets.values().filter(|t| t.status == "staging") {
            let missing = num(&t.begin["declared"], "objects")
                .checked_sub(t.available.len())
                .ok_or_else(integrity)?;
            let added = num(&t.begin["index"], "addedCount");
            // Active progress stays in a replaced head, not immutable versions.
            // Object facts and final dual-key receipts reserve bounded tree paths.
            let commit_rows = 3 * added + 1;
            let retirement_rows = root_objects + num(&t.begin["declared"], "objects");
            let changed = missing + commit_rows + retirement_rows;
            let nodes = missing * 257.min(full_tree_nodes)
                + (257 * commit_rows).min(full_tree_nodes)
                + (257 * retirement_rows).min(full_tree_nodes);
            result = result
                .checked_add(
                    missing * (BLOCK_BYTES + 28)
                        + nodes * (261 + 28)
                        + changed * 1024
                        + PHYSICAL_RESERVE,
                )
                .ok_or_else(integrity)?;
        }
        Ok(result)
    }
    pub(super) fn index_save(&mut self, next: State, before: &Owner) -> Result<()> {
        if self.state.read_only_copy || (next.read_only_copy && self.fingerprint.is_some()) {
            return Err(reject("copy_read_only"));
        }
        self.check(before)?;
        let i = self.indexed.as_ref().ok_or_else(integrity)?;
        let old = rows(&i.baseline)?;
        let mut changed = rows(&next)?;
        let mut original = old.clone();
        if i.complete || i.baseline.custody.is_none() && next.custody.is_some() {
            if let Some(c) = &i.baseline.custody {
                original.insert(
                    ("custody".into(), "complete".into()),
                    serde_json::to_value(c)?,
                );
            }
            if let Some(c) = &next.custody {
                changed.insert(
                    ("custody".into(), "complete".into()),
                    serde_json::to_value(c)?,
                );
            }
        }
        let mut h = i.head.clone();
        let totals = self.index_measure(&next)?;
        if i.baseline.root != next.root {
            if let Some(root) = &i.baseline.root {
                for p in array(&root["body"]["pageHashes"])? {
                    h.retirement_candidates
                        .insert(object_key("body-page", string(p)?));
                }
                for r in self.body_refs(root)? {
                    h.retirement_candidates
                        .insert(object_key("body-block", string(&r["sha256"])?));
                }
            }
        }
        for (id, t) in &i.baseline.tickets {
            if t.status == "staging" && next.tickets.get(id).is_none_or(|n| n.status != "staging") {
                h.retirement_candidates.extend(t.available.iter().cloned());
            }
        }
        if i.clear_candidates {
            h.retirement_candidates.clear();
        }
        for row in next.primary.values() {
            changed.insert(
                ("pin".into(), string(&row.entry["value"]["sha256"])?.into()),
                json!(true),
            );
        }
        for row in i.baseline.primary.values() {
            original.insert(
                ("pin".into(), string(&row.entry["value"]["sha256"])?.into()),
                json!(true),
            );
        }
        let pending = RefCell::new(BTreeMap::<String, Vec<u8>>::new());
        let mut row_blobs = BTreeMap::new();
        let keys = original
            .keys()
            .chain(changed.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        for (kind, id) in keys {
            if original.get(&(kind.clone(), id.clone())) == changed.get(&(kind.clone(), id.clone()))
            {
                continue;
            }
            let bytes = envelope(
                &kind,
                &id,
                changed
                    .get(&(kind.clone(), id.clone()))
                    .unwrap_or(&Value::Null),
            )?;
            let digest = hash(&bytes);
            row_blobs.insert(digest.clone(), bytes);
            if tree::get(h.tree.as_deref(), &key(&kind, &id)?, |n| {
                pending
                    .borrow()
                    .get(n)
                    .cloned()
                    .map(Ok)
                    .unwrap_or_else(|| self.index_read("node", n))
            })?
            .is_none()
            {
                h.leaf_count = h
                    .leaf_count
                    .checked_add(1)
                    .filter(|n| *n <= KEY_CAP)
                    .ok_or_else(integrity)?;
            }
            h.tree = Some(tree::put(
                h.tree.as_deref(),
                &key(&kind, &id)?,
                &digest,
                |n| {
                    pending
                        .borrow()
                        .get(n)
                        .cloned()
                        .map(Ok)
                        .unwrap_or_else(|| self.index_read("node", n))
                },
                |n, b| {
                    pending.borrow_mut().insert(n, b);
                    Ok(())
                },
            )?);
        }
        // Only reachable final nodes are persisted; transient COW intermediates
        // are neither physical objects nor extra budget consumption.
        let mut reachable = BTreeSet::new();
        fn collect(h: &str, p: &BTreeMap<String, Vec<u8>>, r: &mut BTreeSet<String>) -> Result<()> {
            if let Some(b) = p.get(h) {
                if !r.insert(h.into()) {
                    return Ok(());
                }
                let v = crate::canonical::parse_strict(b, 512)?;
                if v["kind"] == "branch" {
                    collect(string(&v["left"])?, p, r)?;
                    collect(string(&v["right"])?, p, r)?;
                }
            }
            Ok(())
        }
        if let Some(root) = &h.tree {
            collect(root, &pending.borrow(), &mut reachable)?;
        }
        let bytes = row_blobs.values().map(|b| b.len() + 28).sum::<usize>()
            + reachable
                .iter()
                .map(|n| pending.borrow()[n].len() + 28)
                .sum::<usize>();
        let writes = row_blobs.len() + reachable.len();
        let leaves = h.leaf_count;
        if i.burn
            .bytes
            .checked_add(bytes)
            .and_then(|n| n.checked_add(self.completion_reserve(&next, leaves).ok()?))
            .is_none_or(|n| n > PHYSICAL_CAP)
        {
            return Err(reject("capacity_exceeded"));
        }
        self.index_burn(bytes, writes, before)?;
        for (id, b) in &row_blobs {
            self.check(before)?;
            self.index_write_blob("row", id, b)?;
        }
        for id in &reachable {
            self.check(before)?;
            self.index_write_blob("node", id, &pending.borrow()[id])?;
        }
        h.revision = h.revision.checked_add(1).ok_or_else(integrity)?;
        h.root = next.root.clone();
        h.read_only_copy = next.read_only_copy;
        h.custody = next
            .custody
            .as_ref()
            .map(custody::CustodyState::indexed_fence);
        h.active = next
            .tickets
            .iter()
            .filter(|(_, t)| t.status == "staging")
            .map(|(id, t)| (id.clone(), t.clone()))
            .collect();
        h.pending_objects = next.pending_objects.clone();
        h.retiring = next.retiring.clone();
        h.totals = totals;
        h.burned_bytes = self.indexed.as_ref().unwrap().burn.bytes;
        h.burned_keys = self.indexed.as_ref().unwrap().burn.keys;
        let plain = Zeroizing::new(serde_json::to_vec(&h)?);
        crate::canonical::parse_json(&plain, META_CAP).map_err(|_| reject("capacity_exceeded"))?;
        let blob = self.cipher.seal(&plain, &self.aad)?;
        let temporary = self.temporary()?;
        let mut f = platform::open_at(&self.directory, &temporary, true, true)?;
        f.write_all(&blob)?;
        f.sync_all()?;
        drop(f);
        self.check(before)?;
        platform::reject_links(&self.path, true)?;
        if self.fingerprint.is_none() && self.path.try_exists()? {
            return Err(reject("target_exists"));
        }
        if platform::replace_at(&self.directory, &temporary, &self.path).is_err() {
            self.uncertain = true;
            return Err(integrity());
        }
        self.fingerprint = Some(hash(&blob));
        self.state = next;
        let i = self.indexed.as_mut().unwrap();
        i.head = h;
        i.baseline = self.state.clone();
        i.clear_candidates = false;
        self.uncertain = false;
        Ok(())
    }
    pub(super) fn index_complete(&mut self) -> Result<()> {
        if self.indexed.as_ref().is_none_or(|i| i.complete) {
            return Ok(());
        }
        let mut all = state(&self.indexed.as_ref().unwrap().head);
        let mut pins = BTreeSet::new();
        let mut leaves = 0usize;
        let root = self.indexed.as_ref().unwrap().head.tree.clone();
        tree::visit(
            root.as_deref(),
            |n| self.index_read("node", n),
            |locator, digest| {
                leaves += 1;
                if leaves > KEY_CAP {
                    return Err(integrity());
                }
                let bytes = self.index_read("row", digest)?;
                let r = crate::canonical::parse_strict(&bytes, ROW_CAP)?;
                let kind = string(&r["kind"])?;
                let id = string(&r["key"])?;
                if r.as_object().is_none_or(|o| o.len() != 3) || key(kind, id)? != locator {
                    return Err(integrity());
                }
                if r["value"].is_null() {
                    return Ok(());
                }
                match kind {
                    "primary" => {
                        all.primary
                            .insert(id.into(), serde_json::from_value(r["value"].clone())?);
                    }
                    "secondary" => {
                        all.secondary.insert(id.into(), string(&r["value"])?.into());
                    }
                    "ticket" => {
                        if all
                            .tickets
                            .insert(id.into(), serde_json::from_value(r["value"].clone())?)
                            .is_some()
                        {
                            return Err(integrity());
                        }
                    }
                    "object" => {
                        all.objects.insert(
                            id.into(),
                            r["value"]
                                .as_u64()
                                .and_then(|n| usize::try_from(n).ok())
                                .ok_or_else(integrity)?,
                        );
                    }
                    "orphan" if r["value"] == true => {
                        all.orphaned.insert(id.into());
                    }
                    "pin" if tree::digest(id) && r["value"] == true => {
                        pins.insert(id.to_owned());
                    }
                    "custody" if id == "complete" => {
                        let c: custody::CustodyState = serde_json::from_value(r["value"].clone())?;
                        all.custody = Some(c);
                    }
                    _ => return Err(integrity()),
                }
                Ok(())
            },
        )?;
        if leaves != self.indexed.as_ref().unwrap().head.leaf_count {
            return Err(integrity());
        }
        let expected = all
            .primary
            .values()
            .map(|r| string(&r.entry["value"]["sha256"]).map(str::to_owned))
            .collect::<Result<BTreeSet<_>>>()?;
        if pins != expected {
            return Err(integrity());
        }
        if serde_json::to_value(
            all.custody
                .as_ref()
                .map(custody::CustodyState::indexed_fence),
        )? != serde_json::to_value(&self.indexed.as_ref().unwrap().head.custody)?
        {
            return Err(integrity());
        }
        let i = self.indexed.as_mut().unwrap();
        i.complete = true;
        self.state = all;
        if serde_json::to_value(self.index_measure(&self.state)?)?
            != serde_json::to_value(&self.indexed.as_ref().unwrap().head.totals)?
        {
            return Err(integrity());
        }
        self.indexed.as_mut().unwrap().baseline = self.state.clone();
        self.audit()?;
        Ok(())
    }
}
impl Inner {
    pub(super) fn index_object_measure(&self) -> Result<Option<(usize, usize)>> {
        if self.indexed.as_ref().is_none_or(|i| i.complete) {
            return Ok(None);
        }
        let totals = self.index_measure(&self.state)?;
        Ok(Some((totals.objects.count, totals.objects.bytes())))
    }
    pub(super) fn index_object_length(&self, id: &str) -> Result<Option<usize>> {
        if self.indexed.as_ref().is_none_or(|i| i.complete) {
            return Ok(None);
        }
        if let Some(len) = self
            .state
            .objects
            .get(id)
            .or_else(|| self.state.retiring.get(id))
        {
            return Ok(Some(*len));
        }
        let v = self.index_row("object", id)?.ok_or_else(integrity)?;
        let len = v
            .as_u64()
            .filter(|n| *n > 0 && *n <= BLOCK_BYTES as u64)
            .ok_or_else(integrity)?;
        Ok(Some(len as usize))
    }
    pub(super) fn index_compact(&mut self, before: &Owner) -> Result<bool> {
        if self.indexed.as_ref().is_none_or(|i| i.complete) {
            return Ok(false);
        }
        if self.state.read_only_copy {
            return Err(reject("copy_read_only"));
        }
        self.check(before)?;
        self.settle_objects(before)?;
        let candidates = self
            .indexed
            .as_ref()
            .unwrap()
            .head
            .retirement_candidates
            .clone();
        if candidates.is_empty() && self.state.retiring.is_empty() {
            return Ok(true);
        }
        let mut keep = BTreeSet::new();
        for root in self.state.root.iter().chain(
            self.state
                .tickets
                .values()
                .filter(|t| t.status == "staging")
                .filter_map(|t| t.base_root.as_ref()),
        ) {
            for h in array(&root["body"]["pageHashes"])? {
                keep.insert(object_key("body-page", string(h)?));
            }
            for r in self.body_refs(root)? {
                keep.insert(object_key("body-block", string(&r["sha256"])?));
            }
        }
        for t in self
            .state
            .tickets
            .values()
            .filter(|t| t.status == "staging")
        {
            keep.extend(t.available.iter().cloned());
        }
        for k in &candidates {
            self.index_select("object", k)?;
        }
        let mut next = self.state.clone();
        for k in &candidates {
            let pinned = split_key(k)?.0 == "receipt-value"
                && self.index_row("pin", split_key(k)?.1)?.is_some();
            if !keep.contains(k) && !pinned && self.index_row("orphan", k)?.is_none() {
                if let Some(len) = next.objects.remove(k) {
                    next.retiring.insert(k.clone(), len);
                }
            }
        }
        if next.retiring.keys().any(|k| keep.contains(k)) {
            return Err(integrity());
        }
        self.indexed.as_mut().unwrap().clear_candidates = true;
        if !candidates.is_empty() {
            self.save(next, before)?;
        }
        for k in self.state.retiring.keys() {
            self.check(before)?;
            let path = self.object_path(k)?;
            if path.try_exists()? {
                self.object(k)?;
                platform::remove_at(&self.directory, &path)?;
            }
        }
        if !self.state.retiring.is_empty() {
            let mut next = self.state.clone();
            next.retiring.clear();
            self.save(next, before)?;
        }
        Ok(true)
    }
}
impl Inner {
    fn index_path(&self, part: &str, id: &str) -> Result<PathBuf> {
        if !matches!(part, "node" | "row" | "burn") || (part != "burn" && !tree::digest(id)) {
            return Err(integrity());
        }
        Ok(self.path.with_file_name(format!(
            "{}.indexed.{part}{id}",
            self.path
                .file_name()
                .and_then(|v| v.to_str())
                .ok_or_else(integrity)?
        )))
    }
    fn index_aad(&self, part: &str, id: &str) -> Result<Vec<u8>> {
        crate::canonical::encode(&json!([INDEX_FORMAT, self.state.identity, part, id]))
    }
    fn index_read(&self, part: &str, id: &str) -> Result<Vec<u8>> {
        let cap = match part {
            "node" => 512,
            "burn" => 32768,
            _ => ROW_CAP,
        };
        let blob = self.read_file(&self.index_path(part, id)?, cap)?;
        let plain = self.cipher.open(&blob, &self.index_aad(part, id)?)?;
        if part != "burn" && hash(&plain) != id {
            return Err(integrity());
        }
        Ok(plain.to_vec())
    }
    fn index_row(&self, kind: &str, id: &str) -> Result<Option<Value>> {
        self.index_row_with_nodes(kind, id, None)
    }
    fn index_row_with_nodes(
        &self,
        kind: &str,
        id: &str,
        nodes: Option<&ProofNodes>,
    ) -> Result<Option<Value>> {
        let indexed = self.indexed.as_ref().ok_or_else(integrity)?;
        let digest = tree::get(indexed.head.tree.as_deref(), &key(kind, id)?, |h| {
            if let Some(bytes) = nodes.and_then(|nodes| nodes.borrow().get(h).map(|b| b.to_vec())) {
                return Ok(bytes);
            }
            let bytes = self.index_read("node", h)?;
            if let Some(nodes) = nodes {
                let mut nodes = nodes.borrow_mut();
                if nodes.len() < PROOF_NODE_CACHE_CAP {
                    nodes.insert(h.into(), Zeroizing::new(bytes.clone()));
                }
            }
            Ok(bytes)
        })?;
        let Some(digest) = digest else {
            return Ok(None);
        };
        let bytes = self.index_read("row", &digest)?;
        let row = crate::canonical::parse_strict(&bytes, ROW_CAP)?;
        if row.as_object().is_none_or(|o| o.len() != 3) || row["kind"] != kind || row["key"] != id {
            return Err(integrity());
        }
        Ok((!row["value"].is_null()).then(|| row["value"].clone()))
    }
    fn index_select(&mut self, kind: &str, id: &str) -> Result<()> {
        self.index_select_with_nodes(kind, id, None, true)
    }
    fn index_select_with_nodes(
        &mut self,
        kind: &str,
        id: &str,
        nodes: Option<&ProofNodes>,
        refresh_baseline: bool,
    ) -> Result<()> {
        let Some(i) = self.indexed.as_ref() else {
            return Ok(());
        };
        if i.complete
            || i.selected.contains(&(kind.into(), id.into()))
            || kind == "ticket"
                && self
                    .state
                    .tickets
                    .get(id)
                    .is_some_and(|t| t.status == "staging")
        {
            return Ok(());
        }
        let value = self.index_row_with_nodes(kind, id, nodes)?;
        if let Some(value) = value {
            match kind {
                "primary" => {
                    let row: EntryRow = serde_json::from_value(value)?;
                    crate::api::validate_wire(CONTRACT, "Entry", &row.entry)?;
                    if row.entry["primaryKey"] != id
                        || row
                            .ordinal
                            .parse::<usize>()
                            .ok()
                            .filter(|n| {
                                *n > 0
                                    && *n
                                        <= self.indexed.as_ref().unwrap().head.totals.primary.count
                            })
                            .is_none()
                    {
                        return Err(integrity());
                    }
                    self.state.primary.insert(id.into(), row);
                }
                "secondary" => {
                    self.state
                        .secondary
                        .insert(id.into(), string(&value)?.into());
                }
                "object" => {
                    split_key(id)?;
                    let len = value
                        .as_u64()
                        .and_then(|v| usize::try_from(v).ok())
                        .filter(|n| *n > 0 && *n <= BLOCK_BYTES)
                        .ok_or_else(integrity)?;
                    self.state.objects.insert(id.into(), len);
                }
                "orphan" => {
                    if value != true {
                        return Err(integrity());
                    }
                    self.state.orphaned.insert(id.into());
                }
                "ticket" => {
                    let t: Ticket = serde_json::from_value(value)?;
                    validate_request(&t.begin, &self.state.identity)?;
                    validate_begin(&t.begin)?;
                    t.owner.validate()?;
                    if t.status == "staging"
                        || t.begin["transferId"] != id
                        || t.owner.scope.application_scope_id
                            != self.state.identity.application_scope_id
                        || t.owner.scope.end_user_id != self.state.identity.end_user_id
                        || t.base_root.is_some()
                        || !t.available.is_empty()
                        || !t.received.is_empty()
                        || !t.pages.is_empty()
                    {
                        return Err(integrity());
                    }
                    if t.status == "committed" {
                        verify_root(
                            t.result.as_ref().ok_or_else(integrity)?,
                            &self.state.identity,
                        )?;
                    }
                    crate::api::validate_wire(CONTRACT, "Transfer", &self.transfer(&t)?)?;
                    self.state.tickets.insert(id.into(), t);
                }
                _ => return Err(integrity()),
            }
        }
        let i = self.indexed.as_mut().unwrap();
        i.selected.insert((kind.into(), id.into()));
        if refresh_baseline {
            i.baseline = self.state.clone();
        }
        Ok(())
    }
    pub(super) fn index_prepare(&mut self, input: &Value) -> Result<()> {
        if self.indexed.as_ref().is_none_or(|i| i.complete) {
            return Ok(());
        }
        let mut objects = BTreeSet::new();
        for t in self
            .state
            .tickets
            .values()
            .filter(|t| t.status == "staging")
        {
            objects.extend(t.available.iter().cloned());
        }
        objects.extend(self.state.pending_objects.keys().cloned());
        // Keep every exact proof and row check, but read shared content-addressed
        // nodes once in this synchronous preparation. tree::get still checks each
        // path's parent/bit/direction relations. No cache crosses a call or await;
        // the existing owner and head checks still bracket the whole operation.
        let nodes = ProofNodes::default();
        for k in &objects {
            self.index_select_with_nodes("object", k, Some(&nodes), false)?;
            if !self.state.objects.contains_key(k) {
                return Err(integrity());
            }
        }
        drop(nodes);
        if !objects.is_empty() {
            // This phase only hydrates verified object rows. Publish the same
            // selected baseline once, before any reducer mutation, instead of
            // cloning the entire active ticket/pages after every object row.
            self.indexed.as_mut().unwrap().baseline = self.state.clone();
        }
        let action = string(&input["action"])?;
        if let Some(id) = input["transferId"].as_str() {
            self.index_select("ticket", id)?;
        }
        if action == "lookup" {
            let digest = string(&input["key"]["digest"])?;
            let primary = if input["key"]["kind"] == "secondary" {
                self.index_select("secondary", digest)?;
                self.state.secondary.get(digest).cloned()
            } else {
                Some(digest.into())
            };
            if let Some(p) = primary {
                self.index_select("primary", &p)?;
                if input["key"]["kind"] == "secondary" && !self.state.primary.contains_key(&p) {
                    return Err(integrity());
                }
                if let Some(row) = self.state.primary.get(&p) {
                    let secondary = string(&row.entry["secondaryKey"])?.to_owned();
                    self.index_select("secondary", &secondary)?;
                    if self.state.secondary.get(&secondary) != Some(&p)
                        || (input["key"]["kind"] == "secondary" && secondary != digest)
                    {
                        return Err(integrity());
                    }
                }
            }
        }
        if action == "put" {
            self.index_select(
                "object",
                &object_key(string(&input["kind"])?, string(&input["sha256"])?),
            )?;
            self.index_select(
                "orphan",
                &object_key(string(&input["kind"])?, string(&input["sha256"])?),
            )?;
            if input["kind"] == "index-page" {
                let bytes = decode(string(&input["base64"])?)?;
                let p = crate::canonical::parse_json(&bytes, BLOCK_BYTES)?;
                self.index_select_entries(&p)?;
            }
        }
        if action == "commit" || action == "put" {
            if let Some(t) = self
                .state
                .tickets
                .get(string(&input["transferId"])?)
                .cloned()
            {
                for p in t.pages.values().filter(|p| p["kind"] == "index-page") {
                    self.index_select_entries(p)?;
                }
            }
        }
        Ok(())
    }
    fn index_select_entries(&mut self, page: &Value) -> Result<()> {
        for e in array(&page["entries"])? {
            self.index_select("primary", string(&e["primaryKey"])?)?;
            self.index_select("secondary", string(&e["secondaryKey"])?)?;
        }
        Ok(())
    }
    pub(super) fn index_reset(&mut self) -> Result<()> {
        let Some(i) = self.indexed.as_mut() else {
            return Ok(());
        };
        self.state = state(&i.head);
        i.baseline = self.state.clone();
        i.selected.clear();
        i.complete = false;
        i.clear_candidates = false;
        Ok(())
    }
    pub(super) fn index_primary_count(&self, s: &State) -> Result<usize> {
        match &self.indexed {
            Some(i) if !i.complete => i
                .head
                .totals
                .primary
                .count
                .checked_sub(i.baseline.primary.len())
                .and_then(|n| n.checked_add(s.primary.len()))
                .ok_or_else(integrity),
            _ => Ok(s.primary.len()),
        }
    }
    fn index_measure(&self, s: &State) -> Result<Totals> {
        let mut result = Totals::default();
        let old = self
            .indexed
            .as_ref()
            .filter(|i| !i.complete)
            .map(|i| (&i.baseline, &i.head.totals));
        for (field, next_value, old_value, target) in [
            (
                "primary",
                serde_json::to_value(&s.primary)?,
                old.map(|(s, _)| serde_json::to_value(&s.primary))
                    .transpose()?,
                &mut result.primary,
            ),
            (
                "secondary",
                serde_json::to_value(&s.secondary)?,
                old.map(|(s, _)| serde_json::to_value(&s.secondary))
                    .transpose()?,
                &mut result.secondary,
            ),
            (
                "tickets",
                serde_json::to_value(&s.tickets)?,
                old.map(|(s, _)| serde_json::to_value(&s.tickets))
                    .transpose()?,
                &mut result.tickets,
            ),
            (
                "objects",
                serde_json::to_value(&s.objects)?,
                old.map(|(s, _)| serde_json::to_value(&s.objects))
                    .transpose()?,
                &mut result.objects,
            ),
        ] {
            let m = Measure::of(&next_value)?;
            *target = if let Some((_, totals)) = old {
                let base = match field {
                    "primary" => &totals.primary,
                    "secondary" => &totals.secondary,
                    "tickets" => &totals.tickets,
                    _ => &totals.objects,
                };
                base.changed(&Measure::of(&old_value.unwrap())?, &m)?
            } else {
                m
            };
        }
        let contribution = |s: &State| -> Result<(usize, usize, usize)> {
            let mut object = 0;
            for (k, len) in &s.objects {
                object += len + object_metadata(k, *len)?;
            }
            let mut entries = 0;
            for row in s.primary.values() {
                entries += encoded_len(&json!({"ordinal":row.ordinal,"entry":row.entry}))?;
            }
            let mut tickets = 0;
            for t in s.tickets.values() {
                tickets += encoded_len(
                    &json!({"begin":t.begin,"owner":t.owner,"baseRoot":t.base_root,"transfer":self.transfer_for_state(t,s)?}),
                )?;
            }
            Ok((object, entries, tickets))
        };
        let n = contribution(s)?;
        let totals = if let Some((before, base)) = old {
            let o = contribution(before)?;
            let d = |a: usize, b: usize, c: usize| {
                a.checked_sub(b)
                    .and_then(|v| v.checked_add(c))
                    .ok_or_else(integrity)
            };
            (
                d(base.object_bytes, o.0, n.0)?,
                d(base.entry_bytes, o.1, n.1)?,
                d(base.ticket_bytes, o.2, n.2)?,
            )
        } else {
            n
        };
        result.object_bytes = totals.0;
        result.entry_bytes = totals.1;
        result.ticket_bytes = totals.2;
        let cv = serde_json::to_value(&s.custody)?;
        if let Some((before, base)) = old {
            let ov = serde_json::to_value(&before.custody)?;
            result.custody_bytes = base
                .custody_bytes
                .checked_sub(serde_json::to_vec(&ov)?.len())
                .and_then(|n| n.checked_add(serde_json::to_vec(&cv).ok()?.len()))
                .ok_or_else(integrity)?;
            result.custody_nodes = base
                .custody_nodes
                .checked_sub(node_count(&ov))
                .and_then(|n| n.checked_add(node_count(&cv)))
                .ok_or_else(integrity)?;
        } else {
            result.custody_bytes = serde_json::to_vec(&cv)?.len();
            result.custody_nodes = node_count(&cv);
        }
        let orphan = Measure::set(&s.orphaned)?;
        if let Some((before, base)) = old {
            result.orphan = base
                .orphan
                .changed(&Measure::set(&before.orphaned)?, &orphan)?;
        } else {
            result.orphan = orphan;
        }
        Ok(result)
    }
    fn transfer_for_state(&self, t: &Ticket, s: &State) -> Result<Value> {
        // Transfer progress only depends on available object lengths. The normal
        // cache contains complete active responsibility plus exact selected rows.
        if t.status != "staging" {
            return self.transfer(t);
        }
        let progress = self.plan_objects(t, &s.objects)?;
        let body = progress
            .body
            .iter()
            .map(|r| {
                r.as_ref().is_some_and(|r| {
                    t.available.contains(&object_key(
                        "body-block",
                        r["sha256"].as_str().unwrap_or(""),
                    ))
                })
            })
            .collect::<Vec<_>>();
        let values = progress
            .entries
            .iter()
            .map(|e| {
                e.as_ref().is_some_and(|e| {
                    t.available.contains(&object_key(
                        "receipt-value",
                        e["value"]["sha256"].as_str().unwrap_or(""),
                    ))
                })
            })
            .collect::<Vec<_>>();
        let received = t.received.iter().try_fold(0usize, |n, k| {
            s.objects
                .get(k)
                .and_then(|v| n.checked_add(*v))
                .ok_or_else(integrity)
        })?;
        Ok(
            json!({"transferId":t.begin["transferId"],"intentSha256":t.begin["intentSha256"],"status":t.status,"progress":{"pagesReady":bits(&progress.pages),"bodyReady":bits(&body),"valuesReady":bits(&values),"receivedBytes":received},"result":t.result,"rejection":t.rejection}),
        )
    }
    pub(super) fn index_capacity(&self) -> Result<Option<Value>> {
        if self.indexed.as_ref().is_none_or(|i| i.complete) {
            return Ok(None);
        }
        let s = &self.state;
        let totals = self.index_measure(s)?;
        let mut retained = totals.object_bytes + totals.entry_bytes + totals.ticket_bytes;
        for (k, len) in &s.retiring {
            retained += len + object_metadata(k, *len)?;
        }
        if let Some(root) = &s.root {
            retained += encoded_len(root)?;
        }
        let (mut reserved, mut reserved_objects, mut reserved_entries, mut active, mut staging) =
            (0, 0, 0, 0, 0);
        for t in s.tickets.values().filter(|t| t.status == "staging") {
            active += 1;
            let meta = encoded_len(
                &json!({"begin":t.begin,"owner":t.owner,"baseRoot":t.base_root,"transfer":self.transfer(t)?}),
            )?;
            let (mut raw, mut reused, mut ready_meta) = (0, 0, 0);
            for k in &t.available {
                let len = *s.objects.get(k).ok_or_else(integrity)?;
                raw += len;
                ready_meta += object_metadata(k, len)?;
                if !t.received.contains(k) {
                    reused += len;
                }
            }
            let d = &t.begin["declared"];
            if t.available.len() > num(d, "objects")
                || raw > num(d, "bytes")
                || meta + ready_meta > RESERVE
            {
                return Err(reject("invalid_request"));
            }
            reserved += num(d, "bytes") - raw + RESERVE - meta - ready_meta;
            reserved_objects += num(d, "objects") - t.available.len();
            reserved_entries += num(&t.begin["index"], "addedCount");
            staging += num(d, "bytes") - reused;
        }
        Ok(Some(
            json!({"limits":s.limits,"used":{"activeTransfers":active,"stagingBytes":staging,"receiptEntries":totals.primary.count,"transferFacts":totals.tickets.count,"objects":totals.objects.count+s.retiring.len(),"retainedBytes":retained,"reservedBytes":reserved,"reservedObjects":reserved_objects,"reservedReceiptEntries":reserved_entries}}),
        ))
    }
    pub(super) fn index_physical_metadata(&self) -> Result<Option<(usize, usize)>> {
        if self.indexed.as_ref().is_none_or(|i| i.complete) {
            return Ok(None);
        }
        let value = serde_json::to_value(&self.state)?;
        let totals = self.index_measure(&self.state)?;
        let mut bytes = serde_json::to_vec(&value)?.len();
        let mut nodes = node_count(&value);
        for (name, m) in [
            ("primary", &totals.primary),
            ("secondary", &totals.secondary),
            ("tickets", &totals.tickets),
            ("objects", &totals.objects),
        ] {
            bytes = bytes - serde_json::to_vec(&value[name])?.len() + m.bytes();
            nodes = nodes - node_count(&value[name]) + 1 + m.nodes;
        }
        if value.get("custody").is_some() {
            bytes = bytes - serde_json::to_vec(&value["custody"])?.len() + totals.custody_bytes;
            nodes = nodes - node_count(&value["custody"]) + totals.custody_nodes;
        }
        bytes = bytes - serde_json::to_vec(&value["orphaned"])?.len() + totals.orphan.bytes();
        nodes = nodes - node_count(&value["orphaned"]) + 1 + totals.orphan.nodes;
        Ok(Some((bytes, nodes)))
    }
}
