//! Read-only verification of frozen bytes, manifest sources and optional original Git blobs.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Component, Path},
    process::Command,
};

const SOURCE_COMMIT: &str = "83c64b2c519623a79994c942a5132300eb8174d4";
const SCHEMA_HASH: &str = "b60e77ffcbf08d985a993dbdbd4cf610f12f7c7e70f090aee5ff8d523f70bb57";
const LOCK_SHA256: &str = "f851aa0cee6d18c4b162cbd12f58e4f197debcfa2d9ac5decf62b2add35968cc";
pub const PUBLIC_FILES: [&str; 20] = [
    "api-error-map.json",
    "api-manifest.json",
    "archive-sync-v1.schema.json",
    "canonical-cross-vectors.json",
    "sdk2-archive-recovery-v1.golden.json",
    "sdk2-archive-recovery-v1.schema.json",
    "sdk2-cache-core-v1.schema.json",
    "sdk2-cache-v1.schema.json",
    "sdk2-ext-v1.schema.json",
    "sdk2-wire-v1.json",
    "terminal-observation-v1.golden.json",
    "terminal-observation-v1.schema.json",
    "terminal-profile-v1.golden.json",
    "terminal-profile-v1.schema.json",
    "terminal-services-v1.golden.json",
    "terminal-services-v1.schema.json",
    "terminal-shell-sandbox-v1.golden.json",
    "terminal-shell-sandbox-v1.schema.json",
    "unified-v1.golden.json",
    "unified-v1.schema.json",
];
const PUBLIC_METADATA: [&str; 3] = ["LOCK.json", "PROVENANCE.json", "DISTRIBUTION.json"];

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Distribution {
    format: String,
    pub scope: String,
    pub contract_file_count: usize,
    source_lock_file_count: usize,
    source_lock_sha256: String,
}

impl Distribution {
    fn public() -> Self {
        Self {
            format: "tansr-rust-contract-distribution-v1".into(),
            scope: "public".into(),
            contract_file_count: PUBLIC_FILES.len(),
            source_lock_file_count: 39,
            source_lock_sha256: LOCK_SHA256.into(),
        }
    }
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Lock {
    format: String,
    baseline: String,
    source_repo: String,
    source_branch: String,
    source_commit: String,
    manifest_revision: u64,
    schema_hash: String,
    operation_count: usize,
    family_count: usize,
    policy: String,
    files: Vec<File>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    path: String,
    source: String,
    sha256: String,
    role: String,
}
pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn valid_path(path: &str) -> bool {
    !path.is_empty()
        && !path.contains(['\\', ':'])
        && !path.chars().any(|ch| ch.is_control())
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
        && Path::new(path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn read_beneath(root: &Path, path: &str) -> Result<Vec<u8>, String> {
    if !valid_path(path) {
        return Err(format!("invalid relative path {path}"));
    }
    let root = root.canonicalize().map_err(|error| error.to_string())?;
    let target = root
        .join(path)
        .canonicalize()
        .map_err(|error| format!("read {path}: {error}"))?;
    if !target.starts_with(&root) || !target.is_file() {
        return Err(format!("path escapes root or is not a file: {path}"));
    }
    fs::read(target).map_err(|error| error.to_string())
}

pub fn check(root: &Path, source: Option<&Path>) -> Result<usize, String> {
    check_files(root, source, false)
}

fn check_files(root: &Path, source: Option<&Path>, public: bool) -> Result<usize, String> {
    let lock: Lock = serde_json::from_slice(&read_beneath(root, "LOCK.json")?)
        .map_err(|error| error.to_string())?;
    if lock.format != "tansr-sdk2-uapi-lock-v1"
        || lock.baseline != "sdk2-uapi-2026-10-07"
        || lock.source_repo != "cpple/tansr"
        || lock.source_branch != "main"
        || lock.source_commit != SOURCE_COMMIT
        || lock.manifest_revision != 7
        || lock.schema_hash != SCHEMA_HASH
        || lock.operation_count != 81
        || lock.family_count != 11
        || lock.files.len() != 39
        || lock.policy.trim().is_empty()
    {
        return Err("frozen lock identity or inventory mismatch".into());
    }
    let mut paths = BTreeSet::new();
    let mut sources = BTreeMap::new();
    for file in &lock.files {
        if !valid_path(&file.path)
            || !valid_path(&file.source)
            || file.path.eq_ignore_ascii_case("LOCK.json")
            || !paths.insert(file.path.to_lowercase())
            || sources.insert(file.source.to_lowercase(), file).is_some()
            || !["contract", "semantics", "reference"].contains(&file.role.as_str())
            || file.sha256.len() != 64
            || !file
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(format!(
                "invalid, duplicate or case-aliased lock entry {}",
                file.path
            ));
        }
        if (!public || PUBLIC_FILES.contains(&file.path.as_str()))
            && digest(&read_beneath(root, &file.path)?) != file.sha256
        {
            return Err(format!("{} frozen-byte drift: {}", file.role, file.path));
        }
    }
    let manifest: serde_json::Value =
        serde_json::from_slice(&read_beneath(root, "api-manifest.json")?)
            .map_err(|error| error.to_string())?;
    let families = manifest["families"]
        .as_array()
        .ok_or("manifest has no families")?;
    if manifest["revision"] != lock.manifest_revision
        || manifest["schemaHash"] != lock.schema_hash
        || manifest["format"] != "tansr-api-manifest-v1"
        || manifest["contract"] != "unified-v1"
        || families.len() != 11
        || manifest["operations"].as_array().map(Vec::len) != Some(81)
    {
        return Err("manifest inventory differs from lock".into());
    }
    let mut hashes = Vec::new();
    let mut family_ids = BTreeSet::new();
    for family in families {
        let id = family["id"].as_str().ok_or("family lacks id")?;
        if !family_ids.insert(id) {
            return Err("duplicate family".into());
        }
        let family_source = family["source"].as_str().ok_or("family lacks source")?;
        let hash = family["sha256"].as_str().ok_or("family lacks sha256")?;
        let file = sources
            .get(&family_source.to_lowercase())
            .ok_or("family source absent from lock")?;
        if file.source != family_source || file.sha256 != hash || file.role != "contract" {
            return Err(format!("family source mismatch {id}"));
        }
        for (source_path, golden_hash) in
            family["golden"].as_object().ok_or("family lacks golden")?
        {
            let file = sources
                .get(&source_path.to_lowercase())
                .ok_or("golden source absent from lock")?;
            if file.source != *source_path || golden_hash.as_str() != Some(&file.sha256) {
                return Err(format!("golden source mismatch {id}"));
            }
        }
        hashes.push(format!("{id}:{hash}\n"));
    }
    hashes.sort();
    if digest(hashes.concat().as_bytes()) != SCHEMA_HASH {
        return Err("aggregate schema hash mismatch".into());
    }
    if let Some(source) = source {
        let ancestor = Command::new("git")
            .arg("-C")
            .arg(source)
            .args(["merge-base", "--is-ancestor", SOURCE_COMMIT, "HEAD"])
            .output()
            .map_err(|error| error.to_string())?;
        if !ancestor.status.success() {
            return Err("source baseline is not an ancestor of checkout HEAD".into());
        }
        for file in &lock.files {
            if digest(&read_beneath(source, &file.source)?) != file.sha256 {
                return Err(format!("working source drift: {}", file.source));
            }
            let object = format!("{SOURCE_COMMIT}:{}", file.source);
            let committed = Command::new("git")
                .arg("-C")
                .arg(source)
                .args(["cat-file", "blob", &object])
                .output()
                .map_err(|error| error.to_string())?;
            if !committed.status.success() || digest(&committed.stdout) != file.sha256 {
                return Err(format!("original committed source drift: {}", file.source));
            }
        }
    }
    Ok(if public {
        PUBLIC_FILES.len()
    } else {
        lock.files.len()
    })
}

/// Check the explicit distribution; missing or malformed declarations never select public mode.
pub fn check_distribution(root: &Path) -> Result<Distribution, String> {
    let distribution = read_distribution(root)?;
    if distribution.scope == "public" {
        check_public(root)?;
    } else {
        check(root, None)?;
    }
    Ok(distribution)
}

fn read_distribution(root: &Path) -> Result<Distribution, String> {
    let distribution: Distribution =
        serde_json::from_slice(&read_beneath(root, "DISTRIBUTION.json")?)
            .map_err(|error| format!("invalid contract distribution: {error}"))?;
    let expected_count = match distribution.scope.as_str() {
        "internal" => 39,
        "public" => PUBLIC_FILES.len(),
        _ => return Err("unknown contract distribution scope".into()),
    };
    if distribution.format != "tansr-rust-contract-distribution-v1"
        || distribution.contract_file_count != expected_count
        || distribution.source_lock_file_count != 39
        || distribution.source_lock_sha256 != LOCK_SHA256
        || digest(&read_beneath(root, "LOCK.json")?) != LOCK_SHA256
    {
        return Err("contract distribution identity or source lock mismatch".into());
    }
    Ok(distribution)
}

/// Verify 20 authorized contract assets, not the 19 private source files named by LOCK.json.
pub fn check_public(root: &Path) -> Result<usize, String> {
    if read_distribution(root)?.scope != "public" {
        return Err("public check requires an explicit public distribution".into());
    }
    // Anchor all 39 source paths/hashes even though private bytes are deliberately absent.
    if digest(&read_beneath(root, "LOCK.json")?) != LOCK_SHA256 {
        return Err("public source lock metadata drift".into());
    }
    let expected: BTreeSet<&str> = PUBLIC_FILES.into_iter().chain(PUBLIC_METADATA).collect();
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "non-UTF-8 public contract path")?;
        if !expected.contains(name.as_str())
            || !entry
                .file_type()
                .map_err(|error| error.to_string())?
                .is_file()
        {
            return Err(format!("unauthorized public contract entry: {name}"));
        }
        actual.insert(name);
    }
    if actual.iter().map(String::as_str).collect::<BTreeSet<_>>() != expected {
        return Err(
            "public contract inventory mismatch: expected 20 assets and 3 metadata files".into(),
        );
    }
    check_provenance(root)?;
    check_files(root, None, true)
}

fn check_provenance(root: &Path) -> Result<(), String> {
    let provenance: serde_json::Value =
        serde_json::from_slice(&read_beneath(root, "PROVENANCE.json")?)
            .map_err(|error| format!("invalid provenance: {error}"))?;
    let provenance_keys = [
        "description",
        "sourceRepo",
        "sourceBranch",
        "sourceCommit",
        "observed",
        "files",
        "freeze",
    ];
    if !has_exact_keys(&provenance, &provenance_keys)
        || provenance["description"]
            .as_str()
            .is_none_or(|value| value.trim().is_empty())
        || provenance["observed"]
            .as_str()
            .is_none_or(|value| value.trim().is_empty())
        || provenance["sourceRepo"] != "cpple/tansr"
        || provenance["sourceBranch"] != "main"
        || provenance["sourceCommit"] != SOURCE_COMMIT
        || provenance["freeze"] != "LOCK.json"
    {
        return Err("public provenance source identity mismatch".into());
    }
    let lock: Lock = serde_json::from_slice(&read_beneath(root, "LOCK.json")?)
        .map_err(|error| error.to_string())?;
    let provenance_paths = [
        "api-manifest.json",
        "unified-v1.schema.json",
        "unified-v1.golden.json",
        "canonical-cross-vectors.json",
        "sdk2-wire-v1.json",
    ];
    if !has_exact_keys(&provenance["files"], &provenance_paths) {
        return Err("public provenance file inventory mismatch".into());
    }
    for (path, metadata) in provenance["files"]
        .as_object()
        .ok_or("provenance lacks files")?
    {
        let file = lock
            .files
            .iter()
            .find(|file| file.path == *path)
            .filter(|_| PUBLIC_FILES.contains(&path.as_str()))
            .ok_or("provenance entry absent from public allowlist")?;
        if metadata["source"] != file.source || metadata["sha256"] != file.sha256 {
            return Err(format!("public provenance source mismatch: {path}"));
        }
        let keys: &[&str] = match path.as_str() {
            "api-manifest.json" => &[
                "source",
                "sha256",
                "revision",
                "schemaHash",
                "operations",
                "closureOperations",
            ],
            "unified-v1.golden.json" => &[
                "source",
                "sha256",
                "schemaSource",
                "schemaSha256",
                "vectors",
            ],
            _ => &["source", "sha256"],
        };
        if !has_exact_keys(metadata, keys) {
            return Err(format!("public provenance fields mismatch: {path}"));
        }
    }
    let manifest = &provenance["files"]["api-manifest.json"];
    let golden = &provenance["files"]["unified-v1.golden.json"];
    let schema = &provenance["files"]["unified-v1.schema.json"];
    if manifest["revision"] != 7
        || manifest["schemaHash"] != SCHEMA_HASH
        || manifest["operations"] != 81
        || manifest["closureOperations"] != 77
        || golden["schemaSource"] != schema["source"]
        || golden["schemaSha256"] != schema["sha256"]
        || golden["vectors"] != 165
    {
        return Err("public provenance manifest or golden metadata mismatch".into());
    }
    Ok(())
}

fn has_exact_keys(value: &serde_json::Value, keys: &[&str]) -> bool {
    value.as_object().is_some_and(|object| {
        object.len() == keys.len() && keys.iter().all(|key| object.contains_key(*key))
    })
}

/// Export only after the internal 39-file gate passes, without overwriting a target.
pub fn export_public(root: &Path, output: &Path) -> Result<usize, String> {
    if read_distribution(root)?.scope != "internal" {
        return Err("contract export requires an explicit internal distribution".into());
    }
    check(root, None)?;
    // Validate and retain all output bytes before touching the new target.
    if digest(&read_beneath(root, "LOCK.json")?) != LOCK_SHA256 {
        return Err("public source lock metadata drift".into());
    }
    check_provenance(root)?;
    let mut files = Vec::new();
    for path in PUBLIC_FILES
        .into_iter()
        .chain(["LOCK.json", "PROVENANCE.json"])
    {
        files.push((path, read_beneath(root, path)?));
    }
    let mut declaration =
        serde_json::to_vec_pretty(&Distribution::public()).map_err(|error| error.to_string())?;
    declaration.push(b'\n');
    files.push(("DISTRIBUTION.json", declaration));
    let root = root.canonicalize().map_err(|error| error.to_string())?;
    let output = if output.is_absolute() {
        output.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| error.to_string())?
            .join(output)
    };
    let parent = output
        .parent()
        .ok_or("export output has no parent")?
        .canonicalize()
        .map_err(|error| format!("resolve export parent: {error}"))?;
    let output = parent.join(
        output
            .file_name()
            .ok_or("export output has no directory name")?,
    );
    if output.starts_with(&root) {
        return Err("export output must be outside the source contract directory".into());
    }
    fs::create_dir(&output).map_err(|error| format!("create new export directory: {error}"))?;
    for (path, bytes) in files {
        write_new(&output.join(path), &bytes)?;
    }
    check_distribution(&output).map(|distribution| distribution.contract_file_count)
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<(), String> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .and_then(|mut file| file.write_all(bytes))
        .map_err(|error| format!("write export {}: {error}", path.display()))
}
