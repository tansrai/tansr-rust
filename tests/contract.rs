#[path = "../xtask/src/contract.rs"]
mod contract;
#[path = "../xtask/src/manifest.rs"]
mod manifest;

use std::{collections::BTreeSet, fs, path::Path};
use tansr_sdk::api::operations::{MANIFEST_REVISION, SCHEMA_HASH, get, operations};

#[test]
fn frozen_contract_distribution_has_its_declared_asset_count() {
    let distribution = contract::check_distribution(&source_contract()).unwrap();
    let count = match distribution.scope.as_str() {
        "internal" => 39,
        "public" => 20,
        other => panic!("unsupported distribution {other}"),
    };
    assert_eq!(distribution.contract_file_count, count);
    eprintln!(
        "verified {} distribution: {count} frozen assets; 39 source metadata entries",
        distribution.scope
    );
}

fn source_contract() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("contract")
}

fn public_fixture() -> tempfile::TempDir {
    let fixture = tempfile::tempdir().unwrap();
    let source = source_contract();
    for path in contract::PUBLIC_FILES
        .into_iter()
        .chain(["LOCK.json", "PROVENANCE.json"])
    {
        fs::copy(source.join(path), fixture.path().join(path)).unwrap();
    }
    let mut declaration: serde_json::Value =
        serde_json::from_slice(&fs::read(source.join("DISTRIBUTION.json")).unwrap()).unwrap();
    declaration["scope"] = "public".into();
    declaration["contractFileCount"] = 20.into();
    fs::write(
        fixture.path().join("DISTRIBUTION.json"),
        serde_json::to_vec(&declaration).unwrap(),
    )
    .unwrap();
    fixture
}

#[test]
fn operation_catalogue_is_generated_and_complete() {
    let generated = manifest::render(include_bytes!("../contract/api-manifest.json")).unwrap();
    assert!(
        generated.as_bytes() == include_bytes!("../src/api/operations.rs"),
        "operation catalogue drift; run cargo run -p xtask -- generate"
    );
    let catalogue = operations();
    assert_eq!(MANIFEST_REVISION, 7);
    assert_eq!(
        SCHEMA_HASH,
        "b60e77ffcbf08d985a993dbdbd4cf610f12f7c7e70f090aee5ff8d523f70bb57"
    );
    assert_eq!(catalogue.len(), 81);
    assert_eq!(
        catalogue
            .iter()
            .map(|operation| operation.name)
            .collect::<BTreeSet<_>>()
            .len(),
        81
    );
    assert_eq!(
        catalogue
            .iter()
            .filter(|operation| operation.fenced)
            .count(),
        77
    );
    assert_eq!(
        catalogue
            .iter()
            .filter(|operation| operation.kind == "read")
            .count(),
        39
    );
    assert_eq!(
        catalogue
            .iter()
            .filter(|operation| operation.kind == "write")
            .count(),
        38
    );
    assert_eq!(
        catalogue.iter().filter(|operation| operation.sse).count(),
        4
    );
    assert_eq!(
        catalogue
            .iter()
            .filter(|operation| operation.expected_revision.is_some())
            .count(),
        9
    );
    assert!(get("does.not.exist").is_none());
    for operation in catalogue {
        assert_eq!(get(operation.name), Some(operation));
    }
}

#[test]
fn generator_rejects_protocol_drift_and_check_does_not_rewrite() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir_all(root.path().join("src/api")).unwrap();
    fs::create_dir(root.path().join("contract")).unwrap();
    let source = include_bytes!("../contract/api-manifest.json");
    fs::write(root.path().join("contract/api-manifest.json"), source).unwrap();
    manifest::generate(root.path(), false).unwrap();
    manifest::generate(root.path(), true).unwrap();
    fs::write(root.path().join("src/api/operations.rs"), "changed").unwrap();
    assert!(manifest::generate(root.path(), true).is_err());
    assert_eq!(
        fs::read_to_string(root.path().join("src/api/operations.rs")).unwrap(),
        "changed"
    );
    let mut value: serde_json::Value = serde_json::from_slice(source).unwrap();
    value["operations"][0]["apiPath"] = "/v3/sdk2/forbidden".into();
    assert!(manifest::render(&serde_json::to_vec(&value).unwrap()).is_err());
    value["operations"][0]["apiPath"] = "/api/manifest".into();
    value["operations"][1]["name"] = value["operations"][0]["name"].clone();
    assert!(manifest::render(&serde_json::to_vec(&value).unwrap()).is_err());
}

#[test]
fn contract_lock_rejects_byte_drift_and_manifest_metadata_tampering() {
    let fixture = public_fixture();
    let source = source_contract();
    let raw_lock = fs::read(source.join("LOCK.json")).unwrap();
    let lock: serde_json::Value = serde_json::from_slice(&raw_lock).unwrap();
    assert_eq!(contract::check_public(fixture.path()).unwrap(), 20);
    let path = fixture.path().join("api-manifest.json");
    let original = fs::read(&path).unwrap();
    let mut changed = original.clone();
    changed.push(b'\n');
    fs::write(&path, changed).unwrap();
    assert!(
        contract::check_public(fixture.path())
            .unwrap_err()
            .contains("frozen-byte drift")
    );
    fs::write(&path, &original).unwrap();
    fs::remove_file(&path).unwrap();
    assert!(
        contract::check_public(fixture.path())
            .unwrap_err()
            .contains("inventory mismatch")
    );
    fs::write(&path, original).unwrap();
    let mut changed = lock.clone();
    changed["sourceCommit"] = "0000000000000000000000000000000000000000".into();
    fs::write(
        fixture.path().join("LOCK.json"),
        serde_json::to_vec(&changed).unwrap(),
    )
    .unwrap();
    assert!(
        contract::check(fixture.path(), None)
            .unwrap_err()
            .contains("identity")
    );
    let mut changed = lock;
    changed["files"][0]["path"] = "../outside.json".into();
    fs::write(
        fixture.path().join("LOCK.json"),
        serde_json::to_vec(&changed).unwrap(),
    )
    .unwrap();
    assert!(
        contract::check(fixture.path(), None)
            .unwrap_err()
            .contains("invalid")
    );
    assert!(contract::check_public(fixture.path()).is_err());
}

#[test]
fn public_gate_rejects_private_extra_and_missing_assets() {
    let fixture = public_fixture();
    for path in contract::PUBLIC_FILES {
        let target = fixture.path().join(path);
        let bytes = fs::read(&target).unwrap();
        fs::write(&target, b"{}").unwrap();
        assert!(
            contract::check_public(fixture.path())
                .unwrap_err()
                .contains("frozen-byte drift"),
            "{path}"
        );
        fs::remove_file(&target).unwrap();
        assert!(
            contract::check_public(fixture.path())
                .unwrap_err()
                .contains("inventory mismatch"),
            "{path}"
        );
        fs::write(target, bytes).unwrap();
    }
    for path in ["unexpected.json", "sdk2-archive-recovery-v1.sqlite.sql"] {
        fs::write(fixture.path().join(path), b"private").unwrap();
        assert!(
            contract::check_public(fixture.path())
                .unwrap_err()
                .contains("unauthorized")
        );
        fs::remove_file(fixture.path().join(path)).unwrap();
    }
    fs::create_dir(fixture.path().join("reference")).unwrap();
    assert!(
        contract::check_public(fixture.path())
            .unwrap_err()
            .contains("unauthorized")
    );
    fs::remove_dir(fixture.path().join("reference")).unwrap();
    // Rename, rather than add an alias, also exercises case-insensitive Windows filesystems.
    fs::rename(
        fixture.path().join("api-error-map.json"),
        fixture.path().join("API-error-map.json"),
    )
    .unwrap();
    assert!(
        contract::check_public(fixture.path())
            .unwrap_err()
            .contains("unauthorized")
    );
}

#[test]
fn distribution_never_infers_public_from_missing_private_files() {
    let fixture = public_fixture();
    let declaration_path = fixture.path().join("DISTRIBUTION.json");
    let bytes = fs::read(&declaration_path).unwrap();
    fs::remove_file(&declaration_path).unwrap();
    assert!(
        contract::check_distribution(fixture.path())
            .unwrap_err()
            .contains("DISTRIBUTION.json")
    );
    fs::write(&declaration_path, &bytes).unwrap();
    let mut declaration: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    declaration["scope"] = "internal".into();
    declaration["contractFileCount"] = 39.into();
    fs::write(&declaration_path, serde_json::to_vec(&declaration).unwrap()).unwrap();
    assert!(
        contract::check_distribution(fixture.path())
            .unwrap_err()
            .contains("read reference/")
    );
    assert!(
        contract::check_public(fixture.path())
            .unwrap_err()
            .contains("explicit public")
    );
    fs::write(&declaration_path, &bytes).unwrap();
    assert!(
        contract::check(fixture.path(), None)
            .unwrap_err()
            .contains("read reference/")
    );
    let lock_path = fixture.path().join("LOCK.json");
    let mut lock_bytes = fs::read(&lock_path).unwrap();
    lock_bytes.push(b'\n');
    fs::write(lock_path, lock_bytes).unwrap();
    assert!(
        contract::check_distribution(fixture.path())
            .unwrap_err()
            .contains("source lock mismatch")
    );
}

#[test]
fn public_provenance_must_match_the_immutable_source_lock() {
    let fixture = public_fixture();
    let path = fixture.path().join("PROVENANCE.json");
    let original: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let mut value = original.clone();
    value["sourceCommit"] = "0000000000000000000000000000000000000000".into();
    fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(
        contract::check_public(fixture.path())
            .unwrap_err()
            .contains("source identity")
    );
    let mut value = original.clone();
    value["files"]["api-manifest.json"]["sha256"] = "0".repeat(64).into();
    fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(
        contract::check_public(fixture.path())
            .unwrap_err()
            .contains("source mismatch")
    );
    let mut value = original.clone();
    value["files"] = serde_json::json!({});
    fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(
        contract::check_public(fixture.path())
            .unwrap_err()
            .contains("inventory mismatch")
    );
    let mut value = original.clone();
    value["files"]["api-manifest.json"]["operations"] = 80.into();
    fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(
        contract::check_public(fixture.path())
            .unwrap_err()
            .contains("metadata mismatch")
    );
    let mut value = original;
    value["files"]["api-manifest.json"]["unexpected"] = true.into();
    fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(
        contract::check_public(fixture.path())
            .unwrap_err()
            .contains("fields mismatch")
    );
}

#[test]
fn public_export_requires_the_full_internal_gate_and_a_new_target() {
    let source = source_contract();
    let distribution = contract::check_distribution(&source).unwrap();
    let parent = tempfile::tempdir().unwrap();
    let target = parent.path().join("contract");
    match distribution.scope.as_str() {
        "internal" => {
            assert_eq!(contract::check(&source, None).unwrap(), 39);
            assert_eq!(contract::export_public(&source, &target).unwrap(), 20);
            assert_eq!(fs::read_dir(&target).unwrap().count(), 23);
            assert_eq!(
                contract::check_distribution(&target).unwrap().scope,
                "public"
            );
            assert!(
                contract::export_public(&source, &target)
                    .unwrap_err()
                    .contains("create new export directory")
            );
            for path in contract::PUBLIC_FILES
                .into_iter()
                .chain(["LOCK.json", "PROVENANCE.json"])
            {
                assert_eq!(
                    fs::read(source.join(path)).unwrap(),
                    fs::read(target.join(path)).unwrap()
                );
            }
        }
        "public" => {
            assert!(
                contract::export_public(&source, &target)
                    .unwrap_err()
                    .contains("explicit internal distribution")
            );
            assert!(!target.exists());
        }
        other => panic!("unsupported distribution {other}"),
    }
    let public = public_fixture();
    let absent = parent.path().join("incomplete-source");
    assert!(contract::export_public(public.path(), &absent).is_err());
    assert!(!absent.exists());
    let declaration_path = public.path().join("DISTRIBUTION.json");
    let mut declaration: serde_json::Value =
        serde_json::from_slice(&fs::read(&declaration_path).unwrap()).unwrap();
    declaration["scope"] = "internal".into();
    declaration["contractFileCount"] = 39.into();
    fs::write(declaration_path, serde_json::to_vec(&declaration).unwrap()).unwrap();
    assert!(
        contract::export_public(public.path(), &absent)
            .unwrap_err()
            .contains("read reference/")
    );
    assert!(!absent.exists());
}

#[cfg(unix)]
#[test]
fn public_gate_rejects_symlink_assets_even_with_matching_bytes() {
    let fixture = public_fixture();
    let path = fixture.path().join("api-manifest.json");
    fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink(source_contract().join("api-manifest.json"), path).unwrap();
    assert!(
        contract::check_public(fixture.path())
            .unwrap_err()
            .contains("unauthorized")
    );
}
