use std::{path::PathBuf, process::Command};

pub fn run(args: Vec<String>) -> Result<(), String> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    match args.first().map(String::as_str) {
        Some("generate") => {
            if args.len() > 2 || args.get(1).is_some_and(|argument| argument != "--check") {
                return Err("usage: generate [--check]".into());
            }
            contract::check_distribution(&root.join("contract"))?;
            manifest::generate(&root, args.len() == 2)
        }
        Some("contract-check") => {
            if args.as_slice() == ["contract-check", "--distribution"] {
                return contract::check_distribution(&root.join("contract")).map(|distribution| {
                    println!("{} contract distribution verified: {} frozen asset files; 39 source lock metadata entries", distribution.scope, distribution.contract_file_count);
                });
            }
            if args.as_slice() == ["contract-check", "--public"] {
                let distribution = contract::check_distribution(&root.join("contract"))?;
                if distribution.scope != "public" {
                    return Err("--public requires an explicit public distribution".into());
                }
                println!("public contract distribution verified: 20 frozen asset files; 39 source lock metadata entries (19 private source files excluded)");
                return Ok(());
            }
            let source = match args.as_slice() {
                [_] => None,
                [_, flag, path] if flag == "--source" => Some(PathBuf::from(path)),
                _ => return Err("usage: contract-check [--source <CLI checkout> | --distribution | --public]".into()),
            };
            contract::check(&root.join("contract"), source.as_deref())
                .map(|count| println!("internal contract lock verified: {count} frozen files"))
        }
        Some("contract-export") => {
            let output = match args.as_slice() {
                [_, flag, path] if flag == "--output" => PathBuf::from(path),
                _ => return Err("usage: contract-export --output <new contract directory>".into()),
            };
            contract::export_public(&root.join("contract"), &output).map(|count| {
                println!("internal 39-file gate passed; exported public contract distribution: {count} assets + 3 metadata files to {}", output.display());
            })
        }
        Some("integration") => {
            if args.as_slice() != ["integration", "--require-serve"] {
                return Err("usage: integration --require-serve".into());
            }
            let fixture = std::env::var_os("TANSR_RUST_SERVE_FIXTURE")
                .ok_or("TANSR_RUST_SERVE_FIXTURE must name the real Serve fixture entry point")?;
            let fixture = PathBuf::from(fixture);
            if !fixture.is_file() {
                return Err("TANSR_RUST_SERVE_FIXTURE is not an existing file".into());
            }
            let fixture = fixture.canonicalize().map_err(|error| format!("resolve Serve fixture: {error}"))?;
            let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
            let status = Command::new(cargo)
                .current_dir(&root)
                .env("TANSR_RUST_SERVE_FIXTURE", fixture)
                .args(["test", "--workspace", "--all-features", "--locked", "--no-fail-fast", "--test", "serve_integration", "--test", "serve_executor", "--test", "serve_archive", "--test", "serve_demos", "--", "--ignored", "--nocapture", "--test-threads=1"])
                .status()
                .map_err(|error| format!("start required Serve integration: {error}"))?;
            if !status.success() {
                return Err(format!("required Serve integration failed: {status}"));
            }
            Ok(())
        }
        _ => Err("usage: cargo run -p xtask -- generate [--check] | contract-check [--source <CLI checkout> | --distribution | --public] | contract-export --output <new contract directory> | integration --require-serve".into()),
    }
}

pub mod contract;
pub mod manifest;
