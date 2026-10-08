# Tansr Rust SDK

[中文](README.md) · [Developer guide](doc/guide.md) · [0.1.0 API documentation](https://docs.rs/tansr-sdk/0.1.0/tansr_sdk/) · [Source](https://github.com/tansrai/tansr-rust)

An asynchronous native Rust client for local or remote Tansr Serve, using the unified `/api` contract. Rust applications need no Go, Node, or JavaScript client proxy. Serve owns the agent loop, sessions, context, memory selection, authorization, arbitration, tool orchestration, and usage accounting. The application owns its login, UI, explicitly installed business functions, and encrypted local archive.

The SDK, Demo, guides and 20 frozen contract JSON files distributed with the SDK are [MIT licensed](LICENSE). This guide uses the exact versions `tansr-sdk 0.1.0` and `tansr-sdk-demo 0.1.0` for crates.io integration, example commands and distribution scope.

## Integrate

Use Rust 1.85 or newer. Windows builds also need the MSVC C/C++ build tools and NASM available to the shell running Cargo; check NASM with `nasm -v`.

A new project does not inherit this repository's `rust-toolchain.toml` (1.95.0). Check `cargo --version` and `rustc --version`. If Windows PATH still selects an older default Cargo, install 1.95.0 and use `rustup run 1.95.0 cargo ...` for the Cargo commands below without changing the global default. See the [guide](doc/guide.md#3-sessions-and-terminal-interaction) for project toolchains and complete setup commands.

Add the exact version from crates.io:

```sh
cargo add tansr-sdk@=0.1.0
```

The equivalent dependency configuration is:

```toml
[dependencies]
tansr-sdk = "=0.1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Your application controls the Tokio runtime; the SDK does not create a global runtime. Communication uses HTTP and SSE, not WebSocket. The built-in transport follows no redirects and never silently replays an uncertain write. Local SDK development can use `tansr-sdk = { path = "../tansr-rust" }`; registry acceptance must remove local path/git/patch overrides.

Copy the complete [quickstart source at v0.1.0](https://github.com/tansrai/tansr-rust/blob/v0.1.0/demo/examples/quickstart.rs) into a new project's `src/main.rs`. It uses `sdk1` only and additionally needs Tokio's `signal` and `time` features plus `futures-util`. The [guide](doc/guide.md#3-sessions-and-terminal-interaction) provides dependency, download and run commands.

## Three examples

Set `TANSR_TOKEN_FILE` to a private file containing the current user's short-lived Serve token. Platform appkeys and model keys belong on the server. The default development origin is `http://127.0.0.1:8787`; use HTTPS for remote production access.

Install the matching Demo package and inspect its options:

```sh
cargo install tansr-sdk-demo --version 0.1.0 --locked
tansr-chat --help
tansr-tools --help
tansr-archive --help
```

Installed commands need no source workspace; the [PowerShell and Unix environment examples](doc/guide.md#2-serve-authentication-and-network) run `tansr-chat` directly. From the source workspace, use `cargo run --locked -p tansr-sdk-demo --bin tansr-chat -- --help`; replace the binary name with `tansr-tools` or `tansr-archive` for the other examples. The [Demo README](https://github.com/tansrai/tansr-rust/blob/main/demo/README.md) covers authentication, storage and recovery options.

| Command | Demonstrates |
|---|---|
| `tansr-chat` | Multi-turn SSE, manual permissions/questions, explicit interruption, same-turn input and resume |
| `tansr-tools` | An explicit synthetic order tool, binding and durable receipts; `--require-output` negotiates business stdout/stderr streaming |
| `tansr-archive` | Encrypted persistence, ACK after durability, original-request reconciliation, explicit stale ACK recovery and requested materials |

The examples call public Rust SDK APIs. The three commands explicitly select `sdk1` or `sdk2-offload-v1`, both using `/api`; quickstart uses `sdk1` only. Missing capabilities fail visibly; there is no family fallback or execution on the Serve host. See the [guide](doc/guide.md) for parameters and lifecycle rules.

## Scope

The first release targets the high-level Go v0.3.0 session, explicit `tool.invoke`, and single-device encrypted archive workflows. All 81 frozen operations remain available through the generic client; this does not mean every operation has a high-level workflow.

Business streaming requires Serve's ordinary `tool.invoke` output budget and per-operation authorization. `--require-output` negotiates `execution-stream-v1`; Runner then validates the original operation's output window before exposing `ToolContext.output`. An older Serve or missing capability rejects execution. The demo sends its first chunk, waits for a cancellable synthetic lookup, and sends its last chunk; Runner owns final seal confirmation. A definite business result and unconfirmed output remain separate: never rerun a function to repair its output. The [guide](doc/guide.md#2-serve-authentication-and-network) lists the required Serve capabilities.

SSE does not reconnect automatically; the host retains its processed cursor and explicitly resumes. Ctrl+C, timeouts and closing a local stream do not prove server cancellation: explicitly interrupt and confirm a terminal state. A human or host authorization policy handles approvals; the Demo never approves automatically.

Arbitrary shell/PTY execution, an OS filesystem sandbox/key vault, GUI/Tauri/WASM, complete local memory publication, replicas/cross-device sync, retention/backup policy, advanced cache orchestration, and Go/Node storage-format interoperability are outside this initial high-level scope. Electron keeps its existing integrated SDK/IPC mode.

## Development

```sh
cargo fmt --all --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo test --doc --workspace --all-features --locked
cargo run -p xtask --locked -- contract-check --public
cargo run -p xtask --locked -- generate --check
cargo doc --workspace --no-deps --all-features --locked
```

The frozen contract source is CLI `83c64b2c`: manifest r7 and 81 operations. Public `contract-check --public` verifies the 20 authorized contract JSON files against the distribution declaration. Internal `contract-check` verifies all 39 frozen assets, with separate archived provenance evidence. These modes are explicit; missing files never reduce the required verification scope.

Both packages are published in the [v0.1.0 release](https://github.com/tansrai/tansr-rust/releases/tag/v0.1.0), with successful [tag CI](https://github.com/tansrai/tansr-rust/actions/runs/37723399484) on three OSes. Independent registry consumption passed on Windows, Linux and macOS: two turns per session family, all three installed Demo help commands, and chat against actual Serve. These runs use synthetic authentication and model inputs; they do not establish paid-model, production-service or all tool/archive coverage. The [guide](doc/guide.md#7-acceptance-and-publication) separates CI and native consumption evidence. This document follows main; it does not replace the immutable 0.1.0 tag, crates or docs.rs pages.

## License and source boundary

MIT covers the authorized Rust SDK, Demo, guides and the 20 contract JSON files shipped in the SDK package. `contract/LOCK.json` and `contract/PROVENANCE.json` preserve provenance. The lock lists all 39 frozen assets; it does not claim that all of them are distributed publicly. Original contract bytes and SHA256 values remain unchanged.

Serve/kernel remains private and is deployed independently. Internal verification materials under `contract/reference/`, `contract/sdk2-archive-recovery-v1.sqlite.sql`, private Serve bundles, credentials and acceptance logs are excluded from public distribution; the root MIT license does not relicense those excluded materials. The public repository is an allowlisted source snapshot with independent Git history, without the internal repository's history or private refs.
