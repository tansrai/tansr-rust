# Rust SDK developer guide

Applies to the [MIT-licensed](../LICENSE) `tansr-sdk 0.1.0` and `tansr-sdk-demo 0.1.0`. This guide covers exact-version registry installation, a new-project quickstart and source development. Commands containing `-p tansr-sdk-demo` require the full source workspace. [中文指南](使用指南.md) · [0.1.0 API documentation](https://docs.rs/tansr-sdk/0.1.0/tansr_sdk/).

The SDK crate includes both READMEs and these two guides. Complete runnable examples belong to the separate `tansr-sdk-demo` package; paths beginning with `demo/` below are relative to the full source workspace. Its own README and example sources ship in that package, not in the SDK crate.

## 1. Integration and responsibilities

The SDK is an asynchronous Rust 2024 library requiring Rust 1.85 or newer; use the pinned toolchain for source development. Windows builds require MSVC C/C++ build tools and NASM for the current TLS provider. Both must be available to the shell running Cargo; check NASM with `nasm -v`. Consult `rust-toolchain.toml`, `Cargo.lock`, and actual platform evidence; an edition minimum alone does not prove the complete dependency graph's MSRV.

Check `cargo --version` and `rustc --version` first. New projects do not inherit this repository's pinned 1.95.0 toolchain. A project-root `rust-toolchain.toml` can contain `[toolchain]` followed by `channel = "1.95.0"` on the next line. If Windows still selects an old standalone Cargo executable, that file cannot switch its version: install with `rustup toolchain install 1.95.0 --profile minimal`, then use `rustup run 1.95.0 cargo ...` without changing the global default. The new-project steps below explicitly select this toolchain.

Add the exact SDK version from crates.io:

```sh
cargo add tansr-sdk@=0.1.0
```

```toml
[dependencies]
tansr-sdk = "=0.1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal", "time"] }
futures-util = "0.3"
```

Local SDK development can use `tansr-sdk = { path = "../tansr-rust" }`. Registry acceptance must not retain a local `path`, `git`, or `[patch]` override. Applications deploy Serve independently and use their login service to issue short-lived user tokens.

Install the exact Demo version separately:

```sh
cargo install tansr-sdk-demo --version 0.1.0 --locked
tansr-chat --help
tansr-tools --help
tansr-archive --help
```

Installed binaries take the same arguments as the source commands in this guide: replace `cargo run -p tansr-sdk-demo --bin NAME --` with `NAME`. The `quickstart` example is source code shipped with the Demo package and is not an installed binary.

| Module | Responsibility |
|---|---|
| `api` | All 81 operations, discovery/fencing, authentication, request headers, HTTP/SSE, unified errors |
| `session` | Create/attach/resume, turns, approval/questions, same-turn input, history/snapshots/compaction, speech requests |
| `executor` | Explicit handlers, execution binding, durable journal and leases; negotiated business output with per-operation eligibility |
| `archive` | Archive verification, encrypted storage, durable ACK, explicit recovery and requested materials |
| `canonical` / `sse` | Frozen encoding/digests and streaming rules, normally used through the high-level modules |

Serve/kernel retains context composition, memory selection, permission arbitration, model execution and accounting. A local archive is not a complete local memory coordinator. Platform prompts and session settings follow the existing Serve contract; the Rust client does not introduce a competing prompt hierarchy.

## 2. Serve, authentication and network

Use an origin such as `https://serve.example.com`, without `/api`, a query string, user information or passwords. Use HTTPS remotely. Reverse proxies must preserve unified contract headers and stream SSE promptly. The client refuses redirects and untrusted certificates; never disable TLS verification to work around configuration.

Compatibility depends on live Serve discovery and current authorization, not just a version number or a reachable `/api`:

| Feature | Required Serve support |
|---|---|
| Generic API | Frozen manifest r7 / 81 operations with matching schemaHash and unified response headers; the SDK validates contract fences |
| Sessions | `session.capabilities` discovery for `sdk2-ext-v1` advertises the selected `sdk1` / `legacy-complete` or `sdk2-offload-v1` / `source-required` pair; steering, speech and other features require their own capabilities |
| Business tools | `tool.invoke` with the matching definition digest, executor registration/initialization, initial device and workspace binding, and current scope, authorization and lease |
| Business output | Accepted `execution-stream-v1` under `terminal-services-v1`, plus a budget, output window and current per-operation permission for the original ordinary `tool.invoke`; Shell/process-only output is insufficient |
| Archives and materials | A host-provisioned Source, `archive.capabilities` and a binding accepting `archive-transfer-v1`, the `split-receipts-v1` ACK format and `source-ack-with-durable-spool` durability; material supply also requires `context-materials-v1` |

Serve/kernel is independently deployed; the host supplies deployment and login entry points. This package has no Serve startup command. Missing capabilities fail explicitly without switching families, weakening durability or broadening permissions.

The examples require `TANSR_TOKEN_FILE`. The three commands reread at most 8 KiB of single-line ASCII token data for each request; quickstart reads it once at startup. Use no UTF-8 BOM and protect the file for the current OS user. Renewal must preserve the same application and end user. Never embed platform appkeys, model keys or token values in CLI arguments, logs, Git or packages. Token renewal does not change authentication on an existing SSE connection. Stop old streams, runners and store access before switching users, then create new SDK instances.

Use `ClientBuilder::token_provider` for your host's token manager. It does not log in automatically or replay a write after a 401. The returned future participates in request cancellation; the host should coordinate concurrent refreshes. `token` can provide a short-lived fixed value when its lifetime is explicitly controlled.

| Environment variable / option | Purpose |
|---|---|
| `TANSR_TOKEN_FILE` | Required private short-lived user token file |
| `TANSR_BASE_URL` / `--base` | Serve origin; option wins, default `http://127.0.0.1:8787` |
| `TANSR_SESSION_FAMILY` / `--family` | Explicit `sdk1` or `sdk2-offload-v1`, default `sdk1` |
| `TANSR_APPLICATION_SCOPE_ID` | Current authenticated application for tools |
| `TANSR_END_USER_ID` | Current authenticated end user for tools |
| `TANSR_AUTHORIZATION_REVISION` | Current authorization generation for tools |
| `TANSR_ARCHIVE_KEY_FILE` | Private file path; its contents are a 32-byte archive key encoded as exactly 64 hexadecimal characters |

The tool scope values must come from authenticated host state and match the token and Serve policy. Declaring them grants no authority. Keep the archive key separately; generating a replacement cannot decrypt an existing file.

For an installed Demo, have the host provision compatible Serve and a private token file first. Replace the origin and file paths below; never put secret values in the command. PowerShell:

```powershell
$env:TANSR_BASE_URL = 'https://serve.example.com'
$env:TANSR_TOKEN_FILE = 'C:\private\tansr\token.txt'
$env:TANSR_SESSION_FAMILY = 'sdk1'
tansr-chat --message 'Briefly describe your available capabilities'
```

Unix shell:

```sh
export TANSR_BASE_URL='https://serve.example.com'
export TANSR_TOKEN_FILE="$HOME/.config/tansr/token.txt"
export TANSR_SESSION_FAMILY='sdk1'
tansr-chat --message 'Briefly describe your available capabilities'
```

Archive modes `sync`, `recover` and `materials` additionally use `$env:TANSR_ARCHIVE_KEY_FILE = 'C:\private\tansr\archive-key.txt'` in PowerShell or `export TANSR_ARCHIVE_KEY_FILE="$HOME/.config/tansr/archive-key.txt"` on Unix, pointing to an existing private file. Journal, archive and intent paths are supplied through command options, not automatically read from path environment variables.

## 3. Sessions and terminal interaction

Use the complete [quickstart at v0.1.0](https://github.com/tansrai/tansr-rust/blob/v0.1.0/demo/examples/quickstart.rs). Create a business crate with registry dependencies:

```sh
rustup toolchain install 1.95.0 --profile minimal
rustup run 1.95.0 cargo new --bin --edition 2024 tansr-quickstart
cd tansr-quickstart
rustup run 1.95.0 cargo add 'tansr-sdk@=0.1.0'
rustup run 1.95.0 cargo add 'futures-util@0.3'
rustup run 1.95.0 cargo add 'tokio@1' --features 'macros,rt-multi-thread,signal,time'
```

Save the complete source as `src/main.rs`, retaining `TANSR_BASE_URL` and `TANSR_TOKEN_FILE` from the preceding section. PowerShell:

```powershell
Invoke-WebRequest -Uri 'https://raw.githubusercontent.com/tansrai/tansr-rust/v0.1.0/demo/examples/quickstart.rs' -OutFile 'src/main.rs'
$env:TANSR_REQUEST_ID = 'quickstart-20261008-001'
rustup run 1.95.0 cargo run --locked
```

Unix shell:

```sh
curl --fail --location 'https://raw.githubusercontent.com/tansrai/tansr-rust/v0.1.0/demo/examples/quickstart.rs' -o src/main.rs
export TANSR_REQUEST_ID='quickstart-20261008-001'
rustup run 1.95.0 cargo run --locked
```

Set and retain a unique `TANSR_REQUEST_ID` for every new run; do not repeatedly use the sample value. It must contain 1–100 ASCII letters, digits, `-` or `_`. The program derives stable `-create` and `-send` request keys; reconcile an uncertain write rather than replacing its identity. It uses only `sdk1` and a built-in English prompt, ignores `TANSR_SESSION_FAMILY`, and takes no chat arguments. Permission, question or tool requests fail explicitly. Each write has a 30-second deadline; total observation is limited to 300 seconds. If saved as `examples/quickstart.rs`, run `rustup run 1.95.0 cargo run --locked --example quickstart` instead.

Only the full source workspace uses `cargo run --locked -p tansr-sdk-demo --example quickstart`; a new business crate does not use `-p tansr-sdk-demo`. The full interactive program is [chat.rs](https://github.com/tansrai/tansr-rust/blob/v0.1.0/demo/src/bin/chat.rs), with shared wiring in [lib.rs](https://github.com/tansrai/tansr-rust/blob/v0.1.0/demo/src/lib.rs). Installed commands run directly:

```sh
tansr-chat --family sdk1 --message "Briefly describe your available capabilities"
tansr-chat --family sdk1 --resume SESSION_ID
```

The demo creates without an initial prompt, opens SSE, then sends. Resume retains the same session; failure does not create a replacement. Sessions remain available after exit. Ctrl+C, the observation timeout and `/quit` stop local observation only; they neither prove Serve cancellation nor implicitly close a session.

`--message` is a noninteractive single-turn mode. A permission/question event is displayed and fails explicitly; resume the same session with its original `--family`, without `--message`, to answer it. Interactive mode never approves automatically. Chat's observation timeout defaults to 600 seconds; `--timeout` accepts 1–86400 seconds.

| Interaction | Meaning |
|---|---|
| Plain text | Starts a turn while idle, never silently truncates an active turn |
| `/interrupt` | Explicitly asks Serve to interrupt, then waits for the terminal event |
| `/allow TICKET` / `/deny TICKET` | Answers an actually observed open ticket with its original digest; never automatic |
| `/answers TICKET JSON_ARRAY` | Answers an observed question, e.g. `[{"questionId":"q1","selectedOptionIds":[],"freeText":"My answer"}]` |
| `/insert JSON_OBJECT` | Targets the existing input ID/history epoch/turn ID; acceptance is not core consumption |
| `/history` | Count-only query with `limit=0` |
| `/quit` | Stops local observation; an active turn can remain unknown |

Use real current identifiers rather than copying these placeholders:

```json
{"inputId":"host-generated-stable-id","target":{"historyEpoch":"CURRENT_EPOCH","turnId":"CURRENT_TURN"},"content":{"text":"Additional instruction: prioritize the first item"},"ack":"durable"}
```

Unavailable durable ACK fails rather than downgrading to memory. Neither SDK event streams nor chat reconnect automatically; the host explicitly reopens observation. Resume from the application's processed event cursor, not a parsed/delivered cursor or an archive/output/material watermark. EOF, HTTP 202, an old turn completion, or session termination is not proof that the current turn completed. New turns match their observed `turn.started`. Running-turn resume prefers the authoritative live input-capabilities target; a custom factory without steering can instead reconstruct the active identity from complete ordered replay, without enabling steering. Only the current identity's terminal event after the previous metadata watermark can complete the observed turn. If the turn ended during these reads, metadata is reconciled instead of waiting forever for an old start event. A replay gap or unresolved running identity fails explicitly rather than guessing success.

Both session families use `/api`. Offload creation and material Source provisioning have additional lifecycle requirements; changing the family is not Source provisioning. The public `Session` methods expose checkpoints, restore, compaction, transcription and speech requests. Speech APIs do not include recording/playback or invent native audio/video message blocks.

A new offload session in chat/tools additionally requires `--request-id STABLE_ID`; it does not read quickstart's `TANSR_REQUEST_ID`. Retain it and query the original create request after uncertainty. Do not provide this option when resuming an existing session.

## 4. Explicit business tools and output

See `demo/src/bin/tools.rs`. The only installed tool is the synthetic read-only `DemoOrderStatus`, with fake order `DEMO-001`. It does not install arbitrary shell/filesystem access or fall back to execution on Serve's host.

```sh
cargo run -p tansr-sdk-demo --bin tansr-tools -- --journal ABSOLUTE_PRIVATE_DIRECTORY
```

The program declares the tool, registers its executor, initializes capabilities, and binds a logical workspace. It prints ready only after Serve enables the business tool. In another terminal, use `tansr-chat --family ORIGINAL_FAMILY --resume SESSION_ID` with the printed session and ask for order DEMO-001. An existing `--session` must declare the identical tool/digest. Compatible Serve must actually support initial device binding and current authorization.

By default the demo does not require output chunks; a handler still returns its business result and durable execution receipt. To explicitly require remote output:

```sh
cargo run -p tansr-sdk-demo --bin tansr-tools -- --journal ABSOLUTE_PRIVATE_DIRECTORY --require-output
```

The demo calls `Client::negotiate_output`, then passes the returned `TerminalOptions` and `require_output: true` to `RunnerOptions`. Runner validates the actual operation's output window, scope and binding before exposing `ToolContext.output`. Older Serve implementations that only admit process/Shell output still reject ordinary business output. An accepted generic feature never bypasses this per-operation check. Do not add Shell permissions or disguise the business tool. Section 2 lists the deployment requirements; missing prerequisites fail before the handler executes.

The order handler captures its first stdout chunk, waits 750ms for a cancellable synthetic lookup, and captures a final stderr chunk. Capture puts original bytes into the bounded asynchronous queue; Runner fixes the final seal and waits for its ACK after the handler returns. Business results and output confirmation are distinct: an unconfirmed output does not erase a definite, durable business receipt or authorize rerunning the function. Ordinary `println!` does not feed this stream; business code explicitly uses the context writer.

Use `Runner::execute_with_output` and its `ExecutionOutcome { receipt, output }` to handle the two outcomes separately. The convenience `execute` and polling `run` report an unconfirmed output as `Error::OutputIncomplete`; `run` submits the definite business receipt first. The demo prints a fixed redacted message: retain the journal, reconcile the original receipt, and never rerun the handler for this error.

The writer preserves captured stdout/stderr byte order. A full queue truncates the continuous prefix while draining the source. Hosts must not emit unfiltered credentials. Abort stops output upload, not necessarily the tool. The demo supplies no process.exec/Shell/PTY adapter; real business streaming acceptance is not real shell acceptance.

| Result | Meaning |
|---|---|
| Valid `status:ok` or business `status:error` | A definite business result, eligible for an execution completed receipt |
| `ToolError::Rejected` | The host can prove no side effect occurred |
| `ToolError::Unknown`, panic, invalid result or uncertain cancellation | Unknown outcome; preserve the journal and original operation |

The host still checks current login, revocation and resource access. The demo's authorizer only pins one short-lived synthetic session, scope, binding and tool; production hosts must add real authorization. `readOnly` does not prove order ownership or prevent hidden writes. Restricted executor tickets cannot perform controller binding steps. A controller must negotiate first; the restricted status route must never elevate or fall back after failure.

The private execution journal is not an encrypted general-purpose store. Preserve it for crash/lost-response reconciliation. Handlers should cooperate with cancellation; Rust cannot safely kill arbitrary uncooperative host threads. The initial output queue does not promise lossless cross-process resumption: unconfirmed output after restart remains unknown.

## 5. Archives and requested materials

Storage responsibilities are separate. Serve owns authoritative session state, context and memory selection. `FileStore` holds one device's encrypted archive and recovery state; the executor journal is an execution record protected by filesystem permissions, without encryption. The SDK does not supply hot/warm/cold memory migration, automatic cache tiering, cross-device sync or backup/retention policy. The host supplies key management and any additional storage policy.

See `demo/src/bin/archive.rs`. `ArchiveClient` reads Source/binding/status, and `FileStore` preserves ownership, exact bodies/attachments, pending ACK and confirmed coverage. ACK follows durable atomic persistence; parsing or an ordinary write return is not sufficient evidence for releasing server-side archives.

Prepare a new binding as an immutable private intent before sending:

```sh
cargo run -p tansr-sdk-demo --bin tansr-archive -- --mode prepare-create --session SESSION_ID --source SOURCE_ID --request-id STABLE_ID --intent ABSOLUTE_PRIVATE_INTENT
cargo run -p tansr-sdk-demo --bin tansr-archive -- --mode create --intent ABSOLUTE_PRIVATE_INTENT
cargo run -p tansr-sdk-demo --bin tansr-archive -- --mode creation-status --intent ABSOLUTE_PRIVATE_INTENT
```

`prepare-create` saves the complete original request ID, operation epoch and body and does not create the binding. Preserve the file on uncertainty. Query the original operation instead of choosing a new request. Source storage/provider setup itself remains a Serve host configuration responsibility; this CLI does not invent a new Source registration endpoint.

Use the original binding, private absolute archive path and key:

```sh
cargo run -p tansr-sdk-demo --bin tansr-archive -- --binding BINDING_ID --file ABSOLUTE_PRIVATE_FILE --mode sync
cargo run -p tansr-sdk-demo --bin tansr-archive -- --binding BINDING_ID --file ABSOLUTE_PRIVATE_FILE --mode recover --request-id STABLE_RECOVERY_ID
```

Sync reconciles an original pending ACK but does not silently create a stale-rebase intent. Explicit recovery only rebases after a confirmed stale If-Match condition; ordinary 412, busy, revocation and network uncertainty do not qualify. Recovering one ACK does not finish synchronization; run sync again. Never delete pending state to bypass a failure.

For material handoff, subscribe to a live request and persist its immutable response before final submission:

```sh
cargo run -p tansr-sdk-demo --bin tansr-archive -- --mode materials --binding BINDING_ID --file ABSOLUTE_PRIVATE_FILE --request-id STABLE_RESPONSE_ID --intent NEW_PRIVATE_RESPONSE_FILE
cargo run -p tansr-sdk-demo --bin tansr-archive -- --mode material-status --intent NEW_PRIVATE_RESPONSE_FILE
```

`material-submit` uses the same saved response after reconciliation when resubmission is appropriate. At request receipt, the demo fixes an absolute `SystemTime` deadline and passes it to `prepare_materials_before` after binding lookup and storage checks. Waiting and retries never restart `remainingTtlMs`. An application persisting a material request must also retain its original deadline; the older `prepare_materials` convenience entry is for freshly received requests only. Only the exact requested verified records and attachments are uploaded. A received/202 response is not core-consumed, archive deletion permission or proof that the model used the content. No full-history upload masks a broken material path.

The built-in store has its own Rust format; it does not promise Go/Node file interoperability. Corruption, a wrong key, exhausted limits, links/reparse points, concurrent writers and revocation fail while retaining the original file. Unix modes, Windows ACLs and atomic replace/sync require native evidence. Process-crash verification is not a physical power-loss SLA.

## 6. Errors and shutdown

`api::Error` distinguishes input, contract, transport, server, cancellation, storage and unknown outcomes. Unified codes/retry actions are distinct from domain details. Do not log tokens, arbitrary response text, arguments or archive contents. Demo text sanitization affects terminal display only, never digest-committed bytes.

`ApiError` has a redacted `Display`, while its derived `Debug` retains the original `message` and `detail` for controlled host diagnostics. Do not print `Debug` directly into user-facing logs. Tests of `Display` redaction do not establish `Debug` redaction.

Preserve the original key, body, precondition and deadline for writes. Replay only when explicitly allowed by the contract; query an uncertain original identity first. Never replace the key or extend a deadline to retry an unknown side effect. HTTP-level hidden retries are disabled.

`WriteOptions.deadline` covers capability discovery and closure preconditions as well as the final write. After an uncertain interrupt, retain the same session and inspect its metadata, trusted live input target, and events from the processed cursor; use `TurnTracker::resume` for the original turn. Do not create a replacement session or invent a request-query API. `input_status` uses the original input ID, history epoch and turn ID: a terminal turn may still return an existing identical receipt, while new inputs are rejected. Checkpoint restore changes the history generation.

The complete quickstart and three command sources compile with `--all-targets`. The shared example library `demo/src/lib.rs` also contains `no_run` doctests for handler output and deadline-preserving material preparation; `cargo test --doc -p tansr-sdk-demo` compiles them without contacting Serve. Compilation is separate from actual Demo acceptance.

Dropping a stream/future releases local resources, not a Serve business state. Explicitly close streams, stop and await the Runner, and release file locks on normal exit. Drop never secretly sends a durable ACK. The SDK starts no global Tokio runtime. The terminal input reader belongs to the short-lived demo process, not SDK background work.

## 7. Acceptance and publication

The [v0.1.0 release](https://github.com/tansrai/tansr-rust/releases/tag/v0.1.0) corresponds to public commit `ae6da0416211202a33a6ef0bb86f7c059bde8675`. [SDK 0.1.0](https://crates.io/crates/tansr-sdk/0.1.0) and [Demo 0.1.0](https://crates.io/crates/tansr-sdk-demo/0.1.0) are published, with downloaded checksums matching the audited packages. Both [SDK docs](https://docs.rs/tansr-sdk/0.1.0/tansr_sdk/) and [Demo docs](https://docs.rs/tansr-sdk-demo/0.1.0/tansr_sdk_demo/) use the exact version. Main-branch guides may improve without replacing the immutable tag, crates or docs.rs content.

Public [main CI](https://github.com/tansrai/tansr-rust/actions/runs/37721335509) and [v0.1.0 tag CI](https://github.com/tansrai/tansr-rust/actions/runs/37723399484) passed on three OSes. They establish only the listed static/controlled checks, not real Serve or registry consumption. Separate Windows/Linux/macOS registry acceptance started from independent empty Cargo caches and targets, used no path/git/patch overrides, and verified both package checksums. Each passed two completed turns per family, all three installed Demo help commands, and chat against actual Serve. Authentication and model responses were synthetic; no paid model or production service was called. This consumption scope does not establish every tool/archive path, physical power-loss durability or a production SLA. Full native logs remain in an internal archive because they include private Serve tooling; public readers need no access to it to install, use or inspect the public release and CI.

MIT authorization covers the Rust SDK, Demo, guides and the 20 contract JSON files distributed with the SDK. `LOCK.json` and `PROVENANCE.json` retain their source evidence. The public snapshot excludes internal `contract/reference/`, the recovery SQL, private Serve code/bundles, credentials, logs and the internal Git history. The license does not extend to those excluded materials. Frozen bytes and hashes remain unchanged. Public `contract-check --public` verifies the declared 20-file distribution; internal `contract-check` continues to require all 39 frozen assets.

No automatic publish job is configured. Future releases still need separate checks of package contents, CI for the exact commit, registry consumption and exact-version documentation. Dry-run is not publication; cross-compilation is not native execution.
