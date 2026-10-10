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

## PST-05: dedicated terminal memory storage (local candidate)

`memory_publication::{MemoryPublicationStore, FileStore, Host}` stores opaque UTF-8 publication bytes produced by Serve. It contains no extraction, recall, deletion-policy or model logic. Archive and its ACK contract remain unchanged. The frozen head/read/begin/chunk/commit/query profile retains its 4 MiB body, 12 KiB chunks, SHA256 and execution receipt semantics.

Open `StoreOptions` with explicit `OpenMode::Create` or `Reopen`, an absolute private path, original `Identity`, a host-provided 32-byte key, and `read_context`. That callback reads the currently authenticated full `Owner` (scope, session and binding, including authorization/connection revisions) before and after each transaction; restored identifiers do not grant access. `Host::new(store, authorizer)` requires durable encrypted storage and checks the original execution operation through the host authorizer. Register `Host::into_tool()` under `TOOL_NAME` and `TOOL_DIGEST` in the existing Runner. This reserved profile is not a model tool and never borrows Shell permission.

AES-256-GCM protects the publication, staging, owner, request and permanent transfer witnesses in one atomic snapshot. AAD binds the format and complete storage identity. Private files, ACLs, locking, sync and atomic replacement reuse the Archive OS boundary. Keys stay in memory; supply them through the host's OS key facility and reject revoked access in the live callback. Use `FileJournal::open_encrypted(path, key)` as well: execution receipts include read-result bodies. Runner refuses a publication Host with a journal that does not declare encryption. Legacy `FileJournal::open` remains available for plaintext business journals; formats cannot silently mix. Application logs, model caches and other independently created copies are outside this encryption boundary.

Mutation success follows durable storage. Commit publishes the body and terminal witness atomically. After a lost response reopen the original path and query the original transfer ID; unknown never creates a replacement. A different owner cannot query old transfers unless the explicit `authorize_recovery` callback establishes revocation of the old authority and current access. Recovery grants query only. `capacity()` reports transfer/staging headroom; the default is 4096 permanent transfers and 8 MiB staging. Exhaustion fails without age-evicting completed IDs. Each mutation rewrites a bounded snapshot; use a custom provider for large domains. A 64 MiB serialization cap and strict JSON node limit also reject before commit. No physical power-loss SLA is claimed.

Stop Runner and await `store.close()` to drain disk work and release the lock. Wrong keys, corruption, changed identity and uncertain commits preserve original files and recovery anchors. `copy_to(new_path, new_key)` copies a consistent snapshot, including staging and terminal facts, into a fresh encrypted target. Quiesce all writers before validating and explicitly adopting the target; the source remains. `FileJournal::copy_to(new_directory, new_key)` now supplies explicit journal migration/key rotation; see the procedure below. Both encrypted copy APIs reject the source key: provide a fresh, previously unused target key for each store.

The compiled attachment example runs through public SDK interfaces:

```sh
cargo run -p tansr-sdk-demo --example memory_publication -- --help
cargo run -p tansr-sdk-demo --example memory_publication -- --config HOST_CONFIG.json --store ABSOLUTE_PRIVATE_FILE --journal ABSOLUTE_PRIVATE_JOURNAL --mode reopen
```

The trusted host config contains `identity`, `owner` and an already registered live `connection` using their public Rust types. The controller first authorizes this fixed profile through existing Serve registration/binding APIs. The example reads `TANSR_TOKEN_FILE` and `TANSR_ARCHIVE_KEY_FILE`; it creates neither a session nor a memory domain. Before opening either store, the example checks the original authenticated Serve binding. Its bounded policy rereads the trusted config on each authorization check and rejects changes; production must integrate current authentication/revocation. Config strings alone are not authorization. Use create only initially: an existing journal path is rejected. Reopen uses `FileJournal::reopen_encrypted`, which requires the original directory, lock and encryption marker, validates the key, and never initializes missing media. The legacy open APIs keep their existing behavior.

Ctrl+C or `--stop-file ABSOLUTE_PATH` cancels the Runner, awaits its signal task and drains accepted storage work before releasing ownership. A stop file must initially be absent. An already issued disk operation may outlive cancellation; its original unknown/receipt must be reconciled, never treated as permission to start a new transfer.

For a known uncertain operation, use the same config, paths and keys with `--mode reopen --recover-operation ORIGINAL_OPERATION_ID`. The example fetches the original envelope from Serve, checks current authority, and lets Runner recover the exact operation/digest journal entry and submit the same receipt. A lost submit response is reconciled against that original operation. It prints only operationId, digest, transferId (when present) and status; `unknown` stays unknown even if the publication's original transfer query proves committed. Keep the original ID in host recovery state. Do not invent a new operation, connection, domain or transfer to bypass recovery; an expired/revoked original authority requires explicit host recovery authorization, not automatic rebinding.

Windows dedicated publication tests consume the sealed Serve/SDK/API packages and exercise this compiled example in independent create/reopen and original-unknown recovery processes. This candidate is not part of published 0.1.0; Linux/macOS native, new package-consumer and release evidence remain pending.
### Explicit journal migration and coordinated key rotation

`FileJournal::copy_to` is a blocking, local operation. Open the existing source with its explicit plaintext/encrypted mode, then call `copy_to` with an absolute, nonexistent target directory and a new host-provided key. It copies the original hashed operation filenames and raw authenticated claim/receipt contents. Completed and `unknown` receipts remain permanent; pending claims remain pending. A damaged plaintext fact stays damaged inside encryption. Unreadable encrypted fact bytes are retained inside an encrypted invalid-UTF-8 envelope under the original filename, so they remain permanently unknown, including after another rotation. A receipt missing its claim also returns unknown without creating a replacement claim. Migration does not change scope, operation digest, owner, connection fences or authorization. Restored records never grant a new owner permission.

Copy holds the same cross-process lock as current SDK claim/complete calls and rechecks the source inventory. Unrecognized entries, links, format/key mismatches and limits reject the whole copy. The current limits are 100,000 entries including the format marker and 262,272 stored bytes per entry; wrapping a damaged envelope must fit the same cap. Rejection never skips or age-evicts a fact. Larger or unsupported journals need a host migration provider preserving the same semantics. Plaintext-to-encrypted copying leaves the original plaintext source intact; neither this API nor publication encryption encrypts or erases that source, backups, logs or other independent copies.

All target facts are encrypted and synced in a private staging directory before one OS atomic no-replace directory rename. Existing targets, even empty ones, are rejected. Unsupported no-replace filesystems fail without a clobbering fallback. A process exit before publication leaves no target; `.journal-copy-*` staging directories may remain and must not be adopted. Errors during/after publication are uncertain: retain both paths and keys and reconcile the intended target by reopening it. Never initialize an empty replacement or infer plaintext from a decryption error. The tool does not delete source files or abandoned staging directories and makes no physical power-loss guarantee.

For a publication host, use this sequence:

1. Stop and drain every Runner/writer, including older SDKs and external processes. Retain the original operation/transfer identities and both source keys. Keep the publication source open until its copy completes; close it after validation and before restart.
2. Copy the publication with `FileStore::copy_to(new_file, fresh_publication_key).await`, and copy execution facts with `FileJournal::copy_to(new_directory, fresh_journal_key)`. There is no transaction across the two stores. If either fails, keep the old host configuration and preserve both sources and any completed target. Do not restart against one new store and one empty journal.
3. Reopen and validate both targets using the new keys and original identity/owner/fences, including original transfer queries and original operation receipts/pending/unknown outcomes. Stop writers throughout this validation. The host must still authorize the current operation and recovery queries.
4. Only after both copies and validation succeed, explicitly switch the host's two paths and keys together, retire old writable handles, then resume. The host owns durable configuration switching and the disposition of retained plaintext sources/backups. Copy APIs cannot prove that an externally supplied key was never used elsewhere; generate and track fresh independent keys in the host key facility.

The offline public example accepts key-file paths, never key values or Serve credentials:

```sh
cargo run -p tansr-sdk-demo --example journal_copy -- --help
cargo run -p tansr-sdk-demo --example journal_copy -- --source ABSOLUTE_OLD_JOURNAL --source-format plaintext --target ABSOLUTE_NEW_JOURNAL --target-key-file NEW_KEY_FILE
cargo run -p tansr-sdk-demo --example journal_copy -- --source ABSOLUTE_OLD_JOURNAL --source-format encrypted --source-key-file OLD_KEY_FILE --target ABSOLUTE_NEW_JOURNAL --target-key-file NEW_KEY_FILE
```

Key files use the existing 64-hex-digit demo format. Production supplies keys from the host's protected key facility. This example only publishes the journal target; it never switches host configuration. The source must already exist and be nonempty. Opening an older plaintext source can add the existing format/lock metadata, but original fact bytes are preserved. The migration remains a local candidate; Windows local tests do not establish Linux/macOS native execution or a release. Dedicated publication Serve evidence is recorded separately from migration fault tests.


### Dedicated publication integration input

Build `cargo build -p tansr-sdk-demo --example memory_publication --locked` and set `TANSR_RUST_MEMORY_DEMO` to that absolute executable path. Set `TANSR_RUST_PUBLICATION_FIXTURE` to the verified packaged publication host. It follows the existing fixture argv/ready/control contract; no private Serve source is included in this repository. The focused command is `cargo test --test serve_memory_publication --locked -- --ignored --nocapture --test-threads=1`. Without explicit inputs these tests remain ignored; an ignored test is not acceptance evidence.

The complete `xtask integration --require-serve` also requires the original `TANSR_RUST_SERVE_FIXTURE` and runs the original targets plus the dedicated publication target. Original Archive success alone cannot sign this publication chain. The new target covers original identity/receipts, permanent unknown and exact-key cold recovery, wrong owner/scope and live revocation before claim, cancellation with accepted disk work, and the compiled public example. Fault injection surrounds real storage or an accepted operation; Serve dispatch, HTTP, authorization, schema and receipts remain real. This does not establish physical power-loss durability.

## Terminal persistence V1 (PST-05, explicit opt-in)

`terminal_persistence::{FileStore, Host}` implements the separately approved
`TansrTerminalPersistenceV1` profile. It stores opaque body blocks and receipt
keys/values; Serve alone decides what to archive. Register `TOOL_NAME` with
`TOOL_DIGEST` and use the existing `MemoryPublication` execution permission and
an **encrypted** `FileJournal`. No new capability HTTP field, feature, route,
lease or business handler is introduced. The legacy profile remains separate.

Use `cargo run -p tansr-sdk-demo --example terminal_persistence -- --help` for the
real host example. Its trusted config, original connection, stop/reopen and
`--recover-operation ORIGINAL_OPERATION_ID` inputs match the legacy example.
A new V1 installation uses a new metadata filename and its own encrypted
journal. Reopen requires both original media; missing or legacy files are
rejected. Select one writable profile per source in trusted host configuration;
this SDK does not fence an unrelated legacy path on the host's behalf.
V1 create is a fresh layout, not legacy migration or cutover. Before any
cross-profile switch the trusted host must stop old writers, reconcile their
unknown operations and keep the original read-only historical query entry.
Without that proof no switch is authorized. V1 same-layout copy/key rotation is
available through the public API below; legacy publication/journal copy tools
remain independent.

`terminal_persistence::FileStore::copy_to(new_file, fresh_key).await` holds the
existing exclusive writer lane and current host authorization. It preserves the
current Root, both indexes, every original ticket/result, pending plans and
accepted material, and retirement witnesses without renumbering facts or
interpreting their business contents. Use a different encryption key. The target
metadata file must be inside a **nonexistent new directory** under an existing
private parent. Complete encrypted objects and metadata are synced in private
staging before one atomic no-replace directory publication. The original files
and key are untouched; the receipt hashes identify a copy, not a cutover grant.

The target stores an authenticated read-only marker and returns
`CopyReceipt { read_only: true, cutover: Pending, ... }`. Ordinary reopen permits
only head/read/lookup/query. Begin/put/commit, including previous successful
requests, and compact remain denied. Copying it again preserves read-only status.
There is no activation override and no automatic retirement of the source. This
version supplies copy/validation only: **do not attach the copy as a writable
Runner**. Cutover awaits a real shared writer fence. On an error during or after
publication, retain source, target, staging and both keys and reconcile the exact
paths and identities; never overwrite or initialize an empty replacement.

For a closed source, use `FileStore::copy_from(reopen_options, new_file, fresh_key).await`.
It locks and authenticates the original without normal reopen repairs: no orphan
adoption or pending-file rename/delete is written to the source. Unrecorded
objects reject explicitly. Instance copy_to starts its source-preservation
boundary at the already-open Store; ordinary open(Reopen) keeps its recovery
behavior. The existing Demo uses this non-repairing copy_from entry offline:

```sh
cargo run -p tansr-sdk-demo --example terminal_persistence -- --config TRUSTED_HOST_JSON --store ORIGINAL_METADATA_FILE --mode reopen --copy-to NEW_DIRECTORY/METADATA_FILE --copy-key-file FRESH_KEY_FILE
```

The config needs the original identity and owner. Supply the source key through
`TANSR_ARCHIVE_KEY_FILE` and a fresh key through --copy-key-file, both 64-digit hex
files. Omit --journal/--stop-file/--recover-operation. This mode does not contact
Serve, load a login token or start an executor. The separate execution journal is
**not included**: retain its original facts/keys. Legacy journal_copy does not
create a transaction with V1 storage. Wrong key/identity/layout, existing target,
authority loss and damage reject explicitly. Encrypted staging may remain and is
neither adopted nor silently deleted. This is not a backup-retention policy,
legacy conversion, device synchronization or physical power-loss guarantee.

The host supplies an absolute metadata filename in a private directory, a key,
full `Identity`, current `Owner` callback and optional query-only recovery
callback. Call `FileStore::close().await` after cancelling/draining Runner.
Current scope/session/binding is checked across storage work. Unknown outcomes
retain the original execution operation and transfer ID/intent; a known storage
commit never permits replaying an unknown execution. Reconciliation under a new
binding grants only query, not put/commit permission.

Object files hold independently authenticated immutable blocks/pages/values;
AEAD metadata atomically publishes the Root, both indexes and complete permanent
transfer result. CAS uses `commitRoot`, not the body SHA. Metadata and execution
receipts are encrypted too. Values are raw opaque bytes. Each object is verified
before use; close/reopen audits indexes, roots, accepted objects and originals.
The source/key/record identity is in AAD. An integrity failure preserves bytes
and never initializes an empty replacement or falls back to plaintext.

This initial file layout defaults to **4,096 receipt entries, 1,024 permanent
transfer facts, 8 active transfers, 8,192 objects, 32 MiB staging and 128 MiB
logical retained+reserved bytes**; options may lower these, and head reports the
actual limits. Body limit remains 4 MiB, objects at most 12,288 bytes. Metadata
has a separate 16 MiB bound and reserves finalization space. Atomic replacement
needs space for old and new encrypted metadata plus a temporary object and the
separate execution journal. These are logical/admission bounds, not a physical
full-disk or power-loss SLA. Metadata is rewritten and audited as a whole;
B-tree maps are in-memory point indexes, **not an O(1) or million-entry claim**.

Begin reserves object/index slots and raw/finalization bytes. No permanent key
or receipt is TTL-evicted. Changed body pages/blocks can reuse the protected base
root; unchanged receipt entries must match both keys and value exactly. Commit
streams all body blocks and validates all physical material before one metadata
publication. Read/lookup require the current exact root. Historical query
returns its original Root after later commits; it does not pin historical body
bytes forever.

Local `compact()` and normal commit reclaim only objects unreferenced by the
current root, every staging base/accepted object and the permanent value index,
under the same exclusive owner lock. A durable retirement witness precedes each
physical removal; a crash preserves charged witnesses for retry. Authenticated
objects with no recorded provenance are charged and retained until the original
plan receives the same verified input. Accepted object progress and its synced
encrypted temporary location share one metadata commit; reopen authenticates
and completes only that recorded rename without charging its last slot twice.
Unreferenced temporary files left before that commit are not adopted or blindly
deleted. Their disk usage remains an explicit host maintenance concern. There is no remote delete/list
or backup API. Do not treat copying these files while active as a supported
backup, or a second JSON rename as a common transaction.

The new schema/golden are separately vendored under
`src/terminal_persistence/assets` with source hashes. The original 39 locked
assets and 20-file public legacy distribution are unchanged. Local tests and
packages do not imply a released crate or new Serve/macOS/Linux execution.
