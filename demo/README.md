# Tansr Rust Demo

通过公开 `tansr-sdk` API 接入 Serve 的三个原生命令与完整最小程序。智能体运行、上下文、记忆选择、权限与计量仍由 Serve 承担。本包 `0.1.0` 采用 [MIT 许可](LICENSE)，依赖固定 `tansr-sdk = "=0.1.0"`。

[源码](https://github.com/tansrai/tansr-rust) · [Demo 0.1.0 API 文档](https://docs.rs/tansr-sdk-demo/0.1.0/tansr_sdk_demo/) · [中文指南（main）](https://github.com/tansrai/tansr-rust/blob/main/doc/使用指南.md) · [English guide (main)](https://github.com/tansrai/tansr-rust/blob/main/doc/guide.md)

| 入口 | 用途 |
|---|---|
| [tansr-chat](src/bin/chat.rs) | 多轮会话、流式文本、人工审批/问答、中断、同轮插入与恢复 |
| [tansr-tools](src/bin/tools.rs) | 显式注册合成订单工具、耐久执行回执、协商后的实时输出 |
| [tansr-archive](src/bin/archive.rs) | 加密档案、耐久 ACK、恢复对账、按需材料提交 |
| [quickstart](examples/quickstart.rs) | 可复制到业务工程的完整非交互单轮程序 |
| [共享示例库](src/lib.rs) | 参数、短期令牌文件、退出处理及可编译文档示例 |

## 安装与运行

从 crates.io 安装固定版的三个命令：

```sh
cargo install tansr-sdk-demo --version 0.1.0 --locked
tansr-chat --help
tansr-tools --help
tansr-archive --help
```

下列 `cargo run` 示例用于源码工作区。安装后可直接用 `tansr-chat`、`tansr-tools`、`tansr-archive` 替换相应的 `cargo run --locked -p tansr-sdk-demo --bin NAME --` 前缀。

需要 Rust 1.85+；完整工作区固定 1.95.0，新工程不会继承它。若 Windows PATH 命中旧默认 Cargo，安装 1.95.0 后用 `rustup run 1.95.0 cargo ...`，项目工具链设置见指南。Windows 还需 MSVC C/C++ 构建环境与当前 shell 可找到的 NASM（`nasm -v`）。SDK 异步能力需要应用自有 Tokio runtime。Serve 单独部署，不随 Demo 打包。

创建仅当前 OS 用户可读的短期令牌文件，将其绝对路径写入 `TANSR_TOKEN_FILE`；不要把令牌值、appkey 或模型密钥放进命令行。设置 `TANSR_BASE_URL` 为 Serve origin（例如 `https://serve.example.com`，不含 `/api`）。本机默认地址为 `http://127.0.0.1:8787`，远程连接使用 HTTPS。

已安装 Demo 可直接运行，无需源码工作区。以下只传文件路径；先由宿主准备兼容 Serve 和不带 BOM 的私有令牌文件，并替换示例地址/路径。PowerShell：

```powershell
$env:TANSR_BASE_URL = 'https://serve.example.com'
$env:TANSR_TOKEN_FILE = 'C:\private\tansr\token.txt'
$env:TANSR_SESSION_FAMILY = 'sdk1'
tansr-chat --message '介绍当前可用能力'
```

Unix shell：

```sh
export TANSR_BASE_URL='https://serve.example.com'
export TANSR_TOKEN_FILE="$HOME/.config/tansr/token.txt"
export TANSR_SESSION_FAMILY='sdk1'
tansr-chat --message 'Briefly describe your available capabilities'
```

完整源码工作区的对应命令：

```sh
cargo run --locked -p tansr-sdk-demo --bin tansr-chat -- --help
cargo run --locked -p tansr-sdk-demo --bin tansr-chat -- --message "介绍当前可用能力"
cargo run --locked -p tansr-sdk-demo --bin tansr-chat -- --resume SESSION_ID
cargo run --locked -p tansr-sdk-demo --example quickstart
```

`quickstart` 不随 `cargo install` 安装；[完整 v0.1.0 源码](https://github.com/tansrai/tansr-rust/blob/v0.1.0/demo/examples/quickstart.rs)可复制到新工程 `src/main.rs`，依赖及运行步骤见指南。它固定使用 `sdk1`，不读 `TANSR_SESSION_FAMILY`，不接受 chat 参数，另需唯一 `TANSR_REQUEST_ID`（1–100 个 ASCII 字母、数字、`-` 或 `_`），由此派生 create/send 两个稳定请求键。token 只在启动时读取一次；三个命令则每次请求重读。遇审批、问题或工具请求会明确失败，使用交互 chat 接手原会话。

三个命令支持 `sdk1`、`sdk2-offload-v1`，均走 `/api`，通过 `--family` 或 `TANSR_SESSION_FAMILY` 选择；默认 `sdk1`。chat/tools 新建 offload 会话另需 `--request-id STABLE_ID`，不读取 quickstart 的 `TANSR_REQUEST_ID`；切换族不等于完成 Source 存储配置。未知结果需保留原身份，不能换号重跑。

交互 chat 支持 `/interrupt`、`/allow TICKET`、`/deny TICKET`、`/answers TICKET JSON_ARRAY`、`/insert JSON_OBJECT`、`/history`、`/quit`。只能回答真实收到且仍有效的票据，不自动批准。Ctrl+C、超时与 `/quit` 停止本地观察；它们不等于服务端任务取消或会话关闭。EOF、202 与旧回合结束事件不算当前回合完成。

SSE 不自动重连。`--message` 遇审批/问题失败后，用 `tansr-chat --family 原会话族 --resume SESSION_ID` 恢复，不带 `--message`。应用恢复事件流需保存已处理水位；档案 ACK、输出和材料水位不能代替它。

## 工具与实时输出

当前身份还需设置 `TANSR_APPLICATION_SCOPE_ID`、`TANSR_END_USER_ID`、`TANSR_AUTHORIZATION_REVISION`。这些值必须来自宿主认证状态并与 Serve 匹配，声明本身不授予权限。为日志提供仅当前用户可访问的绝对目录：

```sh
cargo run --locked -p tansr-sdk-demo --bin tansr-tools -- --journal ABSOLUTE_PRIVATE_DIRECTORY --require-output
```

工具仅查询合成订单 `DEMO-001`。另开 chat，以同一 `--family` 恢复 tools 打印的会话即可调用。未指定 `--require-output` 时仍有业务结果与执行回执；指定后须先协商 `execution-stream-v1`，再核对当前普通 `tool.invoke` 原操作输出窗、业务预算与授权。仅支持 Shell/process 输出的 Serve 不够，完整能力条件见指南。首块在合成查询结束前发送，末块随后发送，Runner 确认最终 seal。输出未确认与业务结果分开，不能为补输出重跑业务函数。缺能力明确拒绝，不借用 Shell 权限，也不在 Serve 宿主执行。此 Demo 不安装 shell、文件访问、进程或 PTY 适配器。

## 档案与材料

Serve 宿主先配置 Source。为存储使用绝对私有路径，并将独立保存的 32 字节档案密钥编码为 64 个十六进制字符，通过私有文件 `TANSR_ARCHIVE_KEY_FILE` 提供；原密钥丢失时，新密钥不能解密旧档案。

```sh
cargo run --locked -p tansr-sdk-demo --bin tansr-archive -- --help
cargo run --locked -p tansr-sdk-demo --bin tansr-archive -- --mode prepare-create --session SESSION_ID --source SOURCE_ID --request-id STABLE_ID --intent NEW_PRIVATE_INTENT
cargo run --locked -p tansr-sdk-demo --bin tansr-archive -- --mode create --intent ORIGINAL_PRIVATE_INTENT
cargo run --locked -p tansr-sdk-demo --bin tansr-archive -- --mode sync --binding BINDING_ID --file ABSOLUTE_PRIVATE_ARCHIVE
```

`prepare-create` 仅耐久保存原请求。失回先用 `creation-status` 查询原 intent；不要换新身份重建。`recover` 使用稳定恢复请求号，仅对已确认的 stale If-Match 执行恢复；普通 412 不触发。材料使用 `materials`、`material-status`、`material-submit`，保存原响应 intent 与原截止，不重算 TTL。耐久落盘后才 ACK，收到材料不等于核心已消费；保留失败时的档案、意图与密钥。Rust 存储格式不承诺与其他语言 SDK 文件互通。

Serve 管理会话上下文与记忆；本地档案加密保存，执行 journal 只受文件权限保护、不加密。SDK 不提供自动热/温/冷存储迁移、跨设备同步或备份保留策略。

## 许可、文件边界与发行

本包采用 MIT 许可，白名单只包含 `src/`、`examples/`、本 README、`LICENSE`、Cargo 元数据与锁；测试工装、验收日志、私有 Serve bundle、凭据及 SDK/合同源码不重复装入 Demo。SDK 与双语指南属于独立 `tansr-sdk` 包。上方 main 指南链接提供当前使用说明；解包本 Demo 不会附带工作区目录。Serve/kernel 和内部参考材料不属于本包的 MIT 授权范围。

[v0.1.0 正式 Release](https://github.com/tansrai/tansr-rust/releases/tag/v0.1.0)的两包已发布，[tag CI](https://github.com/tansrai/tansr-rust/actions/runs/37723399484)三 OS 通过。Windows/Linux/macOS 各自独立 registry 消费通过，覆盖两族各两轮、三个已安装命令的帮助与 chat 实际 Serve 运行；合成认证/模型和私有日志边界见指南第 7 节，不等同生产服务或所有工具/档案验收。本地 `cargo install --path demo --locked` 使用工作区 SDK，不能替代 registry 消费。这次 main 文档补充不覆盖 0.1.0 制品。

English: this published MIT-licensed `0.1.0` package uses only the public Rust SDK. Install all three binaries with `cargo install tansr-sdk-demo --version 0.1.0 --locked`. Use Rust 1.85+ and Windows MSVC/NASM; see the English guide for project toolchain selection if PATH selects old Cargo. The PowerShell/Unix blocks above pass only a private token file path and Serve origin. The standalone quickstart source uses `sdk1` only; the three binaries support both families. There is no automatic approval, SSE reconnection, shell execution or host fallback. Local cancellation does not prove server cancellation. Keep original request identities, processed cursors, archive keys and execution journals for recovery. Serve owns context/memory, the archive is local and encrypted, and the execution journal uses filesystem permissions without encryption; automatic storage tiering and cross-device sync are not supplied. The release, three-OS CI and limited registry acceptance are described in guide section 7; synthetic runs are not production acceptance. Serve/kernel and internal reference materials remain private.

PST-05 local candidate: `memory_publication` is a compiled attachment example for the reserved storage profile and encrypted publication plus execution journal. Run `cargo run -p tansr-sdk-demo --example memory_publication -- --help`; see the [English guide](../doc/guide.md) / [中文指南](../doc/使用指南.md). It requires an existing authorized binding and retains original recovery anchors. Not included in published 0.1.0.
`cargo run -p tansr-sdk-demo --example journal_copy -- --help` demonstrates offline explicit journal migration/key rotation using public `FileJournal::copy_to`. It requires an existing source, a nonexistent target and a fresh target key file. Source plaintext remains plaintext; no host configuration is switched. For publication hosts, stop all writers, copy and validate both publication and journal, and only then switch both paths/keys. The two copies are not one transaction. See the SDK guides for failure recovery and retained staging directories.

The publication attachment checks the original live Serve binding before opening media and rereads trusted host configuration during authorization. Recovery requires both original stores; `reopen` never creates a missing encrypted journal. Use Ctrl+C or `--stop-file ABSOLUTE_PATH` to drain accepted work. `--mode reopen --recover-operation ORIGINAL_OPERATION_ID` fetches the original Serve envelope, recovers/submits the original journal receipt and displays only operation/digest/transfer/status anchors. An unknown receipt stays unknown. The Windows dedicated Serve suite runs independent create/reopen and unknown-recovery demo processes; other platforms and a new release remain separate.

专用示例先核当前原绑定并持续复核受信配置；reopen 缺原 journal 不启空。Ctrl+C/停止文件排空在飞资源，`--recover-operation 原ID` 沿原 operation/digest 和回执查回，unknown 不改成新操作；双库迁移仍须停写、两份核验成功后一起显式切换。

`cargo run -p tansr-sdk-demo --example terminal_persistence -- --help` selects
only the explicitly installed TansrTerminalPersistenceV1 Host. It uses the same
trusted binding config, encrypted execution journal, original-operation
reconciliation and draining close as memory_publication, but a new storage
layout. It neither migrates nor silently upgrades the legacy medium. See the
SDK guide for quotas, original-key recovery and local retirement boundaries.
