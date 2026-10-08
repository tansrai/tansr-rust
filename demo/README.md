# Tansr Rust Demo

通过公开 `tansr-sdk` API 接入 Serve 的三个原生命令与完整最小程序。智能体运行、上下文、记忆选择、权限与计量仍由 Serve 承担。本包 `0.1.0` 采用 [MIT 许可](LICENSE)，依赖固定 `tansr-sdk = "=0.1.0"`。

[源码](https://github.com/tansrai/tansr-rust) · [Demo 0.1.0 API 文档](https://docs.rs/tansr-sdk-demo/0.1.0/tansr_sdk_demo/) · [中文指南](https://github.com/tansrai/tansr-rust/blob/v0.1.0/doc/使用指南.md) · [English guide](https://github.com/tansrai/tansr-rust/blob/v0.1.0/doc/guide.md)

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

在完整工作区使用锁定工具链；SDK 异步能力需要应用自有 Tokio runtime。Windows 的当前 TLS 依赖需要 C/MSVC 编译环境。Serve 单独部署，不随 Demo 打包。

创建仅当前 OS 用户可读的短期令牌文件，将其绝对路径写入 `TANSR_TOKEN_FILE`；不要把令牌值、appkey 或模型密钥放进命令行。设置 `TANSR_BASE_URL` 为 Serve origin（例如 `https://serve.example.com`，不含 `/api`）。本机默认地址为 `http://127.0.0.1:8787`，远程连接使用 HTTPS。

```sh
cargo run --locked -p tansr-sdk-demo --bin tansr-chat -- --help
cargo run --locked -p tansr-sdk-demo --bin tansr-chat -- --message "介绍当前可用能力"
cargo run --locked -p tansr-sdk-demo --bin tansr-chat -- --resume SESSION_ID
cargo run --locked -p tansr-sdk-demo --example quickstart
```

`quickstart` 另需为本次运行保留唯一 `TANSR_REQUEST_ID`，并从该身份派生 create/send 两个稳定请求键。它遇到人工审批或问题会明确失败；使用交互 chat 接手原会话。两种会话族 `sdk1`、`sdk2-offload-v1` 均走 `/api`，通过 `--family` 或 `TANSR_SESSION_FAMILY` 明确选择；默认 `sdk1`。新建 offload 会话另需 `--request-id STABLE_ID`，切换族不等于完成 Source 存储配置。

交互 chat 支持 `/interrupt`、`/allow TICKET`、`/deny TICKET`、`/answers TICKET JSON_ARRAY`、`/insert JSON_OBJECT`、`/history`、`/quit`。只能回答真实收到且仍有效的票据，不自动批准。Ctrl+C、超时与 `/quit` 停止本地观察；它们不等于服务端任务取消或会话关闭。EOF、202 与旧回合结束事件不算当前回合完成。

## 工具与实时输出

当前身份还需设置 `TANSR_APPLICATION_SCOPE_ID`、`TANSR_END_USER_ID`、`TANSR_AUTHORIZATION_REVISION`。这些值必须来自宿主认证状态并与 Serve 匹配，声明本身不授予权限。为日志提供仅当前用户可访问的绝对目录：

```sh
cargo run --locked -p tansr-sdk-demo --bin tansr-tools -- --journal ABSOLUTE_PRIVATE_DIRECTORY --require-output
```

工具仅查询合成订单 `DEMO-001`。另开 chat 恢复 tools 打印的会话即可调用。未指定 `--require-output` 时仍有业务结果与执行回执；指定后须先协商 `execution-stream-v1`，再核对当前原操作输出窗与授权。首块在合成查询结束前发送，末块随后发送，Runner 确认最终 seal。输出未确认与业务结果分开，不能为补输出重跑业务函数。缺能力明确拒绝，不借用 Shell 权限，也不在 Serve 宿主执行。此 Demo 不安装 shell、文件访问、进程或 PTY 适配器。

## 档案与材料

Serve 宿主先配置 Source。为存储使用绝对私有路径，并将独立保存的 32 字节档案密钥编码为 64 位十六进制文本，通过私有文件 `TANSR_ARCHIVE_KEY_FILE` 提供；原密钥丢失时，新密钥不能解密旧档案。

```sh
cargo run --locked -p tansr-sdk-demo --bin tansr-archive -- --help
cargo run --locked -p tansr-sdk-demo --bin tansr-archive -- --mode prepare-create --session SESSION_ID --source SOURCE_ID --request-id STABLE_ID --intent NEW_PRIVATE_INTENT
cargo run --locked -p tansr-sdk-demo --bin tansr-archive -- --mode create --intent ORIGINAL_PRIVATE_INTENT
cargo run --locked -p tansr-sdk-demo --bin tansr-archive -- --mode sync --binding BINDING_ID --file ABSOLUTE_PRIVATE_ARCHIVE
```

`prepare-create` 仅耐久保存原请求。失回先用 `creation-status` 查询原 intent；不要换新身份重建。`recover` 使用稳定恢复请求号，仅对已确认的 stale If-Match 执行恢复；普通 412 不触发。材料使用 `materials`、`material-status`、`material-submit`，保存原响应 intent 与原截止，不重算 TTL。耐久落盘后才 ACK，收到材料不等于核心已消费；保留失败时的档案、意图与密钥。Rust 存储格式不承诺与其他语言 SDK 文件互通。

## 许可、文件边界与发行

本包采用 MIT 许可，白名单只包含 `src/`、`examples/`、本 README、`LICENSE`、Cargo 元数据与锁；测试工装、验收日志、私有 Serve bundle、凭据及 SDK/合同源码不重复装入 Demo。SDK 与双语指南属于独立 `tansr-sdk` 包。上方固定版本指南链接提供完整开发说明；解包本 Demo 不会附带工作区目录。Serve/kernel 和内部参考材料不属于本包的 MIT 授权范围。

发布顺序为先发布并验证 `tansr-sdk 0.1.0`，再发布依赖 `=0.1.0` 的本包。本地可用 `cargo install --path demo --locked`，但它使用工作区 SDK，不能替代 registry 独立消费验收。包文件审核、公开主线 CI、两包实际发布、三平台 registry 下载及 docs.rs 精确版本验证分别记录。

English: this MIT-licensed `0.1.0` package uses only the public Rust SDK. Install all three binaries with `cargo install tansr-sdk-demo --version 0.1.0 --locked` and run `tansr-chat --help`, `tansr-tools --help` or `tansr-archive --help`. Set a private `TANSR_TOKEN_FILE` and a Serve origin before connecting. The binaries demonstrate sessions, explicitly authorized synthetic tools, and durable archives. Preserve original identities and unknown outcomes; no automatic approval, shell execution, hidden replay or host fallback is installed. Serve/kernel and internal reference materials remain private. The release order is SDK first, then the Demo with its exact SDK dependency. Local builds are not registry acceptance.
