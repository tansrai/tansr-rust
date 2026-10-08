# Tansr Rust SDK

[English](README.en.md) · [开发指南](doc/使用指南.md) · [0.1.0 API 文档](https://docs.rs/tansr-sdk/0.1.0/tansr_sdk/) · [源码](https://github.com/tansrai/tansr-rust)

原生异步 Rust SDK，通过统一 `/api` 连接本地或远程 Tansr Serve。客户端无需 Go、Node 或 JavaScript 代理；Serve 独立提供智能体循环、会话、上下文、记忆选材、权限裁决、工具调度和用量管理。Rust 应用管理自己的登录、界面、显式业务工具和本地加密档案。

SDK、Demo、使用指南及随 SDK 分发的 20 份冻结合同 JSON 采用 [MIT 许可](LICENSE)。本文使用固定版本 `tansr-sdk 0.1.0` 和 `tansr-sdk-demo 0.1.0`，介绍 crates.io 接入、示例运行和分发范围。

## 集成

需要 Rust 1.85 或更新版本。Windows 构建还需 MSVC C/C++ 构建工具及 NASM；运行 Cargo 的当前 shell 必须能找到这些工具，可先用 `nasm -v` 检查 NASM。

从 crates.io 添加固定版本：

```sh
cargo add tansr-sdk@=0.1.0
```

对应依赖配置：

```toml
[dependencies]
tansr-sdk = "=0.1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

用户应用控制 Tokio runtime，SDK 不创建全局 runtime。HTTP 请求与 SSE 事件共用安全传输；不使用 WebSocket、不自动重放未知结果的写请求、不跟随重定向。开发本仓时可用 `tansr-sdk = { path = "../tansr-rust" }`，公开消费验收必须移除本地 path/git/patch 替换。

## 三个示例

设置 `TANSR_TOKEN_FILE` 为仅包含当前用户短期 Serve token 的文件；不要把平台 appkey 或模型密钥放到客户端。默认源地址为 `http://127.0.0.1:8787`，远程生产环境使用 HTTPS。

安装同版 Demo 并查看参数：

```sh
cargo install tansr-sdk-demo --version 0.1.0 --locked
tansr-chat --help
tansr-tools --help
tansr-archive --help
```

源码工作区可用 `cargo run --locked -p tansr-sdk-demo --bin tansr-chat -- --help`；将二进制名换为 `tansr-tools` 或 `tansr-archive` 可运行其他示例。[Demo 包说明](https://github.com/tansrai/tansr-rust/blob/main/demo/README.md)列出认证、存储和恢复参数。

| 命令 | 演示 |
|---|---|
| `tansr-chat` | 多轮 SSE、人工审批与问答、显式中断、同轮插入及续接 |
| `tansr-tools` | 显式合成订单工具、绑定和耐久执行回执；`--require-output` 协商业务输出并传输 stdout/stderr 分块 |
| `tansr-archive` | 加密档案、落盘后 ACK、原请求恢复、显式 stale ACK 恢复及按需材料 |

所有示例只通过公开 Rust SDK 接口接线。会话族显式选择 `sdk1` 或 `sdk2-offload-v1`，均走统一 `/api`。能力未启用时明确失败，不换族或回退 Serve 宿主。完整参数、认证、权限与恢复规则见[开发指南](doc/使用指南.md)。

## 首版边界

首版以 Go v0.3.0 的高层会话、显式 `tool.invoke`、单设备加密档案为对照。全部 81 个冻结操作提供通用入口，不等于每个操作都已有高层工作流。

业务输出需要 Serve 装配本批新增的普通 `tool.invoke` 输出预算与逐操作授权。`--require-output` 先协商 `execution-stream-v1`，Runner 再核对原操作输出窗，才将 `ToolContext.output` 交给 handler；旧 Serve 或能力缺席时明确拒绝执行。示例发送首块、等待一段可取消的合成查询、发送末块，Runner 负责最终 seal 确认。确定业务结果与输出未确认分别呈现，不能为补输出重跑业务函数。此次接线的完整验收状态见 源工作区内部台账 `doc/开发进度.md`（不随 SDK crate 打包），不以实现完成替代三平台实跑。

未承诺任意 shell/PTY、OS 文件沙箱或密钥库、GUI/Tauri/WASM、完整本地记忆发布、多副本/跨设备同步、备份/保留策略、高级缓存编排或 Go/Node 介质互读。Electron 继续使用原集成 SDK/IPC 模式。

## 开发与验收

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

冻结源 CLI `83c64b2c`，manifest r7 / 81 操作。公开 `contract-check --public` 按分发声明核对 20 份授权合同 JSON；内部完整 `contract-check` 核验全部 39 份冻结资产，来源证据单独归档。两种入口显式区分，缺少文件不能自动降低核验范围。工具运行还需包含首次绑定修复的 Serve（实现基线 `3a5ba4f8`）。本地静态门、实际 Serve、Windows/Linux/Mac 原生消费及公开发行分别记证据，不相互代替。仅主线或发行 tag 触发 GitHub 工作流，当前未配置自动发布。发行顺序为先 SDK、再依赖 `=0.1.0` 的 Demo。

## 许可与源码边界

MIT 覆盖本次授权的 Rust SDK、Demo、使用指南及 SDK 包内 20 份合同 JSON。`contract/LOCK.json` 与 `contract/PROVENANCE.json` 保留来源记录；锁记录全部 39 份冻结资产，不表示公开包包含全部文件。合同原字节和 SHA256 保持不变。

Serve/kernel 实现继续私有且独立部署。内部核验使用的 `contract/reference/`、`contract/sdk2-archive-recovery-v1.sqlite.sql`、私有 Serve bundle、凭据和验收日志不属于公开发行内容，根 MIT 许可不扩展到这些排除材料。公开仓从授权文件白名单生成独立快照，不能携带原内部仓的 Git 历史或私有 refs。
