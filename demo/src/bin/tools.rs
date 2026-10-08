//! Explicit synthetic business execution. No shell/filesystem tool is installed.
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tansr_sdk::{
    api::{Error, Result},
    executor::{
        self, Authorizer, Binding, Client, FileJournal, Operation, PROTOCOL, Platform,
        Registration, Runner, RunnerOptions, Scope, TerminalSessionReference, Tool, ToolContext,
        ToolDefinition, ToolError, ToolHandler, Workspace,
    },
    session::{CreateOptions, SessionClient},
};
use tansr_sdk_demo::{
    Args, base_url, client, env_required, family, report, request_id, safe_text, write_options,
};
use tokio_util::sync::CancellationToken;

const TOOL: &str = "DemoOrderStatus";
const HELP: &str = "tansr-tools --journal ABSOLUTE_PRIVATE_DIRECTORY [--session ID] [--executor ID] [--base ORIGIN] [--family sdk1|sdk2-offload-v1] [--require-output]\nNew offload sessions additionally require --request-id STABLE_ID.\nCurrent authenticated scope: --application / TANSR_APPLICATION_SCOPE_ID; --user / TANSR_END_USER_ID; --authorization-revision / TANSR_AUTHORIZATION_REVISION.\nRequires TANSR_TOKEN_FILE and a policy permitting DemoOrderStatus. --require-output negotiates business output: stdout before the synthetic lookup, stderr after it, and final seal confirmation tracked separately from the durable business receipt. Missing per-operation output support rejects execution; it never borrows Shell permissions. No shell/filesystem access is installed.";

fn declaration() -> Value {
    json!({"name":TOOL,"description":"Read the status of sample order DEMO-001; this is demonstration data.","parameters":{"orderId":{"type":"string","description":"Sample order ID: DEMO-001"}},"readOnly":true})
}

struct OrderStatus;
#[async_trait]
impl ToolHandler for OrderStatus {
    async fn invoke(
        &self,
        context: ToolContext,
        args: Value,
    ) -> std::result::Result<Value, ToolError> {
        let order = args
            .get("orderId")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::Rejected("invalid_order_arguments".into()))?;
        if args.as_object().is_none_or(|object| object.len() != 1) {
            return Err(ToolError::Rejected("invalid_order_arguments".into()));
        }
        if context.cancellation.is_cancelled() {
            return Err(ToolError::Rejected("cancelled_before_query".into()));
        }
        if let Some(output) = &context.output {
            output
                .capture("stdout", b"order lookup started\n")
                .await
                .map_err(|_| ToolError::Unknown("output_capture_unknown".into()))?;
            // A bounded synthetic I/O delay makes the in-progress chunk visible.
            // Production handlers connect their actual work and cancellation here.
            tokio::select! {
                _ = context.cancellation.cancelled() => return Err(ToolError::Unknown("query_cancelled_after_output".into())),
                _ = tokio::time::sleep(Duration::from_millis(750)) => {}
            }
            output
                .capture("stderr", b"order lookup completed\n")
                .await
                .map_err(|_| ToolError::Unknown("output_capture_unknown".into()))?;
            // Runner owns the final seal and records output confirmation
            // separately from the durable business receipt. Capture is not ACK.
        }
        if order != "DEMO-001" {
            return Ok(
                json!({"status":"error","message":"Sample order not found / 演示订单不存在"}),
            );
        }
        Ok(
            json!({"status":"ok","content":[{"t":"text","text":"DEMO-001: awaiting shipment (sample data) / 待发货（演示数据）"}]}),
        )
    }
}

/// This explicit policy is for one synthetic session, not a production login
/// implementation. Applications must add current revocation/resource checks.
struct DemoAuthorization {
    scope: Scope,
    session: String,
    binding: Binding,
    cancellation: CancellationToken,
}
#[async_trait]
impl Authorizer for DemoAuthorization {
    async fn authorize(&self, operation: &Operation) -> Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if operation.scope != self.scope
            || operation.session_id != self.session
            || operation.binding != self.binding
            || operation.tool_name != TOOL
        {
            return Err(Error::Contract(
                "demo authorization rejected mismatched identity or binding".into(),
            ));
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    report("tansr-tools", run().await)
}

async fn run() -> Result<()> {
    let mut args = Args::parse()?;
    if args.help() {
        println!("{HELP}");
        return Ok(());
    }
    let require_output = args.take("--require-output").is_some();
    let base = base_url(&mut args);
    let family = family(&mut args)?;
    let scope = Scope {
        application_scope_id: env_required(
            &mut args,
            "--application",
            "TANSR_APPLICATION_SCOPE_ID",
        )?,
        end_user_id: env_required(&mut args, "--user", "TANSR_END_USER_ID")?,
        authorization_revision: env_required(
            &mut args,
            "--authorization-revision",
            "TANSR_AUTHORIZATION_REVISION",
        )?,
    };
    let journal_path = PathBuf::from(args.required("--journal")?);
    if !journal_path.is_absolute() {
        return Err(Error::InvalidInput(
            "journal must be an absolute private directory".into(),
        ));
    }
    let existing = args.take("--session");
    let create_request = if family == "sdk2-offload-v1" && existing.is_none() {
        Some(args.required("--request-id")?)
    } else {
        None
    };
    let executor_id = args.value("--executor", "rust-demo");
    args.finish()?;
    let cancellation = CancellationToken::new();
    let api = client(&base, &family)?;
    let executor = Client::new(api.clone(), scope.clone())?;
    let sessions = SessionClient::new(api.clone())?;
    let current = if let Some(id) = existing {
        sessions.attach(&id).await?
    } else {
        sessions
            .create(CreateOptions {
                request_id: create_request,
                client_tools: Some(vec![declaration()]),
                write: write_options(&cancellation),
                ..Default::default()
            })
            .await?
    };
    println!("session: {}", safe_text(current.id()));
    let digest = executor::definition_digest(&declaration())?;
    let workspace = Workspace {
        workspace_id: "rust-business".into(),
        revision: "1".into(),
    };
    let registration = Registration {
        protocol: PROTOCOL.into(),
        executor_id,
        platform: Platform::current(),
        workspaces: vec![workspace.clone()],
        operations: vec!["tool.invoke".into()],
        tools: vec![ToolDefinition {
            name: TOOL.into(),
            definition_digest: digest.clone(),
        }],
        interpreter: None,
    };
    let journal = Arc::new(FileJournal::open(journal_path)?);
    let connection = executor.register(&registration).await?;
    let closure = current.capabilities().await?;
    let initialized = executor
        .initialize(
            current.id(),
            &registration.platform,
            Some(vec![TOOL.into()]),
            &closure.closure_id,
        )
        .await?;
    let closure = current.capabilities().await?;
    let bound = executor
        .bind(
            current.id(),
            &connection,
            &workspace,
            &initialized.capability_revision,
            &closure.closure_id,
        )
        .await?;
    if !bound
        .effective_tools
        .iter()
        .any(|tool| tool.name == TOOL && tool.available)
    {
        return Err(Error::InvalidInput(
            "Serve did not enable the demo tool for this session".into(),
        ));
    }
    let binding = bound
        .binding
        .ok_or_else(|| Error::Contract("execution binding missing".into()))?;
    let terminal = if require_output {
        let request = request_id();
        println!("output binding request: {request}");
        Some(
            executor
                .negotiate_output(
                    &TerminalSessionReference {
                        session_contract: family.clone(),
                        session_id: current.id().into(),
                    },
                    &binding,
                    &request,
                )
                .await?,
        )
    } else {
        None
    };
    let runner = Runner::with_connection(
        RunnerOptions {
            client: executor,
            registration,
            journal,
            tools: BTreeMap::from([(TOOL.into(), Tool::new(digest, OrderStatus))]),
            authorize: Arc::new(DemoAuthorization {
                scope,
                session: current.id().into(),
                binding,
                cancellation: cancellation.clone(),
            }),
            poll_interval: Duration::from_millis(250),
            terminal,
            require_output,
            restricted_status: false,
        },
        connection,
    )?;
    println!(
        "ready: open a second terminal with tansr-chat --base {} --resume {}",
        safe_text(&base),
        safe_text(current.id())
    );
    println!(
        "Ask: 查询订单 DEMO-001。Output is explicitly negotiated when --require-output is set; each operation still needs Serve authorization. Ctrl+C stops this executor; it does not prove a Serve turn was interrupted."
    );
    let running = runner.run(cancellation.clone());
    tokio::pin!(running);
    tokio::select! {
        result = &mut running => result,
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(Error::from)?;
            cancellation.cancel();
            match tokio::time::timeout(Duration::from_secs(10), &mut running).await {
                Ok(Err(Error::Cancelled)) | Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(error),
                Err(_) => Err(Error::Unknown("executor shutdown timed out; retain journal for reconciliation".into())),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ordinary_business_execution_needs_no_unsupported_output_context() {
        let context = ToolContext {
            cancellation: CancellationToken::new(),
            output: None,
        };
        let result = OrderStatus
            .invoke(context, json!({"orderId":"DEMO-001"}))
            .await
            .unwrap();
        assert_eq!(result["status"], "ok");
    }

    #[tokio::test]
    async fn missing_order_is_a_business_result_not_unknown_execution() {
        let context = ToolContext {
            cancellation: CancellationToken::new(),
            output: None,
        };
        let result = OrderStatus
            .invoke(context, json!({"orderId":"DOES-NOT-EXIST"}))
            .await
            .unwrap();
        assert_eq!(result["status"], "error");
    }

    #[tokio::test]
    async fn cancelled_request_is_rejected_before_synthetic_lookup() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let result = OrderStatus
            .invoke(
                ToolContext {
                    cancellation,
                    output: None,
                },
                json!({"orderId":"DEMO-001"}),
            )
            .await;
        assert!(matches!(result, Err(ToolError::Rejected(_))));
    }
}
