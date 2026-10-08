use super::{
    client::{validate_operation, validate_receipt, validate_registration},
    types::*,
    *,
};
use crate::api::{Error, Result};
use futures_util::FutureExt;
use serde_json::json;
use std::{
    collections::BTreeMap,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct TerminalOptions {
    pub session_contract: String,
    pub limits: OutputLimits,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutputOutcome {
    Sealed(OutputStatus),
    Incomplete { last_known: Option<OutputStatus> },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionOutcome {
    pub receipt: Receipt,
    pub output: Option<OutputOutcome>,
}
impl ExecutionOutcome {
    pub fn output_confirmed(&self) -> bool {
        !matches!(self.output, Some(OutputOutcome::Incomplete { .. }))
    }
    fn into_receipt(self) -> Result<Receipt> {
        if !self.output_confirmed() {
            return Err(Error::OutputIncomplete {
                operation_id: self.receipt.operation_id.clone(),
                receipt_id: self.receipt.operation_id,
            });
        }
        Ok(self.receipt)
    }
}
pub struct RunnerOptions {
    pub client: Client,
    pub registration: Registration,
    pub journal: Arc<dyn Journal>,
    pub tools: BTreeMap<String, Tool>,
    pub authorize: Arc<dyn Authorizer>,
    pub poll_interval: Duration,
    /// Set only after the controller has negotiated execution-stream-v1 for the
    /// relevant session and validated scope/binding. Does not grant authority.
    pub terminal: Option<TerminalOptions>,
    /// Require a negotiated, empty output window for each new operation. The
    /// generic binding feature never substitutes for per-operation authority.
    pub require_output: bool,
    /// Uses terminal.execution.state exclusively; never elevates restricted tickets.
    pub restricted_status: bool,
}
pub struct Runner {
    options: RunnerOptions,
    connection: Mutex<Option<Connection>>,
    connect_gate: Mutex<()>,
    execute_gate: Mutex<()>,
    running: AtomicBool,
}
struct RunGuard<'a>(&'a AtomicBool);
impl Drop for RunGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
impl Runner {
    /// Reuse a connection returned by this client's explicit registration, allowing
    /// the controller to bind/negotiate output before starting the runner.
    pub fn with_connection(options: RunnerOptions, connection: Connection) -> Result<Self> {
        live(&connection)?;
        if connection.executor_id != options.registration.executor_id {
            return Err(invalid("preconnected executor identity"));
        }
        let mut runner = Self::new(options)?;
        runner.connection = Mutex::new(Some(connection));
        Ok(runner)
    }
    pub fn new(options: RunnerOptions) -> Result<Self> {
        if options.require_output && options.terminal.is_none() {
            return Err(invalid(
                "required output needs a negotiated terminal binding",
            ));
        }
        if let Some(terminal) = &options.terminal {
            crate::api::validate_wire("terminal-services-v1", "Limits", &value(&terminal.limits)?)?;
        }
        validate_registration(&options.registration)?;
        let p = Platform::current();
        if options.registration.platform.platform != p.platform
            || options.registration.platform.arch != p.arch
        {
            return Err(invalid("registration platform does not describe this host"));
        }
        if options.registration.operations != ["tool.invoke"]
            || options.tools.is_empty()
            || options.tools.len() != options.registration.tools.len()
            || options.registration.interpreter.is_some()
        {
            return Err(invalid("runner supports explicit business tools only"));
        }
        for t in &options.registration.tools {
            if options
                .tools
                .get(&t.name)
                .is_none_or(|v| v.definition_digest != t.definition_digest)
            {
                return Err(invalid("registered handler digest mismatch"));
            }
        }
        if options.poll_interval < Duration::from_millis(10)
            || options.poll_interval > Duration::from_secs(60)
        {
            return Err(invalid("poll interval"));
        }
        if options.restricted_status && options.terminal.is_none() {
            return Err(invalid(
                "restricted execution requires negotiated terminal session",
            ));
        }
        Ok(Self {
            options,
            connection: Mutex::new(None),
            connect_gate: Mutex::new(()),
            execute_gate: Mutex::new(()),
            running: AtomicBool::new(false),
        })
    }
    pub async fn connection(&self) -> Option<Connection> {
        self.connection.lock().await.clone()
    }
    pub async fn connect(&self) -> Result<Connection> {
        let _gate = self.connect_gate.lock().await;
        if let Some(c) = self.connection().await {
            live(&c)?;
            return Ok(c);
        }
        let c = self
            .options
            .client
            .register(&self.options.registration)
            .await?;
        *self.connection.lock().await = Some(c.clone());
        Ok(c)
    }
    async fn current(&self) -> Result<Connection> {
        let c = self
            .connection()
            .await
            .ok_or_else(|| invalid("executor not connected"))?;
        live(&c)?;
        Ok(c)
    }
    async fn check(&self, op: &Operation, cancel: &CancellationToken) -> Result<()> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let before = self.current().await?;
        self.options.client.check_identity(&before, op)?;
        let t = &op.binding.target;
        if !self
            .options
            .registration
            .workspaces
            .iter()
            .any(|w| w.workspace_id == t.workspace_id && w.revision == t.workspace_revision)
        {
            return Err(invalid("workspace not registered"));
        }
        if expiry(&op.expires_at)? <= SystemTime::now() {
            return Err(invalid("operation expired"));
        }
        let remaining = expiry(&op.expires_at)?
            .min(expiry(&before.expires_at)?)
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);
        tokio::select! {
            _=cancel.cancelled()=>return Err(Error::Cancelled),
            r=tokio::time::timeout(remaining,self.options.authorize.authorize(op))=>r.map_err(|_|invalid("authorization exceeded lease"))??,
        };
        let after = self.current().await?;
        if before.executor_id != after.executor_id
            || before.connection_id != after.connection_id
            || before.connection_revision != after.connection_revision
        {
            return Err(invalid("connection changed during authorization"));
        }
        Ok(())
    }
    async fn read_status(&self, op: &Operation) -> Result<Status> {
        let connection = self.current().await?;
        let remaining = expiry(&op.expires_at)?
            .min(expiry(&connection.expires_at)?)
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);
        let s = tokio::time::timeout(remaining, async {
            if self.options.restricted_status {
                let t = self
                    .options
                    .terminal
                    .as_ref()
                    .ok_or_else(|| invalid("terminal missing"))?;
                self.options
                    .client
                    .executor_status(
                        &TerminalSessionReference {
                            session_contract: t.session_contract.clone(),
                            session_id: op.session_id.clone(),
                        },
                        &connection,
                        op,
                    )
                    .await
            } else {
                self.options
                    .client
                    .status(&op.session_id, &op.operation_id)
                    .await
            }
        })
        .await
        .map_err(|_| invalid("status exceeded lease"))??;
        if &s.operation != op {
            return Err(invalid("substituted execution status"));
        }
        Ok(s)
    }
    async fn renew(&self) -> Result<()> {
        let before = self.current().await?;
        let remaining = expiry(&before.expires_at)?
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);
        let after = tokio::time::timeout(remaining, self.options.client.heartbeat(&before))
            .await
            .map_err(|_| invalid("heartbeat exceeded lease"))??;
        if after.connection_revision != before.connection_revision {
            return Err(invalid("connection generation changed"));
        }
        *self.connection.lock().await = Some(after);
        Ok(())
    }
    /// Maintains the lease and dispatches serially. Empty polls never mean completion.
    /// The caller controls the run token; this method leaves no detached runner task.
    pub async fn run(&self, cancel: CancellationToken) -> Result<()> {
        if self.running.swap(true, Ordering::AcqRel) {
            return Err(invalid("runner already running"));
        }
        let _running = RunGuard(&self.running);
        tokio::select! { _=cancel.cancelled()=>return Err(Error::Cancelled), connection=self.connect()=>{connection?;} }
        let mut last_heartbeat = tokio::time::Instant::now();
        loop {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let c = self.current().await?;
            if last_heartbeat.elapsed() >= Duration::from_millis(c.heartbeat_after_ms) {
                tokio::select! { _=cancel.cancelled()=>return Err(Error::Cancelled), renewed=self.renew()=>renewed? }
                last_heartbeat = tokio::time::Instant::now();
            }
            let current = self.current().await?;
            let batch = tokio::select! {_=cancel.cancelled()=>return Err(Error::Cancelled),b=self.options.client.poll(&current)=>b?};
            for operation in batch.operations {
                let outcome = self
                    .execute_inner(operation.clone(), cancel.clone(), true)
                    .await?;
                let receipt = &outcome.receipt;
                if let Err(original) = self.options.client.submit(&operation, receipt).await {
                    let s = self.read_status(&operation).await;
                    match s {
                        Ok(s) if s.receipt.as_ref() == Some(receipt) => {}
                        _ => return Err(original),
                    }
                }
                outcome.into_receipt()?;
            }
            tokio::select! {_=cancel.cancelled()=>return Err(Error::Cancelled),_=tokio::time::sleep(self.options.poll_interval)=>{}}
        }
    }
    /// Direct execution does not renew the connection and cannot outlive its lease.
    /// The durable receipt is returned but is not submitted by this method.
    pub async fn execute(
        &self,
        operation: Operation,
        cancel: CancellationToken,
    ) -> Result<Receipt> {
        self.execute_with_output(operation, cancel)
            .await?
            .into_receipt()
    }
    /// Returns the durable business fact independently of output confirmation.
    /// An incomplete stream never authorizes executing the handler again.
    pub async fn execute_with_output(
        &self,
        operation: Operation,
        cancel: CancellationToken,
    ) -> Result<ExecutionOutcome> {
        self.execute_inner(operation, cancel, false).await
    }
    async fn execute_inner(
        &self,
        op: Operation,
        cancel: CancellationToken,
        monitored: bool,
    ) -> Result<ExecutionOutcome> {
        let _gate = tokio::select! {
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            guard = self.execute_gate.lock() => guard,
        };
        validate_operation(&op)?;
        if op.request.operation != "tool.invoke" {
            return Err(invalid("unsupported resource operation"));
        }
        let tool = self
            .options
            .tools
            .get(&op.tool_name)
            .ok_or_else(|| invalid("tool not installed"))?;
        if op.request.args["definitionDigest"] != tool.definition_digest {
            return Err(invalid("tool definition substituted"));
        }
        self.check(&op, &cancel).await?;
        // A digest and a host Authorizer do not prove that Serve dispatched this
        // operation. Direct execute has the same credential boundary as run.
        let remote = tokio::select! {
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            status = self.read_status(&op) => status?,
        };
        if remote.status != "pending" && remote.receipt.is_none() {
            return Err(invalid(
                "remote execution is not pending and has no durable receipt",
            ));
        }
        let output_status = if let Some(terminal) = &self.options.terminal {
            let status = super::output::query_status(
                self.options.client.api(),
                &TerminalSessionReference {
                    session_contract: terminal.session_contract.clone(),
                    session_id: op.session_id.clone(),
                },
                &OutputOperationReference {
                    operation_id: op.operation_id.clone(),
                    request_digest: op.digest.clone(),
                },
                terminal.limits.max_control_bytes,
                &cancel,
            )
            .await?;
            if self.options.require_output
                && remote.status == "pending"
                && matches!(status.state.as_str(), "unavailable" | "gap")
            {
                return Err(invalid(
                    "required operation output window is not empty and available",
                ));
            }
            Some(status)
        } else {
            None
        };
        let output_ready = output_status
            .as_ref()
            .is_some_and(|s| s.state == "available");
        let mut output_outcome = output_status.as_ref().map(|s| {
            if matches!(s.state.as_str(), "complete" | "truncated") {
                OutputOutcome::Sealed(s.clone())
            } else {
                OutputOutcome::Incomplete {
                    last_known: Some(s.clone()),
                }
            }
        });
        match self.options.journal.claim(&op).await? {
            ClaimResult::Receipt(r) => {
                validate_receipt(&op, &r)?;
                if remote
                    .receipt
                    .as_ref()
                    .is_some_and(|known| known != r.as_ref())
                {
                    return Err(invalid("local and remote durable receipts disagree"));
                }
                return Ok(ExecutionOutcome {
                    receipt: *r,
                    output: output_outcome,
                });
            }
            ClaimResult::Pending => {
                let r = remote.receipt.clone().unwrap_or_else(|| {
                    receipt_for(&op, "unknown", Some("execution_outcome_unknown"), None)
                });
                self.options.journal.complete(&op, &r).await?;
                return Ok(ExecutionOutcome {
                    receipt: r,
                    output: output_outcome,
                });
            }
            ClaimResult::Claimed => {}
        }
        let child = cancel.child_token();
        let receipt = if let Some(receipt) = remote.receipt {
            receipt
        } else if output_status
            .as_ref()
            .is_some_and(|s| matches!(s.state.as_str(), "receiving" | "complete" | "truncated"))
        {
            receipt_for(&op, "unknown", Some("remote_execution_not_pending"), None)
        } else if self.check(&op, &child).await.is_err() {
            receipt_for(&op, "failed", Some("authorization_rejected"), None)
        } else {
            let output = if output_ready {
                let terminal = self
                    .options
                    .terminal
                    .as_ref()
                    .ok_or_else(|| invalid("terminal missing"))?;
                let connection = self.current().await?;
                Some(OutputWriter::new(
                    self.options.client.api().clone(),
                    OutputOptions {
                        session: TerminalSessionReference {
                            session_contract: terminal.session_contract.clone(),
                            session_id: op.session_id.clone(),
                        },
                        operation: OutputOperationReference {
                            operation_id: op.operation_id.clone(),
                            request_digest: op.digest.clone(),
                        },
                        executor_id: connection.executor_id,
                        connection_id: connection.connection_id,
                        limits: terminal.limits.clone(),
                        encoding: "binary".into(),
                        cancellation: child.clone(),
                    },
                )?)
            } else {
                None
            };
            let context = ToolContext {
                cancellation: child.clone(),
                output: output.clone(),
            };
            let (result, observed) = self.invoke(&op, tool, context, monitored).await;
            output_outcome = observed;
            if let Some(writer) = &output {
                // A handler panic/cancellation must leave no orphan network pump.
                if writer.shutdown().await.is_err() {
                    output_outcome = Some(OutputOutcome::Incomplete { last_known: None });
                }
            }
            match result {
                Ok(v) => {
                    let text = serde_json::to_string(&v)?;
                    if super::tool::verify_result(&text).is_ok() {
                        receipt_for(
                            &op,
                            "completed",
                            None,
                            Some(Resource {
                                operation: "tool.invoke".into(),
                                args: json!({"resultJson":text}),
                            }),
                        )
                    } else {
                        receipt_for(&op, "unknown", Some("invalid_tool_result"), None)
                    }
                }
                Err(ToolError::Rejected(code)) if !code.is_empty() && code.len() <= 128 => {
                    receipt_for(&op, "failed", Some(&code), None)
                }
                _ => receipt_for(&op, "unknown", Some("execution_outcome_unknown"), None),
            }
        };
        child.cancel();
        validate_receipt(&op, &receipt)?;
        // Persist even after caller cancellation: cancellation cannot erase side effects.
        tokio::time::timeout(
            Duration::from_secs(5),
            self.options.journal.complete(&op, &receipt),
        )
        .await
        .map_err(|_| Error::Unknown("receipt durability timeout".into()))??;
        Ok(ExecutionOutcome {
            receipt,
            output: output_outcome,
        })
    }
    async fn invoke(
        &self,
        op: &Operation,
        tool: &Tool,
        ctx: ToolContext,
        monitored: bool,
    ) -> (
        std::result::Result<serde_json::Value, ToolError>,
        Option<OutputOutcome>,
    ) {
        let incomplete = || {
            ctx.output
                .as_ref()
                .map(|_| OutputOutcome::Incomplete { last_known: None })
        };
        let connection = match self.current().await {
            Ok(v) => v,
            Err(_) => return (Err(ToolError::Unknown("lease lost".into())), incomplete()),
        };
        let op_deadline = match expiry(&op.expires_at) {
            Ok(v) => v,
            Err(_) => {
                return (
                    Err(ToolError::Unknown("operation expiry".into())),
                    incomplete(),
                );
            }
        };
        let deadline = if monitored {
            op_deadline
        } else {
            op_deadline.min(expiry(&connection.expires_at).expect("current validated lease"))
        };
        let sleep = tokio::time::sleep(
            deadline
                .duration_since(SystemTime::now())
                .unwrap_or(Duration::ZERO),
        );
        tokio::pin!(sleep);
        let args = match parse_tool_arguments(op.request.args["argsJson"].as_str().unwrap_or("")) {
            Ok(v) => v,
            Err(_) => {
                return (
                    Err(ToolError::Rejected("invalid_arguments".into())),
                    incomplete(),
                );
            }
        };
        // Store a returned business fact before awaiting any observation upload.
        // Cancellation, lost ACK or panic during sealing cannot erase that fact.
        let fact = std::sync::Mutex::new(None);
        let work = async {
            let result = tool.handler.invoke(ctx.clone(), args).await;
            *fact.lock().expect("business result lock") = Some(result);
            if let Some(w) = &ctx.output {
                return Some(match w.finish().await {
                    Ok(status) => OutputOutcome::Sealed(status),
                    Err(_) => OutputOutcome::Incomplete { last_known: None },
                });
            }
            None
        };
        let handler = AssertUnwindSafe(work).catch_unwind();
        tokio::pin!(handler);
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        let mut renewed = tokio::time::Instant::now();
        let failure = loop {
            tokio::select! {
                result=&mut handler=>{
                    let observed=result.unwrap_or_else(|_|incomplete());
                    let result=fact.lock().expect("business result lock").take().unwrap_or_else(||Err(ToolError::Unknown("handler panicked".into())));
                    return (result,observed);
                },
                _=ctx.cancellation.cancelled()=>break "execution cancelled",
                _=&mut sleep=>break "execution deadline",
                _=interval.tick()=>{
                    let monitor=async {
                        let current=self.current().await.map_err(|_|"lease lost")?;
                        if monitored && renewed.elapsed()>=Duration::from_millis(current.heartbeat_after_ms){self.renew().await.map_err(|_|"lease renewal failed")?;renewed=tokio::time::Instant::now();}
                        self.check(op,&ctx.cancellation).await.map_err(|_|"authorization revoked")?;
                        match self.read_status(op).await{Ok(s)if s.status=="pending"=>Ok(()),_=>Err("remote execution no longer pending")}
                    };
                    tokio::select! {
                        result=&mut handler=>{
                            let observed=result.unwrap_or_else(|_|incomplete());
                            let result=fact.lock().expect("business result lock").take().unwrap_or_else(||Err(ToolError::Unknown("handler panicked".into())));
                            return (result,observed);
                        },
                        _=ctx.cancellation.cancelled()=>break "execution cancelled",
                        _=&mut sleep=>break "execution deadline",
                        result=monitor=>if let Err(reason)=result {break reason;},
                    }
                }
            }
        };
        ctx.cancellation.cancel();
        if let Some(w) = &ctx.output {
            w.abort();
        }
        // Cooperative handlers get a bounded cleanup window. Dropping an arbitrary
        // future cannot undo already-issued external IO, so the receipt stays unknown.
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut handler).await;
        let result = fact
            .lock()
            .expect("business result lock")
            .take()
            .unwrap_or_else(|| Err(ToolError::Unknown(failure.into())));
        (result, incomplete())
    }
}
