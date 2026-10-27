//! 单次准备组合已有接口，既不创建调度队列，也不把取消令牌移交给会话。
use super::{CreateOptions, Session, SessionClient, invalid};
use crate::{
    api::{CallOptions, Error},
    executor,
};
use async_trait::async_trait;
use futures_util::FutureExt;
use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::{Instant, SystemTime},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparationStage {
    PreparingPersistence,
    Connecting,
    SessionConfirmed,
    InitializingCapabilities,
    Ready,
}
impl PreparationStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreparingPersistence => "preparing-persistence",
            Self::Connecting => "connecting",
            Self::SessionConfirmed => "session-confirmed",
            Self::InitializingCapabilities => "initializing-capabilities",
            Self::Ready => "ready",
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparationStatus {
    Started,
    Completed,
    Failed,
    Cancelled,
}
#[derive(Clone, Debug)]
pub struct PreparationProgress {
    pub stage: PreparationStage,
    pub status: PreparationStatus,
    pub sequence: u64,
    pub elapsed: std::time::Duration,
}

/// 配对已有同传输执行客户端；None 保留工具默认，Some(vec![]) 明确清空。
/// 同步回调须快速返回；panic 被隔离，不启动后台回调或额外任务。
pub struct PreparationOptions<'a> {
    /// Explicit independent source; completion means ready before create.
    pub prepare_persistence: Option<&'a dyn PersistenceProvider>,
    pub execution: &'a executor::Client,
    pub platform: executor::Platform,
    pub requested_tools: Option<Vec<String>>,
    pub on_progress: Option<&'a (dyn Fn(PreparationProgress) + Send + Sync)>,
    pub on_session_confirmed: Option<&'a (dyn Fn(&Session) + Send + Sync)>,
}
impl<'a> PreparationOptions<'a> {
    pub fn new(execution: &'a executor::Client) -> Self {
        Self {
            prepare_persistence: None,
            execution,
            platform: executor::Platform::current(),
            requested_tools: None,
            on_progress: None,
            on_session_confirmed: None,
        }
    }
}
pub struct PreparedSession {
    pub session: Session,
    pub capabilities: executor::Capabilities,
    persistence: Option<Arc<PreparationStop>>,
}

/// Preparation-only control. Do not use it as the transferred source lifetime.
#[derive(Clone)]
pub struct PreparationControl {
    cancellation: CancellationToken,
    parent: CancellationToken,
    deadline: Option<SystemTime>,
}
impl PreparationControl {
    pub fn check(&self) -> crate::api::Result<()> {
        if self.cancellation.is_cancelled() || self.parent.is_cancelled() {
            Err(Error::Cancelled)
        } else if self.deadline.is_some_and(|at| at <= SystemTime::now()) {
            Err(invalid("preparation deadline exceeded"))
        } else {
            Ok(())
        }
    }
    pub fn deadline(&self) -> Option<SystemTime> {
        self.deadline
    }
    pub async fn cancelled(&self) {
        let deadline = async {
            if let Some(at) = self.deadline {
                tokio::time::sleep(at.duration_since(SystemTime::now()).unwrap_or_default()).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            () = self.cancellation.cancelled() => {},
            () = self.parent.cancelled() => {},
            () = deadline => {},
        }
    }
}

#[async_trait]
pub trait PreparationResource: Send + Sync {
    /// Stop only the resource explicitly owned by this preparation, never the remote session.
    async fn stop(&self) -> crate::api::Result<()>;
}

#[async_trait]
pub trait PersistenceProvider: Send + Sync {
    /// Return only when the independent source is ready. None borrows an external
    /// source without stop authority. Clean up internal failures before returning.
    async fn prepare_persistence(
        &self,
        startup: PreparationControl,
    ) -> crate::api::Result<Option<Arc<dyn PreparationResource>>>;
}

enum StopState {
    Ready,
    Started,
    Complete(std::result::Result<(), Arc<Error>>),
}
struct PreparationStop {
    resource: Arc<dyn PreparationResource>,
    state: tokio::sync::Mutex<StopState>,
}
impl PreparationStop {
    async fn stop(&self) -> std::result::Result<(), Arc<Error>> {
        // Per-resource only. The marker survives a dropped stop future: unknown
        // cleanup is never treated as permission to execute stop a second time.
        let mut state = self.state.lock().await;
        match &*state {
            StopState::Complete(result) => return result.clone(),
            StopState::Started => {
                return Err(Arc::new(invalid("persistence cleanup completion unknown")));
            }
            StopState::Ready => {}
        }
        *state = StopState::Started;
        let result = match AssertUnwindSafe(self.resource.stop()).catch_unwind().await {
            Ok(result) => result,
            Err(_) => Err(invalid("persistence cleanup panicked; completion unknown")),
        }
        .map_err(Arc::new);
        *state = StopState::Complete(result.clone());
        result
    }
}
impl PreparedSession {
    /// At most one local stop invocation, with its original outcome retained.
    pub async fn stop(&self) -> std::result::Result<(), Arc<Error>> {
        match &self.persistence {
            Some(owned) => owned.stop().await,
            None => Ok(()),
        }
    }
}

/// 原错误和确认身份均保留。错误不代表远端建会未发生，不自动重建或关闭。
pub struct PreparationError {
    pub stage: PreparationStage,
    pub session: Option<Session>,
    pub cause: Error,
    pub cleanup_error: Option<Arc<Error>>,
    persistence: Option<Arc<PreparationStop>>,
}
impl PreparationError {
    pub async fn stop(&self) -> std::result::Result<(), Arc<Error>> {
        match &self.persistence {
            Some(owned) => owned.stop().await,
            None => Ok(()),
        }
    }
}
impl fmt::Debug for PreparationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparationError")
            .field("stage", &self.stage)
            .field("session_id", &self.session.as_ref().map(Session::id))
            .field("cause", &self.cause)
            .finish()
    }
}
impl fmt::Display for PreparationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "session preparation {} failed: {}",
            self.stage.as_str(),
            self.cause
        )
    }
}
impl std::error::Error for PreparationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

struct Progress<'a> {
    options: &'a PreparationOptions<'a>,
    started: Instant,
    sequence: u64,
    persistence: Option<Arc<PreparationStop>>,
}
impl Progress<'_> {
    fn emit(&mut self, stage: PreparationStage, status: PreparationStatus) {
        self.sequence += 1;
        if let Some(callback) = self.options.on_progress {
            let item = PreparationProgress {
                stage,
                status,
                sequence: self.sequence,
                elapsed: self.started.elapsed(),
            };
            let _ = catch_unwind(AssertUnwindSafe(|| callback(item)));
        }
    }
    async fn failed(
        &mut self,
        stage: PreparationStage,
        session: Option<Session>,
        cause: Error,
    ) -> PreparationError {
        self.emit(
            stage,
            if matches!(cause, Error::Cancelled) {
                PreparationStatus::Cancelled
            } else {
                PreparationStatus::Failed
            },
        );
        let cleanup_error = match &self.persistence {
            Some(owned) => owned.stop().await.err(),
            None => None,
        };
        PreparationError {
            stage,
            session,
            cause,
            cleanup_error,
            persistence: self.persistence.clone(),
        }
    }
}

impl SessionClient {
    /// 使用 create.write 的原取消令牌和期限控制整个准备过程；返回会话仍保持
    /// 原 SessionClient 的生命周期。请取消令牌后继续 await 以接收已确认身份。
    /// 不在后台重试、不自动订阅事件、不隐式启动设备服务。
    pub async fn prepare(
        &self,
        create: CreateOptions,
        options: PreparationOptions<'_>,
    ) -> std::result::Result<PreparedSession, PreparationError> {
        let control = CallOptions {
            cancellation: create.write.cancellation.clone(),
            deadline: create.write.deadline,
            ..Default::default()
        };
        let startup = PreparationControl {
            cancellation: control.cancellation.clone(),
            parent: self.cancellation.clone(),
            deadline: control.deadline,
        };
        let check = || startup.check();
        let mut progress = Progress {
            options: &options,
            started: Instant::now(),
            sequence: 0,
            persistence: None,
        };
        let validation = (|| -> crate::api::Result<()> {
            if !self.api.shares_transport(options.execution.api()) {
                return Err(invalid(
                    "preparation execution client must share the same API transport",
                ));
            }
            crate::api::validate_wire(
                executor::PROTOCOL,
                "ClientPlatform",
                &serde_json::to_value(&options.platform)?,
            )?;
            if let Some(tools) = &options.requested_tools {
                let mut seen = std::collections::HashSet::new();
                if tools
                    .iter()
                    .any(|tool| tool.is_empty() || !seen.insert(tool))
                {
                    return Err(invalid("invalid or duplicate requested tool"));
                }
            }
            check()
        })();
        if let Err(cause) = validation {
            return Err(progress
                .failed(PreparationStage::Connecting, None, cause)
                .await);
        }
        if let Some(provider) = options.prepare_persistence {
            let stage = PreparationStage::PreparingPersistence;
            progress.emit(stage, PreparationStatus::Started);
            if let Err(cause) = check() {
                return Err(progress.failed(stage, None, cause).await);
            }
            // Await settlement rather than racing away and losing a late resource.
            let prepared = AssertUnwindSafe(provider.prepare_persistence(startup.clone()))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| Err(invalid("persistence preparation panicked")));
            match prepared {
                Ok(Some(resource)) => {
                    progress.persistence = Some(Arc::new(PreparationStop {
                        resource,
                        state: tokio::sync::Mutex::new(StopState::Ready),
                    }));
                }
                Ok(None) => {}
                Err(cause) => return Err(progress.failed(stage, None, cause).await),
            }
            if let Err(cause) = check() {
                return Err(progress.failed(stage, None, cause).await);
            }
            progress.emit(stage, PreparationStatus::Completed);
        }
        progress.emit(PreparationStage::Connecting, PreparationStatus::Started);
        if let Err(cause) = check() {
            return Err(progress
                .failed(PreparationStage::Connecting, None, cause)
                .await);
        }
        let session = match self.create(create).await {
            Ok(session) => session,
            Err(cause) => {
                return Err(progress
                    .failed(PreparationStage::Connecting, None, cause)
                    .await);
            }
        };
        // 必须在后续取消检查和初始化之前交出原确认身份。
        if let Some(callback) = options.on_session_confirmed {
            let _ = catch_unwind(AssertUnwindSafe(|| callback(&session)));
        }
        if let Err(cause) = check() {
            return Err(progress
                .failed(PreparationStage::Connecting, Some(session), cause)
                .await);
        }
        progress.emit(PreparationStage::Connecting, PreparationStatus::Completed);
        progress.emit(
            PreparationStage::SessionConfirmed,
            PreparationStatus::Started,
        );
        if let Err(cause) = check() {
            return Err(progress
                .failed(PreparationStage::SessionConfirmed, Some(session), cause)
                .await);
        }
        progress.emit(
            PreparationStage::SessionConfirmed,
            PreparationStatus::Completed,
        );
        progress.emit(
            PreparationStage::InitializingCapabilities,
            PreparationStatus::Started,
        );
        let result = async {
            check()?;
            let closure = session.capabilities_for(&control).await?;
            check()?;
            if closure
                .operations
                .get("execution.initialize")
                .map(String::as_str)
                != Some("enabled")
            {
                return Err(invalid(
                    "execution.initialize is not enabled; no write attempted",
                ));
            }
            // 与既有 SessionClient::call 相同：父令牌关闭本地 HTTP 等待，
            // 不宣称远端初始化已回滚，也不遗留后台请求或替换 Session 的令牌。
            let result = tokio::select! {
                biased;
                () = self.cancellation.cancelled() => Err(Error::Cancelled),
                result = options.execution.initialize_with_control(
                    session.id(), &options.platform, options.requested_tools.clone(),
                    &closure.closure_id, control.clone(),
                ) => result,
            }?;
            check()?;
            Ok(result)
        }
        .await;
        let capabilities = match result {
            Ok(value) => value,
            Err(cause) => {
                return Err(progress
                    .failed(
                        PreparationStage::InitializingCapabilities,
                        Some(session),
                        cause,
                    )
                    .await);
            }
        };
        progress.emit(
            PreparationStage::InitializingCapabilities,
            PreparationStatus::Completed,
        );
        progress.emit(PreparationStage::Ready, PreparationStatus::Started);
        if let Err(cause) = check() {
            return Err(progress
                .failed(PreparationStage::Ready, Some(session), cause)
                .await);
        }
        progress.emit(PreparationStage::Ready, PreparationStatus::Completed);
        Ok(PreparedSession {
            session,
            capabilities,
            persistence: progress.persistence,
        })
    }
}
