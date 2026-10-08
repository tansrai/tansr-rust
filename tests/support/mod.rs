use serde_json::Value;
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
};

/// 仅用于显式启动的内部真实 Serve 验收；不读取生产凭据，不打包进 crate。
pub struct Serve {
    child: Child,
    output: Option<tokio::task::JoinHandle<()>>,
    controls: tokio::sync::mpsc::Receiver<Value>,
    pub info: Value,
    directory: Option<tempfile::TempDir>,
}
fn node_path(path: PathBuf) -> PathBuf {
    #[cfg(windows)]
    {
        let value = path.to_str().expect("synthetic UTF-8 path");
        if let Some(rest) = value.strip_prefix(r"\\?\UNC\") {
            return PathBuf::from(format!(r"\\{rest}"));
        }
        if let Some(rest) = value.strip_prefix(r"\\?\") {
            return PathBuf::from(rest);
        }
    }
    path
}
impl Serve {
    pub async fn start(mode: &str) -> Self {
        Self::start_in(mode, tempfile::tempdir().expect("test directory")).await
    }
    async fn start_in(mode: &str, directory: tempfile::TempDir) -> Self {
        let fixture = PathBuf::from(std::env::var("TANSR_RUST_SERVE_FIXTURE").expect(
            "required: TANSR_RUST_SERVE_FIXTURE must point to the verified private Serve fixture",
        ));
        assert!(fixture.is_file(), "required Serve fixture is unavailable");
        let fixture = node_path(fixture);
        let physical = std::fs::canonicalize(directory.path()).expect("physical test directory");
        // Rust returns Win32 verbatim paths; the existing Node archive host
        // builds file URLs and needs the equivalent ordinary physical path.
        let physical = node_path(physical);
        let mut command =
            Command::new(std::env::var("TANSR_RUST_SERVE_NODE").unwrap_or_else(|_| "node".into()));
        command
            .arg(fixture)
            .arg(".")
            .arg(&physical)
            .arg(mode)
            .current_dir(&physical)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let mut child = command.spawn().expect("start real Serve");
        let mut lines = BufReader::new(child.stdout.take().expect("stdout")).lines();
        let info = tokio::time::timeout(Duration::from_secs(30), async {
            while let Some(line) = lines.next_line().await.expect("fixture stdout") {
                if let Some(json) = line.strip_prefix("TANSR_GO_FIXTURE ") {
                    return serde_json::from_str::<Value>(json).expect("fixture metadata");
                }
            }
            panic!("Serve exited without readiness");
        })
        .await
        .expect("Serve startup timeout");
        assert_eq!(info["manifestRevision"], 7);
        // Keep draining the inherited pipe after readiness. Dropping the reader
        // would turn later fixture diagnostics into EPIPE or a full pipe hang.
        let (control_tx, controls) = tokio::sync::mpsc::channel(16);
        let output = tokio::spawn(async move {
            while let Ok(Some(line)) = lines.next_line().await {
                if let Some(json) = line.strip_prefix("TANSR_RUST_CONTROL ") {
                    let value =
                        serde_json::from_str::<Value>(json).expect("fixture control receipt");
                    if control_tx.try_send(value).is_err() {
                        panic!("fixture control receiver unavailable or overflowed");
                    }
                }
            }
        });
        Self {
            child,
            output: Some(output),
            controls,
            info,
            directory: Some(directory),
        }
    }
    pub fn client(&self, family: &str) -> tansr_sdk::ApiClient {
        tansr_sdk::ClientBuilder::new(self.info["baseURL"].as_str().expect("base URL"))
            .token(self.info["token"].as_str().expect("synthetic token"))
            .session_family(family)
            .build()
            .expect("client")
    }
    #[allow(dead_code)] // Each integration target uses only the controls it needs.
    pub async fn send_control(&mut self, value: &Value) {
        let mut bytes = serde_json::to_vec(value).expect("synthetic fixture control");
        bytes.push(b'\n');
        let stdin = self.child.stdin.as_mut().expect("fixture control pipe");
        tokio::time::timeout(Duration::from_secs(5), async {
            stdin
                .write_all(&bytes)
                .await
                .expect("fixture control write");
            stdin.flush().await.expect("fixture control flush");
            let receipt = self.controls.recv().await.expect("fixture control closed");
            assert_eq!(receipt["requestId"], value["requestId"]);
            assert_eq!(receipt["ok"], true, "fixture control rejected");
        })
        .await
        .expect("fixture control timeout");
    }
    async fn stop_child(&mut self) {
        if let Some(mut stdin) = self.child.stdin.take() {
            let _ = stdin.write_all(b"stop\n").await;
        }
        let status = tokio::time::timeout(Duration::from_secs(20), self.child.wait())
            .await
            .expect("Serve cleanup timeout")
            .expect("Serve exit");
        if let Some(output) = self.output.take() {
            output.await.expect("Serve stdout drain");
        }
        assert!(status.success(), "Serve cleanup failed: {status}");
    }
    pub async fn stop(mut self) {
        self.stop_child().await;
    }
    /// 重启独立 Node 进程并保留同一真实会话介质；不能冒充同进程恢复。
    #[allow(dead_code)]
    pub async fn restart(mut self, mode: &str) -> Self {
        assert!(
            mode.starts_with("session"),
            "only the session host supports reopen"
        );
        self.stop_child().await;
        let directory = self.directory.take().expect("retained test directory");
        Self::start_in(mode, directory).await
    }
}

impl Drop for Serve {
    fn drop(&mut self) {
        // Test panic cleanup is local process cleanup, never a production kill.
        let _ = self.child.start_kill();
        if let Some(output) = self.output.take() {
            output.abort();
        }
    }
}
