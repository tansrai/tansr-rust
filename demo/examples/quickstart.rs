//! Standalone application example. Copy this file into a business crate with
//! tansr-sdk, futures-util and tokio (macros, rt-multi-thread, signal) dependencies.
use futures_util::StreamExt;
use std::{
    io::{Read, Write},
    time::{Duration, SystemTime},
};
use tansr_sdk::{
    api::{ClientBuilder, Error, Result},
    session::{CreateOptions, OutcomeStatus, SessionClient, TurnTracker, WriteOptions},
};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let result = tokio::select! {
        result=run()=>result,
        signal=tokio::signal::ctrl_c()=>signal.map_err(Error::from).and(Err(Error::Cancelled)),
        _=tokio::time::sleep(Duration::from_secs(300))=>Err(Error::Unknown("local observation deadline expired".into())),
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            // No raw transport/server messages, bodies, tokens or arguments.
            if let Error::Api(remote) = error {
                eprintln!("Serve code={} retry={}", remote.code, remote.retry_action);
            } else {
                eprintln!(
                    "Operation did not confirm completion. Retain the original request/session and reconcile; no interrupt or retry was sent."
                );
            }
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let base = std::env::var("TANSR_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:8787".into());
    let path = std::env::var_os("TANSR_TOKEN_FILE")
        .ok_or_else(|| Error::InvalidInput("TANSR_TOKEN_FILE required".into()))?;
    let request = std::env::var("TANSR_REQUEST_ID")
        .map_err(|_| Error::InvalidInput("retain a unique TANSR_REQUEST_ID for this run".into()))?;
    if request.is_empty()
        || request.len() > 100
        || !request
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Error::InvalidInput("invalid request ID".into()));
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(8193)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 8192 {
        return Err(Error::InvalidInput("token too large".into()));
    }
    let token = std::str::from_utf8(&bytes)
        .map_err(|_| Error::InvalidInput("token UTF-8".into()))?
        .trim_end_matches(['\r', '\n']);
    // This short-lived example snapshots one principal's token. Long-lived
    // applications use ClientBuilder::token_provider for same-principal renewal.
    let api = ClientBuilder::new(base)
        .session_family("sdk1")
        .token(token)
        .build()?;
    let sessions = SessionClient::new(api.clone())?;
    let create = WriteOptions {
        idempotency_key: Some(format!("{request}-create")),
        deadline: Some(SystemTime::now() + Duration::from_secs(30)),
        ..Default::default()
    };
    let session = sessions
        .create(CreateOptions {
            write: create,
            ..Default::default()
        })
        .await?;
    println!("session: {}", display(session.id()));
    let mut tracker = TurnTracker::new(session.created().last_seq)?;
    let mut events = session
        .events(
            Some(&session.created().last_seq.to_string()),
            Default::default(),
        )
        .await?;
    let send = WriteOptions {
        idempotency_key: Some(format!("{request}-send")),
        deadline: Some(SystemTime::now() + Duration::from_secs(30)),
        ..Default::default()
    };
    session
        .send("Briefly describe your available capabilities.", send)
        .await?;
    let result=async {
        while let Some(event)=events.next().await {
            let event=event?;
            match event.kind() {
                "msg.text.delta"=>{print!("{}",display(event.raw()["text"].as_str().unwrap_or("")));std::io::stdout().flush()?;}
                "server.permission.request"|"server.question.request"|"server.tool.request"=>return Err(Error::InvalidInput("this noninteractive example cannot answer; use tansr-chat without --message".into())),
                "server.replay.gap"=>return Err(Error::Unknown("replay gap".into())),
                _=>{}
            }
            if let Some(outcome)=tracker.observe(&event) {
                if outcome.status==OutcomeStatus::Completed {println!("\n[turn completed]");return Ok(());}
                return Err(Error::Unknown("turn did not complete".into()));
            }
        }
        Err(Error::Unknown("EOF is not completion".into()))
    }.await;
    events.shutdown().await;
    api.shutdown().await;
    result
}

fn display(value: &str) -> String {
    value.chars().filter(|c|(*c=='\n'||*c=='\t'||!c.is_control())&&!matches!(*c,'\u{061c}'|'\u{200e}'|'\u{200f}'|'\u{202a}'..='\u{202e}'|'\u{2066}'..='\u{2069}')).collect()
}
