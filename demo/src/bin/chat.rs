//! A multi-turn terminal UI; all agent execution remains in Serve.
use futures_util::StreamExt;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, Read, Write},
    time::Duration,
};
use tansr_sdk::{
    api::{Error, Result},
    session::{
        Answer, CreateOptions, Input, OutcomeStatus, Session, SessionClient, SessionEvent,
        TurnTracker,
    },
};
use tansr_sdk_demo::{Args, base_url, client, describe, family, report, safe_text, write_options};
use tokio_util::sync::CancellationToken;

const HELP: &str = "tansr-chat [--base ORIGIN] [--family sdk1|sdk2-offload-v1] [--resume ID] [--model MODEL] [--message TEXT] [--timeout SECONDS]\nNew offload sessions additionally require --request-id STABLE_ID.\nAuthentication: TANSR_TOKEN_FILE. No approval is automatic.\nCommands: /interrupt, /allow TICKET, /deny TICKET, /answers TICKET JSON_ARRAY, /insert JSON_OBJECT, /history, /quit.\nCtrl+C and /quit stop local observation only. /interrupt explicitly requests a Serve interruption.";

#[tokio::main]
async fn main() -> std::process::ExitCode {
    report("tansr-chat", run().await)
}

async fn run() -> Result<()> {
    let mut args = Args::parse()?;
    if args.help() {
        println!("{HELP}");
        return Ok(());
    }
    let base = base_url(&mut args);
    let family = family(&mut args)?;
    let resume = args.take("--resume");
    let create_request = if family == "sdk2-offload-v1" && resume.is_none() {
        Some(args.required("--request-id")?)
    } else {
        None
    };
    let model = args.take("--model");
    let message = args.take("--message");
    let timeout = args
        .value("--timeout", "600")
        .parse::<u64>()
        .map_err(|_| Error::InvalidInput("timeout must be seconds".into()))?;
    if timeout == 0 || timeout > 86400 {
        return Err(Error::InvalidInput(
            "timeout must be 1..86400 seconds".into(),
        ));
    }
    args.finish()?;
    let api = client(&base, &family)?;
    let sessions = SessionClient::new(api)?;
    let cancel = CancellationToken::new();
    let task = async {
        let session = if let Some(id) = resume {
            sessions.resume(&id, write_options(&cancel)).await?
        } else {
            sessions
                .create(CreateOptions {
                    request_id: create_request,
                    model,
                    write: write_options(&cancel),
                    ..Default::default()
                })
                .await?
        };
        println!("session: {}", safe_text(session.id()));
        println!("{HELP}");
        chat(&session, message, cancel.clone()).await
    };
    tokio::pin!(task);
    tokio::select! {
        result = &mut task => result,
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(Error::from)?;
            cancel.cancel();
            Err(Error::Cancelled)
        }
        _ = tokio::time::sleep(Duration::from_secs(timeout)) => {
            cancel.cancel();
            Err(Error::Unknown("local observation deadline expired; query the original session".into()))
        }
    }
}

/// A dedicated console reader avoids putting uncancellable stdin reads in
/// Tokio's blocking pool. It lives only for this CLI process, never in the SDK.
fn console_lines() -> tokio::sync::mpsc::Receiver<Result<String>> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut reader = stdin.lock();
        loop {
            let mut bytes = Vec::new();
            let result = (&mut reader).take(262_145).read_until(b'\n', &mut bytes);
            match result {
                Ok(0) => break,
                Ok(_) if bytes.len() <= 262_144 => {
                    let line = String::from_utf8(bytes)
                        .map_err(|_| Error::InvalidInput("console input is not UTF-8".into()));
                    if tx.blocking_send(line).is_err() {
                        break;
                    }
                }
                Ok(_) => {
                    let _ = tx.blocking_send(Err(Error::InvalidInput(
                        "console line exceeds 256 KiB".into(),
                    )));
                    break;
                }
                Err(_) => {
                    let _ = tx.blocking_send(Err(Error::Io("console read failed".into())));
                    break;
                }
            }
        }
    });
    rx
}

#[derive(Default)]
struct Tickets {
    permissions: BTreeMap<String, String>,
    questions: BTreeSet<String>,
}

async fn chat(session: &Session, message: Option<String>, cancel: CancellationToken) -> Result<()> {
    let meta = session.meta().await?;
    if !meta.live || meta.status == "ended" {
        return Err(Error::InvalidInput("session is not live".into()));
    }
    let mut active = meta.status == "running";
    if active && message.is_some() {
        return Err(Error::InvalidInput(
            "session already has a running turn; resume without --message".into(),
        ));
    }
    let mut floor = meta.last_seq;
    let mut replaying_identity = false;
    let mut tracker = if active {
        let capabilities = session.input_capabilities().await?;
        if let Some(turn_id) = capabilities
            .get("target")
            .and_then(|target| target.get("turnId"))
            .and_then(Value::as_str)
        {
            TurnTracker::resume(floor, turn_id)?
        } else {
            // The turn can finish between the metadata and target reads.
            // Re-read state rather than waiting forever for an old start event.
            let latest = session.meta().await?;
            if latest.status == "running" && latest.live {
                replaying_identity = true;
                TurnTracker::from_replay(floor)?
            } else if latest.status == "idle" && latest.live {
                active = false;
                floor = latest.last_seq;
                TurnTracker::new(floor)?
            } else {
                return Err(Error::Unknown(
                    "running turn identity unavailable; reconcile session before continuing".into(),
                ));
            }
        }
    } else {
        TurnTracker::new(floor)?
    };
    let cursor = if active {
        "0".into()
    } else {
        floor.to_string()
    };
    let mut stream = session.events(Some(&cursor), cancel.clone()).await?;
    let one_shot = message.is_some();
    let mut lines = (!one_shot).then(console_lines);
    let mut tickets = Tickets::default();
    if let Some(text) = message {
        session.send(&text, write_options(&cancel)).await?;
        active = true;
    }
    let result = loop {
        tokio::select! {
            _ = cancel.cancelled() => break Err(Error::Cancelled),
            line = async { match &mut lines { Some(input) => input.recv().await, None => std::future::pending().await } } => {
                let Some(line) = line else { lines = None; if !active { break Ok(()); } continue; };
                let line = line?;
                let text = line.trim();
                if text.is_empty() { continue; }
                let mut parts = text.splitn(3, ' ');
                match parts.next().unwrap_or("") {
                    "/quit" => { if active { println!("local observation stopped; Serve turn is still unconfirmed"); } break Ok(()); }
                    "/interrupt" => {
                        session.interrupt(write_options(&cancel)).await?;
                        println!("interruption requested; waiting for the Serve terminal event");
                    }
                    "/allow" | "/deny" => {
                        let Some(ticket) = parts.next() else { println!("usage: /allow TICKET or /deny TICKET"); continue; };
                        let Some(digest) = tickets.permissions.get(ticket) else { println!("ticket absent or closed; approvals require an observed current ticket"); continue; };
                        let verdict = if text.starts_with("/allow ") { "allow" } else { "deny" };
                        session.permission(ticket, digest, verdict, write_options(&cancel)).await?;
                        tickets.permissions.remove(ticket);
                    }
                    "/answers" => {
                        let (Some(ticket), Some(json)) = (parts.next(), parts.next()) else { println!("usage: /answers TICKET JSON_ARRAY"); continue; };
                        if !tickets.questions.contains(ticket) { println!("question absent or closed"); continue; }
                        let answers: Vec<Answer> = serde_json::from_value(tansr_sdk::canonical::parse_json(json.as_bytes(), 262_144)?)?;
                        session.answer(ticket, answers, write_options(&cancel)).await?;
                        tickets.questions.remove(ticket);
                    }
                    "/insert" => {
                        let Some(json) = text.strip_prefix("/insert ") else { println!("usage: /insert JSON_OBJECT (inputId, target, content, ack)"); continue; };
                        let input: Input = serde_json::from_value(tansr_sdk::canonical::parse_json(json.as_bytes(), 262_144)?)?;
                        session.submit_input(input, write_options(&cancel)).await?;
                        println!("input acknowledged; this does not prove core consumption");
                    }
                    "/history" => {
                        let history = session.history(0, 0).await?;
                        println!("history count-only response: {}", safe_text(&history.to_string()));
                    }
                    value if value.starts_with('/') => println!("unknown command; /quit exits locally, /interrupt stops the Serve turn"),
                    _ => {
                        if active { println!("turn is running; use /insert with its original target or wait"); continue; }
                        let before = session.meta().await?;
                        if before.status != "idle" { println!("session is not idle; inspect the current turn"); continue; }
                        tracker = TurnTracker::new(before.last_seq)?;
                        session.send(text, write_options(&cancel)).await?;
                        active = true;
                    }
                }
            }
            event = stream.next() => {
                let Some(event) = event else { break Err(Error::Unknown("event stream ended without proof of turn completion".into())); };
                let event = event?;
                if event.kind() == "server.replay.gap" { break Err(Error::Unknown("event replay gap; reconcile history before continuing".into())); }
                display(&event, &mut tickets)?;
                if one_shot && matches!(event.kind(), "server.permission.request" | "server.question.request") {
                    break Err(Error::InvalidInput("this --message run has no interactive input; resume without --message to answer the observed ticket".into()));
                }
                let outcome = tracker.observe(&event);
                if replaying_identity && event.envelope.event_id.as_deref().and_then(|id|id.parse::<u64>().ok()).is_some_and(|seq|seq>=floor) {
                    replaying_identity = false;
                    if tracker.active_turn_id().is_none() && outcome.is_none() {
                        let latest = session.meta().await?;
                        if latest.status == "idle" && latest.live {
                            active = false;
                            tickets = Tickets::default();
                            tracker = TurnTracker::new(latest.last_seq)?;
                            if lines.is_none() { break Ok(()); }
                        } else {
                            break Err(Error::Unknown("replay could not establish the current turn identity; reconcile before continuing".into()));
                        }
                    }
                }
                if let Some(outcome) = outcome {
                    if active {
                        active = false;
                        tickets = Tickets::default();
                        if outcome.status != OutcomeStatus::Completed { break Err(Error::Unknown(format!("turn outcome: {:?}", outcome.status))); }
                        println!("\n[turn completed]");
                        if one_shot || lines.is_none() { break Ok(()); }
                    }
                }
            }
        }
    };
    stream.shutdown().await;
    if let Err(error) = &result {
        eprintln!(
            "session retained for reconciliation: {} ({})",
            safe_text(session.id()),
            describe(error)
        );
    }
    result
}

fn display(event: &SessionEvent, tickets: &mut Tickets) -> Result<()> {
    let raw = event.raw();
    let field = |name: &str| raw.get(name).and_then(Value::as_str).unwrap_or("");
    match event.kind() {
        "msg.text.delta" => {
            print!("{}", safe_text(field("text")));
            std::io::stdout().flush()?;
        }
        "server.permission.request" => {
            let (ticket, digest) = (field("requestId"), field("digest"));
            if ticket.is_empty() || digest.is_empty() {
                return Err(Error::Contract("approval ticket missing identity".into()));
            }
            tickets.permissions.insert(ticket.into(), digest.into());
            println!(
                "\n[permission {}] {} {}\n/allow {} or /deny {}",
                safe_text(ticket),
                safe_text(field("name")),
                safe_text(field("summary")),
                safe_text(ticket),
                safe_text(ticket)
            );
        }
        "server.permission.closed" => {
            tickets.permissions.remove(field("requestId"));
        }
        "server.question.request" => {
            let ticket = field("requestId");
            if ticket.is_empty() || !raw.get("questions").is_some_and(Value::is_array) {
                return Err(Error::Contract(
                    "question missing identity or questions".into(),
                ));
            }
            tickets.questions.insert(ticket.into());
            println!(
                "\n[question {}] {}\nUse /answers TICKET JSON_ARRAY",
                safe_text(ticket),
                safe_text(&raw["questions"].to_string())
            );
        }
        "server.question.closed" => {
            tickets.questions.remove(field("requestId"));
        }
        "server.tool.request" => {
            return Err(Error::InvalidInput(
                "legacy inline tool request requires an explicitly bound executor; run tansr-tools"
                    .into(),
            ));
        }
        kind if !kind.starts_with("msg.") => {
            println!(
                "\n[event: {}] {}",
                safe_text(kind),
                safe_text(field("name"))
            )
        }
        _ => {}
    }
    Ok(())
}
