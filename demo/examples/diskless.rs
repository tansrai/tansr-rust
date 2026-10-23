//! Read-only discovery and original-input reconciliation. No local store or
//! writer authority is created, and no unknown input is automatically retried.
use tansr_sdk::{Error, Result, archive::ArchiveClient, serve_persistence};
use tansr_sdk_demo::{Args, base_url, client, family, report, safe_text};

#[tokio::main]
async fn main() -> std::process::ExitCode {
    report("diskless", run().await)
}

async fn run() -> Result<()> {
    let mut args = Args::parse()?;
    if args.help() {
        println!(
            "diskless --session ID --persistence terminal|cloud [--request-id ORIGINAL_ID] [--base ORIGIN] [--family sdk1|sdk2-offload-v1]\nUses TANSR_TOKEN_FILE; performs only discovery and original-key queries."
        );
        return Ok(());
    }
    let base = base_url(&mut args);
    let family = family(&mut args)?;
    let session = args.required("--session")?;
    let expected = args.required("--persistence")?;
    let original = args.take("--request-id");
    args.finish()?;
    if !matches!(expected.as_str(), "terminal" | "cloud") {
        return Err(Error::InvalidInput(
            "explicit terminal or cloud required".into(),
        ));
    }
    let api = client(&base, &family)?;
    let storage = serve_persistence::Client::new(api.clone());
    let placement = storage.discover(&session).await?;
    if placement.mode != expected {
        return Err(Error::Contract(
            "configured and discovered persistence differ".into(),
        ));
    }
    let archive = ArchiveClient::new(api.clone());
    let binding = archive.installed_binding(&session).await?;
    let status = archive.status(&binding.binding_id).await?;
    tansr_sdk::archive::Identity::from_binding(&binding, &status)?;
    println!(
        "mode={} source={} binding={}",
        safe_text(&placement.mode),
        safe_text(&placement.source_id),
        safe_text(&binding.binding_id)
    );
    if let Some(original) = original {
        match storage.input(&session, &original).await? {
            Some(receipt) => println!(
                "original input state={}; query does not resubmit",
                safe_text(&receipt.state)
            ),
            None => println!(
                "original input is not confirmed; retain its key and do not invent a new input"
            ),
        }
    }
    api.shutdown().await;
    Ok(())
}
