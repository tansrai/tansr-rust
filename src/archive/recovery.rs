use super::*;
use crate::api::{Error, Result};

const FAMILY: &str = "sdk2-archive-recovery-v1";
pub(crate) fn verify_rebase(input: &AckRebaseRequest) -> Result<()> {
    crate::api::validate_wire(FAMILY, "AckRebaseRequest", &serde_json::to_value(input)?)?;
    client::verify_coverage(&input.previous.coverage)?;
    if input.binding_id != input.previous.binding_id
        || input.request == input.previous.request
        || input.request.operation_epoch != input.previous.request.operation_epoch
    {
        return Err(integrity());
    }
    if canonical(&input.previous)?.len() > 262144 || canonical(input)?.len() > 263168 {
        return Err(capacity());
    }
    Ok(())
}
pub(crate) fn verify_rebase_result(
    input: &AckRebaseRequest,
    result: &AckRebaseReceipt,
) -> Result<()> {
    verify_rebase(input)?;
    crate::api::validate_wire(FAMILY, "AckRebaseReceipt", &serde_json::to_value(result)?)?;
    if result.protocol != input.protocol
        || result.binding_id != input.binding_id
        || result.previous != input.previous
        || result.request != input.request
        || result.next.request != input.request
        || seq(&result.next.expected_revision)? <= seq(&input.previous.expected_revision)?
    {
        return Err(integrity());
    }
    let mut prior = result.next.clone();
    prior.request = input.previous.request.clone();
    prior.expected_revision = input.previous.expected_revision.clone();
    if prior != input.previous {
        return Err(integrity());
    }
    if canonical(&result.next)?.len() > 262144 {
        return Err(capacity());
    }
    Ok(())
}
impl ArchiveClient {
    /// Send only an already persisted recovery intent. The server chooses the
    /// replacement revision; callers must confirm it in the same store.
    pub async fn rebase_acknowledgement(
        &self,
        input: &AckRebaseRequest,
    ) -> Result<AckRebaseReceipt> {
        verify_rebase(input)?;
        let mut options = client::mutation_options(&input.binding_id, input, &input.request, None)?;
        options.max_response_bytes = Some(store::REBASE_RESERVE);
        let response = self.api.call("archive.ack.rebase", options).await?;
        if response.status != 200 {
            return Err(integrity());
        }
        let value = response.json()?;
        crate::api::validate_wire(FAMILY, "AckRebaseReceipt", &value)?;
        let result: AckRebaseReceipt = serde_json::from_value(value)?;
        verify_rebase_result(input, &result)?;
        Ok(result)
    }
}
fn stale(error: &Error) -> bool {
    matches!(error,Error::Api(e) if e.code=="precondition_failed" && e.detail.get("reason").and_then(|v|v.as_str())==Some("if_match_stale")
        && matches!(e.detail.get("domainCode").and_then(|v|v.as_str()),Some("binding_conflict"|"stale_revision")))
}
/// Explicit ACK recovery. Unknown network results never authorize a new key.
/// An existing durable recovery intent always wins over `request_id`.
pub async fn recover_pending(
    client: &ArchiveClient,
    store: &dyn ArchiveStore,
    request_id: &str,
) -> Result<SyncResult> {
    store.check_access().await?;
    let intent = if let Some(intent) = store.pending_rebase().await? {
        intent
    } else {
        let pending = store.pending().await?.ok_or_else(integrity)?;
        match client.acknowledge(&pending).await {
            Ok(receipt) => {
                store.confirm(receipt.clone()).await?;
                return Ok(SyncResult {
                    recovered: true,
                    receipt: Some(receipt),
                    ..Default::default()
                });
            }
            Err(e) if stale(&e) => {}
            Err(e) => return Err(e),
        }
        store
            .prepare_rebase(RequestIdentity {
                request_id: request_id.into(),
                operation_epoch: pending.request.operation_epoch,
            })
            .await?
    };
    resume(client, store, &intent).await
}
pub(crate) async fn resume(
    client: &ArchiveClient,
    store: &dyn ArchiveStore,
    intent: &AckRebaseRequest,
) -> Result<SyncResult> {
    store.check_access().await?;
    let receipt = match client.rebase_acknowledgement(intent).await {
        Ok(result) => {
            let receipt = result.receipt.clone();
            store.confirm_rebase(result).await?;
            receipt
        }
        Err(e) => {
            if matches!(&e,Error::Api(a) if a.code=="conflict" && a.detail.get("domainCode").and_then(|v|v.as_str())==Some("request_id_conflict"))
            {
                if let Ok(receipt) = client
                    .operation(&intent.binding_id, "archive-ack", &intent.previous.request)
                    .await
                {
                    store.confirm(receipt.clone()).await?;
                    return Ok(SyncResult {
                        recovered: true,
                        receipt: Some(receipt),
                        ..Default::default()
                    });
                }
            }
            return Err(e);
        }
    };
    Ok(SyncResult {
        recovered: true,
        receipt: Some(receipt),
        ..Default::default()
    })
}
