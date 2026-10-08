use super::*;
use crate::api::Result;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default)]
pub struct SyncResult {
    pub records: usize,
    pub complete: bool,
    pub recovered: bool,
    pub receipt: Option<MutationReceipt>,
}

/// Recover a durable original ACK, or receive exactly one bounded page.
/// No SSE cursor is changed; a stale revision requires explicit recovery.
pub async fn sync_once(
    client: &ArchiveClient,
    store: &dyn ArchiveStore,
    request_id: &str,
) -> Result<SyncResult> {
    store.check_access().await?;
    if let Some(intent) = store.pending_rebase().await? {
        return super::recovery::resume(client, store, &intent).await;
    }
    if let Some(pending) = store.pending().await? {
        store.check_access().await?;
        let receipt = client.acknowledge(&pending).await?;
        store.confirm(receipt.clone()).await?;
        return Ok(SyncResult {
            recovered: true,
            receipt: Some(receipt),
            ..Default::default()
        });
    }
    let limits = store.limits().validate()?;
    let identity = store.identity();
    let binding = client.binding(&identity.binding_id).await?;
    let status = client.status(&identity.binding_id).await?;
    if Identity::from_binding(&binding, &status)? != *identity {
        return Err(integrity());
    }
    let head = store.head().await?;
    let page = client
        .records(&binding, head.as_ref().map(|h| h.sequence.as_str()))
        .await?;
    if page.records.is_empty() {
        return Ok(SyncResult {
            complete: page.complete,
            ..Default::default()
        });
    }
    let epoch = binding.operation_epoch.as_ref().ok_or_else(integrity)?;
    let request = RequestIdentity {
        request_id: request_id.into(),
        operation_epoch: epoch.id.clone(),
    };
    validate("RequestIdentity", &request)?;
    let mut refs = BTreeMap::new();
    let mut bytes = 0usize;
    for record in &page.records {
        bytes = bytes
            .checked_add(canonical(record)?.len())
            .ok_or_else(capacity)?;
        for reference in std::iter::once(&record.payload).chain(&record.attachments) {
            if refs
                .insert(reference.artifact_id.clone(), reference.clone())
                .is_none()
            {
                bytes = bytes.checked_add(reference.bytes).ok_or_else(capacity)?;
            }
        }
    }
    if refs.len() > limits.max_artifacts || bytes > limits.max_batch_bytes {
        return Err(capacity());
    }
    let mut bodies = BTreeMap::new();
    for (id, reference) in refs {
        store.check_access().await?;
        bodies.insert(id, client.artifact(&binding, &reference).await?);
    }
    let ack = store
        .receive(&binding, &status, &page, bodies, request)
        .await?;
    store.check_access().await?;
    let receipt = client.acknowledge(&ack).await?;
    store.confirm(receipt.clone()).await?;
    Ok(SyncResult {
        records: page.records.len(),
        complete: page.complete,
        recovered: false,
        receipt: Some(receipt),
    })
}
