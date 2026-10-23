//! Independent durable cloud execution journal. Only `granted` permits IO.
//! Claim secrets live in a bounded in-memory map and never in local storage.
use super::{
    client::{validate_operation, validate_receipt},
    types::*,
};
use crate::{
    api::{ApiClient, CallOptions, Result},
    serve_persistence,
};
use async_trait::async_trait;
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Mutex;
use zeroize::Zeroizing;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CloudJournalRecord {
    pub operation: Operation,
    pub attempt_id: String,
    pub connection_id: String,
    pub status: String,
    pub receipt: Option<Receipt>,
}
impl CloudJournalRecord {
    fn validate(&self, executor: &str) -> Result<()> {
        validate("ExecutionJournalRecord", self)?;
        validate_operation(&self.operation)?;
        if self.operation.binding.target.executor_id != executor
            || (self.status == "pending") != self.receipt.is_none()
        {
            return Err(invalid("cloud journal record identity/state"));
        }
        if let Some(receipt) = &self.receipt {
            validate_receipt(&self.operation, receipt)?;
            if receipt.status != self.status {
                return Err(invalid("cloud journal receipt state"));
            }
        }
        Ok(())
    }
}
struct Grant {
    operation: Operation,
    attempt: String,
    token: Mutex<Option<Zeroizing<String>>>,
}
/// Shared with the existing Runner through `Arc<dyn Journal>`. A restart must
/// construct a new adapter and query original operation IDs; it cannot recover
/// lost grant secrets or convert pending/unknown into execution permission.
pub struct CloudJournal {
    api: ApiClient,
    executor: String,
    max_active: usize,
    grants: Mutex<BTreeMap<String, Arc<Grant>>>,
}
impl CloudJournal {
    pub fn new(
        api: ApiClient,
        executor_id: impl Into<String>,
        max_active: usize,
    ) -> Result<Arc<Self>> {
        let executor = executor_id.into();
        validate("Id", &executor)?;
        if max_active == 0 || max_active > 1024 {
            return Err(invalid("cloud journal active grant limit"));
        }
        Ok(Arc::new(Self {
            api,
            executor,
            max_active,
            grants: Mutex::new(BTreeMap::new()),
        }))
    }
    async fn call(&self, operation: &str, definition: &str, options: CallOptions) -> Result<Value> {
        let response = self
            .api
            .call(
                operation,
                CallOptions {
                    max_response_bytes: Some(CONTROL_BYTES),
                    ..options
                },
            )
            .await?;
        if response.status != 200 {
            return Err(invalid("cloud journal HTTP status"));
        }
        let value = response.json()?;
        validate(definition, &value)?;
        Ok(value)
    }
    fn params(&self, operation: Option<&str>) -> BTreeMap<String, String> {
        let mut p = BTreeMap::from([("id".into(), self.executor.clone())]);
        if let Some(operation) = operation {
            p.insert("targetId".into(), operation.into());
        }
        p
    }
    pub async fn read(
        &self,
        connection_id: &str,
        operation_id: &str,
    ) -> Result<Option<CloudJournalRecord>> {
        validate("Id", &connection_id)?;
        validate("Id", &operation_id)?;
        let v = self
            .call(
                "execution.journal.read",
                "ExecutionJournalReadResponse",
                CallOptions {
                    params: self.params(Some(operation_id)),
                    query: BTreeMap::from([
                        ("protocol".into(), PROTOCOL.into()),
                        ("connectionId".into(), connection_id.into()),
                    ]),
                    ..Default::default()
                },
            )
            .await?;
        let record: Option<CloudJournalRecord> = serde_json::from_value(v["record"].clone())?;
        if let Some(r) = &record {
            r.validate(&self.executor)?;
            if r.operation.operation_id != operation_id {
                return Err(invalid("cloud journal original key"));
            }
            if r.receipt.is_some() {
                self.grants.lock().await.remove(operation_id);
            }
        }
        Ok(record)
    }
    pub async fn list(
        &self,
        connection_id: &str,
        session_id: Option<&str>,
        cursor: Option<&str>,
        limit: u8,
    ) -> Result<(Vec<CloudJournalRecord>, Option<String>)> {
        validate("Id", &connection_id)?;
        if limit == 0 || limit > 100 {
            return Err(invalid("cloud journal page limit"));
        }
        let mut q = BTreeMap::from([
            ("protocol".into(), PROTOCOL.into()),
            ("connectionId".into(), connection_id.into()),
            ("limit".into(), limit.to_string()),
        ]);
        if let Some(s) = session_id {
            validate("LegacyId", &s)?;
            q.insert("sessionId".into(), s.into());
        }
        if let Some(c) = cursor {
            validate("Id", &c)?;
            q.insert("cursor".into(), c.into());
        }
        let v = self
            .call(
                "execution.journal.list",
                "ExecutionJournalListResponse",
                CallOptions {
                    params: self.params(None),
                    query: q,
                    ..Default::default()
                },
            )
            .await?;
        let records: Vec<CloudJournalRecord> = serde_json::from_value(v["records"].clone())?;
        let next: Option<String> = serde_json::from_value(v["nextCursor"].clone())?;
        if records.len() > limit as usize {
            return Err(invalid("cloud journal page exceeded"));
        }
        let mut previous = cursor.unwrap_or("");
        for r in &records {
            r.validate(&self.executor)?;
            if r.operation.operation_id.as_str() <= previous
                || session_id.is_some_and(|s| r.operation.session_id != s)
            {
                return Err(invalid("cloud journal page identity/order"));
            }
            previous = &r.operation.operation_id;
        }
        if next
            .as_deref()
            .is_some_and(|n| records.last().is_none_or(|r| r.operation.operation_id != n))
        {
            return Err(invalid("cloud journal page cursor"));
        }
        Ok((records, next))
    }
}
#[async_trait]
impl Journal for CloudJournal {
    fn independent_cloud_authority(&self) -> bool {
        true
    }
    async fn claim(&self, op: &Operation) -> Result<ClaimResult> {
        validate_operation(op)?;
        if op.binding.target.executor_id != self.executor {
            return Err(invalid("cloud journal executor"));
        }
        let placement = serve_persistence::Client::new(self.api.clone())
            .discover(&op.session_id)
            .await?;
        if placement.mode != "cloud" || !placement.handoff.execution_journal {
            return Err(invalid("independent cloud journal unavailable"));
        }
        let mut random = [0u8; 16];
        OsRng.fill_bytes(&mut random);
        let attempt = random
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let grant = Arc::new(Grant {
            operation: op.clone(),
            attempt: attempt.clone(),
            token: Mutex::new(None),
        });
        {
            let mut grants = self.grants.lock().await;
            if let Some(old) = grants.get(&op.operation_id) {
                if old.operation != *op {
                    return Err(invalid("cloud journal operation changed"));
                }
                return Ok(ClaimResult::Unowned);
            }
            if grants.len() >= self.max_active {
                return Err(invalid("cloud journal unsettled attempt capacity"));
            }
            // Reserve before any mutation. Cancellation or a lost claim response
            // leaves a bounded tombstone, not permission to invent a new attempt.
            grants.insert(op.operation_id.clone(), grant.clone());
        }
        // Cold adapters query the original operation before claiming. A pending
        // record never transfers the old attempt's secret to this process.
        if let Some(existing) = self
            .read(&op.binding.target.connection_id, &op.operation_id)
            .await?
        {
            if existing.operation != *op {
                return Err(invalid("cloud prior operation substitution"));
            }
            return Ok(match existing.receipt {
                Some(receipt) => ClaimResult::Receipt(Box::new(receipt)),
                None => ClaimResult::Unowned,
            });
        }
        let body = json!({"protocol":PROTOCOL,"executorId":self.executor,"connectionId":op.binding.target.connection_id,"operationId":op.operation_id,"digest":op.digest,"attemptId":attempt});
        let value = self
            .call(
                "execution.journal.claim",
                "ExecutionJournalClaimResponse",
                CallOptions {
                    params: self.params(None),
                    body: Some(body),
                    ..Default::default()
                },
            )
            .await?;
        let record: CloudJournalRecord = serde_json::from_value(value["record"].clone())?;
        record.validate(&self.executor)?;
        if record.operation != *op {
            return Err(invalid("cloud claim operation substitution"));
        }
        match value["disposition"].as_str() {
            Some("granted") => {
                if record.status != "pending"
                    || record.attempt_id != attempt
                    || record.connection_id != op.binding.target.connection_id
                {
                    return Err(invalid("cloud grant identity"));
                }
                let token = value["claimToken"]
                    .as_str()
                    .ok_or_else(|| invalid("cloud claim token absent"))?;
                *grant.token.lock().await = Some(Zeroizing::new(token.into()));
                Ok(ClaimResult::Claimed)
            }
            Some("pending" | "busy") => {
                if record.status != "pending" {
                    return Err(invalid("cloud pending state"));
                }
                Ok(ClaimResult::Unowned)
            }
            Some("completed") => {
                self.grants.lock().await.remove(&op.operation_id);
                Ok(ClaimResult::Receipt(Box::new(record.receipt.ok_or_else(
                    || invalid("cloud terminal receipt absent"),
                )?)))
            }
            _ => Err(invalid("cloud claim disposition")),
        }
    }
    async fn complete(&self, op: &Operation, receipt: &Receipt) -> Result<()> {
        validate_receipt(op, receipt)?;
        let g = self
            .grants
            .lock()
            .await
            .get(&op.operation_id)
            .cloned()
            .ok_or_else(|| invalid("cloud claim secret unavailable; read original status"))?;
        if g.operation != *op {
            return Err(invalid("cloud completion operation changed"));
        }
        let secret = g.token.lock().await;
        let token = secret
            .as_ref()
            .ok_or_else(|| invalid("cloud claim response unknown; read original status"))?;
        let body = json!({"protocol":PROTOCOL,"executorId":self.executor,"connectionId":op.binding.target.connection_id,"operationId":op.operation_id,"digest":op.digest,"attemptId":g.attempt,"claimToken":token.as_str(),"receipt":receipt});
        let v = self
            .call(
                "execution.journal.complete",
                "ExecutionJournalReadResponse",
                CallOptions {
                    params: self.params(None),
                    body: Some(body),
                    ..Default::default()
                },
            )
            .await?;
        let r: CloudJournalRecord = serde_json::from_value(v["record"].clone())?;
        r.validate(&self.executor)?;
        if r.operation != *op
            || r.attempt_id != g.attempt
            || r.connection_id != op.binding.target.connection_id
            || r.receipt.as_ref() != Some(receipt)
        {
            return Err(invalid("cloud completion mismatch"));
        }
        self.grants.lock().await.remove(&op.operation_id);
        Ok(())
    }
}
