//! Explicit diskless Serve persistence. Discovery describes trusted placement;
//! it never grants authority, chooses a backend or creates a local fallback.
pub mod custody;

use crate::api::{ApiClient, CallOptions, Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub const CONTRACT: &str = "serve-persistence-v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArchivePersistence {
    pub contract: String,
    pub mode: String,
    pub source_id: String,
    pub source_generation: String,
    pub local_disk: bool,
    pub handoff: String,
    pub trusted_control: bool,
}

pub(crate) fn check_archive_value(value: &Value) -> Result<()> {
    let marker = value
        .get("availability")
        .or_else(|| value.get("sessionPersistence"));
    if let Some(marker) = marker {
        if marker == "source-managed" {
            let p = value
                .get("persistence")
                .ok_or_else(|| invalid("missing archive persistence"))?;
            crate::api::validate_wire("sdk2-ext-v1", "ArchiveSourcePersistence", p)?;
            for key in ["sourceId", "sourceGeneration"] {
                if value.get(key).is_some_and(|v| v != &p[key]) {
                    return Err(invalid("archive source mismatch"));
                }
            }
        } else if value.get("persistence").is_some() {
            return Err(invalid("legacy declaration contains new persistence"));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Handoff {
    pub archive: String,
    pub trusted_control: bool,
    pub execution_journal: bool,
    pub input_receipts: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Persistence {
    pub contract: String,
    pub session_id: String,
    pub mode: String,
    pub source_id: String,
    pub source_generation: String,
    pub coverage: Vec<String>,
    pub handoff: Handoff,
    pub local_disk: bool,
}
impl Persistence {
    pub fn validate(&self) -> Result<()> {
        let expected = match self.mode.as_str() {
            "terminal" => "terminal-durable-ack",
            "cloud" => "cloud-immutable-publish",
            _ => return Err(invalid("unsupported mode")),
        };
        if self.contract != CONTRACT
            || self.local_disk
            || self.session_id.is_empty()
            || self.session_id.len() > 512
            || self.handoff.archive != expected
            || self.handoff.execution_journal && self.mode != "cloud"
            || self.coverage.iter().collect::<BTreeSet<_>>().len() != self.coverage.len()
            || self.coverage.iter().any(|v| {
                ![
                    "history",
                    "memory",
                    "checkpoints",
                    "executions",
                    "input-receipts",
                    "artifacts",
                    "events",
                    "control",
                ]
                .contains(&v.as_str())
            })
        {
            return Err(invalid("invalid persistence declaration"));
        }
        for v in [&self.source_id, &self.source_generation] {
            if v.is_empty() || v.len() > 512 || v.chars().any(|c| c.is_control()) {
                return Err(invalid("invalid source identity"));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InputReceipt {
    pub session_id: String,
    pub request_id: String,
    pub digest: String,
    pub generation: String,
    pub state: String,
    pub accepted_at: String,
}
impl InputReceipt {
    fn validate(&self, session: &str) -> Result<()> {
        request_id(&self.request_id)?;
        if self.session_id != session
            || self.digest.len() != 64
            || !self
                .digest
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
            || self.generation.is_empty()
            || self.generation.len() > 512
            || self.generation.chars().any(|c| c.is_control())
            || !["accepted", "unknown"].contains(&self.state.as_str())
            || time::OffsetDateTime::parse(
                &self.accepted_at,
                &time::format_description::well_known::Rfc3339,
            )
            .is_err()
        {
            return Err(invalid("invalid input receipt"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InputPage {
    pub contract: String,
    pub session_id: String,
    pub receipts: Vec<InputReceipt>,
    pub next_cursor: Option<String>,
}

#[derive(Clone)]
pub struct Client {
    api: ApiClient,
}
impl Client {
    pub fn new(api: ApiClient) -> Self {
        Self { api }
    }
    pub async fn discover(&self, session: &str) -> Result<Persistence> {
        let value = self
            .read("session.persistence.read", session, None, BTreeMap::new())
            .await?;
        let result: Persistence = serde_json::from_value(value)?;
        result.validate()?;
        if result.session_id != session {
            return Err(invalid("foreign session"));
        }
        Ok(result)
    }
    /// Unknown and absent receipts are reconciliation states, not replay permission.
    pub async fn input(
        &self,
        session: &str,
        original_request_id: &str,
    ) -> Result<Option<InputReceipt>> {
        request_id(original_request_id)?;
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct Response {
            contract: String,
            session_id: String,
            receipt: Option<InputReceipt>,
        }
        let value = self
            .read(
                "session.input.query",
                session,
                Some(original_request_id),
                BTreeMap::new(),
            )
            .await?;
        let result: Response = serde_json::from_value(value)?;
        if result.contract != CONTRACT || result.session_id != session {
            return Err(invalid("foreign input query"));
        }
        if let Some(r) = &result.receipt {
            r.validate(session)?;
            if r.request_id != original_request_id {
                return Err(invalid("foreign input key"));
            }
        }
        Ok(result.receipt)
    }
    pub async fn inputs(
        &self,
        session: &str,
        cursor: Option<&str>,
        limit: u8,
    ) -> Result<InputPage> {
        if limit == 0 || limit > 100 {
            return Err(invalid("input page limit"));
        }
        let mut query = BTreeMap::from([("limit".into(), limit.to_string())]);
        if let Some(cursor) = cursor {
            request_id(cursor)?;
            query.insert("cursor".into(), cursor.into());
        }
        let page: InputPage = serde_json::from_value(
            self.read("session.input.list", session, None, query)
                .await?,
        )?;
        if page.contract != CONTRACT
            || page.session_id != session
            || page.receipts.len() > limit as usize
        {
            return Err(invalid("input page scope/limit"));
        }
        let mut previous = cursor.unwrap_or("");
        for receipt in &page.receipts {
            receipt.validate(session)?;
            if receipt.request_id.as_str() <= previous {
                return Err(invalid("input page order"));
            }
            previous = &receipt.request_id;
        }
        if page
            .next_cursor
            .as_deref()
            .is_some_and(|c| page.receipts.last().is_none_or(|r| r.request_id != c))
        {
            return Err(invalid("input page cursor"));
        }
        Ok(page)
    }
    async fn read(
        &self,
        op: &str,
        session: &str,
        key: Option<&str>,
        query: BTreeMap<String, String>,
    ) -> Result<Value> {
        let mut params = BTreeMap::from([("id".into(), session.into())]);
        if let Some(key) = key {
            params.insert("targetId".into(), key.into());
        }
        let response = self
            .api
            .call(
                op,
                CallOptions {
                    params,
                    query,
                    max_response_bytes: Some(262144),
                    ..Default::default()
                },
            )
            .await?;
        if response.status != 200 {
            return Err(invalid("unexpected HTTP status"));
        }
        response.json()
    }
}
pub(crate) fn request_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 128
        || !id.as_bytes()[0].is_ascii_alphanumeric()
        || !id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._~-".contains(&c))
    {
        return Err(invalid("invalid request ID"));
    }
    Ok(())
}
pub(crate) fn invalid(message: &str) -> Error {
    Error::Contract(format!("serve persistence: {message}"))
}
