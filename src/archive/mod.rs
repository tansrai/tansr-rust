//! Durable, single-source archive transfer over the frozen unified API.
//!
//! A successful disk write prepares an ACK; only a verified completed receipt
//! advances confirmed coverage. This module does not implement context selection.
mod client;
mod material;
pub(crate) mod platform;
mod recovery;
mod store;
mod sync;
mod types;

pub use client::ArchiveClient;
pub use material::{MaterialResult, respond_materials};
pub use platform::create_private_directory;
pub use recovery::recover_pending;
pub use store::{ArchiveStore, FileStore, StoreLimits, StoreOptions};
pub use sync::{SyncResult, sync_once};
pub use types::*;

use crate::api::{Error, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};

pub(crate) fn integrity() -> Error {
    Error::Contract("archive integrity or identity mismatch".into())
}
pub(crate) fn capacity() -> Error {
    Error::InvalidInput("archive capacity exceeded".into())
}
pub(crate) fn validate<T: Serialize>(definition: &str, value: &T) -> Result<()> {
    crate::api::validate_wire(PROTOCOL, definition, &serde_json::to_value(value)?)
}
pub(crate) fn seq(value: &str) -> Result<u64> {
    validate("Sequence", &value)?;
    value.parse().map_err(|_| integrity())
}
pub(crate) fn canonical<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    crate::canonical::encode_limited(&serde_json::to_value(value)?, 2 << 20)
}
pub(crate) fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(crate) fn domain_hash(domain: &str, bytes: &[u8]) -> Result<String> {
    crate::canonical::digest_bytes(domain, bytes)
}
pub(crate) fn has(items: &[String], value: &str) -> bool {
    items.iter().any(|x| x == value)
}

impl Identity {
    /// Derive local ownership only from a freshly authenticated binding/status.
    pub fn from_binding(binding: &Binding, status: &Status) -> Result<Self> {
        client::verify_binding(binding)?;
        validate("ArchiveStatus", status)?;
        if binding.binding_id != status.binding_id
            || binding.source_id != status.source_id
            || binding.target.generations != status.generations
            || binding.revision != status.revision
            || binding.state != status.state
            || binding.state == "closed"
        {
            return Err(integrity());
        }
        Ok(Self {
            application_scope_id: binding.scope.application_scope_id.clone(),
            end_user_id: binding.scope.end_user_id.clone(),
            binding_id: binding.binding_id.clone(),
            session_id: binding.target.session_id.clone(),
            generations: binding.target.generations.clone(),
            source_id: binding.source_id.clone(),
            source_generation: status.source_generation.clone(),
        })
    }
}
