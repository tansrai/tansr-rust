//! Native asynchronous Rust client for Tansr Serve's unified `/api` contract.
//!
//! Serve owns the agent runtime, context, memory selection, permissions and
//! accounting. This crate connects a host application to those capabilities;
//! it creates no global runtime and installs no shell or filesystem tools.
//! Both explicit session families, `sdk1` and `sdk2-offload-v1`, use `/api`.
//!
//! # Quick start
//!
//! The host provides a Tokio runtime, Serve origin and short-lived user token.
//! Use HTTPS remotely. Never distribute an application key or model credential.
//! This example attaches to an existing session and reads its metadata without
//! creating a replacement session, starting a turn or approving a tool:
//!
//! ```no_run
//! use tansr_sdk::{ClientBuilder, Result, session::SessionClient};
//!
//! async fn inspect_session(
//!     origin: &str,
//!     short_lived_user_token: &str,
//!     existing_session_id: &str,
//! ) -> Result<String> {
//!     let api = ClientBuilder::new(origin)
//!         .session_family("sdk1")
//!         .token(short_lived_user_token)
//!         .build()?;
//!     let result = async {
//!         let sessions = SessionClient::new(api.clone())?;
//!         let session = sessions.attach(existing_session_id).await?;
//!         Ok(session.meta().await?.status)
//!     }.await;
//!     api.shutdown().await;
//!     result
//! }
//! ```
//!
//! For long-lived applications, [`ClientBuilder::token_provider`] delegates
//! token renewal to the host. Renewal must preserve the authenticated principal;
//! it does not log in, replay a failed write or change an existing SSE stream.
//!
//! # Capability entry points
//!
//! - [`session::SessionClient`]: create, attach and explicitly resume sessions.
//!   Create without an initial prompt, subscribe to [`session::Session::events`],
//!   then send. [`session::TurnTracker`] identifies completion of the current
//!   turn; HTTP acceptance, EOF and old completion events are not completion.
//! - [`executor`]: register explicitly authorized business handlers and retain
//!   durable execution receipts. Negotiated output capture is not an ACK;
//!   a lost output confirmation never authorizes re-executing the business tool.
//! - [`archive`]: verify and persist archives, reconcile durable ACKs, and
//!   provide exactly requested materials. The host owns storage and keys;
//!   Serve still composes context. Local archives do not implement a separate
//!   memory coordinator or promise cross-language file-format compatibility.
//! - [`api`]: generated operations, capability fencing, HTTP/SSE and unified
//!   errors. [`canonical`] and [`sse`] expose the strict wire primitives.
//!
//! Preserve original request identities, preconditions and deadlines after an
//! uncertain write. Local cancellation stops observation, not necessarily
//! Serve execution. Explicitly shut down streams/runners; dropping them never
//! sends a durable archive ACK or silently closes the remote session.
//!
//! # Guides and examples
//!
//! Read the [English guide](https://github.com/tansrai/tansr-rust/blob/v0.1.0/doc/guide.md)
//! or [中文指南](https://github.com/tansrai/tansr-rust/blob/v0.1.0/doc/使用指南.md).
//! Both guides and READMEs are included in this crate's source package.
//! Add the exact SDK release with `cargo add tansr-sdk@=0.1.0`.
//! The separate [Demo package](https://docs.rs/tansr-sdk-demo/0.1.0/tansr_sdk_demo/)
//! contains the full quickstart plus `tansr-chat`, `tansr-tools` and
//! `tansr-archive`; install it with
//! `cargo install tansr-sdk-demo --version 0.1.0 --locked`.
//!
//! The SDK, Demo, guides and 20 distributed contract JSON files are MIT licensed.
//! Serve/kernel and internal reference materials remain private and are not
//! included in the release. Publish the SDK before the exact-version-dependent
//! Demo. Package preparation and local rustdoc builds alone do not establish
//! a crates.io release or a live exact-version docs.rs page.
pub mod api;
pub mod archive;
pub mod canonical;
pub mod executor;
pub mod session;
pub mod sse;
pub use api::{ApiClient, CallOptions, ClientBuilder, Error, Result};
pub use tokio_util::sync::CancellationToken;
