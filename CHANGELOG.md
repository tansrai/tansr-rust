# Changelog

## 0.1.0 — 2026-10-08

Initial MIT release of `tansr-sdk` and `tansr-sdk-demo`.

- Native asynchronous Rust client for the frozen unified `/api` contract: 81 generated operations, strict JSON/canonical handling, bounded HTTP/SSE, explicit cancellation and safe retry semantics.
- High-level `sdk1` and `sdk2-offload-v1` sessions, same-turn input, approval/question handling, media and explicit resume.
- Explicit client business tools with durable execution receipts and negotiated streaming output; no automatic shell or Serve-host execution fallback.
- Encrypted local archives, verified records/materials, durable ACK recovery and explicit rebase.
- Three installable examples: `tansr-chat`, `tansr-tools`, `tansr-archive`; English and Chinese developer guides.

Serve remains a separate service. Consumers need Rust 1.85 or newer; Windows builds require MSVC C/C++ build tools and NASM on PATH. The public source snapshot contains 20 authorized contract JSON assets and source provenance, while private Serve/reference implementation files and their Git history are excluded.
