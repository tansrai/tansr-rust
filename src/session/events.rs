use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;

use crate::api::{EventEnvelope, EventStream};

use super::*;

/// Full envelope/raw fields are retained, including unknown additive events.
#[derive(Clone, Debug)]
pub struct SessionEvent {
    pub envelope: EventEnvelope,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutcomeStatus {
    Completed,
    Aborted,
    /// Unrecoverable `turn.error`; distinct from an explicit turn abort even
    /// though the unified wire terminal classification is `aborted`.
    Failed,
    SessionEnded,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcome {
    pub status: OutcomeStatus,
    pub turn_id: Option<String>,
    pub reason: Option<String>,
}

/// Associates terminal events with one turn started after a known observation
/// watermark. A replay gap invalidates the tracker until the host reconciles
/// state and constructs a new tracker; it never guesses a replacement turn.
#[derive(Clone, Debug)]
pub struct TurnTracker {
    after_seq: u64,
    active_turn_id: Option<String>,
    replaying_prefix: bool,
    invalidated: bool,
    finished: bool,
}

impl TurnTracker {
    pub fn new(after_seq: u64) -> Result<Self> {
        if after_seq > MAX_SAFE_INTEGER {
            return Err(invalid("turn watermark exceeds the safe integer limit"));
        }
        Ok(Self {
            after_seq,
            active_turn_id: None,
            replaying_prefix: false,
            invalidated: false,
            finished: false,
        })
    }

    pub fn active_turn_id(&self) -> Option<&str> {
        self.active_turn_id.as_deref()
    }

    /// Resume one running turn using an identity obtained from an authenticated
    /// Serve response, for example `input_capabilities().target.turnId`.
    pub fn resume(after_seq: u64, turn_id: impl Into<String>) -> Result<Self> {
        let turn_id = turn_id.into();
        if turn_id.is_empty() || turn_id.encode_utf16().count() > 128 {
            return Err(invalid(
                "resumed turn ID must contain 1 to 128 UTF-16 units",
            ));
        }
        let mut tracker = Self::new(after_seq)?;
        tracker.active_turn_id = Some(turn_id);
        Ok(tracker)
    }

    /// Recover the running turn from a complete ordered session replay through
    /// `after_seq`. Subscribe from cursor zero and feed every event: old starts
    /// and matching terminals establish the active identity but never report
    /// completion. Any replay gap invalidates this tracker. This works without
    /// requiring the optional same-turn-input capability.
    pub fn from_replay(after_seq: u64) -> Result<Self> {
        let mut tracker = Self::new(after_seq)?;
        tracker.replaying_prefix = true;
        Ok(tracker)
    }
    pub fn needs_reconciliation(&self) -> bool {
        self.invalidated
    }

    pub fn observe(&mut self, event: &SessionEvent) -> Option<Outcome> {
        if self.finished || self.invalidated {
            return None;
        }
        if event.kind() == "server.replay.gap" {
            self.invalidated = true;
            return None;
        }
        let sequence = event
            .envelope
            .event_id
            .as_deref()
            .and_then(|v| parse_sequence(v).ok())?;
        let turn = event
            .raw()
            .get("turnId")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty());
        if sequence <= self.after_seq {
            if self.replaying_prefix {
                if event.kind() == "turn.started" {
                    self.active_turn_id = turn.map(str::to_owned);
                } else if event.turn_outcome().is_some_and(|outcome| {
                    outcome.status == OutcomeStatus::SessionEnded
                        || self.active_turn_id.is_some() && turn == self.active_turn_id.as_deref()
                }) {
                    self.active_turn_id = None;
                }
            }
            return None;
        }
        self.replaying_prefix = false;
        if event.kind() == "turn.started" && self.active_turn_id.is_none() {
            self.active_turn_id = turn.map(str::to_owned);
            return None;
        }
        let outcome = event.turn_outcome()?;
        if outcome.status == OutcomeStatus::SessionEnded {
            self.finished = true;
            return Some(outcome);
        }
        if self.active_turn_id.as_deref().is_none() || turn != self.active_turn_id.as_deref() {
            return None;
        }
        self.finished = true;
        Some(outcome)
    }
}

impl SessionEvent {
    pub fn kind(&self) -> &str {
        self.envelope.r#type.as_deref().unwrap_or_default()
    }
    pub fn raw(&self) -> &Value {
        &self.envelope.raw
    }

    /// EOF, acceptance and `session.ended` are never evidence of turn success.
    /// Unknown event types remain observable and cannot complete a turn.
    pub fn turn_outcome(&self) -> Option<Outcome> {
        let terminal = self.envelope.terminal_status.as_deref();
        let status = match (self.kind(), terminal) {
            ("turn.completed", Some("completed")) => OutcomeStatus::Completed,
            ("turn.aborted", Some("aborted")) => OutcomeStatus::Aborted,
            ("turn.error", Some("aborted"))
                if self.raw().get("recoverable").and_then(Value::as_bool) == Some(false) =>
            {
                OutcomeStatus::Failed
            }
            ("session.ended", Some("completed")) => OutcomeStatus::SessionEnded,
            (kind, Some("unknown")) if kind.starts_with("turn.") => OutcomeStatus::Unknown,
            _ => return None,
        };
        Some(Outcome {
            status,
            turn_id: self
                .raw()
                .get("turnId")
                .and_then(Value::as_str)
                .map(str::to_owned),
            reason: self
                .raw()
                .get("reason")
                .and_then(Value::as_str)
                .map(str::to_owned),
        })
    }
}

/// A single-consumer stream with no background task or automatic reconnect.
/// The resume cursor advances only when an event is delivered, never when data
/// is merely buffered. Persist it after application processing as appropriate.
pub struct SessionEventStream {
    inner: Option<EventStream>,
    session_id: String,
    cancellation: CancellationToken,
    last_event_id: Option<String>,
    last_seq: Option<u64>,
}

impl Session {
    /// Opens the observation before returning, so callers can subscribe before
    /// sending a prompt. This cancellation token affects this stream only.
    pub async fn events(
        &self,
        last_event_id: Option<&str>,
        cancellation: CancellationToken,
    ) -> Result<SessionEventStream> {
        let last_event_id = last_event_id.filter(|s| !s.is_empty());
        let last_seq = last_event_id.map(parse_sequence).transpose()?;
        let child = cancellation.child_token();
        let options = CallOptions {
            params: self.params(),
            last_event_id: last_event_id.map(str::to_owned),
            cancellation: child.clone(),
            ..CallOptions::default()
        };
        let inner = tokio::select! {
            biased;
            () = self.client.cancellation.cancelled() => return Err(Error::Cancelled),
            result = self.api().events("session.events.observe", options) => result?,
        };
        Ok(SessionEventStream {
            inner: Some(inner),
            session_id: self.id().into(),
            cancellation: child,
            last_event_id: last_event_id.map(str::to_owned),
            last_seq,
        })
    }
}

impl SessionEventStream {
    pub fn last_event_id(&self) -> Option<&str> {
        self.last_event_id.as_deref()
    }

    /// Local cancellation and transport release, with no Serve interrupt.
    pub fn close(&mut self) {
        self.cancellation.cancel();
        self.inner = None;
    }

    /// There is no hidden worker to join; dropping the transport is sufficient.
    pub async fn shutdown(&mut self) {
        self.close();
    }

    fn decode(&mut self, envelope: EventEnvelope) -> Result<Option<SessionEvent>> {
        if envelope.domain != "session" || !envelope.raw.is_object() {
            return Err(contract("session event has no matching object envelope"));
        }
        let kind = envelope.r#type.as_deref().unwrap_or_default();
        if kind == "server.replay.gap" {
            if envelope.event_id.is_some()
                || !envelope
                    .cursor_set
                    .get("eventCursor")
                    .is_some_and(Value::is_null)
                || envelope.raw["sessionId"] != self.session_id
                || envelope.raw["type"] != kind
            {
                return Err(contract("replay gap changed identity or cursor"));
            }
            if envelope.raw["reason"] == "ahead_of_log" {
                self.last_seq = None;
                self.last_event_id = None;
            }
            // Other gaps remain observable. No history fetch/ACK/restart occurs.
            return Ok(Some(SessionEvent { envelope }));
        }
        let id = envelope
            .event_id
            .as_deref()
            .ok_or_else(|| contract("session event lacks an ID"))?;
        if envelope
            .cursor_set
            .get("eventCursor")
            .and_then(Value::as_str)
            != Some(id)
        {
            return Err(contract("event cursor differs from event ID"));
        }
        let sequence = parse_sequence(id)?;
        if kind.starts_with("server.") {
            if !envelope
                .raw
                .get("ts")
                .and_then(Value::as_f64)
                .is_some_and(f64::is_finite)
            {
                return Err(contract("control event has no finite timestamp"));
            }
        } else if envelope.raw["type"] != kind
            || envelope.raw["sessionId"] != self.session_id
            || envelope.raw.get("seq").and_then(Value::as_u64) != Some(sequence)
        {
            return Err(contract(
                "kernel event identity differs from its session stream",
            ));
        }
        if self.last_seq.is_some_and(|last| sequence <= last) {
            return Ok(None);
        }
        self.last_seq = Some(sequence);
        self.last_event_id = Some(id.into());
        Ok(Some(SessionEvent { envelope }))
    }
}

impl Stream for SessionEventStream {
    type Item = Result<SessionEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.inner.is_none() {
            return Poll::Ready(None);
        }
        if this.cancellation.is_cancelled() {
            this.close();
            return Poll::Ready(Some(Err(Error::Cancelled)));
        }
        // Bound duplicate processing per poll to keep a hostile replay from
        // monopolizing the runtime even when its entire body is buffered.
        for _ in 0..64 {
            let polled = match this.inner.as_mut() {
                Some(inner) => inner.as_mut().poll_next(cx),
                None => return Poll::Ready(None),
            };
            match polled {
                Poll::Ready(Some(Ok(envelope))) => match this.decode(envelope) {
                    Ok(Some(event)) => return Poll::Ready(Some(Ok(event))),
                    Ok(None) => continue,
                    Err(error) => {
                        this.close();
                        return Poll::Ready(Some(Err(error)));
                    }
                },
                Poll::Ready(Some(Err(error))) => {
                    this.close();
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(None) => {
                    this.close();
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl Drop for SessionEventStream {
    fn drop(&mut self) {
        self.close();
    }
}

fn parse_sequence(value: &str) -> Result<u64> {
    if value.is_empty()
        || value.len() > 1 && value.starts_with('0')
        || !value.bytes().all(|c| c.is_ascii_digit())
    {
        return Err(invalid(
            "session cursor must be a canonical nonnegative decimal integer",
        ));
    }
    value
        .parse::<u64>()
        .ok()
        .filter(|n| *n <= MAX_SAFE_INTEGER)
        .ok_or_else(|| invalid("session cursor exceeds the safe integer limit"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    fn event(id: Option<&str>, kind: &str, terminal: Option<&str>, raw: Value) -> EventEnvelope {
        EventEnvelope {
            contract: "tansr.unified.v1".into(),
            domain: "session".into(),
            event_id: id.map(str::to_owned),
            r#type: Some(kind.into()),
            terminal_status: terminal.map(str::to_owned),
            raw,
            cursor_set: json!({"eventCursor":id,"archiveCoverage":null,"outputWatermark":null,"materialConsumed":null,"ackReceipt":null}),
        }
    }

    fn stream(events: Vec<EventEnvelope>, cursor: Option<&str>) -> SessionEventStream {
        SessionEventStream {
            inner: Some(Box::pin(futures_util::stream::iter(
                events.into_iter().map(Ok),
            ))),
            session_id: "s1".into(),
            cancellation: CancellationToken::new(),
            last_event_id: cursor.map(str::to_owned),
            last_seq: cursor.map(|v| parse_sequence(v).unwrap()),
        }
    }

    #[test]
    fn outcomes_distinguish_error_recovery_session_end_and_unknown_additions() {
        for (kind, status, recoverable, expected) in [
            (
                "turn.completed",
                Some("completed"),
                None,
                Some(OutcomeStatus::Completed),
            ),
            (
                "turn.aborted",
                Some("aborted"),
                None,
                Some(OutcomeStatus::Aborted),
            ),
            (
                "turn.error",
                Some("aborted"),
                Some(false),
                Some(OutcomeStatus::Failed),
            ),
            ("turn.error", Some("aborted"), Some(true), None),
            (
                "session.ended",
                Some("completed"),
                None,
                Some(OutcomeStatus::SessionEnded),
            ),
            (
                "turn.future",
                Some("unknown"),
                None,
                Some(OutcomeStatus::Unknown),
            ),
            ("future.event", Some("completed"), None, None),
        ] {
            let event = SessionEvent {
                envelope: event(
                    Some("1"),
                    kind,
                    status,
                    json!({"type":kind,"sessionId":"s1","seq":1,"recoverable":recoverable}),
                ),
            };
            assert_eq!(event.turn_outcome().map(|o| o.status), expected);
        }
    }

    #[tokio::test]
    async fn replay_gap_resets_only_ahead_cursor_and_eof_is_not_completion() {
        let mut stream = stream(
            vec![
                event(
                    Some("4"),
                    "turn.completed",
                    Some("completed"),
                    json!({"type":"turn.completed","sessionId":"s1","seq":4}),
                ),
                event(
                    None,
                    "server.replay.gap",
                    None,
                    json!({"type":"server.replay.gap","sessionId":"s1","reason":"ahead_of_log"}),
                ),
                event(
                    Some("1"),
                    "server.permission.request",
                    None,
                    json!({"ts":12.5,"requestId":"ticket","digest":"proof"}),
                ),
            ],
            Some("4"),
        );
        assert_eq!(
            stream.next().await.unwrap().unwrap().kind(),
            "server.replay.gap"
        );
        assert_eq!(stream.last_event_id(), None);
        assert_eq!(
            stream.next().await.unwrap().unwrap().kind(),
            "server.permission.request"
        );
        assert_eq!(stream.last_event_id(), Some("1"));
        assert!(stream.next().await.is_none());
        assert_eq!(stream.last_event_id(), Some("1"));
    }

    #[tokio::test]
    async fn foreign_identity_never_advances_cursor_and_closes_stream() {
        let mut stream = stream(
            vec![event(
                Some("3"),
                "turn.completed",
                Some("completed"),
                json!({"type":"turn.completed","sessionId":"another","seq":3}),
            )],
            Some("2"),
        );
        assert!(stream.next().await.unwrap().is_err());
        assert_eq!(stream.last_event_id(), Some("2"));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn cursors_advance_on_delivery_only_and_close_cancels_only_child() {
        let parent = CancellationToken::new();
        let mut stream = stream(
            vec![event(
                Some("1"),
                "turn.completed",
                Some("completed"),
                json!({"type":"turn.completed","sessionId":"s1","seq":1}),
            )],
            None,
        );
        stream.cancellation = parent.child_token();
        assert_eq!(stream.last_event_id(), None);
        stream.next().await.unwrap().unwrap();
        assert_eq!(stream.last_event_id(), Some("1"));
        stream.shutdown().await;
        assert!(!parent.is_cancelled());
        assert!(stream.next().await.is_none());
    }

    #[test]
    fn resumed_tracker_only_finishes_the_authoritative_current_turn() {
        let terminal = |seq: &str, turn: &str| SessionEvent {
            envelope: event(
                Some(seq),
                "turn.completed",
                Some("completed"),
                json!({"turnId":turn}),
            ),
        };
        let mut tracker = TurnTracker::resume(10, "current").unwrap();
        assert!(tracker.observe(&terminal("9", "current")).is_none());
        assert!(tracker.observe(&terminal("11", "older")).is_none());
        assert_eq!(
            tracker.observe(&terminal("12", "current")).unwrap().status,
            OutcomeStatus::Completed
        );
        assert!(TurnTracker::resume(0, "").is_err());
    }

    #[test]
    fn complete_replay_recovers_running_turn_without_optional_steering() {
        let make = |seq: &str, kind: &str, turn: &str| SessionEvent {
            envelope: event(
                Some(seq),
                kind,
                if kind == "turn.completed" {
                    Some("completed")
                } else {
                    None
                },
                json!({"turnId":turn}),
            ),
        };
        let mut tracker = TurnTracker::from_replay(10).unwrap();
        assert!(tracker.observe(&make("2", "turn.started", "old")).is_none());
        assert!(
            tracker
                .observe(&make("5", "turn.completed", "old"))
                .is_none()
        );
        assert_eq!(tracker.active_turn_id(), None);
        assert!(
            tracker
                .observe(&make("8", "turn.started", "current"))
                .is_none()
        );
        assert_eq!(tracker.active_turn_id(), Some("current"));
        assert!(
            tracker
                .observe(&make("11", "turn.completed", "old"))
                .is_none()
        );
        assert_eq!(
            tracker
                .observe(&make("12", "turn.completed", "current"))
                .unwrap()
                .status,
            OutcomeStatus::Completed
        );
    }

    #[test]
    fn cursor_rejects_lexical_and_numeric_ambiguity() {
        for bad in ["", "01", "+1", "-0", "1.0", "1e1", "9007199254740992", " 1"] {
            assert!(parse_sequence(bad).is_err(), "{bad}");
        }
        for good in ["0", "1", "9007199254740991"] {
            assert!(parse_sequence(good).is_ok());
        }
    }

    #[test]
    fn tracker_never_confuses_old_foreign_or_gap_completion_with_current_turn() {
        let make = |seq: &str, kind: &str, turn: &str| SessionEvent {
            envelope: event(
                Some(seq),
                kind,
                if kind == "turn.completed" {
                    Some("completed")
                } else {
                    None
                },
                json!({"turnId":turn,"type":kind,"sessionId":"s1","seq":seq.parse::<u64>().unwrap()}),
            ),
        };
        let mut tracker = TurnTracker::new(4).unwrap();
        assert!(
            tracker
                .observe(&make("4", "turn.completed", "old"))
                .is_none()
        );
        assert!(
            tracker
                .observe(&make("5", "turn.completed", "old"))
                .is_none()
        );
        assert!(tracker.observe(&make("6", "turn.started", "new")).is_none());
        assert!(
            tracker
                .observe(&make("7", "turn.completed", "old"))
                .is_none()
        );
        assert_eq!(
            tracker
                .observe(&make("8", "turn.completed", "new"))
                .unwrap()
                .status,
            OutcomeStatus::Completed
        );
        assert!(
            tracker
                .observe(&make("9", "turn.completed", "new"))
                .is_none()
        );
        let mut tracker = TurnTracker::new(4).unwrap();
        tracker.observe(&make("6", "turn.started", "new"));
        tracker.observe(&SessionEvent {
            envelope: event(None, "server.replay.gap", None, json!({})),
        });
        assert!(tracker.needs_reconciliation());
        assert!(
            tracker
                .observe(&make("8", "turn.completed", "new"))
                .is_none()
        );
    }
}
