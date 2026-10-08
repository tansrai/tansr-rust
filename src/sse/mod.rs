//! Incremental, bounded SSE framing. EOF never means a completed agent turn.
//!
//! A complete CR-delimited frame is emitted immediately. A trailing LF is then
//! consumed without waiting for another network chunk. Incomplete data at EOF
//! fails closed, matching the strict mode used by the unified transport.

use crate::api::{Error, Result};

pub const DEFAULT_MAX_FRAME_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub event: Option<String>,
    /// Only present when this frame included a valid id field; empty resets it.
    pub id: Option<String>,
    pub data: String,
    /// A transport hint, never permission to automatically replay an operation.
    pub retry: Option<u64>,
}

pub struct Parser {
    max_frame_bytes: usize,
    frame_bytes: usize,
    line: Vec<u8>,
    bom: Vec<u8>,
    bom_checked: bool,
    skip_lf: bool,
    data: String,
    has_data: bool,
    event: Option<String>,
    id: Option<String>,
    retry: Option<u64>,
    last_event_id: String,
    done: bool,
}

impl Parser {
    /// Zero selects the 2 MiB default. The cap includes ignored fields/comments.
    pub fn new(max_frame_bytes: usize) -> Self {
        Self {
            max_frame_bytes: if max_frame_bytes == 0 {
                DEFAULT_MAX_FRAME_BYTES
            } else {
                max_frame_bytes
            },
            frame_bytes: 0,
            line: Vec::new(),
            bom: Vec::new(),
            bom_checked: false,
            skip_lf: false,
            data: String::new(),
            has_data: false,
            event: None,
            id: None,
            retry: None,
            last_event_id: String::new(),
            done: false,
        }
    }

    /// The parser position is not the application's durably processed cursor.
    pub fn last_event_id(&self) -> &str {
        &self.last_event_id
    }

    pub fn feed(&mut self, bytes: &[u8]) -> Result<Vec<Frame>> {
        if self.done {
            return Err(Error::Contract("SSE parser is closed".into()));
        }
        let mut frames = Vec::new();
        let result = self.consume(bytes, &mut frames);
        if result.is_err() {
            self.done = true;
        }
        result?;
        Ok(frames)
    }

    fn consume(&mut self, bytes: &[u8], frames: &mut Vec<Frame>) -> Result<()> {
        for &byte in bytes {
            if !self.bom_checked {
                const BOM: &[u8] = &[0xef, 0xbb, 0xbf];
                if byte == BOM[self.bom.len()] {
                    self.bom.push(byte);
                    if self.bom.len() == 3 {
                        self.bom.clear();
                        self.bom_checked = true;
                    }
                    continue;
                }
                self.bom_checked = true;
                for prefix in std::mem::take(&mut self.bom) {
                    self.byte(prefix, frames)?;
                }
            }
            self.byte(byte, frames)?;
        }
        Ok(())
    }

    fn byte(&mut self, byte: u8, frames: &mut Vec<Frame>) -> Result<()> {
        if self.skip_lf {
            self.skip_lf = false;
            if byte == b'\n' {
                return Ok(());
            }
        }
        if self.frame_bytes == self.max_frame_bytes {
            return Err(Error::Contract("SSE frame exceeds byte limit".into()));
        }
        self.frame_bytes += 1;
        if byte == b'\r' || byte == b'\n' {
            self.skip_lf = byte == b'\r';
            self.complete_line(frames)?;
        } else {
            self.line.push(byte);
        }
        Ok(())
    }

    fn complete_line(&mut self, frames: &mut Vec<Frame>) -> Result<()> {
        let bytes = std::mem::take(&mut self.line);
        let line = std::str::from_utf8(&bytes)
            .map_err(|_| Error::Contract("SSE contains invalid UTF-8".into()))?;
        if line.is_empty() {
            if let Some(frame) = self.dispatch() {
                frames.push(frame);
            }
            return Ok(());
        }
        if line.starts_with(':') {
            return Ok(());
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "data" => {
                if self.has_data {
                    self.data.push('\n');
                }
                self.data.push_str(value);
                self.has_data = true;
            }
            "event" => self.event = Some(value.to_owned()),
            "id" if !value.contains('\0') => self.id = Some(value.to_owned()),
            "retry" if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) => {
                if let Ok(retry) = value.parse() {
                    self.retry = Some(retry);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn dispatch(&mut self) -> Option<Frame> {
        let frame = Frame {
            event: self.event.take(),
            id: self.id.take(),
            data: std::mem::take(&mut self.data),
            retry: self.retry.take(),
        };
        self.frame_bytes = 0;
        if let Some(id) = &frame.id {
            self.last_event_id.clone_from(id);
        }
        if std::mem::take(&mut self.has_data) {
            Some(frame)
        } else {
            None
        }
    }

    /// Close input. Partial frames are errors and are never fabricated as delivered events.
    pub fn finish(&mut self) -> Result<Vec<Frame>> {
        if self.done {
            return Ok(Vec::new());
        }
        self.done = true;
        self.line.extend_from_slice(&self.bom);
        self.bom.clear();
        std::str::from_utf8(&self.line)
            .map_err(|_| Error::Contract("SSE contains incomplete UTF-8".into()))?;
        if !self.line.is_empty() || self.has_data {
            return Err(Error::Contract(
                "SSE ended inside an incomplete frame".into(),
            ));
        }
        // A frame without data does not dispatch but preserves a completed id line.
        self.dispatch();
        Ok(Vec::new())
    }
}
