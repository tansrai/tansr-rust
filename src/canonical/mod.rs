//! Bounded JSON parsing and the frozen UAPI canonical control representation.
//!
//! Ordinary business JSON may contain Unicode keys, negative numbers and fractions.
//! Control JSON instead uses printable ASCII keys and unsigned safe integer tokens.
//! Neither parser silently overwrites duplicate decoded keys. Raw archive and
//! attachment bytes must be kept separately: parsing is not a byte-preserving store.

use crate::api::{Error, Result};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};

/// Root values have depth zero; object keys do not count as nodes.
pub const MAX_DEPTH: usize = 32;
pub const MAX_NODES: usize = 100_000;
pub const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
/// Convenience encoding limit. Protocol-specific callers can tighten it.
pub const DEFAULT_MAX_BYTES: usize = 8 * 1024 * 1024;
pub const DOMAIN_CLOSURE: &str = "tansr.unified.closure.v1";

/// Parse ordinary JSON without losing duplicate-key or numeric lexical evidence.
pub fn parse_json(bytes: &[u8], max_bytes: usize) -> Result<Value> {
    parse(bytes, max_bytes, false)
}

/// Decode control JSON, accepting legal whitespace, key order and equivalent escapes.
pub fn decode(bytes: &[u8], max_bytes: usize) -> Result<Value> {
    parse(bytes, max_bytes, true)
}

/// Decode control JSON and require that the input is already canonical bytes.
pub fn parse_strict(bytes: &[u8], max_bytes: usize) -> Result<Value> {
    let value = decode(bytes, max_bytes)?;
    let encoded = encode_limited(&value, max_bytes)?;
    if encoded != bytes {
        return Err(failure("not_canonical", 0));
    }
    Ok(value)
}

fn failure(code: &str, offset: usize) -> Error {
    // Never include the raw input or field values in errors.
    Error::Contract(format!("JSON {code} at byte {offset}"))
}

fn parse(bytes: &[u8], max_bytes: usize, control: bool) -> Result<Value> {
    if max_bytes == 0 {
        return Err(Error::InvalidInput(
            "JSON max_bytes must be positive".into(),
        ));
    }
    if bytes.len() > max_bytes {
        return Err(failure("bytes_exceeded", 0));
    }
    std::str::from_utf8(bytes).map_err(|e| failure("invalid_utf8", e.valid_up_to()))?;
    let mut parser = Parser {
        bytes,
        at: 0,
        nodes: 0,
        control,
    };
    let value = parser.value(0)?;
    parser.whitespace();
    if parser.at != bytes.len() {
        return Err(failure("trailing_data", parser.at));
    }
    Ok(value)
}

struct Parser<'a> {
    bytes: &'a [u8],
    at: usize,
    nodes: usize,
    control: bool,
}

impl Parser<'_> {
    fn whitespace(&mut self) {
        while self
            .bytes
            .get(self.at)
            .is_some_and(|c| matches!(c, b' ' | b'\t' | b'\r' | b'\n'))
        {
            self.at += 1;
        }
    }

    fn value(&mut self, depth: usize) -> Result<Value> {
        self.whitespace();
        if depth > MAX_DEPTH {
            return Err(failure("depth_exceeded", self.at));
        }
        self.nodes += 1;
        if self.nodes > MAX_NODES {
            return Err(failure("nodes_exceeded", self.at));
        }
        match self.bytes.get(self.at) {
            Some(b'{') => self.object(depth),
            Some(b'[') => self.array(depth),
            Some(b'"') => Ok(Value::String(self.string()?)),
            Some(b't') => self.literal(b"true", Value::Bool(true)),
            Some(b'f') => self.literal(b"false", Value::Bool(false)),
            Some(b'n') => self.literal(b"null", Value::Null),
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(_) => Err(failure("unexpected_token", self.at)),
            None => Err(failure("unexpected_end", self.at)),
        }
    }

    fn literal(&mut self, token: &[u8], value: Value) -> Result<Value> {
        if !self.bytes[self.at..].starts_with(token) {
            return Err(failure("invalid_literal", self.at));
        }
        self.at += token.len();
        Ok(value)
    }

    fn string(&mut self) -> Result<String> {
        let start = self.at;
        self.at += 1;
        while let Some(&byte) = self.bytes.get(self.at) {
            self.at += 1;
            match byte {
                b'"' => {
                    // serde's String decoder rejects malformed escapes and lone surrogates.
                    return serde_json::from_slice(&self.bytes[start..self.at])
                        .map_err(|_| failure("invalid_string", start));
                }
                b'\\' => {
                    // Skip the escaped byte only; full escape validation is above.
                    if self.at == self.bytes.len() {
                        break;
                    }
                    self.at += 1;
                }
                0..=0x1f => return Err(failure("invalid_string", self.at - 1)),
                _ => {}
            }
        }
        Err(failure("unterminated_string", start))
    }

    fn number(&mut self) -> Result<Value> {
        let start = self.at;
        if self.bytes[self.at] == b'-' {
            self.at += 1;
        }
        match self.bytes.get(self.at) {
            Some(b'0') => self.at += 1,
            Some(b'1'..=b'9') => self.digits(),
            _ => return Err(failure("invalid_number", start)),
        }
        if self.bytes.get(self.at) == Some(&b'.') {
            self.at += 1;
            let digits = self.at;
            self.digits();
            if digits == self.at {
                return Err(failure("invalid_number", start));
            }
        }
        if self
            .bytes
            .get(self.at)
            .is_some_and(|b| matches!(b, b'e' | b'E'))
        {
            self.at += 1;
            if self
                .bytes
                .get(self.at)
                .is_some_and(|b| matches!(b, b'+' | b'-'))
            {
                self.at += 1;
            }
            let digits = self.at;
            self.digits();
            if digits == self.at {
                return Err(failure("invalid_number", start));
            }
        }
        if self
            .bytes
            .get(self.at)
            .is_some_and(|b| !matches!(b, b',' | b']' | b'}' | b' ' | b'\t' | b'\r' | b'\n'))
        {
            return Err(failure("invalid_number", start));
        }
        let token = std::str::from_utf8(&self.bytes[start..self.at])
            .map_err(|_| failure("invalid_number", start))?;
        if self.control {
            validate_number(token).map_err(|code| failure(code, start))?;
        }
        // The complete grammar above validates the token before this constructor.
        // Number::from_str with arbitrary_precision normalizes -0 to 0; this
        // pinned serde entry preserves the lexeme for downstream schema checks.
        Ok(Value::Number(Number::from_string_unchecked(
            token.to_owned(),
        )))
    }

    fn digits(&mut self) {
        while self.bytes.get(self.at).is_some_and(u8::is_ascii_digit) {
            self.at += 1;
        }
    }

    fn take(&mut self, byte: u8) -> bool {
        if self.bytes.get(self.at) == Some(&byte) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn object(&mut self, depth: usize) -> Result<Value> {
        self.at += 1;
        self.whitespace();
        let mut object = Map::new();
        if self.take(b'}') {
            return Ok(Value::Object(object));
        }
        loop {
            self.whitespace();
            if self.bytes.get(self.at) != Some(&b'"') {
                return Err(failure("expected_key", self.at));
            }
            let offset = self.at;
            let key = self.string()?;
            if self.control && !valid_key(&key) {
                return Err(failure("invalid_key", offset));
            }
            if object.contains_key(&key) {
                return Err(failure("duplicate_key", offset));
            }
            self.whitespace();
            if !self.take(b':') {
                return Err(failure("expected_colon", self.at));
            }
            object.insert(key, self.value(depth + 1)?);
            self.whitespace();
            if self.take(b'}') {
                return Ok(Value::Object(object));
            }
            if !self.take(b',') {
                return Err(failure("expected_object_separator", self.at));
            }
        }
    }

    fn array(&mut self, depth: usize) -> Result<Value> {
        self.at += 1;
        self.whitespace();
        let mut array = Vec::new();
        if self.take(b']') {
            return Ok(Value::Array(array));
        }
        loop {
            array.push(self.value(depth + 1)?);
            self.whitespace();
            if self.take(b']') {
                return Ok(Value::Array(array));
            }
            if !self.take(b',') {
                return Err(failure("expected_array_separator", self.at));
            }
        }
    }
}

fn valid_key(key: &str) -> bool {
    !key.is_empty() && key.bytes().all(|b| (0x21..=0x7e).contains(&b))
}

fn validate_number(token: &str) -> std::result::Result<(), &'static str> {
    let canonical_token = token == "0"
        || (token
            .as_bytes()
            .first()
            .is_some_and(|b| (b'1'..=b'9').contains(b))
            && token.bytes().all(|b| b.is_ascii_digit()));
    if !canonical_token {
        return Err("invalid_number");
    }
    if token.len() > 16 || token.parse::<u64>().map_or(true, |n| n > MAX_SAFE_INTEGER) {
        return Err("unsafe_integer");
    }
    Ok(())
}

/// Encode a control value with the standard convenience byte cap.
pub fn encode(value: &Value) -> Result<Vec<u8>> {
    encode_limited(value, DEFAULT_MAX_BYTES)
}

/// Encode a control value while bounding accumulated output, depth and nodes.
pub fn encode_limited(value: &Value, max_bytes: usize) -> Result<Vec<u8>> {
    if max_bytes == 0 {
        return Err(Error::InvalidInput(
            "JSON max_bytes must be positive".into(),
        ));
    }
    let mut writer = Writer {
        output: Vec::new(),
        max_bytes,
        nodes: 0,
    };
    writer.value(value, 0)?;
    Ok(writer.output)
}

struct Writer {
    output: Vec<u8>,
    max_bytes: usize,
    nodes: usize,
}

impl Writer {
    fn append(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > self.max_bytes - self.output.len() {
            return Err(failure("bytes_exceeded", self.output.len()));
        }
        self.output.extend_from_slice(bytes);
        Ok(())
    }

    fn string(&mut self, string: &str) -> Result<()> {
        self.append(b"\"")?;
        let mut start = 0;
        for (at, byte) in string.bytes().enumerate() {
            if byte >= 0x20 && byte != b'"' && byte != b'\\' {
                continue;
            }
            self.append(&string.as_bytes()[start..at])?;
            match byte {
                b'"' => self.append(b"\\\"")?,
                b'\\' => self.append(b"\\\\")?,
                b'\x08' => self.append(b"\\b")?,
                b'\t' => self.append(b"\\t")?,
                b'\n' => self.append(b"\\n")?,
                b'\x0c' => self.append(b"\\f")?,
                b'\r' => self.append(b"\\r")?,
                _ => {
                    let hex = b"0123456789abcdef";
                    self.append(&[
                        b'\\',
                        b'u',
                        b'0',
                        b'0',
                        hex[(byte >> 4) as usize],
                        hex[(byte & 15) as usize],
                    ])?;
                }
            }
            start = at + 1;
        }
        self.append(&string.as_bytes()[start..])?;
        self.append(b"\"")
    }

    fn value(&mut self, value: &Value, depth: usize) -> Result<()> {
        if depth > MAX_DEPTH {
            return Err(failure("depth_exceeded", self.output.len()));
        }
        self.nodes += 1;
        if self.nodes > MAX_NODES {
            return Err(failure("nodes_exceeded", self.output.len()));
        }
        match value {
            Value::Null => self.append(b"null"),
            Value::Bool(true) => self.append(b"true"),
            Value::Bool(false) => self.append(b"false"),
            Value::String(s) => self.string(s),
            Value::Number(number) => {
                let token = number.to_string();
                validate_number(&token).map_err(|code| failure(code, self.output.len()))?;
                self.append(token.as_bytes())
            }
            Value::Array(array) => {
                if array.len() > MAX_NODES - self.nodes {
                    return Err(failure("nodes_exceeded", self.output.len()));
                }
                self.append(b"[")?;
                for (at, value) in array.iter().enumerate() {
                    if at != 0 {
                        self.append(b",")?;
                    }
                    self.value(value, depth + 1)?;
                }
                self.append(b"]")
            }
            Value::Object(object) => {
                if object.len() > MAX_NODES - self.nodes {
                    return Err(failure("nodes_exceeded", self.output.len()));
                }
                self.append(b"{")?;
                // Explicit sorting also holds if a downstream crate enables preserve_order.
                let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
                keys.sort_unstable();
                for (at, key) in keys.into_iter().enumerate() {
                    if !valid_key(key) {
                        return Err(failure("invalid_key", self.output.len()));
                    }
                    if at != 0 {
                        self.append(b",")?;
                    }
                    self.string(key)?;
                    self.append(b":")?;
                    self.value(&object[key], depth + 1)?;
                }
                self.append(b"}")
            }
        }
    }
}

/// Hash `UTF8(domain) || 0x00 || canonical(value)` as lowercase SHA-256 hex.
pub fn digest(domain: &str, value: &Value) -> Result<String> {
    digest_bytes(domain, &encode(value)?)
}

/// Frame exact existing bytes without parsing or re-encoding them.
pub fn digest_bytes(domain: &str, bytes: &[u8]) -> Result<String> {
    if domain.is_empty() || domain.contains('\0') {
        return Err(Error::InvalidInput(
            "digest domain must be nonempty without NUL".into(),
        ));
    }
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    hasher.update([0]);
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

/// JavaScript `encodeURIComponent` bytes, including its punctuation safe set.
pub fn encode_path_segment(value: &str) -> String {
    const HEX: &[u8] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[(byte >> 4) as usize]));
            encoded.push(char::from(HEX[(byte & 15) as usize]));
        }
    }
    encoded
}
