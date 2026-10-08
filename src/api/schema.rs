//! Validation against the vendored, frozen schemas. This is deliberately not a general
//! JSON Schema API: schemas and regular expressions are compiled once from trusted assets.
//! Decode untrusted bytes with `canonical::parse_json` before passing a value here.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::OnceLock,
};

use regex::Regex;
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use super::{Error, Result};

struct Schema {
    root: Value,
    patterns: BTreeMap<String, Regex>,
}

impl Schema {
    fn load(bytes: &str) -> std::result::Result<Self, String> {
        let root: Value = serde_json::from_str(bytes).map_err(|error| error.to_string())?;
        audit_rule(&root)?;
        fn patterns(
            value: &Value,
            result: &mut BTreeMap<String, Regex>,
        ) -> std::result::Result<(), String> {
            match value {
                Value::Object(object) => {
                    if let Some(pattern) = object.get("pattern").and_then(Value::as_str) {
                        if !result.contains_key(pattern) {
                            let regex = Regex::new(pattern)
                                .map_err(|error| format!("unsupported frozen pattern: {error}"))?;
                            result.insert(pattern.into(), regex);
                        }
                    }
                    for child in object.values() {
                        patterns(child, result)?;
                    }
                }
                Value::Array(array) => {
                    for child in array {
                        patterns(child, result)?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        let mut compiled = BTreeMap::new();
        patterns(&root, &mut compiled)?;
        Ok(Self {
            root,
            patterns: compiled,
        })
    }
}

fn schema(family: &str) -> Result<&'static Schema> {
    macro_rules! frozen {
        ($file:literal) => {{
            static SCHEMA: OnceLock<std::result::Result<Schema, String>> = OnceLock::new();
            SCHEMA
                .get_or_init(|| Schema::load(include_str!(concat!("../../contract/", $file))))
                .as_ref()
                .map_err(|reason| Error::Contract(reason.clone()))
        }};
    }
    match family {
        "unified-v1" => frozen!("unified-v1.schema.json"),
        "sdk2-ext-v1" => frozen!("sdk2-ext-v1.schema.json"),
        "terminal-services-v1" => frozen!("terminal-services-v1.schema.json"),
        "sdk2-archive-recovery-v1" => frozen!("sdk2-archive-recovery-v1.schema.json"),
        "archive-sync-v1" => frozen!("archive-sync-v1.schema.json"),
        "sdk2-cache-v1" => frozen!("sdk2-cache-v1.schema.json"),
        "sdk2-cache-core-v1" => frozen!("sdk2-cache-core-v1.schema.json"),
        "terminal-observation-v1" => frozen!("terminal-observation-v1.schema.json"),
        "terminal-profile-v1" => frozen!("terminal-profile-v1.schema.json"),
        "terminal-shell-sandbox-v1" => frozen!("terminal-shell-sandbox-v1.schema.json"),
        _ => Err(Error::InvalidInput(format!(
            "unsupported schema family {family}"
        ))),
    }
}
/// Check one named definition in the frozen unified API schema.
pub fn validate(definition: &str, value: &Value) -> Result<()> {
    validate_family("unified-v1", definition, value)
}

/// Check one named definition in an explicitly supported frozen family schema.
/// Unknown families and definitions fail; they never mean "validation skipped".
pub fn validate_family(family: &str, definition: &str, value: &Value) -> Result<()> {
    let schema = schema(family)?;
    let definition_schema = schema
        .root
        .get("definitions")
        .and_then(|defs| defs.get(definition))
        .ok_or_else(|| {
            Error::InvalidInput(format!("unknown schema definition {family}#{definition}"))
        })?;
    let mut nodes = 0;
    check_tree(value, 0, &mut nodes)?;
    let checker = Checker { schema, definition };
    checker.check(definition_schema, value, "", 0)
}

fn check_tree(value: &Value, depth: usize, nodes: &mut usize) -> Result<()> {
    *nodes += 1;
    if depth > 64 || *nodes > 200_000 {
        return Err(Error::Contract("JSON depth or node limit exceeded".into()));
    }
    match value {
        Value::Object(object) => {
            for child in object.values() {
                check_tree(child, depth + 1, nodes)?;
            }
        }
        Value::Array(array) => {
            for child in array {
                check_tree(child, depth + 1, nodes)?;
            }
        }
        Value::Number(number) if number.as_f64().is_none_or(|number| !number.is_finite()) => {
            return Err(Error::Contract("non-finite number".into()));
        }
        _ => {}
    }
    Ok(())
}

struct Checker<'a> {
    schema: &'a Schema,
    definition: &'a str,
}

fn child(path: &str, key: &str) -> String {
    format!("{path}/{}", key.replace('~', "~0").replace('/', "~1"))
}

impl Checker<'_> {
    fn fail(&self, path: &str, reason: &str) -> Error {
        // Do not include values: they may contain credentials or user content.
        Error::Contract(format!("{}{path}: {reason}", self.definition))
    }

    fn check(&self, rule: &Value, value: &Value, path: &str, depth: usize) -> Result<()> {
        if depth > 128 {
            return Err(self.fail(path, "schema recursion limit exceeded"));
        }
        if let Some(accept) = rule.as_bool() {
            return if accept {
                Ok(())
            } else {
                Err(self.fail(path, "value forbidden"))
            };
        }
        let object = rule
            .as_object()
            .ok_or_else(|| self.fail(path, "invalid frozen schema"))?;
        if object.keys().any(|key| !KEYWORDS.contains(&key.as_str())) {
            return Err(self.fail(path, "unsupported schema keyword"));
        }
        if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
            let pointer = reference
                .strip_prefix('#')
                .ok_or_else(|| self.fail(path, "external schema reference forbidden"))?;
            let target = self
                .schema
                .root
                .pointer(pointer)
                .ok_or_else(|| self.fail(path, "unresolved schema reference"))?;
            // Draft 7 ignores siblings of $ref.
            return self.check(target, value, path, depth + 1);
        }
        if let Some(constant) = object.get("const") {
            if value != constant {
                return Err(self.fail(path, "constant mismatch"));
            }
        }
        if let Some(variants) = object.get("enum").and_then(Value::as_array) {
            if !variants.contains(value) {
                return Err(self.fail(path, "unknown enum value"));
            }
        }
        if let Some(kind) = object.get("type") {
            let matches = |kind: &str| match kind {
                "null" => value.is_null(),
                "boolean" => value.is_boolean(),
                "object" => value.is_object(),
                "array" => value.is_array(),
                "string" => value.is_string(),
                "number" => value.is_number(),
                "integer" => value
                    .as_number()
                    .is_some_and(|n| n.as_i64().is_some() || n.as_u64().is_some()),
                _ => false,
            };
            let valid = kind.as_str().is_some_and(matches)
                || kind
                    .as_array()
                    .is_some_and(|kinds| kinds.iter().filter_map(Value::as_str).any(matches));
            if !valid {
                return Err(self.fail(path, "type mismatch"));
            }
        }
        for keyword in ["allOf", "anyOf", "oneOf"] {
            if let Some(choices) = object.get(keyword).and_then(Value::as_array) {
                if keyword == "allOf" {
                    for choice in choices {
                        self.check(choice, value, path, depth + 1)?;
                    }
                } else {
                    let count = choices
                        .iter()
                        .filter(|choice| self.check(choice, value, path, depth + 1).is_ok())
                        .count();
                    if (keyword == "anyOf" && count == 0) || (keyword == "oneOf" && count != 1) {
                        return Err(self.fail(path, "schema alternative mismatch"));
                    }
                }
            }
        }
        if object
            .get("not")
            .is_some_and(|rule| self.check(rule, value, path, depth + 1).is_ok())
        {
            return Err(self.fail(path, "forbidden schema match"));
        }
        if let Some(condition) = object.get("if") {
            let branch = if self.check(condition, value, path, depth + 1).is_ok() {
                "then"
            } else {
                "else"
            };
            if let Some(rule) = object.get(branch) {
                self.check(rule, value, path, depth + 1)?;
            }
        }
        if let Some(text) = value.as_str() {
            let length = text.chars().count() as u64;
            if object
                .get("minLength")
                .and_then(Value::as_u64)
                .is_some_and(|min| length < min)
                || object
                    .get("maxLength")
                    .and_then(Value::as_u64)
                    .is_some_and(|max| length > max)
            {
                return Err(self.fail(path, "string length out of range"));
            }
            if let Some(pattern) = object.get("pattern").and_then(Value::as_str) {
                if !self
                    .schema
                    .patterns
                    .get(pattern)
                    .is_some_and(|regex| regex.is_match(text))
                {
                    return Err(self.fail(path, "string pattern mismatch"));
                }
            }
            if object.get("format").and_then(Value::as_str) == Some("date-time")
                && OffsetDateTime::parse(text, &Rfc3339).is_err()
            {
                return Err(self.fail(path, "invalid RFC 3339 date-time"));
            }
            if object
                .get("format")
                .is_some_and(|format| format.as_str() != Some("date-time"))
            {
                return Err(self.fail(path, "unsupported schema format"));
            }
        }
        if let Some(number) = value.as_number() {
            // Frozen numeric constraints are integer bounds. Preserve lexical integers instead
            // of round-tripping through f64; no 2^53 precision loss or exponent acceptance.
            let numeric = number.to_string().parse::<i128>();
            if let Some(multiple) = object.get("multipleOf") {
                let m = multiple
                    .as_i64()
                    .filter(|value| *value > 0)
                    .ok_or_else(|| self.fail(path, "unsupported multipleOf bound"))?;
                let n = numeric
                    .as_ref()
                    .map_err(|_| self.fail(path, "integer required by multipleOf"))?;
                if *n % i128::from(m) != 0 {
                    return Err(self.fail(path, "multipleOf mismatch"));
                }
            }
            for (keyword, lower, exclusive) in [
                ("minimum", true, false),
                ("maximum", false, false),
                ("exclusiveMinimum", true, true),
                ("exclusiveMaximum", false, true),
            ] {
                if let Some(bound) = object.get(keyword).and_then(Value::as_number) {
                    let n = numeric
                        .as_ref()
                        .map_err(|_| self.fail(path, "integer required by numeric bound"))?;
                    let b = bound
                        .to_string()
                        .parse::<i128>()
                        .map_err(|_| self.fail(path, "invalid frozen numeric bound"))?;
                    let outside = if lower { *n < b } else { *n > b };
                    if outside || exclusive && *n == b {
                        return Err(self.fail(path, "number out of range"));
                    }
                }
            }
        }
        if let Some(array) = value.as_array() {
            let length = array.len() as u64;
            if object
                .get("minItems")
                .and_then(Value::as_u64)
                .is_some_and(|min| length < min)
                || object
                    .get("maxItems")
                    .and_then(Value::as_u64)
                    .is_some_and(|max| length > max)
            {
                return Err(self.fail(path, "array length out of range"));
            }
            if object.get("uniqueItems").and_then(Value::as_bool) == Some(true) {
                let mut unique = BTreeSet::new();
                for item in array {
                    let key = serde_json::to_string(&sorted_value(item))
                        .map_err(|_| self.fail(path, "invalid unique item"))?;
                    if !unique.insert(key) {
                        return Err(self.fail(path, "duplicate array item"));
                    }
                }
            }
            if let Some(items) = object.get("items") {
                for (index, item) in array.iter().enumerate() {
                    self.check(items, item, &child(path, &index.to_string()), depth + 1)?;
                }
            }
            if object.get("contains").is_some_and(|contains| {
                !array
                    .iter()
                    .any(|item| self.check(contains, item, path, depth + 1).is_ok())
            }) {
                return Err(self.fail(path, "required array item missing"));
            }
        }
        if let Some(value) = value.as_object() {
            if let Some(required) = object.get("required").and_then(Value::as_array) {
                for key in required.iter().filter_map(Value::as_str) {
                    if !value.contains_key(key) {
                        return Err(self.fail(&child(path, key), "required property missing"));
                    }
                }
            }
            let length = value.len() as u64;
            if object
                .get("minProperties")
                .and_then(Value::as_u64)
                .is_some_and(|min| length < min)
                || object
                    .get("maxProperties")
                    .and_then(Value::as_u64)
                    .is_some_and(|max| length > max)
            {
                return Err(self.fail(path, "object size out of range"));
            }
            let properties = object.get("properties").and_then(Value::as_object);
            for (key, entry) in value {
                let item_path = child(path, key);
                if let Some(names) = object.get("propertyNames") {
                    self.check(names, &Value::String(key.clone()), &item_path, depth + 1)?;
                }
                if let Some(rule) = properties.and_then(|props| props.get(key)) {
                    self.check(rule, entry, &item_path, depth + 1)?;
                } else if let Some(extra) = object.get("additionalProperties") {
                    self.check(extra, entry, &item_path, depth + 1)?;
                }
            }
        }
        Ok(())
    }
}

const KEYWORDS: &[&str] = &[
    "$comment",
    "$id",
    "$ref",
    "$schema",
    "additionalProperties",
    "allOf",
    "anyOf",
    "const",
    "contains",
    "default",
    "definitions",
    "description",
    "else",
    "enum",
    "format",
    "if",
    "items",
    "maximum",
    "maxItems",
    "maxLength",
    "maxProperties",
    "minimum",
    "minItems",
    "minLength",
    "minProperties",
    "multipleOf",
    "not",
    "oneOf",
    "pattern",
    "properties",
    "propertyNames",
    "required",
    "then",
    "title",
    "type",
    "uniqueItems",
    "x-wire-limits",
    "exclusiveMinimum",
    "exclusiveMaximum",
];

fn audit_rule(rule: &Value) -> std::result::Result<(), String> {
    if rule.is_boolean() {
        return Ok(());
    }
    let object = rule.as_object().ok_or("invalid schema node")?;
    if let Some(keyword) = object.keys().find(|key| !KEYWORDS.contains(&key.as_str())) {
        return Err(format!("unsupported schema keyword {keyword}"));
    }
    if object
        .get("format")
        .is_some_and(|format| format.as_str() != Some("date-time"))
    {
        return Err("unsupported schema format".into());
    }
    for key in ["definitions", "properties"] {
        if let Some(entries) = object.get(key) {
            for node in entries.as_object().ok_or("invalid schema map")?.values() {
                audit_rule(node)?;
            }
        }
    }
    for key in ["allOf", "anyOf", "oneOf"] {
        if let Some(entries) = object.get(key) {
            for node in entries.as_array().ok_or("invalid schema alternatives")? {
                audit_rule(node)?;
            }
        }
    }
    for key in [
        "items",
        "additionalProperties",
        "propertyNames",
        "contains",
        "not",
        "if",
        "then",
        "else",
    ] {
        if let Some(node) = object.get(key) {
            audit_rule(node)?;
        }
    }
    Ok(())
}

// The uniqueItems key remains stable if another dependency enables preserve_order.
fn sorted_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let ordered: BTreeMap<_, _> = object.iter().collect();
            Value::Object(
                ordered
                    .into_iter()
                    .map(|(key, value)| (key.clone(), sorted_value(value)))
                    .collect(),
            )
        }
        Value::Array(array) => Value::Array(array.iter().map(sorted_value).collect()),
        other => other.clone(),
    }
}
