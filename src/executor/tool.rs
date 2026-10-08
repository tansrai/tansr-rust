use super::types::*;
use crate::{api::Result, canonical};
use serde_json::Value;
use std::collections::HashSet;

/// Business arguments preserve ordinary JSON numbers and Unicode object keys.
pub fn parse_tool_arguments(text: &str) -> Result<Value> {
    let v = canonical::parse_json(text.as_bytes(), 32_768)?;
    if !v.is_object() {
        return Err(invalid("tool arguments must be an object"));
    }
    fn numbers(v: &Value) -> bool {
        match v {
            Value::Number(n) => n.as_f64().is_some_and(f64::is_finite),
            Value::Array(a) => a.iter().all(numbers),
            Value::Object(o) => o.values().all(numbers),
            _ => true,
        }
    }
    if !numbers(&v) {
        return Err(invalid("business number outside finite range"));
    }
    Ok(v)
}
pub(crate) fn verify_result(text: &str) -> Result<()> {
    let v = parse_tool_arguments(text)?;
    if v["status"] == "error" {
        if v["message"]
            .as_str()
            .is_some_and(|s| !s.is_empty() && s.encode_utf16().count() <= 4096)
        {
            return Ok(());
        }
        return Err(invalid("invalid business error"));
    }
    if v["status"] != "ok" || v.get("isError").is_some_and(|v| !v.is_boolean()) {
        return Err(invalid("invalid tool result"));
    }
    let a = v["content"]
        .as_array()
        .ok_or_else(|| invalid("missing content"))?;
    if a.is_empty() || a.len() > 64 {
        return Err(invalid("content limit"));
    }
    for i in a {
        let valid = match i["t"].as_str() {
            Some("text") => i["text"].is_string(),
            Some("image") => {
                matches!(
                    i["mime"].as_str(),
                    Some("image/png" | "image/jpeg" | "image/webp" | "image/gif")
                ) && i["data"].is_string()
            }
            _ => false,
        };
        if !valid {
            return Err(invalid("invalid content entry"));
        }
    }
    Ok(())
}
/// Digest of an exact ClientToolDecl, with legacy JavaScript index-key ordering.
/// This intentionally supports printable ASCII keys and safe unsigned integers only.
/// It inserts no optional defaults and is not the control canonical digest.
pub fn definition_digest(v: &Value) -> Result<String> {
    canonical::encode_limited(v, CONTROL_BYTES)?;
    let o = v
        .as_object()
        .ok_or_else(|| invalid("tool declaration must be object"))?;
    if o.keys().any(|k| {
        !matches!(
            k.as_str(),
            "name" | "description" | "parameters" | "readOnly" | "effects" | "timeoutMs"
        )
    }) {
        return Err(invalid("unknown declaration key"));
    }
    let n = v["name"].as_str().ok_or_else(|| invalid("tool name"))?;
    if n.is_empty()
        || n.len() > 64
        || !n.as_bytes()[0].is_ascii_alphabetic()
        || !n.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(invalid("tool name"));
    }
    if !v["description"]
        .as_str()
        .is_some_and(|s| !s.is_empty() && s.encode_utf16().count() <= 2048)
    {
        return Err(invalid("tool description"));
    }
    if o.get("readOnly").is_some_and(|v| !v.is_boolean())
        || o.get("timeoutMs")
            .is_some_and(|v| !v.as_u64().is_some_and(|n| (1000..=600000).contains(&n)))
    {
        return Err(invalid("declaration control field"));
    }
    if let Some(e) = o.get("effects") {
        let a = e.as_array().ok_or_else(|| invalid("effects"))?;
        let mut seen = HashSet::new();
        if a.len() > 4
            || a.iter().any(|v| {
                !matches!(
                    v.as_str(),
                    Some("irreversible" | "financial" | "external" | "affects-others")
                ) || !seen.insert(v.as_str())
            })
        {
            return Err(invalid("effects"));
        }
    }
    if let Some(p) = o.get("parameters") {
        let params = p.as_object().ok_or_else(|| invalid("parameters"))?;
        if legacy_json(p)?.len() > 32768 {
            return Err(invalid("parameters byte limit"));
        }
        for spec in params.values() {
            parameter(spec, 1)?;
        }
    }
    canonical::digest_bytes("tansr.sdk2.client-tool.v1", &legacy_json(v)?)
}
fn parameter(v: &Value, depth: usize) -> Result<()> {
    let o = v.as_object().ok_or_else(|| invalid("parameter"))?;
    if depth > 8
        || o.keys().any(|k| {
            !matches!(
                k.as_str(),
                "type" | "description" | "optional" | "items" | "properties"
            )
        })
        || !matches!(
            v["type"].as_str(),
            Some("string" | "number" | "boolean" | "array" | "object")
        )
    {
        return Err(invalid("parameter shape"));
    }
    if o.get("description")
        .is_some_and(|v| v.as_str().is_none_or(|s| s.encode_utf16().count() > 2048))
        || o.get("optional").is_some_and(|v| !v.is_boolean())
    {
        return Err(invalid("parameter field"));
    }
    if let Some(v) = o.get("items") {
        parameter(v, depth + 1)?;
    }
    if let Some(v) = o.get("properties") {
        for p in v.as_object().ok_or_else(|| invalid("properties"))?.values() {
            parameter(p, depth + 1)?;
        }
    }
    Ok(())
}
pub(crate) fn legacy_json(v: &Value) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    match v {
        Value::Object(o) => {
            fn index(k: &str) -> Option<u32> {
                k.parse::<u32>()
                    .ok()
                    .filter(|n| *n < u32::MAX && n.to_string() == k)
            }
            let mut keys: Vec<_> = o.keys().collect();
            keys.sort_by(|a, b| match (index(a), index(b)) {
                (Some(a), Some(b)) => a.cmp(&b),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => a.cmp(b),
            });
            out.push(b'{');
            for (i, k) in keys.iter().enumerate() {
                if k.as_str() == "__proto__" {
                    return Err(invalid("prototype key"));
                }
                if i > 0 {
                    out.push(b',');
                }
                out.extend(canonical::encode(&Value::String((*k).clone()))?);
                out.push(b':');
                out.extend(legacy_json(&o[*k])?);
            }
            out.push(b'}');
        }
        Value::Array(a) => {
            out.push(b'[');
            for (i, v) in a.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                out.extend(legacy_json(v)?);
            }
            out.push(b']');
        }
        _ => out = canonical::encode(v)?,
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn independent_node_utf8_bytes_and_absent_defaults() {
        // Independently obtained from Node's JSON.stringify(sortKeys(decl));
        // JS array-index ordering and non-BMP UTF-8 are deliberately visible.
        let mut declaration = json!({"name":"UnicodeLookup","description":"订单 😀 𝄞 e\u{301} é \u{2028}\u{2029}","parameters":{"orderId":{"type":"string","description":"订单 😀"},"10":{"type":"number"},"2":{"type":"string"}}});
        let expected = "{\"description\":\"订单 😀 𝄞 e\u{301} é \u{2028}\u{2029}\",\"name\":\"UnicodeLookup\",\"parameters\":{\"2\":{\"type\":\"string\"},\"10\":{\"type\":\"number\"},\"orderId\":{\"description\":\"订单 😀\",\"type\":\"string\"}}}";
        assert_eq!(legacy_json(&declaration).unwrap(), expected.as_bytes());
        assert_eq!(
            definition_digest(&declaration).unwrap(),
            "9502c0d7cd61033b20b49f2de253a236041c8519d044288b10e8d5f8f7538ba4"
        );
        declaration["readOnly"] = json!(false);
        assert_eq!(
            legacy_json(&declaration).unwrap(),
            format!("{},\"readOnly\":false}}", &expected[..expected.len() - 1]).as_bytes()
        );
        assert_eq!(
            definition_digest(&declaration).unwrap(),
            "76515ae630a3db646d0ae7dc798dc38d8fd01d0c996fbccfb952fcf537a9b7a8"
        );
        declaration["effects"] = json!([]);
        declaration["timeoutMs"] = json!(1000);
        assert_eq!(
            definition_digest(&declaration).unwrap(),
            "30288bd7f6e8c68f30861984bf7d069b10ba1bab0eb52fc5b01bc8e2b35dc54a"
        );
    }
}
