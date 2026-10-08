use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fmt::Write;
use tansr_sdk::canonical::{self, MAX_NODES};

fn fixture(name: &str) -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("contract")
        .join(name);
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn vector_input(vector: &Value) -> Vec<u8> {
    if let Some(generator) = vector.get("generator") {
        assert_eq!(generator["kind"], "flat-array");
        let element = generator["element"].as_str().unwrap();
        let count = generator["count"].as_u64().unwrap() as usize;
        let mut elements = vec![element; count];
        if let Some(tail) = generator["tail"].as_str().filter(|s| !s.is_empty()) {
            elements.push(tail);
        }
        return format!("[{}]", elements.join(",")).into_bytes();
    }
    let input = vector["input"].as_str().unwrap();
    match vector["inputKind"].as_str().unwrap() {
        "utf8-text" => input.as_bytes().to_vec(),
        "bytes" => STANDARD.decode(input).unwrap(),
        kind => panic!("unknown vector input {kind}"),
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

#[test]
fn all_127_frozen_cross_vectors() {
    let data = fixture("canonical-cross-vectors.json");
    let vectors = data["vectors"].as_array().unwrap();
    assert_eq!(
        vectors.len(),
        127,
        "changing the frozen matrix is not a repair"
    );
    for vector in vectors {
        let id = vector["id"].as_str().unwrap();
        let bytes = vector_input(vector);
        let max = vector["maxBytes"].as_u64().unwrap_or(262_144) as usize;
        let decoded = canonical::decode(&bytes, max);
        let strict = canonical::parse_strict(&bytes, max);
        if vector["expect"] == "reject" {
            assert!(decoded.is_err(), "decode accepted reject vector {id}");
            assert!(strict.is_err(), "strict accepted reject vector {id}");
            continue;
        }
        let decoded = decoded.unwrap_or_else(|e| panic!("decode {id}: {e}"));
        let encoded =
            canonical::encode_limited(&decoded, max).unwrap_or_else(|e| panic!("encode {id}: {e}"));
        if let Some(expected) = vector["canonicalHex"].as_str() {
            assert_eq!(hex(&encoded), expected, "canonical bytes {id}");
        } else {
            assert_eq!(
                format!("{:x}", Sha256::digest(&encoded)),
                vector["canonicalSha256"],
                "canonical hash {id}"
            );
        }
        assert_eq!(strict.is_ok(), encoded == bytes, "canonical exactness {id}");
        if let Ok(strict) = strict {
            assert_eq!(
                canonical::encode_limited(&strict, max).unwrap(),
                encoded,
                "strict roundtrip {id}"
            );
        }
    }
}

#[test]
fn frozen_wire_metadata_and_path_segments() {
    let data = fixture("sdk2-wire-v1.json");
    for sample in data["metadata"].as_array().unwrap() {
        let expected = sample["utf8"].as_str().unwrap().as_bytes();
        assert_eq!(canonical::encode(&sample["value"]).unwrap(), expected);
        assert_eq!(
            canonical::encode(&canonical::parse_strict(expected, 262_144).unwrap()).unwrap(),
            expected
        );
    }
    for sample in data["invalidMetadata"].as_array().unwrap() {
        let bytes = sample.as_str().unwrap().as_bytes();
        assert!(canonical::decode(bytes, 262_144).is_err());
        assert!(canonical::parse_strict(bytes, 262_144).is_err());
    }
    for sample in data["pathIds"].as_array().unwrap() {
        assert_eq!(
            canonical::encode_path_segment(sample["id"].as_str().unwrap()),
            sample["segment"]
        );
    }
    assert_eq!(
        canonical::encode_path_segment("!~*'()-_.AZ09"),
        "!~*'()-_.AZ09"
    );
    assert_eq!(
        canonical::encode_path_segment("?&#= +/%"),
        "%3F%26%23%3D%20%2B%2F%25"
    );
    let frame = data["sse"]["utf8"].as_str().unwrap();
    let (_, body) = frame.split_once("data: ").unwrap();
    assert_eq!(
        canonical::encode(&data["sse"]["frame"]).unwrap(),
        body.trim_end_matches('\n').as_bytes()
    );
}

#[test]
fn frozen_closure_digest_and_domain_separation() {
    let data = fixture("unified-v1.golden.json");
    let mut checked = 0;
    for vector in data["vectors"].as_array().unwrap() {
        if vector["definition"] != "CapabilityClosure" || vector["expect"] != "valid" {
            continue;
        }
        let value = &vector["value"];
        let body = json!({ "authorizationRevision": value["authorizationRevision"], "domains": value["domains"], "operations": value["operations"] });
        assert_eq!(
            canonical::digest(canonical::DOMAIN_CLOSURE, &body).unwrap(),
            value["closureId"]
        );
        checked += 1;
    }
    assert!(checked >= 2);
    assert_ne!(
        canonical::digest("a", &json!({})).unwrap(),
        canonical::digest("b", &json!({})).unwrap()
    );
    assert!(canonical::digest("", &json!({})).is_err());
    assert!(canonical::digest("a\0b", &json!({})).is_err());
    assert_ne!(
        canonical::digest_bytes("archive", b"{\"a\":1}").unwrap(),
        canonical::digest_bytes("archive", b"{ \"a\":1}").unwrap()
    );
}

#[test]
fn ordinary_json_keeps_number_lexemes_and_unicode_without_control_restrictions() {
    let input = br#"{"negativeZero":-0,"decimal":1.0,"exponent":1e0,"capital":1E+02,"huge":9007199254740993,"negative":-2.5}"#;
    let value = canonical::parse_json(input, 1024).unwrap();
    for (field, token) in [
        ("negativeZero", "-0"),
        ("decimal", "1.0"),
        ("exponent", "1e0"),
        ("capital", "1E+02"),
        ("huge", "9007199254740993"),
        ("negative", "-2.5"),
    ] {
        assert_eq!(value[field].as_number().unwrap().to_string(), token);
        assert!(
            canonical::encode(&value[field]).is_err(),
            "control accepted {token}"
        );
    }
    let value = canonical::parse_json("{\"汉\":1.2,\"\":-2}".as_bytes(), 100).unwrap();
    assert_eq!(value["汉"].to_string(), "1.2");
    assert!(canonical::encode(&value).is_err());
    // serde's internal arbitrary-precision marker must remain an ordinary object key.
    let input = br#"{"$serde_json::private::Number":"123"}"#;
    assert!(canonical::parse_json(input, 1024).unwrap().is_object());
}

#[test]
fn ordinary_json_rejects_lexical_ambiguity_before_any_value_conversion() {
    for input in [
        br#"{"a":1,"a":2}"#.as_slice(),
        br#"{"a":1,"\u0061":2}"#,
        br#"{"a":{"x":1,"x":2}}"#,
        br#"{"s":"\ud800"}"#,
        br#""\udc00""#,
        b"{} {}",
        b"[01]",
        b"[-01]",
        b"[1.]",
        b"[1e]",
        b"[1e+]",
        b"[+1]",
        b"[.5]",
        b"[NaN]",
        b"[Infinity]",
        b"[1,]",
        b"{\"a\":1,}",
        b"\xef\xbb\xbf{}",
        b"\"\xff\"",
        b"\"\xc0\xaf\"",
        b"\"\xed\xa0\x80\"",
        b"truex",
        b"",
        b"\"raw\nline\"",
    ] {
        assert!(
            canonical::parse_json(input, 1024).is_err(),
            "accepted {input:?}"
        );
    }
    assert_eq!(
        canonical::parse_json(br#""\ud83d\ude00""#, 100).unwrap(),
        "😀"
    );
    assert!(
        canonical::parse_json(br#"{"a":1,"secret":2,"a":3}"#, 100)
            .unwrap_err()
            .to_string()
            .contains("duplicate_key")
    );
}

#[test]
fn byte_depth_and_node_limits_are_exact_and_shared_by_writer() {
    assert!(canonical::parse_json(b"null", 0).is_err());
    assert!(canonical::parse_json(b"null", 3).is_err());
    assert!(canonical::parse_json(b"null", 4).is_ok());
    assert!(canonical::encode_limited(&Value::Null, 3).is_err());
    assert_eq!(canonical::encode_limited(&Value::Null, 4).unwrap(), b"null");
    let value = json!({"a": "日本"});
    assert!(canonical::encode_limited(&value, 13).is_err());
    assert_eq!(canonical::encode_limited(&value, 14).unwrap().len(), 14);
    let mut value = json!(0);
    for _ in 0..32 {
        value = Value::Array(vec![value]);
    }
    assert!(canonical::encode(&value).is_ok());
    value = Value::Array(vec![value]);
    assert!(canonical::encode(&value).is_err());
    let deep = format!("{}0{}", "[".repeat(33), "]".repeat(33));
    assert!(canonical::parse_json(deep.as_bytes(), 100).is_err());
    let exact = Value::Array(vec![json!(0); MAX_NODES - 1]);
    let bytes = canonical::encode(&exact).unwrap();
    assert!(canonical::parse_json(&bytes, 262_144).is_ok());
    let over = Value::Array(vec![json!(0); MAX_NODES]);
    assert!(canonical::encode(&over).is_err());
    let bytes = serde_json::to_vec(&over).unwrap();
    assert!(canonical::parse_json(&bytes, 262_144).is_err());
}
