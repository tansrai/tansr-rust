use serde_json::{Value, json};
use tansr_sdk::api::schema::{validate, validate_family};

fn materialize(vector: &Value, vectors: &[Value]) -> Value {
    if let Some(file) = vector["valueFile"].as_str() {
        assert_eq!(file, "packages/server/contract/api-manifest.json");
        return serde_json::from_str(include_str!("../contract/api-manifest.json")).unwrap();
    }
    if let Some(base) = vector["base"].as_str() {
        let mut value = materialize(
            vectors
                .iter()
                .find(|vector| vector["name"] == base)
                .unwrap(),
            vectors,
        );
        for patch in vector["patch"].as_array().unwrap() {
            apply_patch(&mut value, patch);
        }
        return value;
    }
    vector["value"].clone()
}

fn apply_patch(root: &mut Value, patch: &Value) {
    let pointer = patch["path"].as_str().unwrap();
    let operation = patch["op"].as_str().unwrap();
    if pointer.is_empty() {
        *root = patch["value"].clone();
        return;
    }
    let (parent, key) = pointer.rsplit_once('/').unwrap();
    let key = key.replace("~1", "/").replace("~0", "~");
    let target = root.pointer_mut(parent).unwrap();
    match target {
        Value::Object(object) => match operation {
            "remove" => {
                assert!(object.remove(&key).is_some());
            }
            "add" => {
                object.insert(key, patch["value"].clone());
            }
            "replace" => {
                assert!(object.contains_key(&key));
                object.insert(key, patch["value"].clone());
            }
            _ => panic!("unknown patch operation"),
        },
        Value::Array(array) => {
            let index = if key == "-" {
                array.len()
            } else {
                key.parse().unwrap()
            };
            match operation {
                "remove" => {
                    array.remove(index);
                }
                "add" => array.insert(index, patch["value"].clone()),
                "replace" => array[index] = patch["value"].clone(),
                _ => panic!("unknown patch operation"),
            }
        }
        _ => panic!("patch parent is not a container"),
    }
}

#[test]
fn unified_165_frozen_vectors() {
    let golden: Value =
        serde_json::from_str(include_str!("../contract/unified-v1.golden.json")).unwrap();
    let vectors = golden["vectors"].as_array().unwrap();
    assert_eq!(vectors.len(), 165);
    let mut valid = 0;
    for vector in vectors {
        let value = materialize(vector, vectors);
        let expected = vector["expect"] == "valid";
        let result = validate(vector["definition"].as_str().unwrap(), &value);
        assert_eq!(result.is_ok(), expected, "{}: {result:?}", vector["name"]);
        valid += usize::from(expected);
    }
    assert_eq!(valid, 41);
}

#[test]
fn all_available_terminal_structure_goldens() {
    for (family, source) in [
        (
            "terminal-services-v1",
            include_str!("../contract/terminal-services-v1.golden.json"),
        ),
        (
            "terminal-observation-v1",
            include_str!("../contract/terminal-observation-v1.golden.json"),
        ),
        (
            "terminal-profile-v1",
            include_str!("../contract/terminal-profile-v1.golden.json"),
        ),
    ] {
        let golden: Value = serde_json::from_str(source).unwrap();
        for (key, expected) in [("positive", true), ("negative", false)] {
            for vector in golden[key].as_array().unwrap() {
                let result = validate_family(
                    family,
                    vector["definition"].as_str().unwrap(),
                    &vector["value"],
                );
                assert_eq!(
                    result.is_ok(),
                    expected,
                    "{family} {}: {result:?}",
                    vector["id"]
                );
            }
        }
    }
}

#[test]
fn every_frozen_schema_loads_and_unknown_names_fail_closed() {
    for family in [
        "unified-v1",
        "sdk2-ext-v1",
        "sdk2-archive-recovery-v1",
        "archive-sync-v1",
        "sdk2-cache-v1",
        "sdk2-cache-core-v1",
        "terminal-services-v1",
        "terminal-observation-v1",
        "terminal-profile-v1",
        "terminal-shell-sandbox-v1",
    ] {
        let error = validate_family(family, "DoesNotExist", &Value::Null).unwrap_err();
        assert!(
            error.to_string().contains("unknown schema definition"),
            "{family}: {error}"
        );
    }
    assert!(validate_family("agent-session-v1", "Id", &json!("a")).is_err());
    assert!(validate("Sequence", &json!("9223372036854775808")).is_err());
    assert!(validate("Sequence", &json!("9223372036854775807")).is_ok());
    assert!(validate("Sequence", &json!("9007199254740993")).is_ok());
    assert!(validate("Sequence", &json!(9007199254740993_u64)).is_err());
    assert!(validate("Sequence", &json!("01")).is_err());
}

#[test]
fn nullable_required_is_not_optional_and_d18_raw_is_an_object() {
    let mut event = json!({"contract":"unified-v1","eventId":null,"domain":"session","type":null,"cursorSet":{},"terminalStatus":null,"raw":{}});
    // Use the frozen fixture for the complete cursor-set vocabulary.
    let golden: Value =
        serde_json::from_str(include_str!("../contract/unified-v1.golden.json")).unwrap();
    let vectors = golden["vectors"].as_array().unwrap();
    let vector = vectors
        .iter()
        .find(|vector| vector["definition"] == "EventEnvelope" && vector["expect"] == "valid")
        .unwrap();
    event["cursorSet"] = materialize(vector, vectors)["cursorSet"].clone();
    validate("EventEnvelope", &event).unwrap();
    serde_json::from_value::<tansr_sdk::api::EventEnvelope>(event.clone()).unwrap();
    event.as_object_mut().unwrap().remove("eventId");
    assert!(validate("EventEnvelope", &event).is_err());
    assert!(serde_json::from_value::<tansr_sdk::api::EventEnvelope>(event.clone()).is_err());
    event["eventId"] = Value::Null;
    event["raw"] = json!([]);
    assert!(validate("EventEnvelope", &event).is_err());
    assert!(serde_json::from_value::<tansr_sdk::api::EventEnvelope>(event.clone()).is_err());
    event["raw"] = json!({});
    event["payload"] = json!({});
    assert!(validate("EventEnvelope", &event).is_err());
}
