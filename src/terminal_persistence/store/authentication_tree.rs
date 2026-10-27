//! Shared authenticated Patricia encoding; a missing node is never absence.
use super::*;

#[derive(Clone)]
pub(super) struct Node {
    key: String,
    value: Option<String>,
    bit: usize,
    left: String,
    right: String,
}
pub(super) fn digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn nibble(c: u8) -> u8 {
    if c <= b'9' { c - b'0' } else { c - b'a' + 10 }
}
fn difference(a: &str, b: &str) -> usize {
    for (i, (a, b)) in a.bytes().zip(b.bytes()).enumerate() {
        let d = nibble(a) ^ nibble(b);
        if d != 0 {
            return i * 4 + (d.leading_zeros() as usize - 4);
        }
    }
    256
}
fn right(key: &str, bit: usize) -> bool {
    nibble(key.as_bytes()[bit / 4]) & (8 >> (bit % 4)) != 0
}
fn node(
    read: &impl Fn(&str) -> Result<Vec<u8>>,
    hash_key: &str,
    parent: Option<(&Node, bool)>,
    path: &mut BTreeSet<String>,
) -> Result<Node> {
    if !digest(hash_key) || path.len() >= 257 || !path.insert(hash_key.into()) {
        return Err(integrity());
    }
    let bytes = read(hash_key)?;
    if bytes.len() > 512 || hash(&bytes) != hash_key {
        return Err(integrity());
    }
    let v = crate::canonical::parse_strict(&bytes, 512)?;
    let key = string(&v["key"])?;
    if !digest(key) {
        return Err(integrity());
    }
    let n = match v["kind"].as_str() {
        Some("leaf")
            if v.as_object().is_some_and(|o| o.len() == 3)
                && v["value"].as_str().is_some_and(digest) =>
        {
            Node {
                key: key.into(),
                value: Some(string(&v["value"])?.into()),
                bit: 256,
                left: String::new(),
                right: String::new(),
            }
        }
        Some("branch")
            if v.as_object().is_some_and(|o| o.len() == 5)
                && v["bit"].as_u64().is_some_and(|b| b < 256)
                && v["left"].as_str().is_some_and(digest)
                && v["right"].as_str().is_some_and(digest)
                && v["left"] != v["right"] =>
        {
            Node {
                key: key.into(),
                value: None,
                bit: num(&v, "bit"),
                left: string(&v["left"])?.into(),
                right: string(&v["right"])?.into(),
            }
        }
        _ => return Err(integrity()),
    };
    if let Some((p, r)) = parent {
        if n.bit <= p.bit || difference(&n.key, &p.key) < p.bit || right(&n.key, p.bit) != r {
            return Err(integrity());
        }
    }
    Ok(n)
}
fn emit(n: &Node, write: &mut impl FnMut(String, Vec<u8>) -> Result<()>) -> Result<String> {
    let v = match &n.value {
        Some(v) => json!({"kind":"leaf","key":n.key,"value":v}),
        None => json!({"kind":"branch","key":n.key,"bit":n.bit,"left":n.left,"right":n.right}),
    };
    let bytes = crate::canonical::encode(&v)?;
    let h = hash(&bytes);
    write(h.clone(), bytes)?;
    Ok(h)
}
pub(super) fn get(
    root: Option<&str>,
    key: &str,
    read: impl Fn(&str) -> Result<Vec<u8>>,
) -> Result<Option<String>> {
    if !digest(key) {
        return Err(integrity());
    }
    let Some(root) = root else { return Ok(None) };
    let mut at = root.to_owned();
    let mut path = BTreeSet::new();
    let mut parent = None;
    let mut direction = false;
    loop {
        let n = node(
            &read,
            &at,
            parent.as_ref().map(|p| (p, direction)),
            &mut path,
        )?;
        if n.value.is_some() {
            return Ok(if n.key == key { n.value } else { None });
        }
        if difference(&n.key, key) < n.bit {
            return Ok(None);
        }
        direction = right(key, n.bit);
        at = if direction {
            n.right.clone()
        } else {
            n.left.clone()
        };
        parent = Some(n);
    }
}
pub(super) fn put(
    root: Option<&str>,
    key: &str,
    value: &str,
    read: impl Fn(&str) -> Result<Vec<u8>>,
    mut write: impl FnMut(String, Vec<u8>) -> Result<()>,
) -> Result<String> {
    if !digest(key) || !digest(value) {
        return Err(integrity());
    }
    let leaf = Node {
        key: key.into(),
        value: Some(value.into()),
        bit: 256,
        left: String::new(),
        right: String::new(),
    };
    let Some(root) = root else {
        return emit(&leaf, &mut write);
    };
    let mut at = root.to_owned();
    let mut path = BTreeSet::new();
    let mut parent = None;
    let mut direction = false;
    let mut steps = vec![];
    let (mut result, mut representative) = loop {
        let n = node(
            &read,
            &at,
            parent.as_ref().map(|p| (p, direction)),
            &mut path,
        )?;
        let bit = difference(&n.key, key);
        if n.value.is_some() && bit == 256 {
            if n.value.as_deref() == Some(value) {
                return Ok(root.into());
            }
            break (emit(&leaf, &mut write)?, leaf.key.clone());
        }
        if n.value.is_none() && bit >= n.bit {
            direction = right(key, n.bit);
            at = if direction {
                n.right.clone()
            } else {
                n.left.clone()
            };
            steps.push((n.clone(), direction));
            parent = Some(n);
            continue;
        }
        let h = emit(&leaf, &mut write)?;
        let b = if right(key, bit) {
            Node {
                key: n.key.clone(),
                value: None,
                bit,
                left: at,
                right: h,
            }
        } else {
            Node {
                key: key.into(),
                value: None,
                bit,
                left: h,
                right: at,
            }
        };
        break (emit(&b, &mut write)?, b.key);
    };
    for (n, r) in steps.into_iter().rev() {
        let b = if r {
            Node {
                key: n.key,
                value: None,
                bit: n.bit,
                left: n.left,
                right: result,
            }
        } else {
            Node {
                key: representative,
                value: None,
                bit: n.bit,
                left: result,
                right: n.right,
            }
        };
        result = emit(&b, &mut write)?;
        representative = b.key;
    }
    Ok(result)
}
pub(super) fn visit(
    root: Option<&str>,
    read: impl Fn(&str) -> Result<Vec<u8>>,
    mut work: impl FnMut(&str, &str) -> Result<()>,
) -> Result<()> {
    fn walk(
        h: &str,
        p: Option<(&Node, bool)>,
        path: &mut BTreeSet<String>,
        read: &impl Fn(&str) -> Result<Vec<u8>>,
        work: &mut impl FnMut(&str, &str) -> Result<()>,
    ) -> Result<()> {
        let n = node(read, h, p, path)?;
        if let Some(v) = &n.value {
            work(&n.key, v)?;
        } else {
            walk(&n.left, Some((&n, false)), path, read, work)?;
            walk(&n.right, Some((&n, true)), path, read, work)?;
        }
        path.remove(h);
        Ok(())
    }
    if let Some(r) = root {
        walk(r, None, &mut BTreeSet::new(), &read, &mut work)?;
    }
    Ok(())
}
