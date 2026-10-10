//! JSON helpers.
//!
//! Small read and write helpers over [`serde_json::Value`], plus the nesting guard
//! that runs before a message is parsed.

use serde_json::{Map, Value};

/// Nesting depth beyond which a message is refused before parsing.
///
/// The parser is recursive, so this bound is what keeps a crafted message from
/// exhausting the stack.
pub const MAX_DEPTH: usize = 128;

/// An empty JSON object.
pub fn obj() -> Value {
    Value::Object(Map::new())
}

/// An empty JSON array.
pub fn arr() -> Value {
    Value::Array(Vec::new())
}

/// Compact serialisation, with no insignificant whitespace.
pub fn serialize(v: &Value) -> String {
    v.to_string()
}

/// Parse `raw`, refusing anything nested deeper than [`MAX_DEPTH`].
///
/// Returns `None` for over-nested input and for invalid JSON alike.
pub fn parse_into(raw: &str) -> Option<Value> {
    if !within_depth(raw, MAX_DEPTH) {
        return None;
    }
    serde_json::from_str::<Value>(raw).ok()
}

/// Whether `text` nests no deeper than `max_depth` levels.
///
/// This is a cheap structural scan rather than a parse: it counts brackets and
/// braces outside of string literals. It deliberately does not validate the JSON,
/// only the depth, so a message that is too deep is rejected before it reaches the
/// recursive parser.
///
/// The counter is signed: a stray closing bracket drives it below zero rather than
/// being clamped, so a prefix of closers cannot mask the depth of what follows.
pub fn within_depth(text: &str, max_depth: usize) -> bool {
    let limit = max_depth as i64;
    let bytes = text.as_bytes();
    let mut depth: i64 = 0;
    let mut in_string = false;
    let mut escaped = false;
    for &b in bytes {
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
        } else if b == b'"' {
            in_string = true;
        } else if b == b'[' || b == b'{' {
            depth += 1;
            if depth > limit {
                return false;
            }
        } else if b == b']' || b == b'}' {
            depth -= 1;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

/// Insert a string member. Does nothing if `o` is not an object.
pub fn add_str(o: &mut Value, k: &str, s: &str) {
    if let Value::Object(m) = o {
        m.insert(k.to_string(), Value::String(s.to_string()));
    }
}

/// Insert an integer member. Does nothing if `o` is not an object.
pub fn add_int(o: &mut Value, k: &str, n: i64) {
    if let Value::Object(m) = o {
        m.insert(k.to_string(), Value::Number(n.into()));
    }
}

/// Insert a boolean member. Does nothing if `o` is not an object.
pub fn add_bool(o: &mut Value, k: &str, b: bool) {
    if let Value::Object(m) = o {
        m.insert(k.to_string(), Value::Bool(b));
    }
}

/// Insert an owned value under `k`. Does nothing if `o` is not an object.
pub fn add_val(o: &mut Value, k: &str, v: Value) {
    if let Value::Object(m) = o {
        m.insert(k.to_string(), v);
    }
}

/// Insert an owned value under a runtime key. Does nothing if `o` is not an object.
pub fn add_key_val(o: &mut Value, key: &str, v: Value) {
    add_val(o, key, v);
}

/// Append a string to an array. Does nothing if `arr` is not an array.
pub fn push_str(arr: &mut Value, s: &str) {
    if let Value::Array(a) = arr {
        a.push(Value::String(s.to_string()));
    }
}

/// Append an owned value to an array. Does nothing if `arr` is not an array.
pub fn push(arr: &mut Value, v: Value) {
    if let Value::Array(a) = arr {
        a.push(v);
    }
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

/// Loose readers for a `params` object.
///
/// Every reader treats a missing key, a `null` and a wrongly typed value the same
/// way: it falls back to the caller's default. A container that is not an object
/// reads as if it had no members at all.
pub mod args {
    use super::*;

    fn member<'a>(v: &'a Value, k: &str) -> Option<&'a Value> {
        match v {
            Value::Object(m) => m.get(k),
            _ => None,
        }
    }

    /// Whether `key` is present.
    pub fn has(v: &Value, key: &str) -> bool {
        member(v, key).is_some()
    }

    /// The value at `key`, if it is present.
    pub fn get<'a>(v: &'a Value, key: &str) -> Option<&'a Value> {
        member(v, key)
    }

    /// The string at `key`, or `def` when it is missing or not a string.
    pub fn str(v: &Value, key: &str, def: &str) -> String {
        match member(v, key) {
            Some(Value::String(s)) => s.clone(),
            _ => def.to_string(),
        }
    }

    /// The integer at `key`, or `def`.
    ///
    /// Accepts any JSON number; a value with a fractional part is truncated, and an
    /// unsigned value too large for `i64` wraps the same way an `as` cast does.
    pub fn integer(v: &Value, key: &str, def: i64) -> i64 {
        let m = match member(v, key) {
            Some(m) => m,
            None => return def,
        };
        if let Some(i) = m.as_i64() {
            return i;
        }
        if let Some(u) = m.as_u64() {
            return u as i64;
        }
        if let Some(f) = m.as_f64() {
            return f as i64;
        }
        def
    }

    /// The number at `key`, or `def`.
    pub fn number(v: &Value, key: &str, def: f64) -> f64 {
        match member(v, key) {
            Some(m) if m.is_number() => m.as_f64().unwrap_or(def),
            _ => def,
        }
    }

    /// The boolean at `key`, or `def`.
    pub fn boolean(v: &Value, key: &str, def: bool) -> bool {
        match member(v, key) {
            Some(Value::Bool(b)) => *b,
            _ => def,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn depth_guard_counts_brackets_outside_strings() {
        assert!(within_depth("{\"a\":1}", MAX_DEPTH));
        assert!(within_depth("[[[[]]]]", MAX_DEPTH));
        assert!(!within_depth(&"[".repeat(MAX_DEPTH + 1), MAX_DEPTH));
        assert!(within_depth(&"[".repeat(MAX_DEPTH), MAX_DEPTH));
        // brackets inside a string are inert
        assert!(within_depth(
            &format!("\"{}\"", "[".repeat(1000)),
            MAX_DEPTH
        ));
        // escaped quotes do not close the string
        assert!(within_depth(
            &format!("\"\\\"{}\"", "[".repeat(1000)),
            MAX_DEPTH
        ));
        // an unbalanced tail is not a depth failure; the parser reports it instead
        assert!(within_depth("]]]]", MAX_DEPTH));
        // ... and it lowers the count, so it buys that many extra levels
        let mixed = format!("{}{}", "]".repeat(4), "[".repeat(MAX_DEPTH + 3));
        assert!(within_depth(&mixed, MAX_DEPTH));
        let too_deep = format!("{}{}", "]".repeat(4), "[".repeat(MAX_DEPTH + 5));
        assert!(!within_depth(&too_deep, MAX_DEPTH));
    }

    #[test]
    fn loose_readers_fall_back_to_defaults() {
        let v =
            json!({"s":"x","i":42,"big":9007199254740993u64,"f":1.5,"b":true,"nil":null,"arr":[1]});
        assert!(args::has(&v, "s"));
        assert!(!args::has(&v, "nope"));
        assert_eq!(args::str(&v, "s", ""), "x");
        assert_eq!(args::str(&v, "i", "dflt"), "dflt");
        assert_eq!(args::integer(&v, "i", 0), 42);
        assert_eq!(args::integer(&v, "f", 0), 1);
        assert_eq!(args::integer(&v, "big", -1), 9007199254740993);
        assert_eq!(args::integer(&v, "nope", 7), 7);
        assert_eq!(args::number(&v, "f", 0.0), 1.5);
        assert_eq!(args::number(&v, "i", 0.0), 42.0);
        assert!(args::boolean(&v, "b", false));
        assert!(!args::boolean(&v, "s", false));
        // a container that is not an object reads as absent
        assert_eq!(args::str(&json!(5), "s", "def"), "def");
    }

    #[test]
    fn parse_into_refuses_deep_input() {
        assert!(parse_into("{\"a\":[1,2]}").is_some());
        assert!(parse_into("{\"a\":}").is_none());
        assert!(parse_into(&"[".repeat(MAX_DEPTH + 5)).is_none());
    }

    #[test]
    fn writers_append_in_order() {
        let mut o = obj();
        add_str(&mut o, "jsonrpc", "2.0");
        add_int(&mut o, "code", -32601);
        add_bool(&mut o, "isError", true);
        add_val(&mut o, "data", json!({"k": 1}));
        assert_eq!(
            serialize(&o),
            r#"{"jsonrpc":"2.0","code":-32601,"isError":true,"data":{"k":1}}"#
        );
    }

    #[test]
    fn writers_ignore_non_objects() {
        let mut not_an_object = json!([]);
        add_str(&mut not_an_object, "k", "v");
        assert_eq!(serialize(&not_an_object), "[]");
    }
}
