//! Canonical bencode encoder.
//!
//! The encoder always emits canonical output: dictionary keys in strict
//! lexicographic order (guaranteed by [`std::collections::BTreeMap`]),
//! no whitespace, integers without leading zeros. Output of
//! re-encoding a decoded canonical input produces byte-identical output.

use std::io::Write as _;

use crate::value::Value;

/// Encodes `value` into a freshly allocated byte vector.
#[must_use]
pub fn encode(value: &Value<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    encode_into(value, &mut out);
    out
}

/// Encodes `value` by appending to the provided buffer.
pub fn encode_into(value: &Value<'_>, out: &mut Vec<u8>) {
    match value {
        Value::Int(i) => {
            out.push(b'i');
            // itoa-style via stdlib: write! into Vec never fails.
            write!(out, "{i}").expect("writing to Vec is infallible");
            out.push(b'e');
        }
        Value::Bytes(b) => {
            write!(out, "{}", b.len()).expect("writing to Vec is infallible");
            out.push(b':');
            out.extend_from_slice(b);
        }
        Value::List(items) => {
            out.push(b'l');
            for item in items {
                encode_into(item, out);
            }
            out.push(b'e');
        }
        Value::Dict(map) => {
            out.push(b'd');
            for (key, val) in map {
                write!(out, "{}", key.len()).expect("writing to Vec is infallible");
                out.push(b':');
                out.extend_from_slice(key);
                encode_into(val, out);
            }
            out.push(b'e');
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use super::*;
    use crate::decode::decode;

    #[test]
    fn encodes_int() {
        assert_eq!(encode(&Value::Int(0)), b"i0e");
        assert_eq!(encode(&Value::Int(-42)), b"i-42e");
        assert_eq!(
            encode(&Value::Int(i64::MAX)),
            format!("i{}e", i64::MAX).as_bytes()
        );
    }

    #[test]
    fn encodes_bytes() {
        let v = Value::Bytes(Cow::Borrowed(b"spam"));
        assert_eq!(encode(&v), b"4:spam");
    }

    #[test]
    fn encodes_dict_in_sorted_order() {
        let mut m: BTreeMap<Cow<'static, [u8]>, Value<'static>> = BTreeMap::new();
        // Insert out of order.
        m.insert(
            Cow::Borrowed(b"spam".as_slice()),
            Value::Bytes(Cow::Borrowed(b"eggs")),
        );
        m.insert(
            Cow::Borrowed(b"cow".as_slice()),
            Value::Bytes(Cow::Borrowed(b"moo")),
        );
        let encoded = encode(&Value::Dict(m));
        assert_eq!(encoded, b"d3:cow3:moo4:spam4:eggse");
    }

    #[test]
    fn roundtrip_simple() {
        let input: &[u8] = b"d5:itemsli1ei2ei3ee4:rooti7ee";
        let v = decode(input).unwrap();
        assert_eq!(encode(&v), input);
    }
}
