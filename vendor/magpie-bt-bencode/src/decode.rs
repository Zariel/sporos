//! Zero-copy bencode decoder.
//!
//! The decoder is strict and attacker-hostile by default:
//! - dictionary keys must be in strict lexicographic order (duplicates rejected);
//! - integers must follow BEP 3 exactly (no leading zeros, no `-0`);
//! - nesting depth is capped (default [`DEFAULT_MAX_DEPTH`]);
//! - the total number of materialised values is capped (default
//!   [`DEFAULT_MAX_NODES`]) so a small input cannot amplify into a huge
//!   allocation;
//! - byte-string lengths are clamped to the remaining input before allocation.
//!
//! Byte strings are returned as [`std::borrow::Cow::Borrowed`] references into
//! the input; no copy happens on the hot path.

use std::borrow::Cow;
use std::collections::BTreeMap;

use crate::error::{DecodeError, DecodeErrorKind};
use crate::value::Value;

/// Default maximum nesting depth for [`decode`].
///
/// Matches the ceiling used by reference implementations
/// (libtorrent-rasterbar, anacrolix/torrent) and is large enough for every
/// real-world torrent encountered in the wild.
pub const DEFAULT_MAX_DEPTH: u32 = 256;

/// Default maximum number of values [`decode`] will materialise.
///
/// Every integer, byte string, list, and dictionary in the decoded tree counts
/// as one node. Because the smallest encodable value is two bytes (`le`, `de`,
/// `0:`) while a [`Value`] node is tens of bytes, an unbounded decode lets a
/// small input amplify into a large allocation; this ceiling bounds the total
/// spine allocation to the low hundreds of MiB at worst.
///
/// Matches the default token budget of libtorrent-rasterbar's `bdecode`
/// (`token_limit`) and is far above any real-world torrent (even a
/// multi-thousand-entry `files` list is orders of magnitude below it).
pub const DEFAULT_MAX_NODES: u32 = 2_000_000;

/// Tunable decoder limits.
///
/// Construct with [`DecodeOptions::default`] and override fields as needed.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct DecodeOptions {
    /// Maximum nesting depth. Inputs exceeding this are rejected with
    /// [`DecodeErrorKind::DepthExceeded`].
    pub max_depth: u32,
    /// Maximum number of values to materialise. Inputs whose decoded tree would
    /// contain more nodes than this are rejected with
    /// [`DecodeErrorKind::NodeLimitExceeded`]. Applies only to the allocating
    /// decode paths ([`decode`], [`decode_with`], [`decode_prefix`],
    /// [`decode_prefix_with`]); the non-allocating walkers ([`skip_value`],
    /// [`dict_value_span`]) ignore it.
    pub max_nodes: u32,
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH,
            max_nodes: DEFAULT_MAX_NODES,
        }
    }
}

/// Decodes a single bencode value from `input`, requiring that the entire
/// input is consumed.
///
/// # Errors
/// Returns [`DecodeError`] when the input is malformed, exceeds the default
/// depth limit, or contains trailing bytes past the root value.
pub fn decode(input: &[u8]) -> Result<Value<'_>, DecodeError> {
    decode_with(input, DecodeOptions::default())
}

/// Decodes a single bencode value using the supplied [`DecodeOptions`].
///
/// # Errors
/// See [`decode`].
pub fn decode_with(input: &[u8], opts: DecodeOptions) -> Result<Value<'_>, DecodeError> {
    let mut cur = Cursor::new(input);
    let value = parse_value(&mut cur, opts, 0)?;
    if cur.pos != input.len() {
        return Err(cur.err(DecodeErrorKind::TrailingData));
    }
    Ok(value)
}

/// Decodes a single bencode value from the start of `input`, returning both
/// the parsed value and the unconsumed remainder.
///
/// Useful when a framing layer concatenates multiple bencode blobs.
///
/// # Errors
/// See [`decode`].
pub fn decode_prefix(input: &[u8]) -> Result<(Value<'_>, &[u8]), DecodeError> {
    decode_prefix_with(input, DecodeOptions::default())
}

/// [`decode_prefix`] with explicit [`DecodeOptions`].
///
/// # Errors
/// See [`decode_prefix`].
pub fn decode_prefix_with(
    input: &[u8],
    opts: DecodeOptions,
) -> Result<(Value<'_>, &[u8]), DecodeError> {
    let mut cur = Cursor::new(input);
    let value = parse_value(&mut cur, opts, 0)?;
    Ok((value, &input[cur.pos..]))
}

/// Walks a single bencode value at the start of `input` without materialising
/// an AST, returning the byte range that the value occupies.
///
/// Intended for extracting raw sub-slices that must be hashed or re-used
/// verbatim (e.g. the `info` dictionary when computing a v1/v2 info-hash).
/// Applies the same strict validation as [`decode`] (sorted dict keys, no
/// leading zeros, etc.) but does no allocation.
///
/// # Errors
/// Same conditions as [`decode`], except that trailing bytes past the value
/// are **allowed** — use the returned span to slice the consumed prefix.
pub fn skip_value(input: &[u8]) -> Result<std::ops::Range<usize>, DecodeError> {
    skip_value_with(input, DecodeOptions::default())
}

/// [`skip_value`] with explicit [`DecodeOptions`].
///
/// # Errors
/// See [`skip_value`].
pub fn skip_value_with(
    input: &[u8],
    opts: DecodeOptions,
) -> Result<std::ops::Range<usize>, DecodeError> {
    let mut cur = Cursor::new(input);
    let start = cur.pos;
    skip_one(&mut cur, opts, 0)?;
    Ok(start..cur.pos)
}

/// Walks `input` as a bencode dictionary and returns the byte range of the
/// value associated with `key`, if present.
///
/// The walker validates the dict structure strictly (sorted unique keys) but
/// allocates nothing. Ideal for metainfo: after identifying the top-level
/// `info` key, this returns the exact span to hash.
///
/// # Errors
/// Returns [`DecodeError`] if `input` does not begin with a well-formed dict.
pub fn dict_value_span(
    input: &[u8],
    key: &[u8],
) -> Result<Option<std::ops::Range<usize>>, DecodeError> {
    let mut cur = Cursor::new(input);
    let first = cur
        .peek()
        .ok_or_else(|| cur.err(DecodeErrorKind::UnexpectedEof))?;
    if first != b'd' {
        return Err(cur.err(DecodeErrorKind::UnexpectedByte { byte: first }));
    }
    cur.pos += 1;
    let opts = DecodeOptions::default();
    let mut last_key: Option<&[u8]> = None;
    let mut found: Option<std::ops::Range<usize>> = None;
    loop {
        match cur.peek() {
            Some(b'e') => return Ok(found),
            None => return Err(cur.err(DecodeErrorKind::UnexpectedEof)),
            _ => {
                let key_start = cur.pos;
                let this_key = parse_bytes(&mut cur)?;
                if let Some(prev) = last_key {
                    if this_key == prev {
                        return Err(DecodeError {
                            offset: key_start,
                            kind: DecodeErrorKind::DuplicateDictKey,
                        });
                    }
                    if this_key < prev {
                        return Err(DecodeError {
                            offset: key_start,
                            kind: DecodeErrorKind::UnsortedDictKeys,
                        });
                    }
                }
                last_key = Some(this_key);
                let value_start = cur.pos;
                skip_one(&mut cur, opts, 1)?;
                if this_key == key && found.is_none() {
                    found = Some(value_start..cur.pos);
                }
            }
        }
    }
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
    /// Number of [`Value`] nodes materialised so far, charged by
    /// [`Cursor::charge_node`]. Only the allocating decode paths charge nodes;
    /// the non-allocating walkers leave this at zero.
    nodes: u32,
}

impl<'a> Cursor<'a> {
    const fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            nodes: 0,
        }
    }

    /// Accounts for one materialised value, rejecting the input once `max`
    /// nodes have already been charged. Checking before incrementing admits
    /// exactly `max` nodes and never overflows `nodes` (it stays `<= max`).
    const fn charge_node(&mut self, max: u32) -> Result<(), DecodeError> {
        if self.nodes >= max {
            return Err(self.err(DecodeErrorKind::NodeLimitExceeded { max }));
        }
        self.nodes += 1;
        Ok(())
    }

    fn peek(&self) -> Option<u8> {
        self.buf.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let b = self.peek()?;
        self.pos += 1;
        Some(b)
    }

    const fn err(&self, kind: DecodeErrorKind) -> DecodeError {
        DecodeError {
            offset: self.pos,
            kind,
        }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| self.err(DecodeErrorKind::LengthOverflow))?;
        if end > self.buf.len() {
            return Err(self.err(DecodeErrorKind::LengthExceedsInput));
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }
}

fn skip_one(cur: &mut Cursor<'_>, opts: DecodeOptions, depth: u32) -> Result<(), DecodeError> {
    if depth > opts.max_depth {
        return Err(cur.err(DecodeErrorKind::DepthExceeded {
            max: opts.max_depth,
        }));
    }
    let b = cur
        .peek()
        .ok_or_else(|| cur.err(DecodeErrorKind::UnexpectedEof))?;
    match b {
        b'i' => {
            parse_int(cur)?;
            Ok(())
        }
        b'l' => {
            cur.pos += 1;
            loop {
                match cur.peek() {
                    Some(b'e') => {
                        cur.pos += 1;
                        return Ok(());
                    }
                    None => return Err(cur.err(DecodeErrorKind::UnexpectedEof)),
                    _ => skip_one(cur, opts, depth + 1)?,
                }
            }
        }
        b'd' => {
            cur.pos += 1;
            let mut last_key: Option<&[u8]> = None;
            loop {
                match cur.peek() {
                    Some(b'e') => {
                        cur.pos += 1;
                        return Ok(());
                    }
                    None => return Err(cur.err(DecodeErrorKind::UnexpectedEof)),
                    _ => {
                        let key_start = cur.pos;
                        let key = parse_bytes(cur)?;
                        if let Some(prev) = last_key {
                            if key == prev {
                                return Err(DecodeError {
                                    offset: key_start,
                                    kind: DecodeErrorKind::DuplicateDictKey,
                                });
                            }
                            if key < prev {
                                return Err(DecodeError {
                                    offset: key_start,
                                    kind: DecodeErrorKind::UnsortedDictKeys,
                                });
                            }
                        }
                        last_key = Some(key);
                        skip_one(cur, opts, depth + 1)?;
                    }
                }
            }
        }
        b'0'..=b'9' => {
            parse_bytes(cur)?;
            Ok(())
        }
        _ => Err(cur.err(DecodeErrorKind::UnexpectedByte { byte: b })),
    }
}

fn parse_value<'a>(
    cur: &mut Cursor<'a>,
    opts: DecodeOptions,
    depth: u32,
) -> Result<Value<'a>, DecodeError> {
    if depth > opts.max_depth {
        return Err(cur.err(DecodeErrorKind::DepthExceeded {
            max: opts.max_depth,
        }));
    }
    let b = cur
        .peek()
        .ok_or_else(|| cur.err(DecodeErrorKind::UnexpectedEof))?;
    // Charge one node per materialised value. `parse_value` is the single
    // choke point for every node in the tree (root, list items, dict values),
    // so this bounds total allocation regardless of how nodes are distributed.
    // Charged after the EOF check so a truncated input still reports
    // `UnexpectedEof` rather than the budget error when the budget is exhausted.
    cur.charge_node(opts.max_nodes)?;
    match b {
        b'i' => parse_int(cur),
        b'l' => parse_list(cur, opts, depth),
        b'd' => parse_dict(cur, opts, depth),
        b'0'..=b'9' => parse_bytes(cur).map(|s| Value::Bytes(Cow::Borrowed(s))),
        _ => Err(cur.err(DecodeErrorKind::UnexpectedByte { byte: b })),
    }
}

fn parse_int<'a>(cur: &mut Cursor<'a>) -> Result<Value<'a>, DecodeError> {
    let start = cur.pos;
    debug_assert_eq!(cur.peek(), Some(b'i'));
    cur.pos += 1;
    let payload_start = cur.pos;
    if cur.peek() == Some(b'-') {
        cur.pos += 1;
    }
    let digits_start = cur.pos;
    while matches!(cur.peek(), Some(b'0'..=b'9')) {
        cur.pos += 1;
    }
    if cur.pos == digits_start {
        return Err(DecodeError {
            offset: start,
            kind: DecodeErrorKind::InvalidInteger,
        });
    }
    let digits = &cur.buf[digits_start..cur.pos];
    // Disallow leading zeros (except the single digit "0") and "-0".
    if digits.len() > 1 && digits[0] == b'0' {
        return Err(DecodeError {
            offset: start,
            kind: DecodeErrorKind::InvalidInteger,
        });
    }
    let negative = cur.buf[payload_start] == b'-';
    if negative && digits == b"0" {
        return Err(DecodeError {
            offset: start,
            kind: DecodeErrorKind::InvalidInteger,
        });
    }
    if cur.bump() != Some(b'e') {
        return Err(DecodeError {
            offset: cur.pos.saturating_sub(1),
            kind: DecodeErrorKind::InvalidInteger,
        });
    }
    let text = std::str::from_utf8(&cur.buf[payload_start..cur.pos - 1])
        .expect("only ASCII digits and optional leading minus are accepted above");
    text.parse::<i64>()
        .map(Value::Int)
        .map_err(|_| DecodeError {
            offset: start,
            kind: DecodeErrorKind::IntegerOverflow,
        })
}

fn parse_bytes<'a>(cur: &mut Cursor<'a>) -> Result<&'a [u8], DecodeError> {
    let start = cur.pos;
    let digits_start = cur.pos;
    while matches!(cur.peek(), Some(b'0'..=b'9')) {
        cur.pos += 1;
    }
    let digits = &cur.buf[digits_start..cur.pos];
    if digits.is_empty() {
        return Err(DecodeError {
            offset: start,
            kind: DecodeErrorKind::InvalidLength,
        });
    }
    if digits.len() > 1 && digits[0] == b'0' {
        return Err(DecodeError {
            offset: start,
            kind: DecodeErrorKind::InvalidLength,
        });
    }
    if cur.bump() != Some(b':') {
        return Err(DecodeError {
            offset: cur.pos.saturating_sub(1),
            kind: DecodeErrorKind::InvalidLength,
        });
    }
    let text = std::str::from_utf8(digits).expect("ASCII digits only");
    let len: usize = text.parse().map_err(|_| DecodeError {
        offset: start,
        kind: DecodeErrorKind::LengthOverflow,
    })?;
    cur.take(len)
}

fn parse_list<'a>(
    cur: &mut Cursor<'a>,
    opts: DecodeOptions,
    depth: u32,
) -> Result<Value<'a>, DecodeError> {
    debug_assert_eq!(cur.peek(), Some(b'l'));
    cur.pos += 1;
    let mut items = Vec::new();
    loop {
        match cur.peek() {
            Some(b'e') => {
                cur.pos += 1;
                return Ok(Value::List(items));
            }
            None => return Err(cur.err(DecodeErrorKind::UnexpectedEof)),
            _ => items.push(parse_value(cur, opts, depth + 1)?),
        }
    }
}

fn parse_dict<'a>(
    cur: &mut Cursor<'a>,
    opts: DecodeOptions,
    depth: u32,
) -> Result<Value<'a>, DecodeError> {
    debug_assert_eq!(cur.peek(), Some(b'd'));
    cur.pos += 1;
    let mut map: BTreeMap<Cow<'a, [u8]>, Value<'a>> = BTreeMap::new();
    let mut last_key: Option<&'a [u8]> = None;
    loop {
        match cur.peek() {
            Some(b'e') => {
                cur.pos += 1;
                return Ok(Value::Dict(map));
            }
            None => return Err(cur.err(DecodeErrorKind::UnexpectedEof)),
            _ => {
                let key_start = cur.pos;
                let key = parse_bytes(cur)?;
                if let Some(prev) = last_key {
                    if key == prev {
                        return Err(DecodeError {
                            offset: key_start,
                            kind: DecodeErrorKind::DuplicateDictKey,
                        });
                    }
                    if key < prev {
                        return Err(DecodeError {
                            offset: key_start,
                            kind: DecodeErrorKind::UnsortedDictKeys,
                        });
                    }
                }
                last_key = Some(key);
                let value = parse_value(cur, opts, depth + 1)?;
                map.insert(Cow::Borrowed(key), value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_zero() {
        assert_eq!(decode(b"i0e").unwrap(), Value::Int(0));
    }

    #[test]
    fn int_negative() {
        assert_eq!(decode(b"i-42e").unwrap(), Value::Int(-42));
    }

    #[test]
    fn int_rejects_leading_zero() {
        assert!(matches!(
            decode(b"i07e").unwrap_err().kind,
            DecodeErrorKind::InvalidInteger
        ));
    }

    #[test]
    fn int_rejects_negative_zero() {
        assert!(matches!(
            decode(b"i-0e").unwrap_err().kind,
            DecodeErrorKind::InvalidInteger
        ));
    }

    #[test]
    fn int_rejects_overflow() {
        assert!(matches!(
            decode(b"i99999999999999999999e").unwrap_err().kind,
            DecodeErrorKind::IntegerOverflow
        ));
    }

    #[test]
    fn bytes_basic() {
        let v = decode(b"4:spam").unwrap();
        assert_eq!(v.as_bytes(), Some(&b"spam"[..]));
    }

    #[test]
    fn bytes_empty() {
        let v = decode(b"0:").unwrap();
        assert_eq!(v.as_bytes(), Some(&b""[..]));
    }

    #[test]
    fn bytes_rejects_length_leading_zero() {
        assert!(matches!(
            decode(b"04:spam").unwrap_err().kind,
            DecodeErrorKind::InvalidLength
        ));
    }

    #[test]
    fn bytes_rejects_length_exceeds_input() {
        assert!(matches!(
            decode(b"10:spam").unwrap_err().kind,
            DecodeErrorKind::LengthExceedsInput
        ));
    }

    #[test]
    fn list_of_mixed() {
        let v = decode(b"l4:spami42ee").unwrap();
        let list = v.as_list().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].as_bytes(), Some(&b"spam"[..]));
        assert_eq!(list[1].as_int(), Some(42));
    }

    #[test]
    fn dict_sorted_ok() {
        let v = decode(b"d3:cow3:moo4:spam4:eggse").unwrap();
        let dict = v.as_dict().unwrap();
        assert_eq!(dict.len(), 2);
    }

    #[test]
    fn dict_rejects_unsorted() {
        let err = decode(b"d4:spam4:eggs3:cow3:mooe").unwrap_err();
        assert_eq!(err.kind, DecodeErrorKind::UnsortedDictKeys);
    }

    #[test]
    fn dict_rejects_duplicate() {
        let err = decode(b"d3:cow3:moo3:cow3:mooe").unwrap_err();
        assert_eq!(err.kind, DecodeErrorKind::DuplicateDictKey);
    }

    #[test]
    fn trailing_data_rejected() {
        let err = decode(b"i1eextra").unwrap_err();
        assert_eq!(err.kind, DecodeErrorKind::TrailingData);
    }

    #[test]
    fn decode_prefix_returns_remainder() {
        let (v, rest) = decode_prefix(b"i1eextra").unwrap();
        assert_eq!(v.as_int(), Some(1));
        assert_eq!(rest, b"extra");
    }

    #[test]
    fn depth_exceeded() {
        // Deeply nested list `lllll...ee...e` with depth beyond the cap.
        let mut buf = vec![b'l'; usize::try_from(DEFAULT_MAX_DEPTH).unwrap() + 2];
        buf.extend(std::iter::repeat_n(
            b'e',
            usize::try_from(DEFAULT_MAX_DEPTH).unwrap() + 2,
        ));
        let err = decode(&buf).unwrap_err();
        assert!(matches!(err.kind, DecodeErrorKind::DepthExceeded { .. }));
    }

    #[test]
    fn empty_input() {
        let err = decode(b"").unwrap_err();
        assert_eq!(err.kind, DecodeErrorKind::UnexpectedEof);
    }

    #[test]
    fn int_rejects_minus_alone() {
        assert!(matches!(
            decode(b"i-e").unwrap_err().kind,
            DecodeErrorKind::InvalidInteger
        ));
    }

    #[test]
    fn int_rejects_double_minus() {
        assert!(matches!(
            decode(b"i--1e").unwrap_err().kind,
            DecodeErrorKind::InvalidInteger
        ));
    }

    #[test]
    fn int_rejects_empty() {
        assert!(matches!(
            decode(b"ie").unwrap_err().kind,
            DecodeErrorKind::InvalidInteger
        ));
    }

    #[test]
    fn skip_value_reports_span() {
        let input = b"d3:cow3:moo4:spami42eel1:ai2ee";
        let span = skip_value(input).unwrap();
        // Dict occupies the first 22 bytes; trailing `l1:ai2ee` is ignored.
        assert_eq!(span, 0..22);
        // Byte-identity: the span's bytes decode back to the same tree.
        let slice = &input[span];
        let v = decode(slice).unwrap();
        assert!(v.as_dict().is_some());
    }

    #[test]
    fn skip_value_handles_nested() {
        let input = b"ld3:cow3:mooeli1ei2eee";
        let span = skip_value(input).unwrap();
        assert_eq!(span, 0..input.len());
    }

    #[test]
    fn skip_value_rejects_malformed() {
        assert!(skip_value(b"l3:abc").is_err());
        assert!(matches!(
            skip_value(b"d2:aa3:foo2:aa3:bare").unwrap_err().kind,
            DecodeErrorKind::DuplicateDictKey
        ));
    }

    #[test]
    fn dict_value_span_finds_info() {
        let input = b"d8:announce3:foo4:infod4:name5:helloee";
        let span = dict_value_span(input, b"info").unwrap().unwrap();
        assert_eq!(&input[span], b"d4:name5:helloe");
    }

    #[test]
    fn dict_value_span_absent_key() {
        let input = b"d3:cow3:mooe";
        assert!(dict_value_span(input, b"info").unwrap().is_none());
    }

    #[test]
    fn dict_value_span_rejects_non_dict() {
        assert!(dict_value_span(b"i1e", b"k").is_err());
    }

    #[test]
    fn decoded_bytes_borrow_input() {
        let input = b"4:spam".to_vec();
        let v = decode(&input).unwrap();
        let b = v.as_bytes().unwrap();
        // Confirms zero-copy: the byte slice lies inside the original buffer.
        let base = input.as_ptr() as usize;
        let slice = b.as_ptr() as usize;
        assert!(slice >= base && slice + b.len() <= base + input.len());
    }

    fn with_nodes(max_nodes: u32) -> DecodeOptions {
        DecodeOptions {
            max_nodes,
            ..DecodeOptions::default()
        }
    }

    #[test]
    fn node_limit_is_exact() {
        assert!(decode_with(b"li0ei1ee", with_nodes(3)).is_ok());
        let err = decode_with(b"li0ei1ee", with_nodes(2)).unwrap_err();
        assert_eq!(err.kind, DecodeErrorKind::NodeLimitExceeded { max: 2 });
        assert_eq!(err.offset, 4);
    }

    #[test]
    fn node_limit_does_not_apply_to_walkers() {
        let input = b"d4:infod4:name5:helloee";
        assert!(skip_value_with(input, with_nodes(0)).is_ok());
        assert!(dict_value_span(input, b"info").unwrap().is_some());
    }
}
