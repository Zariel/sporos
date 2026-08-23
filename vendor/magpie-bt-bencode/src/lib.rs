//! Zero-copy [bencode](https://wiki.theory.org/BitTorrentSpecification#Bencoding)
//! codec for the magpie BitTorrent library.
//!
//! - Decoder: strict by default (rejects unsorted / duplicate dict keys, `-0`,
//!   leading zeros, excessive nesting, and over-budget node counts); returns a
//!   [`Value`] tree whose byte strings borrow from the input.
//! - Encoder: always emits canonical output (BTreeMap-sorted keys, no slack).
//! - Errors: typed [`DecodeError`] with byte offsets for precise diagnostics.
//!
//! # Example
//! ```
//! use magpie_bt_bencode::{decode, encode, Value};
//!
//! let input: &[u8] = b"d3:cow3:moo4:spam4:eggse";
//! let parsed = decode(input).unwrap();
//!
//! // Byte strings borrow from `input` — no allocation on the hot path.
//! let dict = parsed.as_dict().unwrap();
//! assert_eq!(dict.get(&b"cow"[..]).and_then(Value::as_bytes), Some(&b"moo"[..]));
//!
//! // Canonical re-encode is byte-identical to a canonical input.
//! assert_eq!(encode(&parsed), input);
//! ```
#![forbid(unsafe_code)]

mod decode;
mod encode;
mod error;
mod value;

pub use decode::{
    DEFAULT_MAX_DEPTH, DEFAULT_MAX_NODES, DecodeOptions, decode, decode_prefix,
    decode_prefix_with, decode_with, dict_value_span, skip_value, skip_value_with,
};
pub use encode::{encode, encode_into};
pub use error::{DecodeError, DecodeErrorKind};
pub use value::Value;
