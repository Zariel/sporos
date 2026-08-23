//! Typed errors for bencode decoding.

use thiserror::Error;

/// Error returned when decoding bencode input.
///
/// Carries the byte offset into the original input at which the failure was
/// detected so callers can surface precise diagnostics.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("bencode decode error at offset {offset}: {kind}")]
pub struct DecodeError {
    /// Byte offset into the original input where the error was detected.
    pub offset: usize,
    /// What went wrong.
    pub kind: DecodeErrorKind,
}

/// Classification of a bencode decode failure.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum DecodeErrorKind {
    /// Input ended before the current value was complete.
    #[error("unexpected end of input")]
    UnexpectedEof,
    /// A byte was seen where a type tag (`i`, `l`, `d`, or an ASCII digit) was expected.
    #[error("unexpected byte {byte:#04x}")]
    UnexpectedByte {
        /// The offending byte.
        byte: u8,
    },
    /// Integer syntax violated BEP 3 (empty digits, leading zero, `-0`, missing `e`).
    #[error("invalid integer syntax")]
    InvalidInteger,
    /// Integer is syntactically well-formed but does not fit in an `i64`.
    #[error("integer does not fit in i64")]
    IntegerOverflow,
    /// Byte-string length prefix was missing, empty, or had a leading zero.
    #[error("invalid length prefix")]
    InvalidLength,
    /// Byte-string length prefix did not fit in a `usize`.
    #[error("length prefix overflows usize")]
    LengthOverflow,
    /// Byte-string length prefix exceeded the remaining input.
    #[error("length prefix exceeds remaining input")]
    LengthExceedsInput,
    /// Dictionary keys were not in strict lexicographic order.
    #[error("dictionary keys not in lexicographic order")]
    UnsortedDictKeys,
    /// The same dictionary key appeared more than once.
    #[error("duplicate dictionary key")]
    DuplicateDictKey,
    /// Nesting depth exceeded the configured maximum.
    #[error("nesting depth exceeded maximum of {max}")]
    DepthExceeded {
        /// Configured maximum depth.
        max: u32,
    },
    /// The number of values materialised exceeded the configured maximum.
    #[error("node count exceeded maximum of {max}")]
    NodeLimitExceeded {
        /// Configured maximum node count.
        max: u32,
    },
    /// Input contained additional bytes after a fully-parsed root value.
    #[error("trailing data after root value")]
    TrailingData,
}
