//! Bencode value AST.

use std::borrow::Cow;
use std::collections::BTreeMap;

/// A decoded bencode value.
///
/// Byte strings and dictionary keys borrow from the input by default (via
/// [`Cow::Borrowed`]) so a fresh decode allocates only for the structural
/// [`Vec`] / [`BTreeMap`] spines. Call [`Value::into_owned`] to lift the
/// lifetime to `'static` when you need to outlive the source buffer.
///
/// Dictionaries use [`BTreeMap`] keyed on byte strings, which matches BEP 3's
/// requirement that dictionary keys are emitted in strict lexicographic order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value<'a> {
    /// A bencoded integer (`i<...>e`).
    Int(i64),
    /// A bencoded byte string (`<len>:<bytes>`).
    Bytes(Cow<'a, [u8]>),
    /// A bencoded list (`l...e`).
    List(Vec<Self>),
    /// A bencoded dictionary (`d...e`), with keys in lexicographic order.
    Dict(BTreeMap<Cow<'a, [u8]>, Self>),
}

impl<'a> Value<'a> {
    /// Returns `Some(i64)` when the value is [`Value::Int`].
    #[must_use]
    pub const fn as_int(&self) -> Option<i64> {
        if let Self::Int(i) = *self {
            Some(i)
        } else {
            None
        }
    }

    /// Returns `Some(&[u8])` when the value is [`Value::Bytes`].
    ///
    /// The returned slice is tied to the lifetime of `self`. For a value
    /// produced by [`crate::decode`] (always `Cow::Borrowed`) prefer
    /// [`Value::as_borrowed_bytes`], which preserves the original input
    /// lifetime.
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if let Self::Bytes(b) = self {
            Some(b)
        } else {
            None
        }
    }

    /// Returns the slice of a [`Value::Bytes`] variant with the original
    /// input lifetime `'a`, for values produced by the decoder (which always
    /// stores `Cow::Borrowed`).
    ///
    /// Returns `None` for non-bytes variants **and** for `Cow::Owned` byte
    /// strings that a caller constructed manually.
    #[must_use]
    pub const fn as_borrowed_bytes(&self) -> Option<&'a [u8]> {
        if let Self::Bytes(Cow::Borrowed(b)) = self {
            Some(*b)
        } else {
            None
        }
    }

    /// Returns `Some(&[Value])` when the value is [`Value::List`].
    #[must_use]
    pub fn as_list(&self) -> Option<&[Self]> {
        if let Self::List(l) = self {
            Some(l)
        } else {
            None
        }
    }

    /// Returns `Some(&BTreeMap<..>)` when the value is [`Value::Dict`].
    #[must_use]
    pub const fn as_dict(&self) -> Option<&BTreeMap<Cow<'a, [u8]>, Self>> {
        if let Self::Dict(d) = self {
            Some(d)
        } else {
            None
        }
    }

    /// If `self` is a dictionary, returns the value associated with `key`
    /// (matched against the underlying byte slice).
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&Self> {
        self.as_dict().and_then(|d| d.get(key))
    }

    /// Converts every borrowed byte string in the tree into an owned allocation,
    /// producing a `Value<'static>` that no longer references the source buffer.
    #[must_use]
    pub fn into_owned(self) -> Value<'static> {
        match self {
            Self::Int(i) => Value::Int(i),
            Self::Bytes(c) => Value::Bytes(Cow::Owned(c.into_owned())),
            Self::List(v) => Value::List(v.into_iter().map(Value::into_owned).collect()),
            Self::Dict(m) => Value::Dict(
                m.into_iter()
                    .map(|(k, v)| (Cow::Owned(k.into_owned()), v.into_owned()))
                    .collect(),
            ),
        }
    }
}
