# magpie-bt-bencode

Zero-copy [bencode](https://wiki.theory.org/BitTorrentSpecification#Bencoding) codec for the [magpie](https://github.com/mlamp/magpie) BitTorrent library.

**Status**: M0 phase A complete — zero-copy decoder + canonical encoder, property tests, fuzz target, benchmark baseline.

## Highlights

- **Strict**: rejects unsorted dict keys, duplicates, leading-zero integers, `-0`, oversized nesting.
- **Zero-copy**: byte strings and dict keys borrow from the input (`Cow::Borrowed`).
- **Canonical re-encode**: `encode ∘ decode` is byte-identical on canonical input.
- **Typed errors**: `DecodeError { offset, kind }` points at the failing byte.

## Quick start

```rust
use magpie_bt_bencode::{decode, encode, Value};

let input: &[u8] = b"d3:cow3:moo4:spam4:eggse";
let value = decode(input).unwrap();
assert_eq!(encode(&value), input);
```

## Strictness vs other libraries

magpie-bt-bencode is **strict by design**: any input that violates BEP 3's
canonical form is rejected at parse time. Concretely, the decoder rejects:

- unsorted dictionary keys;
- duplicate dictionary keys;
- integers with leading zeros (except `i0e`) or `-0`;
- byte-string length prefixes with leading zeros;
- nesting deeper than 256 levels.

This matches [anacrolix/torrent](https://github.com/anacrolix/torrent)'s stance
and is **stricter than libtorrent-rasterbar**, which accepts unsorted/duplicate
keys with a `soft_error` signal. If you need to parse corrupt or quirky
`.torrent` files the way rasterbar does, this crate will reject them — that is
the intended behaviour.

## Raw-byte spans

For applications that need the **exact bytes** of a sub-value (notably
computing a v1 info-hash, which is `SHA1(bencode(info))` over the **original**
`.torrent` bytes), use [`skip_value`] or [`dict_value_span`] — they walk the
input with the same strict validation but allocate nothing and return a
byte `Range` into the source buffer.

See the [M0 foundations milestone](https://github.com/mlamp/magpie/blob/main/docs/milestones/000-foundations.md) for scope.
