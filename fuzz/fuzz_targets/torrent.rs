#![no_main]

use libfuzzer_sys::fuzz_target;
use magpie_bt_bencode::{DecodeOptions, decode_with};

const MAX_BENCODE_DEPTH: u32 = 32;
const MAX_BENCODE_NODES: u32 = 100_000;

fuzz_target!(|input: &[u8]| {
    let mut options = DecodeOptions::default();
    options.max_depth = MAX_BENCODE_DEPTH;
    options.max_nodes = MAX_BENCODE_NODES;
    if let Ok(tree) = decode_with(input, options) {
        drop(tree);
        let _ = magpie_bt_metainfo::parse(input);
    }
});
