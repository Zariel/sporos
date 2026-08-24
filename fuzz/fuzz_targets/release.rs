#![no_main]

use libfuzzer_sys::fuzz_target;
use sporos_matcher::parse_release;

fuzz_target!(|input: &[u8]| {
    if let Ok(name) = std::str::from_utf8(input) {
        let _ = parse_release(name);
    }
});
