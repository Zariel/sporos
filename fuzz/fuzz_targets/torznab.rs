#![no_main]

use libfuzzer_sys::fuzz_target;

#[path = "../../crates/sporos-service/src/torznab.rs"]
mod torznab;

use torznab::parse_torznab;

fuzz_target!(|input: &[u8]| {
    let _ = parse_torznab(input, 1024 * 1024, 100, |_| Ok(()));
});
