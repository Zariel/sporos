#![allow(missing_docs)]
//! Criterion benchmarks for the bencode decoder.
//!
//! Three workloads:
//! - `decode/small_dict` — the BEP 3 canonical example (`d3:cow...`).
//! - `decode/flat_list` — a large flat list of integers.
//! - `decode/metainfo_like` — a dict shaped like a v1 .torrent `info`
//!   dictionary (pieces-style long byte string + nested file list).
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use magpie_bt_bencode::decode;

fn small_dict() -> Vec<u8> {
    b"d3:cow3:moo4:spam4:eggse".to_vec()
}

fn flat_list(n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n * 5 + 2);
    out.push(b'l');
    for i in 0..n {
        out.extend_from_slice(format!("i{i}e").as_bytes());
    }
    out.push(b'e');
    out
}

fn metainfo_like(pieces_len: usize, files: usize) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"d4:info");
    out.push(b'd');
    out.extend_from_slice(b"5:filesl");
    for i in 0..files {
        let path = format!("file-{i:04}.bin");
        let entry = format!(
            "d6:lengthi{len}e4:pathl{pl}:{p}ee",
            len = 4096 + i,
            pl = path.len(),
            p = path,
        );
        out.extend_from_slice(entry.as_bytes());
    }
    out.extend_from_slice(b"e"); // end files list
    out.extend_from_slice(b"4:name4:test");
    out.extend_from_slice(b"12:piece lengthi16384e");
    // Long pieces blob (multiple of 20).
    let pieces = vec![0xab_u8; pieces_len];
    out.extend_from_slice(format!("6:pieces{}:", pieces.len()).as_bytes());
    out.extend_from_slice(&pieces);
    out.extend_from_slice(b"e"); // end info dict
    out.extend_from_slice(b"e"); // end outer dict
    out
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode");

    let input = small_dict();
    group.throughput(Throughput::Bytes(input.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("small_dict", input.len()),
        &input,
        |b, data| {
            b.iter(|| {
                let v = decode(black_box(data)).unwrap();
                black_box(v);
            });
        },
    );

    for n in [16usize, 256, 4096] {
        let input = flat_list(n);
        group.throughput(Throughput::Bytes(input.len() as u64));
        group.bench_with_input(BenchmarkId::new("flat_list", n), &input, |b, data| {
            b.iter(|| {
                let v = decode(black_box(data)).unwrap();
                black_box(v);
            });
        });
    }

    // 1 MiB of fake pieces across 64 files — representative of a medium torrent.
    let input = metainfo_like(1024 * 1024, 64);
    group.throughput(Throughput::Bytes(input.len() as u64));
    group.bench_with_input(
        BenchmarkId::new("metainfo_like", input.len()),
        &input,
        |b, data| {
            b.iter(|| {
                let v = decode(black_box(data)).unwrap();
                black_box(v);
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
