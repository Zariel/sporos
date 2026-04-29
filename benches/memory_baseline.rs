use std::{
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
};

use criterion::{Criterion, criterion_group, criterion_main};

const FIXTURE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/memory");

fn stream_file_len(path: impl AsRef<Path>) -> io::Result<usize> {
    let mut file = File::open(path)?;
    let mut buffer = [0_u8; 8 * 1024];
    let mut total = 0;

    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total += read;
    }

    Ok(total)
}

fn walk_data_dir(path: impl AsRef<Path>) -> io::Result<(usize, u64)> {
    let mut pending = vec![PathBuf::from(path.as_ref())];
    let mut files = 0;
    let mut bytes = 0;

    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;

            if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() {
                files += 1;
                bytes += metadata.len();
            }
        }
    }

    Ok((files, bytes))
}

fn memory_fixtures(c: &mut Criterion) {
    c.bench_function("stream representative torrent fixture", |bench| {
        bench.iter(|| stream_file_len(format!("{FIXTURE_ROOT}/torrents/representative.torrent")))
    });

    c.bench_function("walk representative data-dir fixture", |bench| {
        bench.iter(|| walk_data_dir(format!("{FIXTURE_ROOT}/data-dir")))
    });

    c.bench_function("stream representative rss fixture", |bench| {
        bench.iter(|| stream_file_len(format!("{FIXTURE_ROOT}/rss/torznab.xml")))
    });

    c.bench_function("stream representative search fixture", |bench| {
        bench.iter(|| stream_file_len(format!("{FIXTURE_ROOT}/search/results.json")))
    });
}

criterion_group!(benches, memory_fixtures);
criterion_main!(benches);
