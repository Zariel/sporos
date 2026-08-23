use std::io::{self, Read};

use sporos_service::inventory::parse_inventory;

const TORRENTS: usize = 10_000;
const BATCH_SIZE: usize = 250;

fn main() {
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let count = parse_inventory(SyntheticInventory::default(), 32 * 1024 * 1024, |torrent| {
        batch.push(torrent);
        if batch.len() == BATCH_SIZE {
            batch.clear();
        }
        Ok(())
    })
    .expect("parse synthetic inventory");
    assert_eq!(count, TORRENTS);
    println!("torrents={count} peak_rss_kib={}", peak_rss_kib());
}

#[derive(Default)]
struct SyntheticInventory {
    next: usize,
    current: io::Cursor<Vec<u8>>,
}

impl Read for SyntheticInventory {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.current.position() == self.current.get_ref().len() as u64 {
            let bytes = if self.next == 0 {
                self.next += 1;
                b"[".to_vec()
            } else if self.next <= TORRENTS {
                let index = self.next - 1;
                self.next += 1;
                let separator = if index == 0 { "" } else { "," };
                format!(
                    "{separator}{{\"hash\":\"{index:040x}\",\"name\":\"release-{index}\",\"amount_left\":0,\"progress\":1.0,\"state\":\"stoppedUP\",\"save_path\":\"/data\",\"content_path\":\"/data/release-{index}\",\"category\":\"video\",\"tags\":\"sporos\"}}"
                )
                .into_bytes()
            } else if self.next == TORRENTS + 1 {
                self.next += 1;
                b"]".to_vec()
            } else {
                return Ok(0);
            };
            self.current = io::Cursor::new(bytes);
        }
        self.current.read(output)
    }
}

fn peak_rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmHWM:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
        })
        .unwrap_or(0)
}
