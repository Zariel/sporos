use std::{alloc::System, borrow::Cow, sync::Mutex};

use sporos::{
    clients::{ClientTorrent, client_torrent_to_searchee},
    config::MatchMode,
    domain::{
        Decision, File as TorrentFile, InfoHash, Label, MediaType, Metafile, Searchee,
        TorrentClientKind, TorrentClientMetadata,
    },
    integrations::parse_torznab_rss,
    matching::{AssessmentOptions, assess_metafile},
    memory::ALLOCATION_REGRESSION_GATES,
    search::Blocklist,
};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, Stats, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

static ALLOCATION_GATE_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn client_inventory_conversion_stays_under_allocation_gate() {
    let _guard = allocation_gate_lock();
    let gate = allocation_gate("client inventory to searchees");
    let metadata = TorrentClientMetadata::new(
        "gate-client",
        0,
        TorrentClientKind::QBittorrent,
        false,
        "qbittorrent",
    );
    let torrents = synthetic_client_torrents(gate.fixture_items);
    let region = Region::new(&GLOBAL);

    let searchees = torrents
        .into_iter()
        .filter_map(|torrent| client_torrent_to_searchee(&metadata, torrent))
        .collect::<Vec<_>>();
    let stats = region.change();

    assert_eq!(searchees.len(), gate.fixture_items);
    assert_allocation_gate(
        gate.name,
        stats,
        gate.max_live_bytes,
        gate.max_total_allocated_bytes,
    );
    std::hint::black_box(searchees);
}

#[test]
fn rss_parsing_stays_under_allocation_gate() {
    let _guard = allocation_gate_lock();
    let gate = allocation_gate("RSS candidate parsing");
    let rss = synthetic_rss(gate.fixture_items);
    let region = Region::new(&GLOBAL);

    let candidates = parse_torznab_rss(&rss, 1).expect("rss candidates");
    let stats = region.change();

    assert_eq!(candidates.len(), gate.fixture_items);
    assert_allocation_gate(
        gate.name,
        stats,
        gate.max_live_bytes,
        gate.max_total_allocated_bytes,
    );
    std::hint::black_box(candidates);
}

#[test]
fn candidate_assessment_stays_under_allocation_gate() {
    let _guard = allocation_gate_lock();
    let gate = allocation_gate("candidate assessment");
    let (searchee, metafile) = assessment_fixture();
    let excluded = std::collections::BTreeSet::new();
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let options = AssessmentOptions {
        match_mode: MatchMode::Strict,
        fuzzy_size_threshold: 0.05,
        season_from_episodes: 1.0,
        include_single_episodes: true,
        info_hashes_to_exclude: &excluded,
        blocklist: &blocklist,
    };
    let region = Region::new(&GLOBAL);

    let mut matches = 0usize;
    for _ in 0..gate.fixture_items {
        let assessment = assess_metafile(&metafile, &searchee, &options, true, 0.05);
        if assessment.decision == Decision::Match {
            matches += 1;
        }
    }
    let stats = region.change();

    assert_eq!(matches, gate.fixture_items);
    assert_allocation_gate(
        gate.name,
        stats,
        gate.max_live_bytes,
        gate.max_total_allocated_bytes,
    );
}

fn allocation_gate(name: &str) -> sporos::memory::AllocationRegressionGate {
    *ALLOCATION_REGRESSION_GATES
        .iter()
        .find(|gate| gate.name == name)
        .expect("allocation gate")
}

fn allocation_gate_lock() -> std::sync::MutexGuard<'static, ()> {
    ALLOCATION_GATE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn assert_allocation_gate(
    name: &str,
    stats: Stats,
    max_live_bytes: usize,
    max_total_allocated_bytes: usize,
) {
    let live_bytes = live_bytes(stats);
    assert!(
        live_bytes <= max_live_bytes,
        "{name} live heap bytes {live_bytes} exceeded budget {max_live_bytes}; stats: {stats:?}"
    );
    assert!(
        stats.bytes_allocated <= max_total_allocated_bytes,
        "{name} allocated bytes {} exceeded budget {max_total_allocated_bytes}; stats: {stats:?}",
        stats.bytes_allocated
    );
}

fn live_bytes(stats: Stats) -> usize {
    let allocated = stats.bytes_allocated as isize + stats.bytes_reallocated;
    allocated.saturating_sub(stats.bytes_deallocated as isize) as usize
}

fn synthetic_client_torrents(count: usize) -> Vec<ClientTorrent<'static>> {
    (0..count).map(client_torrent).collect()
}

fn client_torrent(index: usize) -> ClientTorrent<'static> {
    let hash = format!("{index:040x}");
    ClientTorrent {
        info_hash: InfoHash::new(hash).expect("hash").into_owned(),
        name: Cow::Owned(format!("Example.Show.S01E{:02}", index % 100)),
        files: vec![TorrentFile::new(
            format!("Example.Show.S01E{:02}.mkv", index % 100),
            1_000_000_000 + index as u64,
        )],
        save_path: Cow::Borrowed("/downloads"),
        category: None,
        tags: Vec::new(),
        trackers: Vec::new(),
        complete: true,
        checking: false,
    }
}

fn synthetic_rss(count: usize) -> String {
    let mut output = String::from(r#"<?xml version="1.0"?><rss><channel>"#);
    for index in 0..count {
        output.push_str(&format!(
            r#"<item><title>Example.Show.S01E{episode:02}</title><guid>guid-{index}</guid><link>https://indexer.example/{index}.torrent</link><size>{size}</size><pubDate>Fri, 01 May 2026 00:00:00 GMT</pubDate></item>"#,
            episode = index % 100,
            size = 1_000_000_000_u64 + index as u64,
        ));
    }
    output.push_str("</channel></rss>");
    output
}

fn assessment_fixture() -> (Searchee<'static>, Metafile<'static>) {
    let files = vec![TorrentFile::new("Example.Show.S01E01.mkv", 1_000_000_000)];
    let mut searchee =
        Searchee::from_files("Example.Show.S01E01", "Example.Show.S01E01", files.clone());
    searchee.media_type = MediaType::Episode;
    searchee.label = Some(Label::Search);
    let mut metafile = Metafile::from_files(
        InfoHash::from_validated("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        "Example.Show.S01E01",
        "Example.Show.S01E01",
        16_384,
        files,
    );
    metafile.media_type = MediaType::Episode;
    (searchee, metafile)
}
