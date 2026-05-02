use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    hint::black_box,
    io::{self, Read},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use sporos::{
    actions::{FileLinkOptions, link_all_files_in_metafile},
    clients::{
        ClientErrorCode, ClientTorrent, DownloadDirOptions, InjectionOptions, NewTorrent,
        ResumeOptions, TorrentClient, client_torrent_to_searchee,
    },
    config::{LinkType, RawConfig, RuntimeConfig, TorrentClientConfig},
    domain::{
        Decision, File as TorrentFile, InfoHash, InjectionResult, Label, MediaType, Metafile,
        Searchee, TorrentClientKind, TorrentClientMetadata,
    },
    integrations::{SnatchOptions, TorznabSearchOptions, parse_torznab_rss},
    matching::{AssessmentOptions, assess_metafile},
    memory::{CLIENT_INVENTORY_BASELINE_TORRENTS, MEMORY_REGRESSION_GATES},
    operations::cleanup_db_with_clients,
    persistence::Database,
    search::{Blocklist, ContentFilterOptions, SearchPipelineOptions, find_searchable_searchees},
};

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

fn large_memory_paths(c: &mut Criterion) {
    let mut group = c.benchmark_group("large memory paths");
    group.sample_size(10);

    let client_metadata = TorrentClientMetadata::new(
        "bench-client",
        0,
        TorrentClientKind::QBittorrent,
        false,
        "qbittorrent",
    );
    let client_torrents = synthetic_client_torrents(CLIENT_INVENTORY_BASELINE_TORRENTS);
    group.bench_function("client inventory to searchees 10k", |bench| {
        bench.iter(|| {
            let mut produced = 0usize;
            for torrent in &client_torrents {
                if client_torrent_to_searchee(&client_metadata, torrent.clone()).is_some() {
                    produced += 1;
                }
            }
            black_box(produced)
        })
    });

    let searchees = synthetic_searchees(CLIENT_INVENTORY_BASELINE_TORRENTS);
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let excluded = BTreeSet::new();
    let assessment_options = AssessmentOptions {
        match_mode: sporos::config::MatchMode::Strict,
        fuzzy_size_threshold: 0.05,
        season_from_episodes: 1.0,
        include_single_episodes: true,
        info_hashes_to_exclude: &excluded,
        blocklist: &blocklist,
    };
    let filter = ContentFilterOptions {
        include_single_episodes: true,
        include_non_videos: true,
        blocklist_only: false,
        ignore_cross_seeds: false,
        link_category: None,
        label: None,
        fuzzy_size_threshold: 0.05,
        blocklist: &blocklist,
    };
    let search_options = SearchPipelineOptions {
        label: Label::Search,
        filter,
        assessment: assessment_options,
        snatch: SnatchOptions::default(),
        torznab: TorznabSearchOptions::default(),
        arr_configs: &[],
        arr_timeout: None,
        virtual_season: None,
        exclude_older: None,
        exclude_recent_search: None,
    };
    group.bench_function("search filtering 10k searchees", |bench| {
        bench.iter(|| {
            find_searchable_searchees(
                black_box(searchees.clone()),
                black_box(&[]),
                black_box(1),
                black_box(&search_options),
            )
        })
    });

    let rss = synthetic_rss(1_000);
    group.bench_function("parse 1k rss candidates", |bench| {
        bench.iter(|| parse_torznab_rss(black_box(&rss), black_box(1)))
    });

    let (searchee, metafile) = assessment_fixture();
    group.bench_function("candidate assessment 10k", |bench| {
        bench.iter(|| {
            let mut matches = 0usize;
            for _ in 0..10_000 {
                let assessment = assess_metafile(
                    black_box(&metafile),
                    black_box(&searchee),
                    black_box(&search_options.assessment),
                    black_box(true),
                    black_box(0.05),
                );
                if assessment.decision == Decision::Match {
                    matches += 1;
                }
            }
            black_box(matches)
        })
    });

    let link_options = FileLinkOptions {
        link_dirs: &[],
        link_type: LinkType::Symlink,
        flat_linking: false,
        ignore_missing: true,
        unwrap_symlinks: false,
    };
    group.bench_function("injection linking 10k candidates", |bench| {
        bench.iter_batched(
            temp_path,
            |root| {
                let mut linked = 0usize;
                for _ in 0..10_000 {
                    if link_all_files_in_metafile(
                        black_box(&searchee),
                        black_box(&metafile),
                        black_box(Decision::Match),
                        black_box(&root),
                        black_box(&link_options),
                    )
                    .is_ok()
                    {
                        linked += 1;
                    }
                }
                if let Err(_error) = fs::remove_dir_all(root) {}
                black_box(linked)
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("cleanup client refresh 10k", |bench| {
        bench.iter_batched(
            cleanup_fixture,
            |(root, database, config, client)| {
                let client_ref: &dyn TorrentClient = &client;
                let result = cleanup_db_with_clients(
                    black_box(&database),
                    black_box(&root),
                    black_box(&config),
                    black_box(2_000_000),
                    black_box(&[client_ref]),
                );
                if let Err(_error) = fs::remove_dir_all(root) {}
                result
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("memory gate registry", |bench| {
        bench.iter(|| {
            black_box(
                MEMORY_REGRESSION_GATES
                    .iter()
                    .map(|gate| gate.target_items)
                    .sum::<usize>(),
            )
        })
    });

    group.finish();
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

fn synthetic_searchees(count: usize) -> Vec<Searchee<'static>> {
    (0..count)
        .map(|index| {
            let mut searchee = Searchee::from_files(
                format!("Example.Show.S01E{:02}", index % 100),
                format!("Example.Show.S01E{:02}", index % 100),
                vec![TorrentFile::new(
                    format!("Example.Show.S01E{:02}.mkv", index % 100),
                    1_000_000_000 + index as u64,
                )],
            );
            searchee.media_type = MediaType::Episode;
            searchee
        })
        .collect()
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

fn cleanup_fixture() -> (PathBuf, Database, RuntimeConfig, StreamingClient) {
    let root = temp_path();
    fs::create_dir_all(&root).expect("root");
    let database = Database::open_app_dir(&root).expect("database");
    let config = RuntimeConfig::normalize(
        RawConfig {
            use_client_torrents: Some(true),
            season_from_episodes: Some(1.0),
            torrent_clients: vec![
                TorrentClientConfig::parse("qbittorrent:http://localhost:8080").expect("client"),
            ],
            ..RawConfig::default()
        },
        &root,
    )
    .expect("config");
    (
        root,
        database,
        config,
        StreamingClient::new(CLIENT_INVENTORY_BASELINE_TORRENTS),
    )
}

fn temp_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "sporos-memory-bench-{}-{nanos}",
        std::process::id()
    ))
}

struct StreamingClient {
    metadata: TorrentClientMetadata<'static>,
    count: usize,
}

impl StreamingClient {
    fn new(count: usize) -> Self {
        Self {
            metadata: TorrentClientMetadata::new(
                "localhost",
                0,
                TorrentClientKind::QBittorrent,
                false,
                "bench",
            ),
            count,
        }
    }
}

impl TorrentClient for StreamingClient {
    fn metadata(&self) -> &TorrentClientMetadata<'_> {
        &self.metadata
    }

    fn is_torrent_in_client(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<bool> {
        Ok(false)
    }

    fn is_torrent_complete(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<bool> {
        Ok(false)
    }

    fn is_torrent_checking(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<bool> {
        Ok(false)
    }

    fn get_all_torrents(&self) -> sporos::Result<Vec<ClientTorrent<'static>>> {
        Ok(synthetic_client_torrents(self.count))
    }

    fn for_each_torrent(
        &self,
        visitor: &mut dyn FnMut(ClientTorrent<'static>) -> sporos::Result<()>,
    ) -> sporos::Result<()> {
        for index in 0..self.count {
            visitor(client_torrent(index))?;
        }
        Ok(())
    }

    fn get_download_dir(
        &self,
        _metafile: &Metafile<'_>,
        _options: DownloadDirOptions,
    ) -> sporos::Result<Result<PathBuf, ClientErrorCode>> {
        Ok(Err(ClientErrorCode::NotFound))
    }

    fn get_all_download_dirs(&self) -> sporos::Result<BTreeMap<String, PathBuf>> {
        Ok(BTreeMap::new())
    }

    fn inject(
        &self,
        _new_torrent: &NewTorrent<'_>,
        _searchee: &Searchee<'_>,
        _decision: Decision,
        _options: &InjectionOptions,
    ) -> sporos::Result<InjectionResult> {
        Ok(InjectionResult::Injected)
    }

    fn recheck_torrent(&self, _info_hash: &InfoHash<'_>) -> sporos::Result<()> {
        Ok(())
    }

    fn resume_injection(
        &self,
        _metafile: &Metafile<'_>,
        _decision: Decision,
        _options: ResumeOptions,
    ) -> sporos::Result<()> {
        Ok(())
    }

    fn validate_config(&self) -> sporos::Result<()> {
        Ok(())
    }
}

criterion_group!(benches, memory_fixtures, large_memory_paths);
criterion_main!(benches);
