use super::{
    Blocklist, CANDIDATE_SEARCH_CACHE_CANDIDATE_LIMIT, CANDIDATE_SEARCH_CACHE_ENTRY_LIMIT,
    CachedCandidates, CandidateSearchCache, ContentFilterOptions, ContentFilterRejection,
    MediaCapabilities, PIPELINE_ATTEMPT_RETAIN_LIMIT, PipelineAttempt, PipelineSummary,
    ReverseLookupRuntime, SearchPipelineOptions, SearchPipelineRuntime, SearcheeSources,
    TimestampDecision, VirtualSeasonOptions, affected_roots_for_changed_path, bulk_search,
    check_new_candidate_match, create_searchee_from_path, create_virtual_season_searchees,
    filter_by_content, filter_duplicate_searchees, find_all_searchees, find_potential_nested_roots,
    find_searchable_searchees, get_media_type, index_torrent_dir, indexer_supports_media,
    lookup_fields, parse_title, reverse_lookup_client_rows, reverse_lookup_data_rows,
    reverse_lookup_keys, reverse_lookup_searchees, search_group_key, timestamp_excludes,
};
use crate::{
    domain::{
        ActionResult, Candidate, ClientLabel, ClientTorrentMetadata, Decision, File, Label,
        MediaType, SaveResult, Searchee,
    },
    integrations::{SearchIndexer, SnatchOptions, TorznabCaps, TorznabSearchOptions},
    matching::AssessmentOptions,
    persistence::{ClientSearcheeRecord, DataRootRecord, Database, DecisionRecord, SqlValue},
};
use std::{
    borrow::Cow,
    collections::BTreeSet,
    fs,
    io::{Read, Write},
    net::TcpListener,
    path::PathBuf,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[test]
fn pipeline_summary_bounds_retained_attempts() {
    let mut summary = PipelineSummary::default();
    for index in 0..PIPELINE_ATTEMPT_RETAIN_LIMIT + 5 {
        summary.record_attempt(test_attempt(index));
    }

    assert_eq!(summary.attempts_total, PIPELINE_ATTEMPT_RETAIN_LIMIT + 5);
    assert_eq!(summary.attempts.len(), PIPELINE_ATTEMPT_RETAIN_LIMIT);
    assert_eq!(
        summary
            .attempts
            .last()
            .map(|attempt| attempt.candidate_guid.as_str()),
        Some("guid-255")
    );
}

#[test]
fn candidate_search_cache_bounds_entries_and_large_results() {
    let mut cache = CandidateSearchCache::default();
    let small = vec![Candidate::new(
        "Small",
        "small-guid",
        None::<String>,
        "tracker",
    )];
    for index in 0..CANDIDATE_SEARCH_CACHE_ENTRY_LIMIT + 5 {
        cache.insert_candidates(
            (format!("group-{index}"), 1),
            format!("ids-{index}"),
            &small,
        );
    }
    assert_eq!(cache.entries.len(), CANDIDATE_SEARCH_CACHE_ENTRY_LIMIT);
    assert!(!cache.entries.contains_key(&("group-0".to_owned(), 1)));
    assert!(cache.entries.contains_key(&(
        format!("group-{}", CANDIDATE_SEARCH_CACHE_ENTRY_LIMIT + 4),
        1
    )));

    let large = (0..CANDIDATE_SEARCH_CACHE_CANDIDATE_LIMIT + 1)
        .map(|index| {
            Candidate::new(
                format!("Large {index}"),
                format!("large-{index}"),
                None::<String>,
                "tracker",
            )
        })
        .collect::<Vec<_>>();
    cache.insert_candidates(("large".to_owned(), 1), "large".to_owned(), &large);
    assert!(!cache.entries.contains_key(&("large".to_owned(), 1)));
}

#[test]
fn classifies_media_type_in_documented_order() {
    assert_eq!(get_media_type("Show S01E02", &[]), MediaType::Episode);
    assert_eq!(get_media_type("Show Season 2", &[]), MediaType::Pack);
    assert_eq!(
        get_media_type("Movie 2020", &[File::new("Movie.2020.mkv", 10)]),
        MediaType::Movie
    );
    assert_eq!(
        get_media_type("Album", &[File::new("track.flac", 10)]),
        MediaType::Audio
    );
    assert_eq!(
        get_media_type("Book", &[File::new("book.epub", 10)]),
        MediaType::Book
    );
    assert_eq!(
        get_media_type("Archive", &[File::new("data.bin", 10)]),
        MediaType::Unknown
    );
}

#[test]
fn keeps_digit_names_as_compatibility_titles() {
    let parsed = parse_title(
        "Example.Show.S01E02.1080p.WEB-DL-GROUP",
        &[File::new("Example.Show.S01E02.1080p.WEB-DL-GROUP.mkv", 10)],
        None,
    )
    .expect("title parses");

    assert_eq!(parsed.title, "Example.Show.S01E02.1080p.WEB-DL-GROUP");
    assert_eq!(parsed.media_type, MediaType::Episode);
    assert_eq!(parsed.resolution.as_deref(), Some("1080p"));
    assert_eq!(parsed.source.as_deref(), Some("WEB-DL"));
    assert_eq!(parsed.release_group.as_deref(), Some("GROUP"));
}

#[test]
fn infers_episode_title_from_video_file() {
    let parsed = parse_title(
        "Example Show",
        &[File::new("Example.Show.S01E02.1080p.WEB-DL-GROUP.mkv", 10)],
        None,
    )
    .expect("title parses");

    assert_eq!(parsed.title, "Example Show S01E02");
    assert_eq!(parsed.media_type, MediaType::Episode);
    assert_eq!(parsed.resolution.as_deref(), Some("1080p"));
    assert_eq!(parsed.source.as_deref(), Some("WEB-DL"));
    assert_eq!(parsed.release_group.as_deref(), Some("GROUP"));
}

#[test]
fn infers_short_season_folder_from_parent_path() {
    let parsed = parse_title(
        "Season 2",
        &[
            File::new("Episode.One.S02E01.mkv", 10),
            File::new("Episode.Two.S02E02.mkv", 10),
        ],
        Some("/media/Example Show (2020)/Season 2"),
    )
    .expect("season parses");

    assert_eq!(parsed.title, "Example Show (2020) S02");
    assert_eq!(parsed.media_type, MediaType::Pack);
}

#[test]
fn skips_short_season_folder_without_parent_title() {
    assert!(parse_title("Season 2", &[File::new("Episode.One.S02E01.mkv", 10)], None).is_none());
}

#[test]
fn discovers_nested_roots_deepest_first_and_ignores_samples() {
    let root = temp_path("nested-roots");
    fs::create_dir_all(root.join("Show/Season 1")).expect("season dir");
    fs::create_dir_all(root.join("Show/Sample")).expect("sample dir");
    fs::write(root.join("Show/Season 1/Show.S01E01.mkv"), b"video").expect("episode");
    fs::write(root.join("Show/Season 1/Show.S01E02.mkv"), b"video").expect("episode");
    fs::write(root.join("Show/Sample/sample.mkv"), b"sample").expect("sample");
    fs::write(root.join("readme.txt"), b"text").expect("text");

    let roots = find_potential_nested_roots(&root, 2).expect("roots");

    assert_eq!(roots, vec![root.join("Show/Season 1")]);
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn creates_data_dir_searchee_with_title_mtime_and_files() {
    let root = temp_path("searchee");
    let release = root.join("Example Show");
    fs::create_dir_all(&release).expect("root");
    let episode = release.join("Example.Show.S01E02.mkv");
    let subtitle = release.join("Example.Show.S01E02.srt");
    fs::write(&episode, b"video bytes").expect("episode");
    fs::write(&subtitle, b"sub").expect("subtitle");

    let searchee = create_searchee_from_path(&release)
        .expect("create")
        .expect("searchee");

    assert_eq!(searchee.title, "Example Show S01E02");
    assert_eq!(searchee.media_type, MediaType::Episode);
    assert_eq!(searchee.files.len(), 2);
    assert!(searchee.mtime_millis.is_some());
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn changed_path_maps_to_parents_within_max_depth() {
    let data_dir = PathBuf::from("/data");
    let changed = PathBuf::from("/data/show/season/episode.mkv");

    let affected = affected_roots_for_changed_path(&data_dir, &changed, 2);

    assert_eq!(
        affected,
        vec![
            PathBuf::from("/data/show/season"),
            PathBuf::from("/data/show"),
            PathBuf::from("/data")
        ]
    );
}

#[test]
fn indexes_torrent_dir_and_prunes_removed_files() {
    let root = temp_path("torrent-dir");
    let torrent_dir = root.join("torrents");
    fs::create_dir_all(&torrent_dir).expect("torrent dir");
    let first = torrent_dir.join("first.torrent");
    let second = torrent_dir.join("second.torrent");
    fs::write(&first, torrent_bytes("First.Release", 10)).expect("first");
    fs::write(&second, torrent_bytes("Second.Release", 20)).expect("second");
    let database = Database::open_app_dir(&root).expect("database");

    let result = index_torrent_dir(&database, &torrent_dir).expect("index");

    assert_eq!(result.files_seen, 2);
    assert_eq!(result.torrents_indexed, 2);
    let count: i64 = database
        .query_scalar("SELECT COUNT(*) FROM torrent", &[])
        .expect("count");
    assert_eq!(count, 2);

    fs::remove_file(second).expect("remove second");
    fs::write(&first, torrent_bytes("First.Changed", 30)).expect("change first");
    let result = index_torrent_dir(&database, &torrent_dir).expect("reindex");

    assert_eq!(result.files_seen, 1);
    assert_eq!(result.torrents_indexed, 1);
    assert_eq!(result.torrents_removed, 1);
    let names: String = database
        .query_scalar(
            "SELECT name FROM torrent WHERE file_path = ?1",
            &[SqlValue::Text(Cow::Owned(first.display().to_string()))],
        )
        .expect("name");
    assert_eq!(names, "First.Changed");
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn invalid_torrent_dir_files_remove_stale_rows() {
    let root = temp_path("torrent-dir-invalid");
    let torrent_dir = root.join("torrents");
    fs::create_dir_all(&torrent_dir).expect("torrent dir");
    let path = torrent_dir.join("stale.torrent");
    fs::write(&path, torrent_bytes("Stale.Release", 10)).expect("torrent");
    let database = Database::open_app_dir(&root).expect("database");
    index_torrent_dir(&database, &torrent_dir).expect("index");

    fs::write(&path, b"not bencode").expect("invalid");
    let result = index_torrent_dir(&database, &torrent_dir).expect("reindex");

    assert_eq!(result.files_seen, 1);
    assert_eq!(result.files_failed, 1);
    let count: i64 = database
        .query_scalar("SELECT COUNT(*) FROM torrent", &[])
        .expect("count");
    assert_eq!(count, 0);
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn source_selection_prefers_explicit_torrents_and_adds_data_dirs() {
    let root = temp_path("source-selection");
    let torrent_dir = root.join("torrents");
    let data_dir = root.join("data");
    let release = data_dir.join("Example Show");
    fs::create_dir_all(&torrent_dir).expect("torrent dir");
    fs::create_dir_all(&release).expect("release dir");
    let explicit = root.join("explicit.torrent");
    let ignored = torrent_dir.join("ignored.torrent");
    fs::write(&explicit, torrent_bytes("Explicit.Release", 10)).expect("explicit");
    fs::write(&ignored, torrent_bytes("Ignored.Release", 10)).expect("ignored");
    fs::write(release.join("Example.Show.S01E01.mkv"), b"video").expect("video");

    let searchees = find_all_searchees(
        &SearcheeSources {
            torrents: Some(std::slice::from_ref(&explicit)),
            use_client_torrents: false,
            client_searchees: &[],
            torrent_dir: Some(&torrent_dir),
            data_dirs: std::slice::from_ref(&data_dir),
            max_data_depth: 2,
        },
        Label::Webhook,
    )
    .expect("sources");

    assert!(searchees.iter().any(|item| item.name == "Explicit.Release"));
    assert!(!searchees.iter().any(|item| item.name == "Ignored.Release"));
    assert!(
        searchees
            .iter()
            .any(|item| item.source() == crate::domain::SearcheeSource::DataDir)
    );
    assert!(
        searchees
            .iter()
            .all(|item| item.label == Some(Label::Webhook))
    );
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn parses_typed_blocklist_entries() {
    let blocklist = Blocklist::parse(&[
        "name:bad.release".to_owned(),
        "name_regex:(?i)evil".to_owned(),
        "category:blocked".to_owned(),
        "tag:".to_owned(),
        "tracker:tracker.example".to_owned(),
        "folder_regex:/downloads".to_owned(),
        "info_hash:0123456789abcdef0123456789abcdef01234567".to_owned(),
        "size_below:20".to_owned(),
    ])
    .expect("blocklist");
    let mut searchee = Searchee::from_files("Good", "Good", vec![File::new("Good.mkv", 10)]);
    searchee.client = Some(ClientTorrentMetadata::new(
        "client",
        "/downloads",
        Some(ClientLabel::new("blocked")),
        Vec::new(),
        vec!["tracker.example".into()],
    ));
    searchee.path = Some("/downloads/Good".into());
    searchee.info_hash = crate::domain::InfoHash::new("0123456789abcdef0123456789abcdef01234567");

    assert!(blocklist.matches_searchee(&searchee));
}

#[test]
fn rejects_unsupported_blocklist_entries() {
    let error = Blocklist::parse(&["folderRegex:/downloads".to_owned()])
        .expect_err("unsupported entry rejected");

    assert!(error.to_string().contains("invalid block_list entry type"));
}

#[test]
fn validates_blocklist_size_pair() {
    let error = Blocklist::parse(&["size_below:20".to_owned(), "size_above:10".to_owned()])
        .expect_err("inverted size range rejected");

    assert!(error.to_string().contains("size_below <= size_above"));
}

#[test]
fn content_filter_rejects_blocklisted_and_single_episode() {
    let blocklist = Blocklist::parse(&["name:blocked".to_owned()]).expect("blocklist");
    let options = filter_options(&blocklist);
    let mut blocked = Searchee::from_files(
        "Blocked.Release",
        "Blocked.Release",
        vec![File::new("Blocked.mkv", 10)],
    );
    blocked.media_type = MediaType::Video;

    assert_eq!(
        filter_by_content(&blocked, &options),
        Some(ContentFilterRejection::Blocklisted)
    );

    let empty = Blocklist::parse(&[]).expect("empty");
    let options = filter_options(&empty);
    let mut episode = Searchee::from_files(
        "Show.S01E02",
        "Show S01E02",
        vec![File::new("Show.S01E02.mkv", 10)],
    );
    episode.media_type = MediaType::Episode;

    assert_eq!(
        filter_by_content(&episode, &options),
        Some(ContentFilterRejection::SingleEpisode)
    );
}

#[test]
fn announce_allows_single_episode_but_non_video_ratio_can_reject() {
    let empty = Blocklist::parse(&[]).expect("empty");
    let mut options = filter_options(&empty);
    options.label = Some(Label::Announce);
    let mut searchee = Searchee::from_files(
        "Show.S01E02",
        "Show S01E02",
        vec![File::new("Show.S01E02.mkv", 10), File::new("extra.nfo", 10)],
    );
    searchee.media_type = MediaType::Episode;

    assert_eq!(
        filter_by_content(&searchee, &options),
        Some(ContentFilterRejection::NonVideoRatio)
    );
}

#[test]
fn content_filter_rejects_cross_seed_and_specials() {
    let empty = Blocklist::parse(&[]).expect("empty");
    let mut options = filter_options(&empty);
    options.ignore_cross_seeds = true;
    let mut searchee =
        Searchee::from_files("Release", "Release", vec![File::new("Release.mkv", 10)]);
    searchee.media_type = MediaType::Video;
    searchee.client = Some(ClientTorrentMetadata::new(
        "client",
        "/downloads",
        Some(ClientLabel::new("tv.cross-seed")),
        vec![ClientLabel::new("tag")],
        Vec::<Cow<'static, str>>::new(),
    ));

    assert_eq!(
        filter_by_content(&searchee, &options),
        Some(ContentFilterRejection::CrossSeed)
    );

    let mut specials = Searchee::from_files(
        "Show Specials",
        "Show Specials",
        vec![File::new("Show.S00E01.mkv", 10)],
    );
    specials.media_type = MediaType::Episode;
    let mut options = filter_options(&empty);
    options.include_single_episodes = true;
    assert_eq!(
        filter_by_content(&specials, &options),
        Some(ContentFilterRejection::Specials)
    );
}

fn filter_options<'a>(blocklist: &'a Blocklist) -> ContentFilterOptions<'a> {
    ContentFilterOptions {
        blocklist,
        blocklist_only: false,
        include_single_episodes: false,
        include_non_videos: false,
        fuzzy_size_threshold: 0.05,
        ignore_cross_seeds: false,
        link_category: None,
        label: Some(Label::Search),
    }
}

fn pipeline_options<'a>(
    blocklist: &'a Blocklist,
    exclude: &'a BTreeSet<String>,
    _root: &PathBuf,
    label: Label,
) -> SearchPipelineOptions<'a> {
    SearchPipelineOptions {
        label,
        filter: ContentFilterOptions {
            label: Some(label),
            ..filter_options(blocklist)
        },
        assessment: AssessmentOptions {
            match_mode: crate::config::MatchMode::Strict,
            fuzzy_size_threshold: 0.05,
            season_from_episodes: 1.0,
            include_single_episodes: true,
            info_hashes_to_exclude: exclude,
            blocklist,
        },
        snatch: SnatchOptions::default(),
        torznab: TorznabSearchOptions {
            now_millis: 1_000,
            ..TorznabSearchOptions::default()
        },
        arr_configs: &[],
        arr_timeout: None,
        virtual_season: None,
        exclude_older: None,
        exclude_recent_search: None,
    }
}

fn episode_searchee(episode: u32, mtime_millis: u64) -> Searchee<'static> {
    let title = format!("Example Show S01E{episode:02}");
    let mut searchee = Searchee::from_files(
        title.clone(),
        title,
        vec![File::new(format!("Example.Show.S01E{episode:02}.mkv"), 100)],
    );
    searchee.media_type = MediaType::Episode;
    searchee.mtime_millis = Some(mtime_millis);
    searchee
}

#[test]
fn duplicate_filter_prefers_info_hash_sources() {
    let mut with_hash =
        Searchee::from_files("Release A", "Same Title", vec![File::new("a.mkv", 10)]);
    with_hash.info_hash = crate::domain::InfoHash::new("0123456789abcdef0123456789abcdef01234567");
    let duplicate = Searchee::from_files("Release B", "Same Title", vec![File::new("b.mkv", 10)]);

    let filtered = filter_duplicate_searchees(vec![duplicate, with_hash]);

    assert_eq!(filtered.len(), 1);
    assert!(filtered[0].info_hash.is_some());
}

#[test]
fn timestamp_filter_honors_recent_old_and_virtual_freshness() {
    let timestamp = TimestampDecision {
        first_searched: 1_000,
        last_searched: 9_000,
    };

    assert!(timestamp_excludes(
        Some(timestamp),
        10_000,
        None,
        Some(2_000),
        None
    ));
    assert!(timestamp_excludes(
        Some(timestamp),
        10_000,
        Some(5_000),
        None,
        None
    ));
    assert!(!timestamp_excludes(
        Some(timestamp),
        10_000,
        Some(5_000),
        Some(2_000),
        Some(9_500)
    ));
    assert!(!timestamp_excludes(
        None,
        10_000,
        Some(5_000),
        Some(2_000),
        None
    ));
}

#[test]
fn media_caps_and_group_key_are_stable() {
    let caps = MediaCapabilities {
        tv: true,
        ..MediaCapabilities::default()
    };
    assert!(indexer_supports_media(MediaType::Episode, caps));
    assert!(!indexer_supports_media(MediaType::Movie, caps));

    let mut searchee = Searchee::from_files(
        "Example.Show.S01E02.1080p",
        "Example Show S01E02",
        vec![File::new("Example.Show.S01E02.mkv", 10)],
    );
    searchee.media_type = MediaType::Episode;
    assert_eq!(search_group_key(&searchee), "example.show.s01.e02");
    let mut decorated = Searchee::from_files(
        "Example.Show.S01E02.1080p.WEB-DL",
        "Example Show S01E02 1080p WEB-DL",
        vec![File::new("Example.Show.S01E02.1080p.WEB-DL.mkv", 10)],
    );
    decorated.media_type = MediaType::Episode;
    assert_eq!(search_group_key(&decorated), search_group_key(&searchee));
}

#[test]
fn searchable_pipeline_filters_virtuals_and_dispatches_cached_candidates() {
    let root = temp_path("bulk-pipeline");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let exclude = BTreeSet::new();
    let mut options = pipeline_options(&blocklist, &exclude, &root, Label::Search);
    options.filter.include_single_episodes = true;
    options.virtual_season = Some(VirtualSeasonOptions {
        season_from_episodes: 0.5,
        use_filters: true,
        now_millis: 1_000 + 9 * 24 * 60 * 60 * 1000,
    });

    let searchees = (1..=3)
        .map(|episode| episode_searchee(episode, 1_000))
        .collect::<Vec<_>>();
    let searchable = find_searchable_searchees(searchees, &[], 1, &options).expect("searchable");

    assert_eq!(searchable.len(), 4);
    assert!(
        searchable
            .iter()
            .any(|item| item.media_type == MediaType::Pack)
    );

    let target = searchable
        .iter()
        .find(|item| item.title.as_ref() == "Example Show S01E01")
        .expect("target");
    let searchee_id = database
        .get_or_insert_searchee(target.title.as_ref())
        .expect("searchee");
    database
        .upsert_decision(&DecisionRecord {
            searchee_id,
            guid: "guid-1",
            info_hash: None,
            decision: Decision::Match,
            first_seen: 1_000,
            last_seen: 1_000,
            fuzzy_size_factor: 0.05,
        })
        .expect("decision");

    let mut cache = CandidateSearchCache::default();
    cache.entries.insert(
        (search_group_key(target), 7),
        CachedCandidates {
            ids_key: search_group_key(target),
            candidates: vec![Candidate::new(
                "Example.Show.S01E01",
                "guid-1",
                None::<String>,
                "tracker",
            )],
        },
    );
    let indexer = SearchIndexer {
        id: 7,
        url: "https://indexer.example/api".to_owned(),
        apikey: "secret".to_owned(),
        caps: TorznabCaps {
            search: true,
            tv_search: true,
            ..TorznabCaps::default()
        },
    };
    database
        .execute_sql(
            "INSERT INTO indexer (id, url, apikey, active)
                 VALUES (?1, ?2, ?3, 1)",
            &[
                SqlValue::I64(indexer.id),
                SqlValue::Text(Cow::Borrowed(indexer.url.as_str())),
                SqlValue::Text(Cow::Borrowed(indexer.apikey.as_str())),
            ],
        )
        .expect("indexer");
    let mut actions = 0;
    let mut notifications = 0;
    let mut runtime = SearchPipelineRuntime {
        database: &database,
        app_dir: &root,
        options: &options,
        cache: &mut cache,
    };
    let summary = bulk_search(
        &mut runtime,
        std::slice::from_ref(target),
        &[indexer],
        |action| {
            assert_eq!(action.label, Label::Search);
            assert_eq!(action.assessment.decision, Decision::Match);
            actions += 1;
            Ok(Some(ActionResult::Save(SaveResult::Saved)))
        },
        |_| {
            notifications += 1;
            Ok(())
        },
    )
    .expect("bulk search");

    assert_eq!(summary.indexer_searches, 0);
    assert_eq!(summary.candidates_assessed, 1);
    assert_eq!(
        summary.attempts[0].action_result,
        Some(ActionResult::Save(SaveResult::Saved))
    );
    assert_eq!(actions, 1);
    assert_eq!(notifications, 1);
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn bulk_search_waits_between_real_indexer_requests() {
    let server = http_server(vec![
        rss_response("<rss><channel></channel></rss>"),
        rss_response("<rss><channel></channel></rss>"),
    ]);
    let root = temp_path("bulk-search-delay");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let exclude = BTreeSet::new();
    let mut options = pipeline_options(&blocklist, &exclude, &root, Label::Search);
    options.filter.include_single_episodes = true;
    options.filter.include_non_videos = true;
    options.torznab.delay = Duration::from_millis(20);
    let indexer = SearchIndexer {
        id: 7,
        url: format!("{}/api", server.url),
        apikey: "secret".to_owned(),
        caps: TorznabCaps {
            tv_search: true,
            ..TorznabCaps::default()
        },
    };
    database
        .execute_sql(
            "INSERT INTO indexer (id, url, apikey, active)
                 VALUES (?1, ?2, ?3, 1)",
            &[
                SqlValue::I64(indexer.id),
                SqlValue::Text(Cow::Borrowed(indexer.url.as_str())),
                SqlValue::Text(Cow::Borrowed(indexer.apikey.as_str())),
            ],
        )
        .expect("indexer");
    let searchees = vec![episode_searchee(1, 1_000), episode_searchee(2, 1_000)];
    let mut cache = CandidateSearchCache::default();
    let mut runtime = SearchPipelineRuntime {
        database: &database,
        app_dir: &root,
        options: &options,
        cache: &mut cache,
    };

    let summary = bulk_search(
        &mut runtime,
        &searchees,
        &[indexer],
        |_| Ok(None),
        |_| Ok(()),
    )
    .expect("bulk search");

    assert_eq!(summary.indexer_searches, 2);
    let requests = server.join();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].raw.contains("/api?apikey=secret&t=tvsearch"));
    assert!(
        requests[1]
            .accepted_at
            .duration_since(requests[0].accepted_at)
            >= options.torznab.delay
    );
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn bulk_search_counts_search_limit_per_indexer_batch() {
    let server = http_server(vec![rss_response(
        r#"<rss><channel>
              <item><title>Example.Show.S01E01</title><guid>guid-1</guid><indexer>tracker</indexer></item>
            </channel></rss>"#,
    )]);
    let root = temp_path("bulk-search-limit");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let exclude = BTreeSet::new();
    let mut options = pipeline_options(&blocklist, &exclude, &root, Label::Search);
    options.filter.include_single_episodes = true;
    options.filter.include_non_videos = true;
    options.torznab.search_limit = Some(1);
    let indexer = SearchIndexer {
        id: 7,
        url: format!("{}/api", server.url),
        apikey: "secret".to_owned(),
        caps: TorznabCaps {
            tv_search: true,
            ..TorznabCaps::default()
        },
    };
    database
        .execute_sql(
            "INSERT INTO indexer (id, url, apikey, active)
                 VALUES (?1, ?2, ?3, 1)",
            &[
                SqlValue::I64(indexer.id),
                SqlValue::Text(Cow::Borrowed(indexer.url.as_str())),
                SqlValue::Text(Cow::Borrowed(indexer.apikey.as_str())),
            ],
        )
        .expect("indexer");
    let searchees = vec![episode_searchee(1, 1_000), episode_searchee(2, 1_000)];
    let mut cache = CandidateSearchCache::default();
    let mut runtime = SearchPipelineRuntime {
        database: &database,
        app_dir: &root,
        options: &options,
        cache: &mut cache,
    };

    let summary = bulk_search(
        &mut runtime,
        &searchees,
        &[indexer],
        |_| Ok(None),
        |_| Ok(()),
    )
    .expect("bulk search");

    assert_eq!(summary.indexer_searches, 1);
    assert_eq!(summary.candidates_assessed, 1);
    let requests = server.join();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].raw.contains("/api?apikey=secret&t=tvsearch"));
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn reverse_lookup_filters_sorts_and_stops_after_success() {
    let root = temp_path("reverse-pipeline");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let exclude = BTreeSet::new();
    let mut options = pipeline_options(&blocklist, &exclude, &root, Label::Rss);
    options.filter.include_single_episodes = true;
    let candidate = Candidate::new("Example.Show.S01E01", "guid-rss", None::<String>, "tracker");
    let mut client = episode_searchee(1, 1_000);
    client.client = Some(ClientTorrentMetadata::new(
        "client-a",
        "/downloads",
        None,
        Vec::new(),
        Vec::<Cow<'static, str>>::new(),
    ));
    let unrelated = Searchee::from_files(
        "Other.Movie.2020",
        "Other Movie 2020",
        vec![File::new("movie.mkv", 1)],
    );
    let local = vec![unrelated, client];

    let matches = reverse_lookup_searchees(&candidate, &local, &options.filter);
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].title, "Example Show S01E01");

    let searchee_id = database
        .get_or_insert_searchee(matches[0].title.as_ref())
        .expect("searchee");
    database
        .upsert_decision(&DecisionRecord {
            searchee_id,
            guid: "guid-rss",
            info_hash: None,
            decision: Decision::MatchSizeOnly,
            first_seen: 1_000,
            last_seen: 1_000,
            fuzzy_size_factor: 0.05,
        })
        .expect("decision");

    let gate = super::ReverseLookupGate::new();
    let mut actions = 0;
    let runtime = ReverseLookupRuntime {
        gate: &gate,
        database: &database,
        app_dir: &root,
        options: &options,
    };
    let attempt = check_new_candidate_match(
        &runtime,
        &candidate,
        &local,
        |_| {
            actions += 1;
            Ok(Some(ActionResult::Save(SaveResult::Saved)))
        },
        |_| Ok(()),
    )
    .expect("reverse lookup")
    .expect("attempt");

    assert_eq!(attempt.decision, Decision::MatchSizeOnly);
    assert_eq!(actions, 1);
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn reverse_lookup_gates_share_one_runtime_permit() {
    let first = super::ReverseLookupGate::new();
    let second = super::ReverseLookupGate::new();
    let _held = first.permit.lock().expect("first gate");

    second.permit.try_lock().unwrap_err();
}

#[test]
fn reverse_lookup_uses_cached_client_rows() {
    let root = temp_path("reverse-client-cache");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let exclude = BTreeSet::new();
    let mut options = pipeline_options(&blocklist, &exclude, &root, Label::Rss);
    options.filter.include_single_episodes = true;
    let files = [File::new("Example.Show.S01E01.mkv", 10)];
    let mut cached =
        Searchee::from_files("Example.Show.S01E01", "Example Show S01E01", files.to_vec());
    cached.media_type = MediaType::Episode;
    let lookup = lookup_fields(&cached);
    database
        .upsert_client_searchee(&ClientSearcheeRecord {
            client_host: "client-a",
            info_hash: "0123456789abcdef0123456789abcdef01234567",
            name: "Example.Show.S01E01",
            title: "Example Show S01E01",
            files: &files,
            length: 10,
            save_path: "/downloads",
            category: None,
            tags: &[],
            trackers: &[Cow::Borrowed("tracker.example")],
            lookup: Some(&lookup),
        })
        .expect("client searchee");
    database
            .execute_sql(
                "INSERT INTO client_searchee
                    (client_host, info_hash, name, title, files, length, save_path, trackers, search_key, media_type, season, episode, file_count, video_bytes, non_video_bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                &[
                    SqlValue::Text(Cow::Borrowed("client-a")),
                    SqlValue::Text(Cow::Borrowed("fedcba9876543210fedcba9876543210fedcba98")),
                    SqlValue::Text(Cow::Borrowed("Example.Show.S01E01.poison")),
                    SqlValue::Text(Cow::Borrowed("Example Show S01E01")),
                    SqlValue::Text(Cow::Borrowed("not-json")),
                    SqlValue::I64(10),
                    SqlValue::Text(Cow::Borrowed("/downloads")),
                    SqlValue::Text(Cow::Borrowed("[]")),
                    SqlValue::Text(Cow::Borrowed("other.show.s01e01")),
                    SqlValue::Text(Cow::Borrowed("episode")),
                    SqlValue::I64(1),
                    SqlValue::I64(1),
                    SqlValue::I64(1),
                    SqlValue::I64(10),
                    SqlValue::I64(0),
                ],
            )
            .expect("poison row");
    let searchee_id = database
        .get_or_insert_searchee("Example Show S01E01")
        .expect("searchee");
    database
        .upsert_decision(&DecisionRecord {
            searchee_id,
            guid: "guid-db",
            info_hash: None,
            decision: Decision::MatchSizeOnly,
            first_seen: 1_000,
            last_seen: 1_000,
            fuzzy_size_factor: 0.05,
        })
        .expect("decision");
    let gate = super::ReverseLookupGate::new();
    let runtime = ReverseLookupRuntime {
        gate: &gate,
        database: &database,
        app_dir: &root,
        options: &options,
    };
    let candidate = Candidate::new("Example.Show.S01E01", "guid-db", None::<String>, "tracker");
    let mut actions = 0;

    let attempt = check_new_candidate_match(
        &runtime,
        &candidate,
        &[],
        |_| {
            actions += 1;
            Ok(Some(ActionResult::Save(SaveResult::Saved)))
        },
        |_| Ok(()),
    )
    .expect("reverse lookup")
    .expect("attempt");

    assert_eq!(attempt.decision, Decision::MatchSizeOnly);
    assert_eq!(attempt.searchee_client_host.as_deref(), Some("client-a"));
    assert_eq!(actions, 1);
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn reverse_lookup_data_selector_skips_nonmatching_lookup_rows() {
    let root = temp_path("reverse-data-selector");
    let release = root.join("Example.Show.S01E01");
    fs::create_dir_all(&release).expect("release dir");
    fs::write(release.join("Example.Show.S01E01.mkv"), b"video").expect("video");
    let database = Database::open_app_dir(&root).expect("database");
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let exclude = BTreeSet::new();
    let mut options = pipeline_options(&blocklist, &exclude, &root, Label::Rss);
    options.filter.include_single_episodes = true;
    let mut other = Searchee::from_files(
        "Other.Show.S01E01",
        "Other Show S01E01",
        vec![File::new("Other.Show.S01E01.mkv", 5)],
    );
    other.media_type = MediaType::Episode;
    let lookup = lookup_fields(&other);
    database
        .upsert_data_root(&DataRootRecord {
            path: release.to_str().expect("utf-8 path"),
            title: "Example Show S01E01",
            lookup: Some(&lookup),
        })
        .expect("data root");
    let candidate = Candidate::new(
        "Example.Show.S01E01",
        "guid-data",
        None::<String>,
        "tracker",
    );
    let keys = reverse_lookup_keys(candidate.name.as_ref());

    let rows = reverse_lookup_data_rows(&database, &candidate, &keys, &options.filter)
        .expect("data reverse lookup");

    assert!(rows.is_empty());
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn reverse_lookup_selectors_skip_large_unrelated_caches() {
    let root = temp_path("reverse-large-selector");
    let data = root.join("data");
    fs::create_dir_all(&data).expect("data dir");
    let database = Database::open_app_dir(&root).expect("database");
    let blocklist = Blocklist::parse(&[]).expect("blocklist");
    let exclude = BTreeSet::new();
    let mut options = pipeline_options(&blocklist, &exclude, &root, Label::Rss);
    options.filter.include_single_episodes = true;

    for index in 0..300 {
        let info_hash = format!("{index:040x}");
        let data_path = data.join(format!("missing-{index}"));
        database
                .execute_sql(
                    "INSERT INTO client_searchee
                        (client_host, info_hash, name, title, files, length, save_path, trackers, search_key, media_type, season, episode, file_count, video_bytes, non_video_bytes)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                    &[
                        SqlValue::Text(Cow::Borrowed("client-a")),
                        SqlValue::Text(Cow::Owned(info_hash)),
                        SqlValue::Text(Cow::Borrowed("Example.Show.S01E01.poison")),
                        SqlValue::Text(Cow::Borrowed("Example Show S01E01")),
                        SqlValue::Text(Cow::Borrowed("not-json")),
                        SqlValue::I64(10),
                        SqlValue::Text(Cow::Borrowed("/downloads")),
                        SqlValue::Text(Cow::Borrowed("[]")),
                        SqlValue::Text(Cow::Borrowed("other.show.s01e01")),
                        SqlValue::Text(Cow::Borrowed("episode")),
                        SqlValue::I64(1),
                        SqlValue::I64(1),
                        SqlValue::I64(1),
                        SqlValue::I64(10),
                        SqlValue::I64(0),
                    ],
                )
                .expect("client poison row");
        database
                .execute_sql(
                    "INSERT INTO data
                        (path, title, search_key, media_type, season, episode, length, file_count, video_bytes, non_video_bytes)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    &[
                        SqlValue::Text(Cow::Owned(data_path.display().to_string())),
                        SqlValue::Text(Cow::Borrowed("Example Show S01E01")),
                        SqlValue::Text(Cow::Borrowed("other.show.s01e01")),
                        SqlValue::Text(Cow::Borrowed("episode")),
                        SqlValue::I64(1),
                        SqlValue::I64(1),
                        SqlValue::I64(10),
                        SqlValue::I64(1),
                        SqlValue::I64(10),
                        SqlValue::I64(0),
                    ],
                )
                .expect("data poison row");
    }

    let false_positive = data.join("Different.Show.S01E01");
    fs::create_dir_all(&false_positive).expect("false-positive dir");
    fs::write(false_positive.join("Different.Show.S01E01.mkv"), b"video")
        .expect("false-positive file");
    database
            .execute_sql(
                "INSERT INTO client_searchee
                    (client_host, info_hash, name, title, files, length, save_path, trackers, search_key, media_type, season, episode, file_count, video_bytes, non_video_bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                &[
                    SqlValue::Text(Cow::Borrowed("client-a")),
                    SqlValue::Text(Cow::Borrowed("ffffffffffffffffffffffffffffffffffffffff")),
                    SqlValue::Text(Cow::Borrowed("Different.Show.S01E01")),
                    SqlValue::Text(Cow::Borrowed("Different Show S01E01")),
                    SqlValue::Text(Cow::Borrowed(
                        r#"[{"name":"Different.Show.S01E01.mkv","path":"Different.Show.S01E01.mkv","length":5}]"#,
                    )),
                    SqlValue::I64(5),
                    SqlValue::Text(Cow::Borrowed("/downloads")),
                    SqlValue::Text(Cow::Borrowed("[]")),
                    SqlValue::Text(Cow::Borrowed("example.show.s01e01")),
                    SqlValue::Text(Cow::Borrowed("episode")),
                    SqlValue::I64(1),
                    SqlValue::I64(1),
                    SqlValue::I64(1),
                    SqlValue::I64(5),
                    SqlValue::I64(0),
                ],
            )
            .expect("client false-positive row");
    database
            .execute_sql(
                "INSERT INTO data
                    (path, title, search_key, media_type, season, episode, length, file_count, video_bytes, non_video_bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                &[
                    SqlValue::Text(Cow::Owned(false_positive.display().to_string())),
                    SqlValue::Text(Cow::Borrowed("Different Show S01E01")),
                    SqlValue::Text(Cow::Borrowed("example.show.s01e01")),
                    SqlValue::Text(Cow::Borrowed("episode")),
                    SqlValue::I64(1),
                    SqlValue::I64(1),
                    SqlValue::I64(5),
                    SqlValue::I64(1),
                    SqlValue::I64(5),
                    SqlValue::I64(0),
                ],
            )
            .expect("data false-positive row");

    let candidate = Candidate::new(
        "Example.Show.S01E01",
        "guid-large",
        None::<String>,
        "tracker",
    );
    let keys = reverse_lookup_keys(candidate.name.as_ref());
    let client_rows = reverse_lookup_client_rows(&database, &candidate, &keys, &options.filter)
        .expect("client reverse lookup");
    let data_rows = reverse_lookup_data_rows(&database, &candidate, &keys, &options.filter)
        .expect("data reverse lookup");

    assert!(client_rows.is_empty());
    assert!(data_rows.is_empty());
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn builds_virtual_season_from_episode_searchees() {
    let episodes = (1..=3)
        .map(|episode| {
            let title = format!("Example Show S01E{episode:02}");
            let mut searchee = Searchee::from_files(
                title.clone(),
                title,
                vec![File::new(format!("Example.Show.S01E{episode:02}.mkv"), 100)],
            );
            searchee.media_type = MediaType::Episode;
            searchee.mtime_millis = Some(1_000);
            searchee.client = Some(ClientTorrentMetadata::new(
                "client-a",
                "/downloads",
                None,
                Vec::new(),
                Vec::<Cow<'static, str>>::new(),
            ));
            searchee
        })
        .collect::<Vec<_>>();

    let virtuals = create_virtual_season_searchees(
        &episodes,
        VirtualSeasonOptions {
            season_from_episodes: 0.5,
            use_filters: true,
            now_millis: 1_000 + 9 * 24 * 60 * 60 * 1000,
        },
    );

    assert_eq!(virtuals.len(), 1);
    assert_eq!(virtuals[0].title, "Example Show S01");
    assert_eq!(virtuals[0].media_type, MediaType::Pack);
    assert_eq!(virtuals[0].length, 300);
    assert_eq!(
        virtuals[0]
            .client
            .as_ref()
            .map(|client| client.host.as_ref()),
        Some("client-a")
    );
}

#[test]
fn virtual_seasons_respect_existing_pack_ratio_and_age() {
    let mut pack = Searchee::from_files(
        "Example Show S01",
        "Example Show S01",
        vec![File::new("pack.mkv", 1)],
    );
    pack.media_type = MediaType::Pack;
    let mut episode = Searchee::from_files(
        "Example Show S01E01",
        "Example Show S01E01",
        vec![File::new("e1.mkv", 1)],
    );
    episode.media_type = MediaType::Episode;
    episode.mtime_millis = Some(1_000);

    assert!(
        create_virtual_season_searchees(
            &[pack, episode],
            VirtualSeasonOptions {
                season_from_episodes: 0.5,
                use_filters: true,
                now_millis: 1_000 + 9 * 24 * 60 * 60 * 1000,
            },
        )
        .is_empty()
    );
}

fn torrent_bytes(name: &str, length: u64) -> Vec<u8> {
    format!(
            "d4:infod6:lengthi{length}e4:name{}:{name}12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
            name.len()
        )
        .into_bytes()
}

fn test_attempt(index: usize) -> PipelineAttempt {
    PipelineAttempt {
        label: Label::Search,
        searchee_title: "Local".to_owned(),
        candidate_name: "Candidate".to_owned(),
        candidate_guid: format!("guid-{index}"),
        candidate_info_hashes: Vec::new(),
        trackers: Vec::new(),
        decision: Decision::Match,
        action_result: None,
        searchee_category: None,
        searchee_tags: Vec::new(),
        searchee_trackers: Vec::new(),
        searchee_length: 0,
        searchee_client_host: None,
        searchee_info_hash: None,
        searchee_path: None,
        searchee_source_type: "data".to_owned(),
    }
}

fn temp_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("sporos-search-{label}-{nanos}"))
}

fn rss_response(body: &str) -> String {
    let mut response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n", body.len());
    response.push_str("Content-Type: application/rss+xml\r\n\r\n");
    response.push_str(body);
    response
}

#[derive(Debug)]
struct TestRequest {
    raw: String,
    accepted_at: Instant,
}

struct TestServer {
    url: String,
    requests: Arc<Mutex<Vec<TestRequest>>>,
    handle: thread::JoinHandle<()>,
}

impl TestServer {
    fn join(self) -> Vec<TestRequest> {
        self.handle.join().expect("server thread");
        Arc::try_unwrap(self.requests)
            .expect("requests still shared")
            .into_inner()
            .expect("requests lock")
    }
}

fn http_server(responses: Vec<String>) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
    let url = format!("http://{}", listener.local_addr().expect("local addr"));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let server_requests = Arc::clone(&requests);
    let handle = thread::spawn(move || {
        for response in responses {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut buffer = [0; 4096];
            let read = stream.read(&mut buffer).expect("read request");
            server_requests
                .lock()
                .expect("requests lock")
                .push(TestRequest {
                    raw: String::from_utf8_lossy(&buffer[..read]).into_owned(),
                    accepted_at: Instant::now(),
                });
            stream
                .write_all(response.as_bytes())
                .expect("write response");
        }
    });
    TestServer {
        url,
        requests,
        handle,
    }
}
