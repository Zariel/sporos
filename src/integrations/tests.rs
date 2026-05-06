use super::{
    ArrKind, CategoryCaps, LimitCaps, RssPagerOptions, SearchIndexer, SnatchHistory, SnatchOptions,
    SnatchResult, TorznabCaps, TorznabConfig, TorznabQuery, TorznabSearchIds, TorznabSearchOptions,
    arr_search_cache_key, cache_torrent_file, create_torznab_search_queries, enabled_indexers,
    enabled_search_indexers, fetch_torznab_caps, for_each_rss_page, get_cached_torrent,
    guid_lookup, ids_for_torznab_caps, lookup_arr_ids, parse_retry_after_delay_millis,
    parse_torznab_caps, parse_torznab_rss, rss_pager, search_torznab_indexer, set_indexer_status,
    snatch, snatch_once, sync_torznab_indexers, torznab_request_url, update_indexer_caps,
    validate_arr_instance, validate_arr_url, validate_torznab_url,
};
use crate::{
    domain::{Candidate, Decision, File, MediaType, Searchee},
    persistence::{Database, DecisionRecord, SqlValue},
};
use sqlx::Row;
use std::{
    borrow::Cow,
    fs,
    io::{Read, Write},
    net::TcpListener,
    path::PathBuf,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[test]
fn validates_and_sanitizes_torznab_urls() {
    let parsed =
        validate_torznab_url("https://indexer.example/api?apikey=secret&x=1").expect("url");

    assert_eq!(parsed.url, "https://indexer.example/api");
    assert_eq!(parsed.apikey, "secret");
    let _error =
        validate_torznab_url("https://indexer.example/search?apikey=secret").expect_err("path");
    let _error = validate_torznab_url("https://indexer.example/api").expect_err("apikey");
}

#[test]
fn parses_caps_xml() {
    let caps = parse_torznab_caps(
            r#"
            <caps>
              <limits default="50" max="200" />
              <searching searchAvailable="yes" tv-searchAvailable="yes" movie-searchAvailable="no" />
              <tv-search supportedParams="q,season,ep,tvdbid" />
              <movie-search supportedParams="q,imdbid" />
              <categories>
                <category id="5000" name="TV" />
                <category id="2000" name="Movies" />
                <category id="7000" name="Books" />
                <category id="1000" name="Other" />
              </categories>
            </caps>
            "#,
        )
        .expect("caps");

    assert!(caps.search);
    assert!(caps.tv_search);
    assert!(!caps.movie_search);
    assert_eq!(caps.tv_ids, vec!["q", "season", "ep", "tvdbid"]);
    assert!(caps.categories.tv);
    assert!(caps.categories.movie);
    assert!(caps.categories.book);
    assert!(caps.categories.additional);
    assert_eq!(caps.limits.default, 50);
    assert_eq!(caps.limits.max, 200);
}

#[test]
fn syncs_caps_and_enabled_indexers() {
    let root = temp_path("indexers");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let first = validate_torznab_url("https://one.example/api?apikey=one").expect("one");
    let second = validate_torznab_url("https://two.example/api?apikey=two").expect("two");

    let result = sync_torznab_indexers(&database, &[first.clone(), second]).expect("sync");
    assert_eq!(result.inserted, 2);
    let result = sync_torznab_indexers(&database, std::slice::from_ref(&first)).expect("sync");
    assert_eq!(result.updated, 1);
    assert_eq!(result.deactivated, 1);
    let fresh = enabled_search_indexers(&database, 1_000).expect("fresh enabled");
    assert_eq!(fresh.len(), 1);
    assert!(fresh[0].caps.search);
    assert!(fresh[0].caps.tv_search);
    assert!(fresh[0].caps.movie_search);

    let id = database.indexer_id(&first.url).expect("id");
    let caps = parse_torznab_caps(
            r#"<caps><searching searchAvailable="yes" /><categories><category id="5000" name="TV" /></categories></caps>"#,
        )
        .expect("caps");
    update_indexer_caps(&database, id, &caps).expect("update caps");
    let refreshed = enabled_search_indexers(&database, 1_000).expect("refreshed enabled");
    assert_eq!(refreshed.len(), 1);
    assert!(refreshed[0].caps.search);
    assert!(!refreshed[0].caps.tv_search);
    assert!(refreshed[0].caps.categories.tv);
    assert_eq!(
        enabled_indexers(&database, 1_000).expect("enabled").len(),
        1
    );
    set_indexer_status(&database, id, Some("RATE_LIMITED"), Some(2_000)).expect("status");
    assert!(
        enabled_indexers(&database, 1_000)
            .expect("enabled")
            .is_empty()
    );
    assert_eq!(
        enabled_indexers(&database, 3_000).expect("enabled").len(),
        1
    );

    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn fetch_torznab_caps_retries_transient_status() {
    let server = http_server(vec![
        http_response("502 Bad Gateway", &[], ""),
        http_response(
            "200 OK",
            &[("Content-Type", "application/xml")],
            r#"<caps><searching searchAvailable="yes" /><limits default="25" max="100" /></caps>"#,
        ),
    ]);
    let indexer = TorznabConfig {
        url: format!("{}/api", server.url),
        apikey: "key".to_owned(),
    };

    let caps = fetch_torznab_caps(&indexer).expect("caps");

    assert!(caps.search);
    assert_eq!(caps.limits.default, 25);
    let requests = server.join();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].contains("apikey=key"));
}

#[test]
fn builds_media_aware_torznab_queries_and_urls() {
    let mut caps = TorznabCaps {
        search: true,
        tv_search: true,
        movie_search: true,
        tv_ids: vec!["tvdbid".to_owned()],
        movie_ids: vec!["imdbid".to_owned()],
        categories: CategoryCaps {
            tv: true,
            movie: true,
            ..CategoryCaps::default()
        },
        limits: LimitCaps::default(),
        ..TorznabCaps::default()
    };
    let mut episode = Searchee::from_files(
        "Example.Show.S01E02.1080p.WEB-DL-GRP",
        "Example.Show.S01E02",
        vec![File::new("Example.Show.S01E02.mkv", 10)],
    );
    episode.media_type = MediaType::Episode;
    let ids = TorznabSearchIds {
        tvdbid: Some("1234".to_owned()),
        ..TorznabSearchIds::default()
    };

    let queries = create_torznab_search_queries(&episode, &caps, Some(&ids));

    assert_eq!(queries.len(), 1);
    assert!(
        queries[0]
            .params
            .contains(&("t".to_owned(), "tvsearch".to_owned()))
    );
    assert!(
        queries[0]
            .params
            .contains(&("season".to_owned(), "01".to_owned()))
    );
    assert!(
        queries[0]
            .params
            .contains(&("ep".to_owned(), "02".to_owned()))
    );
    assert!(
        queries[0]
            .params
            .contains(&("tvdbid".to_owned(), "1234".to_owned()))
    );
    assert!(!queries[0].params.iter().any(|(key, _)| key == "q"));

    caps.tv_search = false;
    let queries = create_torznab_search_queries(&episode, &caps, Some(&ids));
    assert!(queries.is_empty());

    let indexer = SearchIndexer {
        id: 7,
        url: "https://indexer.example/api".to_owned(),
        apikey: "secret".to_owned(),
        caps,
    };
    let url = torznab_request_url(
        &indexer,
        &TorznabQuery {
            params: vec![
                ("t".to_owned(), "search".to_owned()),
                ("q".to_owned(), "a b".to_owned()),
            ],
        },
    )
    .expect("url");
    assert_eq!(
        url,
        "https://indexer.example/api?apikey=secret&t=search&q=a+b"
    );
}

#[test]
fn parses_torznab_rss_candidates() {
    let candidates = parse_torznab_rss(
        r#"
            <rss><channel>
              <item>
                <title>Example.Release</title>
                <guid>guid-1</guid>
                <link>https://indexer.example/download/1</link>
                <size>12345</size>
                <pubDate>Thu, 01 Jan 1970 00:00:02 +0000</pubDate>
                <prowlarrindexer>TrackerOne</prowlarrindexer>
              </item>
              <item>
                <title>Other.Release</title>
                <guid>guid-2</guid>
                <link>https://indexer.example/download/2</link>
              </item>
            </channel></rss>
            "#,
        42,
    )
    .expect("rss");

    assert_eq!(candidates.len(), 2);
    assert_eq!(candidates[0].name, "Example.Release");
    assert_eq!(candidates[0].guid, "guid-1");
    assert_eq!(
        candidates[0].link.as_deref(),
        Some("https://indexer.example/download/1")
    );
    assert_eq!(candidates[0].size, Some(12_345));
    assert_eq!(candidates[0].pub_date_millis, Some(2_000));
    assert_eq!(candidates[0].tracker, "TrackerOne");
    assert_eq!(candidates[0].indexer_id, Some(42));
    assert_eq!(candidates[1].tracker, "UnknownTracker");
}

#[test]
fn parses_retry_after_seconds_and_http_dates() {
    assert_eq!(parse_retry_after_delay_millis("2", 1_000), Some(2_000));
    assert_eq!(
        parse_retry_after_delay_millis("Thu, 01 Jan 1970 00:00:04 GMT", 1_000),
        Some(3_000)
    );
    assert_eq!(
        parse_retry_after_delay_millis("Thu, 01 Jan 1970 00:00:00 GMT", 1_000),
        Some(0)
    );
}

#[test]
fn searches_torznab_and_updates_indexer_name() {
    let server = http_server(vec![http_response(
        "200 OK",
        &[("Content-Type", "application/rss+xml")],
        r#"<rss><channel>
              <item><title>One</title><guid>g1</guid><link>https://idx/1</link><indexer>NamedTracker</indexer></item>
              <item><title>Two</title><guid>g2</guid><link>https://idx/2</link><indexer>NamedTracker</indexer></item>
            </channel></rss>"#,
    )]);
    let root = temp_path("torznab-search");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    database
        .execute_sql(
            "INSERT INTO indexer (url, apikey, active, search_cap) VALUES (?1, 'key', 1, 1)",
            &[SqlValue::Text(Cow::Owned(format!("{}/api", server.url)))],
        )
        .expect("indexer");
    let id = database
        .query_scalar("SELECT id FROM indexer", &[])
        .expect("id");
    let indexer = SearchIndexer {
        id,
        url: format!("{}/api", server.url),
        apikey: "key".to_owned(),
        caps: TorznabCaps::default(),
    };

    let candidates = search_torznab_indexer(
        &database,
        &indexer,
        &[TorznabQuery {
            params: vec![("t".to_owned(), "search".to_owned())],
        }],
        TorznabSearchOptions {
            search_limit: Some(1),
            ..TorznabSearchOptions::default()
        },
    )
    .expect("search");

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].guid, "g1");
    let name: String = database
        .query_scalar(
            "SELECT name FROM indexer WHERE id = ?1",
            &[SqlValue::I64(id)],
        )
        .expect("name");
    assert_eq!(name, "NamedTracker");
    let requests = server.join();
    assert!(requests[0].contains("/api?apikey=key&t=search"));
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn search_snoozes_rate_limited_indexers() {
    let server = http_server(vec![http_response(
        "429 Too Many Requests",
        &[("Retry-After", "2")],
        "",
    )]);
    let root = temp_path("torznab-rate");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    database
        .execute_sql(
            "INSERT INTO indexer (url, apikey, active, search_cap) VALUES (?1, 'key', 1, 1)",
            &[SqlValue::Text(Cow::Owned(format!("{}/api", server.url)))],
        )
        .expect("indexer");
    let id = database
        .query_scalar("SELECT id FROM indexer", &[])
        .expect("id");
    let indexer = SearchIndexer {
        id,
        url: format!("{}/api", server.url),
        apikey: "key".to_owned(),
        caps: TorznabCaps::default(),
    };

    let candidates = search_torznab_indexer(
        &database,
        &indexer,
        &[TorznabQuery {
            params: vec![("t".to_owned(), "search".to_owned())],
        }],
        TorznabSearchOptions {
            now_millis: 1_000,
            ..TorznabSearchOptions::default()
        },
    )
    .expect("search");

    assert!(candidates.is_empty());
    let (status, retry_after): (String, u64) = database
        .query_row(
            "SELECT status, retry_after FROM indexer WHERE id = ?1",
            &[SqlValue::I64(id)],
            |row| (row.get(0), row.get(1)),
        )
        .expect("status");
    assert_eq!(status, "RATE_LIMITED");
    assert_eq!(retry_after, 3_000);
    server.join();
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn search_retries_transient_status_per_indexer() {
    let server = http_server(vec![
        http_response("500 Internal Server Error", &[], ""),
        http_response(
            "200 OK",
            &[("Content-Type", "application/rss+xml")],
            r#"<rss><channel>
                  <item><title>One</title><guid>g1</guid><link>https://idx/1</link><indexer>NamedTracker</indexer></item>
                </channel></rss>"#,
        ),
    ]);
    let root = temp_path("torznab-search-retry");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    database
        .execute_sql(
            "INSERT INTO indexer (url, apikey, active, search_cap) VALUES (?1, 'key', 1, 1)",
            &[SqlValue::Text(Cow::Owned(format!("{}/api", server.url)))],
        )
        .expect("indexer");
    let id = database
        .query_scalar("SELECT id FROM indexer", &[])
        .expect("id");
    let indexer = SearchIndexer {
        id,
        url: format!("{}/api", server.url),
        apikey: "key".to_owned(),
        caps: TorznabCaps::default(),
    };

    let candidates = search_torznab_indexer(
        &database,
        &indexer,
        &[TorznabQuery {
            params: vec![("t".to_owned(), "search".to_owned())],
        }],
        TorznabSearchOptions::default(),
    )
    .expect("search");

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].guid, "g1");
    let requests = server.join();
    assert_eq!(requests.len(), 2);
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn rss_pager_stops_at_previous_cursor_and_persists_newest_guid() {
    let server = http_server(vec![http_response(
        "200 OK",
        &[("Content-Type", "application/rss+xml")],
        r#"<rss><channel>
              <item><title>New</title><guid>new-guid</guid><link>https://idx/new</link><pubDate>Thu, 01 Jan 1970 00:00:10 +0000</pubDate></item>
              <item><title>Old</title><guid>old-guid</guid><link>https://idx/old</link><pubDate>Thu, 01 Jan 1970 00:00:09 +0000</pubDate></item>
            </channel></rss>"#,
    )]);
    let root = temp_path("rss-cursor");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let indexer = insert_search_indexer(&database, &server.url, 2);
    database
        .update_rss_cursor(indexer.id, "old-guid")
        .expect("cursor");

    let candidates = rss_pager(
        &database,
        &indexer,
        RssPagerOptions {
            time_since_last_run: Duration::from_secs(60),
            now_millis: 20_000,
            ..RssPagerOptions::default()
        },
    )
    .expect("rss");

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].guid, "new-guid");
    let cursor: String = database
        .read_rss_cursor(indexer.id)
        .expect("cursor")
        .expect("cursor");
    assert_eq!(cursor, "new-guid");
    server.join();
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn rss_pager_uses_age_cutoff_when_previous_cursor_is_missing() {
    let server = http_server(vec![http_response(
        "200 OK",
        &[("Content-Type", "application/rss+xml")],
        r#"<rss><channel>
              <item><title>Fresh</title><guid>fresh</guid><link>https://idx/fresh</link><pubDate>Thu, 01 Jan 1970 00:00:10 +0000</pubDate></item>
              <item><title>Stale</title><guid>stale</guid><link>https://idx/stale</link><pubDate>Thu, 01 Jan 1970 00:00:04 +0000</pubDate></item>
            </channel></rss>"#,
    )]);
    let root = temp_path("rss-age");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let indexer = insert_search_indexer(&database, &server.url, 2);
    database
        .update_rss_cursor(indexer.id, "missing")
        .expect("cursor");

    let candidates = rss_pager(
        &database,
        &indexer,
        RssPagerOptions {
            time_since_last_run: Duration::from_secs(5),
            now_millis: 20_000,
            ..RssPagerOptions::default()
        },
    )
    .expect("rss");

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].guid, "fresh");
    server.join();
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn rss_pager_requests_offsets_until_empty_page() {
    let server = http_server(vec![
        http_response(
            "200 OK",
            &[("Content-Type", "application/rss+xml")],
            r#"<rss><channel>
                  <item><title>Only</title><guid>only</guid><link>https://idx/only</link><pubDate>Thu, 01 Jan 1970 00:00:10 +0000</pubDate></item>
                </channel></rss>"#,
        ),
        http_response(
            "200 OK",
            &[("Content-Type", "application/rss+xml")],
            "<rss><channel></channel></rss>",
        ),
    ]);
    let root = temp_path("rss-offsets");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let indexer = insert_search_indexer(&database, &server.url, 1);

    let candidates = rss_pager(
        &database,
        &indexer,
        RssPagerOptions {
            now_millis: 20_000,
            ..RssPagerOptions::default()
        },
    )
    .expect("rss");

    assert_eq!(candidates.len(), 1);
    let requests = server.join();
    assert!(requests[0].contains("limit=1"));
    assert!(requests[0].contains("offset=0"));
    assert!(requests[1].contains("offset=1"));
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn rss_page_handler_receives_pages_without_cross_feed_buffer() {
    let first = http_server(vec![
        http_response(
            "200 OK",
            &[("Content-Type", "application/rss+xml")],
            r#"<rss><channel>
                  <item><title>First A</title><guid>first-a</guid><link>https://idx/first-a</link><pubDate>Thu, 01 Jan 1970 00:00:10 +0000</pubDate></item>
                </channel></rss>"#,
        ),
        http_response(
            "200 OK",
            &[("Content-Type", "application/rss+xml")],
            "<rss><channel></channel></rss>",
        ),
    ]);
    let second = http_server(vec![
        http_response(
            "200 OK",
            &[("Content-Type", "application/rss+xml")],
            r#"<rss><channel>
                  <item><title>Second A</title><guid>second-a</guid><link>https://idx/second-a</link><pubDate>Thu, 01 Jan 1970 00:00:11 +0000</pubDate></item>
                </channel></rss>"#,
        ),
        http_response(
            "200 OK",
            &[("Content-Type", "application/rss+xml")],
            "<rss><channel></channel></rss>",
        ),
    ]);
    let root = temp_path("rss-pages");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let indexers = vec![
        insert_search_indexer(&database, &first.url, 1),
        insert_search_indexer(&database, &second.url, 1),
    ];
    let mut pages = Vec::new();

    let candidates = for_each_rss_page(
        &database,
        &indexers,
        RssPagerOptions {
            now_millis: 20_000,
            ..RssPagerOptions::default()
        },
        |page| {
            pages.push(
                page.iter()
                    .map(|candidate| candidate.guid.to_string())
                    .collect::<Vec<_>>(),
            );
            Ok(())
        },
    )
    .expect("rss");

    assert_eq!(candidates, 2);
    assert_eq!(pages, vec![vec!["first-a"], vec!["second-a"]]);
    assert_eq!(
        database
            .read_rss_cursor(indexers[0].id)
            .expect("cursor")
            .expect("cursor"),
        "first-a"
    );
    assert_eq!(
        database
            .read_rss_cursor(indexers[1].id)
            .expect("cursor")
            .expect("cursor"),
        "second-a"
    );
    first.join();
    second.join();
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn validates_arr_urls_and_instances() {
    let server = http_server(vec![http_response(
        "200 OK",
        &[("Content-Type", "application/json")],
        r#"{"current":"4.0.0"}"#,
    )]);
    let config = validate_arr_url(
        &format!("{}/sonarr?apikey=secret&ignored=1#frag", server.url),
        ArrKind::Sonarr,
    )
    .expect("config");

    assert_eq!(config.url, format!("{}/sonarr", server.url));
    assert_eq!(config.apikey, "secret");
    assert_eq!(config.kind, ArrKind::Sonarr);
    validate_arr_instance(&config, Some(Duration::from_secs(1))).expect("validate");
    let requests = server.join();
    assert!(requests[0].contains("GET /sonarr/api "));
    assert!(
        requests[0]
            .to_ascii_lowercase()
            .contains("x-api-key: secret")
    );
}

#[test]
fn validate_arr_instance_retries_transient_status() {
    let server = http_server(vec![
        http_response("503 Service Unavailable", &[], ""),
        http_response(
            "200 OK",
            &[("Content-Type", "application/json")],
            r#"{"current":"4.0.0"}"#,
        ),
    ]);
    let config = validate_arr_url(&format!("{}?apikey=secret", server.url), ArrKind::Sonarr)
        .expect("config");

    validate_arr_instance(&config, Some(Duration::from_secs(1))).expect("validate");

    let requests = server.join();
    assert_eq!(requests.len(), 2);
}

#[test]
fn looks_up_arr_ids_and_prepares_titles() {
    let server = http_server(vec![http_response(
        "200 OK",
        &[("Content-Type", "application/json")],
        r#"{"series":{"tvdbId":123,"tvMazeId":456,"imdbId":"tt123"}}"#,
    )]);
    let config = validate_arr_url(&format!("{}?apikey=secret", server.url), ArrKind::Sonarr)
        .expect("config");
    let mut searchee = Searchee::from_files(
        "Example.Show.S01E02",
        "Example.Show.S01E02",
        vec![File::new("Example.Show.S01E02.mkv", 10)],
    );
    searchee.media_type = MediaType::Episode;

    let lookup = lookup_arr_ids(&[config], &searchee, Some(Duration::from_secs(1)))
        .expect("lookup")
        .expect("ids");

    assert_eq!(lookup.query_title, "Example.Show.S01E02");
    assert_eq!(lookup.ids.tvdbid.as_deref(), Some("123"));
    assert_eq!(lookup.ids.tvmazeid.as_deref(), Some("456"));
    assert_eq!(lookup.ids.imdbid.as_deref(), Some("tt123"));
    assert!(lookup.cache_key.contains("tvdbid=123"));
    let requests = server.join();
    assert!(requests[0].contains("/api/v3/parse?title=Example.Show.S01E02"));
}

#[test]
fn arr_lookup_continues_after_retry_exhaustion() {
    let server = http_server(vec![
        http_response("500 Internal Server Error", &[], ""),
        http_response("500 Internal Server Error", &[], ""),
        http_response("500 Internal Server Error", &[], ""),
        http_response(
            "200 OK",
            &[("Content-Type", "application/json")],
            r#"{"movie":{"tmdbId":888,"imdbId":"tt888"}}"#,
        ),
    ]);
    let sonarr = validate_arr_url(&format!("{}?apikey=sonarr", server.url), ArrKind::Sonarr)
        .expect("sonarr");
    let radarr = validate_arr_url(&format!("{}?apikey=radarr", server.url), ArrKind::Radarr)
        .expect("radarr");
    let mut searchee = Searchee::from_files(
        "Loose.Video.1080p.WEB-DL-GRP",
        "Loose.Video.1080p.WEB-DL-GRP",
        vec![File::new("Loose.Video.1080p.WEB-DL-GRP.mkv", 10)],
    );
    searchee.media_type = MediaType::Video;

    let lookup = lookup_arr_ids(&[sonarr, radarr], &searchee, Some(Duration::from_secs(1)))
        .expect("lookup")
        .expect("ids");

    assert_eq!(lookup.ids.tmdbid.as_deref(), Some("888"));
    assert_eq!(lookup.ids.imdbid.as_deref(), Some("tt888"));
    let requests = server.join();
    assert_eq!(requests.len(), 4);
}

#[test]
fn arr_video_lookup_tries_sonarr_then_filters_ids_for_caps() {
    let server = http_server(vec![http_response(
        "200 OK",
        &[("Content-Type", "application/json")],
        r#"{"series":{"tvdbId":777},"movie":{"tmdbId":888}}"#,
    )]);
    let sonarr = validate_arr_url(&format!("{}?apikey=secret", server.url), ArrKind::Sonarr)
        .expect("sonarr");
    let radarr =
        validate_arr_url("https://radarr.example?apikey=radarr", ArrKind::Radarr).expect("radarr");
    let mut searchee = Searchee::from_files(
        "Loose.Video.1080p.WEB-DL-GRP",
        "Loose.Video.1080p.WEB-DL-GRP",
        vec![File::new("Loose.Video.1080p.WEB-DL-GRP.mkv", 10)],
    );
    searchee.media_type = MediaType::Video;

    let lookup = lookup_arr_ids(&[sonarr, radarr], &searchee, Some(Duration::from_secs(1)))
        .expect("lookup")
        .expect("ids");

    assert_eq!(lookup.query_title, "Loose Video GRP S00E00");
    assert_eq!(lookup.ids.tvdbid.as_deref(), Some("777"));
    let caps = TorznabCaps {
        tv_ids: vec!["tvdbid".to_owned()],
        movie_ids: Vec::new(),
        ..TorznabCaps::default()
    };
    let filtered = ids_for_torznab_caps(&lookup.ids, &caps);
    assert_eq!(filtered.tvdbid.as_deref(), Some("777"));
    assert_eq!(filtered.tmdbid, None);

    let changed = TorznabSearchIds {
        tvdbid: Some("778".to_owned()),
        ..TorznabSearchIds::default()
    };
    assert_ne!(
        arr_search_cache_key(searchee.title.as_ref(), &lookup.ids),
        arr_search_cache_key(searchee.title.as_ref(), &changed)
    );
    server.join();
}

#[test]
fn caches_reads_and_deletes_corrupted_torrents() {
    let root = temp_path("cache");
    fs::create_dir_all(&root).expect("temp dir");
    let bytes = torrent_bytes("Cached.Release", 10);

    let metafile = cache_torrent_file(&root, &bytes).expect("cache");
    let cached = get_cached_torrent(&root, &metafile.info_hash)
        .expect("read")
        .expect("cached");
    assert_eq!(cached.info_hash, metafile.info_hash);

    let path = crate::torrent::torrent_cache_path(&root, &metafile.info_hash);
    fs::write(&path, b"not a torrent").expect("corrupt");
    let _error = get_cached_torrent(&root, &metafile.info_hash).expect_err("corrupted");
    assert!(!path.exists());
    let _cleanup = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn cache_write_refuses_symlink_target() {
    let root = temp_path("cache-symlink");
    fs::create_dir_all(&root).expect("temp dir");
    let bytes = torrent_bytes("Cached.Symlink.Release", 10);
    let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
    let path = crate::torrent::torrent_cache_path(&root, &metafile.info_hash);
    fs::create_dir_all(path.parent().expect("cache parent")).expect("cache parent");
    let target = root.join("outside.torrent");
    fs::write(&target, b"outside").expect("outside");
    std::os::unix::fs::symlink(&target, &path).expect("symlink");

    let error = cache_torrent_file(&root, &bytes).expect_err("symlink rejected");

    assert!(
        error
            .to_string()
            .contains("refusing to write cached torrent through symlink")
    );
    assert_eq!(fs::read(&target).expect("target"), b"outside");
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn guid_lookup_checks_guid_link_and_tracker_fallback() {
    let root = temp_path("guid");
    fs::create_dir_all(&root).expect("temp dir");
    let database = Database::open_app_dir(&root).expect("database");
    let searchee_id = database
        .get_or_insert_searchee("release")
        .expect("searchee");
    database
        .upsert_decision(&DecisionRecord {
            searchee_id,
            guid: "guid-1",
            info_hash: Some("0123456789abcdef0123456789abcdef01234567"),
            decision: Decision::Match,
            first_seen: 1,
            last_seen: 1,
            fuzzy_size_factor: 0.05,
        })
        .expect("decision");
    database
        .upsert_decision(&DecisionRecord {
            searchee_id,
            guid: "https://tracker.tv/torrent/123/group",
            info_hash: Some("abcdef0123456789abcdef0123456789abcdef01"),
            decision: Decision::Match,
            first_seen: 1,
            last_seen: 1,
            fuzzy_size_factor: 0.05,
        })
        .expect("decision");

    assert_eq!(
        guid_lookup(&database, "guid-1", None)
            .expect("guid")
            .as_deref(),
        Some("0123456789abcdef0123456789abcdef01234567")
    );
    assert_eq!(
        guid_lookup(
            &database,
            "missing",
            Some("https://mirror.tv/torrent/123/group")
        )
        .expect("fallback")
        .as_deref(),
        Some("abcdef0123456789abcdef0123456789abcdef01")
    );
    let _cleanup = fs::remove_dir_all(root);
}

#[test]
fn snatch_once_maps_http_results() {
    let magnet = Candidate::new(
        "Magnet.Release",
        "magnet-guid",
        Some("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"),
        "tracker",
    );
    assert_eq!(
        snatch_once(&magnet, None).expect("magnet"),
        SnatchResult::MagnetLink
    );

    let redirect = http_server(vec![http_response(
        "302 Found",
        &[(
            "Location",
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
        )],
        "",
    )]);
    let redirect_candidate = Candidate::new(
        "Redirect.Release",
        "redirect-guid",
        Some(redirect.url.clone()),
        "tracker",
    );
    assert_eq!(
        snatch_once(&redirect_candidate, None).expect("redirect"),
        SnatchResult::MagnetLink
    );
    redirect.join();

    let redirected_torrent = torrent_bytes("Redirected.Download", 10);
    let http_redirect = http_server(vec![
        http_response("302 Found", &[("Location", "/download")], ""),
        torrent_response(&redirected_torrent),
    ]);
    let http_redirect_candidate = Candidate::new(
        "Redirected.Download",
        "http-redirect-guid",
        Some(http_redirect.url.clone()),
        "tracker",
    );
    let result =
        snatch_once(&http_redirect_candidate, Some(Duration::from_secs(1))).expect("redirect");
    assert!(matches!(result, SnatchResult::Metafile { .. }));
    if let SnatchResult::Metafile { metafile, bytes } = result {
        assert_eq!(bytes, redirected_torrent);
        assert_eq!(metafile.name, "Redirected.Download");
    }
    let requests = http_redirect.join();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].starts_with("GET /download "));

    let limited = http_server(vec![http_response(
        "429 Too Many Requests",
        &[("Retry-After", "2")],
        "",
    )]);
    let limited_candidate = Candidate::new(
        "Limited.Release",
        "limited-guid",
        Some(limited.url.clone()),
        "tracker",
    );
    assert_eq!(
        snatch_once(&limited_candidate, None).expect("limited"),
        SnatchResult::RateLimited {
            retry_after_millis: Some(2_000)
        }
    );
    limited.join();

    let rss = http_server(vec![http_response(
        "200 OK",
        &[("Content-Type", "application/rss+xml")],
        "<rss />",
    )]);
    let rss_candidate = Candidate::new("Rss.Release", "rss-guid", Some(rss.url.clone()), "tracker");
    assert_eq!(
        snatch_once(&rss_candidate, None).expect("rss"),
        SnatchResult::InvalidContents
    );
    rss.join();

    let torrent = torrent_bytes("Downloaded.Release", 10);
    let ok = http_server(vec![torrent_response(&torrent)]);
    let mut ok_candidate = Candidate::new(
        "Downloaded.Release",
        "ok-guid",
        Some(ok.url.clone()),
        "tracker",
    );
    ok_candidate.cookie = Some(Cow::Borrowed("session=secret"));
    let result = snatch_once(&ok_candidate, Some(Duration::from_secs(1))).expect("torrent");
    assert!(matches!(result, SnatchResult::Metafile { .. }));
    if let SnatchResult::Metafile { metafile, bytes } = result {
        assert_eq!(bytes, torrent);
        assert_eq!(metafile.name, "Downloaded.Release");
    }
    let requests = ok.join();
    let request = requests.first().expect("request").to_ascii_lowercase();
    assert!(request.contains("cookie: session=secret"));
}

#[test]
fn snatch_retries_failures_and_clears_history_on_success() {
    let torrent = torrent_bytes("Retry.Release", 10);
    let server = http_server(vec![
        http_response("500 Internal Server Error", &[("Retry-After", "0")], ""),
        torrent_response(&torrent),
    ]);
    let candidate = Candidate::new(
        "Retry.Release",
        "retry-guid",
        Some(server.url.clone()),
        "tracker",
    );
    let options = SnatchOptions {
        retries: 1,
        delay: Duration::ZERO,
        timeout: Some(Duration::from_secs(1)),
    };
    let mut history = SnatchHistory::default();

    assert!(matches!(
        snatch(&candidate, options, &mut history).expect("snatch"),
        SnatchResult::Metafile { .. }
    ));
    assert!(history.is_empty());
    assert_eq!(server.join().len(), 2);
}

#[test]
fn snatch_stops_when_retry_after_exceeds_retry_window() {
    let server = http_server(vec![http_response(
        "500 Internal Server Error",
        &[("Retry-After", "2")],
        "",
    )]);
    let candidate = Candidate::new(
        "Window.Release",
        "window-guid",
        Some(server.url.clone()),
        "tracker",
    );
    let options = SnatchOptions {
        retries: 1,
        delay: Duration::from_millis(1),
        timeout: Some(Duration::from_secs(1)),
    };
    let mut history = SnatchHistory::default();

    assert_eq!(
        snatch(&candidate, options, &mut history).expect("snatch"),
        SnatchResult::UnknownError {
            retry_after_millis: Some(2_000)
        }
    );
    assert_eq!(server.join().len(), 1);
}

fn torrent_bytes(name: &str, length: u64) -> Vec<u8> {
    format!(
            "d4:infod6:lengthi{length}e4:name{}:{name}12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
            name.len()
        )
        .into_bytes()
}

fn torrent_response(bytes: &[u8]) -> String {
    let body = std::str::from_utf8(bytes).expect("ascii torrent fixture");
    http_response(
        "200 OK",
        &[("Content-Type", "application/x-bittorrent")],
        body,
    )
}

fn http_response(status: &str, headers: &[(&str, &str)], body: &str) -> String {
    let mut response = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\n", body.len());
    for (name, value) in headers {
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");
    response.push_str(body);
    response
}

struct TestServer {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
    handle: thread::JoinHandle<()>,
}

impl TestServer {
    fn join(self) -> Vec<String> {
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
                .push(String::from_utf8_lossy(&buffer[..read]).into_owned());
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

fn insert_search_indexer(database: &Database, server_url: &str, limit: u32) -> SearchIndexer {
    let url = format!("{server_url}/api");
    database
        .execute_sql(
            "INSERT INTO indexer (url, apikey, active, search_cap) VALUES (?1, 'key', 1, 1)",
            &[SqlValue::Text(Cow::Borrowed(url.as_str()))],
        )
        .expect("indexer");
    let id = database.indexer_id(&url).expect("id");
    SearchIndexer {
        id,
        url,
        apikey: "key".to_owned(),
        caps: TorznabCaps {
            limits: LimitCaps {
                default: limit,
                max: limit,
            },
            ..TorznabCaps::default()
        },
    }
}

fn temp_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("sporos-integrations-{label}-{nanos}"))
}
