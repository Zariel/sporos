//! Maintenance operations for cache, indexer, API-key, diff, and tree commands.

use std::{
    borrow::Cow,
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::sync::Semaphore;

use crate::{
    SporosError,
    actions::{
        InjectionAction, InjectionActionOptions, RestoreSummary, SavedInjectionOptions,
        SavedInjectionSummary, inject_saved_torrents, perform_injection_action,
        restore_from_torrent_cache, save_candidate_torrent,
    },
    api::{ApiOutcome, WebhookRequest},
    clients::{TorrentClient, build_torrent_clients, client_torrent_to_searchee},
    config::{Action as ConfigAction, RuntimeConfig},
    domain::{ActionResult, Candidate, ClientLabel, InfoHash, Label},
    integrations::{
        ArrConfig, ArrKind, RssPagerOptions, SnatchOptions, TorznabSearchOptions,
        enabled_search_indexers, fetch_torznab_caps, query_rss_feeds, sync_torznab_indexers,
        update_indexer_caps, validate_arr_config, validate_torznab_config,
    },
    matching::AssessmentOptions,
    notifications::NotificationSender,
    persistence::{
        AsyncDatabase, CacheTable, ClientSearcheeRecord, DataRootRecord, Database, EnsembleRecord,
    },
    search::{
        Blocklist, CandidateSearchCache, ContentFilterOptions, PipelineAction, PipelineSummary,
        ReverseLookupGate, ReverseLookupRuntime, SearchPipelineOptions, SearchPipelineRuntime,
        SearcheeSources, VirtualSeasonOptions, bulk_search, check_new_candidate_match,
        check_new_candidate_matches, episode_ensemble, find_all_searchees, find_on_other_sites,
        find_searchable_searchees, for_each_data_dir_searchee,
    },
    torrent::{
        Bencode, BencodeValue, bdecode, bencode, parse_metafile, torrent_cache_dir,
        torrent_cache_path,
    },
};

const ONE_DAY_MILLIS: u64 = 86_400_000;
const THIRTY_DAYS_MILLIS: u64 = 30 * ONE_DAY_MILLIS;
const ONE_YEAR_MILLIS: u64 = 365 * ONE_DAY_MILLIS;
const CLEANUP_DB_PAGE_SIZE: i64 = 1_000;
const LOCAL_WORK_CONCURRENCY_LIMIT: usize = 4;

static LOCAL_WORK_PERMITS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(LOCAL_WORK_CONCURRENCY_LIMIT)));

/// Result counts from cache cleanup.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ClearCacheResult {
    /// Decision rows with null info hashes removed.
    pub decisions_removed: usize,
    /// Timestamp rows removed.
    pub timestamps_removed: usize,
}

/// Result counts from client cache cleanup.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct ClearClientCacheResult {
    /// Torrent-dir rows removed.
    pub torrents_removed: usize,
    /// Client searchee rows removed.
    pub client_searchees_removed: usize,
    /// Data-dir rows removed.
    pub data_removed: usize,
    /// Ensemble rows removed.
    pub ensemble_removed: usize,
}

/// Result from tracker URL replacement.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TrackerUpdateResult {
    /// Cached torrent files inspected.
    pub files_seen: usize,
    /// Cached torrent files rewritten.
    pub files_updated: usize,
}

/// Result counts from daily cleanup.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct CleanupDbResult {
    /// Client searchee rows refreshed from configured clients.
    pub client_searchees_refreshed: usize,
    /// Stale client searchee rows pruned after refresh.
    pub client_searchees_pruned: usize,
    /// Ensemble rows rebuilt from refreshed client searchees.
    pub client_ensemble_rows_rebuilt: usize,
    /// Data-dir rows removed because paths no longer exist.
    pub data_rows_removed: usize,
    /// Ensemble rows removed because paths no longer exist.
    pub ensemble_rows_removed: usize,
    /// Torrent cache files removed because no recent decision references them.
    pub torrent_cache_files_removed: usize,
    /// Decision rows with null info hashes removed.
    pub null_decisions_removed: usize,
    /// Decision rows removed because their cache files are missing.
    pub missing_cache_decisions_removed: usize,
    /// Missing-cache decision cleanup was skipped by the catastrophic guard.
    pub catastrophic_decision_cleanup_skipped: bool,
    /// GUID to info-hash rows read while rebuilding the map.
    pub guid_info_hash_rows: usize,
}

/// Summary from one CLI search workflow run.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct SearchWorkflowResult {
    /// Local searchees considered.
    pub searchees: usize,
    /// Enabled indexers considered.
    pub indexers: usize,
    /// Pipeline summary.
    pub pipeline: PipelineSummary,
}

/// Summary from one CLI RSS workflow run.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct RssWorkflowResult {
    /// RSS candidates loaded from indexers.
    pub candidates: usize,
    /// Reverse-match attempts returned.
    pub attempts: usize,
}

/// Summary from refreshing configured indexer capabilities.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct IndexerCapsRefreshResult {
    /// Configured indexers considered.
    pub indexers: usize,
    /// Capability rows successfully updated.
    pub updated: usize,
}

/// Compact tree output for a parsed torrent.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TorrentTree {
    /// Torrent name.
    pub name: String,
    /// Info hash.
    pub info_hash: String,
    /// File paths and lengths.
    pub files: Vec<(String, u64)>,
}

/// Return configured, persisted, or newly generated API key in that order.
pub fn api_key(database: &Database, configured: Option<&str>) -> crate::Result<String> {
    if let Some(configured) = configured {
        return Ok(configured.to_owned());
    }
    if let Some(stored) = database.get_api_key()? {
        return Ok(stored);
    }
    reset_api_key(database)
}

/// Return configured, persisted, or newly generated API key through async persistence.
pub async fn api_key_async(
    database: &AsyncDatabase,
    configured: Option<&str>,
) -> crate::Result<String> {
    if let Some(configured) = configured {
        return Ok(configured.to_owned());
    }
    if let Some(stored) = database.get_api_key().await? {
        return Ok(stored);
    }
    reset_api_key_async(database).await
}

/// Generate and persist a fresh API key.
pub fn reset_api_key(database: &Database) -> crate::Result<String> {
    let key = generate_api_key()?;
    database.set_api_key(&key)?;
    Ok(key)
}

/// Generate and persist a fresh API key through async persistence.
pub async fn reset_api_key_async(database: &AsyncDatabase) -> crate::Result<String> {
    let key = generate_api_key()?;
    database.set_api_key(&key).await?;
    Ok(key)
}

/// Clear decision cache rows without cached torrents and search timestamps.
pub fn clear_cache(database: &Database) -> crate::Result<ClearCacheResult> {
    let decisions_removed = database.delete_null_decisions()?;
    let timestamps_removed = database.clear_timestamps()?;
    Ok(ClearCacheResult {
        decisions_removed,
        timestamps_removed,
    })
}

/// Clear decision cache rows and search timestamps through async persistence.
pub async fn clear_cache_async(database: &AsyncDatabase) -> crate::Result<ClearCacheResult> {
    let decisions_removed = database.delete_null_decisions().await?;
    let timestamps_removed = database.clear_timestamps().await?;
    Ok(ClearCacheResult {
        decisions_removed,
        timestamps_removed,
    })
}

/// Clear cached client, torrent-dir, data-dir, and ensemble state.
pub fn clear_client_cache(database: &Database) -> crate::Result<ClearClientCacheResult> {
    let torrents_removed = database.clear_table(CacheTable::Torrent)?;
    let client_searchees_removed = database.clear_table(CacheTable::ClientSearchee)?;
    let data_removed = database.clear_table(CacheTable::Data)?;
    let ensemble_removed = database.clear_table(CacheTable::Ensemble)?;
    Ok(ClearClientCacheResult {
        torrents_removed,
        client_searchees_removed,
        data_removed,
        ensemble_removed,
    })
}

/// Clear cached client, torrent-dir, data-dir, and ensemble state asynchronously.
pub async fn clear_client_cache_async(
    database: &AsyncDatabase,
) -> crate::Result<ClearClientCacheResult> {
    let torrents_removed = database.clear_table(CacheTable::Torrent).await?;
    let client_searchees_removed = database.clear_table(CacheTable::ClientSearchee).await?;
    let data_removed = database.clear_table(CacheTable::Data).await?;
    let ensemble_removed = database.clear_table(CacheTable::Ensemble).await?;
    Ok(ClearClientCacheResult {
        torrents_removed,
        client_searchees_removed,
        data_removed,
        ensemble_removed,
    })
}

/// Clear indexer failure status and retry timestamps.
pub fn clear_indexer_failures(database: &Database) -> crate::Result<usize> {
    database.clear_indexer_failures()
}

/// Clear indexer failure status and retry timestamps asynchronously.
pub async fn clear_indexer_failures_async(database: &AsyncDatabase) -> crate::Result<usize> {
    database.clear_indexer_failures().await
}

/// Run daily database and torrent-cache cleanup.
pub fn cleanup_db(
    database: &Database,
    app_dir: &Path,
    config: &RuntimeConfig,
    now_millis: i64,
) -> crate::Result<CleanupDbResult> {
    cleanup_db_with_clients(database, app_dir, config, now_millis, &[])
}

/// Run daily database and torrent-cache cleanup from async orchestration.
pub async fn cleanup_db_async(
    app_dir: PathBuf,
    config: RuntimeConfig,
    now_millis: i64,
) -> crate::Result<CleanupDbResult> {
    run_blocking_workflow("cleanup", move || {
        let database = Database::open_app_dir(&app_dir)?;
        let client_timeout = config.search_timeout.map(Duration::from_millis);
        let client_adapters = if config.use_client_torrents {
            build_torrent_clients(&config.torrent_clients, client_timeout)?
        } else {
            Vec::new()
        };
        let client_refs = client_refs(&client_adapters);
        cleanup_db_with_clients(&database, &app_dir, &config, now_millis, &client_refs)
    })
    .await
}

/// Run daily database and torrent-cache cleanup with live client refresh.
pub fn cleanup_db_with_clients(
    database: &Database,
    app_dir: &Path,
    config: &RuntimeConfig,
    now_millis: i64,
    clients: &[&dyn TorrentClient],
) -> crate::Result<CleanupDbResult> {
    let mut result = CleanupDbResult::default();
    if config.use_client_torrents {
        refresh_cleanup_client_searchees(database, config, clients, &mut result)?;
    }
    if !config.data_dirs.is_empty() {
        result.data_rows_removed = prune_missing_data_rows(database)?;
    }
    if !config.data_dirs.is_empty() || config.season_from_episodes.is_some() {
        result.ensemble_rows_removed += prune_missing_ensemble_rows(database)?;
    }
    result.torrent_cache_files_removed =
        prune_unused_torrent_cache(database, app_dir, config, now_millis)?;
    result.null_decisions_removed = database.delete_null_decisions()?;
    let decision_cleanup = prune_missing_cache_decisions(database, app_dir)?;
    result.missing_cache_decisions_removed = decision_cleanup.removed;
    result.catastrophic_decision_cleanup_skipped = decision_cleanup.catastrophic_skipped;
    result.guid_info_hash_rows = rebuild_guid_info_hash_map(database)?;
    Ok(result)
}

/// Run one bulk search workflow.
pub fn run_search_workflow(
    database: &Database,
    app_dir: &Path,
    config: &RuntimeConfig,
    notifier: &NotificationSender,
) -> crate::Result<SearchWorkflowResult> {
    sync_configured_indexers(database, config)?;
    index_torrents_and_data_dirs(database, config)?;
    let client_adapters = build_workflow_clients(config)?;
    let client_refs = client_refs(&client_adapters);
    let client_searchees = collect_client_searchees(config, &client_refs)?;
    let blocklist = Blocklist::parse(&config.block_list)?;
    let indexers = enabled_search_indexers(database, current_time_millis())?;
    let arr_configs = build_arr_configs(config)?;
    let base = find_all_searchees(
        &SearcheeSources {
            torrents: config.torrents.as_deref(),
            use_client_torrents: config.use_client_torrents,
            client_searchees: &client_searchees,
            torrent_dir: config.torrent_dir.as_deref(),
            data_dirs: &config.data_dirs,
            max_data_depth: config.max_data_depth,
        },
        Label::Search,
    )?;
    let excluded = local_info_hashes(&base);
    let options =
        search_pipeline_options(config, &blocklist, &excluded, &arr_configs, Label::Search);
    let searchees = find_searchable_searchees(base, &[], config.max_data_depth, &options)?;
    let injection = injection_options(config, &client_refs);
    let mut cache = CandidateSearchCache::default();
    let mut runtime = SearchPipelineRuntime {
        database,
        app_dir,
        options: &options,
        cache: &mut cache,
    };
    let pipeline = bulk_search(
        &mut runtime,
        &searchees,
        &indexers,
        |action| dispatch_pipeline_action(app_dir, config, &injection, action),
        |attempt| {
            let _report = notifier.send_result(attempt);
            Ok(())
        },
    )?;
    Ok(SearchWorkflowResult {
        searchees: searchees.len(),
        indexers: indexers.len(),
        pipeline,
    })
}

/// Run one bulk search workflow from async orchestration.
pub async fn run_search_workflow_async(
    app_dir: PathBuf,
    config: RuntimeConfig,
    notifier: NotificationSender,
) -> crate::Result<SearchWorkflowResult> {
    run_blocking_workflow("search workflow", move || {
        let database = Database::open_app_dir(&app_dir)?;
        run_search_workflow(&database, &app_dir, &config, &notifier)
    })
    .await
}

/// Run one RSS reverse-match workflow.
pub fn run_rss_workflow(
    database: &Database,
    app_dir: &Path,
    config: &RuntimeConfig,
    notifier: &NotificationSender,
) -> crate::Result<RssWorkflowResult> {
    sync_configured_indexers(database, config)?;
    index_torrents_and_data_dirs(database, config)?;
    let client_adapters = build_workflow_clients(config)?;
    let client_refs = client_refs(&client_adapters);
    let client_searchees = collect_client_searchees(config, &client_refs)?;
    let blocklist = Blocklist::parse(&config.block_list)?;
    let indexers = enabled_search_indexers(database, current_time_millis())?;
    let arr_configs = build_arr_configs(config)?;
    let now_millis = current_time_millis();
    let time_since_last_run = rss_time_since_last_run(database, config, now_millis)?;
    let candidates = query_rss_feeds(
        database,
        &indexers,
        RssPagerOptions {
            time_since_last_run,
            timeout: config.search_timeout.map(Duration::from_millis),
            delay: Duration::from_secs(config.delay),
            now_millis,
        },
    )?;
    let local = find_all_searchees(
        &SearcheeSources {
            torrents: None,
            use_client_torrents: config.use_client_torrents,
            client_searchees: &client_searchees,
            torrent_dir: config.torrent_dir.as_deref(),
            data_dirs: &config.data_dirs,
            max_data_depth: config.max_data_depth,
        },
        Label::Rss,
    )?;
    let excluded = local_info_hashes(&local);
    let options = search_pipeline_options(config, &blocklist, &excluded, &arr_configs, Label::Rss);
    let injection = injection_options(config, &client_refs);
    let gate = ReverseLookupGate::new();
    let runtime = ReverseLookupRuntime {
        gate: &gate,
        database,
        app_dir,
        options: &options,
    };
    let attempts = check_new_candidate_matches(
        &runtime,
        &candidates,
        &local,
        |action| dispatch_pipeline_action(app_dir, config, &injection, action),
        |attempt| {
            let _report = notifier.send_result(attempt);
            Ok(())
        },
    )?;
    Ok(RssWorkflowResult {
        candidates: candidates.len(),
        attempts: attempts.len(),
    })
}

/// Run one RSS reverse-match workflow from async orchestration.
pub async fn run_rss_workflow_async(
    app_dir: PathBuf,
    config: RuntimeConfig,
    notifier: NotificationSender,
) -> crate::Result<RssWorkflowResult> {
    run_blocking_workflow("rss workflow", move || {
        let database = Database::open_app_dir(&app_dir)?;
        run_rss_workflow(&database, &app_dir, &config, &notifier)
    })
    .await
}

/// Reverse-match one announce API candidate.
pub fn run_announce_match(
    database: &Database,
    app_dir: &Path,
    config: &RuntimeConfig,
    candidate: Candidate<'static>,
    notifier: &NotificationSender,
) -> crate::Result<Option<ApiOutcome>> {
    if !config.use_client_torrents && config.torrent_dir.is_none() && config.data_dirs.is_empty() {
        return Err(operation_error(
            "announce requires torrent_dir, use_client_torrents, or data_dirs",
        ));
    }
    index_torrents_and_data_dirs(database, config)?;
    let client_adapters = build_workflow_clients(config)?;
    let client_refs = client_refs(&client_adapters);
    let client_searchees = collect_client_searchees(config, &client_refs)?;
    let blocklist = Blocklist::parse(&config.block_list)?;
    let arr_configs = build_arr_configs(config)?;
    let local = find_all_searchees(
        &SearcheeSources {
            torrents: None,
            use_client_torrents: config.use_client_torrents,
            client_searchees: &client_searchees,
            torrent_dir: config.torrent_dir.as_deref(),
            data_dirs: &config.data_dirs,
            max_data_depth: config.max_data_depth,
        },
        Label::Announce,
    )?;
    let excluded = local_info_hashes(&local);
    let options =
        search_pipeline_options(config, &blocklist, &excluded, &arr_configs, Label::Announce);
    let injection = injection_options(config, &client_refs);
    let gate = ReverseLookupGate::new();
    let runtime = ReverseLookupRuntime {
        gate: &gate,
        database,
        app_dir,
        options: &options,
    };
    let attempt = check_new_candidate_match(
        &runtime,
        &candidate,
        &local,
        |action| dispatch_pipeline_action(app_dir, config, &injection, action),
        |attempt| {
            let _report = notifier.send_result(attempt);
            Ok(())
        },
    )?;
    Ok(attempt.map(|attempt| ApiOutcome {
        decision: attempt.decision,
        action_result: attempt.action_result,
    }))
}

/// Reverse-match one announce API candidate from async orchestration.
pub async fn run_announce_match_async(
    app_dir: PathBuf,
    config: RuntimeConfig,
    candidate: Candidate<'static>,
    notifier: NotificationSender,
) -> crate::Result<Option<ApiOutcome>> {
    run_blocking_workflow("announce workflow", move || {
        let database = Database::open_app_dir(&app_dir)?;
        run_announce_match(&database, &app_dir, &config, candidate, &notifier)
    })
    .await
}

/// Run one targeted webhook search from an info hash or filesystem path.
pub fn run_webhook_search(
    database: &Database,
    app_dir: &Path,
    config: &RuntimeConfig,
    request: WebhookRequest,
    notifier: &NotificationSender,
) -> crate::Result<PipelineSummary> {
    let mut config = config.clone();
    if request.include_single_episodes {
        config.include_single_episodes = true;
    }
    if request.include_non_videos {
        config.include_non_videos = true;
    }
    if request.ignore_exclude_recent_search {
        config.exclude_recent_search = Some(1);
    }
    if request.ignore_exclude_older {
        config.exclude_older = Some(u64::MAX);
    }
    if request.ignore_block_list {
        config.block_list.clear();
    }

    sync_configured_indexers(database, &config)?;
    index_torrents_and_data_dirs(database, &config)?;
    let client_adapters = build_workflow_clients(&config)?;
    let client_refs = client_refs(&client_adapters);
    let client_searchees = collect_client_searchees(&config, &client_refs)?;
    let blocklist = Blocklist::parse(&config.block_list)?;
    let indexers = enabled_search_indexers(database, current_time_millis())?;
    let arr_configs = build_arr_configs(&config)?;
    let local = find_all_searchees(
        &SearcheeSources {
            torrents: config.torrents.as_deref(),
            use_client_torrents: config.use_client_torrents,
            client_searchees: &client_searchees,
            torrent_dir: config.torrent_dir.as_deref(),
            data_dirs: &config.data_dirs,
            max_data_depth: config.max_data_depth,
        },
        Label::Webhook,
    )?;
    let (targets, excluded) = webhook_targets_and_excluded(local, &request);
    let mut options =
        search_pipeline_options(&config, &blocklist, &excluded, &arr_configs, Label::Webhook);
    if request.ignore_cross_seeds {
        options.filter.ignore_cross_seeds = false;
    }
    let injection = injection_options(&config, &client_refs);
    let mut cache = CandidateSearchCache::default();
    let mut summary = PipelineSummary::default();
    for searchee in targets {
        let mut runtime = SearchPipelineRuntime {
            database,
            app_dir,
            options: &options,
            cache: &mut cache,
        };
        let result = find_on_other_sites(
            &mut runtime,
            searchee,
            &indexers,
            |action| dispatch_pipeline_action(app_dir, &config, &injection, action),
            |attempt| {
                let _report = notifier.send_result(attempt);
                Ok(())
            },
        )?;
        summary.searchees_seen = summary.searchees_seen.saturating_add(result.searchees_seen);
        summary.searchees_filtered = summary
            .searchees_filtered
            .saturating_add(result.searchees_filtered);
        summary.indexer_searches = summary
            .indexer_searches
            .saturating_add(result.indexer_searches);
        summary.candidates_assessed = summary
            .candidates_assessed
            .saturating_add(result.candidates_assessed);
        summary.attempts.extend(result.attempts);
    }
    Ok(summary)
}

/// Run one targeted webhook search from async orchestration.
pub async fn run_webhook_search_async(
    app_dir: PathBuf,
    config: RuntimeConfig,
    request: WebhookRequest,
    notifier: NotificationSender,
) -> crate::Result<PipelineSummary> {
    run_blocking_workflow("webhook workflow", move || {
        let database = Database::open_app_dir(&app_dir)?;
        run_webhook_search(&database, &app_dir, &config, request, &notifier)
    })
    .await
}

/// Run one saved torrent injection workflow.
pub fn run_inject_workflow(
    database: &Database,
    _app_dir: &Path,
    config: &RuntimeConfig,
) -> crate::Result<SavedInjectionSummary> {
    index_torrents_and_data_dirs(database, config)?;
    let client_adapters = build_workflow_clients(config)?;
    let client_refs = client_refs(&client_adapters);
    let client_searchees = collect_client_searchees(config, &client_refs)?;
    let blocklist = Blocklist::parse(&config.block_list)?;
    let searchees = find_all_searchees(
        &SearcheeSources {
            torrents: None,
            use_client_torrents: config.use_client_torrents,
            client_searchees: &client_searchees,
            torrent_dir: config.torrent_dir.as_deref(),
            data_dirs: &config.data_dirs,
            max_data_depth: config.max_data_depth,
        },
        Label::Inject,
    )?;
    let excluded = local_info_hashes(&searchees);
    let assessment = assessment_options(config, &blocklist, &excluded);
    let injection = injection_options(config, &client_refs);
    let input_dir = config.inject_dir.as_deref().unwrap_or(&config.output_dir);
    inject_saved_torrents(
        &SavedInjectionOptions {
            input_dir,
            injection: &injection,
            assessment: &assessment,
            ignore_titles: config.ignore_titles.unwrap_or(false),
        },
        &searchees,
        |_| Ok(()),
    )
}

/// Run one saved torrent injection workflow from async orchestration.
pub async fn run_inject_workflow_async(
    app_dir: PathBuf,
    config: RuntimeConfig,
) -> crate::Result<SavedInjectionSummary> {
    run_blocking_workflow("inject workflow", move || {
        let database = Database::open_app_dir(&app_dir)?;
        run_inject_workflow(&database, &app_dir, &config)
    })
    .await
}

/// Run one restore workflow.
pub fn run_restore_workflow(
    database: &Database,
    app_dir: &Path,
    config: &RuntimeConfig,
) -> crate::Result<RestoreSummary> {
    restore_from_torrent_cache(database, app_dir, &config.output_dir, |_| Ok(()))
}

/// Run one restore workflow from async orchestration.
pub async fn run_restore_workflow_async(
    app_dir: PathBuf,
    config: RuntimeConfig,
) -> crate::Result<RestoreSummary> {
    run_blocking_workflow("restore workflow", move || {
        let database = Database::open_app_dir(&app_dir)?;
        run_restore_workflow(&database, &app_dir, &config)
    })
    .await
}

/// Refresh capabilities for configured Torznab indexers.
pub fn run_update_indexer_caps(
    database: &Database,
    config: &RuntimeConfig,
) -> crate::Result<IndexerCapsRefreshResult> {
    let configured = config
        .torznab
        .iter()
        .map(validate_torznab_config)
        .collect::<crate::Result<Vec<_>>>()?;
    sync_torznab_indexers(database, &configured)?;
    let mut result = IndexerCapsRefreshResult {
        indexers: configured.len(),
        updated: 0,
    };
    for indexer in configured {
        let caps = fetch_torznab_caps(&indexer)?;
        let id = indexer_id(database, &indexer.url)?;
        update_indexer_caps(database, id, &caps)?;
        result.updated += 1;
    }
    Ok(result)
}

/// Refresh capabilities for configured Torznab indexers from async orchestration.
pub async fn run_update_indexer_caps_async(
    app_dir: PathBuf,
    config: RuntimeConfig,
) -> crate::Result<IndexerCapsRefreshResult> {
    run_blocking_workflow("indexer caps workflow", move || {
        let database = Database::open_app_dir(&app_dir)?;
        run_update_indexer_caps(&database, &config)
    })
    .await
}

/// Replace tracker URLs inside cached torrent files.
pub fn update_torrent_cache_trackers(
    app_dir: &Path,
    old_announce_url: &str,
    new_announce_url: &str,
) -> crate::Result<TrackerUpdateResult> {
    let cache_dir = torrent_cache_dir(app_dir);
    let mut result = TrackerUpdateResult {
        files_seen: 0,
        files_updated: 0,
    };
    let entries = match fs::read_dir(&cache_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(result),
        Err(error) => return Err(operation_error(format!("failed to read cache: {error}"))),
    };

    for entry in entries {
        let entry = entry.map_err(|error| operation_error(format!("cache entry: {error}")))?;
        let path = entry.path();
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("torrent") {
            continue;
        }
        result.files_seen += 1;
        let bytes = fs::read(&path)
            .map_err(|error| operation_error(format!("failed to read torrent: {error}")))?;
        if let Some(updated) = replace_torrent_tracker_urls(
            &bytes,
            old_announce_url.as_bytes(),
            new_announce_url.as_bytes(),
        )? {
            fs::write(&path, updated).map_err(|error| {
                operation_error(format!("failed to write updated torrent: {error}"))
            })?;
            result.files_updated += 1;
        }
    }

    Ok(result)
}

/// Parse and compare two torrent files by normalized metafile structure.
pub fn diff_torrents(left: &Path, right: &Path) -> crate::Result<Option<String>> {
    let left = parse_metafile(
        &fs::read(left)
            .map_err(|error| operation_error(format!("failed to read left torrent: {error}")))?,
    )?;
    let right = parse_metafile(
        &fs::read(right)
            .map_err(|error| operation_error(format!("failed to read right torrent: {error}")))?,
    )?;

    if left == right {
        Ok(None)
    } else {
        Ok(Some(format!("{left:#?}\n---\n{right:#?}")))
    }
}

/// Parse a torrent file and return displayable tree metadata.
pub fn torrent_tree(path: &Path) -> crate::Result<TorrentTree> {
    let metafile = parse_metafile(
        &fs::read(path)
            .map_err(|error| operation_error(format!("failed to read torrent: {error}")))?,
    )?;
    Ok(TorrentTree {
        name: metafile.name.into_owned(),
        info_hash: metafile.info_hash.as_str().to_owned(),
        files: metafile
            .files
            .into_iter()
            .map(|file| (file.path.into_owned(), file.length))
            .collect(),
    })
}

fn sync_configured_indexers(database: &Database, config: &RuntimeConfig) -> crate::Result<()> {
    let configured = config
        .torznab
        .iter()
        .map(validate_torznab_config)
        .collect::<crate::Result<Vec<_>>>()?;
    sync_torznab_indexers(database, &configured)?;
    Ok(())
}

fn build_arr_configs(config: &RuntimeConfig) -> crate::Result<Vec<ArrConfig>> {
    config
        .sonarr
        .iter()
        .map(|entry| validate_arr_config(entry, ArrKind::Sonarr))
        .chain(
            config
                .radarr
                .iter()
                .map(|entry| validate_arr_config(entry, ArrKind::Radarr)),
        )
        .collect()
}

fn indexer_id(database: &Database, url: &str) -> crate::Result<i64> {
    database.indexer_id(url)
}

fn index_torrents_and_data_dirs(database: &Database, config: &RuntimeConfig) -> crate::Result<()> {
    if let Some(torrent_dir) = &config.torrent_dir {
        let _result = crate::search::index_torrent_dir(database, torrent_dir)?;
    }
    if !config.data_dirs.is_empty() {
        database.begin_data_root_refresh()?;
        for_each_data_dir_searchee(&config.data_dirs, config.max_data_depth, |searchee| {
            let Some(path) = searchee.path.as_deref() else {
                return Ok(());
            };
            database.upsert_data_root(&DataRootRecord {
                path,
                title: searchee.title.as_ref(),
            })?;
            database.mark_refreshed_data_root(path)
        })?;
        database.finish_data_root_refresh()?;
    }
    Ok(())
}

fn build_workflow_clients(config: &RuntimeConfig) -> crate::Result<Vec<Box<dyn TorrentClient>>> {
    if config.torrent_clients.is_empty() {
        return Ok(Vec::new());
    }
    build_torrent_clients(
        &config.torrent_clients,
        config.search_timeout.map(Duration::from_millis),
    )
}

fn client_refs(clients: &[Box<dyn TorrentClient>]) -> Vec<&dyn TorrentClient> {
    clients.iter().map(|client| client.as_ref()).collect()
}

fn collect_client_searchees(
    config: &RuntimeConfig,
    clients: &[&dyn TorrentClient],
) -> crate::Result<Vec<crate::domain::Searchee<'static>>> {
    if !config.use_client_torrents {
        return Ok(Vec::new());
    }
    let mut output = Vec::new();
    for client in clients {
        let metadata = client.metadata().clone().into_owned();
        client.for_each_torrent(&mut |torrent| {
            if let Some(searchee) = client_torrent_to_searchee(&metadata, torrent) {
                output.push(searchee);
            }
            Ok(())
        })?;
    }
    Ok(output)
}

fn local_info_hashes(searchees: &[crate::domain::Searchee<'_>]) -> BTreeSet<String> {
    searchees
        .iter()
        .filter_map(|searchee| searchee.info_hash.as_ref())
        .map(ToString::to_string)
        .collect()
}

fn webhook_targets_and_excluded(
    local: Vec<crate::domain::Searchee<'static>>,
    request: &WebhookRequest,
) -> (Vec<crate::domain::Searchee<'static>>, BTreeSet<String>) {
    let excluded = local_info_hashes(&local);
    let targets = local
        .into_iter()
        .filter(|searchee| webhook_matches_request(searchee, request))
        .collect();
    (targets, excluded)
}

fn webhook_matches_request(
    searchee: &crate::domain::Searchee<'_>,
    request: &WebhookRequest,
) -> bool {
    if let Some(info_hash) = request.info_hash.as_deref() {
        return searchee
            .info_hash
            .as_ref()
            .is_some_and(|local| local.as_str().eq_ignore_ascii_case(info_hash));
    }
    let Some(path) = request.path.as_deref() else {
        return false;
    };
    searchee
        .path
        .as_deref()
        .is_some_and(|local| webhook_path_matches(local, path))
        || searchee
            .files
            .iter()
            .any(|file| webhook_path_matches(file.path.as_ref(), path))
}

fn webhook_path_matches(local: &str, requested: &str) -> bool {
    if local == requested {
        return true;
    }
    let Ok(local) = fs::canonicalize(Path::new(local)) else {
        return false;
    };
    let Ok(requested) = fs::canonicalize(Path::new(requested)) else {
        return false;
    };
    local == requested
}

fn search_pipeline_options<'a>(
    config: &'a RuntimeConfig,
    blocklist: &'a Blocklist,
    excluded: &'a BTreeSet<String>,
    arr_configs: &'a [ArrConfig],
    label: Label,
) -> SearchPipelineOptions<'a> {
    let now_millis = current_time_millis();
    SearchPipelineOptions {
        label,
        filter: ContentFilterOptions {
            blocklist,
            blocklist_only: false,
            include_single_episodes: config.include_single_episodes,
            include_non_videos: config.include_non_videos,
            fuzzy_size_threshold: config.fuzzy_size_threshold,
            ignore_cross_seeds: false,
            link_category: config.link_category.as_deref(),
            label: Some(label),
        },
        assessment: assessment_options(config, blocklist, excluded),
        snatch: SnatchOptions {
            retries: 0,
            delay: Duration::from_secs(config.delay),
            timeout: config.snatch_timeout.map(Duration::from_millis),
        },
        torznab: TorznabSearchOptions {
            timeout: config.search_timeout.map(Duration::from_millis),
            delay: Duration::from_secs(config.delay),
            search_limit: config
                .search_limit
                .map(|value| usize::try_from(value).unwrap_or(usize::MAX)),
            now_millis,
        },
        arr_configs,
        arr_timeout: config.search_timeout.map(Duration::from_millis),
        virtual_season: config.season_from_episodes.map(|season_from_episodes| {
            VirtualSeasonOptions {
                season_from_episodes,
                use_filters: true,
                now_millis,
            }
        }),
        exclude_older: config.exclude_older,
        exclude_recent_search: config.exclude_recent_search,
    }
}

fn assessment_options<'a>(
    config: &'a RuntimeConfig,
    blocklist: &'a Blocklist,
    excluded: &'a BTreeSet<String>,
) -> AssessmentOptions<'a> {
    AssessmentOptions {
        match_mode: config.match_mode,
        fuzzy_size_threshold: config.fuzzy_size_threshold,
        season_from_episodes: config.season_from_episodes.unwrap_or(1.0),
        include_single_episodes: config.include_single_episodes,
        info_hashes_to_exclude: excluded,
        blocklist,
    }
}

fn injection_options<'a>(
    config: &'a RuntimeConfig,
    clients: &'a [&'a dyn TorrentClient],
) -> InjectionActionOptions<'a> {
    InjectionActionOptions {
        clients,
        output_dir: Some(&config.output_dir),
        link_dirs: &config.link_dirs,
        link_type: config.link_type,
        flat_linking: config.flat_linking,
        unwrap_symlinks: false,
        skip_recheck: config.skip_recheck,
        match_mode: config.match_mode,
        auto_resume_max_download: config.auto_resume_max_download,
        ignore_non_relevant_files_to_resume: config.ignore_non_relevant_files_to_resume,
        category: config.link_category.clone().map(ClientLabel::new),
        tags: Vec::new(),
        duplicate_categories: config.duplicate_categories,
    }
}

fn dispatch_pipeline_action(
    app_dir: &Path,
    config: &RuntimeConfig,
    injection: &InjectionActionOptions<'_>,
    action: &PipelineAction<'_>,
) -> crate::Result<Option<ActionResult>> {
    let Some(metafile) = action.assessment.metafile.as_ref() else {
        return Ok(None);
    };
    let bytes = fs::read(torrent_cache_path(app_dir, &metafile.info_hash)).map_err(|error| {
        operation_error(format!(
            "failed to read cached candidate torrent {}: {error}",
            metafile.info_hash
        ))
    })?;
    match config.action {
        ConfigAction::Save => {
            let saved = save_candidate_torrent(
                &config.output_dir,
                action.candidate.tracker.as_ref(),
                metafile,
                &bytes,
                |_| Ok(()),
            )?;
            Ok(Some(ActionResult::Save(saved.result)))
        }
        ConfigAction::Inject => {
            let result = perform_injection_action(
                &InjectionAction {
                    searchee: action.searchee,
                    candidate: action.candidate,
                    metafile,
                    bytes: &bytes,
                    decision: action.assessment.decision,
                },
                injection,
                |_| Ok(()),
            )?;
            Ok(Some(ActionResult::Injection(result)))
        }
    }
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn rss_time_since_last_run(
    database: &Database,
    config: &RuntimeConfig,
    now_millis: u64,
) -> crate::Result<Duration> {
    let fallback = config
        .rss_cadence
        .map(Duration::from_millis)
        .unwrap_or(Duration::ZERO);
    let last_run = database.read_last_run("rss")?;
    let Some(last_run) = last_run.and_then(|value| u64::try_from(value).ok()) else {
        return Ok(fallback);
    };
    if now_millis > last_run {
        Ok(Duration::from_millis(now_millis.saturating_sub(last_run)))
    } else {
        Ok(fallback)
    }
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
struct MissingCacheDecisionCleanup {
    removed: usize,
    catastrophic_skipped: bool,
}

fn prune_missing_data_rows(database: &Database) -> crate::Result<usize> {
    let mut removed = 0usize;
    let mut after_rowid = 0_i64;
    loop {
        let rows = database.data_rowid_path_page(after_rowid, CLEANUP_DB_PAGE_SIZE)?;
        let Some(last) = rows.last() else {
            break;
        };
        after_rowid = last.rowid;
        for row in rows {
            let path = row.path;
            if !Path::new(&path).exists() {
                removed += database.delete_data_rowid(row.rowid)?;
            }
        }
    }
    Ok(removed)
}

fn refresh_cleanup_client_searchees(
    database: &Database,
    config: &RuntimeConfig,
    clients: &[&dyn TorrentClient],
    result: &mut CleanupDbResult,
) -> crate::Result<()> {
    for client in clients {
        let metadata = client.metadata().clone().into_owned();
        database.begin_client_searchee_refresh()?;
        let mut refreshed = 0usize;
        let mut ensemble_rows = 0usize;
        client.for_each_torrent(&mut |torrent| {
            let Some(searchee) = client_torrent_to_searchee(&metadata, torrent) else {
                return Ok(());
            };
            let Some(client_metadata) = &searchee.client else {
                return Ok(());
            };
            let Some(info_hash) = searchee.info_hash.as_ref() else {
                return Ok(());
            };
            database.upsert_client_searchee(&ClientSearcheeRecord {
                client_host: metadata.host.as_ref(),
                info_hash: info_hash.as_str(),
                name: searchee.name.as_ref(),
                title: searchee.title.as_ref(),
                files: &searchee.files,
                length: searchee.length,
                save_path: client_metadata.save_path.as_ref(),
                category: client_metadata
                    .category
                    .as_ref()
                    .map(|label| label.as_str()),
                tags: &client_metadata.tags,
                trackers: &client_metadata.trackers,
            })?;
            database.mark_refreshed_client_info_hash(info_hash.as_str())?;
            refreshed = refreshed.saturating_add(1);
            if config.season_from_episodes.is_some() {
                if let Some(ensemble) = episode_ensemble(&searchee) {
                    database.upsert_ensemble(&EnsembleRecord {
                        client_host: Some(metadata.host.as_ref()),
                        path: &ensemble.path,
                        info_hash: Some(info_hash.as_str()),
                        ensemble: &ensemble.ensemble,
                        element: &ensemble.element,
                    })?;
                    database.mark_refreshed_client_ensemble_path(&ensemble.path)?;
                    ensemble_rows = ensemble_rows.saturating_add(1);
                }
            }
            Ok(())
        })?;
        result.client_searchees_refreshed =
            result.client_searchees_refreshed.saturating_add(refreshed);
        result.client_ensemble_rows_rebuilt = result
            .client_ensemble_rows_rebuilt
            .saturating_add(ensemble_rows);
        result.client_searchees_pruned = result
            .client_searchees_pruned
            .saturating_add(database.finish_client_searchee_refresh(metadata.host.as_ref())?);
    }
    Ok(())
}

fn prune_missing_ensemble_rows(database: &Database) -> crate::Result<usize> {
    let mut removed = 0usize;
    let mut after_rowid = 0_i64;
    loop {
        let rows = database.data_ensemble_rowid_path_page(after_rowid, CLEANUP_DB_PAGE_SIZE)?;
        let Some(last) = rows.last() else {
            break;
        };
        after_rowid = last.rowid;
        for row in rows {
            let path = row.path;
            if !Path::new(&path).exists() {
                removed += database.delete_ensemble_rowid(row.rowid)?;
            }
        }
    }
    Ok(removed)
}

fn prune_unused_torrent_cache(
    database: &Database,
    app_dir: &Path,
    config: &RuntimeConfig,
    now_millis: i64,
) -> crate::Result<usize> {
    let cache_dir = torrent_cache_dir(app_dir);
    let entries = match fs::read_dir(&cache_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => {
            return Err(operation_error(format!(
                "failed to read torrent cache {}: {error}",
                cache_dir.display()
            )));
        }
    };
    let age = config
        .exclude_recent_search
        .map(|value| value.saturating_add(THIRTY_DAYS_MILLIS))
        .unwrap_or(0)
        .max(ONE_YEAR_MILLIS);
    let cutoff = now_millis.saturating_sub(i64::try_from(age).unwrap_or(i64::MAX));
    let mut removed = 0usize;
    for entry in entries {
        let entry =
            entry.map_err(|error| operation_error(format!("torrent cache entry: {error}")))?;
        let path = entry.path();
        let Some(info_hash) = cache_info_hash(&path) else {
            continue;
        };
        if !recent_decision_exists(database, &info_hash, cutoff)? {
            fs::remove_file(&path).map_err(|error| {
                operation_error(format!(
                    "failed to remove torrent cache file {}: {error}",
                    path.display()
                ))
            })?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn prune_missing_cache_decisions(
    database: &Database,
    app_dir: &Path,
) -> crate::Result<MissingCacheDecisionCleanup> {
    let mut valid_count = 0usize;
    let mut missing_count = 0usize;
    for_each_decision_info_hash(database, |info_hash| {
        if let Some(hash) = InfoHash::new(info_hash) {
            valid_count = valid_count.saturating_add(1);
            if !torrent_cache_path(app_dir, &hash).exists() {
                missing_count = missing_count.saturating_add(1);
            }
        }
        Ok(())
    })?;

    if valid_count > 0 && missing_count == valid_count {
        return Ok(MissingCacheDecisionCleanup {
            removed: 0,
            catastrophic_skipped: true,
        });
    }

    let mut removed = 0usize;
    for_each_decision_info_hash(database, |info_hash| {
        if let Some(hash) = InfoHash::new(info_hash) {
            if !torrent_cache_path(app_dir, &hash).exists() {
                removed += database.delete_decisions_by_info_hash(info_hash)?;
            }
        }
        Ok(())
    })?;
    Ok(MissingCacheDecisionCleanup {
        removed,
        catastrophic_skipped: false,
    })
}

fn rebuild_guid_info_hash_map(database: &Database) -> crate::Result<usize> {
    let mut after_id = 0_i64;
    let mut count = 0usize;
    loop {
        let page = database.guid_info_hash_page(after_id, 1_000)?;
        let Some(last) = page.last() else {
            break;
        };
        after_id = last.id;
        count += page.len();
    }
    Ok(count)
}

fn recent_decision_exists(
    database: &Database,
    info_hash: &str,
    cutoff_millis: i64,
) -> crate::Result<bool> {
    database.recent_decision_exists(info_hash, cutoff_millis)
}

fn for_each_decision_info_hash<F>(database: &Database, mut handle: F) -> crate::Result<()>
where
    F: FnMut(&str) -> crate::Result<()>,
{
    let mut after_info_hash: Option<String> = None;
    loop {
        let page =
            database.decision_info_hash_page(after_info_hash.as_deref(), CLEANUP_DB_PAGE_SIZE)?;
        let Some(last) = page.last() else {
            break;
        };
        after_info_hash = Some(last.clone());
        for info_hash in page {
            handle(&info_hash)?;
        }
    }
    Ok(())
}

fn cache_info_hash(path: &Path) -> Option<String> {
    let filename = path.file_name()?.to_str()?;
    let info_hash = filename.strip_suffix(".cached.torrent")?;
    InfoHash::new(info_hash).map(|hash| hash.to_string())
}

fn generate_api_key() -> crate::Result<String> {
    let mut bytes = [0_u8; 24];
    getrandom::fill(&mut bytes)
        .map_err(|error| operation_error(format!("failed to generate api key: {error}")))?;
    let mut output = String::with_capacity(48);
    for byte in bytes {
        output.push(hex_digit(byte >> 4));
        output.push(hex_digit(byte & 0x0f));
    }
    Ok(output)
}

fn replace_bytes(input: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
    if from.is_empty() {
        return input.to_vec();
    }
    let mut output = Vec::with_capacity(input.len());
    let mut offset = 0;
    while offset < input.len() {
        let candidate = offset
            .checked_add(from.len())
            .and_then(|end| input.get(offset..end));
        if candidate == Some(from) {
            output.extend_from_slice(to);
            offset += from.len();
        } else if let Some(byte) = input.get(offset) {
            output.push(*byte);
            offset += 1;
        } else {
            break;
        }
    }
    output
}

fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => char::from(b'0' + nibble),
        _ => char::from(b'a' + (nibble - 10)),
    }
}

fn replace_torrent_tracker_urls(
    input: &[u8],
    from: &[u8],
    to: &[u8],
) -> crate::Result<Option<Vec<u8>>> {
    if from.is_empty() {
        return Ok(None);
    }

    let mut decoded = bdecode(input)?;
    let BencodeValue::Dict(entries) = &mut decoded.value else {
        return Err(operation_error("cached torrent root must be a dictionary"));
    };

    let mut changed = false;
    for (key, value) in entries {
        match key.as_ref() {
            b"announce" => changed |= replace_bencode_bytes(value, from, to),
            b"announce-list" => changed |= replace_bencode_bytes_recursive(value, from, to),
            _ => {}
        }
    }

    Ok(changed.then(|| bencode(&decoded)))
}

async fn run_blocking_workflow<T>(
    name: &'static str,
    task: impl FnOnce() -> crate::Result<T> + Send + 'static,
) -> crate::Result<T>
where
    T: Send + 'static,
{
    let permit = Arc::clone(&LOCAL_WORK_PERMITS)
        .acquire_owned()
        .await
        .map_err(|error| operation_error(format!("{name} local-work queue closed: {error}")))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        task()
    })
    .await
    .map_err(|error| operation_error(format!("{name} task failed: {error}")))?
}

fn operation_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::Operation {
        message: message.into(),
    }
}

fn replace_bencode_bytes(value: &mut Bencode<'_>, from: &[u8], to: &[u8]) -> bool {
    let BencodeValue::Bytes(bytes) = &mut value.value else {
        return false;
    };
    let updated = replace_bytes(bytes.as_ref(), from, to);
    if updated == bytes.as_ref() {
        false
    } else {
        *bytes = Cow::Owned(updated);
        true
    }
}

fn replace_bencode_bytes_recursive(value: &mut Bencode<'_>, from: &[u8], to: &[u8]) -> bool {
    match &mut value.value {
        BencodeValue::Bytes(_) => replace_bencode_bytes(value, from, to),
        BencodeValue::List(items) => {
            let mut changed = false;
            for item in items {
                changed |= replace_bencode_bytes_recursive(item, from, to);
            }
            changed
        }
        BencodeValue::Integer(_) | BencodeValue::Dict(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        api_key, api_key_async, cleanup_db, cleanup_db_with_clients, clear_cache,
        clear_cache_async, clear_client_cache, clear_client_cache_async, clear_indexer_failures,
        clear_indexer_failures_async, reset_api_key, reset_api_key_async, rss_time_since_last_run,
        run_announce_match, run_webhook_search, update_torrent_cache_trackers,
        webhook_matches_request, webhook_targets_and_excluded,
    };
    use crate::{
        api::WebhookRequest,
        clients::{
            ClientErrorCode, ClientTorrent, DownloadDirOptions, InjectionOptions, NewTorrent,
            ResumeOptions, TorrentClient,
        },
        config::{RawConfig, RuntimeConfig},
        domain::{
            Candidate, Decision, File, InfoHash, InjectionResult, Metafile, Searchee,
            TorrentClientKind, TorrentClientMetadata,
        },
        notifications::NotificationSender,
        persistence::{
            AsyncDatabase, ClientSearcheeRecord, Database, DecisionRecord, EnsembleRecord,
        },
        startup::Redactor,
    };
    use std::{
        borrow::Cow,
        collections::BTreeMap,
        fs,
        path::PathBuf,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn api_key_prefers_config_then_db_then_generated() {
        let root = temp_path("api");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");

        assert_eq!(
            api_key(&database, Some("configured-api-key")).expect("configured"),
            "configured-api-key"
        );
        let generated = api_key(&database, None).expect("generated");
        assert_eq!(generated.len(), 48);
        assert_eq!(api_key(&database, None).expect("stored"), generated);
        let reset = reset_api_key(&database).expect("reset");
        assert_eq!(reset.len(), 48);
        assert_ne!(reset, generated);

        let _cleanup = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn async_api_key_prefers_config_then_db_then_generated() {
        let root = temp_path("async-api");
        fs::create_dir_all(&root).expect("temp dir");
        let database = AsyncDatabase::open_app_dir(&root).await.expect("database");

        assert_eq!(
            api_key_async(&database, Some("configured-api-key"))
                .await
                .expect("configured"),
            "configured-api-key"
        );
        let generated = api_key_async(&database, None).await.expect("generated");
        assert_eq!(generated.len(), 48);
        assert_eq!(
            api_key_async(&database, None).await.expect("stored"),
            generated
        );
        let reset = reset_api_key_async(&database).await.expect("reset");
        assert_eq!(reset.len(), 48);
        assert_ne!(reset, generated);

        database.close().await;
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn clears_cache_tables() {
        let root = temp_path("clear-cache");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        let searchee_id = database
            .get_or_insert_searchee("name", 1)
            .expect("searchee");
        database
            .upsert_decision(&DecisionRecord {
                searchee_id,
                guid: "guid",
                info_hash: None,
                decision: crate::domain::Decision::NoDownloadLink,
                first_seen: 1,
                last_seen: 1,
                fuzzy_size_factor: 0.05,
            })
            .expect("decision");

        let result = clear_cache(&database).expect("clear");

        assert_eq!(result.decisions_removed, 1);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn async_clears_cache_tables_and_indexer_failures() {
        let root = temp_path("async-clear-cache");
        fs::create_dir_all(&root).expect("temp dir");
        let sync_database = Database::open_app_dir(&root).expect("database");
        let searchee_id = sync_database
            .get_or_insert_searchee("name", 1)
            .expect("searchee");
        sync_database
            .upsert_decision(&DecisionRecord {
                searchee_id,
                guid: "guid",
                info_hash: None,
                decision: crate::domain::Decision::NoDownloadLink,
                first_seen: 1,
                last_seen: 1,
                fuzzy_size_factor: 0.05,
            })
            .expect("decision");
        sync_database
            .connection()
            .execute(
                "INSERT INTO indexer (url, apikey, active, status, retry_after)
                 VALUES ('https://indexer.example', 'key', 1, 'RATE_LIMITED', 100)",
                [],
            )
            .expect("indexer");
        drop(sync_database);

        let database = AsyncDatabase::open_app_dir(&root).await.expect("database");

        let cache = clear_cache_async(&database).await.expect("clear");
        let failures = clear_indexer_failures_async(&database)
            .await
            .expect("failures");
        let client = clear_client_cache_async(&database)
            .await
            .expect("client cache");

        assert_eq!(cache.decisions_removed, 1);
        assert_eq!(failures, 1);
        assert_eq!(client.torrents_removed, 0);

        database.close().await;
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn clears_client_cache_tables_and_indexer_failures() {
        let root = temp_path("client-cache");
        fs::create_dir_all(&root).expect("temp dir");
        let database = Database::open_app_dir(&root).expect("database");
        database
            .connection()
            .execute(
                "INSERT INTO indexer (url, apikey, active, status, retry_after)
                 VALUES ('https://indexer.example', 'key', 1, 'RATE_LIMITED', 100)",
                [],
            )
            .expect("indexer");

        let failures = clear_indexer_failures(&database).expect("failures");
        let client = clear_client_cache(&database).expect("client cache");

        assert_eq!(failures, 1);
        assert_eq!(client.torrents_removed, 0);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn updates_cached_torrent_tracker_urls() {
        let root = temp_path("trackers");
        let cache_dir = root.join("torrent_cache");
        fs::create_dir_all(&cache_dir).expect("cache dir");
        let path = cache_dir.join("0123456789abcdef0123456789abcdef01234567.cached.torrent");
        fs::write(
            &path,
            b"d8:announce28:https://old.example/announce13:announce-listll28:https://old.example/announceeee",
        )
        .expect("write");

        let result = update_torrent_cache_trackers(
            &root,
            "https://old.example/announce",
            "https://longer-new.example/announce",
        )
        .expect("update");

        assert_eq!(result.files_seen, 1);
        assert_eq!(result.files_updated, 1);
        assert_eq!(
            fs::read(&path).expect("read"),
            b"d8:announce35:https://longer-new.example/announce13:announce-listll35:https://longer-new.example/announceeee"
        );
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn rss_elapsed_time_uses_persisted_last_run_with_cadence_fallback() {
        let root = temp_path("rss-last-run");
        let app_dir = root.join("app");
        let data_dir = root.join("data");
        fs::create_dir_all(&app_dir).expect("app dir");
        fs::create_dir_all(&data_dir).expect("data dir");
        let database = Database::open_app_dir(&app_dir).expect("database");
        let config = RuntimeConfig::normalize(
            RawConfig {
                rss_cadence: Some(600_000),
                data_dirs: vec![data_dir],
                ..RawConfig::default()
            },
            &app_dir,
        )
        .expect("config");

        assert_eq!(
            rss_time_since_last_run(&database, &config, 1_000_000).expect("missing cursor"),
            Duration::from_millis(600_000)
        );
        database
            .connection()
            .execute(
                "INSERT INTO job_log (name, last_run) VALUES ('rss', 100_000)",
                [],
            )
            .expect("job log");

        assert_eq!(
            rss_time_since_last_run(&database, &config, 250_000).expect("elapsed"),
            Duration::from_millis(150_000)
        );
        database
            .connection()
            .execute(
                "UPDATE job_log SET last_run = 300_000 WHERE name = 'rss'",
                [],
            )
            .expect("future job log");

        assert_eq!(
            rss_time_since_last_run(&database, &config, 250_000).expect("current cursor"),
            Duration::from_millis(600_000)
        );
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn cleanup_prunes_cache_null_decisions_and_missing_paths() {
        let root = temp_path("cleanup");
        fs::create_dir_all(root.join("torrent_cache")).expect("cache dir");
        let database = Database::open_app_dir(&root).expect("database");
        let existing_data = root.join("data");
        fs::create_dir_all(&existing_data).expect("data dir");
        let missing_data = root.join("missing-data");
        let missing_ensemble = root.join("missing-episode.mkv");
        database
            .connection()
            .execute(
                "INSERT INTO data (path, title) VALUES (?1, 'Existing'), (?2, 'Missing')",
                rusqlite::params![
                    existing_data.to_string_lossy(),
                    missing_data.to_string_lossy()
                ],
            )
            .expect("data");
        database
            .connection()
            .execute(
                "INSERT INTO ensemble (client_host, path, info_hash, ensemble, element)
                 VALUES (NULL, ?1, NULL, 'show s01', 'e01')",
                [missing_ensemble.to_string_lossy()],
            )
            .expect("ensemble");
        let searchee_id = database
            .get_or_insert_searchee("name", 1)
            .expect("searchee");
        let old_hash = "0123456789012345678901234567890123456789";
        let recent_hash = "1111111111111111111111111111111111111111";
        let missing_hash = "2222222222222222222222222222222222222222";
        fs::write(
            root.join("torrent_cache")
                .join(format!("{old_hash}.cached.torrent")),
            b"old",
        )
        .expect("old cache");
        fs::write(
            root.join("torrent_cache")
                .join(format!("{recent_hash}.cached.torrent")),
            b"recent",
        )
        .expect("recent cache");
        let now = 800 * 86_400_000;
        insert_decision(&database, searchee_id, "old-guid", Some(old_hash), 1);
        insert_decision(
            &database,
            searchee_id,
            "recent-guid",
            Some(recent_hash),
            now,
        );
        insert_decision(
            &database,
            searchee_id,
            "missing-guid",
            Some(missing_hash),
            now,
        );
        insert_decision(&database, searchee_id, "null-guid", None, now);
        let config = RuntimeConfig::normalize(
            RawConfig {
                data_dirs: vec![existing_data],
                season_from_episodes: Some(1.0),
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");

        let result = cleanup_db(&database, &root, &config, now).expect("cleanup");

        assert_eq!(result.data_rows_removed, 1);
        assert_eq!(result.ensemble_rows_removed, 1);
        assert_eq!(result.torrent_cache_files_removed, 1);
        assert_eq!(result.null_decisions_removed, 1);
        assert_eq!(result.missing_cache_decisions_removed, 2);
        assert!(!result.catastrophic_decision_cleanup_skipped);
        assert_eq!(result.guid_info_hash_rows, 1);
        assert!(
            !root
                .join("torrent_cache")
                .join(format!("{old_hash}.cached.torrent"))
                .exists()
        );
        assert!(
            root.join("torrent_cache")
                .join(format!("{recent_hash}.cached.torrent"))
                .exists()
        );
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn cleanup_skips_catastrophic_missing_decision_prune() {
        let root = temp_path("cleanup-guard");
        fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        let searchee_id = database
            .get_or_insert_searchee("name", 1)
            .expect("searchee");
        insert_decision(
            &database,
            searchee_id,
            "missing-guid",
            Some("0123456789012345678901234567890123456789"),
            2_000_000,
        );
        let config = RuntimeConfig::normalize(RawConfig::default(), &root).expect("config");

        let result = cleanup_db(&database, &root, &config, 2_000_000).expect("cleanup");

        assert!(result.catastrophic_decision_cleanup_skipped);
        assert_eq!(result.missing_cache_decisions_removed, 0);
        let remaining: i64 = database
            .connection()
            .query_row("SELECT COUNT(*) FROM decision", [], |row| row.get(0))
            .expect("count");
        assert_eq!(remaining, 1);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn announce_match_uses_reverse_lookup_pipeline() {
        let root = temp_path("announce");
        let torrent_dir = root.join("torrents");
        fs::create_dir_all(&torrent_dir).expect("torrent dir");
        let database = Database::open_app_dir(&root).expect("database");
        database
            .upsert_client_searchee(&ClientSearcheeRecord {
                client_host: "client-a",
                info_hash: "0123456789abcdef0123456789abcdef01234567",
                name: "Example.Show.S01E01",
                title: "Example Show S01E01",
                files: &[File::new("Example.Show.S01E01.mkv", 10)],
                length: 10,
                save_path: "/downloads",
                category: None,
                tags: &[],
                trackers: &[Cow::Borrowed("tracker.example")],
            })
            .expect("client searchee");
        let searchee_id = database
            .get_or_insert_searchee("Example Show S01E01", 1_000)
            .expect("searchee");
        database
            .upsert_decision(&DecisionRecord {
                searchee_id,
                guid: "https://tracker.example/download",
                info_hash: None,
                decision: Decision::MatchSizeOnly,
                first_seen: 1_000,
                last_seen: 1_000,
                fuzzy_size_factor: 0.05,
            })
            .expect("decision");
        let config = RuntimeConfig::normalize(
            RawConfig {
                torrent_dir: Some(torrent_dir),
                include_single_episodes: Some(true),
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");
        let notifier = NotificationSender::new(Vec::new(), Redactor::default()).expect("notifier");
        let candidate = Candidate::new(
            "Example.Show.S01E01",
            "https://tracker.example/download",
            Some("https://tracker.example/download"),
            "tracker",
        );

        let outcome = run_announce_match(&database, &root, &config, candidate, &notifier)
            .expect("announce")
            .expect("outcome");

        assert_eq!(outcome.decision, Decision::MatchSizeOnly);
        assert_eq!(outcome.action_result, None);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn webhook_search_targets_requested_path() {
        let root = temp_path("webhook");
        let release = root.join("Example.Show.S01E01");
        fs::create_dir_all(&release).expect("release dir");
        fs::write(release.join("Example.Show.S01E01.mkv"), b"video").expect("video");
        let database = Database::open_app_dir(&root).expect("database");
        let config = RuntimeConfig::normalize(
            RawConfig {
                data_dirs: vec![release.clone()],
                include_single_episodes: Some(false),
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");
        let notifier = NotificationSender::new(Vec::new(), Redactor::default()).expect("notifier");

        let summary = run_webhook_search(
            &database,
            &root,
            &config,
            WebhookRequest {
                info_hash: None,
                path: Some(release.display().to_string()),
                ignore_cross_seeds: false,
                ignore_exclude_recent_search: true,
                ignore_exclude_older: true,
                ignore_block_list: false,
                include_single_episodes: true,
                include_non_videos: false,
            },
            &notifier,
        )
        .expect("webhook search");

        assert_eq!(summary.searchees_seen, 1);
        assert_eq!(summary.indexer_searches, 0);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn webhook_match_canonicalizes_requested_path() {
        let root = temp_path("webhook-canonical");
        let data = root.join("data");
        fs::create_dir_all(&data).expect("data dir");
        let file = data.join("episode.mkv");
        fs::write(&file, b"video").expect("video");
        let mut searchee = Searchee::from_files(
            "Episode",
            "Episode",
            vec![File::new(file.display().to_string(), 5)],
        );
        searchee.path = Some(Cow::Owned(file.display().to_string()));
        let request = WebhookRequest {
            info_hash: None,
            path: Some(
                data.join("..")
                    .join("data")
                    .join("episode.mkv")
                    .display()
                    .to_string(),
            ),
            ignore_cross_seeds: false,
            ignore_exclude_recent_search: false,
            ignore_exclude_older: false,
            ignore_block_list: false,
            include_single_episodes: false,
            include_non_videos: false,
        };

        assert!(webhook_matches_request(&searchee, &request));
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn webhook_search_excludes_all_local_hashes_not_only_targets() {
        let mut target = Searchee::from_files(
            "Target",
            "Target",
            vec![File::new("/downloads/target.mkv", 5)],
        );
        target.path = Some(Cow::Borrowed("/downloads/target.mkv"));
        let mut other = Searchee::from_files(
            "Existing",
            "Existing",
            vec![File::new("/downloads/existing.mkv", 5)],
        );
        other.info_hash = Some(InfoHash::from_validated(
            "0123456789abcdef0123456789abcdef01234567",
        ));
        let request = WebhookRequest {
            info_hash: None,
            path: Some("/downloads/target.mkv".to_owned()),
            ignore_cross_seeds: false,
            ignore_exclude_recent_search: false,
            ignore_exclude_older: false,
            ignore_block_list: false,
            include_single_episodes: false,
            include_non_videos: false,
        };

        let (targets, excluded) =
            webhook_targets_and_excluded(vec![target.into_owned(), other.into_owned()], &request);

        assert_eq!(targets.len(), 1);
        assert!(excluded.contains("0123456789abcdef0123456789abcdef01234567"));
    }

    #[test]
    fn cleanup_refreshes_client_searchees_and_rebuilds_ensemble() {
        let root = temp_path("cleanup-client-refresh");
        fs::create_dir_all(&root).expect("root");
        let database = Database::open_app_dir(&root).expect("database");
        let stale_files = [File::new("Old.Show.S01E01.mkv", 1)];
        database
            .refresh_client_searchees(
                "localhost",
                [ClientSearcheeRecord {
                    client_host: "localhost",
                    info_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    name: "Old.Show.S01E01",
                    title: "Old Show S01E01",
                    files: &stale_files,
                    length: 1,
                    save_path: "/downloads",
                    category: None,
                    tags: &[],
                    trackers: &[],
                }],
            )
            .expect("seed client");
        database
            .upsert_ensemble(&EnsembleRecord {
                client_host: Some("localhost"),
                path: "/downloads/Old.Show.S01E01.mkv",
                info_hash: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                ensemble: "old.show S01",
                element: "01",
            })
            .expect("stale ensemble");
        database
            .upsert_ensemble(&EnsembleRecord {
                client_host: Some("localhost"),
                path: "/downloads/Example.Show.S01E01.old.mkv",
                info_hash: Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                ensemble: "example.show S01",
                element: "01",
            })
            .expect("stale same-hash ensemble");
        let client = FakeClient::new(vec![ClientTorrent {
            info_hash: InfoHash::new("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
                .expect("hash")
                .into_owned(),
            name: Cow::Borrowed("Example.Show.S01E01"),
            files: vec![File::new("Example.Show.S01E01.mkv", 42)],
            save_path: Cow::Borrowed("/downloads"),
            category: None,
            tags: Vec::new(),
            trackers: Vec::new(),
            complete: true,
            checking: false,
        }]);
        let config = RuntimeConfig::normalize(
            RawConfig {
                use_client_torrents: Some(true),
                season_from_episodes: Some(1.0),
                torrent_clients: vec![
                    crate::config::TorrentClientConfig::parse("qbittorrent:http://localhost:8080")
                        .expect("client"),
                ],
                ..RawConfig::default()
            },
            &root,
        )
        .expect("config");

        let result = cleanup_db_with_clients(&database, &root, &config, 2_000_000, &[&client])
            .expect("cleanup");

        assert_eq!(result.client_searchees_refreshed, 1);
        assert_eq!(result.client_searchees_pruned, 1);
        assert_eq!(result.client_ensemble_rows_rebuilt, 1);
        let client_rows: i64 = database
            .connection()
            .query_row("SELECT COUNT(*) FROM client_searchee", [], |row| row.get(0))
            .expect("client count");
        let ensemble_path: String = database
            .connection()
            .query_row("SELECT path FROM ensemble", [], |row| row.get(0))
            .expect("ensemble path");
        let ensemble_rows: i64 = database
            .connection()
            .query_row("SELECT COUNT(*) FROM ensemble", [], |row| row.get(0))
            .expect("ensemble count");
        assert_eq!(client_rows, 1);
        assert_eq!(ensemble_rows, 1);
        assert_eq!(ensemble_path, "/downloads/Example.Show.S01E01.mkv");
        let _cleanup = fs::remove_dir_all(root);
    }

    fn insert_decision(
        database: &Database,
        searchee_id: i64,
        guid: &str,
        info_hash: Option<&str>,
        last_seen: i64,
    ) {
        database
            .upsert_decision(&DecisionRecord {
                searchee_id,
                guid,
                info_hash,
                decision: Decision::Match,
                first_seen: last_seen,
                last_seen,
                fuzzy_size_factor: 0.05,
            })
            .expect("decision");
    }

    fn temp_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("sporos-ops-{label}-{}-{nanos}", std::process::id()))
    }

    struct FakeClient {
        metadata: TorrentClientMetadata<'static>,
        torrents: Vec<ClientTorrent<'static>>,
    }

    impl FakeClient {
        fn new(torrents: Vec<ClientTorrent<'static>>) -> Self {
            Self {
                metadata: TorrentClientMetadata::new(
                    "localhost",
                    0,
                    TorrentClientKind::QBittorrent,
                    false,
                    "fake",
                ),
                torrents,
            }
        }
    }

    impl TorrentClient for FakeClient {
        fn metadata(&self) -> &TorrentClientMetadata<'_> {
            &self.metadata
        }

        fn is_torrent_in_client(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
            Ok(false)
        }

        fn is_torrent_complete(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
            Ok(false)
        }

        fn is_torrent_checking(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
            Ok(false)
        }

        fn get_all_torrents(&self) -> crate::Result<Vec<ClientTorrent<'static>>> {
            Ok(self.torrents.clone())
        }

        fn get_download_dir(
            &self,
            _metafile: &Metafile<'_>,
            _options: DownloadDirOptions,
        ) -> crate::Result<Result<PathBuf, ClientErrorCode>> {
            Ok(Err(ClientErrorCode::NotFound))
        }

        fn get_all_download_dirs(&self) -> crate::Result<BTreeMap<String, PathBuf>> {
            Ok(BTreeMap::new())
        }

        fn inject(
            &self,
            _new_torrent: &NewTorrent<'_>,
            _searchee: &Searchee<'_>,
            _decision: Decision,
            _options: &InjectionOptions,
        ) -> crate::Result<InjectionResult> {
            Ok(InjectionResult::Injected)
        }

        fn recheck_torrent(&self, _info_hash: &InfoHash<'_>) -> crate::Result<()> {
            Ok(())
        }

        fn resume_injection(
            &self,
            _metafile: &Metafile<'_>,
            _decision: Decision,
            _options: ResumeOptions,
        ) -> crate::Result<()> {
            Ok(())
        }

        fn validate_config(&self) -> crate::Result<()> {
            Ok(())
        }
    }
}
