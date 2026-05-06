//! Maintenance operations for cache, indexer, API-key, diff, and tree commands.

use std::{
    borrow::Cow,
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

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
        enabled_search_indexers, fetch_torznab_caps, for_each_rss_page, sync_torznab_indexers,
        update_indexer_caps, validate_arr_config, validate_torznab_config,
    },
    matching::AssessmentOptions,
    notifications::NotificationSender,
    persistence::{
        AsyncDatabase, CacheTable, ClientSearcheeRecord, DataRootRecord, Database, EnsembleRecord,
    },
    runtime::{BlockingTaskError, RuntimeBlockingExecutor},
    search::{
        Blocklist, CandidateSearchCache, ContentFilterOptions, PipelineAction, PipelineSummary,
        ReverseLookupGate, ReverseLookupRuntime, SearchPipelineOptions, SearchPipelineRuntime,
        SearcheeSources, TorrentDirIndexResult, VirtualSeasonOptions, bulk_search,
        check_new_candidate_match, check_new_candidate_matches, episode_ensemble,
        find_all_searchees, find_on_other_sites, find_searchable_searchees,
        for_each_data_dir_searchee, lookup_fields,
    },
    torrent::{torrent_cache_dir, torrent_cache_path},
};

mod torrents;

pub use torrents::{diff_torrents, torrent_tree, update_torrent_cache_trackers};

const ONE_DAY_MILLIS: u64 = 86_400_000;
const THIRTY_DAYS_MILLIS: u64 = 30 * ONE_DAY_MILLIS;
const ONE_YEAR_MILLIS: u64 = 365 * ONE_DAY_MILLIS;
const CLEANUP_DB_PAGE_SIZE: i64 = 1_000;

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

/// Result from refreshing configured local search indexes.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub struct IndexRefreshResult {
    /// Torrent-dir refresh counts when a torrent_dir is configured.
    pub torrent_dir: Option<TorrentDirIndexResult>,
    /// Data-dir roots indexed.
    pub data_roots_indexed: usize,
    /// Data-dir rows pruned because roots disappeared.
    pub data_roots_removed: usize,
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
    let ensemble_removed = database.clear_table(CacheTable::Ensemble)?;
    let client_searchees_removed = database.clear_table(CacheTable::ClientSearchee)?;
    let data_removed = database.clear_table(CacheTable::Data)?;
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
    let ensemble_removed = database.clear_table(CacheTable::Ensemble).await?;
    let client_searchees_removed = database.clear_table(CacheTable::ClientSearchee).await?;
    let data_removed = database.clear_table(CacheTable::Data).await?;
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
    blocking: RuntimeBlockingExecutor,
    _app_dir: PathBuf,
    config: RuntimeConfig,
    now_millis: i64,
) -> crate::Result<CleanupDbResult> {
    run_blocking_operation(blocking, "cleanup", move || {
        let database = Database::open(&config.database_path)?;
        let client_timeout = config.search_timeout.map(Duration::from_millis);
        let client_adapters = if config.use_client_torrents {
            build_torrent_clients(&config.torrent_clients, client_timeout)?
        } else {
            Vec::new()
        };
        let client_refs = client_refs(&client_adapters);
        cleanup_db_with_clients(
            &database,
            &config.state_dir,
            &config,
            now_millis,
            &client_refs,
        )
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
    refresh_torrent_and_data_indexes(database, config)?;
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
    blocking: RuntimeBlockingExecutor,
    _app_dir: PathBuf,
    config: RuntimeConfig,
    notifier: NotificationSender,
) -> crate::Result<SearchWorkflowResult> {
    run_blocking_operation(blocking, "search workflow", move || {
        let database = Database::open(&config.database_path)?;
        run_search_workflow(&database, &config.state_dir, &config, &notifier)
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
    refresh_torrent_and_data_indexes(database, config)?;
    let client_adapters = build_workflow_clients(config)?;
    let client_refs = client_refs(&client_adapters);
    refresh_workflow_client_searchees(database, config, &client_refs)?;
    let blocklist = Blocklist::parse(&config.block_list)?;
    let indexers = enabled_search_indexers(database, current_time_millis())?;
    let arr_configs = build_arr_configs(config)?;
    let now_millis = current_time_millis();
    let time_since_last_run = rss_time_since_last_run(database, config, now_millis)?;
    let local = find_all_searchees(
        &SearcheeSources {
            torrents: None,
            use_client_torrents: false,
            client_searchees: &[],
            torrent_dir: if config.use_client_torrents {
                None
            } else {
                config.torrent_dir.as_deref()
            },
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
    let mut attempts = 0usize;
    let candidates = for_each_rss_page(
        database,
        &indexers,
        RssPagerOptions {
            time_since_last_run,
            timeout: config.search_timeout.map(Duration::from_millis),
            delay: Duration::from_secs(config.delay),
            now_millis,
        },
        |page| {
            let page_attempts = check_new_candidate_matches(
                &runtime,
                page,
                &local,
                |action| dispatch_pipeline_action(app_dir, config, &injection, action),
                |attempt| {
                    let _report = notifier.send_result(attempt);
                    Ok(())
                },
            )?;
            attempts = attempts.saturating_add(page_attempts.len());
            Ok(())
        },
    )?;
    Ok(RssWorkflowResult {
        candidates,
        attempts,
    })
}

/// Run one RSS reverse-match workflow from async orchestration.
pub async fn run_rss_workflow_async(
    blocking: RuntimeBlockingExecutor,
    _app_dir: PathBuf,
    config: RuntimeConfig,
    notifier: NotificationSender,
) -> crate::Result<RssWorkflowResult> {
    run_blocking_operation(blocking, "rss workflow", move || {
        let database = Database::open(&config.database_path)?;
        run_rss_workflow(&database, &config.state_dir, &config, &notifier)
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
    refresh_torrent_and_data_indexes(database, config)?;
    let client_adapters = build_workflow_clients(config)?;
    let client_refs = client_refs(&client_adapters);
    refresh_workflow_client_searchees(database, config, &client_refs)?;
    let blocklist = Blocklist::parse(&config.block_list)?;
    let arr_configs = build_arr_configs(config)?;
    let local = find_all_searchees(
        &SearcheeSources {
            torrents: None,
            use_client_torrents: false,
            client_searchees: &[],
            torrent_dir: if config.use_client_torrents {
                None
            } else {
                config.torrent_dir.as_deref()
            },
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
    blocking: RuntimeBlockingExecutor,
    _app_dir: PathBuf,
    config: RuntimeConfig,
    candidate: Candidate<'static>,
    notifier: NotificationSender,
) -> crate::Result<Option<ApiOutcome>> {
    run_blocking_operation(blocking, "announce workflow", move || {
        let database = Database::open(&config.database_path)?;
        run_announce_match(&database, &config.state_dir, &config, candidate, &notifier)
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
    request.revalidate_path()?;
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
    refresh_torrent_and_data_indexes(database, &config)?;
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
        summary.merge(result);
    }
    Ok(summary)
}

/// Run one targeted webhook search from async orchestration.
pub async fn run_webhook_search_async(
    blocking: RuntimeBlockingExecutor,
    _app_dir: PathBuf,
    config: RuntimeConfig,
    request: WebhookRequest,
    notifier: NotificationSender,
) -> crate::Result<PipelineSummary> {
    run_blocking_operation(blocking, "webhook workflow", move || {
        let database = Database::open(&config.database_path)?;
        run_webhook_search(&database, &config.state_dir, &config, request, &notifier)
    })
    .await
}

/// Run one saved torrent injection workflow.
pub fn run_inject_workflow(
    database: &Database,
    _app_dir: &Path,
    config: &RuntimeConfig,
) -> crate::Result<SavedInjectionSummary> {
    refresh_torrent_and_data_indexes(database, config)?;
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
    blocking: RuntimeBlockingExecutor,
    _app_dir: PathBuf,
    config: RuntimeConfig,
) -> crate::Result<SavedInjectionSummary> {
    run_blocking_operation(blocking, "inject workflow", move || {
        let database = Database::open(&config.database_path)?;
        run_inject_workflow(&database, &config.state_dir, &config)
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
    blocking: RuntimeBlockingExecutor,
    _app_dir: PathBuf,
    config: RuntimeConfig,
) -> crate::Result<RestoreSummary> {
    run_blocking_operation(blocking, "restore workflow", move || {
        let database = Database::open(&config.database_path)?;
        run_restore_workflow(&database, &config.state_dir, &config)
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
    blocking: RuntimeBlockingExecutor,
    _app_dir: PathBuf,
    config: RuntimeConfig,
) -> crate::Result<IndexerCapsRefreshResult> {
    run_blocking_operation(blocking, "indexer caps workflow", move || {
        let database = Database::open(&config.database_path)?;
        run_update_indexer_caps(&database, &config)
    })
    .await
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

/// Refresh configured torrent_dir and data_dir indexes.
pub fn refresh_torrent_and_data_indexes(
    database: &Database,
    config: &RuntimeConfig,
) -> crate::Result<IndexRefreshResult> {
    let mut result = IndexRefreshResult::default();
    if let Some(torrent_dir) = &config.torrent_dir {
        let indexed = crate::search::index_torrent_dir(database, torrent_dir)?;
        tracing::info!(
            files_seen = indexed.files_seen,
            torrents_indexed = indexed.torrents_indexed,
            torrents_removed = indexed.torrents_removed,
            files_failed = indexed.files_failed,
            "indexed torrent_dir"
        );
        result.torrent_dir = Some(indexed);
    }
    if !config.data_dirs.is_empty() {
        database.begin_data_root_refresh()?;
        result.data_roots_indexed =
            for_each_data_dir_searchee(&config.data_dirs, config.max_data_depth, |searchee| {
                let Some(path) = searchee.path.as_deref() else {
                    return Ok(());
                };
                let lookup = lookup_fields(&searchee);
                database.upsert_data_root(&DataRootRecord {
                    path,
                    title: searchee.title.as_ref(),
                    lookup: Some(&lookup),
                })?;
                database.mark_refreshed_data_root(path)
            })?;
        result.data_roots_removed = database.finish_data_root_refresh()?;
        tracing::info!(
            roots_indexed = result.data_roots_indexed,
            roots_removed = result.data_roots_removed,
            "indexed data_dirs"
        );
    }
    Ok(result)
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

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
struct ClientSearcheeRefreshCounts {
    refreshed: usize,
    ensemble_rows: usize,
    pruned: usize,
}

fn refresh_workflow_client_searchees(
    database: &Database,
    config: &RuntimeConfig,
    clients: &[&dyn TorrentClient],
) -> crate::Result<()> {
    if !config.use_client_torrents {
        return Ok(());
    }
    for client in clients {
        refresh_client_searchee_cache(database, config, *client)?;
    }
    Ok(())
}

fn refresh_client_searchee_cache(
    database: &Database,
    config: &RuntimeConfig,
    client: &dyn TorrentClient,
) -> crate::Result<ClientSearcheeRefreshCounts> {
    let metadata = client.metadata().clone().into_owned();
    database.begin_client_searchee_refresh()?;
    let mut counts = ClientSearcheeRefreshCounts::default();
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
        let lookup = lookup_fields(&searchee);
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
            lookup: Some(&lookup),
        })?;
        database.mark_refreshed_client_info_hash(info_hash.as_str())?;
        counts.refreshed = counts.refreshed.saturating_add(1);
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
                counts.ensemble_rows = counts.ensemble_rows.saturating_add(1);
            }
        }
        Ok(())
    })?;
    counts.pruned = database.finish_client_searchee_refresh(metadata.host.as_ref())?;
    Ok(counts)
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
            retries: config.snatch_retries,
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
        category: config.injection_category.clone().map(ClientLabel::new),
        tags: config
            .injection_tags
            .iter()
            .cloned()
            .map(ClientLabel::new)
            .collect(),
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
        let counts = refresh_client_searchee_cache(database, config, *client)?;
        result.client_searchees_refreshed = result
            .client_searchees_refreshed
            .saturating_add(counts.refreshed);
        result.client_ensemble_rows_rebuilt = result
            .client_ensemble_rows_rebuilt
            .saturating_add(counts.ensemble_rows);
        result.client_searchees_pruned =
            result.client_searchees_pruned.saturating_add(counts.pruned);
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

fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => char::from(b'0' + nibble),
        _ => char::from(b'a' + (nibble - 10)),
    }
}

async fn run_blocking_operation<T>(
    blocking: RuntimeBlockingExecutor,
    name: &'static str,
    task: impl FnOnce() -> crate::Result<T> + Send + 'static,
) -> crate::Result<T>
where
    T: Send + 'static,
{
    let task = move || {
        let span = tracing::info_span!("blocking operation", operation = name);
        let _guard = span.enter();
        task()
    };
    blocking
        .submit(name, task)
        .await
        .map_err(|error| blocking_operation_error(name, error))?
}

fn operation_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::Operation {
        message: message.into(),
    }
}

fn blocking_operation_error(name: &'static str, error: BlockingTaskError) -> SporosError {
    operation_error(match error {
        BlockingTaskError::Queue(error) => {
            format!("{name} local-work queue rejected task: {error}")
        }
        BlockingTaskError::Cancelled { executor, kind } => {
            format!("{name} local-work task cancelled in {executor} for {kind}")
        }
        BlockingTaskError::Panicked { executor, kind } => {
            format!("{name} local-work task panicked in {executor} for {kind}")
        }
    })
}

#[cfg(test)]
mod tests;
