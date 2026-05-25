use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

use crate::clients::qbittorrent::{QbitAddTorrent, QbitContentLayout, QbittorrentClient};
use crate::clients::rtorrent::RtorrentClient;
use crate::config::ConfigTorrentClientKind;
use crate::config::{CONFIG_SCHEMA, DEFAULT_CONFIG_PATH, load_config};
use crate::domain::{
    ByteSize, CandidateGuid, DownloadUrl, IndexerId, InfoHash, ItemTitle, RemoteCandidate,
    TrackerName,
};
use crate::indexers::TorznabRegistry;
use crate::persistence::repository::Repository;
use crate::persistence::torrent_cache::cached_torrent_path;
use crate::runtime::announce_worker::unix_time_ms;
use crate::runtime::app::validate_runtime_config;
use crate::runtime::daemon;

const SYSTEM_TEST_DIAGNOSTIC_LIMIT: u16 = 8;
const SYSTEM_TEST_TEXT_LIMIT: usize = 160;

#[derive(Debug, Parser)]
#[command(name = "sporos", about = "Sporos torrent automation service")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },
    CheckConfig {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },
    PrintConfigSchema,
    #[command(hide = true)]
    SystemTestSeed {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long, default_value = "fixture")]
        indexer: String,
        #[arg(long, default_value = "http://torznab-fixture:8080/torrents")]
        fixture_base_url: String,
    },
    #[command(hide = true)]
    SystemTestSnapshot {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },
    #[command(hide = true)]
    SystemTestLoadSources {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
    },
    #[command(hide = true)]
    SystemTestClientState {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
    },
    #[command(hide = true)]
    SystemTestDiagnostics {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },
}

pub fn run(args: impl IntoIterator<Item = OsString>) -> Result<String, String> {
    let cli = Cli::try_parse_from(args).map_err(|error| error.to_string())?;

    match cli.command {
        Command::Serve { config } => {
            let loaded = load_config(&config).map_err(|error| error.to_string())?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            runtime
                .block_on(daemon::serve(loaded))
                .map_err(|error| error.to_string())?;
            Ok(String::new())
        }
        Command::CheckConfig { config } => {
            let loaded = load_config(&config).map_err(|error| error.to_string())?;
            validate_runtime_config(&loaded).map_err(|error| error.to_string())?;
            Ok(format!("sporos config ok: {}", config.display()))
        }
        Command::PrintConfigSchema => Ok(CONFIG_SCHEMA.to_owned()),
        Command::SystemTestSeed {
            config,
            manifest,
            indexer,
            fixture_base_url,
        } => {
            let loaded = load_config(&config).map_err(|error| error.to_string())?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            let seeded = runtime.block_on(seed_system_test_candidates(
                loaded,
                manifest,
                &indexer,
                &fixture_base_url,
            ))?;
            Ok(format!(
                "seeded {seeded} system-test candidates for indexer {indexer}"
            ))
        }
        Command::SystemTestSnapshot { config } => {
            let loaded = load_config(&config).map_err(|error| error.to_string())?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            let snapshot = runtime.block_on(system_test_snapshot(loaded))?;
            serde_json::to_string(&snapshot).map_err(|error| error.to_string())
        }
        Command::SystemTestLoadSources { config, manifest } => {
            let loaded = load_config(&config).map_err(|error| error.to_string())?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            let loaded = runtime.block_on(load_system_test_sources(loaded, manifest))?;
            Ok(format!("loaded {loaded} system-test source torrents"))
        }
        Command::SystemTestClientState { config, manifest } => {
            let loaded = load_config(&config).map_err(|error| error.to_string())?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            let state = runtime.block_on(system_test_client_state(loaded, manifest))?;
            serde_json::to_string(&state).map_err(|error| error.to_string())
        }
        Command::SystemTestDiagnostics { config } => {
            let loaded = load_config(&config).map_err(|error| error.to_string())?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            let diagnostics = runtime.block_on(system_test_diagnostics(loaded))?;
            serde_json::to_string(&diagnostics).map_err(|error| error.to_string())
        }
    }
}

#[derive(Debug, Deserialize)]
struct SystemFixtureManifest {
    fixtures: Vec<SystemFixture>,
}

#[derive(Debug, Deserialize)]
struct SystemFixture {
    slug: String,
    name: String,
    torrent_path: PathBuf,
    info_hash: String,
    files: Vec<SystemFixtureFile>,
}

#[derive(Debug, Deserialize)]
struct SystemFixtureFile {
    size: u64,
}

#[derive(Debug, Serialize)]
struct SystemTestSnapshot {
    local_items: i64,
    local_files: i64,
    remote_candidates: i64,
    cached_candidates: i64,
    match_decisions: i64,
    enabled_indexers: i64,
    saved_torrents: i64,
    client_items: Vec<SystemTestClientItem>,
}

#[derive(Debug, Serialize)]
struct SystemTestClientItem {
    title: String,
    source_key: String,
    info_hash: Option<String>,
    save_path: Option<String>,
    file_count: i64,
}

#[derive(Debug, Serialize)]
struct SystemTestClientState {
    qbittorrent: Option<SystemTestQbitTorrent>,
    rtorrent: Option<SystemTestRtorrentDownload>,
}

#[derive(Debug, Serialize)]
struct SystemTestQbitTorrent {
    hash: String,
    name: String,
    save_path: Option<String>,
    category: Option<String>,
    tags: Option<String>,
    amount_left: Option<u64>,
    files: Vec<SystemTestTorrentFileDiagnostic>,
}

#[derive(Debug, Serialize)]
struct SystemTestRtorrentDownload {
    hash: String,
    name: String,
    directory: String,
    label: Option<String>,
    left_bytes: u64,
    complete: bool,
    files: Vec<SystemTestTorrentFileDiagnostic>,
}

#[derive(Debug, Serialize)]
struct SystemTestDiagnostics {
    snapshot: SystemTestSnapshot,
    local_items: Vec<SystemTestLocalItemDiagnostic>,
    local_files: Vec<SystemTestLocalFileDiagnostic>,
    remote_candidates: Vec<SystemTestRemoteCandidateDiagnostic>,
    match_decisions: Vec<SystemTestMatchDecisionDiagnostic>,
    indexers: Vec<SystemTestIndexerDiagnostic>,
    dependency_health: Vec<SystemTestDependencyHealthDiagnostic>,
    jobs: Vec<SystemTestJobDiagnostic>,
    announce_work: Vec<SystemTestAnnounceWorkDiagnostic>,
}

#[derive(Debug, Serialize)]
struct SystemTestLocalItemDiagnostic {
    id: i64,
    source_type: String,
    source_key: String,
    title: String,
    media_type: String,
    info_hash: Option<String>,
    save_path: Option<String>,
    total_size: i64,
}

#[derive(Debug, Serialize)]
struct SystemTestLocalFileDiagnostic {
    item_id: i64,
    relative_path: String,
    file_name: String,
    size: i64,
    file_index: i64,
}

#[derive(Debug, Serialize)]
struct SystemTestRemoteCandidateDiagnostic {
    id: i64,
    guid: String,
    title: String,
    tracker: String,
    size: Option<i64>,
    info_hash: Option<String>,
    torrent_cache_path: Option<String>,
    last_seen_at: i64,
}

#[derive(Debug, Serialize)]
struct SystemTestMatchDecisionDiagnostic {
    local_item_id: i64,
    candidate_id: i64,
    decision: String,
    matched_size: Option<i64>,
    matched_ratio: Option<f64>,
    reason_code: String,
    assessed_at: i64,
}

#[derive(Debug, Serialize)]
struct SystemTestIndexerDiagnostic {
    id: i64,
    name: String,
    source_kind: String,
    enabled: bool,
    state: String,
    retry_after: Option<i64>,
    last_caps_refresh_at: Option<i64>,
}

#[derive(Debug, Serialize)]
struct SystemTestDependencyHealthDiagnostic {
    dependency_type: String,
    dependency_name: String,
    state: String,
    reason: Option<String>,
    retry_after: Option<i64>,
    failure_count: i64,
    checked_at: i64,
}

#[derive(Debug, Serialize)]
struct SystemTestJobDiagnostic {
    name: String,
    state: String,
    last_started_at: Option<i64>,
    last_finished_at: Option<i64>,
    next_run_at: Option<i64>,
    last_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct SystemTestAnnounceWorkDiagnostic {
    id: String,
    tracker: String,
    title: String,
    info_hash: Option<String>,
    status: String,
    reason: String,
    attempt_count: i64,
    next_attempt_at: i64,
    last_error_class: Option<String>,
    last_decision: Option<String>,
    last_action_outcome: Option<String>,
}

#[derive(Debug, Serialize)]
struct SystemTestTorrentFileDiagnostic {
    relative_path: String,
    file_name: String,
    size: u64,
    file_index: u32,
}

async fn seed_system_test_candidates(
    config: crate::config::SporosConfig,
    manifest_path: PathBuf,
    indexer_name: &str,
    fixture_base_url: &str,
) -> Result<usize, String> {
    let manifest_bytes = fs::read(&manifest_path)
        .map_err(|error| format!("read fixture manifest {}: {error}", manifest_path.display()))?;
    let manifest: SystemFixtureManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|error| {
            format!(
                "parse fixture manifest {}: {error}",
                manifest_path.display()
            )
        })?;
    let manifest_dir = manifest_path.parent().ok_or_else(|| {
        format!(
            "fixture manifest has no parent: {}",
            manifest_path.display()
        )
    })?;
    fs::create_dir_all(&config.paths.torrent_cache_dir).map_err(|error| {
        format!(
            "create torrent cache dir {}: {error}",
            config.paths.torrent_cache_dir.display()
        )
    })?;

    let repository = Repository::connect(&config.paths.database)
        .await
        .map_err(|error| error.to_string())?;
    let registry =
        TorznabRegistry::from_config(&config.indexers).map_err(|error| error.to_string())?;
    repository
        .sync_torznab_indexers(registry.indexers(), unix_time_ms())
        .await
        .map_err(|error| error.to_string())?;
    let indexer = repository
        .indexer_registry_snapshot(1_000)
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .find(|row| row.name.as_str() == indexer_name && row.enabled)
        .ok_or_else(|| format!("indexer `{indexer_name}` is not synced and enabled"))?;
    let indexer_id = IndexerId::new(indexer.id)
        .map_err(|error| format!("indexer `{indexer_name}` has invalid id: {error}"))?;

    let mut seeded = 0_usize;
    for fixture in manifest
        .fixtures
        .into_iter()
        .filter(|fixture| fixture.slug.ends_with("-candidate"))
    {
        let info_hash = InfoHash::new(fixture.info_hash.clone())
            .map_err(|error| format!("invalid fixture info hash {}: {error}", fixture.slug))?;
        let source = manifest_dir.join(&fixture.torrent_path);
        let cache_path = cached_torrent_path(&config.paths.torrent_cache_dir, &info_hash);
        fs::copy(&source, &cache_path).map_err(|error| {
            format!(
                "copy fixture torrent {} to {}: {error}",
                source.display(),
                cache_path.display()
            )
        })?;
        repository
            .upsert_remote_candidate(&RemoteCandidate {
                id: None,
                indexer_id,
                guid: CandidateGuid::new(format!("sporos-{}", fixture.slug))
                    .map_err(|error| error.to_string())?,
                download_url: DownloadUrl::new(format!(
                    "{}/{}.torrent",
                    fixture_base_url.trim_end_matches('/'),
                    fixture.slug
                ))
                .map_err(|error| error.to_string())?,
                title: ItemTitle::new(fixture.name).map_err(|error| error.to_string())?,
                tracker: TrackerName::new(indexer_name.to_owned())
                    .map_err(|error| error.to_string())?,
                size: Some(ByteSize::new(
                    fixture
                        .files
                        .iter()
                        .map(|file| file.size)
                        .fold(0_u64, u64::saturating_add),
                )),
                published_at_ms: None,
                info_hash: Some(info_hash),
                torrent_cache_path: Some(cache_path),
            })
            .await
            .map_err(|error| error.to_string())?;
        seeded = seeded.saturating_add(1);
    }

    Ok(seeded)
}

async fn system_test_snapshot(
    config: crate::config::SporosConfig,
) -> Result<SystemTestSnapshot, String> {
    let repository = Repository::connect(&config.paths.database)
        .await
        .map_err(|error| error.to_string())?;
    let snapshot = repository
        .system_test_snapshot(SYSTEM_TEST_DIAGNOSTIC_LIMIT)
        .await
        .map_err(|error| error.to_string())?;
    let saved_torrents = fs::read_dir(&config.paths.output_dir)
        .map_err(|error| {
            format!(
                "read output dir {}: {error}",
                config.paths.output_dir.display()
            )
        })?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .ends_with(crate::persistence::torrent_cache::SAVED_TORRENT_SUFFIX)
        })
        .count();
    let saved_torrents =
        i64::try_from(saved_torrents).map_err(|error| format!("count saved torrents: {error}"))?;

    Ok(SystemTestSnapshot {
        local_items: snapshot.local_items,
        local_files: snapshot.local_files,
        remote_candidates: snapshot.remote_candidates,
        cached_candidates: snapshot.cached_candidates,
        match_decisions: snapshot.match_decisions,
        enabled_indexers: snapshot.enabled_indexers,
        saved_torrents,
        client_items: snapshot
            .client_items
            .into_iter()
            .map(|row| SystemTestClientItem {
                title: truncated(row.title),
                source_key: truncated(row.source_key),
                info_hash: row.info_hash,
                save_path: truncated_option(row.save_path),
                file_count: row.file_count,
            })
            .collect(),
    })
}

async fn load_system_test_sources(
    config: crate::config::SporosConfig,
    manifest_path: PathBuf,
) -> Result<usize, String> {
    let manifest_bytes = fs::read(&manifest_path)
        .map_err(|error| format!("read fixture manifest {}: {error}", manifest_path.display()))?;
    let manifest: SystemFixtureManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|error| {
            format!(
                "parse fixture manifest {}: {error}",
                manifest_path.display()
            )
        })?;
    let manifest_dir = manifest_path.parent().ok_or_else(|| {
        format!(
            "fixture manifest has no parent: {}",
            manifest_path.display()
        )
    })?;

    let mut loaded = 0_usize;
    for (name, client_config) in &config.torrent_clients {
        let slug = match client_config.kind {
            ConfigTorrentClientKind::Qbittorrent => "qbittorrent-source",
            ConfigTorrentClientKind::Rtorrent => "rtorrent-source",
        };
        let fixture = manifest
            .fixtures
            .iter()
            .find(|fixture| fixture.slug == slug)
            .ok_or_else(|| format!("fixture manifest missing {slug}"))?;
        let torrent = fs::read(manifest_dir.join(&fixture.torrent_path)).map_err(|error| {
            format!(
                "read fixture torrent {}: {error}",
                fixture.torrent_path.display()
            )
        })?;
        match client_config.kind {
            ConfigTorrentClientKind::Qbittorrent => {
                let client = QbittorrentClient::new(
                    name,
                    &client_config.url,
                    client_config.username.clone(),
                    client_config
                        .password
                        .as_ref()
                        .map(|password| password.expose_secret().to_owned()),
                    std::time::Duration::from_secs(30),
                );
                if let Some(category) = client_config.default_category.as_deref() {
                    client
                        .create_category(category, Some(&client_config.default_save_path))
                        .await
                        .map_err(|error| error.to_string())?;
                }
                for tag in &client_config.default_tags {
                    client
                        .create_tag(tag)
                        .await
                        .map_err(|error| error.to_string())?;
                }
                client
                    .inject(QbitAddTorrent {
                        torrent_bytes: &torrent,
                        save_path: Some(&client_config.default_save_path),
                        category: client_config.default_category.as_deref(),
                        tags: &client_config.default_tags,
                        pause_for_recheck: false,
                        content_layout: QbitContentLayout::Original,
                    })
                    .await
                    .map_err(|error| error.to_string())?;
            }
            ConfigTorrentClientKind::Rtorrent => {
                let client = RtorrentClient::new(
                    name,
                    &client_config.url,
                    std::time::Duration::from_secs(30),
                );
                client
                    .inject(
                        &torrent,
                        Some(&client_config.default_save_path),
                        &client_config.default_label,
                        true,
                    )
                    .await
                    .map_err(|error| error.to_string())?;
            }
        }
        loaded = loaded.saturating_add(1);
    }

    Ok(loaded)
}

async fn system_test_client_state(
    config: crate::config::SporosConfig,
    manifest_path: PathBuf,
) -> Result<SystemTestClientState, String> {
    let manifest_bytes = fs::read(&manifest_path)
        .map_err(|error| format!("read fixture manifest {}: {error}", manifest_path.display()))?;
    let manifest: SystemFixtureManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|error| {
            format!(
                "parse fixture manifest {}: {error}",
                manifest_path.display()
            )
        })?;
    let qbit_hash = manifest_info_hash(&manifest, "qbittorrent-candidate")?;
    let rtorrent_hash = manifest_info_hash(&manifest, "rtorrent-candidate")?;
    let mut qbittorrent = None;
    let mut rtorrent = None;

    for (name, client_config) in &config.torrent_clients {
        match client_config.kind {
            ConfigTorrentClientKind::Qbittorrent => {
                let client = QbittorrentClient::new(
                    name,
                    &client_config.url,
                    client_config.username.clone(),
                    client_config
                        .password
                        .as_ref()
                        .map(|password| password.expose_secret().to_owned()),
                    std::time::Duration::from_secs(30),
                );
                let torrent = client
                    .torrent_info(&qbit_hash)
                    .await
                    .map_err(|error| error.to_string())?;
                if let Some(torrent) = torrent {
                    let files = client
                        .fetch_files(&qbit_hash)
                        .await
                        .map_err(|error| error.to_string())?;
                    qbittorrent = Some(SystemTestQbitTorrent {
                        hash: torrent.hash,
                        name: torrent.name,
                        save_path: torrent
                            .save_path
                            .map(|path| path.to_string_lossy().into_owned()),
                        category: torrent.category,
                        tags: torrent.tags,
                        amount_left: torrent.amount_left,
                        files: diagnostic_torrent_files(files),
                    });
                }
            }
            ConfigTorrentClientKind::Rtorrent => {
                let client = RtorrentClient::new(
                    name,
                    &client_config.url,
                    std::time::Duration::from_secs(30),
                );
                let download = client
                    .download_info(&rtorrent_hash)
                    .await
                    .map_err(|error| error.to_string())?;
                if let Some(download) = download {
                    let files = client
                        .fetch_files(&rtorrent_hash)
                        .await
                        .map_err(|error| error.to_string())?;
                    rtorrent = Some(SystemTestRtorrentDownload {
                        hash: download.info_hash.as_str().to_owned(),
                        name: download.name.as_str().to_owned(),
                        directory: download.directory.to_string_lossy().into_owned(),
                        label: download.label,
                        left_bytes: download.left_bytes.get(),
                        complete: download.complete,
                        files: diagnostic_torrent_files(files),
                    });
                }
            }
        }
    }

    Ok(SystemTestClientState {
        qbittorrent,
        rtorrent,
    })
}

fn diagnostic_torrent_files(
    files: Vec<crate::domain::TorrentFile>,
) -> Vec<SystemTestTorrentFileDiagnostic> {
    files
        .into_iter()
        .take(usize::from(SYSTEM_TEST_DIAGNOSTIC_LIMIT))
        .map(|file| SystemTestTorrentFileDiagnostic {
            relative_path: truncated(file.relative_path.to_string_lossy().into_owned()),
            file_name: truncated(file.file_name.as_str().to_owned()),
            size: file.size.get(),
            file_index: file.file_index.get(),
        })
        .collect()
}

async fn system_test_diagnostics(
    config: crate::config::SporosConfig,
) -> Result<SystemTestDiagnostics, String> {
    let snapshot = system_test_snapshot(config.clone()).await?;
    let repository = Repository::connect(&config.paths.database)
        .await
        .map_err(|error| error.to_string())?;
    let diagnostics = repository
        .system_test_diagnostics(SYSTEM_TEST_DIAGNOSTIC_LIMIT)
        .await
        .map_err(|error| error.to_string())?;
    let local_items = diagnostics
        .local_items
        .into_iter()
        .map(|row| SystemTestLocalItemDiagnostic {
            id: row.id,
            source_type: truncated(row.source_type),
            source_key: truncated(row.source_key),
            title: truncated(row.title),
            media_type: truncated(row.media_type),
            info_hash: row.info_hash,
            save_path: truncated_option(row.save_path),
            total_size: row.total_size,
        })
        .collect();
    let local_files = diagnostics
        .local_files
        .into_iter()
        .map(|row| SystemTestLocalFileDiagnostic {
            item_id: row.item_id,
            relative_path: truncated(row.relative_path),
            file_name: truncated(row.file_name),
            size: row.size,
            file_index: row.file_index,
        })
        .collect();
    let remote_candidates = diagnostics
        .remote_candidates
        .into_iter()
        .map(|row| SystemTestRemoteCandidateDiagnostic {
            id: row.id,
            guid: truncated(row.guid),
            title: truncated(row.title),
            tracker: truncated(row.tracker),
            size: row.size,
            info_hash: row.info_hash,
            torrent_cache_path: truncated_option(row.torrent_cache_path),
            last_seen_at: row.last_seen_at,
        })
        .collect();
    let match_decisions = diagnostics
        .match_decisions
        .into_iter()
        .map(|row| SystemTestMatchDecisionDiagnostic {
            local_item_id: row.local_item_id,
            candidate_id: row.candidate_id,
            decision: truncated(row.decision),
            matched_size: row.matched_size,
            matched_ratio: row.matched_ratio,
            reason_code: truncated(row.reason_code),
            assessed_at: row.assessed_at,
        })
        .collect();
    let indexers = diagnostics
        .indexers
        .into_iter()
        .map(|row| SystemTestIndexerDiagnostic {
            id: row.id,
            name: truncated(row.name),
            source_kind: truncated(row.source_kind),
            enabled: row.enabled,
            state: truncated(row.state),
            retry_after: row.retry_after,
            last_caps_refresh_at: row.last_caps_refresh_at,
        })
        .collect();
    let dependency_health = diagnostics
        .dependency_health
        .into_iter()
        .map(|row| SystemTestDependencyHealthDiagnostic {
            dependency_type: truncated(row.dependency_type),
            dependency_name: truncated(row.dependency_name),
            state: truncated(row.state),
            reason: truncated_option(row.reason),
            retry_after: row.retry_after,
            failure_count: row.failure_count,
            checked_at: row.checked_at,
        })
        .collect();
    let jobs = diagnostics
        .jobs
        .into_iter()
        .map(|row| SystemTestJobDiagnostic {
            name: truncated(row.name),
            state: truncated(row.state),
            last_started_at: row.last_started_at,
            last_finished_at: row.last_finished_at,
            next_run_at: row.next_run_at,
            last_error: truncated_option(row.last_error),
        })
        .collect();
    let announce_work = diagnostics
        .announce_work
        .into_iter()
        .map(|row| SystemTestAnnounceWorkDiagnostic {
            id: truncated(row.id),
            tracker: truncated(row.tracker),
            title: truncated(row.title),
            info_hash: row.info_hash,
            status: truncated(row.status),
            reason: truncated(row.reason),
            attempt_count: row.attempt_count,
            next_attempt_at: row.next_attempt_at,
            last_error_class: truncated_option(row.last_error_class),
            last_decision: truncated_option(row.last_decision),
            last_action_outcome: truncated_option(row.last_action_outcome),
        })
        .collect();

    Ok(SystemTestDiagnostics {
        snapshot,
        local_items,
        local_files,
        remote_candidates,
        match_decisions,
        indexers,
        dependency_health,
        jobs,
        announce_work,
    })
}

fn manifest_info_hash(manifest: &SystemFixtureManifest, slug: &str) -> Result<InfoHash, String> {
    manifest
        .fixtures
        .iter()
        .find(|fixture| fixture.slug == slug)
        .ok_or_else(|| format!("fixture manifest missing {slug}"))
        .and_then(|fixture| {
            InfoHash::new(&fixture.info_hash)
                .map_err(|error| format!("invalid fixture info hash {slug}: {error}"))
        })
}

fn truncated(value: String) -> String {
    if value.len() <= SYSTEM_TEST_TEXT_LIMIT {
        return value;
    }
    value.chars().take(SYSTEM_TEST_TEXT_LIMIT).collect()
}

fn truncated_option(value: Option<String>) -> Option<String> {
    value.map(truncated)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::indexers::TorznabRegistry;
    use crate::persistence::torrent_cache::CACHED_TORRENT_SUFFIX;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn check_config_loads_typed_toml() {
        let config_path = write_temp_config(
            r#"
            [server]
            bind = "127.0.0.1:2468"
            "#,
        );

        let output = run([
            OsString::from("sporos"),
            OsString::from("check-config"),
            OsString::from("--config"),
            config_path.clone().into_os_string(),
        ])
        .unwrap();

        assert!(output.contains("sporos config ok"));
        remove_temp_config(config_path);
    }

    #[test]
    fn check_config_rejects_unsupported_keys() {
        let root = unique_temp_root();
        fs::create_dir_all(&root).unwrap();
        let config_path = root.join("config.toml");
        fs::write(
            &config_path,
            format!(
                r#"
                [paths]
                database = "{}/state/sporos.db"
                torrent_cache_dir = "{}/cache/torrents"
                output_dir = "{}/output"
                base_dir = "/data"
                "#,
                root.display(),
                root.display(),
                root.display()
            ),
        )
        .unwrap();

        let error = run([
            OsString::from("sporos"),
            OsString::from("check-config"),
            OsString::from("--config"),
            config_path.clone().into_os_string(),
        ])
        .unwrap_err();

        assert!(error.contains("unknown field"));
        assert!(error.contains("base_dir"));
        remove_temp_config(config_path);
    }

    #[test]
    fn check_config_rejects_missing_integration_api_keys() {
        let prowlarr_config = write_temp_config(
            r#"
            [indexers.prowlarr.main]
            url = "https://prowlarr.example"
            "#,
        );
        let arr_config = write_temp_config(
            r#"
            [indexers.arr.sonarr.main]
            url = "http://sonarr:8989"
            "#,
        );

        let prowlarr_error = run([
            OsString::from("sporos"),
            OsString::from("check-config"),
            OsString::from("--config"),
            prowlarr_config.clone().into_os_string(),
        ])
        .unwrap_err();
        let arr_error = run([
            OsString::from("sporos"),
            OsString::from("check-config"),
            OsString::from("--config"),
            arr_config.clone().into_os_string(),
        ])
        .unwrap_err();

        assert!(prowlarr_error.contains("indexers.prowlarr.api_key"));
        assert!(arr_error.contains("indexers.arr.sonarr.api_key"));
        remove_temp_config(prowlarr_config);
        remove_temp_config(arr_config);
    }

    #[test]
    fn check_config_rejects_duplicate_torznab_urls() {
        let config_path = write_temp_config(
            r#"
            [indexers.torznab.one]
            url = "https://indexer.example/api?t=caps"

            [indexers.torznab.two]
            url = "https://indexer.example/api"
            "#,
        );

        let error = run([
            OsString::from("sporos"),
            OsString::from("check-config"),
            OsString::from("--config"),
            config_path.clone().into_os_string(),
        ])
        .unwrap_err();

        assert!(error.contains("duplicate Torznab URL"));
        remove_temp_config(config_path);
    }

    #[test]
    fn check_config_rejects_rtorrent_auth_fields() {
        let config_path = write_temp_config(
            r#"
            [torrent_clients.rtorrent]
            kind = "rtorrent"
            url = "http://rtorrent:5000/RPC2"
            username = "sporos"
            default_save_path = "/downloads"
            label_field = "custom1"
            "#,
        );

        let error = run([
            OsString::from("sporos"),
            OsString::from("check-config"),
            OsString::from("--config"),
            config_path.clone().into_os_string(),
        ])
        .unwrap_err();

        assert!(error.contains("does not support configured auth"));
        remove_temp_config(config_path);
    }

    #[test]
    fn check_config_rejects_runtime_intervals() {
        let config_path = write_temp_config(
            r#"
            [scheduling]
            client_inventory_interval = "0s"
            "#,
        );

        let error = run([
            OsString::from("sporos"),
            OsString::from("check-config"),
            OsString::from("--config"),
            config_path.clone().into_os_string(),
        ])
        .unwrap_err();

        assert!(error.contains("client inventory interval"));
        remove_temp_config(config_path);
    }

    #[test]
    fn print_config_schema_reports_supported_surface() {
        let output = run([
            OsString::from("sporos"),
            OsString::from("print-config-schema"),
        ])
        .unwrap();

        assert!(output.contains("[paths]"));
        assert!(output.contains("[scheduling]"));
    }

    #[test]
    fn hidden_system_test_seed_copies_candidate_torrents_and_upserts_rows() {
        let root = unique_temp_root();
        let state_dir = root.join("state");
        let cache_dir = root.join("cache/torrents");
        let output_dir = root.join("output");
        fs::create_dir_all(&state_dir).unwrap();
        fs::create_dir_all(&cache_dir).unwrap();
        fs::create_dir_all(&output_dir).unwrap();
        let config_path = root.join("config.toml");
        fs::write(
            &config_path,
            format!(
                r#"
                [paths]
                database = "{}/sporos.db"
                torrent_cache_dir = "{}"
                output_dir = "{}"

                [indexers.torznab.fixture]
                url = "http://torznab-fixture:8080/api"
                "#,
                state_dir.display(),
                cache_dir.display(),
                output_dir.display()
            ),
        )
        .unwrap();
        let loaded = load_config(&config_path).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let repository = Repository::connect(&loaded.paths.database).await.unwrap();
            let registry = TorznabRegistry::from_config(&loaded.indexers).unwrap();
            repository
                .sync_torznab_indexers(registry.indexers(), 100)
                .await
                .unwrap();
        });

        let manifest =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docker/system/fixtures/manifest.json");
        let output = run([
            OsString::from("sporos"),
            OsString::from("system-test-seed"),
            OsString::from("--config"),
            config_path.clone().into_os_string(),
            OsString::from("--manifest"),
            manifest.into_os_string(),
        ])
        .unwrap();

        assert_eq!(
            "seeded 2 system-test candidates for indexer fixture",
            output
        );
        let cached = fs::read_dir(&cache_dir).unwrap().count();
        assert_eq!(2, cached);
        runtime.block_on(async {
            let repository = Repository::connect(&loaded.paths.database).await.unwrap();
            let row: (i64, i64) = sqlx::query_as(
                r#"
                SELECT COUNT(*), COUNT(torrent_cache_path)
                FROM remote_candidates
                WHERE guid IN ('sporos-qbittorrent-candidate', 'sporos-rtorrent-candidate')
                  AND info_hash IS NOT NULL
                "#,
            )
            .fetch_one(repository.pool())
            .await
            .unwrap();
            assert_eq!((2, 2), row);
        });
        assert!(fs::read_dir(&cache_dir).unwrap().all(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(CACHED_TORRENT_SUFFIX)
        }));
        runtime.block_on(async {
            let repository = Repository::connect(&loaded.paths.database).await.unwrap();
            for index in 0..12 {
                let title = format!("{}-{index}", "x".repeat(256));
                let source_key = format!("client:{index}");
                sqlx::query(
                    r#"
                    INSERT INTO local_items (
                        source_type, source_key, title, display_name, media_type, info_hash,
                        path, save_path, total_size, mtime_ms, metadata_json, created_at, updated_at
                    )
                    VALUES ('client', ?, ?, ?, 'movie', NULL, NULL, ?, 100, NULL, '{}', 100, ?)
                    "#,
                )
                .bind(&source_key)
                .bind(&title)
                .bind(&title)
                .bind(format!("/downloads/{title}"))
                .bind(100 + i64::from(index))
                .execute(repository.pool())
                .await
                .unwrap();
                let item_id: i64 = sqlx::query_scalar(
                    "SELECT id FROM local_items WHERE source_type = 'client' AND source_key = ?",
                )
                .bind(&source_key)
                .fetch_one(repository.pool())
                .await
                .unwrap();
                sqlx::query(
                    r#"
                    INSERT INTO local_files (item_id, relative_path, file_name, size, mtime_ms, file_index)
                    VALUES (?, ?, ?, 100, NULL, 0)
                    "#,
                )
                .bind(item_id)
                .bind(format!("{}-{index}.mkv", "f".repeat(256)))
                .bind(format!("{}-{index}.mkv", "f".repeat(256)))
                .execute(repository.pool())
                .await
                .unwrap();
            }
        });
        let snapshot = run([
            OsString::from("sporos"),
            OsString::from("system-test-snapshot"),
            OsString::from("--config"),
            config_path.clone().into_os_string(),
        ])
        .unwrap();
        let snapshot: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
        assert_eq!(2, snapshot["remote_candidates"]);
        assert_eq!(2, snapshot["cached_candidates"]);
        assert_eq!(1, snapshot["enabled_indexers"]);

        let diagnostics = run([
            OsString::from("sporos"),
            OsString::from("system-test-diagnostics"),
            OsString::from("--config"),
            config_path.into_os_string(),
        ])
        .unwrap();
        let diagnostics: serde_json::Value = serde_json::from_str(&diagnostics).unwrap();
        assert_eq!(2, diagnostics["snapshot"]["remote_candidates"]);
        assert_eq!(
            2,
            diagnostics["remote_candidates"].as_array().unwrap().len()
        );
        assert_eq!(1, diagnostics["indexers"].as_array().unwrap().len());
        assert_eq!(8, diagnostics["local_items"].as_array().unwrap().len());
        assert_eq!(8, diagnostics["local_files"].as_array().unwrap().len());
        assert!(
            diagnostics["local_items"][0]["title"]
                .as_str()
                .unwrap()
                .len()
                <= SYSTEM_TEST_TEXT_LIMIT
        );
        fs::remove_dir_all(root).unwrap();
    }

    fn write_temp_config(contents: &str) -> PathBuf {
        let root = unique_temp_root();
        fs::create_dir_all(&root).unwrap();
        let path = root.join("config.toml");
        let contents = format!(
            r#"
            [paths]
            database = "{}/state/sporos.db"
            torrent_cache_dir = "{}/cache/torrents"
            output_dir = "{}/output"

            {contents}
            "#,
            root.display(),
            root.display(),
            root.display()
        );
        fs::write(&path, contents).unwrap();
        path
    }

    fn unique_temp_root() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "sporos-cli-test-{}-{nanos}-{counter}",
            std::process::id()
        ))
    }

    fn remove_temp_config(path: PathBuf) {
        let Some(root) = path.parent() else {
            return;
        };
        fs::remove_dir_all(root).unwrap();
    }
}
