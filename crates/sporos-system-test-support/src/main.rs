use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};

use sporos::clients::qbittorrent::{QbitAddTorrent, QbitContentLayout, QbittorrentClient};
use sporos::clients::rtorrent::RtorrentClient;
use sporos::config::{ConfigTorrentClientKind, DEFAULT_CONFIG_PATH, load_config};
use sporos::domain::{
    ByteSize, CandidateGuid, DownloadUrl, IndexerId, InfoHash, ItemTitle, RemoteCandidate,
    TrackerName,
};
use sporos::indexers::TorznabRegistry;
use sporos::persistence::repository::Repository;
use sporos::persistence::torrent_cache::cached_torrent_path;
use sporos::runtime::announce_worker::unix_time_ms;
use sporos::secrets::sanitize_url_for_logging;

fn main() -> std::process::ExitCode {
    if let Err(error) = sporos::logging::init_from_env() {
        eprintln!("sporos-system-test-support: failed to initialize logging: {error}");
        return std::process::ExitCode::FAILURE;
    }

    match run(std::env::args_os()) {
        Ok(output) => {
            if !output.is_empty() {
                println!("{output}");
            }
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("sporos-system-test-support: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

const SYSTEM_TEST_DIAGNOSTIC_LIMIT: u16 = 8;
const SYSTEM_TEST_TEXT_LIMIT: usize = 160;

#[derive(Debug, Parser)]
#[command(
    name = "sporos-system-test-support",
    about = "Sporos system-test support helper"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(name = "system-test-seed")]
    Seed {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long, default_value = "fixture")]
        indexer: String,
        #[arg(long, default_value = "http://torznab-fixture:8080/torrents")]
        fixture_base_url: String,
    },
    #[command(name = "system-test-snapshot")]
    Snapshot {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },
    #[command(name = "system-test-load-sources")]
    LoadSources {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
    },
    #[command(name = "system-test-client-state")]
    ClientState {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
    },
    #[command(name = "system-test-diagnostics")]
    Diagnostics {
        #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
        config: PathBuf,
    },
}

pub fn run(args: impl IntoIterator<Item = OsString>) -> Result<String, String> {
    let cli = Cli::try_parse_from(args).map_err(|error| error.to_string())?;

    match cli.command {
        Command::Seed {
            config,
            manifest,
            indexer,
            fixture_base_url,
        } => {
            let loaded = load_config(&config).map_err(|error| error.to_string())?;
            let runtime = build_current_thread_runtime()?;
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
        Command::Snapshot { config } => {
            let loaded = load_config(&config).map_err(|error| error.to_string())?;
            let runtime = build_current_thread_runtime()?;
            let snapshot = runtime.block_on(system_test_snapshot(loaded))?;
            serde_json::to_string(&snapshot).map_err(|error| error.to_string())
        }
        Command::LoadSources { config, manifest } => {
            let loaded = load_config(&config).map_err(|error| error.to_string())?;
            let runtime = build_current_thread_runtime()?;
            let loaded = runtime.block_on(load_system_test_sources(loaded, manifest))?;
            Ok(format!("loaded {loaded} system-test source torrents"))
        }
        Command::ClientState { config, manifest } => {
            let loaded = load_config(&config).map_err(|error| error.to_string())?;
            let runtime = build_current_thread_runtime()?;
            let state = runtime.block_on(system_test_client_state(loaded, manifest))?;
            serde_json::to_string(&state).map_err(|error| error.to_string())
        }
        Command::Diagnostics { config } => {
            let loaded = load_config(&config).map_err(|error| error.to_string())?;
            let runtime = build_current_thread_runtime()?;
            let diagnostics = runtime.block_on(system_test_diagnostics(loaded))?;
            serde_json::to_string(&diagnostics).map_err(|error| error.to_string())
        }
    }
}

fn build_current_thread_runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())
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

#[derive(Debug, Clone, Eq, PartialEq)]
struct SystemTestClientItemRow {
    title: String,
    source_key: String,
    info_hash: Option<String>,
    save_path: Option<String>,
    file_count: i64,
}

#[derive(Debug, Clone, PartialEq)]
struct SystemTestDiagnosticRows {
    local_items: Vec<SystemTestLocalItemRow>,
    local_files: Vec<SystemTestLocalFileRow>,
    remote_candidates: Vec<SystemTestRemoteCandidateRow>,
    match_decisions: Vec<SystemTestMatchDecisionRow>,
    indexers: Vec<SystemTestIndexerRow>,
    dependency_health: Vec<SystemTestDependencyHealthRow>,
    jobs: Vec<SystemTestJobRow>,
    announce_work: Vec<SystemTestAnnounceWorkRow>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct SystemTestLocalItemRow {
    id: i64,
    source_type: String,
    source_key: String,
    title: String,
    media_type: String,
    info_hash: Option<String>,
    save_path: Option<String>,
    total_size: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct SystemTestLocalFileRow {
    item_id: i64,
    relative_path: String,
    file_name: String,
    size: i64,
    file_index: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct SystemTestRemoteCandidateRow {
    id: i64,
    guid: String,
    title: String,
    tracker: String,
    size: Option<i64>,
    info_hash: Option<String>,
    torrent_cache_path: Option<String>,
    last_seen_at: i64,
}

#[derive(Debug, Clone, PartialEq)]
struct SystemTestMatchDecisionRow {
    local_item_id: i64,
    candidate_id: i64,
    decision: String,
    matched_size: Option<i64>,
    matched_ratio: Option<f64>,
    reason_code: String,
    assessed_at: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct SystemTestIndexerRow {
    id: i64,
    name: String,
    source_kind: String,
    enabled: bool,
    state: String,
    retry_after: Option<i64>,
    last_caps_refresh_at: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct SystemTestDependencyHealthRow {
    dependency_type: String,
    dependency_name: String,
    state: String,
    reason: Option<String>,
    retry_after: Option<i64>,
    failure_count: i64,
    checked_at: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct SystemTestJobRow {
    name: String,
    state: String,
    last_started_at: Option<i64>,
    last_finished_at: Option<i64>,
    next_run_at: Option<i64>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct SystemTestAnnounceWorkRow {
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

async fn seed_system_test_candidates(
    config: sporos::config::SporosConfig,
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
    config: sporos::config::SporosConfig,
) -> Result<SystemTestSnapshot, String> {
    let pool = SqlitePool::connect(&config.paths.database.to_string_lossy())
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
                .ends_with(sporos::persistence::torrent_cache::SAVED_TORRENT_SUFFIX)
        })
        .count();
    let saved_torrents =
        i64::try_from(saved_torrents).map_err(|error| format!("count saved torrents: {error}"))?;

    Ok(SystemTestSnapshot {
        local_items: count_rows(&pool, "SELECT COUNT(*) FROM local_items").await?,
        local_files: count_rows(&pool, "SELECT COUNT(*) FROM local_files").await?,
        remote_candidates: count_rows(&pool, "SELECT COUNT(*) FROM remote_candidates").await?,
        cached_candidates: count_rows(
            &pool,
            "SELECT COUNT(*) FROM remote_candidates WHERE info_hash IS NOT NULL AND torrent_cache_path IS NOT NULL",
        )
        .await?,
        match_decisions: count_rows(&pool, "SELECT COUNT(*) FROM match_decisions").await?,
        enabled_indexers: count_rows(&pool, "SELECT COUNT(*) FROM indexers WHERE enabled = 1")
            .await?,
        saved_torrents,
        client_items: system_test_client_items(&pool, SYSTEM_TEST_DIAGNOSTIC_LIMIT)
            .await?
            .into_iter()
            .map(|row| SystemTestClientItem {
                title: diagnostic_text(row.title),
                source_key: diagnostic_text(row.source_key),
                info_hash: row.info_hash,
                save_path: diagnostic_text_option(row.save_path),
                file_count: row.file_count,
            })
            .collect(),
    })
}

async fn load_system_test_sources(
    config: sporos::config::SporosConfig,
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
    config: sporos::config::SporosConfig,
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
    files: Vec<sporos::domain::TorrentFile>,
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
    config: sporos::config::SporosConfig,
) -> Result<SystemTestDiagnostics, String> {
    let snapshot = system_test_snapshot(config.clone()).await?;
    let pool = SqlitePool::connect(&config.paths.database.to_string_lossy())
        .await
        .map_err(|error| error.to_string())?;
    let diagnostics = system_test_diagnostic_rows(&pool, SYSTEM_TEST_DIAGNOSTIC_LIMIT).await?;
    let local_items = diagnostics
        .local_items
        .into_iter()
        .map(|row| SystemTestLocalItemDiagnostic {
            id: row.id,
            source_type: diagnostic_text(row.source_type),
            source_key: diagnostic_text(row.source_key),
            title: diagnostic_text(row.title),
            media_type: diagnostic_text(row.media_type),
            info_hash: row.info_hash,
            save_path: diagnostic_text_option(row.save_path),
            total_size: row.total_size,
        })
        .collect();
    let local_files = diagnostics
        .local_files
        .into_iter()
        .map(|row| SystemTestLocalFileDiagnostic {
            item_id: row.item_id,
            relative_path: diagnostic_text(row.relative_path),
            file_name: diagnostic_text(row.file_name),
            size: row.size,
            file_index: row.file_index,
        })
        .collect();
    let remote_candidates = diagnostics
        .remote_candidates
        .into_iter()
        .map(|row| SystemTestRemoteCandidateDiagnostic {
            id: row.id,
            guid: diagnostic_text(row.guid),
            title: diagnostic_text(row.title),
            tracker: diagnostic_text(row.tracker),
            size: row.size,
            info_hash: row.info_hash,
            torrent_cache_path: diagnostic_text_option(row.torrent_cache_path),
            last_seen_at: row.last_seen_at,
        })
        .collect();
    let match_decisions = diagnostics
        .match_decisions
        .into_iter()
        .map(|row| SystemTestMatchDecisionDiagnostic {
            local_item_id: row.local_item_id,
            candidate_id: row.candidate_id,
            decision: diagnostic_text(row.decision),
            matched_size: row.matched_size,
            matched_ratio: row.matched_ratio,
            reason_code: diagnostic_text(row.reason_code),
            assessed_at: row.assessed_at,
        })
        .collect();
    let indexers = diagnostics
        .indexers
        .into_iter()
        .map(|row| SystemTestIndexerDiagnostic {
            id: row.id,
            name: diagnostic_text(row.name),
            source_kind: diagnostic_text(row.source_kind),
            enabled: row.enabled,
            state: diagnostic_text(row.state),
            retry_after: row.retry_after,
            last_caps_refresh_at: row.last_caps_refresh_at,
        })
        .collect();
    let dependency_health = diagnostics
        .dependency_health
        .into_iter()
        .map(|row| SystemTestDependencyHealthDiagnostic {
            dependency_type: diagnostic_text(row.dependency_type),
            dependency_name: diagnostic_text(row.dependency_name),
            state: diagnostic_text(row.state),
            reason: diagnostic_text_option(row.reason),
            retry_after: row.retry_after,
            failure_count: row.failure_count,
            checked_at: row.checked_at,
        })
        .collect();
    let jobs = diagnostics
        .jobs
        .into_iter()
        .map(|row| SystemTestJobDiagnostic {
            name: diagnostic_text(row.name),
            state: diagnostic_text(row.state),
            last_started_at: row.last_started_at,
            last_finished_at: row.last_finished_at,
            next_run_at: row.next_run_at,
            last_error: diagnostic_text_option(row.last_error),
        })
        .collect();
    let announce_work = diagnostics
        .announce_work
        .into_iter()
        .map(|row| SystemTestAnnounceWorkDiagnostic {
            id: diagnostic_text(row.id),
            tracker: diagnostic_text(row.tracker),
            title: diagnostic_text(row.title),
            info_hash: row.info_hash,
            status: diagnostic_text(row.status),
            reason: diagnostic_text(row.reason),
            attempt_count: row.attempt_count,
            next_attempt_at: row.next_attempt_at,
            last_error_class: diagnostic_text_option(row.last_error_class),
            last_decision: diagnostic_text_option(row.last_decision),
            last_action_outcome: diagnostic_text_option(row.last_action_outcome),
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

async fn system_test_diagnostic_rows(
    pool: &SqlitePool,
    limit: u16,
) -> Result<SystemTestDiagnosticRows, String> {
    let limit = i64::from(limit);
    let local_items = sqlx::query(
        r#"
        SELECT id, source_type, source_key, title, media_type, info_hash, save_path, total_size
        FROM local_items
        ORDER BY updated_at DESC, id DESC
        LIMIT ?
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| format!("read system test local items: {error}"))?
    .into_iter()
    .map(|row| SystemTestLocalItemRow {
        id: row.get("id"),
        source_type: row.get("source_type"),
        source_key: row.get("source_key"),
        title: row.get("title"),
        media_type: row.get("media_type"),
        info_hash: row.get("info_hash"),
        save_path: row.get("save_path"),
        total_size: row.get("total_size"),
    })
    .collect();

    let local_files = sqlx::query(
        r#"
        SELECT local_files.item_id, local_files.relative_path, local_files.file_name,
               local_files.size, local_files.file_index
        FROM local_files
        JOIN local_items ON local_items.id = local_files.item_id
        ORDER BY local_items.updated_at DESC, local_files.item_id DESC, local_files.file_index
        LIMIT ?
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| format!("read system test local files: {error}"))?
    .into_iter()
    .map(|row| SystemTestLocalFileRow {
        item_id: row.get("item_id"),
        relative_path: row.get("relative_path"),
        file_name: row.get("file_name"),
        size: row.get("size"),
        file_index: row.get("file_index"),
    })
    .collect();

    let remote_candidates = sqlx::query(
        r#"
        SELECT id, guid, title, tracker, size, info_hash, torrent_cache_path, last_seen_at
        FROM remote_candidates
        ORDER BY last_seen_at DESC, id DESC
        LIMIT ?
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| format!("read system test remote candidates: {error}"))?
    .into_iter()
    .map(|row| SystemTestRemoteCandidateRow {
        id: row.get("id"),
        guid: row.get("guid"),
        title: row.get("title"),
        tracker: row.get("tracker"),
        size: row.get("size"),
        info_hash: row.get("info_hash"),
        torrent_cache_path: row.get("torrent_cache_path"),
        last_seen_at: row.get("last_seen_at"),
    })
    .collect();

    let match_decisions = sqlx::query(
        r#"
        SELECT local_item_id, candidate_id, decision, matched_size, matched_ratio, reason_code, assessed_at
        FROM match_decisions
        ORDER BY assessed_at DESC, candidate_id DESC
        LIMIT ?
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| format!("read system test match decisions: {error}"))?
    .into_iter()
    .map(|row| SystemTestMatchDecisionRow {
        local_item_id: row.get("local_item_id"),
        candidate_id: row.get("candidate_id"),
        decision: row.get("decision"),
        matched_size: row.get("matched_size"),
        matched_ratio: row.get("matched_ratio"),
        reason_code: row.get("reason_code"),
        assessed_at: row.get("assessed_at"),
    })
    .collect();

    let indexers = sqlx::query(
        r#"
        SELECT id, name, source_kind, enabled, state, retry_after, last_caps_refresh_at
        FROM indexers
        ORDER BY name
        LIMIT ?
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| format!("read system test indexers: {error}"))?
    .into_iter()
    .map(|row| SystemTestIndexerRow {
        id: row.get("id"),
        name: row.get("name"),
        source_kind: row.get("source_kind"),
        enabled: row.get::<i64, _>("enabled") != 0,
        state: row.get("state"),
        retry_after: row.get("retry_after"),
        last_caps_refresh_at: row.get("last_caps_refresh_at"),
    })
    .collect();

    let dependency_health = sqlx::query(
        r#"
        SELECT dependency_type, dependency_name, state, reason, retry_after, failure_count, checked_at
        FROM dependency_health
        ORDER BY checked_at DESC, dependency_type, dependency_name
        LIMIT ?
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| format!("read system test dependency health: {error}"))?
    .into_iter()
    .map(|row| SystemTestDependencyHealthRow {
        dependency_type: row.get("dependency_type"),
        dependency_name: row.get("dependency_name"),
        state: row.get("state"),
        reason: row.get("reason"),
        retry_after: row.get("retry_after"),
        failure_count: row.get("failure_count"),
        checked_at: row.get("checked_at"),
    })
    .collect();

    let jobs = sqlx::query(
        r#"
        SELECT name, state, last_started_at, last_finished_at, next_run_at, last_error
        FROM jobs
        ORDER BY name
        LIMIT ?
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| format!("read system test jobs: {error}"))?
    .into_iter()
    .map(|row| SystemTestJobRow {
        name: row.get("name"),
        state: row.get("state"),
        last_started_at: row.get("last_started_at"),
        last_finished_at: row.get("last_finished_at"),
        next_run_at: row.get("next_run_at"),
        last_error: row.get("last_error"),
    })
    .collect();

    let announce_work = sqlx::query(
        r#"
        SELECT id, tracker, title, info_hash, status, reason, attempt_count, next_attempt_at,
               last_error_class, last_decision, last_action_outcome
        FROM announce_work
        ORDER BY updated_at DESC, received_at DESC
        LIMIT ?
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|error| format!("read system test announce work: {error}"))?
    .into_iter()
    .map(|row| SystemTestAnnounceWorkRow {
        id: row.get("id"),
        tracker: row.get("tracker"),
        title: row.get("title"),
        info_hash: row.get("info_hash"),
        status: row.get("status"),
        reason: row.get("reason"),
        attempt_count: row.get("attempt_count"),
        next_attempt_at: row.get("next_attempt_at"),
        last_error_class: row.get("last_error_class"),
        last_decision: row.get("last_decision"),
        last_action_outcome: row.get("last_action_outcome"),
    })
    .collect();

    Ok(SystemTestDiagnosticRows {
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

async fn system_test_client_items(
    pool: &SqlitePool,
    limit: u16,
) -> Result<Vec<SystemTestClientItemRow>, String> {
    let rows = sqlx::query(
        r#"
        SELECT
            local_items.title,
            local_items.source_key,
            local_items.info_hash,
            local_items.save_path,
            COUNT(local_files.file_index) AS file_count
        FROM local_items
        LEFT JOIN local_files ON local_files.item_id = local_items.id
        WHERE local_items.source_type = 'client'
        GROUP BY local_items.id
        ORDER BY local_items.source_key
        LIMIT ?
        "#,
    )
    .bind(i64::from(limit))
    .fetch_all(pool)
    .await
    .map_err(|error| format!("read system test client items: {error}"))?;

    Ok(rows
        .into_iter()
        .map(|row| SystemTestClientItemRow {
            title: row.get("title"),
            source_key: row.get("source_key"),
            info_hash: row.get("info_hash"),
            save_path: row.get("save_path"),
            file_count: row.get("file_count"),
        })
        .collect())
}

async fn count_rows(pool: &SqlitePool, query: &'static str) -> Result<i64, String> {
    sqlx::query_scalar(query)
        .fetch_one(pool)
        .await
        .map_err(|error| format!("count system test rows: {error}"))
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

fn diagnostic_text(value: String) -> String {
    truncated(sanitize_url_for_logging(&value).to_string())
}

fn diagnostic_text_option(value: Option<String>) -> Option<String> {
    value.map(diagnostic_text)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use sporos::indexers::TorznabRegistry;
    use sporos::persistence::torrent_cache::CACHED_TORRENT_SUFFIX;
    use sqlx::SqlitePool;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

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

        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("docker/system/fixtures/manifest.json");
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
            let pool = SqlitePool::connect(&loaded.paths.database.to_string_lossy())
                .await
                .unwrap();
            let row: (i64, i64) = sqlx::query_as(
                r#"
                SELECT COUNT(*), COUNT(torrent_cache_path)
                FROM remote_candidates
                WHERE guid IN ('sporos-qbittorrent-candidate', 'sporos-rtorrent-candidate')
                  AND info_hash IS NOT NULL
                "#,
            )
            .fetch_one(&pool)
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
            let pool = SqlitePool::connect(&loaded.paths.database.to_string_lossy())
                .await
                .unwrap();
            sqlx::query(
                r#"
                INSERT INTO announce_work (
                    id, dedupe_hash, received_at, updated_at, first_attempt_at, finished_at,
                    tracker, guid, info_hash, title, size, download_url, redacted_download_url,
                    cookie, status, reason, attempt_count, next_attempt_at, expires_at,
                    lease_owner, lease_until, last_dependency_kind, last_dependency_name,
                    last_error_class, last_error_message
                )
                VALUES (
                    'ann_system_test_secret', 'dedupe-system-test-secret', 100, 100, NULL, NULL,
                    'tracker.example', 'guid-system-test-secret', NULL, 'Secret fixture', NULL,
                    'https://tracker.example/download?id=1&passkey=diagnostic-secret',
                    'https://tracker.example/download?id=1&passkey=[REDACTED]',
                    'sid=diagnostic-cookie', 'queued', 'accepted', 0, 100, 10000,
                    NULL, NULL, NULL, NULL,
                    'https://tracker.example/error?token=diagnostic-error-secret',
                    NULL
                )
                "#,
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                r#"
                UPDATE remote_candidates
                SET guid = 'https://tracker.example/guid?passkey=diagnostic-guid-secret'
                WHERE guid = 'sporos-qbittorrent-candidate'
                "#,
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                r#"
                INSERT INTO dependency_health (
                    dependency_type, dependency_name, state, reason, retry_after,
                    failure_count, checked_at
                )
                VALUES (
                    'indexer',
                    'https://tracker.example/health?passkey=diagnostic-dependency-secret',
                    'degraded',
                    'https://tracker.example/reason?token=diagnostic-reason-secret',
                    200, 1, 100
                )
                "#,
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                r#"
                INSERT INTO jobs (name, state, last_started_at, last_finished_at, next_run_at, last_error)
                VALUES (
                    'system-test-secret-job', 'failed', 100, 200, 300,
                    'https://tracker.example/job?apikey=diagnostic-job-secret'
                )
                "#,
            )
            .execute(&pool)
            .await
            .unwrap();
            for index in 0..12 {
                let title = format!("{}-{index}", "x".repeat(256));
                let source_key = if index == 11 {
                    "https://tracker.example/client?passkey=diagnostic-client-source-secret"
                        .to_owned()
                } else {
                    format!("client:{index}")
                };
                let save_path = if index == 11 {
                    "https://tracker.example/save?token=diagnostic-client-save-secret".to_owned()
                } else {
                    format!("/downloads/{title}")
                };
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
                .bind(save_path)
                .bind(100 + i64::from(index))
                .execute(&pool)
                .await
                .unwrap();
                let item_id: i64 = sqlx::query_scalar(
                    "SELECT id FROM local_items WHERE source_type = 'client' AND source_key = ?",
                )
                .bind(&source_key)
                .fetch_one(&pool)
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
                .execute(&pool)
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
        assert!(!diagnostics.contains("diagnostic-secret"));
        assert!(!diagnostics.contains("diagnostic-cookie"));
        assert!(!diagnostics.contains("diagnostic-guid-secret"));
        assert!(!diagnostics.contains("diagnostic-error-secret"));
        assert!(!diagnostics.contains("diagnostic-dependency-secret"));
        assert!(!diagnostics.contains("diagnostic-reason-secret"));
        assert!(!diagnostics.contains("diagnostic-job-secret"));
        assert!(!diagnostics.contains("diagnostic-client-source-secret"));
        assert!(!diagnostics.contains("diagnostic-client-save-secret"));
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
}
