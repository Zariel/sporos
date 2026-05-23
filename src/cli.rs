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
}

#[derive(Debug, Serialize)]
struct SystemTestRtorrentDownload {
    hash: String,
    name: String,
    directory: String,
    label: Option<String>,
    left_bytes: u64,
    complete: bool,
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
    let local_items = count_rows(repository.pool(), "SELECT COUNT(*) FROM local_items").await?;
    let local_files = count_rows(repository.pool(), "SELECT COUNT(*) FROM local_files").await?;
    let remote_candidates =
        count_rows(repository.pool(), "SELECT COUNT(*) FROM remote_candidates").await?;
    let cached_candidates = count_rows(
        repository.pool(),
        "SELECT COUNT(*) FROM remote_candidates WHERE info_hash IS NOT NULL AND torrent_cache_path IS NOT NULL",
    )
    .await?;
    let match_decisions =
        count_rows(repository.pool(), "SELECT COUNT(*) FROM match_decisions").await?;
    let enabled_indexers = count_rows(
        repository.pool(),
        "SELECT COUNT(*) FROM indexers WHERE enabled = 1",
    )
    .await?;
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
    let client_items = system_test_client_items(repository.pool()).await?;

    Ok(SystemTestSnapshot {
        local_items,
        local_files,
        remote_candidates,
        cached_candidates,
        match_decisions,
        enabled_indexers,
        saved_torrents,
        client_items,
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
                qbittorrent = client
                    .torrent_info(&qbit_hash)
                    .await
                    .map_err(|error| error.to_string())?
                    .map(|torrent| SystemTestQbitTorrent {
                        hash: torrent.hash,
                        name: torrent.name,
                        save_path: torrent
                            .save_path
                            .map(|path| path.to_string_lossy().into_owned()),
                        category: torrent.category,
                        tags: torrent.tags,
                        amount_left: torrent.amount_left,
                    });
            }
            ConfigTorrentClientKind::Rtorrent => {
                let client = RtorrentClient::new(
                    name,
                    &client_config.url,
                    std::time::Duration::from_secs(30),
                );
                rtorrent = client
                    .download_info(&rtorrent_hash)
                    .await
                    .map_err(|error| error.to_string())?
                    .map(|download| SystemTestRtorrentDownload {
                        hash: download.info_hash.as_str().to_owned(),
                        name: download.name.as_str().to_owned(),
                        directory: download.directory.to_string_lossy().into_owned(),
                        label: download.label,
                        left_bytes: download.left_bytes.get(),
                        complete: download.complete,
                    });
            }
        }
    }

    Ok(SystemTestClientState {
        qbittorrent,
        rtorrent,
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

async fn system_test_client_items(
    pool: &sqlx::SqlitePool,
) -> Result<Vec<SystemTestClientItem>, String> {
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
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;

    Ok(rows
        .into_iter()
        .map(|row| SystemTestClientItem {
            title: sqlx::Row::get(&row, "title"),
            source_key: sqlx::Row::get(&row, "source_key"),
            info_hash: sqlx::Row::get(&row, "info_hash"),
            save_path: sqlx::Row::get(&row, "save_path"),
            file_count: sqlx::Row::get(&row, "file_count"),
        })
        .collect())
}

async fn count_rows(pool: &sqlx::SqlitePool, query: &str) -> Result<i64, String> {
    sqlx::query_scalar(query)
        .fetch_one(pool)
        .await
        .map_err(|error| error.to_string())
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
        let snapshot = run([
            OsString::from("sporos"),
            OsString::from("system-test-snapshot"),
            OsString::from("--config"),
            config_path.into_os_string(),
        ])
        .unwrap();
        let snapshot: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
        assert_eq!(2, snapshot["remote_candidates"]);
        assert_eq!(2, snapshot["cached_candidates"]);
        assert_eq!(1, snapshot["enabled_indexers"]);
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
