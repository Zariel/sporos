//! Save, inject, link, restore, and cleanup actions.

use std::{borrow::Cow, fs, path::Path};

use crate::{
    SporosError,
    domain::{Metafile, SaveResult},
    persistence::Database,
    torrent::{SavedTorrentMetadata, parse_metafile, torrent_cache_dir, torrent_save_path},
};
use filetime::FileTime;

/// Result from saving one torrent to `outputDir`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SavedTorrent {
    /// Compatibility action result.
    pub result: SaveResult,
    /// Output path.
    pub path: std::path::PathBuf,
    /// Whether the file already existed and was touched.
    pub existed: bool,
}

/// Notification payload emitted by save and restore hooks.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SaveNotification {
    /// Saved torrent output path.
    pub path: std::path::PathBuf,
    /// Saved torrent metadata.
    pub metadata: SavedTorrentMetadata<'static>,
    /// Whether this came from cache restore.
    pub restored: bool,
    /// Whether the file already existed.
    pub existed: bool,
}

/// Summary from restoring cached torrents to `outputDir`.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct RestoreSummary {
    /// Cached torrent files scanned.
    pub scanned: usize,
    /// Cached torrents successfully restored or touched.
    pub restored: usize,
    /// Cached files that could not be read, parsed, or copied.
    pub failed: usize,
}

/// Save a matched candidate torrent to `outputDir`.
pub fn save_candidate_torrent<N>(
    output_dir: &Path,
    tracker: &str,
    metafile: &Metafile<'_>,
    bytes: &[u8],
    mut notify: N,
) -> crate::Result<SavedTorrent>
where
    N: FnMut(&SaveNotification) -> crate::Result<()>,
{
    let metadata = SavedTorrentMetadata::new(
        metafile.media_type,
        tracker.to_owned(),
        metafile.name.as_ref().to_owned(),
        metafile.info_hash.clone().into_owned(),
        false,
    );
    save_torrent_with_metadata(output_dir, &metadata, bytes, false, &mut notify)
}

/// Save torrent bytes using explicit output filename metadata.
pub fn save_torrent_with_metadata<N>(
    output_dir: &Path,
    metadata: &SavedTorrentMetadata<'_>,
    bytes: &[u8],
    restored: bool,
    mut notify: N,
) -> crate::Result<SavedTorrent>
where
    N: FnMut(&SaveNotification) -> crate::Result<()>,
{
    fs::create_dir_all(output_dir)
        .map_err(|error| action_error(format!("failed to create outputDir: {error}")))?;
    let path = torrent_save_path(output_dir, metadata);
    let existed = path.exists();
    if existed {
        touch_existing_file(&path)?;
    } else {
        fs::write(&path, bytes)
            .map_err(|error| action_error(format!("failed to save torrent: {error}")))?;
    }
    notify(&SaveNotification {
        path: path.clone(),
        metadata: metadata.clone().into_owned(),
        restored,
        existed,
    })?;
    Ok(SavedTorrent {
        result: SaveResult::Saved,
        path,
        existed,
    })
}

/// Restore cached candidate torrents into `outputDir` without deleting cache files.
pub fn restore_from_torrent_cache<N>(
    database: &Database,
    app_dir: &Path,
    output_dir: &Path,
    mut notify: N,
) -> crate::Result<RestoreSummary>
where
    N: FnMut(&SaveNotification) -> crate::Result<()>,
{
    let cache_dir = torrent_cache_dir(app_dir);
    let entries = match fs::read_dir(&cache_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RestoreSummary::default());
        }
        Err(error) => {
            return Err(action_error(format!(
                "failed to read torrent cache {}: {error}",
                cache_dir.display()
            )));
        }
    };

    let tracker_names = indexer_tracker_names(database)?;
    let mut summary = RestoreSummary::default();
    for entry in entries {
        summary.scanned += 1;
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::debug!("skipping torrent cache entry: {error}");
                summary.failed += 1;
                continue;
            }
        };
        let path = entry.path();
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("torrent") {
            continue;
        }
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::debug!("failed to read cached torrent {}: {error}", path.display());
                summary.failed += 1;
                continue;
            }
        };
        let metafile = match parse_metafile(&bytes) {
            Ok(metafile) => metafile,
            Err(error) => {
                tracing::debug!("failed to parse cached torrent {}: {error}", path.display());
                summary.failed += 1;
                continue;
            }
        };
        let tracker = tracker_name_for_metafile(&tracker_names, &metafile)
            .unwrap_or_else(|| "UnknownTracker".to_owned());
        let metadata = SavedTorrentMetadata::new(
            metafile.media_type,
            tracker,
            metafile.name.as_ref().to_owned(),
            metafile.info_hash.into_owned(),
            true,
        );
        save_torrent_with_metadata(output_dir, &metadata, &bytes, true, &mut notify)?;
        summary.restored += 1;
    }
    Ok(summary)
}

impl<'a> SavedTorrentMetadata<'a> {
    fn into_owned(self) -> SavedTorrentMetadata<'static> {
        SavedTorrentMetadata::new(
            self.media_type,
            self.tracker.into_owned(),
            self.name.into_owned(),
            self.info_hash.into_owned(),
            self.cached,
        )
    }
}

fn touch_existing_file(path: &Path) -> crate::Result<()> {
    let metadata = fs::metadata(path)
        .map_err(|error| action_error(format!("failed to stat saved torrent: {error}")))?;
    let modified = FileTime::from_last_modification_time(&metadata);
    filetime::set_file_times(path, FileTime::now(), modified)
        .map_err(|error| action_error(format!("failed to touch saved torrent: {error}")))?;
    Ok(())
}

fn indexer_tracker_names(database: &Database) -> crate::Result<Vec<(String, Vec<String>)>> {
    let mut statement = database
        .connection()
        .prepare("SELECT COALESCE(name, 'UnknownTracker'), trackers FROM indexer WHERE trackers IS NOT NULL")
        .map_err(persistence_error)?;
    let rows = statement
        .query_map([], |row| {
            let name: String = row.get(0)?;
            let trackers: String = row.get(1)?;
            Ok((name, trackers))
        })
        .map_err(persistence_error)?;
    let mut output = Vec::new();
    for row in rows {
        let (name, trackers) = row.map_err(persistence_error)?;
        let trackers = serde_json::from_str::<Vec<String>>(&trackers).map_err(json_error)?;
        output.push((name, trackers));
    }
    Ok(output)
}

fn tracker_name_for_metafile(
    tracker_names: &[(String, Vec<String>)],
    metafile: &Metafile<'_>,
) -> Option<String> {
    for tracker in &metafile.trackers {
        for (name, known_trackers) in tracker_names {
            if known_trackers
                .iter()
                .any(|known| known.eq_ignore_ascii_case(tracker.as_ref()))
            {
                return Some(name.clone());
            }
        }
    }
    metafile.trackers.first().map(|tracker| tracker.to_string())
}

fn action_error(message: impl Into<Cow<'static, str>>) -> SporosError {
    SporosError::Action {
        message: message.into(),
    }
}

fn persistence_error(error: rusqlite::Error) -> SporosError {
    SporosError::Persistence {
        message: Cow::Owned(error.to_string()),
    }
}

fn json_error(error: serde_json::Error) -> SporosError {
    SporosError::Persistence {
        message: Cow::Owned(format!("failed to parse indexer trackers JSON: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::{restore_from_torrent_cache, save_candidate_torrent, save_torrent_with_metadata};
    use crate::{
        domain::{InfoHash, MediaType},
        persistence::Database,
        torrent::{SavedTorrentMetadata, parse_metadata_from_filename, torrent_cache_path},
    };
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn save_action_writes_and_touches_existing_output() {
        let root = temp_path("save-action");
        let output_dir = root.join("out");
        fs::create_dir_all(&output_dir).expect("output dir");
        let bytes = torrent_bytes("Saved.Release", "https://tracker.example/announce", 10);
        let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
        let mut notifications = Vec::new();

        let saved = save_candidate_torrent(
            &output_dir,
            "TrackerOne",
            &metafile,
            &bytes,
            |notification| {
                notifications.push(notification.clone());
                Ok(())
            },
        )
        .expect("save");

        assert!(!saved.existed);
        assert!(saved.path.exists());
        let filename = saved
            .path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .expect("filename");
        let parsed = parse_metadata_from_filename(filename).expect("metadata");
        assert_eq!(parsed.tracker, "TrackerOne");
        assert_eq!(parsed.name, "Saved.Release");
        assert!(!parsed.cached);
        assert_eq!(notifications.len(), 1);

        let saved_again =
            save_candidate_torrent(&output_dir, "TrackerOne", &metafile, b"changed", |_| Ok(()))
                .expect("save again");

        assert!(saved_again.existed);
        assert_eq!(fs::read(saved.path).expect("saved bytes"), bytes);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn restore_from_cache_uses_indexer_tracker_names_and_keeps_cache() {
        let root = temp_path("restore-cache");
        fs::create_dir_all(&root).expect("root");
        let output_dir = root.join("out");
        let database = Database::open_app_dir(&root).expect("database");
        database
            .connection()
            .execute(
                "INSERT INTO indexer (name, url, apikey, trackers, active)
                 VALUES ('TrackerName', 'https://indexer.example/api', 'secret', ?1, 1)",
                [r#"["tracker.example"]"#],
            )
            .expect("indexer");
        let bytes = torrent_bytes("Cached.Release", "https://tracker.example/announce", 20);
        let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
        let cache_path = torrent_cache_path(&root, &metafile.info_hash);
        fs::create_dir_all(cache_path.parent().expect("cache parent")).expect("cache dir");
        fs::write(&cache_path, &bytes).expect("cache write");
        let mut notifications = 0;

        let summary = restore_from_torrent_cache(&database, &root, &output_dir, |_| {
            notifications += 1;
            Ok(())
        })
        .expect("restore");

        assert_eq!(summary.scanned, 1);
        assert_eq!(summary.restored, 1);
        assert_eq!(summary.failed, 0);
        assert!(cache_path.exists());
        assert_eq!(notifications, 1);
        let outputs = fs::read_dir(&output_dir)
            .expect("output read")
            .collect::<Result<Vec<_>, _>>()
            .expect("entries");
        assert_eq!(outputs.len(), 1);
        let filename = outputs[0].file_name().into_string().expect("utf8 filename");
        let metadata = parse_metadata_from_filename(&filename).expect("metadata");
        assert_eq!(metadata.tracker, "TrackerName");
        assert!(metadata.cached);
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn explicit_metadata_save_supports_unknown_tracker_fallback() {
        let root = temp_path("metadata-save");
        let hash = InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567");
        let metadata = SavedTorrentMetadata::new(
            MediaType::Unknown,
            "UnknownTracker",
            "Restored.Release",
            hash,
            true,
        );

        let saved = save_torrent_with_metadata(&root, &metadata, b"torrent", true, |_| Ok(()))
            .expect("save");

        let filename = saved
            .path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .expect("filename");
        let parsed = parse_metadata_from_filename(filename).expect("metadata");
        assert_eq!(parsed.tracker, "UnknownTracker");
        assert!(parsed.cached);
        let _cleanup = fs::remove_dir_all(root);
    }

    fn torrent_bytes(name: &str, announce: &str, length: u64) -> Vec<u8> {
        format!(
            "d8:announce{}:{}4:infod6:lengthi{}e4:name{}:{}12:piece lengthi1e6:pieces20:aaaaaaaaaaaaaaaaaaaaee",
            announce.len(),
            announce,
            length,
            name.len(),
            name
        )
        .into_bytes()
    }

    fn temp_path(name: &str) -> PathBuf {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("sporos-{name}-{millis}"))
    }
}
