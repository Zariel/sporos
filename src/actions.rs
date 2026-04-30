//! Save, inject, link, restore, and cleanup actions.

use std::{
    borrow::Cow,
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use crate::{
    SporosError,
    config::LinkType,
    domain::{Decision, File, Metafile, SaveResult, Searchee},
    persistence::Database,
    torrent::{SavedTorrentMetadata, parse_metafile, torrent_cache_dir, torrent_save_path},
};

/// Options for link creation.
#[derive(Debug, Clone)]
pub struct FileLinkOptions<'a> {
    /// Configured link directories.
    pub link_dirs: &'a [PathBuf],
    /// Link mode.
    pub link_type: LinkType,
    /// Put links directly under the link dir rather than `<linkDir>/<tracker>`.
    pub flat_linking: bool,
    /// Ignore missing source files.
    pub ignore_missing: bool,
    /// Resolve file symlink sources before creating links.
    pub unwrap_symlinks: bool,
}

/// Result of linking files for one candidate.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct FileLinkResult {
    /// New links or copies created.
    pub linked: usize,
    /// Destination files that already existed.
    pub skipped_existing: usize,
    /// Source files missing and ignored.
    pub missing_sources: usize,
    /// Top-level destination roots created by this call.
    pub created_roots: Vec<PathBuf>,
}

/// Choose a link directory compatible with the source path.
pub fn select_link_dir(
    source_path: &Path,
    link_dirs: &[PathBuf],
    link_type: LinkType,
) -> crate::Result<Option<PathBuf>> {
    for link_dir in link_dirs {
        if probe_link_dir(source_path, link_dir, link_type).unwrap_or(false) {
            return Ok(Some(link_dir.clone()));
        }
    }
    if link_type == LinkType::Symlink {
        Ok(link_dirs.first().cloned())
    } else {
        Ok(None)
    }
}

/// Compute the destination directory for a tracker under a selected link dir.
pub fn link_destination_dir(link_dir: &Path, tracker: &str, flat_linking: bool) -> PathBuf {
    if flat_linking {
        link_dir.to_path_buf()
    } else {
        link_dir.join(filesystem_safe_segment(tracker))
    }
}

/// Link all candidate files from local searchee data into the destination directory.
pub fn link_all_files_in_metafile(
    searchee: &Searchee<'_>,
    candidate: &Metafile<'_>,
    decision: Decision,
    destination_dir: &Path,
    options: &FileLinkOptions<'_>,
) -> crate::Result<FileLinkResult> {
    let pairs = file_link_pairs(searchee, candidate, decision)?;
    let mut result = FileLinkResult::default();
    let mut created_roots = BTreeSet::new();
    for pair in pairs {
        let source = if options.unwrap_symlinks {
            unwrap_file_symlink(&pair.source)?
        } else {
            pair.source
        };
        if !source.exists() {
            if options.ignore_missing {
                result.missing_sources += 1;
                continue;
            }
            return Err(action_error(format!(
                "link source does not exist: {}",
                source.display()
            )));
        }
        let destination = destination_dir.join(pair.destination);
        if destination.exists() {
            result.skipped_existing += 1;
            continue;
        }
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                action_error(format!("failed to create link destination: {error}"))
            })?;
        }
        create_link(&source, &destination, options.link_type)?;
        result.linked += 1;
        if let Some(root) = created_root(destination_dir, &destination) {
            created_roots.insert(root);
        }
    }
    result.created_roots = created_roots.into_iter().collect();
    Ok(result)
}

/// Remove destination roots that were created during a failed action.
pub fn cleanup_created_roots(roots: &[PathBuf]) -> crate::Result<usize> {
    let mut removed = 0;
    for root in roots.iter().rev() {
        if root.is_dir() {
            fs::remove_dir_all(root)
        } else {
            fs::remove_file(root)
        }
        .map_err(|error| action_error(format!("failed to remove linked root: {error}")))?;
        removed += 1;
    }
    Ok(removed)
}
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

#[derive(Debug, Clone, Eq, PartialEq)]
struct FileLinkPair {
    source: PathBuf,
    destination: PathBuf,
}

fn file_link_pairs(
    searchee: &Searchee<'_>,
    candidate: &Metafile<'_>,
    decision: Decision,
) -> crate::Result<Vec<FileLinkPair>> {
    if decision == Decision::Match {
        if let Some(pairs) = exact_file_link_pairs(searchee, candidate) {
            return Ok(pairs);
        }
    }
    greedy_file_link_pairs(&searchee.files, &candidate.files, source_root(searchee))
}

fn exact_file_link_pairs(
    searchee: &Searchee<'_>,
    candidate: &Metafile<'_>,
) -> Option<Vec<FileLinkPair>> {
    let root = source_root(searchee)?;
    let mut pairs = Vec::with_capacity(candidate.files.len());
    for candidate_file in &candidate.files {
        let local = searchee.files.iter().find(|local| {
            local.length == candidate_file.length && local.path == candidate_file.path
        })?;
        pairs.push(FileLinkPair {
            source: source_file_path(local, Some(&root)),
            destination: PathBuf::from(candidate_file.path.as_ref()),
        });
    }
    Some(pairs)
}

fn greedy_file_link_pairs(
    searchee_files: &[File<'_>],
    candidate_files: &[File<'_>],
    root: Option<PathBuf>,
) -> crate::Result<Vec<FileLinkPair>> {
    let mut used = vec![false; searchee_files.len()];
    let mut pairs = Vec::with_capacity(candidate_files.len());
    for candidate in candidate_files {
        let Some((index, local)) = searchee_files
            .iter()
            .enumerate()
            .filter(|(index, local)| {
                used.get(*index).is_some_and(|used| !*used) && local.length == candidate.length
            })
            .max_by_key(|(_, local)| local.name.eq_ignore_ascii_case(candidate.name.as_ref()))
        else {
            return Err(action_error(format!(
                "no local file matches candidate file {}",
                candidate.path
            )));
        };
        if let Some(used_slot) = used.get_mut(index) {
            *used_slot = true;
        }
        pairs.push(FileLinkPair {
            source: source_file_path(local, root.as_ref()),
            destination: PathBuf::from(candidate.path.as_ref()),
        });
    }
    Ok(pairs)
}

fn source_root(searchee: &Searchee<'_>) -> Option<PathBuf> {
    searchee
        .client
        .as_ref()
        .map(|client| PathBuf::from(client.save_path.as_ref()))
        .or_else(|| {
            searchee.path.as_ref().and_then(|path| {
                let path = Path::new(path.as_ref());
                if path.is_dir() {
                    Some(path.to_path_buf())
                } else {
                    path.parent().map(Path::to_path_buf)
                }
            })
        })
}

fn source_file_path(file: &File<'_>, root: Option<&PathBuf>) -> PathBuf {
    let path = Path::new(file.path.as_ref());
    if path.is_absolute() {
        path.to_path_buf()
    } else if let Some(root) = root {
        root.join(path)
    } else {
        path.to_path_buf()
    }
}

fn create_link(source: &Path, destination: &Path, link_type: LinkType) -> crate::Result<()> {
    match link_type {
        LinkType::Hardlink => fs::hard_link(source, destination),
        LinkType::Symlink => symlink_file(source, destination),
        LinkType::Reflink | LinkType::ReflinkOrCopy => fs::copy(source, destination).map(|_| ()),
    }
    .map_err(|error| action_error(format!("failed to link file: {error}")))
}

#[cfg(unix)]
fn symlink_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(source, destination)
}

#[cfg(windows)]
fn symlink_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_file(source, destination)
}

fn unwrap_file_symlink(path: &Path) -> crate::Result<PathBuf> {
    let mut current = path.to_path_buf();
    for _ in 0..16 {
        let metadata = fs::symlink_metadata(&current)
            .map_err(|error| action_error(format!("failed to inspect symlink source: {error}")))?;
        if !metadata.file_type().is_symlink() {
            return Ok(current);
        }
        let target = fs::read_link(&current)
            .map_err(|error| action_error(format!("failed to read symlink source: {error}")))?;
        current = if target.is_absolute() {
            target
        } else {
            current
                .parent()
                .map(|parent| parent.join(&target))
                .unwrap_or(target)
        };
    }
    Err(action_error("too many nested symlinks in link source"))
}

fn created_root(destination_dir: &Path, destination: &Path) -> Option<PathBuf> {
    let relative = destination.strip_prefix(destination_dir).ok()?;
    let first = relative.components().next()?;
    Some(destination_dir.join(first.as_os_str()))
}

fn probe_link_dir(source_path: &Path, link_dir: &Path, link_type: LinkType) -> crate::Result<bool> {
    fs::create_dir_all(link_dir)
        .map_err(|error| action_error(format!("failed to create linkDir: {error}")))?;
    let probe_source = if source_path.is_file() {
        source_path.to_path_buf()
    } else {
        link_dir.join(".cross-seed-link-probe-source")
    };
    let created_probe = !probe_source.exists();
    if created_probe {
        fs::write(&probe_source, b"probe")
            .map_err(|error| action_error(format!("failed to create link probe: {error}")))?;
    }
    let probe_dest = link_dir.join(".cross-seed-link-probe-dest");
    let _cleanup = fs::remove_file(&probe_dest);
    let result = create_link(&probe_source, &probe_dest, link_type).is_ok();
    let _cleanup = fs::remove_file(&probe_dest);
    if created_probe {
        let _cleanup = fs::remove_file(&probe_source);
    }
    Ok(result)
}

fn filesystem_safe_segment(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            character if character.is_control() => '_',
            character => character,
        })
        .collect()
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
    use super::{
        FileLinkOptions, cleanup_created_roots, link_all_files_in_metafile, link_destination_dir,
        restore_from_torrent_cache, save_candidate_torrent, save_torrent_with_metadata,
        select_link_dir,
    };
    use crate::{
        config::LinkType,
        domain::{ClientTorrentMetadata, Decision, File, InfoHash, MediaType, Metafile, Searchee},
        persistence::Database,
        torrent::{SavedTorrentMetadata, parse_metadata_from_filename, torrent_cache_path},
    };
    use std::{
        borrow::Cow,
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

    #[test]
    fn links_exact_tree_with_hardlinks_and_cleans_roots() {
        let root = temp_path("link-exact");
        let source = root.join("downloads/Release");
        let link_dir = root.join("links");
        fs::create_dir_all(&source).expect("source dir");
        fs::write(source.join("file.mkv"), b"video").expect("source file");
        let mut searchee =
            Searchee::from_files("Release", "Release", vec![File::new("Release/file.mkv", 5)]);
        searchee.client = Some(ClientTorrentMetadata::new(
            "client",
            root.join("downloads").display().to_string(),
            None,
            Vec::new(),
            Vec::<Cow<'static, str>>::new(),
        ));
        let candidate = Metafile::from_files(
            InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
            "Release",
            "Release",
            1,
            vec![File::new("Release/file.mkv", 5)],
        );
        let destination = link_destination_dir(&link_dir, "Tracker/One", false);

        let result = link_all_files_in_metafile(
            &searchee,
            &candidate,
            Decision::Match,
            &destination,
            &FileLinkOptions {
                link_dirs: std::slice::from_ref(&link_dir),
                link_type: LinkType::Hardlink,
                flat_linking: false,
                ignore_missing: false,
                unwrap_symlinks: false,
            },
        )
        .expect("link");

        assert_eq!(result.linked, 1);
        assert!(destination.join("Release/file.mkv").exists());
        assert_eq!(result.created_roots, vec![destination.join("Release")]);
        assert_eq!(
            cleanup_created_roots(&result.created_roots).expect("cleanup"),
            1
        );
        assert!(!destination.join("Release").exists());
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn greedy_linking_prefers_same_name_and_supports_symlink_fallback() {
        let root = temp_path("link-greedy");
        let data = root.join("data");
        let link_dir = root.join("links");
        fs::create_dir_all(&data).expect("data dir");
        fs::write(data.join("same.mkv"), b"video").expect("same");
        fs::write(data.join("other.mkv"), b"video").expect("other");
        let searchee = Searchee::from_files(
            "Release",
            "Release",
            vec![
                File::new(data.join("other.mkv").display().to_string(), 5),
                File::new(data.join("same.mkv").display().to_string(), 5),
            ],
        );
        let candidate = Metafile::from_files(
            InfoHash::from_validated("fedcba9876543210fedcba9876543210fedcba98"),
            "Candidate",
            "Candidate",
            1,
            vec![File::new("Candidate/same.mkv", 5)],
        );
        let selected = select_link_dir(&data, std::slice::from_ref(&link_dir), LinkType::Symlink)
            .expect("select")
            .expect("link dir");
        let destination = link_destination_dir(&selected, "Tracker", true);

        let result = link_all_files_in_metafile(
            &searchee,
            &candidate,
            Decision::MatchSizeOnly,
            &destination,
            &FileLinkOptions {
                link_dirs: std::slice::from_ref(&link_dir),
                link_type: LinkType::Symlink,
                flat_linking: true,
                ignore_missing: false,
                unwrap_symlinks: true,
            },
        )
        .expect("link");

        assert_eq!(result.linked, 1);
        let linked = destination.join("Candidate/same.mkv");
        assert!(
            fs::symlink_metadata(&linked)
                .expect("link metadata")
                .file_type()
                .is_symlink()
        );
        let target = fs::read_link(linked).expect("read link");
        assert!(target.ends_with("same.mkv"));
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
