//! Save, inject, link, restore, and cleanup actions.

use std::{
    borrow::Cow,
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

use crate::{
    SporosError,
    clients::{
        ClientErrorCode, DownloadDirOptions, InjectionOptions, NewTorrent, ResumeOptions,
        TorrentClient, select_injection_client,
    },
    config::LinkType,
    domain::{
        Candidate, ClientLabel, Decision, File, InjectionResult, Metafile, SaveResult, Searchee,
    },
    matching::{AssessmentOptions, assess_metafile},
    persistence::Database,
    torrent::{
        SavedTorrentMetadata, parse_metadata_from_filename, parse_metafile, torrent_cache_dir,
        torrent_save_path,
    },
};

static CLIENT_INJECTION: Mutex<()> = Mutex::new(());

/// Options for link creation.
#[derive(Debug, Clone)]
pub struct FileLinkOptions<'a> {
    /// Configured link directories.
    pub link_dirs: &'a [PathBuf],
    /// Link mode.
    pub link_type: LinkType,
    /// Put links directly under the link dir rather than `<link_dir>/<tracker>`.
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

/// Inputs for one matched candidate injection.
pub struct InjectionAction<'a> {
    /// Local item being cross-seeded.
    pub searchee: &'a Searchee<'a>,
    /// Remote candidate metadata.
    pub candidate: &'a Candidate<'a>,
    /// Matched candidate torrent metadata.
    pub metafile: &'a Metafile<'a>,
    /// Raw candidate torrent bytes.
    pub bytes: &'a [u8],
    /// Conservative match decision.
    pub decision: Decision,
}

/// Runtime settings for injecting a matched candidate.
pub struct InjectionActionOptions<'a> {
    /// Configured torrent clients in priority order.
    pub clients: &'a [&'a dyn TorrentClient],
    /// Optional output directory for save-for-retry behavior.
    pub output_dir: Option<&'a Path>,
    /// Configured link directories.
    pub link_dirs: &'a [PathBuf],
    /// Link mode.
    pub link_type: LinkType,
    /// Put links directly under the link dir rather than `<link_dir>/<tracker>`.
    pub flat_linking: bool,
    /// Resolve file symlink sources before creating links.
    pub unwrap_symlinks: bool,
    /// Skip client-side recheck.
    pub skip_recheck: bool,
    /// Category or label to apply during injection.
    pub category: Option<ClientLabel<'static>>,
    /// Tags or labels to apply during injection.
    pub tags: Vec<ClientLabel<'static>>,
}

/// Runtime settings for retrying saved torrent injection.
pub struct SavedInjectionOptions<'a> {
    /// Directory containing saved `.torrent` files.
    pub input_dir: &'a Path,
    /// Shared inject action settings.
    pub injection: &'a InjectionActionOptions<'a>,
    /// Matching options for saved torrent assessment.
    pub assessment: &'a AssessmentOptions<'a>,
    /// Allow saved torrents to match even when filename title metadata differs.
    pub ignore_titles: bool,
}

/// Summary from one saved torrent injection pass.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct SavedInjectionSummary {
    /// Saved `.torrent` files scanned.
    pub scanned: usize,
    /// Saved torrents injected and deleted.
    pub injected: usize,
    /// Saved torrents already present in a client and deleted.
    pub already_exists: usize,
    /// Saved torrents kept because the source was incomplete.
    pub incomplete: usize,
    /// Saved torrents kept because matching or injection failed.
    pub failed: usize,
    /// Saved files deleted after terminal success.
    pub deleted: usize,
}

/// Perform inject-mode action side effects for one matched candidate.
pub fn perform_injection_action<N>(
    action: &InjectionAction<'_>,
    options: &InjectionActionOptions<'_>,
    notify_saved: N,
) -> crate::Result<InjectionResult>
where
    N: FnMut(&SaveNotification) -> crate::Result<()>,
{
    let _guard = CLIENT_INJECTION
        .lock()
        .map_err(|_error| action_error("client injection mutex was poisoned"))?;
    perform_injection_action_without_mutex(action, options, notify_saved)
}

/// Retry injection for saved `.torrent` files in `inject_dir` or `output_dir`.
pub fn inject_saved_torrents<N>(
    options: &SavedInjectionOptions<'_>,
    searchees: &[Searchee<'static>],
    mut notify_saved: N,
) -> crate::Result<SavedInjectionSummary>
where
    N: FnMut(&SaveNotification) -> crate::Result<()>,
{
    let mut summary = SavedInjectionSummary::default();
    let entries = match fs::read_dir(options.input_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(summary),
        Err(error) => {
            return Err(action_error(format!(
                "failed to read saved torrent dir {}: {error}",
                options.input_dir.display()
            )));
        }
    };

    for entry in entries {
        let entry = entry.map_err(|error| {
            action_error(format!("failed to read saved torrent dir entry: {error}"))
        })?;
        let path = entry.path();
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("torrent") {
            continue;
        }
        summary.scanned += 1;
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::debug!("failed to read saved torrent {}: {error}", path.display());
                summary.failed += 1;
                continue;
            }
        };
        let metafile = match parse_metafile(&bytes) {
            Ok(metafile) => metafile,
            Err(error) => {
                tracing::debug!("failed to parse saved torrent {}: {error}", path.display());
                summary.failed += 1;
                continue;
            }
        };
        let filename_metadata = path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .and_then(parse_metadata_from_filename);
        let Some((searchee, decision)) =
            best_saved_match(&metafile, filename_metadata.as_ref(), searchees, options)
        else {
            summary.failed += 1;
            continue;
        };
        let tracker = filename_metadata
            .as_ref()
            .map(|metadata| metadata.tracker.as_ref())
            .or_else(|| metafile.trackers.first().map(Cow::as_ref))
            .unwrap_or("UnknownTracker");
        let result = perform_injection_action(
            &InjectionAction {
                searchee,
                candidate: &Candidate::new(
                    metafile.name.as_ref(),
                    path.display().to_string(),
                    None::<String>,
                    tracker,
                ),
                metafile: &metafile,
                bytes: &bytes,
                decision,
            },
            options.injection,
            &mut notify_saved,
        )?;
        match result {
            InjectionResult::Injected => {
                delete_saved_torrent(&path)?;
                summary.injected += 1;
                summary.deleted += 1;
            }
            InjectionResult::AlreadyExists => {
                delete_saved_torrent(&path)?;
                summary.already_exists += 1;
                summary.deleted += 1;
            }
            InjectionResult::TorrentNotComplete => summary.incomplete += 1,
            InjectionResult::Failure => summary.failed += 1,
        }
    }
    Ok(summary)
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

fn perform_injection_action_without_mutex<N>(
    action: &InjectionAction<'_>,
    options: &InjectionActionOptions<'_>,
    mut notify_saved: N,
) -> crate::Result<InjectionResult>
where
    N: FnMut(&SaveNotification) -> crate::Result<()>,
{
    let Some(client) = select_injection_client(options.clients, action.searchee)? else {
        return Err(action_error(
            "no writable torrent client available for injection",
        ));
    };

    if candidate_exists_elsewhere(options.clients, action.metafile)? {
        return Ok(InjectionResult::AlreadyExists);
    }

    let mut linked = FileLinkResult::default();
    let source_dir = match source_save_path(action.searchee, options.clients, true)? {
        Ok(path) => path,
        Err(ClientErrorCode::TorrentNotComplete) => {
            save_for_retry(action, options, &mut notify_saved)?;
            return Ok(InjectionResult::TorrentNotComplete);
        }
        Err(error) => {
            return Err(action_error(format!(
                "failed to resolve source path: {error:?}"
            )));
        }
    };
    let destination_dir = destination_dir(action, options, &source_dir)?;

    if !options.link_dirs.is_empty() {
        let Some(destination_dir) = destination_dir.as_ref() else {
            return Err(action_error("linking requires a destination directory"));
        };
        linked = link_all_files_in_metafile(
            action.searchee,
            action.metafile,
            action.decision,
            destination_dir,
            &FileLinkOptions {
                link_dirs: options.link_dirs,
                link_type: options.link_type,
                flat_linking: options.flat_linking,
                ignore_missing: false,
                unwrap_symlinks: options.unwrap_symlinks,
            },
        )?;
    }

    let should_recheck = should_recheck(action.metafile, action.decision, options.skip_recheck);
    let new_torrent = NewTorrent {
        metafile: action.metafile.clone(),
        bytes: Cow::Borrowed(action.bytes),
    };
    let result = client.inject(
        &new_torrent,
        action.searchee,
        action.decision,
        &InjectionOptions {
            destination_dir: destination_dir.clone(),
            category: options.category.clone(),
            tags: options.tags.clone(),
            paused: should_recheck,
            skip_checking: !should_recheck,
        },
    );

    if let Err(error) = result {
        if !linked.created_roots.is_empty() {
            cleanup_created_roots(&linked.created_roots)?;
        }
        save_for_retry(action, options, &mut notify_saved)?;
        tracing::warn!("torrent injection failed: {error}");
        return Ok(InjectionResult::Failure);
    }

    if should_recheck {
        save_for_retry(action, options, &mut notify_saved)?;
        client.recheck_torrent(&action.metafile.info_hash)?;
        client.resume_injection(
            action.metafile,
            action.decision,
            ResumeOptions { check_once: true },
        )?;
    } else if action.searchee.info_hash.is_none() {
        save_for_retry(action, options, &mut notify_saved)?;
    }

    if linked.linked > 0 && linked.skipped_existing > 0 {
        client.recheck_torrent(&action.metafile.info_hash)?;
        client.resume_injection(
            action.metafile,
            action.decision,
            ResumeOptions { check_once: true },
        )?;
    }

    Ok(InjectionResult::Injected)
}

fn candidate_exists_elsewhere(
    clients: &[&dyn TorrentClient],
    metafile: &Metafile<'_>,
) -> crate::Result<bool> {
    for client in clients {
        if client.is_torrent_in_client(&metafile.info_hash)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn best_saved_match<'a>(
    metafile: &Metafile<'_>,
    metadata: Option<&SavedTorrentMetadata<'_>>,
    searchees: &'a [Searchee<'static>],
    options: &SavedInjectionOptions<'_>,
) -> Option<(&'a Searchee<'static>, Decision)> {
    let mut matches = searchees
        .iter()
        .filter(|searchee| {
            options.ignore_titles
                || metadata.is_none_or(|metadata| {
                    metadata
                        .name
                        .as_ref()
                        .eq_ignore_ascii_case(searchee.title.as_ref())
                        || metadata
                            .name
                            .as_ref()
                            .eq_ignore_ascii_case(searchee.name.as_ref())
                })
        })
        .filter_map(|searchee| {
            let assessment = assess_metafile(
                metafile,
                searchee,
                options.assessment,
                true,
                options.assessment.fuzzy_size_threshold,
            );
            assessment
                .decision
                .is_match()
                .then_some((searchee, assessment.decision))
        })
        .collect::<Vec<_>>();
    matches.sort_by_key(|(searchee, decision)| {
        (
            searchee_client_priority(searchee, options.injection.clients),
            decision_rank(*decision),
        )
    });
    matches.into_iter().next()
}

fn searchee_client_priority(searchee: &Searchee<'_>, clients: &[&dyn TorrentClient]) -> u16 {
    searchee
        .client
        .as_ref()
        .and_then(|metadata| {
            clients
                .iter()
                .find(|client| client.metadata().host == metadata.host)
                .map(|client| client.metadata().priority)
        })
        .unwrap_or(u16::MAX)
}

fn decision_rank(decision: Decision) -> u8 {
    match decision {
        Decision::Match => 0,
        Decision::MatchSizeOnly => 1,
        Decision::MatchPartial => 2,
        _ => 3,
    }
}

fn delete_saved_torrent(path: &Path) -> crate::Result<()> {
    fs::remove_file(path)
        .map_err(|error| action_error(format!("failed to delete saved torrent: {error}")))
}

fn source_save_path(
    searchee: &Searchee<'_>,
    clients: &[&dyn TorrentClient],
    only_completed: bool,
) -> crate::Result<Result<PathBuf, ClientErrorCode>> {
    if let Some(client_metadata) = &searchee.client {
        let Some(info_hash) = &searchee.info_hash else {
            return Ok(Err(ClientErrorCode::NotFound));
        };
        let Some(client) = clients
            .iter()
            .copied()
            .find(|client| client.metadata().host == client_metadata.host)
        else {
            return Ok(Err(ClientErrorCode::NotFound));
        };
        let metafile = Metafile::from_files(
            info_hash.clone().into_owned(),
            searchee.name.as_ref().to_owned(),
            searchee.title.as_ref().to_owned(),
            0,
            searchee
                .files
                .iter()
                .cloned()
                .map(File::into_owned)
                .collect(),
        );
        return client.get_download_dir(&metafile, DownloadDirOptions { only_completed });
    }

    let Some(path) = searchee.path.as_ref() else {
        return Ok(Err(ClientErrorCode::Unsupported));
    };
    let path = PathBuf::from(path.as_ref());
    if !path.exists() {
        return Ok(Err(ClientErrorCode::NotFound));
    }
    if only_completed && !source_files_unchanged(searchee) {
        return Ok(Err(ClientErrorCode::TorrentNotComplete));
    }
    if path.is_dir() {
        Ok(Ok(path))
    } else if let Some(parent) = path.parent() {
        Ok(Ok(parent.to_path_buf()))
    } else {
        Ok(Err(ClientErrorCode::NotFound))
    }
}

fn destination_dir(
    action: &InjectionAction<'_>,
    options: &InjectionActionOptions<'_>,
    source_dir: &Path,
) -> crate::Result<Option<PathBuf>> {
    if options.link_dirs.is_empty() {
        return Ok(Some(source_dir.to_path_buf()));
    }
    let Some(link_dir) = select_link_dir(source_dir, options.link_dirs, options.link_type)? else {
        return Err(action_error("no compatible link_dir found for injection"));
    };
    Ok(Some(link_destination_dir(
        &link_dir,
        action.candidate.tracker.as_ref(),
        options.flat_linking,
    )))
}

fn source_files_unchanged(searchee: &Searchee<'_>) -> bool {
    searchee.files.iter().all(|file| {
        let path = source_file_path(file, source_root(searchee).as_ref());
        path.metadata()
            .ok()
            .is_some_and(|metadata| metadata.len() == file.length)
    })
}

fn save_for_retry<N>(
    action: &InjectionAction<'_>,
    options: &InjectionActionOptions<'_>,
    notify_saved: &mut N,
) -> crate::Result<()>
where
    N: FnMut(&SaveNotification) -> crate::Result<()>,
{
    if let Some(output_dir) = options.output_dir {
        save_candidate_torrent(
            output_dir,
            action.candidate.tracker.as_ref(),
            action.metafile,
            action.bytes,
            notify_saved,
        )?;
    }
    Ok(())
}

fn should_recheck(metafile: &Metafile<'_>, decision: Decision, skip_recheck: bool) -> bool {
    !skip_recheck
        && (decision == Decision::MatchPartial || metafile.files.iter().any(is_video_disc_file))
}

fn is_video_disc_file(file: &File<'_>) -> bool {
    let path = file.path.as_ref().to_ascii_lowercase();
    path.ends_with(".vob")
        || path.ends_with(".ifo")
        || path.ends_with(".bup")
        || path.contains("/bdmv/")
        || path.contains("\\bdmv\\")
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
        let created_root =
            created_root(destination_dir, &destination).filter(|root| !root.exists());
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                action_error(format!("failed to create link destination: {error}"))
            })?;
        }
        create_link(&source, &destination, options.link_type)?;
        result.linked += 1;
        if let Some(root) = created_root {
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

/// Result from saving one torrent to `output_dir`.
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

/// Summary from restoring cached torrents to `output_dir`.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct RestoreSummary {
    /// Cached torrent files scanned.
    pub scanned: usize,
    /// Cached torrents successfully restored or touched.
    pub restored: usize,
    /// Cached files that could not be read, parsed, or copied.
    pub failed: usize,
}

/// Save a matched candidate torrent to `output_dir`.
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
        .map_err(|error| action_error(format!("failed to create output_dir: {error}")))?;
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

/// Restore cached candidate torrents into `output_dir` without deleting cache files.
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
        .map_err(|error| action_error(format!("failed to create link_dir: {error}")))?;
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
        FileLinkOptions, InjectionAction, InjectionActionOptions, SavedInjectionOptions,
        cleanup_created_roots, inject_saved_torrents, link_all_files_in_metafile,
        link_destination_dir, perform_injection_action, restore_from_torrent_cache,
        save_candidate_torrent, save_torrent_with_metadata, select_link_dir,
    };
    use crate::{
        clients::{
            ClientErrorCode, ClientTorrent, DownloadDirOptions, InjectionOptions, NewTorrent,
            ResumeOptions, TorrentClient,
        },
        config::{LinkType, MatchMode},
        domain::{
            Candidate, ClientLabel, ClientTorrentMetadata, Decision, File, InfoHash,
            InjectionResult, MediaType, Metafile, Searchee, TorrentClientKind,
            TorrentClientMetadata,
        },
        matching::AssessmentOptions,
        persistence::Database,
        search::Blocklist,
        torrent::{SavedTorrentMetadata, parse_metadata_from_filename, torrent_cache_path},
    };
    use std::{
        borrow::Cow,
        collections::{BTreeMap, BTreeSet},
        fs,
        path::PathBuf,
        sync::Mutex,
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
    fn link_cleanup_ignores_preexisting_destination_roots() {
        let root = temp_path("link-preexisting-root");
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
        let destination = link_destination_dir(&link_dir, "Tracker", false);
        fs::create_dir_all(destination.join("Release")).expect("preexisting root");
        fs::write(destination.join("Release/user-file.txt"), b"keep").expect("user file");

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
        assert!(result.created_roots.is_empty());
        assert_eq!(
            cleanup_created_roots(&result.created_roots).expect("cleanup"),
            0
        );
        assert!(destination.join("Release/user-file.txt").exists());
        assert!(destination.join("Release/file.mkv").exists());
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

    #[test]
    fn injection_action_links_saves_rechecks_and_resumes() {
        let root = temp_path("inject-action");
        let data = root.join("data");
        let link_dir = root.join("links");
        let output_dir = root.join("out");
        fs::create_dir_all(&data).expect("data dir");
        fs::write(data.join("source.mkv"), b"video-data").expect("source");
        let mut searchee = Searchee::from_files(
            "Source.Release",
            "Source.Release",
            vec![File::new("source.mkv", 10)],
        );
        searchee.path = Some(Cow::Owned(data.display().to_string()));
        let bytes = torrent_bytes("Candidate.Release", "https://tracker.example/announce", 10);
        let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
        let candidate = Candidate::new(
            "Candidate.Release",
            "guid",
            Some("https://indexer.example/download"),
            "Tracker/One",
        );
        let client = FakeClient::new("client");
        let clients: [&dyn TorrentClient; 1] = [&client];
        let mut saved = 0;

        let result = perform_injection_action(
            &InjectionAction {
                searchee: &searchee,
                candidate: &candidate,
                metafile: &metafile,
                bytes: &bytes,
                decision: Decision::MatchPartial,
            },
            &InjectionActionOptions {
                clients: &clients,
                output_dir: Some(&output_dir),
                link_dirs: std::slice::from_ref(&link_dir),
                link_type: LinkType::Symlink,
                flat_linking: false,
                unwrap_symlinks: false,
                skip_recheck: false,
                category: Some(ClientLabel::new("tv")),
                tags: vec![ClientLabel::new("cross-seed")],
            },
            |_| {
                saved += 1;
                Ok(())
            },
        )
        .expect("inject action");

        assert_eq!(result, InjectionResult::Injected);
        assert_eq!(saved, 1);
        assert!(link_dir.join("Tracker_One/Candidate.Release").exists());
        let calls = client.calls.lock().expect("calls").clone();
        assert_eq!(calls, vec!["inject", "recheck", "resume"]);
        assert_eq!(
            client
                .last_options
                .lock()
                .expect("options")
                .as_ref()
                .map(|options| options.paused),
            Some(true)
        );
        assert_eq!(
            fs::read_dir(&output_dir)
                .expect("output")
                .collect::<Result<Vec<_>, _>>()
                .expect("entries")
                .len(),
            1
        );
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn injection_action_saves_incomplete_sources_for_retry() {
        let root = temp_path("inject-incomplete");
        let output_dir = root.join("out");
        let mut searchee = Searchee::from_files(
            "Source.Release",
            "Source.Release",
            vec![File::new("file.mkv", 10)],
        );
        searchee.client = Some(ClientTorrentMetadata::new(
            "client",
            "/downloads",
            None,
            Vec::new(),
            Vec::<Cow<'static, str>>::new(),
        ));
        searchee.info_hash = Some(InfoHash::from_validated(
            "0123456789abcdef0123456789abcdef01234567",
        ));
        let bytes = torrent_bytes("Candidate.Release", "https://tracker.example/announce", 10);
        let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
        let candidate = Candidate::new("Candidate.Release", "guid", None::<String>, "Tracker");
        let mut client = FakeClient::new("client");
        client.download_dir = Err(ClientErrorCode::TorrentNotComplete);
        let clients: [&dyn TorrentClient; 1] = [&client];

        let result = perform_injection_action(
            &InjectionAction {
                searchee: &searchee,
                candidate: &candidate,
                metafile: &metafile,
                bytes: &bytes,
                decision: Decision::Match,
            },
            &InjectionActionOptions {
                clients: &clients,
                output_dir: Some(&output_dir),
                link_dirs: &[],
                link_type: LinkType::Symlink,
                flat_linking: false,
                unwrap_symlinks: false,
                skip_recheck: true,
                category: None,
                tags: Vec::new(),
            },
            |_| Ok(()),
        )
        .expect("inject action");

        assert_eq!(result, InjectionResult::TorrentNotComplete);
        assert!(client.calls.lock().expect("calls").is_empty());
        assert_eq!(
            fs::read_dir(&output_dir)
                .expect("output")
                .collect::<Result<Vec<_>, _>>()
                .expect("entries")
                .len(),
            1
        );
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn saved_torrent_injection_deletes_successful_retry() {
        let root = temp_path("saved-inject");
        let input_dir = root.join("saved");
        let data = root.join("data");
        fs::create_dir_all(&data).expect("data");
        fs::write(data.join("Candidate.Release"), b"video-data").expect("source");
        let bytes = torrent_bytes("Candidate.Release", "https://tracker.example/announce", 10);
        let metafile = crate::torrent::parse_metafile(&bytes).expect("metafile");
        let saved = save_candidate_torrent(&input_dir, "Tracker", &metafile, &bytes, |_| Ok(()))
            .expect("save");
        let mut searchee = Searchee::from_files(
            "Candidate.Release",
            "Candidate.Release",
            vec![File::new("Candidate.Release", 10)],
        );
        searchee.path = Some(Cow::Owned(data.display().to_string()));
        let client = FakeClient::new("client");
        let clients: [&dyn TorrentClient; 1] = [&client];
        let blocklist = Blocklist::parse(&[]).expect("blocklist");
        let excluded = BTreeSet::new();
        let assessment = AssessmentOptions {
            match_mode: MatchMode::Strict,
            fuzzy_size_threshold: 0.05,
            season_from_episodes: 0.75,
            include_single_episodes: true,
            info_hashes_to_exclude: &excluded,
            blocklist: &blocklist,
        };
        let injection = InjectionActionOptions {
            clients: &clients,
            output_dir: Some(&input_dir),
            link_dirs: &[],
            link_type: LinkType::Symlink,
            flat_linking: false,
            unwrap_symlinks: false,
            skip_recheck: true,
            category: None,
            tags: vec![ClientLabel::new("cross-seed")],
        };

        let summary = inject_saved_torrents(
            &SavedInjectionOptions {
                input_dir: &input_dir,
                injection: &injection,
                assessment: &assessment,
                ignore_titles: false,
            },
            &[searchee],
            |_| Ok(()),
        )
        .expect("inject saved");

        assert_eq!(summary.scanned, 1);
        assert_eq!(summary.injected, 1);
        assert_eq!(summary.deleted, 1);
        assert!(!saved.path.exists());
        assert_eq!(client.calls.lock().expect("calls").clone(), vec!["inject"]);
        let _cleanup = fs::remove_dir_all(root);
    }

    struct FakeClient {
        metadata: TorrentClientMetadata<'static>,
        existing: bool,
        download_dir: Result<PathBuf, ClientErrorCode>,
        calls: Mutex<Vec<&'static str>>,
        last_options: Mutex<Option<InjectionOptions>>,
    }

    impl FakeClient {
        fn new(host: &str) -> Self {
            Self {
                metadata: TorrentClientMetadata::new(
                    host.to_owned(),
                    0,
                    TorrentClientKind::QBittorrent,
                    false,
                    "fake",
                ),
                existing: false,
                download_dir: Ok(PathBuf::from("/downloads")),
                calls: Mutex::new(Vec::new()),
                last_options: Mutex::new(None),
            }
        }
    }

    impl TorrentClient for FakeClient {
        fn metadata(&self) -> &TorrentClientMetadata<'_> {
            &self.metadata
        }

        fn is_torrent_in_client(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
            Ok(self.existing)
        }

        fn is_torrent_complete(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
            Ok(true)
        }

        fn is_torrent_checking(&self, _info_hash: &InfoHash<'_>) -> crate::Result<bool> {
            Ok(false)
        }

        fn get_all_torrents(&self) -> crate::Result<Vec<ClientTorrent<'static>>> {
            Ok(Vec::new())
        }

        fn get_download_dir(
            &self,
            _metafile: &Metafile<'_>,
            _options: DownloadDirOptions,
        ) -> crate::Result<Result<PathBuf, ClientErrorCode>> {
            Ok(self.download_dir.clone())
        }

        fn get_all_download_dirs(&self) -> crate::Result<BTreeMap<String, PathBuf>> {
            Ok(BTreeMap::new())
        }

        fn inject(
            &self,
            _new_torrent: &NewTorrent<'_>,
            _searchee: &Searchee<'_>,
            _decision: Decision,
            options: &InjectionOptions,
        ) -> crate::Result<InjectionResult> {
            self.calls
                .lock()
                .map_err(|_error| super::action_error("calls lock poisoned"))?
                .push("inject");
            *self
                .last_options
                .lock()
                .map_err(|_error| super::action_error("options lock poisoned"))? =
                Some(options.clone());
            Ok(InjectionResult::Injected)
        }

        fn recheck_torrent(&self, _info_hash: &InfoHash<'_>) -> crate::Result<()> {
            self.calls
                .lock()
                .map_err(|_error| super::action_error("calls lock poisoned"))?
                .push("recheck");
            Ok(())
        }

        fn resume_injection(
            &self,
            _metafile: &Metafile<'_>,
            _decision: Decision,
            _options: ResumeOptions,
        ) -> crate::Result<()> {
            self.calls
                .lock()
                .map_err(|_error| super::action_error("calls lock poisoned"))?
                .push("resume");
            Ok(())
        }

        fn validate_config(&self) -> crate::Result<()> {
            Ok(())
        }
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
