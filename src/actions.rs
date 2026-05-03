//! Save, inject, link, restore, and cleanup actions.

use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::BTreeSet,
    fs,
    io::Write,
    path::{Component, Path, PathBuf},
    sync::{LazyLock, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    SporosError,
    clients::{
        ClientErrorCode, DownloadDirOptions, InjectionOptions, NewTorrent, ResumeOptions,
        TorrentClient, ensure_writable,
    },
    config::{LinkType, MatchMode},
    domain::{
        Candidate, ClientLabel, Decision, File, InjectionResult, Metafile, SaveResult, Searchee,
        SearcheeSource,
    },
    matching::{AssessmentOptions, assess_metafile},
    persistence::Database,
    torrent::{
        SavedTorrentMetadata, parse_metadata_from_filename, parse_metafile, torrent_cache_dir,
        torrent_save_path,
    },
};

static INJECTION_ACTOR: LazyLock<InjectionActor> = LazyLock::new(InjectionActor::new);

struct InjectionActor {
    permit: Mutex<()>,
}

impl InjectionActor {
    const fn new() -> Self {
        Self {
            permit: Mutex::new(()),
        }
    }

    fn submit<T>(&self, command: impl FnOnce() -> crate::Result<T>) -> crate::Result<T> {
        let _permit = self
            .permit
            .lock()
            .map_err(|_error| action_error("injection actor was poisoned"))?;
        command()
    }
}

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
    /// Configured match mode for partial resume policy.
    pub match_mode: MatchMode,
    /// Maximum missing bytes allowed for automatic partial resume.
    pub auto_resume_max_download: u64,
    /// Allow resume policy to account for non-relevant missing files.
    pub ignore_non_relevant_files_to_resume: bool,
    /// Category or label to apply during injection.
    pub category: Option<ClientLabel<'static>>,
    /// Tags or labels to apply during injection.
    pub tags: Vec<ClientLabel<'static>>,
    /// Derive duplicate cross-seed categories from source client metadata.
    pub duplicate_categories: bool,
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

struct InjectionTarget<'a> {
    client: &'a dyn TorrentClient,
    destination_dir: Option<PathBuf>,
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
    INJECTION_ACTOR.submit(|| perform_injection_action_in_actor(action, options, notify_saved))
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
        let filename_metadata = path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .and_then(parse_metadata_from_filename);
        let Some(filename_metadata) = filename_metadata else {
            tracing::debug!(
                "skipping saved torrent with unsupported filename: {}",
                path.display()
            );
            summary.failed += 1;
            continue;
        };
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
        if safe_saved_torrent_already_in_client(&filename_metadata, &metafile, options)? {
            delete_saved_torrent(&path)?;
            summary.already_exists += 1;
            summary.deleted += 1;
            continue;
        }
        let Some((searchee, decision)) =
            best_saved_match(&metafile, &filename_metadata, searchees, options)
        else {
            summary.failed += 1;
            continue;
        };
        let result = perform_injection_action(
            &InjectionAction {
                searchee,
                candidate: &Candidate::new(
                    metafile.name.as_ref(),
                    path.display().to_string(),
                    None::<String>,
                    filename_metadata.tracker.as_ref(),
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

fn safe_saved_torrent_already_in_client(
    metadata: &SavedTorrentMetadata<'_>,
    metafile: &Metafile<'_>,
    options: &SavedInjectionOptions<'_>,
) -> crate::Result<bool> {
    if metadata.info_hash.as_str() != metafile.info_hash.as_str() {
        return Ok(false);
    }
    candidate_existing_client(options.injection.clients, metafile).map(|client| client.is_some())
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

fn perform_injection_action_in_actor<N>(
    action: &InjectionAction<'_>,
    options: &InjectionActionOptions<'_>,
    mut notify_saved: N,
) -> crate::Result<InjectionResult>
where
    N: FnMut(&SaveNotification) -> crate::Result<()>,
{
    let existing_client = candidate_existing_client(options.clients, action.metafile)?;
    if existing_client.is_some() && options.link_dirs.is_empty() {
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
    let target = select_injection_target(action, options, &source_dir, existing_client)?;
    let client = target.client;
    let destination_dir = target.destination_dir;

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

    if existing_client.is_some() {
        if linked.linked > 0 {
            client.recheck_torrent(&action.metafile.info_hash)?;
            client.resume_injection(
                action.metafile,
                action.decision,
                resume_options(action, options, true),
            )?;
        }
        return Ok(InjectionResult::AlreadyExists);
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
            duplicate_categories: options.duplicate_categories,
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
            resume_options(action, options, true),
        )?;
    } else if action.searchee.info_hash.is_none() {
        save_for_retry(action, options, &mut notify_saved)?;
    }

    if linked.linked > 0 && linked.skipped_existing > 0 {
        client.recheck_torrent(&action.metafile.info_hash)?;
        client.resume_injection(
            action.metafile,
            action.decision,
            resume_options(action, options, true),
        )?;
    }

    Ok(InjectionResult::Injected)
}

fn candidate_existing_client<'a>(
    clients: &'a [&'a dyn TorrentClient],
    metafile: &Metafile<'_>,
) -> crate::Result<Option<&'a dyn TorrentClient>> {
    for client in clients {
        if client.is_torrent_in_client(&metafile.info_hash)? {
            return Ok(Some(*client));
        }
    }
    Ok(None)
}

fn best_saved_match<'a>(
    metafile: &Metafile<'_>,
    metadata: &SavedTorrentMetadata<'_>,
    searchees: &'a [Searchee<'static>],
    options: &SavedInjectionOptions<'_>,
) -> Option<(&'a Searchee<'static>, Decision)> {
    let mut matches = searchees
        .iter()
        .filter(|searchee| {
            options.ignore_titles || saved_title_matches(metadata.name.as_ref(), metafile, searchee)
        })
        .filter(|searchee| !options.assessment.blocklist.matches_searchee(searchee))
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
            searchee_source_rank(searchee.source()),
            Reverse(searchee.files.len()),
        )
    });
    matches.into_iter().next()
}

fn saved_title_matches(saved_name: &str, metafile: &Metafile<'_>, searchee: &Searchee<'_>) -> bool {
    let saved_keys =
        title_match_keys([saved_name, metafile.title.as_ref(), metafile.name.as_ref()]);
    let searchee_keys = title_match_keys([searchee.title.as_ref(), searchee.name.as_ref()]);
    saved_keys.iter().any(|saved| {
        searchee_keys
            .iter()
            .any(|searchee| title_key_matches(saved, searchee))
    })
}

fn title_match_keys<'a>(titles: impl IntoIterator<Item = &'a str>) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    for title in titles {
        insert_title_key(&mut keys, title);
        if let Some((primary, alternate)) = alternate_title_parts(title) {
            insert_title_key(&mut keys, primary);
            insert_title_key(&mut keys, alternate);
        }
    }
    keys
}

fn insert_title_key(keys: &mut BTreeSet<String>, title: &str) {
    let key = normalized_title_key(title);
    if !key.is_empty() {
        keys.insert(key);
    }
}

fn alternate_title_parts(title: &str) -> Option<(&str, &str)> {
    let without_close = title.strip_suffix(')')?;
    let (primary, alternate) = without_close.rsplit_once('(')?;
    let primary = primary.trim();
    let alternate = alternate.trim();
    (!primary.is_empty() && !alternate.is_empty()).then_some((primary, alternate))
}

fn normalized_title_key(title: &str) -> String {
    title
        .chars()
        .map(|character| {
            if character.is_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(".")
}

fn title_key_matches(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    if left.len().min(right.len()) >= 6 && (left.contains(right) || right.contains(left)) {
        return true;
    }
    fuzzy_title_match(left, right)
}

fn fuzzy_title_match(left: &str, right: &str) -> bool {
    let max_distance = left.len().max(right.len()) / 3;
    levenshtein_at_most(left, right, max_distance)
        .is_some_and(|distance| distance <= left.len().min(right.len()) / 3)
}

fn levenshtein_at_most(left: &str, right: &str, max_distance: usize) -> Option<usize> {
    let left = left.chars().collect::<Vec<_>>();
    let right = right.chars().collect::<Vec<_>>();
    if left.len().abs_diff(right.len()) > max_distance {
        return None;
    }
    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    let mut current = vec![0; right.len() + 1];
    for (left_index, left_char) in left.iter().enumerate() {
        *current.get_mut(0)? = left_index + 1;
        let mut row_min = *current.first()?;
        for (right_index, right_char) in right.iter().enumerate() {
            let cost = usize::from(left_char != right_char);
            let insert = previous.get(right_index + 1)?.saturating_add(1);
            let delete = current.get(right_index)?.saturating_add(1);
            let replace = previous.get(right_index)?.saturating_add(cost);
            let cell = insert.min(delete).min(replace);
            *current.get_mut(right_index + 1)? = cell;
            row_min = row_min.min(cell);
        }
        if row_min > max_distance {
            return None;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous.get(right.len()).copied()
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

fn searchee_source_rank(source: SearcheeSource) -> u8 {
    match source {
        SearcheeSource::TorrentClient | SearcheeSource::TorrentFile => 0,
        SearcheeSource::DataDir => 1,
        SearcheeSource::Virtual => 2,
    }
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

fn select_injection_target<'a>(
    action: &InjectionAction<'_>,
    options: &'a InjectionActionOptions<'a>,
    source_dir: &Path,
    existing_client: Option<&'a dyn TorrentClient>,
) -> crate::Result<InjectionTarget<'a>> {
    let destination_dir = destination_dir(action, options, source_dir)?;
    if options.clients.len() == 1 {
        let client = options.clients.first().copied().ok_or_else(|| {
            action_error("no compatible writable torrent client available for injection")
        })?;
        ensure_writable(client)?;
        return Ok(InjectionTarget {
            client,
            destination_dir,
        });
    }

    if let Some(client) = existing_client.filter(|client| !client.metadata().readonly) {
        return Ok(InjectionTarget {
            client,
            destination_dir,
        });
    }

    if let Some(client) = writable_source_client(options.clients, action.searchee)? {
        return Ok(InjectionTarget {
            client,
            destination_dir,
        });
    }

    let client = if options.link_dirs.is_empty() {
        first_writable_client(options.clients)
    } else {
        compatible_link_client(options.clients, source_dir, options.link_type)?
    };
    let Some(client) = client else {
        return Err(action_error(
            "no compatible writable torrent client available for injection",
        ));
    };
    Ok(InjectionTarget {
        client,
        destination_dir,
    })
}

fn writable_source_client<'a>(
    clients: &'a [&'a dyn TorrentClient],
    searchee: &Searchee<'_>,
) -> crate::Result<Option<&'a dyn TorrentClient>> {
    let Some(host) = searchee.client.as_ref().map(|client| client.host.as_ref()) else {
        return Ok(None);
    };
    let Some(client) = clients
        .iter()
        .copied()
        .find(|client| client.metadata().host.as_ref() == host)
    else {
        return Ok(None);
    };
    if client.metadata().readonly {
        Ok(None)
    } else {
        ensure_writable(client).map(|()| Some(client))
    }
}

fn first_writable_client<'a>(
    clients: &'a [&'a dyn TorrentClient],
) -> Option<&'a dyn TorrentClient> {
    clients
        .iter()
        .copied()
        .filter(|client| !client.metadata().readonly)
        .min_by_key(|client| client.metadata().priority)
}

fn compatible_link_client<'a>(
    clients: &'a [&'a dyn TorrentClient],
    source_dir: &Path,
    link_type: LinkType,
) -> crate::Result<Option<&'a dyn TorrentClient>> {
    let mut writable = clients
        .iter()
        .copied()
        .filter(|client| !client.metadata().readonly)
        .collect::<Vec<_>>();
    writable.sort_by_key(|client| client.metadata().priority);
    for client in writable {
        if client.has_matching_download_dir(&mut |download_dir| {
            probe_link_dir(source_dir, download_dir, link_type).or(Ok(false))
        })? {
            return Ok(Some(client));
        }
    }
    Ok(None)
}

fn source_files_unchanged(searchee: &Searchee<'_>) -> bool {
    let mut newest_mtime = None;
    for file in &searchee.files {
        let path = source_file_path(file, source_root(searchee).as_ref());
        let Ok(metadata) = path.metadata() else {
            return false;
        };
        if metadata.len() != file.length {
            return false;
        }
        if let Some(millis) = metadata_mtime_millis(&metadata) {
            newest_mtime = Some(newest_mtime.map_or(millis, |current: u64| current.max(millis)));
        }
    }
    match (searchee.mtime_millis, newest_mtime) {
        (Some(indexed), Some(current)) => current <= indexed,
        _ => true,
    }
}

fn metadata_mtime_millis(metadata: &fs::Metadata) -> Option<u64> {
    let modified = metadata.modified().ok()?;
    let duration = modified.duration_since(UNIX_EPOCH).ok()?;
    Some(duration.as_millis().min(u128::from(u64::MAX)) as u64)
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
        || decision == Decision::MatchPartial
        || metafile.files.iter().any(is_video_disc_file)
}

fn resume_options(
    action: &InjectionAction<'_>,
    options: &InjectionActionOptions<'_>,
    check_once: bool,
) -> ResumeOptions {
    ResumeOptions {
        check_once,
        max_remaining_bytes: max_remaining_bytes(
            action.metafile,
            action.decision,
            options.match_mode,
            options.auto_resume_max_download,
        ),
        ignore_non_relevant_files: options.ignore_non_relevant_files_to_resume,
    }
}

fn max_remaining_bytes(
    metafile: &Metafile<'_>,
    decision: Decision,
    match_mode: MatchMode,
    auto_resume_max_download: u64,
) -> u64 {
    if decision == Decision::MatchPartial
        && match_mode == MatchMode::Partial
        && !metafile.files.iter().any(is_video_disc_file)
    {
        auto_resume_max_download
    } else {
        0
    }
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
    ensure_not_filesystem_root(destination_dir, "link destination")?;
    for link_dir in options.link_dirs {
        ensure_not_filesystem_root(link_dir, "link root")?;
    }
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
        let destination = safe_link_destination(destination_dir, &pair.destination)?;
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
        ensure_link_parent_within_destination(destination_dir, &destination)?;
        create_link(&source, &destination, options.link_type)?;
        result.linked += 1;
        if let Some(root) = created_root {
            created_roots.insert(root);
        }
    }
    result.created_roots = created_roots.into_iter().collect();
    Ok(result)
}

fn safe_link_destination(destination_dir: &Path, relative: &Path) -> crate::Result<PathBuf> {
    if relative.is_absolute() {
        return Err(action_error(format!(
            "link destination must be relative: {}",
            relative.display()
        )));
    }
    for component in relative.components() {
        match component {
            Component::Normal(value) => {
                let text = value.to_string_lossy();
                if text.contains('\\') || has_windows_prefix(text.as_ref()) {
                    return Err(action_error(format!(
                        "unsafe link destination component: {}",
                        relative.display()
                    )));
                }
            }
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(action_error(format!(
                    "unsafe link destination path: {}",
                    relative.display()
                )));
            }
        }
    }
    Ok(destination_dir.join(relative))
}

fn ensure_link_parent_within_destination(
    destination_dir: &Path,
    destination: &Path,
) -> crate::Result<()> {
    let Some(parent) = destination.parent() else {
        return Err(action_error(format!(
            "link destination has no parent: {}",
            destination.display()
        )));
    };
    let root = fs::canonicalize(destination_dir).map_err(|error| {
        action_error(format!(
            "failed to canonicalize link destination root {}: {error}",
            destination_dir.display()
        ))
    })?;
    let parent = fs::canonicalize(parent).map_err(|error| {
        action_error(format!(
            "failed to canonicalize link destination parent {}: {error}",
            parent.display()
        ))
    })?;
    if !parent.starts_with(&root) {
        return Err(action_error(format!(
            "unsafe link destination escapes link root: {}",
            destination.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_not_filesystem_root(path: &Path, label: &str) -> crate::Result<()> {
    use std::os::unix::fs::MetadataExt;

    let root = fs::metadata("/")
        .map_err(|error| action_error(format!("failed to stat filesystem root: {error}")))?;
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(action_error(format!(
                "failed to stat {label} {}: {error}",
                path.display()
            )));
        }
    };
    if metadata.dev() == root.dev() && metadata.ino() == root.ino() {
        return Err(action_error(format!(
            "{label} must not be filesystem root: {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_not_filesystem_root(path: &Path, label: &str) -> crate::Result<()> {
    if path.parent().is_none() {
        return Err(action_error(format!(
            "{label} must not be filesystem root: {}",
            path.display()
        )));
    }
    Ok(())
}

fn has_windows_prefix(component: &str) -> bool {
    matches!(component.as_bytes(), [drive, b':', ..] if drive.is_ascii_alphabetic())
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
    let mut existed = saved_torrent_path_exists_without_symlink(&path)?;
    if existed {
        touch_existing_file(&path)?;
    } else {
        existed = write_new_saved_torrent(&path, bytes)?;
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

fn saved_torrent_path_exists_without_symlink(path: &Path) -> crate::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(action_error(format!(
            "refusing to save torrent through symlink: {}",
            path.display()
        ))),
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(action_error(format!(
            "failed to inspect saved torrent path {}: {error}",
            path.display()
        ))),
    }
}

fn write_new_saved_torrent(path: &Path, bytes: &[u8]) -> crate::Result<bool> {
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map(Some)
        .or_else(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                return Ok(None);
            }
            Err(error)
        })
        .map_err(|error| action_error(format!("failed to create saved torrent: {error}")))?;
    let Some(mut file) = file else {
        if saved_torrent_path_exists_without_symlink(path)? {
            touch_existing_file(path)?;
            return Ok(true);
        }
        return Err(action_error(format!(
            "saved torrent path disappeared before write: {}",
            path.display()
        )));
    };
    file.write_all(bytes)
        .map_err(|error| action_error(format!("failed to save torrent: {error}")))?;
    Ok(false)
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
        LinkType::Reflink => reflink_copy::reflink(source, destination),
        LinkType::ReflinkOrCopy => reflink_copy::reflink_or_copy(source, destination).map(|_| ()),
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
    let (probe_source, created_probe) = probe_source_path(source_path)?;
    let probe_dest = unique_probe_destination(link_dir)?;
    let result = create_link(&probe_source, &probe_dest, link_type).is_ok();
    let _cleanup = fs::remove_file(&probe_dest);
    if created_probe {
        let _cleanup = fs::remove_file(&probe_source);
    }
    Ok(result)
}

fn unique_probe_destination(link_dir: &Path) -> crate::Result<PathBuf> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    for attempt in 0..128 {
        let candidate = link_dir.join(format!(
            ".cross-seed-link-probe-dest-{}-{nanos}-{attempt}",
            std::process::id()
        ));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(action_error("failed to allocate unique link probe path"))
}

fn probe_source_path(source_path: &Path) -> crate::Result<(PathBuf, bool)> {
    if source_path.is_file() {
        return Ok((source_path.to_path_buf(), false));
    }
    if !source_path.is_dir() {
        return Err(action_error(format!(
            "link probe source is not readable: {}",
            source_path.display()
        )));
    }
    if let Some(file) = representative_probe_file(source_path)? {
        return Ok((file, false));
    }
    let probe_source = source_path.join(".cross-seed-link-probe-source");
    fs::write(&probe_source, b"probe")
        .map_err(|error| action_error(format!("failed to create link probe: {error}")))?;
    Ok((probe_source, true))
}

fn representative_probe_file(source_dir: &Path) -> crate::Result<Option<PathBuf>> {
    for entry in walkdir::WalkDir::new(source_dir).follow_links(false) {
        let entry = entry
            .map_err(|error| action_error(format!("failed to inspect link source: {error}")))?;
        if entry.file_type().is_file() {
            return Ok(Some(entry.path().to_path_buf()));
        }
    }
    Ok(None)
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
    let mut output = Vec::new();
    for row in database.indexer_tracker_rows()? {
        let trackers = serde_json::from_str::<Vec<String>>(&row.trackers).map_err(json_error)?;
        output.push((row.name, trackers));
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

fn json_error(error: serde_json::Error) -> SporosError {
    SporosError::Persistence {
        message: Cow::Owned(format!("failed to parse indexer trackers JSON: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FileLinkOptions, InjectionAction, InjectionActionOptions, SavedInjectionOptions,
        best_saved_match, cleanup_created_roots, inject_saved_torrents, link_all_files_in_metafile,
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
        persistence::{Database, SqlValue},
        search::Blocklist,
        torrent::{
            SavedTorrentMetadata, parse_metadata_from_filename, torrent_cache_path,
            torrent_save_path,
        },
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
            .execute_sql(
                "INSERT INTO indexer (name, url, apikey, trackers, active)
                 VALUES ('TrackerName', 'https://indexer.example/api', 'secret', ?1, 1)",
                &[SqlValue::Text(std::borrow::Cow::Borrowed(
                    r#"["tracker.example"]"#,
                ))],
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

    #[cfg(unix)]
    #[test]
    fn saved_torrent_save_refuses_symlink_target() {
        let root = temp_path("metadata-save-symlink");
        fs::create_dir_all(&root).expect("root");
        let target = root.join("outside.torrent");
        fs::write(&target, b"outside").expect("outside");
        let hash = InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567");
        let metadata = SavedTorrentMetadata::new(
            MediaType::Unknown,
            "UnknownTracker",
            "Symlink.Release",
            hash,
            true,
        );
        let path = torrent_save_path(&root, &metadata);
        std::os::unix::fs::symlink(&target, &path).expect("symlink");

        let error = save_torrent_with_metadata(&root, &metadata, b"torrent", true, |_| Ok(()))
            .expect_err("symlink rejected");

        assert!(
            error
                .to_string()
                .contains("refusing to save torrent through symlink")
        );
        assert_eq!(fs::read(&target).expect("target"), b"outside");
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
    fn linking_rejects_candidate_paths_outside_destination() {
        let root = temp_path("link-traversal");
        let data = root.join("data");
        let destination = root.join("links");
        fs::create_dir_all(&data).expect("data dir");
        fs::write(data.join("source.mkv"), b"video").expect("source file");
        let mut searchee =
            Searchee::from_files("Source", "Source", vec![File::new("source.mkv", 5)]);
        searchee.path = Some(Cow::Owned(data.join("source.mkv").display().to_string()));
        let candidate = Metafile::from_files(
            InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
            "Release",
            "Release",
            1,
            vec![File::new("../escape.mkv", 5)],
        );

        let error = link_all_files_in_metafile(
            &searchee,
            &candidate,
            Decision::MatchSizeOnly,
            &destination,
            &FileLinkOptions {
                link_dirs: std::slice::from_ref(&destination),
                link_type: LinkType::Hardlink,
                flat_linking: false,
                ignore_missing: false,
                unwrap_symlinks: false,
            },
        )
        .expect_err("unsafe destination rejected");

        assert!(error.to_string().contains("unsafe link destination"));
        assert!(!root.join("escape.mkv").exists());
        let _cleanup = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn linking_rejects_destination_parent_symlink_escape() {
        let root = temp_path("link-parent-symlink");
        let data = root.join("data");
        let destination = root.join("links");
        let outside = root.join("outside");
        fs::create_dir_all(&data).expect("data dir");
        fs::create_dir_all(&destination).expect("destination");
        fs::create_dir_all(&outside).expect("outside");
        std::os::unix::fs::symlink(&outside, destination.join("Release")).expect("escape symlink");
        fs::write(data.join("source.mkv"), b"video").expect("source file");
        let mut searchee =
            Searchee::from_files("Source", "Source", vec![File::new("source.mkv", 5)]);
        searchee.path = Some(Cow::Owned(data.join("source.mkv").display().to_string()));
        let candidate = Metafile::from_files(
            InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
            "Release",
            "Release",
            1,
            vec![File::new("Release/file.mkv", 5)],
        );

        let error = link_all_files_in_metafile(
            &searchee,
            &candidate,
            Decision::MatchSizeOnly,
            &destination,
            &FileLinkOptions {
                link_dirs: std::slice::from_ref(&destination),
                link_type: LinkType::Hardlink,
                flat_linking: false,
                ignore_missing: false,
                unwrap_symlinks: false,
            },
        )
        .expect_err("symlink escape rejected");

        assert!(error.to_string().contains("escapes link root"));
        assert!(!outside.join("file.mkv").exists());
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn linking_rejects_filesystem_root_destination() {
        let root = temp_path("link-root-destination");
        let data = root.join("data");
        fs::create_dir_all(&data).expect("data dir");
        fs::write(data.join("source.mkv"), b"video").expect("source file");
        let mut searchee =
            Searchee::from_files("Source", "Source", vec![File::new("source.mkv", 5)]);
        searchee.path = Some(Cow::Owned(data.join("source.mkv").display().to_string()));
        let candidate = Metafile::from_files(
            InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
            "Release",
            "Release",
            1,
            vec![File::new("safe.mkv", 5)],
        );
        let destination = PathBuf::from("/");

        let error = link_all_files_in_metafile(
            &searchee,
            &candidate,
            Decision::MatchSizeOnly,
            &destination,
            &FileLinkOptions {
                link_dirs: std::slice::from_ref(&destination),
                link_type: LinkType::Hardlink,
                flat_linking: true,
                ignore_missing: false,
                unwrap_symlinks: false,
            },
        )
        .expect_err("root destination rejected");

        assert!(error.to_string().contains("filesystem root"));
        let _cleanup = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn linking_rejects_link_root_that_stats_as_filesystem_root() {
        let root = temp_path("link-root-symlink");
        let data = root.join("data");
        let link_root = root.join("root-link");
        fs::create_dir_all(&data).expect("data dir");
        std::os::unix::fs::symlink("/", &link_root).expect("root symlink");
        fs::write(data.join("source.mkv"), b"video").expect("source file");
        let mut searchee =
            Searchee::from_files("Source", "Source", vec![File::new("source.mkv", 5)]);
        searchee.path = Some(Cow::Owned(data.join("source.mkv").display().to_string()));
        let candidate = Metafile::from_files(
            InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
            "Release",
            "Release",
            1,
            vec![File::new("safe.mkv", 5)],
        );

        let error = link_all_files_in_metafile(
            &searchee,
            &candidate,
            Decision::MatchSizeOnly,
            &link_root,
            &FileLinkOptions {
                link_dirs: std::slice::from_ref(&link_root),
                link_type: LinkType::Hardlink,
                flat_linking: true,
                ignore_missing: false,
                unwrap_symlinks: false,
            },
        )
        .expect_err("root symlink rejected");

        assert!(error.to_string().contains("filesystem root"));
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
    fn link_probe_source_uses_source_directory() {
        let root = temp_path("link-probe-source");
        let source = root.join("source");
        let nested = source.join("nested");
        fs::create_dir_all(&nested).expect("source dir");
        fs::write(nested.join("episode.mkv"), b"video").expect("source file");

        let (probe, created) = super::probe_source_path(&source).expect("probe source");

        assert!(!created);
        assert_eq!(probe, nested.join("episode.mkv"));
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn link_probe_temp_source_is_created_in_source_directory() {
        let root = temp_path("link-probe-empty-source");
        let source = root.join("source");
        let link_dir = root.join("links");
        fs::create_dir_all(&source).expect("source dir");
        fs::create_dir_all(&link_dir).expect("link dir");

        let (probe, created) = super::probe_source_path(&source).expect("probe source");

        assert!(created);
        assert_eq!(probe, source.join(".cross-seed-link-probe-source"));
        assert!(probe.exists());
        assert!(!link_dir.join(".cross-seed-link-probe-source").exists());
        fs::remove_file(&probe).expect("cleanup probe");
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn link_probe_does_not_remove_existing_probe_destination() {
        let root = temp_path("link-probe-collision");
        let source = root.join("source");
        let link_dir = root.join("links");
        let existing_probe = link_dir.join(".cross-seed-link-probe-dest");
        fs::create_dir_all(&source).expect("source dir");
        fs::create_dir_all(&link_dir).expect("link dir");
        fs::write(source.join("episode.mkv"), b"video").expect("source file");
        fs::write(&existing_probe, b"user data").expect("existing probe");

        let compatible =
            super::probe_link_dir(&source, &link_dir, LinkType::Hardlink).expect("probe link dir");

        assert!(compatible);
        assert_eq!(
            fs::read(&existing_probe).expect("existing probe"),
            b"user data"
        );
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn source_files_unchanged_rejects_same_size_modified_file() {
        let root = temp_path("source-mtime");
        let data = root.join("data");
        fs::create_dir_all(&data).expect("data dir");
        fs::write(data.join("source.mkv"), b"video").expect("source file");
        let mut searchee =
            Searchee::from_files("Source", "Source", vec![File::new("source.mkv", 5)]);
        searchee.path = Some(Cow::Owned(data.display().to_string()));
        searchee.mtime_millis = Some(0);

        assert!(!super::source_files_unchanged(&searchee));
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
                match_mode: MatchMode::Partial,
                auto_resume_max_download: 0,
                ignore_non_relevant_files_to_resume: false,
                category: Some(ClientLabel::new("tv")),
                tags: vec![ClientLabel::new("cross-seed")],
                duplicate_categories: false,
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
    fn linked_data_injection_selects_compatible_client() {
        let root = temp_path("inject-compatible-client");
        let data = root.join("data");
        let link_dir = root.join("links");
        let incompatible_downloads = root.join("incompatible-downloads");
        let compatible_downloads = root.join("compatible-downloads");
        fs::create_dir_all(&data).expect("data dir");
        fs::create_dir_all(&link_dir).expect("link dir");
        fs::create_dir_all(&compatible_downloads).expect("downloads");
        fs::write(&incompatible_downloads, b"not a directory").expect("blocked downloads");
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
            "Tracker",
        );
        let incompatible = FakeClient::new("incompatible")
            .with_priority(0)
            .with_download_dir("old", incompatible_downloads);
        let compatible = FakeClient::new("compatible")
            .with_priority(1)
            .with_download_dir("old", compatible_downloads);
        let clients: [&dyn TorrentClient; 2] = [&incompatible, &compatible];

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
                output_dir: None,
                link_dirs: std::slice::from_ref(&link_dir),
                link_type: LinkType::Hardlink,
                flat_linking: false,
                unwrap_symlinks: false,
                skip_recheck: true,
                match_mode: MatchMode::Strict,
                auto_resume_max_download: 0,
                ignore_non_relevant_files_to_resume: false,
                category: None,
                tags: Vec::new(),
                duplicate_categories: false,
            },
            |_| Ok(()),
        )
        .expect("inject action");

        assert_eq!(result, InjectionResult::Injected);
        assert!(incompatible.calls.lock().expect("calls").is_empty());
        assert_eq!(
            compatible.calls.lock().expect("calls").clone(),
            vec!["inject"]
        );
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn recheck_policy_matches_documented_cases() {
        let exact = Metafile::from_files(
            InfoHash::from_validated("0123456789abcdef0123456789abcdef01234567"),
            "Exact.Release",
            "Exact.Release",
            16_384,
            vec![File::new("movie.mkv", 10)],
        );
        let disc = Metafile::from_files(
            InfoHash::from_validated("1111111111111111111111111111111111111111"),
            "Disc.Release",
            "Disc.Release",
            16_384,
            vec![File::new("VIDEO_TS/VTS_01_1.VOB", 10)],
        );

        assert!(super::should_recheck(&exact, Decision::Match, false));
        assert!(!super::should_recheck(&exact, Decision::Match, true));
        assert!(super::should_recheck(&exact, Decision::MatchPartial, true));
        assert!(super::should_recheck(&disc, Decision::Match, true));
    }

    #[test]
    fn partial_resume_waits_when_remaining_exceeds_policy() {
        let root = temp_path("partial-resume-policy");
        let data = root.join("data");
        let link_dir = root.join("links");
        fs::create_dir_all(&data).expect("data dir");
        fs::create_dir_all(&link_dir).expect("link dir");
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
            "Tracker",
        );
        let client = FakeClient::new("client").with_torrent(ClientTorrent {
            info_hash: metafile.info_hash.clone().into_owned(),
            name: Cow::Borrowed("Candidate.Release"),
            files: metafile
                .files
                .iter()
                .cloned()
                .map(File::into_owned)
                .collect(),
            save_path: Cow::Borrowed("/downloads"),
            category: None,
            tags: Vec::new(),
            trackers: Vec::new(),
            complete: false,
            checking: false,
        });
        let clients: [&dyn TorrentClient; 1] = [&client];

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
                output_dir: None,
                link_dirs: std::slice::from_ref(&link_dir),
                link_type: LinkType::Symlink,
                flat_linking: false,
                unwrap_symlinks: false,
                skip_recheck: true,
                match_mode: MatchMode::Partial,
                auto_resume_max_download: 0,
                ignore_non_relevant_files_to_resume: false,
                category: None,
                tags: Vec::new(),
                duplicate_categories: false,
            },
            |_| Ok(()),
        )
        .expect("inject action");

        assert_eq!(result, InjectionResult::Injected);
        assert_eq!(
            client.calls.lock().expect("calls").clone(),
            vec!["inject", "recheck"]
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
                match_mode: MatchMode::Strict,
                auto_resume_max_download: 0,
                ignore_non_relevant_files_to_resume: false,
                category: None,
                tags: Vec::new(),
                duplicate_categories: false,
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
    fn existing_injection_repairs_missing_links_and_rechecks() {
        let root = temp_path("inject-existing-links");
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
        let mut client = FakeClient::new("client");
        client.existing = true;
        let clients: [&dyn TorrentClient; 1] = [&client];

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
                skip_recheck: true,
                match_mode: MatchMode::Partial,
                auto_resume_max_download: 0,
                ignore_non_relevant_files_to_resume: false,
                category: Some(ClientLabel::new("tv")),
                tags: vec![ClientLabel::new("cross-seed")],
                duplicate_categories: false,
            },
            |_| Ok(()),
        )
        .expect("inject action");

        assert_eq!(result, InjectionResult::AlreadyExists);
        assert!(link_dir.join("Tracker_One/Candidate.Release").exists());
        assert_eq!(
            client.calls.lock().expect("calls").clone(),
            vec!["recheck", "resume"]
        );
        assert!(client.last_options.lock().expect("options").is_none());
        assert!(!output_dir.exists());
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
            match_mode: MatchMode::Strict,
            auto_resume_max_download: 0,
            ignore_non_relevant_files_to_resume: false,
            category: None,
            tags: vec![ClientLabel::new("cross-seed")],
            duplicate_categories: false,
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

    #[test]
    fn saved_torrent_injection_deletes_retry_already_in_client() {
        let root = temp_path("saved-inject-existing");
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
        let mut client = FakeClient::new("client");
        client.existing = true;
        let clients: [&dyn TorrentClient; 1] = [&client];
        let blocklist = Blocklist::parse(&[]).expect("blocklist");
        let excluded = BTreeSet::from([metafile.info_hash.as_str().to_owned()]);
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
            match_mode: MatchMode::Strict,
            auto_resume_max_download: 0,
            ignore_non_relevant_files_to_resume: false,
            category: None,
            tags: vec![ClientLabel::new("cross-seed")],
            duplicate_categories: false,
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
        assert_eq!(summary.already_exists, 1);
        assert_eq!(summary.deleted, 1);
        assert_eq!(summary.failed, 0);
        assert!(!saved.path.exists());
        assert!(client.calls.lock().expect("calls").is_empty());
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn saved_torrent_injection_keeps_unrecognized_torrent_files() {
        let root = temp_path("saved-inject-arbitrary");
        let input_dir = root.join("saved");
        let data = root.join("data");
        fs::create_dir_all(&input_dir).expect("input");
        fs::create_dir_all(&data).expect("data");
        fs::write(data.join("Candidate.Release"), b"video-data").expect("source");
        let bytes = torrent_bytes("Candidate.Release", "https://tracker.example/announce", 10);
        let arbitrary = input_dir.join("manual-upload.torrent");
        fs::write(&arbitrary, &bytes).expect("arbitrary torrent");
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
            match_mode: MatchMode::Strict,
            auto_resume_max_download: 0,
            ignore_non_relevant_files_to_resume: false,
            category: None,
            tags: vec![ClientLabel::new("cross-seed")],
            duplicate_categories: false,
        };

        let summary = inject_saved_torrents(
            &SavedInjectionOptions {
                input_dir: &input_dir,
                injection: &injection,
                assessment: &assessment,
                ignore_titles: true,
            },
            &[searchee],
            |_| Ok(()),
        )
        .expect("inject saved");

        assert_eq!(summary.scanned, 1);
        assert_eq!(summary.injected, 0);
        assert_eq!(summary.deleted, 0);
        assert_eq!(summary.failed, 1);
        assert!(arbitrary.exists());
        assert!(client.calls.lock().expect("calls").is_empty());
        let _cleanup = fs::remove_dir_all(root);
    }

    #[test]
    fn saved_match_accepts_alternate_title_similarity() {
        let metadata = saved_metadata("Foreign Title");
        let metafile = saved_metafile("Foreign Title", vec![File::new("episode.mkv", 10)]);
        let searchee = Searchee::from_files(
            "Example Show (Foreign Title)",
            "Example Show (Foreign Title)",
            vec![File::new("episode.mkv", 10)],
        );
        let clients: [&dyn TorrentClient; 0] = [];
        let injection = test_injection_options(&clients);
        let blocklist = Blocklist::parse(&[]).expect("blocklist");
        let excluded = BTreeSet::new();
        let assessment = test_assessment_options(&blocklist, &excluded, MatchMode::Strict);
        let input_dir = PathBuf::from(".");
        let searchees = [searchee];

        let matched = best_saved_match(
            &metafile,
            &metadata,
            &searchees,
            &SavedInjectionOptions {
                input_dir: &input_dir,
                injection: &injection,
                assessment: &assessment,
                ignore_titles: false,
            },
        );

        assert!(matched.is_some());
    }

    #[test]
    fn saved_match_filters_blocklisted_searchees() {
        let metadata = saved_metadata("Candidate Release");
        let metafile = saved_metafile("Candidate Release", vec![File::new("candidate.mkv", 10)]);
        let blocked = Searchee::from_files(
            "Blocked Candidate Release",
            "Candidate Release",
            vec![File::new("candidate.mkv", 10)],
        );
        let allowed = Searchee::from_files(
            "Candidate Release",
            "Candidate Release",
            vec![File::new("candidate.mkv", 10)],
        );
        let clients: [&dyn TorrentClient; 0] = [];
        let injection = test_injection_options(&clients);
        let blocklist = Blocklist::parse(&["name:blocked".to_owned()]).expect("blocklist");
        let excluded = BTreeSet::new();
        let assessment = test_assessment_options(&blocklist, &excluded, MatchMode::Strict);
        let input_dir = PathBuf::from(".");
        let searchees = [blocked, allowed];

        let (matched, decision) = best_saved_match(
            &metafile,
            &metadata,
            &searchees,
            &SavedInjectionOptions {
                input_dir: &input_dir,
                injection: &injection,
                assessment: &assessment,
                ignore_titles: false,
            },
        )
        .expect("saved match");

        assert_eq!(matched.name, "Candidate Release");
        assert_eq!(decision, Decision::Match);
    }

    #[test]
    fn saved_match_sorts_by_source_and_file_count() {
        let metadata = saved_metadata("Candidate Release");
        let metafile = saved_metafile("Candidate Release", vec![File::new("candidate.mkv", 10)]);
        let mut data = Searchee::from_files(
            "Candidate Release",
            "Candidate Release",
            vec![
                File::new("candidate.mkv", 10),
                File::new("extra-feature.mkv", 5),
            ],
        );
        data.path = Some(Cow::Borrowed("/data/Candidate Release"));
        let mut torrent = Searchee::from_files(
            "Candidate Release",
            "Candidate Release",
            vec![File::new("candidate.mkv", 10)],
        );
        torrent.info_hash = Some(InfoHash::from_validated(
            "2222222222222222222222222222222222222222",
        ));
        let clients: [&dyn TorrentClient; 0] = [];
        let injection = test_injection_options(&clients);
        let blocklist = Blocklist::parse(&[]).expect("blocklist");
        let excluded = BTreeSet::new();
        let assessment = test_assessment_options(&blocklist, &excluded, MatchMode::Strict);
        let input_dir = PathBuf::from(".");
        let searchees = [data, torrent];

        let (matched, _) = best_saved_match(
            &metafile,
            &metadata,
            &searchees,
            &SavedInjectionOptions {
                input_dir: &input_dir,
                injection: &injection,
                assessment: &assessment,
                ignore_titles: false,
            },
        )
        .expect("saved match");

        assert!(matched.info_hash.is_some());

        let mut first = Searchee::from_files(
            "Candidate Release",
            "Candidate Release",
            vec![File::new("candidate.mkv", 10)],
        );
        first.path = Some(Cow::Borrowed("/data/one"));
        let mut more_files = Searchee::from_files(
            "Candidate Release",
            "Candidate Release",
            vec![
                File::new("candidate.mkv", 10),
                File::new("extra-feature.mkv", 5),
            ],
        );
        more_files.path = Some(Cow::Borrowed("/data/two"));
        let searchees = [first, more_files];

        let (matched, _) = best_saved_match(
            &metafile,
            &metadata,
            &searchees,
            &SavedInjectionOptions {
                input_dir: &input_dir,
                injection: &injection,
                assessment: &assessment,
                ignore_titles: false,
            },
        )
        .expect("saved match");

        assert_eq!(matched.files.len(), 2);
    }

    fn saved_metadata(name: &str) -> SavedTorrentMetadata<'static> {
        SavedTorrentMetadata::new(
            MediaType::Video,
            "Tracker",
            name.to_owned(),
            InfoHash::from_validated("1111111111111111111111111111111111111111"),
            false,
        )
    }

    fn saved_metafile(name: &str, files: Vec<File<'static>>) -> Metafile<'static> {
        Metafile::from_files(
            InfoHash::from_validated("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            name.to_owned(),
            name.to_owned(),
            1,
            files,
        )
    }

    fn test_assessment_options<'a>(
        blocklist: &'a Blocklist,
        excluded: &'a BTreeSet<String>,
        match_mode: MatchMode,
    ) -> AssessmentOptions<'a> {
        AssessmentOptions {
            match_mode,
            fuzzy_size_threshold: 0.05,
            season_from_episodes: 0.75,
            include_single_episodes: true,
            info_hashes_to_exclude: excluded,
            blocklist,
        }
    }

    fn test_injection_options<'a>(
        clients: &'a [&'a dyn TorrentClient],
    ) -> InjectionActionOptions<'a> {
        InjectionActionOptions {
            clients,
            output_dir: None,
            link_dirs: &[],
            link_type: LinkType::Symlink,
            flat_linking: false,
            unwrap_symlinks: false,
            skip_recheck: true,
            match_mode: MatchMode::Strict,
            auto_resume_max_download: 0,
            ignore_non_relevant_files_to_resume: false,
            category: None,
            tags: Vec::new(),
            duplicate_categories: false,
        }
    }

    struct FakeClient {
        metadata: TorrentClientMetadata<'static>,
        existing: bool,
        download_dir: Result<PathBuf, ClientErrorCode>,
        all_download_dirs: BTreeMap<String, PathBuf>,
        all_torrents: Vec<ClientTorrent<'static>>,
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
                all_download_dirs: BTreeMap::new(),
                all_torrents: Vec::new(),
                calls: Mutex::new(Vec::new()),
                last_options: Mutex::new(None),
            }
        }

        fn with_priority(mut self, priority: u16) -> Self {
            self.metadata.priority = priority;
            self
        }

        fn with_download_dir(mut self, info_hash: &str, path: PathBuf) -> Self {
            self.all_download_dirs.insert(info_hash.to_owned(), path);
            self
        }

        fn with_torrent(mut self, torrent: ClientTorrent<'static>) -> Self {
            self.all_torrents.push(torrent);
            self
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
            Ok(self.all_torrents.clone())
        }

        fn get_download_dir(
            &self,
            _metafile: &Metafile<'_>,
            _options: DownloadDirOptions,
        ) -> crate::Result<Result<PathBuf, ClientErrorCode>> {
            Ok(self.download_dir.clone())
        }

        fn get_all_download_dirs(&self) -> crate::Result<BTreeMap<String, PathBuf>> {
            Ok(self.all_download_dirs.clone())
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
            metafile: &Metafile<'_>,
            _decision: Decision,
            options: ResumeOptions,
        ) -> crate::Result<()> {
            let remaining = self
                .all_torrents
                .iter()
                .find(|torrent| torrent.info_hash == metafile.info_hash)
                .map(|torrent| if torrent.complete { 0 } else { metafile.length })
                .unwrap_or(0);
            if remaining > options.max_remaining_bytes {
                return Ok(());
            }
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
