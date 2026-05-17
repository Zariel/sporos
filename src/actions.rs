#![expect(
    clippy::indexing_slicing,
    clippy::let_underscore_must_use,
    reason = "mechanical clippy gate enablement leaves existing action safety cleanup to linked lint-class beads"
)]
#![cfg_attr(
    test,
    expect(
        clippy::cloned_ref_to_slice_refs,
        reason = "test fixture cleanup is lower risk than the production clippy gate fix"
    )
)]

use std::error::Error;
use std::fmt;
use std::fs::{self, File, FileTimes, OpenOptions};
use std::io::{self, Write};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use crate::domain::{
    ByteSize, LocalFile, MatchDecision, MediaType, RemoteCandidate, TorrentFile, TorrentMetafile,
};
use crate::metrics::ActionOutcome;
use crate::persistence::torrent_cache::{
    TorrentCachePathError, TorrentOutputMetadata, torrent_output_path,
};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
const DEFAULT_LINK_SCAN_LIMIT: usize = 10_000;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LinkType {
    Hardlink,
    Symlink,
    Reflink,
    ReflinkOrCopy,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LinkDirOptions {
    pub link_type: LinkType,
    pub max_directory_entries: usize,
}

impl LinkDirOptions {
    pub const fn new(link_type: LinkType) -> Self {
        Self {
            link_type,
            max_directory_entries: DEFAULT_LINK_SCAN_LIMIT,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LinkFilesOptions {
    pub link_type: LinkType,
    pub ignore_missing: bool,
}

impl LinkFilesOptions {
    pub const fn new(link_type: LinkType) -> Self {
        Self {
            link_type,
            ignore_missing: false,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CreatedLink {
    pub source: PathBuf,
    pub destination: PathBuf,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LinkFilesOutcome {
    pub created_links: Vec<CreatedLink>,
    pub created_roots: Vec<PathBuf>,
    pub missing_sources: Vec<PathBuf>,
    pub already_existing: bool,
}

impl LinkFilesOutcome {
    pub fn is_empty(&self) -> bool {
        self.created_links.is_empty()
            && self.created_roots.is_empty()
            && self.missing_sources.is_empty()
            && !self.already_existing
    }
}

#[derive(Debug)]
pub enum LinkActionError {
    EmptyLinkDirs,
    InvalidLinkDir {
        path: PathBuf,
    },
    InvalidSourcePath {
        path: PathBuf,
    },
    InvalidDestinationPath {
        destination_dir: PathBuf,
        relative_path: PathBuf,
    },
    UnsafeComponent {
        field: &'static str,
        value: String,
    },
    NoCompatibleLinkDir {
        source: PathBuf,
    },
    ConflictingVirtualLinkDirs {
        first: PathBuf,
        other: PathBuf,
    },
    MissingSource {
        path: PathBuf,
    },
    NoSourceMatch {
        candidate: PathBuf,
        size: ByteSize,
    },
    Io {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
}

impl fmt::Display for LinkActionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyLinkDirs => formatter.write_str("no link directories are configured"),
            Self::InvalidLinkDir { path } => {
                write!(
                    formatter,
                    "link directory is not a directory: {}",
                    path.display()
                )
            }
            Self::InvalidSourcePath { path } => {
                write!(
                    formatter,
                    "source path is not a file or directory: {}",
                    path.display()
                )
            }
            Self::InvalidDestinationPath {
                destination_dir,
                relative_path,
            } => write!(
                formatter,
                "destination path {} would escape {}",
                relative_path.display(),
                destination_dir.display()
            ),
            Self::UnsafeComponent { field, value } => {
                write!(formatter, "unsafe {field} component: {value}")
            }
            Self::NoCompatibleLinkDir { source } => {
                write!(
                    formatter,
                    "no compatible link directory for {}",
                    source.display()
                )
            }
            Self::ConflictingVirtualLinkDirs { first, other } => write!(
                formatter,
                "virtual source files resolve to different link directories: {} and {}",
                first.display(),
                other.display()
            ),
            Self::MissingSource { path } => {
                write!(formatter, "source file is missing: {}", path.display())
            }
            Self::NoSourceMatch { candidate, size } => write!(
                formatter,
                "no source file matches candidate {} with size {}",
                candidate.display(),
                size.get()
            ),
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "failed to {operation} {}: {source}",
                path.display()
            ),
        }
    }
}

impl Error for LinkActionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::EmptyLinkDirs
            | Self::InvalidLinkDir { .. }
            | Self::InvalidSourcePath { .. }
            | Self::InvalidDestinationPath { .. }
            | Self::UnsafeComponent { .. }
            | Self::NoCompatibleLinkDir { .. }
            | Self::ConflictingVirtualLinkDirs { .. }
            | Self::MissingSource { .. }
            | Self::NoSourceMatch { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SaveTorrentOutcome {
    Saved { path: PathBuf },
    AlreadyExisting { path: PathBuf },
}

impl SaveTorrentOutcome {
    pub fn path(&self) -> &Path {
        match self {
            Self::Saved { path } | Self::AlreadyExisting { path } => path,
        }
    }

    pub const fn action_outcome(&self) -> ActionOutcome {
        match self {
            Self::Saved { .. } => ActionOutcome::Saved,
            Self::AlreadyExisting { .. } => ActionOutcome::AlreadyExisting,
        }
    }
}

#[derive(Debug)]
pub enum SaveTorrentError {
    InvalidOutputPath {
        output_dir: PathBuf,
        path: PathBuf,
    },
    InvalidMetadata(TorrentCachePathError),
    ExistingPathNotFile {
        path: PathBuf,
    },
    Io {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
}

impl fmt::Display for SaveTorrentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOutputPath { output_dir, path } => write!(
                formatter,
                "torrent output path {} is not directly under configured output directory {}",
                path.display(),
                output_dir.display()
            ),
            Self::InvalidMetadata(error) => {
                write!(formatter, "invalid torrent output metadata: {error}")
            }
            Self::ExistingPathNotFile { path } => {
                write!(
                    formatter,
                    "torrent output path is not a regular file: {}",
                    path.display()
                )
            }
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "failed to {operation} {}: {source}",
                path.display()
            ),
        }
    }
}

impl Error for SaveTorrentError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidMetadata(error) => Some(error),
            Self::Io { source, .. } => Some(source),
            Self::InvalidOutputPath { .. } | Self::ExistingPathNotFile { .. } => None,
        }
    }
}

pub fn candidate_output_metadata(
    media_type: MediaType,
    candidate: &RemoteCandidate,
    metafile: &TorrentMetafile,
) -> TorrentOutputMetadata {
    TorrentOutputMetadata {
        media_type,
        tracker: candidate.tracker.as_str().to_owned(),
        name: candidate.title.as_str().to_owned(),
        info_hash: metafile.info_hash().clone(),
        cached: false,
    }
}

pub fn save_candidate_torrent(
    output_dir: &Path,
    metadata: &TorrentOutputMetadata,
    torrent_bytes: &[u8],
) -> Result<SaveTorrentOutcome, SaveTorrentError> {
    let path =
        torrent_output_path(output_dir, metadata).map_err(SaveTorrentError::InvalidMetadata)?;
    ensure_output_child(output_dir, &path)?;
    create_output_dir(output_dir)?;

    match existing_file_status(&path)? {
        ExistingFileStatus::Regular => {
            touch_existing_file(&path)?;
            Ok(SaveTorrentOutcome::AlreadyExisting { path })
        }
        ExistingFileStatus::NotFile => Err(SaveTorrentError::ExistingPathNotFile { path }),
        ExistingFileStatus::Missing => write_new_file(&path, torrent_bytes),
    }
}

pub fn select_link_dir(
    source_path: &Path,
    link_dirs: &[PathBuf],
    options: LinkDirOptions,
) -> Result<PathBuf, LinkActionError> {
    if link_dirs.is_empty() {
        return Err(LinkActionError::EmptyLinkDirs);
    }
    validate_link_dirs(link_dirs)?;

    if let Some(selected) = select_link_dir_by_device(source_path, link_dirs)? {
        return Ok(selected);
    }

    let representative = representative_source_file(source_path, options.max_directory_entries)?;
    for link_dir in link_dirs {
        if test_link_compatibility(&representative.path, link_dir, options.link_type)? {
            representative.cleanup()?;
            return Ok(link_dir.clone());
        }
    }
    representative.cleanup()?;

    if options.link_type == LinkType::Symlink {
        tracing::warn!(
            source = %source_path.display(),
            link_dir = %link_dirs[0].display(),
            "using first symlink directory after compatibility tests failed"
        );
        return Ok(link_dirs[0].clone());
    }

    Err(LinkActionError::NoCompatibleLinkDir {
        source: source_path.to_path_buf(),
    })
}

pub fn select_virtual_link_dir(
    source_files: &[PathBuf],
    link_dirs: &[PathBuf],
    options: LinkDirOptions,
) -> Result<PathBuf, LinkActionError> {
    let mut selected: Option<PathBuf> = None;
    for source_file in source_files {
        let link_dir = select_link_dir(source_file, link_dirs, options.clone())?;
        if let Some(first) = &selected {
            if first != &link_dir {
                return Err(LinkActionError::ConflictingVirtualLinkDirs {
                    first: first.clone(),
                    other: link_dir,
                });
            }
        } else {
            selected = Some(link_dir);
        }
    }
    selected.ok_or(LinkActionError::InvalidSourcePath {
        path: PathBuf::new(),
    })
}

pub fn link_destination_dir(
    link_dir: &Path,
    tracker: &str,
    flat_linking: bool,
) -> Result<PathBuf, LinkActionError> {
    if flat_linking {
        Ok(link_dir.to_path_buf())
    } else {
        Ok(link_dir.join(safe_directory_component("tracker", tracker)?))
    }
}

pub fn link_metafile_files(
    source_root: &Path,
    local_files: &[LocalFile],
    candidate_files: &[TorrentFile],
    decision: MatchDecision,
    destination_dir: &Path,
    options: LinkFilesOptions,
) -> Result<LinkFilesOutcome, LinkActionError> {
    let pairs = pair_link_files(source_root, local_files, candidate_files, decision)?;
    let mut outcome = LinkFilesOutcome {
        created_links: Vec::new(),
        created_roots: Vec::new(),
        missing_sources: Vec::new(),
        already_existing: false,
    };

    for pair in pairs {
        let destination = safe_destination_path(destination_dir, &pair.destination_relative_path)?;
        if existing_file_status_for_link(&destination)? {
            outcome.already_existing = true;
            continue;
        }
        match fs::symlink_metadata(&pair.source) {
            Ok(metadata) if metadata.file_type().is_file() => {}
            Ok(_) => {
                return Err(LinkActionError::InvalidSourcePath { path: pair.source });
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound && options.ignore_missing => {
                outcome.missing_sources.push(pair.source);
                continue;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(LinkActionError::MissingSource { path: pair.source });
            }
            Err(source) => {
                return Err(LinkActionError::Io {
                    operation: "inspect source file",
                    path: pair.source,
                    source,
                });
            }
        }

        let root = destination_root(destination_dir, &pair.destination_relative_path)?;
        let root_preexisted = root_exists(&root)?;
        let parent =
            destination
                .parent()
                .ok_or_else(|| LinkActionError::InvalidDestinationPath {
                    destination_dir: destination_dir.to_path_buf(),
                    relative_path: pair.destination_relative_path.clone(),
                })?;
        fs::create_dir_all(parent).map_err(|source| LinkActionError::Io {
            operation: "create link destination directory",
            path: parent.to_path_buf(),
            source,
        })?;

        create_link(&pair.source, &destination, options.link_type)?;
        outcome.created_links.push(CreatedLink {
            source: pair.source,
            destination,
        });
        if !root_preexisted && !outcome.created_roots.contains(&root) {
            outcome.created_roots.push(root);
        }
    }

    Ok(outcome)
}

pub fn cleanup_created_roots(roots: &[PathBuf]) -> Result<(), LinkActionError> {
    for root in roots {
        let Ok(metadata) = fs::symlink_metadata(root) else {
            continue;
        };
        if metadata.file_type().is_dir() {
            fs::remove_dir_all(root).map_err(|source| LinkActionError::Io {
                operation: "remove created link root",
                path: root.clone(),
                source,
            })?;
        } else {
            fs::remove_file(root).map_err(|source| LinkActionError::Io {
                operation: "remove created link root",
                path: root.clone(),
                source,
            })?;
        }
    }
    Ok(())
}

#[derive(Debug)]
struct LinkPair {
    source: PathBuf,
    destination_relative_path: PathBuf,
}

#[derive(Debug)]
struct RepresentativeSourceFile {
    path: PathBuf,
    temporary: bool,
}

impl RepresentativeSourceFile {
    fn cleanup(&self) -> Result<(), LinkActionError> {
        if self.temporary {
            remove_test_file(&self.path)?;
        }
        Ok(())
    }
}

fn validate_link_dirs(link_dirs: &[PathBuf]) -> Result<(), LinkActionError> {
    for link_dir in link_dirs {
        let metadata = fs::symlink_metadata(link_dir).map_err(|source| LinkActionError::Io {
            operation: "inspect link directory",
            path: link_dir.clone(),
            source,
        })?;
        if !metadata.file_type().is_dir() {
            return Err(LinkActionError::InvalidLinkDir {
                path: link_dir.clone(),
            });
        }
    }
    Ok(())
}

fn select_link_dir_by_device(
    source_path: &Path,
    link_dirs: &[PathBuf],
) -> Result<Option<PathBuf>, LinkActionError> {
    let Some(source_device) = device_id(source_path)? else {
        return Ok(None);
    };

    let mut devices = Vec::with_capacity(link_dirs.len());
    for link_dir in link_dirs {
        let Some(device) = device_id(link_dir)? else {
            return Ok(None);
        };
        if devices.contains(&device) {
            return Ok(None);
        }
        devices.push(device);
    }

    Ok(link_dirs
        .iter()
        .zip(devices)
        .find_map(|(link_dir, device)| (device == source_device).then(|| link_dir.clone())))
}

#[cfg(unix)]
fn device_id(path: &Path) -> Result<Option<u64>, LinkActionError> {
    fs::metadata(path)
        .map(|metadata| Some(metadata.dev()))
        .map_err(|source| LinkActionError::Io {
            operation: "inspect filesystem device",
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(not(unix))]
fn device_id(_path: &Path) -> Result<Option<u64>, LinkActionError> {
    Ok(None)
}

fn representative_source_file(
    source_path: &Path,
    max_directory_entries: usize,
) -> Result<RepresentativeSourceFile, LinkActionError> {
    let metadata = fs::symlink_metadata(source_path).map_err(|source| LinkActionError::Io {
        operation: "inspect source path",
        path: source_path.to_path_buf(),
        source,
    })?;
    if metadata.file_type().is_file() {
        return Ok(RepresentativeSourceFile {
            path: source_path.to_path_buf(),
            temporary: false,
        });
    }
    if !metadata.file_type().is_dir() {
        return Err(LinkActionError::InvalidSourcePath {
            path: source_path.to_path_buf(),
        });
    }

    if let Some(path) = find_representative_file(source_path, max_directory_entries)? {
        return Ok(RepresentativeSourceFile {
            path,
            temporary: false,
        });
    }

    let path = source_path.join(format!(
        ".sporos-link-source-{}-{}",
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    fs::write(&path, b"sporos link compatibility probe").map_err(|source| LinkActionError::Io {
        operation: "create temporary source probe",
        path: path.clone(),
        source,
    })?;
    Ok(RepresentativeSourceFile {
        path,
        temporary: true,
    })
}

fn find_representative_file(
    source_dir: &Path,
    max_directory_entries: usize,
) -> Result<Option<PathBuf>, LinkActionError> {
    let mut pending = vec![source_dir.to_path_buf()];
    let mut visited = 0usize;
    while let Some(dir) = pending.pop() {
        let entries = fs::read_dir(&dir).map_err(|source| LinkActionError::Io {
            operation: "read source directory",
            path: dir.clone(),
            source,
        })?;
        for entry in entries {
            visited = visited.saturating_add(1);
            if visited > max_directory_entries {
                return Ok(None);
            }
            let entry = entry.map_err(|source| LinkActionError::Io {
                operation: "read source directory entry",
                path: dir.clone(),
                source,
            })?;
            let path = entry.path();
            let metadata = entry.metadata().map_err(|source| LinkActionError::Io {
                operation: "inspect source directory entry",
                path: path.clone(),
                source,
            })?;
            if metadata.file_type().is_file() {
                return Ok(Some(path));
            }
            if metadata.file_type().is_dir() {
                pending.push(path);
            }
        }
    }
    Ok(None)
}

fn test_link_compatibility(
    source_file: &Path,
    link_dir: &Path,
    link_type: LinkType,
) -> Result<bool, LinkActionError> {
    let destination = link_dir.join(format!(
        ".sporos-link-test-{}-{}",
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let result = match link_type {
        LinkType::Hardlink | LinkType::Symlink => fs::hard_link(source_file, &destination),
        LinkType::Reflink => reflink_copy::reflink(source_file, &destination),
        LinkType::ReflinkOrCopy => {
            reflink_copy::reflink_or_copy(source_file, &destination).map(|_| ())
        }
    };
    match result {
        Ok(()) => {
            remove_test_file(&destination)?;
            Ok(true)
        }
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::CrossesDevices
                    | io::ErrorKind::Unsupported
                    | io::ErrorKind::PermissionDenied
                    | io::ErrorKind::InvalidInput
                    | io::ErrorKind::Other
            ) =>
        {
            let _ = remove_test_file(&destination);
            Ok(false)
        }
        Err(source) => Err(LinkActionError::Io {
            operation: "test link compatibility",
            path: destination,
            source,
        }),
    }
}

fn pair_link_files(
    source_root: &Path,
    local_files: &[LocalFile],
    candidate_files: &[TorrentFile],
    decision: MatchDecision,
) -> Result<Vec<LinkPair>, LinkActionError> {
    if decision == MatchDecision::Exact {
        return candidate_files
            .iter()
            .map(|candidate| {
                validate_relative_path(&candidate.relative_path)?;
                Ok(LinkPair {
                    source: source_root.join(&candidate.relative_path),
                    destination_relative_path: candidate.relative_path.clone(),
                })
            })
            .collect();
    }

    let mut used = vec![false; local_files.len()];
    let mut pairs = Vec::with_capacity(candidate_files.len());
    for candidate in candidate_files {
        validate_relative_path(&candidate.relative_path)?;
        let index = best_source_match(local_files, &used, candidate).ok_or_else(|| {
            LinkActionError::NoSourceMatch {
                candidate: candidate.relative_path.clone(),
                size: candidate.size,
            }
        })?;
        used[index] = true;
        let source = &local_files[index];
        validate_relative_path(&source.relative_path)?;
        pairs.push(LinkPair {
            source: source_root.join(&source.relative_path),
            destination_relative_path: candidate.relative_path.clone(),
        });
    }
    Ok(pairs)
}

fn best_source_match(
    local_files: &[LocalFile],
    used: &[bool],
    candidate: &TorrentFile,
) -> Option<usize> {
    local_files
        .iter()
        .enumerate()
        .filter(|(index, file)| !used[*index] && file.size == candidate.size)
        .min_by_key(|(_, file)| {
            let same_name = file.file_name.as_str() == candidate.file_name.as_str();
            (!same_name, file.relative_path.as_path())
        })
        .map(|(index, _)| index)
}

fn existing_file_status_for_link(destination: &Path) -> Result<bool, LinkActionError> {
    match fs::symlink_metadata(destination) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(LinkActionError::Io {
            operation: "inspect link destination",
            path: destination.to_path_buf(),
            source,
        }),
    }
}

fn create_link(
    source: &Path,
    destination: &Path,
    link_type: LinkType,
) -> Result<(), LinkActionError> {
    let result = match link_type {
        LinkType::Hardlink => fs::hard_link(source, destination),
        LinkType::Symlink => symlink_file(source, destination),
        LinkType::Reflink => reflink_copy::reflink(source, destination),
        LinkType::ReflinkOrCopy => reflink_copy::reflink_or_copy(source, destination).map(|_| ()),
    };
    result.map_err(|source| LinkActionError::Io {
        operation: "create linked file",
        path: destination.to_path_buf(),
        source,
    })
}

#[cfg(unix)]
fn symlink_file(source: &Path, destination: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(source, destination)
}

#[cfg(windows)]
fn symlink_file(source: &Path, destination: &Path) -> io::Result<()> {
    std::os::windows::fs::symlink_file(source, destination)
}

fn safe_destination_path(
    destination_dir: &Path,
    relative_path: &Path,
) -> Result<PathBuf, LinkActionError> {
    validate_relative_path(relative_path)?;
    let destination = destination_dir.join(relative_path);
    if destination.starts_with(destination_dir) {
        Ok(destination)
    } else {
        Err(LinkActionError::InvalidDestinationPath {
            destination_dir: destination_dir.to_path_buf(),
            relative_path: relative_path.to_path_buf(),
        })
    }
}

fn destination_root(
    destination_dir: &Path,
    relative_path: &Path,
) -> Result<PathBuf, LinkActionError> {
    validate_relative_path(relative_path)?;
    let mut components = relative_path.components();
    let Some(first) = components.next() else {
        return Err(LinkActionError::InvalidDestinationPath {
            destination_dir: destination_dir.to_path_buf(),
            relative_path: relative_path.to_path_buf(),
        });
    };
    Ok(destination_dir.join(first.as_os_str()))
}

fn root_exists(root: &Path) -> Result<bool, LinkActionError> {
    match fs::symlink_metadata(root) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(LinkActionError::Io {
            operation: "inspect link root",
            path: root.to_path_buf(),
            source,
        }),
    }
}

fn validate_relative_path(path: &Path) -> Result<(), LinkActionError> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Err(LinkActionError::InvalidDestinationPath {
            destination_dir: PathBuf::new(),
            relative_path: path.to_path_buf(),
        });
    }
    if path
        .components()
        .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(LinkActionError::InvalidDestinationPath {
            destination_dir: PathBuf::new(),
            relative_path: path.to_path_buf(),
        });
    }
    Ok(())
}

fn safe_directory_component(field: &'static str, value: &str) -> Result<String, LinkActionError> {
    if value.contains("..") {
        return Err(LinkActionError::UnsafeComponent {
            field,
            value: value.to_owned(),
        });
    }

    let mut sanitized = String::with_capacity(value.len());
    for character in value.trim().chars() {
        if character.is_control()
            || matches!(
                character,
                '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|'
            )
        {
            sanitized.push('_');
        } else {
            sanitized.push(character);
        }
    }
    let sanitized = sanitized.trim_matches(|character| matches!(character, '.' | ' ' | '_'));
    if sanitized.is_empty() || sanitized.contains("..") {
        return Err(LinkActionError::UnsafeComponent {
            field,
            value: value.to_owned(),
        });
    }
    Ok(sanitized.to_owned())
}

fn remove_test_file(path: &Path) -> Result<(), LinkActionError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(LinkActionError::Io {
            operation: "remove link probe",
            path: path.to_path_buf(),
            source,
        }),
    }
}

enum ExistingFileStatus {
    Missing,
    Regular,
    NotFile,
}

fn ensure_output_child(output_dir: &Path, path: &Path) -> Result<(), SaveTorrentError> {
    if path.parent() == Some(output_dir) && path.file_name().is_some() {
        Ok(())
    } else {
        Err(SaveTorrentError::InvalidOutputPath {
            output_dir: output_dir.to_path_buf(),
            path: path.to_path_buf(),
        })
    }
}

fn create_output_dir(output_dir: &Path) -> Result<(), SaveTorrentError> {
    fs::create_dir_all(output_dir).map_err(|source| SaveTorrentError::Io {
        operation: "create output directory",
        path: output_dir.to_path_buf(),
        source,
    })
}

fn existing_file_status(path: &Path) -> Result<ExistingFileStatus, SaveTorrentError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_file() {
                Ok(ExistingFileStatus::Regular)
            } else {
                Ok(ExistingFileStatus::NotFile)
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(ExistingFileStatus::Missing),
        Err(source) => Err(SaveTorrentError::Io {
            operation: "inspect torrent output path",
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn touch_existing_file(path: &Path) -> Result<(), SaveTorrentError> {
    let file = File::open(path).map_err(|source| SaveTorrentError::Io {
        operation: "open existing torrent output",
        path: path.to_path_buf(),
        source,
    })?;
    file.set_times(FileTimes::new().set_accessed(SystemTime::now()))
        .map_err(|source| SaveTorrentError::Io {
            operation: "update existing torrent output access time",
            path: path.to_path_buf(),
            source,
        })
}

fn write_new_file(
    path: &Path,
    torrent_bytes: &[u8],
) -> Result<SaveTorrentOutcome, SaveTorrentError> {
    let temporary = create_temporary_file(path, torrent_bytes)?;
    match fs::hard_link(&temporary, path) {
        Ok(()) => {
            remove_temporary_file(&temporary)?;
            Ok(SaveTorrentOutcome::Saved {
                path: path.to_path_buf(),
            })
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            remove_temporary_file(&temporary)?;
            touch_existing_file(path)?;
            Ok(SaveTorrentOutcome::AlreadyExisting {
                path: path.to_path_buf(),
            })
        }
        Err(source) => {
            let cleanup = remove_temporary_file(&temporary);
            if cleanup.is_err() {
                tracing::warn!(
                    path = %temporary.display(),
                    "failed to remove temporary torrent output after link failure"
                );
            }
            Err(SaveTorrentError::Io {
                operation: "install torrent output",
                path: path.to_path_buf(),
                source,
            })
        }
    }
}

fn create_temporary_file(path: &Path, torrent_bytes: &[u8]) -> Result<PathBuf, SaveTorrentError> {
    let parent = path
        .parent()
        .ok_or_else(|| SaveTorrentError::InvalidOutputPath {
            output_dir: PathBuf::new(),
            path: path.to_path_buf(),
        })?;
    for _ in 0..16 {
        let temporary = temporary_path(parent, path);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(mut file) => {
                write_temporary_file(&mut file, &temporary, torrent_bytes)?;
                return Ok(temporary);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(SaveTorrentError::Io {
                    operation: "create temporary torrent output",
                    path: temporary,
                    source,
                });
            }
        }
    }

    Err(SaveTorrentError::Io {
        operation: "create unique temporary torrent output",
        path: path.to_path_buf(),
        source: io::Error::new(
            io::ErrorKind::AlreadyExists,
            "temporary torrent output name collision",
        ),
    })
}

fn write_temporary_file(
    file: &mut File,
    temporary: &Path,
    torrent_bytes: &[u8],
) -> Result<(), SaveTorrentError> {
    if let Err(source) = file.write_all(torrent_bytes) {
        let cleanup = remove_temporary_file(temporary);
        if cleanup.is_err() {
            tracing::warn!(
                path = %temporary.display(),
                "failed to remove temporary torrent output after write failure"
            );
        }
        return Err(SaveTorrentError::Io {
            operation: "write temporary torrent output",
            path: temporary.to_path_buf(),
            source,
        });
    }
    file.sync_all().map_err(|source| SaveTorrentError::Io {
        operation: "sync temporary torrent output",
        path: temporary.to_path_buf(),
        source,
    })
}

fn temporary_path(parent: &Path, path: &Path) -> PathBuf {
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("torrent-output");
    parent.join(format!(
        ".{file_name}.sporos-tmp-{}-{counter}",
        std::process::id()
    ))
}

fn remove_temporary_file(path: &Path) -> Result<(), SaveTorrentError> {
    fs::remove_file(path).map_err(|source| SaveTorrentError::Io {
        operation: "remove temporary torrent output",
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::domain::{
        ByteSize, CandidateGuid, DisplayName, DownloadUrl, FileIndex, IndexerId, InfoHash,
        ItemTitle, LocalFile, TorrentFile, TrackerName,
    };

    const SHA1: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn save_action_writes_torrent_atomically_under_output_dir() {
        let output_dir = unique_temp_dir("save-action");
        let metadata = test_metadata();

        let outcome = save_candidate_torrent(&output_dir, &metadata, b"torrent bytes").unwrap();

        assert!(matches!(outcome, SaveTorrentOutcome::Saved { .. }));
        assert_eq!(
            b"torrent bytes",
            fs::read(outcome.path()).unwrap().as_slice()
        );
        assert_eq!(Some(output_dir.as_path()), outcome.path().parent());
        assert!(fs::read_dir(&output_dir).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("sporos-tmp")
        }));

        remove_temp_dir(&output_dir);
    }

    #[test]
    fn save_action_touches_existing_safe_file_without_rewriting() {
        let output_dir = unique_temp_dir("save-action-existing");
        let metadata = test_metadata();
        let path = torrent_output_path(&output_dir, &metadata).unwrap();
        fs::create_dir_all(&output_dir).unwrap();
        fs::write(&path, b"existing bytes").unwrap();
        let modified_before = fs::metadata(&path).unwrap().modified().unwrap();

        let outcome = save_candidate_torrent(&output_dir, &metadata, b"new bytes").unwrap();

        assert_eq!(
            SaveTorrentOutcome::AlreadyExisting { path: path.clone() },
            outcome
        );
        assert_eq!(b"existing bytes", fs::read(&path).unwrap().as_slice());
        assert_eq!(
            modified_before,
            fs::metadata(&path).unwrap().modified().unwrap()
        );

        remove_temp_dir(&output_dir);
    }

    #[test]
    fn save_action_rejects_unsafe_metadata_before_writing() {
        let output_dir = unique_temp_dir("save-action-unsafe");
        let mut metadata = test_metadata();
        metadata.name = "../outside".to_owned();

        let error = save_candidate_torrent(&output_dir, &metadata, b"torrent bytes").unwrap_err();

        assert!(matches!(error, SaveTorrentError::InvalidMetadata(_)));
        assert!(!output_dir.exists());

        remove_temp_dir(&output_dir);
    }

    #[test]
    fn save_action_metadata_uses_candidate_and_metafile_fields() {
        let candidate = RemoteCandidate {
            id: None,
            indexer_id: IndexerId::new(1).unwrap(),
            guid: CandidateGuid::new("guid").unwrap(),
            download_url: DownloadUrl::new("https://indexer.example/download").unwrap(),
            title: ItemTitle::new("Candidate Title").unwrap(),
            tracker: TrackerName::new("tracker.example").unwrap(),
            size: None,
            published_at_ms: None,
            info_hash: None,
            torrent_cache_path: None,
        };
        let metafile = TorrentMetafile::new(
            InfoHash::new(SHA1).unwrap(),
            DisplayName::new("Metafile Name").unwrap(),
            vec![
                TorrentFile::new(
                    PathBuf::from("file.mkv"),
                    ByteSize::new(42),
                    FileIndex::new(0),
                )
                .unwrap(),
            ],
        )
        .unwrap();

        let metadata = candidate_output_metadata(MediaType::Movie, &candidate, &metafile);

        assert_eq!(MediaType::Movie, metadata.media_type);
        assert_eq!("tracker.example", metadata.tracker);
        assert_eq!("Candidate Title", metadata.name);
        assert_eq!(metafile.info_hash(), &metadata.info_hash);
        assert!(!metadata.cached);
    }

    #[test]
    fn link_dir_selection_falls_back_to_bounded_test_link() {
        let root = unique_temp_dir("link-dir");
        let source_dir = root.join("source");
        let link_dir = root.join("links");
        let other_link_dir = root.join("other-links");
        fs::create_dir_all(&source_dir).unwrap();
        fs::create_dir_all(&link_dir).unwrap();
        fs::create_dir_all(&other_link_dir).unwrap();
        fs::write(source_dir.join("episode.mkv"), b"data").unwrap();

        let selected = select_link_dir(
            &source_dir,
            &[link_dir.clone(), other_link_dir.clone()],
            LinkDirOptions {
                link_type: LinkType::Hardlink,
                max_directory_entries: 16,
            },
        )
        .unwrap();

        assert_eq!(link_dir, selected);
        assert_eq!(0, fs::read_dir(&link_dir).unwrap().count());
        assert_eq!(0, fs::read_dir(&other_link_dir).unwrap().count());

        remove_temp_dir(&root);
    }

    #[test]
    fn virtual_link_dir_requires_consistent_source_resolution() {
        let root = unique_temp_dir("virtual-link-dir");
        let link_dir = root.join("links");
        let one = root.join("one.mkv");
        let two = root.join("two.mkv");
        fs::create_dir_all(&link_dir).unwrap();
        fs::write(&one, b"one").unwrap();
        fs::write(&two, b"two").unwrap();

        let selected = select_virtual_link_dir(
            &[one, two],
            &[link_dir.clone()],
            LinkDirOptions::new(LinkType::Hardlink),
        )
        .unwrap();

        assert_eq!(link_dir, selected);

        remove_temp_dir(&root);
    }

    #[test]
    fn link_action_creates_hardlinks_and_tracks_new_roots() {
        let root = unique_temp_dir("link-files");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"episode").unwrap();

        let outcome = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 7, 0)],
            &[torrent_file("Show/Episode.mkv", 7, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Hardlink),
        )
        .unwrap();

        assert_eq!(1, outcome.created_links.len());
        assert_eq!(vec![destination.join("Show")], outcome.created_roots);
        assert_eq!(
            fs::metadata(source.join("Show/Episode.mkv")).unwrap().len(),
            fs::metadata(destination.join("Show/Episode.mkv"))
                .unwrap()
                .len()
        );

        cleanup_created_roots(&outcome.created_roots).unwrap();
        assert!(!destination.join("Show").exists());
        remove_temp_dir(&root);
    }

    #[test]
    fn link_action_skips_existing_destinations_without_tracking_existing_roots() {
        let root = unique_temp_dir("link-existing");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::create_dir_all(destination.join("Show")).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"source").unwrap();
        fs::write(destination.join("Show/Episode.mkv"), b"existing").unwrap();

        let outcome = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 6, 0)],
            &[torrent_file("Show/Episode.mkv", 6, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Hardlink),
        )
        .unwrap();

        assert!(outcome.created_links.is_empty());
        assert!(outcome.created_roots.is_empty());
        assert!(outcome.already_existing);
        cleanup_created_roots(&outcome.created_roots).unwrap();
        assert_eq!(
            b"existing",
            fs::read(destination.join("Show/Episode.mkv"))
                .unwrap()
                .as_slice()
        );

        remove_temp_dir(&root);
    }

    #[test]
    fn link_action_rejects_destination_traversal() {
        let root = unique_temp_dir("link-traversal");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("Episode.mkv"), b"episode").unwrap();

        let error = link_metafile_files(
            &source,
            &[local_file("Episode.mkv", 7, 0)],
            &[torrent_file("../Episode.mkv", 7, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Hardlink),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            LinkActionError::InvalidDestinationPath { .. }
        ));
        assert!(!destination.exists());

        remove_temp_dir(&root);
    }

    #[test]
    fn link_action_pairs_non_exact_files_by_size_and_name() {
        let root = unique_temp_dir("link-pairing");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("old")).unwrap();
        fs::write(source.join("old/a.mkv"), b"aaaa").unwrap();
        fs::write(source.join("old/b.mkv"), b"bbbb").unwrap();

        let outcome = link_metafile_files(
            &source,
            &[local_file("old/a.mkv", 4, 0), local_file("old/b.mkv", 4, 1)],
            &[torrent_file("new/b.mkv", 4, 0)],
            MatchDecision::SizeOnly,
            &destination,
            LinkFilesOptions::new(LinkType::Hardlink),
        )
        .unwrap();

        assert_eq!(source.join("old/b.mkv"), outcome.created_links[0].source);
        assert!(destination.join("new/b.mkv").exists());

        remove_temp_dir(&root);
    }

    #[test]
    fn link_destination_uses_tracker_unless_flat() {
        let link_dir = PathBuf::from("/links");

        assert_eq!(
            PathBuf::from("/links/tracker_example"),
            link_destination_dir(&link_dir, "tracker/example", false).unwrap()
        );
        assert_eq!(
            link_dir,
            link_destination_dir(Path::new("/links"), "tracker/example", true).unwrap()
        );
    }

    fn test_metadata() -> TorrentOutputMetadata {
        TorrentOutputMetadata {
            media_type: MediaType::Movie,
            tracker: "tracker.example".to_owned(),
            name: "Example Movie".to_owned(),
            info_hash: InfoHash::new(SHA1).unwrap(),
            cached: false,
        }
    }

    fn local_file(path: &str, size: u64, index: u32) -> LocalFile {
        LocalFile::new(
            None,
            PathBuf::from(path),
            ByteSize::new(size),
            FileIndex::new(index),
        )
        .unwrap()
    }

    fn torrent_file(path: &str, size: u64, index: u32) -> TorrentFile {
        TorrentFile::new(
            PathBuf::from(path),
            ByteSize::new(size),
            FileIndex::new(index),
        )
        .unwrap()
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!("sporos-{label}-{nanos}-{}", std::process::id()))
    }

    fn remove_temp_dir(path: &Path) {
        if path.exists() {
            fs::remove_dir_all(path).unwrap();
        }
    }
}
