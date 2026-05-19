#![expect(
    clippy::indexing_slicing,
    clippy::let_underscore_must_use,
    reason = "mechanical clippy gate enablement leaves existing action safety cleanup to linked lint-class beads"
)]
#[cfg(unix)]
use std::collections::BTreeMap;
use std::error::Error;
#[cfg(unix)]
use std::ffi::OsString;
use std::fmt;
#[cfg(not(unix))]
use std::fs::DirBuilder;
use std::fs::{self, File, FileTimes, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

#[cfg(unix)]
use rustix::fd::OwnedFd;
#[cfg(unix)]
use rustix::fs::{AtFlags, CWD, Mode, OFlags, RenameFlags};

use crate::domain::{
    ByteSize, LocalFile, MatchDecision, MediaType, RemoteCandidate, TorrentFile, TorrentMetafile,
};
use crate::metrics::ActionOutcome;
use crate::persistence::torrent_cache::{
    TorrentCachePathError, TorrentOutputMetadata, torrent_output_path,
};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);
const DEFAULT_LINK_SCAN_LIMIT: usize = 10_000;
const LINK_COMPARE_BUFFER_SIZE: usize = 64 * 1024;
#[cfg(not(test))]
const MAX_PREPARED_CLEANUP_FDS: usize = 512;
#[cfg(test)]
const MAX_PREPARED_CLEANUP_FDS: usize = 32;

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
    link_root: Option<SelectedLinkDir>,
}

impl LinkFilesOptions {
    pub const fn new(link_type: LinkType) -> Self {
        Self {
            link_type,
            ignore_missing: false,
            link_root: None,
        }
    }

    pub fn with_link_root(mut self, link_root: SelectedLinkDir) -> Self {
        self.link_root = Some(link_root);
        self
    }
}

#[derive(Debug)]
pub struct CreatedLink {
    pub source: PathBuf,
    pub destination: PathBuf,
    #[cfg(unix)]
    basename: PathBuf,
    #[cfg(unix)]
    parent_fd: Arc<OwnedFd>,
    #[cfg(unix)]
    identity: FileIdentity,
    #[cfg(unix)]
    symlink_target: Option<PathBuf>,
}

#[derive(Debug)]
pub struct CreatedRoot {
    pub path: PathBuf,
    #[cfg(unix)]
    basename: PathBuf,
    #[cfg(unix)]
    parent_fd: OwnedFd,
    #[cfg(unix)]
    identity: FileIdentity,
}

#[derive(Debug, Clone)]
pub struct PreparedLink {
    pub source: PathBuf,
    pub destination: PathBuf,
    pub link_type: LinkType,
    expected_size: ByteSize,
    #[cfg(unix)]
    source_identity: FileIdentity,
    destination_match_mode: DestinationMatchMode,
    parent: PathBuf,
    basename: PathBuf,
    #[cfg(unix)]
    parent_identity: FileIdentity,
}

#[derive(Debug)]
pub struct LinkFilesOutcome {
    pub created_links: Vec<CreatedLink>,
    pub prepared_links: Vec<PreparedLink>,
    pub created_roots: Vec<CreatedRoot>,
    pub missing_sources: Vec<PathBuf>,
    pub already_existing: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SelectedLinkDir {
    path: PathBuf,
    #[cfg(unix)]
    identity: FileIdentity,
}

impl SelectedLinkDir {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn into_path(self) -> PathBuf {
        self.path
    }
}

impl LinkFilesOutcome {
    pub fn is_empty(&self) -> bool {
        self.created_links.is_empty()
            && self.prepared_links.is_empty()
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
    ExistingDestinationMismatch {
        source: PathBuf,
        destination: PathBuf,
    },
    PreparedDestinationLeftInPlace {
        source: PathBuf,
        destination: PathBuf,
        reason: &'static str,
    },
    CleanupIncomplete {
        primary: Box<LinkActionError>,
        cleanup: Box<LinkActionError>,
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
            Self::ExistingDestinationMismatch {
                source,
                destination,
            } => write!(
                formatter,
                "existing link destination {} does not match source {}",
                destination.display(),
                source.display()
            ),
            Self::PreparedDestinationLeftInPlace {
                source,
                destination,
                reason,
            } => write!(
                formatter,
                "prepared link destination {} for source {} was left in place: {reason}",
                destination.display(),
                source.display()
            ),
            Self::CleanupIncomplete { primary, cleanup } => {
                write!(formatter, "{primary}; cleanup incomplete: {cleanup}")
            }
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
            Self::CleanupIncomplete { primary, .. } => Some(primary),
            Self::EmptyLinkDirs
            | Self::InvalidLinkDir { .. }
            | Self::InvalidSourcePath { .. }
            | Self::InvalidDestinationPath { .. }
            | Self::UnsafeComponent { .. }
            | Self::NoCompatibleLinkDir { .. }
            | Self::ConflictingVirtualLinkDirs { .. }
            | Self::ExistingDestinationMismatch { .. }
            | Self::PreparedDestinationLeftInPlace { .. }
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

#[cfg(test)]
fn select_link_dir(
    source_path: &Path,
    link_dirs: &[PathBuf],
    options: LinkDirOptions,
) -> Result<PathBuf, LinkActionError> {
    select_link_dir_pinned(source_path, link_dirs, options).map(SelectedLinkDir::into_path)
}

pub fn select_link_dir_pinned(
    source_path: &Path,
    link_dirs: &[PathBuf],
    options: LinkDirOptions,
) -> Result<SelectedLinkDir, LinkActionError> {
    if link_dirs.is_empty() {
        return Err(LinkActionError::EmptyLinkDirs);
    }
    validate_link_dirs(link_dirs)?;

    if let Some(selected) = select_link_dir_by_device(source_path, link_dirs)? {
        return selected_link_dir(selected);
    }

    let representative = representative_source_file(source_path, options.max_directory_entries)?;
    for link_dir in link_dirs {
        if test_link_compatibility(&representative.path, link_dir, options.link_type)? {
            representative.cleanup()?;
            return selected_link_dir(link_dir.clone());
        }
    }
    representative.cleanup()?;

    if options.link_type == LinkType::Symlink {
        tracing::warn!(
            source = %source_path.display(),
            link_dir = %link_dirs[0].display(),
            "using first symlink directory after compatibility tests failed"
        );
        return selected_link_dir(link_dirs[0].clone());
    }

    Err(LinkActionError::NoCompatibleLinkDir {
        source: source_path.to_path_buf(),
    })
}

fn selected_link_dir(path: PathBuf) -> Result<SelectedLinkDir, LinkActionError> {
    #[cfg(unix)]
    {
        let file = open_existing_directory(&path).map_err(|source| LinkActionError::Io {
            operation: "open selected link directory",
            path: path.clone(),
            source,
        })?;
        let identity = file_identity(&file.metadata().map_err(|source| LinkActionError::Io {
            operation: "record selected link directory identity",
            path: path.clone(),
            source,
        })?)?;
        Ok(SelectedLinkDir { path, identity })
    }
    #[cfg(not(unix))]
    {
        Ok(SelectedLinkDir { path })
    }
}

#[cfg(test)]
fn select_virtual_link_dir(
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
        prepared_links: Vec::new(),
        created_roots: Vec::new(),
        missing_sources: Vec::new(),
        already_existing: false,
    };
    let mut pending_links = Vec::with_capacity(pairs.len());

    for pair in pairs {
        let destination = safe_destination_path(destination_dir, &pair.destination_relative_path)?;
        let existing_destination = match fs::symlink_metadata(&destination) {
            Ok(_) => true,
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(source) => {
                return Err(LinkActionError::Io {
                    operation: "inspect link destination",
                    path: destination,
                    source,
                });
            }
        };
        let source_file =
            match verified_source_file(&pair.source, pair.expected_size, options.link_type) {
                Ok(source_file) => source_file,
                Err(error) if error.kind() == io::ErrorKind::NotFound && options.ignore_missing => {
                    outcome.missing_sources.push(pair.source);
                    continue;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    return Err(LinkActionError::MissingSource { path: pair.source });
                }
                Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
                    return Err(LinkActionError::InvalidSourcePath { path: pair.source });
                }
                Err(source) => {
                    return Err(LinkActionError::Io {
                        operation: "inspect source file",
                        path: pair.source,
                        source,
                    });
                }
            };
        if existing_destination {
            let parent = prepare_destination_parent(
                destination_dir,
                &pair.destination_relative_path,
                false,
                options.link_root.as_ref(),
            )?;
            if existing_link_destination_matches(
                &pair.source,
                &source_file,
                &destination,
                options.link_type,
                DestinationMatchMode::ReuseExisting,
            )? {
                outcome.already_existing = true;
                outcome.prepared_links.push(prepared_link_manifest(
                    pair.source,
                    destination,
                    options.link_type,
                    pair.expected_size,
                    &source_file,
                    &parent,
                    DestinationMatchMode::ReuseExisting,
                )?);
                continue;
            }
        }

        pending_links.push(VerifiedLinkPair {
            source: pair.source,
            destination,
            destination_relative_path: pair.destination_relative_path,
            expected_size: pair.expected_size,
        });
    }

    #[cfg(unix)]
    let mut cleanup_parent_fds: BTreeMap<(PathBuf, FileIdentity), Arc<OwnedFd>> = BTreeMap::new();
    for pair in pending_links {
        #[cfg(unix)]
        let retained_cleanup_handles = outcome.created_roots.len() + cleanup_parent_fds.len();
        let mut parent = match prepare_destination_parent_with_retained_cleanup_handles(
            destination_dir,
            &pair.destination_relative_path,
            true,
            options.link_root.as_ref(),
            #[cfg(unix)]
            retained_cleanup_handles,
        ) {
            Ok(parent) => parent,
            Err(error) => {
                return Err(cleanup_created_links_and_roots_before_error(
                    &outcome,
                    &[],
                    error,
                ));
            }
        };
        let created_roots = std::mem::take(&mut parent.created_roots);
        #[cfg(unix)]
        if cleanup_handle_count(
            outcome.created_roots.len() + created_roots.len(),
            cleanup_parent_fds.len(),
            0,
        )
        .is_err()
        {
            let error = LinkActionError::Io {
                operation: "retain prepared link cleanup handles",
                path: parent.path.clone(),
                source: io::Error::other("too many prepared link cleanup handles"),
            };
            return Err(cleanup_created_links_and_roots_before_error(
                &outcome,
                &created_roots,
                error,
            ));
        }
        let mut source_file =
            match verified_source_file(&pair.source, pair.expected_size, options.link_type) {
                Ok(source_file) => source_file,
                Err(source) => {
                    let error = LinkActionError::Io {
                        operation: "open source file for link creation",
                        path: pair.source.clone(),
                        source,
                    };
                    return Err(cleanup_created_links_and_roots_before_error(
                        &outcome,
                        &created_roots,
                        error,
                    ));
                }
            };

        if let Err(error) = ensure_destination_parent_matches_path(&parent) {
            return Err(cleanup_created_links_and_roots_before_error(
                &outcome,
                &created_roots,
                error,
            ));
        }

        let created_entry =
            match create_link_in_parent(&pair.source, &source_file, &parent, options.link_type) {
                Ok(created_entry) => created_entry,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    match existing_link_destination_matches(
                        &pair.source,
                        &source_file,
                        &pair.destination,
                        options.link_type,
                        DestinationMatchMode::ReuseExisting,
                    ) {
                        Ok(true) => {
                            outcome.already_existing = true;
                            outcome.prepared_links.push(prepared_link_manifest(
                                pair.source,
                                pair.destination,
                                options.link_type,
                                pair.expected_size,
                                &source_file,
                                &parent,
                                DestinationMatchMode::ReuseExisting,
                            )?);
                            continue;
                        }
                        Ok(false) => {
                            source_file = match verified_source_file(
                                &pair.source,
                                pair.expected_size,
                                options.link_type,
                            ) {
                                Ok(source_file) => source_file,
                                Err(source) => {
                                    let error = LinkActionError::Io {
                                        operation: "open source file for link creation retry",
                                        path: pair.source.clone(),
                                        source,
                                    };
                                    return Err(cleanup_created_links_and_roots_before_error(
                                        &outcome,
                                        &created_roots,
                                        error,
                                    ));
                                }
                            };
                            match create_link_in_parent(
                                &pair.source,
                                &source_file,
                                &parent,
                                options.link_type,
                            ) {
                                Ok(created_entry) => created_entry,
                                Err(source) => {
                                    let error = link_creation_error(
                                        &pair.source,
                                        &pair.destination,
                                        "create linked file",
                                        source,
                                    );
                                    return Err(cleanup_created_links_and_roots_before_error(
                                        &outcome,
                                        &created_roots,
                                        error,
                                    ));
                                }
                            }
                        }
                        Err(error) => {
                            return Err(cleanup_created_links_and_roots_before_error(
                                &outcome,
                                &created_roots,
                                error,
                            ));
                        }
                    }
                }
                Err(source) => {
                    let error = link_creation_error(
                        &pair.source,
                        &pair.destination,
                        "create linked file",
                        source,
                    );
                    return Err(cleanup_created_links_and_roots_before_error(
                        &outcome,
                        &created_roots,
                        error,
                    ));
                }
            };
        if let Err(error) = verify_created_entry_path(&parent, &created_entry, &pair.destination) {
            return Err(cleanup_created_entry_and_roots_before_error(
                &outcome,
                &parent,
                &created_entry,
                &created_roots,
                error,
            ));
        }
        match existing_link_destination_matches(
            &pair.source,
            &source_file,
            &pair.destination,
            options.link_type,
            DestinationMatchMode::VerifyPrepared,
        ) {
            Ok(true) => {}
            Ok(false) => {
                let error = LinkActionError::PreparedDestinationLeftInPlace {
                    source: pair.source.clone(),
                    destination: pair.destination.clone(),
                    reason: "post-create verification did not match the requested source",
                };
                return Err(cleanup_created_entry_and_roots_before_error(
                    &outcome,
                    &parent,
                    &created_entry,
                    &created_roots,
                    error,
                ));
            }
            Err(error) => {
                let error = LinkActionError::PreparedDestinationLeftInPlace {
                    source: pair.source.clone(),
                    destination: pair.destination.clone(),
                    reason: match error {
                        LinkActionError::ExistingDestinationMismatch { .. } => {
                            "post-create verification found a destination mismatch"
                        }
                        _ => "post-create verification failed",
                    },
                };
                return Err(cleanup_created_entry_and_roots_before_error(
                    &outcome,
                    &parent,
                    &created_entry,
                    &created_roots,
                    error,
                ));
            }
        }
        #[cfg(unix)]
        let cleanup_parent_fd = match cleanup_parent_fd(
            &parent,
            &mut cleanup_parent_fds,
            outcome.created_roots.len() + created_roots.len(),
        ) {
            Ok(fd) => fd,
            Err(error) => {
                return Err(cleanup_created_entry_and_roots_before_error(
                    &outcome,
                    &parent,
                    &created_entry,
                    &created_roots,
                    error,
                ));
            }
        };
        let created_link = match created_link(
            pair.source.clone(),
            pair.destination.clone(),
            &parent,
            &created_entry,
            #[cfg(unix)]
            cleanup_parent_fd,
        ) {
            Ok(link) => link,
            Err(error) => {
                return Err(cleanup_created_entry_and_roots_before_error(
                    &outcome,
                    &parent,
                    &created_entry,
                    &created_roots,
                    error,
                ));
            }
        };
        let prepared_link = match prepared_link_manifest(
            pair.source,
            pair.destination,
            options.link_type,
            pair.expected_size,
            &source_file,
            &parent,
            DestinationMatchMode::VerifyPrepared,
        ) {
            Ok(link) => link,
            Err(error) => {
                return Err(cleanup_created_entry_and_roots_before_error(
                    &outcome,
                    &parent,
                    &created_entry,
                    &created_roots,
                    error,
                ));
            }
        };
        outcome.created_links.push(created_link);
        outcome.prepared_links.push(prepared_link);
        outcome.created_roots.extend(created_roots);
    }

    Ok(outcome)
}

pub fn cleanup_created_roots(roots: &[CreatedRoot]) -> Result<(), LinkActionError> {
    for root in roots.iter().rev() {
        cleanup_created_root(root)?;
    }
    Ok(())
}

fn cleanup_created_links_and_roots_before_error(
    outcome: &LinkFilesOutcome,
    extra_roots: &[CreatedRoot],
    error: LinkActionError,
) -> LinkActionError {
    match cleanup_created_links_and_roots_with_extra(
        &outcome.created_links,
        &outcome.created_roots,
        extra_roots,
    ) {
        Ok(()) => error,
        Err(cleanup_error) => LinkActionError::CleanupIncomplete {
            primary: Box::new(error),
            cleanup: Box::new(cleanup_error),
        },
    }
}

fn cleanup_created_entry_and_roots_before_error(
    outcome: &LinkFilesOutcome,
    parent: &DestinationParent,
    entry: &CreatedEntry,
    extra_roots: &[CreatedRoot],
    error: LinkActionError,
) -> LinkActionError {
    match cleanup_created_entry_at(parent, entry).and_then(|()| {
        cleanup_created_links_and_roots_with_extra(
            &outcome.created_links,
            &outcome.created_roots,
            extra_roots,
        )
    }) {
        Ok(()) => error,
        Err(cleanup_error) => LinkActionError::CleanupIncomplete {
            primary: Box::new(error),
            cleanup: Box::new(cleanup_error),
        },
    }
}

fn link_creation_error(
    source: &Path,
    destination: &Path,
    operation: &'static str,
    error: io::Error,
) -> LinkActionError {
    if destination_exists(destination) {
        LinkActionError::PreparedDestinationLeftInPlace {
            source: source.to_path_buf(),
            destination: destination.to_path_buf(),
            reason: "link creation failed after creating or racing with a public destination",
        }
    } else {
        LinkActionError::Io {
            operation,
            path: destination.to_path_buf(),
            source: error,
        }
    }
}

fn destination_exists(destination: &Path) -> bool {
    match fs::symlink_metadata(destination) {
        Ok(_) => true,
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(_) => false,
    }
}

pub fn cleanup_created_links_and_roots(
    links: &[CreatedLink],
    roots: &[CreatedRoot],
) -> Result<(), LinkActionError> {
    cleanup_created_links_and_roots_with_extra(links, roots, &[])
}

fn cleanup_created_links_and_roots_with_extra(
    links: &[CreatedLink],
    roots: &[CreatedRoot],
    extra_roots: &[CreatedRoot],
) -> Result<(), LinkActionError> {
    for link in links {
        cleanup_created_link(link)?;
    }
    cleanup_created_roots(extra_roots)?;
    cleanup_created_roots(roots)
}

pub fn validate_prepared_links(links: &[PreparedLink]) -> Result<(), LinkActionError> {
    for link in links {
        validate_prepared_link_parent(link)?;
        let source_file = verified_source_file(&link.source, link.expected_size, link.link_type)
            .map_err(|source| LinkActionError::Io {
                operation: "open prepared link source for revalidation",
                path: link.source.clone(),
                source,
            })?;
        #[cfg(unix)]
        if metadata_identity(&source_file.metadata) != link.source_identity {
            return Err(LinkActionError::Io {
                operation: "verify prepared link source identity",
                path: link.source.clone(),
                source: io::Error::other("prepared link source changed before injection"),
            });
        }
        let destination = link.parent.join(&link.basename);
        if !existing_link_destination_matches(
            &link.source,
            &source_file,
            &destination,
            link.link_type,
            link.destination_match_mode,
        )? {
            return Err(LinkActionError::ExistingDestinationMismatch {
                source: link.source.clone(),
                destination,
            });
        }
    }
    Ok(())
}

fn validate_prepared_link_parent(link: &PreparedLink) -> Result<(), LinkActionError> {
    #[cfg(unix)]
    {
        let parent =
            open_existing_directory(&link.parent).map_err(|source| LinkActionError::Io {
                operation: "open prepared link parent for revalidation",
                path: link.parent.clone(),
                source,
            })?;
        let identity = file_identity(&parent.metadata().map_err(|source| LinkActionError::Io {
            operation: "record prepared link parent identity for revalidation",
            path: link.parent.clone(),
            source,
        })?);
        if identity? == link.parent_identity {
            Ok(())
        } else {
            Err(LinkActionError::Io {
                operation: "verify prepared link parent identity",
                path: link.parent.clone(),
                source: io::Error::other("prepared link parent changed before injection"),
            })
        }
    }
    #[cfg(not(unix))]
    {
        let _ = link;
        Ok(())
    }
}

#[derive(Debug)]
struct LinkPair {
    source: PathBuf,
    destination_relative_path: PathBuf,
    expected_size: ByteSize,
}

#[derive(Debug)]
struct VerifiedLinkPair {
    source: PathBuf,
    destination: PathBuf,
    destination_relative_path: PathBuf,
    expected_size: ByteSize,
}

#[derive(Debug)]
struct VerifiedSourceFile {
    metadata: fs::Metadata,
    identity: same_file::Handle,
    file: File,
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
struct FileIdentity {
    dev: u64,
    ino: u64,
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
        validate_link_dir(link_dir)?;
    }
    Ok(())
}

#[cfg(unix)]
fn validate_link_dir(link_dir: &Path) -> Result<(), LinkActionError> {
    open_existing_directory(link_dir)
        .map(|_| ())
        .map_err(|source| LinkActionError::Io {
            operation: "open link directory",
            path: link_dir.to_path_buf(),
            source,
        })
}

#[cfg(not(unix))]
fn validate_link_dir(link_dir: &Path) -> Result<(), LinkActionError> {
    let metadata = fs::symlink_metadata(link_dir).map_err(|source| LinkActionError::Io {
        operation: "inspect link directory",
        path: link_dir.to_path_buf(),
        source,
    })?;
    if !metadata.file_type().is_dir() {
        return Err(LinkActionError::InvalidLinkDir {
            path: link_dir.to_path_buf(),
        });
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
        let Some(device) = link_dir_device_id(link_dir)? else {
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
fn link_dir_device_id(path: &Path) -> Result<Option<u64>, LinkActionError> {
    open_existing_directory(path)
        .and_then(|directory| directory.metadata())
        .map(|metadata| Some(metadata.dev()))
        .map_err(|source| LinkActionError::Io {
            operation: "inspect link directory filesystem device",
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(not(unix))]
fn link_dir_device_id(path: &Path) -> Result<Option<u64>, LinkActionError> {
    device_id(path)
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
    #[cfg(unix)]
    {
        test_link_compatibility_at(source_file, link_dir, link_type)
    }
    #[cfg(not(unix))]
    {
        let stage_dir = create_private_stage_dir(link_dir, "link-test").map_err(|source| {
            LinkActionError::Io {
                operation: "create link compatibility staging directory",
                path: link_dir.to_path_buf(),
                source,
            }
        })?;
        let destination = stage_dir.join("probe");
        let result = match link_type {
            LinkType::Hardlink | LinkType::Symlink => fs::hard_link(source_file, &destination),
            LinkType::Reflink => reflink_copy::reflink(source_file, &destination),
            LinkType::ReflinkOrCopy => {
                create_reflink_or_copy_no_overwrite(source_file, &destination)
            }
        };
        match result {
            Ok(()) => {
                remove_stage_dir(&stage_dir).map_err(|source| LinkActionError::Io {
                    operation: "remove link compatibility staging directory",
                    path: stage_dir,
                    source,
                })?;
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
                let _ = remove_stage_dir(&stage_dir);
                Ok(false)
            }
            Err(source) => Err(LinkActionError::Io {
                operation: "test link compatibility",
                path: destination,
                source,
            }),
        }
    }
}

#[cfg(unix)]
fn test_link_compatibility_at(
    source_file: &Path,
    link_dir: &Path,
    link_type: LinkType,
) -> Result<bool, LinkActionError> {
    let parent = DestinationParent {
        path: link_dir.to_path_buf(),
        basename: PathBuf::new(),
        created_roots: Vec::new(),
        fd: open_directory_path(link_dir, false).map_err(|source| LinkActionError::Io {
            operation: "open link compatibility directory",
            path: link_dir.to_path_buf(),
            source,
        })?,
    };
    let stage = create_private_stage_dir_at(&parent, "link-test").map_err(|source| {
        LinkActionError::Io {
            operation: "create link compatibility staging directory",
            path: link_dir.to_path_buf(),
            source,
        }
    })?;
    let destination = Path::new("probe");
    let result = match link_type {
        LinkType::Hardlink | LinkType::Symlink => {
            verified_source_file_without_expected_size(source_file, link_type).and_then(|source| {
                create_staged_hardlink(source_file, &source, &stage, destination)
            })
        }
        LinkType::Reflink => verified_source_file_without_expected_size(source_file, link_type)
            .and_then(|source| create_staged_reflink(&source, &stage, destination)),
        LinkType::ReflinkOrCopy => {
            verified_source_file_without_expected_size(source_file, link_type).and_then(|source| {
                create_staged_reflink(&source, &stage, destination).or_else(|error| {
                    if should_fallback_to_copy(error.kind()) {
                        copy_file_to_stage(&source, &stage, destination).map(|_| ())
                    } else {
                        Err(error)
                    }
                })
            })
        }
    };
    match result {
        Ok(()) => {
            remove_stage_dir_entry_at(&parent, &stage, destination).map_err(|source| {
                LinkActionError::Io {
                    operation: "remove link compatibility staging directory",
                    path: link_dir.to_path_buf(),
                    source,
                }
            })?;
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
            let _ = remove_stage_dir_entry_at(&parent, &stage, destination);
            Ok(false)
        }
        Err(source) => {
            let _ = remove_stage_dir_entry_at(&parent, &stage, destination);
            Err(LinkActionError::Io {
                operation: "test link compatibility",
                path: link_dir.join(destination),
                source,
            })
        }
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
                    expected_size: candidate.size,
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
            expected_size: candidate.size,
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

fn verified_source_file(
    path: &Path,
    expected_size: ByteSize,
    _link_type: LinkType,
) -> io::Result<VerifiedSourceFile> {
    let source_file = verified_source_file_without_expected_size(path, _link_type)?;
    if source_file.metadata.len() == expected_size.get() {
        Ok(source_file)
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "source file size {} did not match expected size {}",
                source_file.metadata.len(),
                expected_size.get()
            ),
        ))
    }
}

fn verified_source_file_without_expected_size(
    path: &Path,
    _link_type: LinkType,
) -> io::Result<VerifiedSourceFile> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "source path is not a regular file",
        ));
    }

    let file = open_existing_regular_file(path)?;
    let opened_metadata = file.metadata()?;
    if !opened_metadata.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "source handle is not a regular file",
        ));
    }
    #[cfg(unix)]
    if metadata_identity(&metadata) != metadata_identity(&opened_metadata) {
        return Err(io::Error::other(
            "source path changed while opening source file",
        ));
    }
    let identity = same_file::Handle::from_file(file.try_clone()?)?;
    Ok(VerifiedSourceFile {
        metadata: opened_metadata,
        identity,
        file,
    })
}

#[cfg(unix)]
fn metadata_identity(metadata: &fs::Metadata) -> FileIdentity {
    FileIdentity {
        dev: metadata.dev(),
        ino: metadata.ino(),
    }
}

fn existing_link_destination_matches(
    source: &Path,
    source_file: &VerifiedSourceFile,
    destination: &Path,
    link_type: LinkType,
    mode: DestinationMatchMode,
) -> Result<bool, LinkActionError> {
    let destination_metadata = match fs::symlink_metadata(destination) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(source) => Err(LinkActionError::Io {
            operation: "inspect link destination",
            path: destination.to_path_buf(),
            source,
        })?,
    };
    if !destination_matches_source(
        source,
        source_file,
        destination,
        &destination_metadata,
        link_type,
        mode,
    )? {
        return Err(LinkActionError::ExistingDestinationMismatch {
            source: source.to_path_buf(),
            destination: destination.to_path_buf(),
        });
    }
    Ok(true)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum DestinationMatchMode {
    ReuseExisting,
    VerifyPrepared,
}

fn destination_matches_source(
    source: &Path,
    source_file: &VerifiedSourceFile,
    destination: &Path,
    destination_metadata: &fs::Metadata,
    link_type: LinkType,
    mode: DestinationMatchMode,
) -> Result<bool, LinkActionError> {
    match link_type {
        LinkType::Hardlink => {
            if !destination_metadata.file_type().is_file() {
                return Ok(false);
            }
            destination_matches_source_identity(source_file, destination)
        }
        LinkType::Symlink => {
            if !destination_metadata.file_type().is_symlink() {
                return Ok(false);
            }
            destination_symlink_matches_source(source, source_file, destination)
        }
        LinkType::Reflink | LinkType::ReflinkOrCopy => match mode {
            DestinationMatchMode::ReuseExisting => existing_destination_has_source_identity(
                source,
                source_file,
                destination_metadata,
                destination,
            ),
            DestinationMatchMode::VerifyPrepared => {
                file_contents_match(source, source_file, destination, destination_metadata)
            }
        },
    }
}

fn existing_destination_has_source_identity(
    source: &Path,
    source_file: &VerifiedSourceFile,
    destination_metadata: &fs::Metadata,
    destination: &Path,
) -> Result<bool, LinkActionError> {
    if destination_metadata.file_type().is_symlink() {
        return destination_symlink_matches_source(source, source_file, destination);
    }
    if !destination_metadata.file_type().is_file()
        || destination_metadata.len() != source_file.metadata.len()
    {
        return Ok(false);
    }
    destination_matches_source_identity(source_file, destination)
}

fn file_contents_match(
    source: &Path,
    source_file: &VerifiedSourceFile,
    destination: &Path,
    destination_metadata: &fs::Metadata,
) -> Result<bool, LinkActionError> {
    if !destination_metadata.file_type().is_file()
        || destination_metadata.len() != source_file.metadata.len()
    {
        return Ok(false);
    }

    let mut source_reader =
        source_file
            .file
            .try_clone()
            .map_err(|source_error| LinkActionError::Io {
                operation: "clone source for link destination comparison",
                path: source.to_path_buf(),
                source: source_error,
            })?;
    source_reader
        .seek(SeekFrom::Start(0))
        .map_err(|source_error| LinkActionError::Io {
            operation: "rewind source for link destination comparison",
            path: source.to_path_buf(),
            source: source_error,
        })?;
    let mut destination_file =
        open_existing_regular_file(destination).map_err(|source_error| LinkActionError::Io {
            operation: "open existing link destination for comparison",
            path: destination.to_path_buf(),
            source: source_error,
        })?;
    let destination_identity =
        same_file::Handle::from_file(destination_file.try_clone().map_err(|source_error| {
            LinkActionError::Io {
                operation: "clone existing link destination handle for comparison",
                path: destination.to_path_buf(),
                source: source_error,
            }
        })?)
        .map_err(|source_error| LinkActionError::Io {
            operation: "record existing link destination identity for comparison",
            path: destination.to_path_buf(),
            source: source_error,
        })?;

    let mut source_buffer = vec![0; LINK_COMPARE_BUFFER_SIZE];
    let mut destination_buffer = vec![0; LINK_COMPARE_BUFFER_SIZE];

    loop {
        let source_len = source_reader
            .read(&mut source_buffer)
            .map_err(|source_error| LinkActionError::Io {
                operation: "read source for link destination comparison",
                path: source.to_path_buf(),
                source: source_error,
            })?;
        let destination_len =
            destination_file
                .read(&mut destination_buffer)
                .map_err(|source_error| LinkActionError::Io {
                    operation: "read existing link destination for comparison",
                    path: destination.to_path_buf(),
                    source: source_error,
                })?;
        if source_len != destination_len {
            return Ok(false);
        }
        if source_len == 0 {
            return path_matches_file_identity(
                destination,
                &destination_identity,
                "open existing link destination after comparison",
            );
        }
        if source_buffer[..source_len] != destination_buffer[..destination_len] {
            return Ok(false);
        }
    }
}

fn destination_symlink_matches_source(
    source: &Path,
    source_file: &VerifiedSourceFile,
    destination: &Path,
) -> Result<bool, LinkActionError> {
    let target = fs::read_link(destination).map_err(|source_error| LinkActionError::Io {
        operation: "read existing symlink destination",
        path: destination.to_path_buf(),
        source: source_error,
    })?;
    if target != source {
        return Ok(false);
    }
    if !path_matches_file_identity(
        source,
        &source_file.identity,
        "open symlink source after target comparison",
    )? {
        return Ok(false);
    }
    fs::read_link(destination)
        .map(|current_target| current_target == target)
        .map_err(|source_error| LinkActionError::Io {
            operation: "re-read existing symlink destination",
            path: destination.to_path_buf(),
            source: source_error,
        })
}

fn destination_matches_source_identity(
    source_file: &VerifiedSourceFile,
    destination: &Path,
) -> Result<bool, LinkActionError> {
    let destination_file =
        open_existing_regular_file(destination).map_err(|source_error| LinkActionError::Io {
            operation: "open existing hardlink destination for comparison",
            path: destination.to_path_buf(),
            source: source_error,
        })?;
    let destination_identity =
        same_file::Handle::from_file(destination_file).map_err(|source_error| {
            LinkActionError::Io {
                operation: "record destination identity for hardlink comparison",
                path: destination.to_path_buf(),
                source: source_error,
            }
        })?;
    if source_file.identity != destination_identity {
        return Ok(false);
    }
    path_matches_file_identity(
        destination,
        &destination_identity,
        "open existing hardlink destination after comparison",
    )
}

fn path_matches_file_identity(
    path: &Path,
    identity: &same_file::Handle,
    operation: &'static str,
) -> Result<bool, LinkActionError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(LinkActionError::Io {
                operation: "inspect file identity path",
                path: path.to_path_buf(),
                source,
            });
        }
    };
    if !metadata.file_type().is_file() {
        return Ok(false);
    }
    let file = open_existing_regular_file(path).map_err(|source| LinkActionError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    })?;
    same_file::Handle::from_file(file)
        .map(|current| &current == identity)
        .map_err(|source| LinkActionError::Io {
            operation: "record file identity",
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(unix)]
fn source_path_matches_identity(path: &Path, identity: &same_file::Handle) -> io::Result<bool> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Ok(false);
    }
    let file = open_existing_regular_file(path)?;
    same_file::Handle::from_file(file).map(|current| &current == identity)
}

#[cfg(unix)]
fn open_existing_regular_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

#[cfg(not(unix))]
fn open_existing_regular_file(_path: &Path) -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "no no-follow regular-file open is available on this platform",
    ))
}

#[cfg(any(not(unix), test))]
fn copy_file_no_overwrite(source: &Path, destination: &Path) -> io::Result<u64> {
    let mut source_file = open_existing_regular_file(source)?;
    let mut destination_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;

    match copy_file_contents(&mut source_file, &mut destination_file)
        .and_then(|written| destination_file.sync_all().map(|()| written))
    {
        Ok(written) => Ok(written),
        Err(error) => Err(io::Error::new(
            error.kind(),
            format!(
                "copy failed after creating {}; destination left in place: {error}",
                destination.display()
            ),
        )),
    }
}

fn copy_file_contents(source: &mut File, destination: &mut File) -> io::Result<u64> {
    let mut written = 0u64;
    let mut buffer = vec![0; LINK_COMPARE_BUFFER_SIZE];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            return Ok(written);
        }
        destination.write_all(&buffer[..read])?;
        written = written
            .checked_add(u64::try_from(read).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("copied byte count overflow"))?;
    }
}

#[derive(Debug)]
struct DestinationParent {
    path: PathBuf,
    basename: PathBuf,
    created_roots: Vec<CreatedRoot>,
    #[cfg(unix)]
    fd: OwnedFd,
}

#[derive(Debug)]
struct CreatedEntry {
    #[cfg(unix)]
    identity: FileIdentity,
    #[cfg(unix)]
    symlink_target: Option<PathBuf>,
}

#[cfg(unix)]
#[derive(Debug)]
struct PrivateStageDir {
    name: PathBuf,
    fd: OwnedFd,
}

fn prepare_destination_parent(
    destination_dir: &Path,
    relative_path: &Path,
    create_missing: bool,
    link_root: Option<&SelectedLinkDir>,
) -> Result<DestinationParent, LinkActionError> {
    prepare_destination_parent_with_retained_cleanup_handles(
        destination_dir,
        relative_path,
        create_missing,
        link_root,
        #[cfg(unix)]
        0,
    )
}

fn prepare_destination_parent_with_retained_cleanup_handles(
    destination_dir: &Path,
    relative_path: &Path,
    create_missing: bool,
    link_root: Option<&SelectedLinkDir>,
    #[cfg(unix)] retained_cleanup_handles: usize,
) -> Result<DestinationParent, LinkActionError> {
    validate_relative_path(relative_path)?;
    let mut components: Vec<PathBuf> = relative_path
        .components()
        .map(|component| PathBuf::from(component.as_os_str()))
        .collect();
    let Some(basename) = components.pop() else {
        return Err(LinkActionError::InvalidDestinationPath {
            destination_dir: destination_dir.to_path_buf(),
            relative_path: relative_path.to_path_buf(),
        });
    };
    let parent_path = components
        .iter()
        .fold(destination_dir.to_path_buf(), |path, component| {
            path.join(component)
        });

    #[cfg(unix)]
    {
        let (fd, created_roots) = match link_root {
            Some(link_root) => open_destination_parent_in_selected_root(
                link_root,
                destination_dir,
                &components,
                create_missing,
                retained_cleanup_handles,
            ),
            None => open_destination_parent_at(
                destination_dir,
                &components,
                create_missing,
                retained_cleanup_handles,
            ),
        }
        .map_err(|source| LinkActionError::Io {
            operation: "open link destination directory",
            path: parent_path.clone(),
            source,
        })?;
        Ok(DestinationParent {
            path: parent_path,
            basename,
            created_roots,
            fd,
        })
    }
    #[cfg(not(unix))]
    {
        let _ = link_root;
        if create_missing {
            fs::create_dir_all(&parent_path).map_err(|source| LinkActionError::Io {
                operation: "create link destination directory",
                path: parent_path.clone(),
                source,
            })?;
        } else if !parent_path.is_dir() {
            return Err(LinkActionError::Io {
                operation: "open existing link destination directory",
                path: parent_path.clone(),
                source: io::Error::new(
                    io::ErrorKind::NotFound,
                    "link destination directory is missing",
                ),
            });
        }
        Ok(DestinationParent {
            path: parent_path,
            basename,
            created_roots: Vec::new(),
        })
    }
}

fn ensure_destination_parent_matches_path(
    parent: &DestinationParent,
) -> Result<(), LinkActionError> {
    #[cfg(unix)]
    {
        let path = parent.path.join(".");
        let path_file = open_existing_directory(&path).map_err(|source| LinkActionError::Io {
            operation: "reopen link destination directory after creation",
            path: parent.path.clone(),
            source,
        })?;
        let path_identity =
            same_file::Handle::from_file(path_file).map_err(|source| LinkActionError::Io {
                operation: "record link destination directory identity",
                path: parent.path.clone(),
                source,
            })?;
        let fd_file = File::from(
            rustix::fs::openat(
                &parent.fd,
                Path::new("."),
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(io::Error::from)
            .map_err(|source| LinkActionError::Io {
                operation: "reopen pinned link destination directory",
                path: parent.path.clone(),
                source,
            })?,
        );
        let fd_identity =
            same_file::Handle::from_file(fd_file).map_err(|source| LinkActionError::Io {
                operation: "record opened link destination directory identity",
                path: parent.path.clone(),
                source,
            })?;
        if path_identity == fd_identity {
            Ok(())
        } else {
            Err(LinkActionError::Io {
                operation: "verify link destination directory identity",
                path: parent.path.clone(),
                source: io::Error::other(
                    "destination directory path changed while preparing links",
                ),
            })
        }
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
        Ok(())
    }
}

fn verify_created_entry_path(
    parent: &DestinationParent,
    entry: &CreatedEntry,
    destination: &Path,
) -> Result<(), LinkActionError> {
    #[cfg(unix)]
    {
        ensure_destination_parent_matches_path(parent)?;
        let metadata = fs::symlink_metadata(destination).map_err(|source| LinkActionError::Io {
            operation: "inspect published link path identity",
            path: destination.to_path_buf(),
            source,
        })?;
        let path_identity = file_identity(&metadata)?;
        if path_identity == entry.identity {
            Ok(())
        } else {
            Err(LinkActionError::Io {
                operation: "verify published link path identity",
                path: destination.to_path_buf(),
                source: io::Error::other("published path does not match fd-created link"),
            })
        }
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
        let _ = entry;
        let _ = destination;
        Ok(())
    }
}

fn cleanup_created_entry_at(
    parent: &DestinationParent,
    entry: &CreatedEntry,
) -> Result<(), LinkActionError> {
    #[cfg(unix)]
    {
        cleanup_created_entry_at_io(parent, entry).map_err(|source| LinkActionError::Io {
            operation: "remove fd-created link after verification failure",
            path: parent.path.join(&parent.basename),
            source,
        })
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
        let _ = entry;
        Ok(())
    }
}

fn created_link(
    source: PathBuf,
    destination: PathBuf,
    parent: &DestinationParent,
    entry: &CreatedEntry,
    #[cfg(unix)] parent_fd: Arc<OwnedFd>,
) -> Result<CreatedLink, LinkActionError> {
    #[cfg(unix)]
    {
        Ok(CreatedLink {
            source,
            destination,
            basename: parent.basename.clone(),
            parent_fd,
            identity: entry.identity,
            symlink_target: entry.symlink_target.clone(),
        })
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
        let _ = entry;
        Ok(CreatedLink {
            source,
            destination,
        })
    }
}

#[cfg(unix)]
fn created_root_at(path: &Path, parent_fd: &OwnedFd, basename: &Path) -> io::Result<CreatedRoot> {
    Ok(CreatedRoot {
        path: path.to_path_buf(),
        basename: basename.to_path_buf(),
        parent_fd: duplicate_directory_fd_from(parent_fd)?,
        identity: entry_identity_in_fd(parent_fd, basename)?,
    })
}

fn cleanup_created_link(link: &CreatedLink) -> Result<(), LinkActionError> {
    #[cfg(unix)]
    {
        cleanup_created_entry_in_fd(
            &link.parent_fd,
            &link.basename,
            link.identity,
            link.symlink_target.as_deref(),
            &link.destination,
            "prepared link",
            AtFlags::empty(),
        )
    }
    #[cfg(not(unix))]
    {
        match fs::symlink_metadata(&link.destination) {
            Ok(_) => Err(LinkActionError::Io {
                operation: "clean prepared link",
                path: link.destination.clone(),
                source: io::Error::other(
                    "refusing path-based cleanup of prepared link destination",
                ),
            }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(LinkActionError::Io {
                operation: "inspect prepared link for cleanup",
                path: link.destination.clone(),
                source,
            }),
        }
    }
}

fn cleanup_created_root(root: &CreatedRoot) -> Result<(), LinkActionError> {
    #[cfg(unix)]
    {
        cleanup_created_entry_in_fd(
            &root.parent_fd,
            &root.basename,
            root.identity,
            None,
            &root.path,
            "prepared link root",
            AtFlags::REMOVEDIR,
        )
    }
    #[cfg(not(unix))]
    {
        match fs::symlink_metadata(&root.path) {
            Ok(_) => Err(LinkActionError::Io {
                operation: "clean prepared link root",
                path: root.path.clone(),
                source: io::Error::other("refusing path-based cleanup of prepared link root"),
            }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(LinkActionError::Io {
                operation: "inspect prepared link root for cleanup",
                path: root.path.clone(),
                source,
            }),
        }
    }
}

#[cfg(unix)]
fn cleanup_created_entry_in_fd(
    parent_fd: &OwnedFd,
    basename: &Path,
    identity: FileIdentity,
    symlink_target: Option<&Path>,
    display_path: &Path,
    label: &'static str,
    unlink_flags: AtFlags,
) -> Result<(), LinkActionError> {
    // The retained directory fd pins the parent we created in, and the identity
    // check prevents stale or replaced entries from being removed. POSIX does
    // not provide an atomic "unlink only if this inode still matches" operation,
    // so another writer with access to the same directory could still swap the
    // basename between this check and unlinkat.
    match entry_matches_cleanup_identity(parent_fd, basename, identity, symlink_target) {
        Ok(true) => rustix::fs::unlinkat(parent_fd, basename, unlink_flags).map_err(|source| {
            LinkActionError::Io {
                operation: "clean prepared link entry",
                path: display_path.to_path_buf(),
                source: io::Error::from(source),
            }
        }),
        Ok(false) => Err(LinkActionError::Io {
            operation: "verify prepared link entry before cleanup",
            path: display_path.to_path_buf(),
            source: io::Error::other(format!("{label} was replaced before cleanup")),
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(LinkActionError::Io {
            operation: "inspect prepared link entry for cleanup",
            path: display_path.to_path_buf(),
            source,
        }),
    }
}

#[cfg(unix)]
fn cleanup_created_entry_at_io(parent: &DestinationParent, entry: &CreatedEntry) -> io::Result<()> {
    if entry_matches_cleanup_identity(
        &parent.fd,
        &parent.basename,
        entry.identity,
        entry.symlink_target.as_deref(),
    )? {
        rustix::fs::unlinkat(&parent.fd, &parent.basename, AtFlags::empty())
            .map_err(io::Error::from)
    } else {
        Err(io::Error::other(
            "fd-created link was replaced before cleanup",
        ))
    }
}

fn prepared_link_manifest(
    source: PathBuf,
    destination: PathBuf,
    link_type: LinkType,
    expected_size: ByteSize,
    source_file: &VerifiedSourceFile,
    parent: &DestinationParent,
    destination_match_mode: DestinationMatchMode,
) -> Result<PreparedLink, LinkActionError> {
    Ok(PreparedLink {
        source,
        destination,
        link_type,
        expected_size,
        #[cfg(unix)]
        source_identity: metadata_identity(&source_file.metadata),
        destination_match_mode,
        parent: parent.path.clone(),
        basename: parent.basename.clone(),
        #[cfg(unix)]
        parent_identity: directory_identity_from_parent(parent)?,
    })
}

#[cfg(unix)]
fn directory_identity_from_parent(
    parent: &DestinationParent,
) -> Result<FileIdentity, LinkActionError> {
    let file = File::from(
        rustix::fs::openat(
            &parent.fd,
            Path::new("."),
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io::Error::from)
        .map_err(|source| LinkActionError::Io {
            operation: "reopen prepared link parent directory",
            path: parent.path.clone(),
            source,
        })?,
    );
    file_identity(&file.metadata().map_err(|source| LinkActionError::Io {
        operation: "record prepared link parent identity",
        path: parent.path.clone(),
        source,
    })?)
}

#[cfg(unix)]
fn cleanup_parent_fd(
    parent: &DestinationParent,
    cache: &mut BTreeMap<(PathBuf, FileIdentity), Arc<OwnedFd>>,
    retained_root_fds: usize,
) -> Result<Arc<OwnedFd>, LinkActionError> {
    let identity = directory_identity_from_parent(parent)?;
    let key = (parent.path.clone(), identity);
    if let Some(fd) = cache.get(&key) {
        return Ok(Arc::clone(fd));
    }
    cleanup_handle_count(retained_root_fds, cache.len(), 1).map_err(|source| {
        LinkActionError::Io {
            operation: "retain prepared link cleanup handle",
            path: parent.path.clone(),
            source,
        }
    })?;
    let fd = Arc::new(
        duplicate_directory_fd(parent).map_err(|source| LinkActionError::Io {
            operation: "duplicate prepared link parent handle",
            path: parent.path.clone(),
            source,
        })?,
    );
    cache.insert(key, Arc::clone(&fd));
    Ok(fd)
}

#[cfg(unix)]
fn cleanup_handle_count(root_fds: usize, parent_fds: usize, additional: usize) -> io::Result<()> {
    let count = root_fds
        .checked_add(parent_fds)
        .and_then(|count| count.checked_add(additional))
        .ok_or_else(|| io::Error::other("prepared link cleanup handle count overflow"))?;
    if count > MAX_PREPARED_CLEANUP_FDS {
        Err(io::Error::other("too many prepared link cleanup handles"))
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn file_identity(metadata: &fs::Metadata) -> Result<FileIdentity, LinkActionError> {
    Ok(FileIdentity {
        dev: metadata.dev(),
        ino: metadata.ino(),
    })
}

#[cfg(unix)]
fn entry_identity_in_fd(parent_fd: &OwnedFd, path: &Path) -> io::Result<FileIdentity> {
    let stat =
        rustix::fs::statat(parent_fd, path, AtFlags::SYMLINK_NOFOLLOW).map_err(io::Error::from)?;
    Ok(FileIdentity {
        dev: u64::try_from(stat.st_dev).map_err(io::Error::other)?,
        ino: stat.st_ino,
    })
}

#[cfg(unix)]
fn entry_matches_cleanup_identity(
    parent_fd: &OwnedFd,
    path: &Path,
    identity: FileIdentity,
    symlink_target: Option<&Path>,
) -> io::Result<bool> {
    if entry_identity_in_fd(parent_fd, path)? != identity {
        return Ok(false);
    }
    if let Some(expected) = symlink_target {
        return read_symlink_target_in_fd(parent_fd, path).map(|actual| actual == expected);
    }
    Ok(true)
}

#[cfg(unix)]
fn read_symlink_target_in_fd(parent_fd: &OwnedFd, path: &Path) -> io::Result<PathBuf> {
    let target = rustix::fs::readlinkat(parent_fd, path, Vec::new()).map_err(io::Error::from)?;
    Ok(PathBuf::from(OsString::from_vec(target.into_bytes())))
}

#[cfg(unix)]
fn duplicate_directory_fd(parent: &DestinationParent) -> io::Result<OwnedFd> {
    duplicate_directory_fd_from(&parent.fd)
}

#[cfg(unix)]
fn duplicate_directory_fd_from(parent_fd: &OwnedFd) -> io::Result<OwnedFd> {
    rustix::fs::openat(
        parent_fd,
        Path::new("."),
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(io::Error::from)
}

#[cfg(unix)]
fn clean_new_directory_after_open_error(
    parent_fd: &OwnedFd,
    basename: &Path,
    path: &Path,
    earlier_roots: &[CreatedRoot],
    error: io::Error,
) -> io::Error {
    let remove_new =
        rustix::fs::unlinkat(parent_fd, basename, AtFlags::REMOVEDIR).map_err(io::Error::from);
    let cleanup_earlier = cleanup_created_roots(earlier_roots);
    match (remove_new, cleanup_earlier) {
        (Ok(()), Ok(())) => error,
        (remove_result, cleanup_result) => io::Error::other(format!(
            "failed to clean created link directory {} after open error: {}; new directory cleanup: {}; earlier root cleanup: {}",
            path.display(),
            error,
            cleanup_io_result_description(remove_result),
            cleanup_link_result_description(cleanup_result),
        )),
    }
}

#[cfg(unix)]
fn clean_created_roots_after_open_error(roots: &[CreatedRoot], error: io::Error) -> io::Error {
    match cleanup_created_roots(roots) {
        Ok(()) => error,
        Err(cleanup_error) => io::Error::other(format!(
            "failed to clean created link roots after open error: {error}; cleanup error: {cleanup_error}"
        )),
    }
}

#[cfg(unix)]
fn cleanup_io_result_description(result: io::Result<()>) -> String {
    result
        .map(|()| "completed".to_string())
        .unwrap_or_else(|error| error.to_string())
}

#[cfg(unix)]
fn cleanup_link_result_description(result: Result<(), LinkActionError>) -> String {
    result
        .map(|()| "completed".to_string())
        .unwrap_or_else(|error| error.to_string())
}

#[cfg(unix)]
fn open_destination_parent_at(
    destination_dir: &Path,
    components: &[PathBuf],
    create_missing: bool,
    retained_cleanup_handles: usize,
) -> io::Result<(OwnedFd, Vec<CreatedRoot>)> {
    let mut current = if create_missing {
        open_or_create_directory_path(destination_dir)?
    } else {
        open_directory_path(destination_dir, false)?
    };
    let mut created_roots = Vec::new();
    let mut current_path = destination_dir.to_path_buf();
    for component in components {
        match open_child_directory(&current, component) {
            Ok(next) => current = next,
            Err(error) if create_missing && error.kind() == io::ErrorKind::NotFound => {
                if let Err(error) =
                    rustix::fs::mkdirat(&current, component, Mode::from_raw_mode(0o777))
                        .map_err(io::Error::from)
                {
                    return if created_roots.is_empty() {
                        Err(error)
                    } else {
                        Err(clean_created_roots_after_open_error(&created_roots, error))
                    };
                }
                current_path.push(component);
                if let Err(error) =
                    cleanup_handle_count(created_roots.len(), retained_cleanup_handles, 1)
                {
                    return Err(clean_new_directory_after_open_error(
                        &current,
                        component,
                        &current_path,
                        &created_roots,
                        error,
                    ));
                }
                let root = match created_root_at(&current_path, &current, component) {
                    Ok(root) => root,
                    Err(error) => {
                        return Err(clean_new_directory_after_open_error(
                            &current,
                            component,
                            &current_path,
                            &created_roots,
                            error,
                        ));
                    }
                };
                created_roots.push(root);
                current = match open_child_directory(&current, component) {
                    Ok(next) => next,
                    Err(error) => {
                        return Err(clean_created_roots_after_open_error(&created_roots, error));
                    }
                };
                continue;
            }
            Err(error) => {
                return if created_roots.is_empty() {
                    Err(error)
                } else {
                    Err(clean_created_roots_after_open_error(&created_roots, error))
                };
            }
        }
        current_path.push(component);
    }
    Ok((current, created_roots))
}

#[cfg(unix)]
fn open_destination_parent_in_selected_root(
    link_root: &SelectedLinkDir,
    destination_dir: &Path,
    components: &[PathBuf],
    create_missing: bool,
    retained_cleanup_handles: usize,
) -> io::Result<(OwnedFd, Vec<CreatedRoot>)> {
    let mut relative_components: Vec<PathBuf> = destination_dir
        .strip_prefix(&link_root.path)
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("destination directory is outside the selected link root: {error}"),
            )
        })?
        .components()
        .map(|component| match component {
            std::path::Component::Normal(name) => Ok(PathBuf::from(name)),
            std::path::Component::CurDir => Ok(PathBuf::new()),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "destination directory contains unsafe component",
            )),
        })
        .filter(|component| match component {
            Ok(component) => !component.as_os_str().is_empty(),
            Err(_) => true,
        })
        .collect::<io::Result<Vec<_>>>()?;
    relative_components.extend_from_slice(components);

    let root = open_directory_path(&link_root.path, false)?;
    let root_file = File::from(
        rustix::fs::openat(
            &root,
            Path::new("."),
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io::Error::from)?,
    );
    let metadata = root_file.metadata()?;
    let root_identity = FileIdentity {
        dev: metadata.dev(),
        ino: metadata.ino(),
    };
    if root_identity != link_root.identity {
        return Err(io::Error::other(
            "selected link root changed before link preparation",
        ));
    }

    open_child_directories_at(
        root,
        &link_root.path,
        &relative_components,
        create_missing,
        retained_cleanup_handles,
    )
}

#[cfg(unix)]
fn open_child_directories_at(
    mut current: OwnedFd,
    base_path: &Path,
    components: &[PathBuf],
    create_missing: bool,
    retained_cleanup_handles: usize,
) -> io::Result<(OwnedFd, Vec<CreatedRoot>)> {
    let mut created_roots = Vec::new();
    let mut current_path = base_path.to_path_buf();
    for component in components {
        match open_child_directory(&current, component) {
            Ok(next) => current = next,
            Err(error) if create_missing && error.kind() == io::ErrorKind::NotFound => {
                if let Err(error) =
                    rustix::fs::mkdirat(&current, component, Mode::from_raw_mode(0o777))
                        .map_err(io::Error::from)
                {
                    return if created_roots.is_empty() {
                        Err(error)
                    } else {
                        Err(clean_created_roots_after_open_error(&created_roots, error))
                    };
                }
                current_path.push(component);
                if let Err(error) =
                    cleanup_handle_count(created_roots.len(), retained_cleanup_handles, 1)
                {
                    return Err(clean_new_directory_after_open_error(
                        &current,
                        component,
                        &current_path,
                        &created_roots,
                        error,
                    ));
                }
                let root = match created_root_at(&current_path, &current, component) {
                    Ok(root) => root,
                    Err(error) => {
                        return Err(clean_new_directory_after_open_error(
                            &current,
                            component,
                            &current_path,
                            &created_roots,
                            error,
                        ));
                    }
                };
                created_roots.push(root);
                current = match open_child_directory(&current, component) {
                    Ok(next) => next,
                    Err(error) => {
                        return Err(clean_created_roots_after_open_error(&created_roots, error));
                    }
                };
                continue;
            }
            Err(error) => {
                return if created_roots.is_empty() {
                    Err(error)
                } else {
                    Err(clean_created_roots_after_open_error(&created_roots, error))
                };
            }
        }
        current_path.push(component);
    }
    Ok((current, created_roots))
}

#[cfg(unix)]
fn open_existing_directory(path: &Path) -> io::Result<File> {
    open_directory_path(path, false).map(File::from)
}

#[cfg(unix)]
fn open_or_create_directory_path(path: &Path) -> io::Result<OwnedFd> {
    open_directory_path(path, true)
}

#[cfg(unix)]
fn open_directory_path(path: &Path, create_missing: bool) -> io::Result<OwnedFd> {
    let mut components = path.components();
    let mut current = if path.is_absolute() {
        match components.next() {
            Some(std::path::Component::RootDir) => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "absolute directory path did not start at root",
                ));
            }
        }
        rustix::fs::openat(
            CWD,
            Path::new("/"),
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io::Error::from)?
    } else {
        rustix::fs::openat(
            CWD,
            Path::new("."),
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io::Error::from)?
    };

    for component in components {
        let std::path::Component::Normal(name) = component else {
            if matches!(component, std::path::Component::CurDir) {
                continue;
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "directory path contains unsafe component",
            ));
        };
        let child = Path::new(name);
        match open_child_directory(&current, child) {
            Ok(next) => current = next,
            Err(error) if create_missing && error.kind() == io::ErrorKind::NotFound => {
                rustix::fs::mkdirat(&current, child, Mode::from_raw_mode(0o777))
                    .map_err(io::Error::from)?;
                current = open_child_directory(&current, child)?;
            }
            Err(error) => return Err(error),
        }
    }

    Ok(current)
}

#[cfg(unix)]
fn open_child_directory(parent: &OwnedFd, component: &Path) -> io::Result<OwnedFd> {
    rustix::fs::openat(
        parent,
        component,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        Mode::empty(),
    )
    .map_err(io::Error::from)
}

fn create_link_in_parent(
    source: &Path,
    source_file: &VerifiedSourceFile,
    parent: &DestinationParent,
    link_type: LinkType,
) -> io::Result<CreatedEntry> {
    #[cfg(unix)]
    {
        match link_type {
            LinkType::Hardlink => create_hardlink_staged_at(source, source_file, parent),
            LinkType::Symlink => create_symlink_staged_at(source, source_file, parent),
            LinkType::Reflink => create_reflink_staged_at(source_file, parent, false),
            LinkType::ReflinkOrCopy => create_reflink_staged_at(source_file, parent, true),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = source_file;
        create_link(source, &parent.path.join(&parent.basename), link_type)?;
        Ok(CreatedEntry {})
    }
}

#[cfg(unix)]
fn create_hardlink_staged_at(
    source: &Path,
    source_file: &VerifiedSourceFile,
    parent: &DestinationParent,
) -> io::Result<CreatedEntry> {
    let stage = create_private_stage_dir_at(parent, "link-create")?;
    let staged = Path::new("data");
    if let Err(error) = create_staged_hardlink(source, source_file, &stage, staged) {
        let _ = remove_stage_dir_entry_at(parent, &stage, staged);
        return Err(error);
    }
    publish_staged_entry(parent, &stage, staged)
}

#[cfg(unix)]
fn create_symlink_staged_at(
    source: &Path,
    source_file: &VerifiedSourceFile,
    parent: &DestinationParent,
) -> io::Result<CreatedEntry> {
    let stage = create_private_stage_dir_at(parent, "link-create")?;
    let staged = Path::new("data");
    // Symlinks are necessarily path-based. Preparation and later revalidation
    // pin the source size and identity, but the client can still observe a
    // source path mutation that happens after revalidation.
    if let Err(error) = rustix::fs::symlinkat(source, &stage.fd, staged).map_err(io::Error::from) {
        let _ = remove_stage_dir_entry_at(parent, &stage, staged);
        return Err(error);
    }
    if !source_path_matches_identity(source, &source_file.identity)? {
        let _ = remove_stage_dir_entry_at(parent, &stage, staged);
        return Err(io::Error::other(
            "symlink source changed before link publication",
        ));
    }
    let identity = entry_identity_in_fd(&stage.fd, staged)?;
    rustix::fs::renameat_with(
        &stage.fd,
        staged,
        &parent.fd,
        &parent.basename,
        RenameFlags::NOREPLACE,
    )
    .map_err(io::Error::from)?;
    remove_empty_stage_dir_at(parent, &stage).or_else(|error| {
        cleanup_created_entry_at_io(
            parent,
            &CreatedEntry {
                identity,
                symlink_target: Some(source.to_path_buf()),
            },
        )?;
        Err(error)
    })?;
    Ok(CreatedEntry {
        identity,
        symlink_target: Some(source.to_path_buf()),
    })
}

#[cfg(unix)]
fn create_reflink_staged_at(
    source_file: &VerifiedSourceFile,
    parent: &DestinationParent,
    copy_fallback: bool,
) -> io::Result<CreatedEntry> {
    let stage = create_private_stage_dir_at(parent, "link-create")?;
    let staged = Path::new("data");
    if let Err(error) = create_staged_reflink(source_file, &stage, staged).or_else(|error| {
        if copy_fallback && should_fallback_to_copy(error.kind()) {
            copy_file_to_stage(source_file, &stage, staged).map(|_| ())
        } else {
            Err(error)
        }
    }) {
        let _ = remove_stage_dir_entry_at(parent, &stage, staged);
        return Err(error);
    }

    publish_staged_entry(parent, &stage, staged)
}

#[cfg(all(
    unix,
    any(target_os = "linux", target_os = "android"),
    not(any(target_arch = "sparc", target_arch = "sparc64"))
))]
fn create_staged_reflink(
    source_file: &VerifiedSourceFile,
    stage: &PrivateStageDir,
    staged: &Path,
) -> io::Result<()> {
    let destination = rustix::fs::openat(
        &stage.fd,
        staged,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o666),
    )
    .map_err(io::Error::from)?;
    rustix::fs::ioctl_ficlone(&destination, &source_file.file).map_err(io::Error::from)
}

#[cfg(all(unix, target_vendor = "apple"))]
fn create_staged_reflink(
    source_file: &VerifiedSourceFile,
    stage: &PrivateStageDir,
    staged: &Path,
) -> io::Result<()> {
    rustix::fs::fclonefileat(
        &source_file.file,
        &stage.fd,
        staged,
        rustix::fs::CloneFlags::NOOWNERCOPY,
    )
    .map_err(io::Error::from)
}

#[cfg(all(
    unix,
    not(all(
        any(target_os = "linux", target_os = "android"),
        not(any(target_arch = "sparc", target_arch = "sparc64"))
    )),
    not(target_vendor = "apple")
))]
fn create_staged_reflink(
    _source_file: &VerifiedSourceFile,
    _stage: &PrivateStageDir,
    _staged: &Path,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "fd-relative reflink is not supported on this platform",
    ))
}

#[cfg(unix)]
fn copy_file_to_stage(
    source_file: &VerifiedSourceFile,
    stage: &PrivateStageDir,
    staged: &Path,
) -> io::Result<u64> {
    let destination = rustix::fs::openat(
        &stage.fd,
        staged,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC,
        Mode::from_raw_mode(0o666),
    )
    .map_err(io::Error::from)?;
    let mut source = source_file.file.try_clone()?;
    source.seek(SeekFrom::Start(0))?;
    let mut destination = File::from(destination);
    copy_file_contents(&mut source, &mut destination)
        .and_then(|written| destination.sync_all().map(|()| written))
}

fn should_fallback_to_copy(kind: io::ErrorKind) -> bool {
    !matches!(
        kind,
        io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied | io::ErrorKind::AlreadyExists
    )
}

#[cfg(unix)]
fn create_staged_hardlink(
    source: &Path,
    source_file: &VerifiedSourceFile,
    stage: &PrivateStageDir,
    staged: &Path,
) -> io::Result<()> {
    // POSIX hardlink creation is path-based here; immediately verify that the
    // staged link landed on the source handle accepted by source verification.
    rustix::fs::linkat(CWD, source, &stage.fd, staged, AtFlags::empty())
        .map_err(io::Error::from)?;
    let staged_file = File::from(
        rustix::fs::openat(
            &stage.fd,
            staged,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .map_err(io::Error::from)?,
    );
    let staged_identity = same_file::Handle::from_file(staged_file)?;
    if staged_identity == source_file.identity {
        Ok(())
    } else {
        Err(io::Error::other(
            "staged hardlink did not match verified source",
        ))
    }
}

#[cfg(unix)]
fn publish_staged_entry(
    parent: &DestinationParent,
    stage: &PrivateStageDir,
    staged: &Path,
) -> io::Result<CreatedEntry> {
    let identity = entry_identity_in_fd(&stage.fd, staged)?;
    rustix::fs::linkat(
        &stage.fd,
        staged,
        &parent.fd,
        &parent.basename,
        AtFlags::empty(),
    )
    .map_err(io::Error::from)?;
    remove_stage_dir_entry_at(parent, stage, staged).or_else(|error| {
        cleanup_created_entry_at_io(
            parent,
            &CreatedEntry {
                identity,
                symlink_target: None,
            },
        )?;
        Err(error)
    })?;
    Ok(CreatedEntry {
        identity,
        symlink_target: None,
    })
}

#[cfg(unix)]
fn create_private_stage_dir_at(
    parent: &DestinationParent,
    label: &str,
) -> io::Result<PrivateStageDir> {
    for _ in 0..16 {
        let name = PathBuf::from(OsString::from(format!(
            ".sporos-{label}-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        )));
        match rustix::fs::mkdirat(&parent.fd, &name, Mode::from_raw_mode(0o700)) {
            Ok(()) => {
                let fd = open_child_directory(&parent.fd, &name)?;
                return Ok(PrivateStageDir { name, fd });
            }
            Err(error) if io::Error::from(error).kind() == io::ErrorKind::AlreadyExists => {
                continue;
            }
            Err(error) => return Err(io::Error::from(error)),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "staging directory name collision",
    ))
}

#[cfg(unix)]
fn remove_empty_stage_dir_at(
    parent: &DestinationParent,
    stage: &PrivateStageDir,
) -> io::Result<()> {
    rustix::fs::unlinkat(&parent.fd, &stage.name, AtFlags::REMOVEDIR).map_err(io::Error::from)
}

#[cfg(unix)]
fn remove_stage_dir_entry_at(
    parent: &DestinationParent,
    stage: &PrivateStageDir,
    entry: &Path,
) -> io::Result<()> {
    match rustix::fs::unlinkat(&stage.fd, entry, AtFlags::empty()) {
        Ok(()) => {}
        Err(error) if io::Error::from(error).kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(io::Error::from(error)),
    }
    remove_empty_stage_dir_at(parent, stage)
}

#[cfg(not(unix))]
fn create_reflink_staged(source: &Path, destination: &Path, copy_fallback: bool) -> io::Result<()> {
    let parent = destination
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "destination has no parent"))?;
    let stage_dir = create_private_stage_dir(parent, "link-create")?;
    let staged = stage_dir.join("data");
    let create_result = match reflink_copy::reflink(source, &staged) {
        Ok(()) => Ok(()),
        Err(error)
            if copy_fallback
                && !matches!(
                    error.kind(),
                    io::ErrorKind::NotFound
                        | io::ErrorKind::PermissionDenied
                        | io::ErrorKind::AlreadyExists
                ) =>
        {
            copy_file_no_overwrite(source, &staged).map(|_| ())
        }
        Err(error) => Err(error),
    };

    match create_result.and_then(|()| fs::hard_link(&staged, destination)) {
        Ok(()) => {
            let _ = remove_stage_dir(&stage_dir);
            Ok(())
        }
        Err(error) => {
            let _ = remove_stage_dir(&stage_dir);
            Err(error)
        }
    }
}

#[cfg(not(unix))]
fn create_reflink_or_copy_no_overwrite(source: &Path, destination: &Path) -> io::Result<()> {
    create_reflink_staged(source, destination, true)
}

#[cfg(not(unix))]
fn create_link(source: &Path, destination: &Path, link_type: LinkType) -> io::Result<()> {
    match link_type {
        LinkType::Hardlink => fs::hard_link(source, destination),
        LinkType::Symlink => symlink_file(source, destination),
        LinkType::Reflink => create_reflink_staged(source, destination, false),
        LinkType::ReflinkOrCopy => create_reflink_or_copy_no_overwrite(source, destination),
    }
}

#[cfg(not(unix))]
fn create_private_stage_dir(parent: &Path, label: &str) -> io::Result<PathBuf> {
    for _ in 0..16 {
        let path = parent.join(format!(
            ".sporos-{label}-{}-{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        match create_private_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "staging directory name collision",
    ))
}

#[cfg(not(unix))]
fn create_private_dir(path: &Path) -> io::Result<()> {
    DirBuilder::new().create(path)
}

#[cfg(not(unix))]
fn remove_stage_dir(path: &Path) -> io::Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
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

    #[cfg(unix)]
    #[test]
    fn link_dir_selection_rejects_symlink_root_component() {
        let root = unique_temp_dir("link-dir-symlink-root");
        let source_dir = root.join("source");
        let outside = root.join("outside");
        fs::create_dir_all(&source_dir).unwrap();
        fs::create_dir_all(outside.join("links")).unwrap();
        fs::write(source_dir.join("episode.mkv"), b"data").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("redirect")).unwrap();

        let error = select_link_dir(
            &source_dir,
            &[root.join("redirect/links")],
            LinkDirOptions {
                link_type: LinkType::Hardlink,
                max_directory_entries: 16,
            },
        )
        .unwrap_err();

        assert!(matches!(error, LinkActionError::Io { .. }));
        assert_eq!(0, fs::read_dir(outside.join("links")).unwrap().count());

        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn link_action_rejects_selected_root_replacement() {
        let root = unique_temp_dir("link-root-replaced");
        let source = root.join("source");
        let link_root = root.join("links");
        let old_link_root = root.join("links-old");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::create_dir_all(&link_root).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"source").unwrap();

        let selected = select_link_dir_pinned(
            &source,
            std::slice::from_ref(&link_root),
            LinkDirOptions::new(LinkType::Hardlink),
        )
        .unwrap();
        let destination = link_destination_dir(selected.path(), "tracker", false).unwrap();
        fs::rename(&link_root, &old_link_root).unwrap();
        fs::create_dir_all(&link_root).unwrap();

        let error = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 6, 0)],
            &[torrent_file("Show/Episode.mkv", 6, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Hardlink).with_link_root(selected),
        )
        .unwrap_err();

        assert!(matches!(error, LinkActionError::Io { .. }));
        assert!(!link_root.join("tracker/Show/Episode.mkv").exists());
        assert!(!old_link_root.join("tracker/Show/Episode.mkv").exists());

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
            std::slice::from_ref(&link_dir),
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
        assert_eq!(1, outcome.created_roots.len());
        assert_eq!(destination.join("Show"), outcome.created_roots[0].path);
        assert_eq!(
            fs::metadata(source.join("Show/Episode.mkv")).unwrap().len(),
            fs::metadata(destination.join("Show/Episode.mkv"))
                .unwrap()
                .len()
        );

        cleanup_created_links_and_roots(&outcome.created_links, &outcome.created_roots).unwrap();
        assert!(!destination.join("Show/Episode.mkv").exists());
        assert!(!destination.join("Show").exists());
        remove_temp_dir(&root);
    }

    #[test]
    fn link_action_cleans_nested_roots_bottom_up() {
        let root = unique_temp_dir("link-nested-roots");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show/Season")).unwrap();
        fs::write(source.join("Show/Season/Episode.mkv"), b"episode").unwrap();

        let outcome = link_metafile_files(
            &source,
            &[local_file("Show/Season/Episode.mkv", 7, 0)],
            &[torrent_file("Show/Season/Episode.mkv", 7, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Hardlink),
        )
        .unwrap();

        assert_eq!(2, outcome.created_roots.len());
        assert_eq!(destination.join("Show"), outcome.created_roots[0].path);
        assert_eq!(
            destination.join("Show/Season"),
            outcome.created_roots[1].path
        );

        cleanup_created_links_and_roots(&outcome.created_links, &outcome.created_roots).unwrap();
        assert!(!destination.join("Show/Season/Episode.mkv").exists());
        assert!(!destination.join("Show/Season").exists());
        assert!(!destination.join("Show").exists());
        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_created_roots_keeps_same_path_latest_identity() {
        let root = unique_temp_dir("link-root-recreated");
        let destination = root.join("destination");
        fs::create_dir_all(&destination).unwrap();
        let parent_fd = open_directory_path(&destination, false).unwrap();
        rustix::fs::mkdirat(&parent_fd, Path::new("Show"), Mode::from_raw_mode(0o777)).unwrap();
        let first =
            created_root_at(&destination.join("Show"), &parent_fd, Path::new("Show")).unwrap();
        rustix::fs::unlinkat(&parent_fd, Path::new("Show"), AtFlags::REMOVEDIR).unwrap();
        rustix::fs::mkdirat(&parent_fd, Path::new("Show"), Mode::from_raw_mode(0o777)).unwrap();
        let second =
            created_root_at(&destination.join("Show"), &parent_fd, Path::new("Show")).unwrap();

        cleanup_created_roots(&[first, second]).unwrap();

        assert!(!destination.join("Show").exists());
        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn link_action_shares_cleanup_parent_handles() {
        let root = unique_temp_dir("link-shared-cleanup-parent");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::write(source.join("Show/First.mkv"), b"first").unwrap();
        fs::write(source.join("Show/Second.mkv"), b"second").unwrap();

        let outcome = link_metafile_files(
            &source,
            &[
                local_file("Show/First.mkv", 5, 0),
                local_file("Show/Second.mkv", 6, 1),
            ],
            &[
                torrent_file("Show/First.mkv", 5, 0),
                torrent_file("Show/Second.mkv", 6, 1),
            ],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Hardlink),
        )
        .unwrap();

        assert_eq!(2, outcome.created_links.len());
        assert!(Arc::ptr_eq(
            &outcome.created_links[0].parent_fd,
            &outcome.created_links[1].parent_fd
        ));

        cleanup_created_links_and_roots(&outcome.created_links, &outcome.created_roots).unwrap();
        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn link_action_caps_retained_cleanup_handles() {
        let root = unique_temp_dir("link-cleanup-handle-cap");
        let source = root.join("source");
        let destination = root.join("destination");
        let mut local_files = Vec::new();
        let mut torrent_files = Vec::new();
        for index in 0..(MAX_PREPARED_CLEANUP_FDS + 1) {
            let path = format!("Dir{index}/Episode.mkv");
            fs::create_dir_all(source.join(format!("Dir{index}"))).unwrap();
            fs::write(source.join(&path), b"episode").unwrap();
            let index = u32::try_from(index).unwrap();
            local_files.push(local_file(&path, 7, index));
            torrent_files.push(torrent_file(&path, 7, index));
        }

        let error = link_metafile_files(
            &source,
            &local_files,
            &torrent_files,
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Hardlink),
        )
        .unwrap_err();

        assert!(matches!(error, LinkActionError::Io { .. }));
        assert_eq!(0, fs::read_dir(&destination).unwrap().count());
        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn link_action_caps_deep_retained_cleanup_handles() {
        let root = unique_temp_dir("link-deep-cleanup-handle-cap");
        let source = root.join("source");
        let destination = root.join("destination");
        let mut relative = PathBuf::new();
        for index in 0..(MAX_PREPARED_CLEANUP_FDS + 1) {
            relative.push(format!("Dir{index}"));
        }
        relative.push("Episode.mkv");
        fs::create_dir_all(source.join(relative.parent().unwrap())).unwrap();
        fs::write(source.join(&relative), b"episode").unwrap();
        let path = relative.to_string_lossy();

        let error = link_metafile_files(
            &source,
            &[local_file(&path, 7, 0)],
            &[torrent_file(&path, 7, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Hardlink),
        )
        .unwrap_err();

        assert!(matches!(error, LinkActionError::Io { .. }));
        assert_eq!(0, fs::read_dir(&destination).unwrap().count());
        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn staged_hardlink_rejects_source_swap_before_publish() {
        let root = unique_temp_dir("link-hardlink-source-swap");
        let source = root.join("source.mkv");
        let destination = root.join("destination");
        fs::create_dir_all(&destination).unwrap();
        fs::write(&source, b"source").unwrap();
        let source_file =
            verified_source_file_without_expected_size(&source, LinkType::Hardlink).unwrap();
        fs::remove_file(&source).unwrap();
        fs::write(&source, b"replacement").unwrap();
        let parent =
            prepare_destination_parent(&destination, Path::new("Episode.mkv"), true, None).unwrap();

        let error = create_hardlink_staged_at(&source, &source_file, &parent).unwrap_err();

        assert_eq!(io::ErrorKind::Other, error.kind());
        assert!(!destination.join("Episode.mkv").exists());
        assert_eq!(0, fs::read_dir(&destination).unwrap().count());

        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn fd_created_link_cleanup_handles_parent_path_replacement() {
        let root = unique_temp_dir("link-parent-swap-cleanup");
        let source = root.join("source.mkv");
        let destination = root.join("destination");
        let old_destination = root.join("destination-old");
        fs::create_dir_all(&destination).unwrap();
        fs::write(&source, b"source").unwrap();
        let source_file =
            verified_source_file_without_expected_size(&source, LinkType::Hardlink).unwrap();
        let parent =
            prepare_destination_parent(&destination, Path::new("Episode.mkv"), true, None).unwrap();
        fs::rename(&destination, &old_destination).unwrap();
        fs::create_dir_all(&destination).unwrap();

        let created =
            create_link_in_parent(&source, &source_file, &parent, LinkType::Hardlink).unwrap();
        let error = verify_created_entry_path(&parent, &created, &destination.join("Episode.mkv"))
            .unwrap_err();
        cleanup_created_entry_at(&parent, &created).unwrap();

        assert!(matches!(error, LinkActionError::Io { .. }));
        assert_eq!(0, fs::read_dir(&destination).unwrap().count());
        assert_eq!(0, fs::read_dir(&old_destination).unwrap().count());

        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn fd_created_link_cleanup_reports_replaced_entry() {
        let root = unique_temp_dir("link-created-entry-replaced");
        let source = root.join("source.mkv");
        let destination = root.join("destination");
        fs::create_dir_all(&destination).unwrap();
        fs::write(&source, b"source").unwrap();
        let source_file =
            verified_source_file_without_expected_size(&source, LinkType::Hardlink).unwrap();
        let parent =
            prepare_destination_parent(&destination, Path::new("Episode.mkv"), true, None).unwrap();
        let created =
            create_link_in_parent(&source, &source_file, &parent, LinkType::Hardlink).unwrap();
        fs::remove_file(destination.join("Episode.mkv")).unwrap();
        fs::write(destination.join("Episode.mkv"), b"replacement").unwrap();

        let error = cleanup_created_entry_at(&parent, &created).unwrap_err();

        assert!(matches!(error, LinkActionError::Io { .. }));
        assert_eq!(
            b"replacement",
            fs::read(destination.join("Episode.mkv"))
                .unwrap()
                .as_slice()
        );

        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn staged_symlink_rejects_source_swap_before_publish() {
        let root = unique_temp_dir("link-symlink-source-swap");
        let source = root.join("source.mkv");
        let destination = root.join("destination");
        fs::create_dir_all(&destination).unwrap();
        fs::write(&source, b"source").unwrap();
        let source_file =
            verified_source_file_without_expected_size(&source, LinkType::Symlink).unwrap();
        fs::remove_file(&source).unwrap();
        fs::write(&source, b"replacement").unwrap();
        let parent =
            prepare_destination_parent(&destination, Path::new("Episode.mkv"), true, None).unwrap();

        let error =
            create_link_in_parent(&source, &source_file, &parent, LinkType::Symlink).unwrap_err();

        assert_eq!(io::ErrorKind::Other, error.kind());
        assert!(!destination.join("Episode.mkv").exists());
        assert_eq!(0, fs::read_dir(&destination).unwrap().count());

        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn published_symlink_cleanup_handles_source_swap_after_publish() {
        let root = unique_temp_dir("link-symlink-post-publish-swap");
        let source = root.join("source.mkv");
        let destination = root.join("destination");
        fs::create_dir_all(&destination).unwrap();
        fs::write(&source, b"source").unwrap();
        let source_file =
            verified_source_file_without_expected_size(&source, LinkType::Symlink).unwrap();
        let parent =
            prepare_destination_parent(&destination, Path::new("Episode.mkv"), true, None).unwrap();
        let created =
            create_link_in_parent(&source, &source_file, &parent, LinkType::Symlink).unwrap();
        fs::remove_file(&source).unwrap();
        fs::write(&source, b"replacement").unwrap();

        let error = existing_link_destination_matches(
            &source,
            &source_file,
            &destination.join("Episode.mkv"),
            LinkType::Symlink,
            DestinationMatchMode::VerifyPrepared,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            LinkActionError::ExistingDestinationMismatch { .. }
        ));
        cleanup_created_entry_at(&parent, &created).unwrap();
        assert!(!destination.join("Episode.mkv").exists());

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
        fs::hard_link(
            source.join("Show/Episode.mkv"),
            destination.join("Show/Episode.mkv"),
        )
        .unwrap();

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
        cleanup_created_links_and_roots(&outcome.created_links, &outcome.created_roots).unwrap();
        assert_eq!(
            b"source",
            fs::read(destination.join("Show/Episode.mkv"))
                .unwrap()
                .as_slice()
        );

        remove_temp_dir(&root);
    }

    #[test]
    fn link_action_rejects_stale_existing_destinations() {
        let root = unique_temp_dir("link-stale-existing");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::create_dir_all(destination.join("Show")).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"source").unwrap();
        fs::write(destination.join("Show/Episode.mkv"), b"stale").unwrap();

        let error = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 6, 0)],
            &[torrent_file("Show/Episode.mkv", 6, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Hardlink),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            LinkActionError::ExistingDestinationMismatch { .. }
        ));
        assert_eq!(
            b"stale",
            fs::read(destination.join("Show/Episode.mkv"))
                .unwrap()
                .as_slice()
        );

        remove_temp_dir(&root);
    }

    #[test]
    fn link_action_rejects_source_size_mismatch_before_creating_destination() {
        let root = unique_temp_dir("link-source-size-mismatch");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"too-large").unwrap();

        let error = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 6, 0)],
            &[torrent_file("Show/Episode.mkv", 6, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Hardlink),
        )
        .unwrap_err();

        assert!(matches!(error, LinkActionError::Io { .. }));
        assert!(!destination.exists());
        remove_temp_dir(&root);
    }

    #[test]
    fn link_action_rejects_same_size_reflink_or_copy_destinations() {
        let root = unique_temp_dir("link-stale-copy-existing");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::create_dir_all(destination.join("Show")).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"source").unwrap();
        fs::write(destination.join("Show/Episode.mkv"), b"stale!").unwrap();

        let error = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 6, 0)],
            &[torrent_file("Show/Episode.mkv", 6, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::ReflinkOrCopy),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            LinkActionError::ExistingDestinationMismatch { .. }
        ));
        assert_eq!(
            b"stale!",
            fs::read(destination.join("Show/Episode.mkv"))
                .unwrap()
                .as_slice()
        );

        remove_temp_dir(&root);
    }

    #[test]
    fn link_action_rejects_matching_plain_reflink_or_copy_destinations() {
        let root = unique_temp_dir("link-matching-copy-existing");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::create_dir_all(destination.join("Show")).unwrap();
        let bytes = vec![b'x'; LINK_COMPARE_BUFFER_SIZE * 2 + 17];
        fs::write(source.join("Show/Episode.mkv"), &bytes).unwrap();
        fs::write(destination.join("Show/Episode.mkv"), &bytes).unwrap();

        let error = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", bytes.len() as u64, 0)],
            &[torrent_file("Show/Episode.mkv", bytes.len() as u64, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::ReflinkOrCopy),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            LinkActionError::ExistingDestinationMismatch { .. }
        ));
        assert_eq!(
            bytes,
            fs::read(destination.join("Show/Episode.mkv")).unwrap()
        );

        remove_temp_dir(&root);
    }

    #[test]
    fn link_action_accepts_existing_hardlink_in_reflink_or_copy_mode() {
        let root = unique_temp_dir("link-hardlinked-copy-existing");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::create_dir_all(destination.join("Show")).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"source").unwrap();
        fs::hard_link(
            source.join("Show/Episode.mkv"),
            destination.join("Show/Episode.mkv"),
        )
        .unwrap();

        let outcome = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 6, 0)],
            &[torrent_file("Show/Episode.mkv", 6, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::ReflinkOrCopy),
        )
        .unwrap();

        assert!(outcome.created_links.is_empty());
        assert!(outcome.already_existing);

        remove_temp_dir(&root);
    }

    #[test]
    fn prepared_reflink_or_copy_revalidation_rejects_replaced_reused_hardlink() {
        let root = unique_temp_dir("link-reused-hardlink-replaced");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::create_dir_all(destination.join("Show")).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"source").unwrap();
        fs::hard_link(
            source.join("Show/Episode.mkv"),
            destination.join("Show/Episode.mkv"),
        )
        .unwrap();
        let outcome = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 6, 0)],
            &[torrent_file("Show/Episode.mkv", 6, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::ReflinkOrCopy),
        )
        .unwrap();
        assert!(outcome.created_links.is_empty());
        fs::remove_file(destination.join("Show/Episode.mkv")).unwrap();
        fs::write(destination.join("Show/Episode.mkv"), b"source").unwrap();

        let error = validate_prepared_links(&outcome.prepared_links).unwrap_err();

        assert!(matches!(
            error,
            LinkActionError::ExistingDestinationMismatch { .. }
        ));
        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn prepared_reflink_or_copy_accepts_existing_symlink_and_rejects_replacement() {
        let root = unique_temp_dir("link-reused-symlink-replaced");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::create_dir_all(destination.join("Show")).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"source").unwrap();
        std::os::unix::fs::symlink(
            source.join("Show/Episode.mkv"),
            destination.join("Show/Episode.mkv"),
        )
        .unwrap();
        let outcome = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 6, 0)],
            &[torrent_file("Show/Episode.mkv", 6, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::ReflinkOrCopy),
        )
        .unwrap();
        assert!(outcome.created_links.is_empty());
        validate_prepared_links(&outcome.prepared_links).unwrap();
        fs::remove_file(destination.join("Show/Episode.mkv")).unwrap();
        fs::write(destination.join("Show/Episode.mkv"), b"source").unwrap();

        let error = validate_prepared_links(&outcome.prepared_links).unwrap_err();

        assert!(matches!(
            error,
            LinkActionError::ExistingDestinationMismatch { .. }
        ));
        remove_temp_dir(&root);
    }

    #[test]
    fn copy_link_fallback_does_not_overwrite_existing_destinations() {
        let root = unique_temp_dir("link-copy-no-overwrite");
        let source = root.join("source.mkv");
        let destination = root.join("destination.mkv");
        fs::create_dir_all(&root).unwrap();
        fs::write(&source, b"source").unwrap();
        fs::write(&destination, b"stale").unwrap();

        let error = copy_file_no_overwrite(&source, &destination).unwrap_err();

        assert_eq!(io::ErrorKind::AlreadyExists, error.kind());
        assert_eq!(b"stale", fs::read(&destination).unwrap().as_slice());

        remove_temp_dir(&root);
    }

    #[test]
    fn cleanup_created_links_refuses_replaced_destinations() {
        let root = unique_temp_dir("link-cleanup-replaced");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"source").unwrap();

        let outcome = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 6, 0)],
            &[torrent_file("Show/Episode.mkv", 6, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::ReflinkOrCopy),
        )
        .unwrap();
        fs::remove_file(destination.join("Show/Episode.mkv")).unwrap();
        fs::write(destination.join("Show/Episode.mkv"), b"replacement").unwrap();

        let error = cleanup_created_links_and_roots(&outcome.created_links, &outcome.created_roots)
            .unwrap_err();

        assert!(matches!(error, LinkActionError::Io { .. }));
        assert_eq!(
            b"replacement",
            fs::read(destination.join("Show/Episode.mkv"))
                .unwrap()
                .as_slice()
        );

        remove_temp_dir(&root);
    }

    #[test]
    fn link_action_rejects_stale_destinations_before_creating_later_links() {
        let root = unique_temp_dir("link-stale-existing-preflight");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::create_dir_all(destination.join("Show")).unwrap();
        fs::write(source.join("Show/First.mkv"), b"first").unwrap();
        fs::write(source.join("Show/Second.mkv"), b"second").unwrap();
        fs::write(destination.join("Show/Second.mkv"), b"stale!").unwrap();

        let error = link_metafile_files(
            &source,
            &[
                local_file("Show/First.mkv", 5, 0),
                local_file("Show/Second.mkv", 6, 1),
            ],
            &[
                torrent_file("Show/First.mkv", 5, 0),
                torrent_file("Show/Second.mkv", 6, 1),
            ],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Hardlink),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            LinkActionError::ExistingDestinationMismatch { .. }
        ));
        assert!(!destination.join("Show/First.mkv").exists());
        assert_eq!(
            b"stale!",
            fs::read(destination.join("Show/Second.mkv"))
                .unwrap()
                .as_slice()
        );

        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn link_action_creates_symlink_destinations() {
        let root = unique_temp_dir("link-create-symlink");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"source").unwrap();

        let outcome = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 6, 0)],
            &[torrent_file("Show/Episode.mkv", 6, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Symlink),
        )
        .unwrap();

        assert_eq!(1, outcome.created_links.len());
        assert_eq!(
            source.join("Show/Episode.mkv"),
            fs::read_link(destination.join("Show/Episode.mkv")).unwrap()
        );

        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn link_action_accepts_matching_symlink_destinations() {
        let root = unique_temp_dir("link-existing-symlink");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::create_dir_all(destination.join("Show")).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"source").unwrap();
        std::os::unix::fs::symlink(
            source.join("Show/Episode.mkv"),
            destination.join("Show/Episode.mkv"),
        )
        .unwrap();

        let outcome = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 6, 0)],
            &[torrent_file("Show/Episode.mkv", 6, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Symlink),
        )
        .unwrap();

        assert!(outcome.created_links.is_empty());
        assert!(outcome.already_existing);

        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn prepared_symlink_revalidation_rejects_same_size_source_replacement() {
        let root = unique_temp_dir("link-symlink-source-replaced");
        let source = root.join("source");
        let destination = root.join("destination");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"source").unwrap();

        let outcome = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 6, 0)],
            &[torrent_file("Show/Episode.mkv", 6, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Symlink),
        )
        .unwrap();
        fs::remove_file(source.join("Show/Episode.mkv")).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"change").unwrap();

        let error = validate_prepared_links(&outcome.prepared_links).unwrap_err();

        assert!(matches!(error, LinkActionError::Io { .. }));
        cleanup_created_links_and_roots(&outcome.created_links, &outcome.created_roots).unwrap();
        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn link_action_rejects_stale_symlink_destinations() {
        let root = unique_temp_dir("link-stale-symlink");
        let source = root.join("source");
        let destination = root.join("destination");
        let other = root.join("other.mkv");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::create_dir_all(destination.join("Show")).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"source").unwrap();
        fs::write(&other, b"other").unwrap();
        std::os::unix::fs::symlink(&other, destination.join("Show/Episode.mkv")).unwrap();

        let error = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 6, 0)],
            &[torrent_file("Show/Episode.mkv", 6, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Symlink),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            LinkActionError::ExistingDestinationMismatch { .. }
        ));
        assert_eq!(
            other,
            fs::read_link(destination.join("Show/Episode.mkv")).unwrap()
        );

        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn link_action_rejects_symlink_destination_directory() {
        let root = unique_temp_dir("link-symlink-parent");
        let source = root.join("source");
        let destination = root.join("destination");
        let outside = root.join("outside");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::create_dir_all(&destination).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"source").unwrap();
        std::os::unix::fs::symlink(&outside, destination.join("Show")).unwrap();

        let error = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 6, 0)],
            &[torrent_file("Show/Episode.mkv", 6, 0)],
            MatchDecision::Exact,
            &destination,
            LinkFilesOptions::new(LinkType::Hardlink),
        )
        .unwrap_err();

        assert!(matches!(error, LinkActionError::Io { .. }));
        assert!(!outside.join("Episode.mkv").exists());

        remove_temp_dir(&root);
    }

    #[cfg(unix)]
    #[test]
    fn link_action_rejects_symlink_destination_root_component() {
        let root = unique_temp_dir("link-symlink-root-component");
        let source = root.join("source");
        let outside = root.join("outside");
        fs::create_dir_all(source.join("Show")).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(source.join("Show/Episode.mkv"), b"source").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("redirect")).unwrap();

        let error = link_metafile_files(
            &source,
            &[local_file("Show/Episode.mkv", 6, 0)],
            &[torrent_file("Show/Episode.mkv", 6, 0)],
            MatchDecision::Exact,
            &root.join("redirect/links"),
            LinkFilesOptions::new(LinkType::Hardlink),
        )
        .unwrap_err();

        assert!(matches!(error, LinkActionError::Io { .. }));
        assert!(!outside.join("links/Show/Episode.mkv").exists());

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
        std::env::temp_dir()
            .canonicalize()
            .unwrap_or_else(|_| std::env::temp_dir())
            .join(format!("sporos-{label}-{nanos}-{}", std::process::id()))
    }

    fn remove_temp_dir(path: &Path) {
        if path.exists() {
            fs::remove_dir_all(path).unwrap();
        }
    }
}
