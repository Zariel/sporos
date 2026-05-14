use std::error::Error;
use std::fmt;
use std::fs::{self, File, FileTimes, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use crate::domain::{MediaType, RemoteCandidate, TorrentMetafile};
use crate::metrics::ActionOutcome;
use crate::persistence::torrent_cache::{
    TorrentCachePathError, TorrentOutputMetadata, torrent_output_path,
};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

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
        info_hash: metafile.info_hash.clone(),
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
        ItemTitle, TorrentFile, TrackerName,
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
        assert_eq!(metafile.info_hash, metadata.info_hash);
        assert!(!metadata.cached);
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
