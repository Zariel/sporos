use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};

use rustix::fd::{AsFd, OwnedFd};
use rustix::fs::{ABS, AtFlags, Mode, OFlags, ResolveFlags, linkat, mkdirat, openat2};
use rustix::io::Errno;
use thiserror::Error;

const DIRECTORY_MODE: Mode = Mode::from_raw_mode(0o750);
const RESOLVE_BENEATH: ResolveFlags = ResolveFlags::BENEATH
    .union(ResolveFlags::NO_MAGICLINKS)
    .union(ResolveFlags::NO_SYMLINKS);
const RESOLVE_ABSOLUTE: ResolveFlags = ResolveFlags::NO_MAGICLINKS.union(ResolveFlags::NO_SYMLINKS);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedLink {
    pub source_root: PathBuf,
    pub source_relative: PathBuf,
    pub destination_relative: PathBuf,
    pub expected_size: u64,
    pub expected_device: Option<u64>,
    pub expected_inode: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedLink {
    pub destination_relative: PathBuf,
    pub device: u64,
    pub inode: u64,
    pub size: u64,
    pub existing: bool,
}

pub struct HardlinkMaterializer {
    link_root: File,
    link_device: u64,
}

impl HardlinkMaterializer {
    pub fn open(link_root: &Path) -> Result<Self, MaterializeError> {
        let link_root =
            open_absolute_directory(link_root).map_err(MaterializeError::OpenLinkRoot)?;
        let metadata = link_root
            .metadata()
            .map_err(MaterializeError::InspectLinkRoot)?;
        if !metadata.is_dir() {
            return Err(MaterializeError::LinkRootNotDirectory);
        }
        Ok(Self {
            link_root,
            link_device: metadata.dev(),
        })
    }

    pub fn materialize(
        &self,
        namespace_relative: &Path,
        links: &[PlannedLink],
    ) -> Result<Vec<MaterializedLink>, MaterializeError> {
        let namespace = ensure_directory(&self.link_root, namespace_relative)?;
        let mut materialized = Vec::with_capacity(links.len());
        for link in links {
            materialized.push(self.materialize_one(&namespace, link)?);
        }
        Ok(materialized)
    }

    fn materialize_one(
        &self,
        namespace: &OwnedFd,
        link: &PlannedLink,
    ) -> Result<MaterializedLink, MaterializeError> {
        validate_relative(&link.source_relative)?;
        validate_relative(&link.destination_relative)?;
        let source_root =
            open_absolute_directory(&link.source_root).map_err(MaterializeError::OpenSourceRoot)?;
        if !source_root
            .metadata()
            .map_err(MaterializeError::InspectSource)?
            .is_dir()
        {
            return Err(MaterializeError::SourceRootNotDirectory);
        }
        let source = openat2(
            &source_root,
            &link.source_relative,
            OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
            RESOLVE_BENEATH,
        )
        .map_err(MaterializeError::OpenSource)?;
        let source = File::from(source);
        let metadata = source.metadata().map_err(MaterializeError::InspectSource)?;
        if !metadata.is_file() {
            return Err(MaterializeError::SourceNotRegular);
        }
        if metadata.len() != link.expected_size {
            return Err(MaterializeError::SourceSizeChanged);
        }
        if link
            .expected_device
            .is_some_and(|device| device != metadata.dev())
            || link
                .expected_inode
                .is_some_and(|inode| inode != metadata.ino())
        {
            return Err(MaterializeError::SourceIdentityChanged);
        }
        if metadata.dev() != self.link_device {
            return Err(MaterializeError::DeviceMismatch);
        }

        let parent = link
            .destination_relative
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(|parent| ensure_directory(namespace, parent))
            .transpose()?;
        let parent = parent
            .as_ref()
            .map_or_else(|| namespace.as_fd(), AsFd::as_fd);
        let name = link
            .destination_relative
            .file_name()
            .ok_or(MaterializeError::UnsafeRelativePath)?;
        match linkat(&source, "", parent, name, AtFlags::EMPTY_PATH) {
            Ok(()) => Ok(MaterializedLink {
                destination_relative: link.destination_relative.clone(),
                device: metadata.dev(),
                inode: metadata.ino(),
                size: metadata.len(),
                existing: false,
            }),
            Err(Errno::EXIST) => {
                let existing = openat2(
                    parent,
                    name,
                    OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                    RESOLVE_BENEATH,
                )
                .map_err(MaterializeError::OpenDestination)?;
                let existing = File::from(existing);
                let existing = existing
                    .metadata()
                    .map_err(MaterializeError::InspectDestination)?;
                if existing.is_file()
                    && existing.dev() == metadata.dev()
                    && existing.ino() == metadata.ino()
                    && existing.len() == metadata.len()
                {
                    Ok(MaterializedLink {
                        destination_relative: link.destination_relative.clone(),
                        device: metadata.dev(),
                        inode: metadata.ino(),
                        size: metadata.len(),
                        existing: true,
                    })
                } else {
                    Err(MaterializeError::LinkConflict)
                }
            }
            Err(error) => Err(MaterializeError::CreateLink(error)),
        }
    }
}

fn open_absolute_directory(path: &Path) -> Result<File, Errno> {
    if !path.is_absolute() {
        return Err(Errno::INVAL);
    }
    openat2(
        ABS,
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
        RESOLVE_ABSOLUTE,
    )
    .map(File::from)
}

fn ensure_directory(root: impl AsFd, relative: &Path) -> Result<OwnedFd, MaterializeError> {
    validate_relative(relative)?;
    let mut current = open_directory(root, ".")?;
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(MaterializeError::UnsafeRelativePath);
        };
        match mkdirat(&current, name, DIRECTORY_MODE) {
            Ok(()) | Err(Errno::EXIST) => {}
            Err(error) => return Err(MaterializeError::CreateDirectory(error)),
        }
        current = open_directory(&current, name)?;
    }
    Ok(current)
}

fn open_directory(
    root: impl AsFd,
    path: impl rustix::path::Arg,
) -> Result<OwnedFd, MaterializeError> {
    openat2(
        root,
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
        RESOLVE_BENEATH,
    )
    .map_err(MaterializeError::OpenDirectory)
}

fn validate_relative(path: &Path) -> Result<(), MaterializeError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        Err(MaterializeError::UnsafeRelativePath)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum MaterializeError {
    #[error("failed to open the managed link root")]
    OpenLinkRoot(#[source] Errno),
    #[error("failed to inspect the managed link root")]
    InspectLinkRoot(#[source] std::io::Error),
    #[error("managed link root is not a directory")]
    LinkRootNotDirectory,
    #[error("planned path is not a safe relative path")]
    UnsafeRelativePath,
    #[error("failed to create a managed directory")]
    CreateDirectory(#[source] Errno),
    #[error("failed to open a managed directory without following links")]
    OpenDirectory(#[source] Errno),
    #[error("failed to open an approved source root")]
    OpenSourceRoot(#[source] Errno),
    #[error("approved source root is not a directory")]
    SourceRootNotDirectory,
    #[error("failed to open a source beneath its approved root")]
    OpenSource(#[source] Errno),
    #[error("failed to inspect a source")]
    InspectSource(#[source] std::io::Error),
    #[error("hardlink source is not a regular file")]
    SourceNotRegular,
    #[error("hardlink source size changed after matching")]
    SourceSizeChanged,
    #[error("hardlink source identity changed after matching")]
    SourceIdentityChanged,
    #[error("hardlink source and managed link root are on different devices")]
    DeviceMismatch,
    #[error("failed to create a hardlink")]
    CreateLink(#[source] Errno),
    #[error("failed to inspect an existing hardlink destination")]
    OpenDestination(#[source] Errno),
    #[error("failed to inspect an existing hardlink destination")]
    InspectDestination(#[source] std::io::Error),
    #[error("hardlink destination is occupied by different content")]
    LinkConflict,
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{MetadataExt, symlink};

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn creates_nested_links_and_recognises_retries() {
        let directory = TempDir::new().unwrap();
        let source_root = directory.path().join("source");
        let link_root = directory.path().join("links");
        std::fs::create_dir_all(&source_root).unwrap();
        std::fs::create_dir_all(&link_root).unwrap();
        std::fs::write(source_root.join("video.mkv"), b"video").unwrap();
        let planned = PlannedLink {
            source_root,
            source_relative: "video.mkv".into(),
            destination_relative: "Release/video.mkv".into(),
            expected_size: 5,
            expected_device: None,
            expected_inode: None,
        };
        let materializer = HardlinkMaterializer::open(&link_root).unwrap();

        let first = materializer
            .materialize(Path::new("ab/candidate"), std::slice::from_ref(&planned))
            .unwrap();
        let second = materializer
            .materialize(Path::new("ab/candidate"), &[planned])
            .unwrap();

        assert!(!first[0].existing);
        assert!(second[0].existing);
        let destination = link_root.join("ab/candidate/Release/video.mkv");
        assert_eq!(
            std::fs::metadata(destination).unwrap().ino(),
            first[0].inode
        );
    }

    #[test]
    fn never_overwrites_a_conflicting_destination() {
        let directory = TempDir::new().unwrap();
        let source_root = directory.path().join("source");
        let link_root = directory.path().join("links");
        std::fs::create_dir_all(&source_root).unwrap();
        std::fs::create_dir_all(link_root.join("ab/candidate")).unwrap();
        std::fs::write(source_root.join("video.mkv"), b"source").unwrap();
        let destination = link_root.join("ab/candidate/video.mkv");
        std::fs::write(&destination, b"other!").unwrap();
        let materializer = HardlinkMaterializer::open(&link_root).unwrap();

        let error = materializer
            .materialize(
                Path::new("ab/candidate"),
                &[PlannedLink {
                    source_root,
                    source_relative: "video.mkv".into(),
                    destination_relative: "video.mkv".into(),
                    expected_size: 6,
                    expected_device: None,
                    expected_inode: None,
                }],
            )
            .unwrap_err();

        assert!(matches!(error, MaterializeError::LinkConflict));
        assert_eq!(std::fs::read(destination).unwrap(), b"other!");
    }

    #[test]
    fn rejects_symlinked_destination_traversal() {
        let directory = TempDir::new().unwrap();
        let source_root = directory.path().join("source");
        let link_root = directory.path().join("links");
        let outside = directory.path().join("outside");
        std::fs::create_dir_all(&source_root).unwrap();
        std::fs::create_dir_all(link_root.join("ab/candidate")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(source_root.join("video.mkv"), b"video").unwrap();
        symlink(&outside, link_root.join("ab/candidate/escape")).unwrap();
        let materializer = HardlinkMaterializer::open(&link_root).unwrap();

        assert!(
            materializer
                .materialize(
                    Path::new("ab/candidate"),
                    &[PlannedLink {
                        source_root,
                        source_relative: "video.mkv".into(),
                        destination_relative: "escape/video.mkv".into(),
                        expected_size: 5,
                        expected_device: None,
                        expected_inode: None,
                    }],
                )
                .is_err()
        );
        assert!(!outside.join("video.mkv").exists());
    }

    #[test]
    fn rejects_symlinked_managed_and_source_roots() {
        let directory = TempDir::new().unwrap();
        let actual_source = directory.path().join("actual-source");
        let actual_links = directory.path().join("actual-links");
        std::fs::create_dir_all(&actual_source).unwrap();
        std::fs::create_dir_all(&actual_links).unwrap();
        std::fs::write(actual_source.join("video.mkv"), b"video").unwrap();
        let linked_source = directory.path().join("source");
        let linked_root = directory.path().join("links");
        symlink(&actual_source, &linked_source).unwrap();
        symlink(&actual_links, &linked_root).unwrap();

        assert!(matches!(
            HardlinkMaterializer::open(&linked_root),
            Err(MaterializeError::OpenLinkRoot(_))
        ));

        let materializer = HardlinkMaterializer::open(&actual_links).unwrap();
        let error = materializer
            .materialize(
                Path::new("candidate"),
                &[PlannedLink {
                    source_root: linked_source,
                    source_relative: "video.mkv".into(),
                    destination_relative: "video.mkv".into(),
                    expected_size: 5,
                    expected_device: None,
                    expected_inode: None,
                }],
            )
            .unwrap_err();
        assert!(matches!(error, MaterializeError::OpenSourceRoot(_)));
        assert!(!actual_links.join("candidate/video.mkv").exists());
    }
}
