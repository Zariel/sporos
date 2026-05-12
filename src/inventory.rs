use std::collections::VecDeque;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use crate::domain::{
    ByteSize, DisplayName, DomainError, FileIndex, ItemTitle, LocalFile, LocalItem,
    LocalItemSource, MediaType,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct InventoryScanOptions {
    pub max_depth: u16,
}

impl Default for InventoryScanOptions {
    fn default() -> Self {
        Self { max_depth: 3 }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScannedLocalItem {
    pub item: LocalItem,
    pub files: Vec<LocalFile>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct InventoryScanReport {
    pub items: Vec<ScannedLocalItem>,
    pub failures: Vec<InventoryScanFailure>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InventoryScanFailure {
    pub path: PathBuf,
    pub kind: InventoryScanFailureKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InventoryScanFailureKind {
    Metadata,
    ReadDirectory,
    NonUtf8Path,
    Domain,
    Overflow,
}

impl fmt::Display for InventoryScanFailureKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Metadata => "metadata",
            Self::ReadDirectory => "read directory",
            Self::NonUtf8Path => "non-UTF-8 path",
            Self::Domain => "domain",
            Self::Overflow => "overflow",
        };
        formatter.write_str(label)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ScannedFile {
    relative_path: PathBuf,
    size: ByteSize,
    mtime_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct InventoryScanner {
    options: InventoryScanOptions,
}

impl InventoryScanner {
    pub const fn new(options: InventoryScanOptions) -> Self {
        Self { options }
    }

    pub fn scan_media_dirs(&self, media_dirs: &[PathBuf]) -> InventoryScanReport {
        let mut report = InventoryScanReport::default();
        for media_dir in media_dirs {
            let roots = self.discover_roots(media_dir, &mut report);
            for root in roots {
                if let Some(item) = self.scan_item_root(&root, &mut report) {
                    report.items.push(item);
                }
            }
        }
        report
    }

    fn discover_roots(&self, root: &Path, report: &mut InventoryScanReport) -> Vec<PathBuf> {
        let Ok(metadata) = fs::symlink_metadata(root) else {
            push_io_failure(
                report,
                root,
                InventoryScanFailureKind::Metadata,
                "read metadata",
            );
            return Vec::new();
        };

        if metadata.is_file() {
            return if is_video_file(root) {
                vec![root.to_path_buf()]
            } else {
                Vec::new()
            };
        }

        if !metadata.is_dir() {
            return Vec::new();
        }

        self.discover_directory_roots(root, 0, true, report)
    }

    fn discover_directory_roots(
        &self,
        dir: &Path,
        depth: u16,
        is_scan_root: bool,
        report: &mut InventoryScanReport,
    ) -> Vec<PathBuf> {
        if !is_scan_root && should_ignore_dir(dir) {
            return Vec::new();
        }

        if !path_has_utf8_name(dir) {
            push_failure(
                report,
                dir,
                InventoryScanFailureKind::NonUtf8Path,
                "directory name is not valid UTF-8",
            );
            return Vec::new();
        }

        let Ok(entries) = fs::read_dir(dir) else {
            push_io_failure(
                report,
                dir,
                InventoryScanFailureKind::ReadDirectory,
                "read directory",
            );
            return Vec::new();
        };

        let mut direct_files = Vec::new();
        let mut child_roots = Vec::new();
        for entry in entries {
            let Ok(entry) = entry else {
                push_io_failure(
                    report,
                    dir,
                    InventoryScanFailureKind::ReadDirectory,
                    "read directory entry",
                );
                continue;
            };
            let path = entry.path();
            let Ok(metadata) = entry.metadata() else {
                push_io_failure(
                    report,
                    &path,
                    InventoryScanFailureKind::Metadata,
                    "read metadata",
                );
                continue;
            };

            if metadata.is_dir() {
                if depth < self.options.max_depth {
                    child_roots.extend(self.discover_directory_roots(
                        &path,
                        depth + 1,
                        false,
                        report,
                    ));
                }
            } else if metadata.is_file() && is_video_file(&path) {
                if is_scan_root {
                    direct_files.push(path);
                } else {
                    direct_files.push(dir.to_path_buf());
                }
            }
        }

        if !child_roots.is_empty() {
            return child_roots;
        }

        dedupe_preserving_order(direct_files)
    }

    fn scan_item_root(
        &self,
        root: &Path,
        report: &mut InventoryScanReport,
    ) -> Option<ScannedLocalItem> {
        let display_name = root.file_name().and_then(|name| name.to_str())?;
        let mut files = Vec::new();
        collect_video_files(root, root, self.options.max_depth, &mut files, report);
        if files.is_empty() {
            return None;
        }

        let total_size = total_size(&files, root, report)?;
        let newest_mtime = files.iter().filter_map(|file| file.mtime_ms).max();
        let item = LocalItem {
            id: None,
            source: LocalItemSource::DataRoot {
                path: root.to_path_buf(),
            },
            title: ItemTitle::new(display_name).ok()?,
            display_name: DisplayName::new(display_name).ok()?,
            media_type: MediaType::Video,
            info_hash: None,
            path: Some(root.to_path_buf()),
            save_path: None,
            total_size,
            mtime_ms: newest_mtime,
        };

        let mut local_files = Vec::with_capacity(files.len());
        for (index, file) in files.into_iter().enumerate() {
            let Ok(index) = u32::try_from(index) else {
                push_failure(
                    report,
                    root,
                    InventoryScanFailureKind::Overflow,
                    "too many files under one local item",
                );
                return None;
            };
            match LocalFile::new(None, file.relative_path, file.size, FileIndex::new(index)) {
                Ok(file) => local_files.push(file),
                Err(error) => {
                    push_domain_failure(report, root, error);
                }
            }
        }

        if local_files.is_empty() {
            None
        } else {
            Some(ScannedLocalItem {
                item,
                files: local_files,
            })
        }
    }
}

fn collect_video_files(
    root: &Path,
    current: &Path,
    remaining_depth: u16,
    files: &mut Vec<ScannedFile>,
    report: &mut InventoryScanReport,
) {
    let Ok(metadata) = fs::symlink_metadata(current) else {
        push_io_failure(
            report,
            current,
            InventoryScanFailureKind::Metadata,
            "read metadata",
        );
        return;
    };

    if metadata.is_file() {
        collect_one_file(root, current, &metadata, files, report);
        return;
    }

    if !metadata.is_dir() || should_ignore_dir(current) {
        return;
    }

    let Ok(entries) = fs::read_dir(current) else {
        push_io_failure(
            report,
            current,
            InventoryScanFailureKind::ReadDirectory,
            "read directory",
        );
        return;
    };

    for entry in entries {
        let Ok(entry) = entry else {
            push_io_failure(
                report,
                current,
                InventoryScanFailureKind::ReadDirectory,
                "read directory entry",
            );
            continue;
        };
        let path = entry.path();
        let Ok(metadata) = entry.metadata() else {
            push_io_failure(
                report,
                &path,
                InventoryScanFailureKind::Metadata,
                "read metadata",
            );
            continue;
        };

        if metadata.is_file() {
            collect_one_file(root, &path, &metadata, files, report);
        } else if metadata.is_dir() && remaining_depth > 0 {
            collect_video_files(root, &path, remaining_depth - 1, files, report);
        }
    }
}

fn collect_one_file(
    root: &Path,
    path: &Path,
    metadata: &fs::Metadata,
    files: &mut Vec<ScannedFile>,
    report: &mut InventoryScanReport,
) {
    if !is_video_file(path) {
        return;
    }
    if !path_has_utf8_name(path) {
        push_failure(
            report,
            path,
            InventoryScanFailureKind::NonUtf8Path,
            "file name is not valid UTF-8",
        );
        return;
    }

    let relative_path = if root == path {
        match path.file_name() {
            Some(name) => PathBuf::from(name),
            None => {
                push_failure(
                    report,
                    path,
                    InventoryScanFailureKind::Domain,
                    "file path has no file name",
                );
                return;
            }
        }
    } else {
        match path.strip_prefix(root) {
            Ok(relative_path) => relative_path.to_path_buf(),
            Err(error) => {
                push_failure(
                    report,
                    path,
                    InventoryScanFailureKind::Domain,
                    format!("file is not under item root: {error}"),
                );
                return;
            }
        }
    };

    files.push(ScannedFile {
        relative_path,
        size: ByteSize::new(metadata.len()),
        mtime_ms: metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .and_then(|duration| i64::try_from(duration.as_millis()).ok()),
    });
}

fn total_size(
    files: &[ScannedFile],
    root: &Path,
    report: &mut InventoryScanReport,
) -> Option<ByteSize> {
    let mut total = 0_u64;
    for file in files {
        let Some(next_total) = total.checked_add(file.size.get()) else {
            push_failure(
                report,
                root,
                InventoryScanFailureKind::Overflow,
                "local item file sizes exceed u64",
            );
            return None;
        };
        total = next_total;
    }
    Some(ByteSize::new(total))
}

fn is_video_file(path: &Path) -> bool {
    let Some(extension) = path.extension().and_then(|extension| extension.to_str()) else {
        return false;
    };
    matches!(
        extension.to_ascii_lowercase().as_str(),
        "mkv" | "mp4" | "avi" | "mov" | "m4v" | "ts" | "m2ts" | "wmv" | "flv" | "webm"
    )
}

fn should_ignore_dir(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return true;
    };
    let name = name.to_ascii_lowercase();
    [
        "sample",
        "proof",
        "bdmv",
        "bdrom",
        "certificate",
        "video_ts",
    ]
    .iter()
    .any(|ignored| name.contains(ignored))
}

fn path_has_utf8_name(path: &Path) -> bool {
    path.file_name().is_some_and(|name| name.to_str().is_some())
}

fn dedupe_preserving_order(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut unique = Vec::new();
    let mut pending = VecDeque::from(paths);
    while let Some(path) = pending.pop_front() {
        if !unique.iter().any(|existing| existing == &path) {
            unique.push(path);
        }
    }
    unique
}

fn push_io_failure(
    report: &mut InventoryScanReport,
    path: &Path,
    kind: InventoryScanFailureKind,
    operation: &'static str,
) {
    let message = match fs::metadata(path) {
        Ok(_) => operation.to_owned(),
        Err(error) => format!("{operation}: {error}"),
    };
    push_failure(report, path, kind, message);
}

fn push_domain_failure(report: &mut InventoryScanReport, path: &Path, error: DomainError) {
    push_failure(
        report,
        path,
        InventoryScanFailureKind::Domain,
        error.to_string(),
    );
}

fn push_failure(
    report: &mut InventoryScanReport,
    path: &Path,
    kind: InventoryScanFailureKind,
    message: impl Into<String>,
) {
    report.failures.push(InventoryScanFailure {
        path: path.to_path_buf(),
        kind,
        message: message.into(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn scan_media_dirs_builds_items_and_ignores_noise_dirs() {
        let root = unique_temp_dir("basic");
        let release = root.join("Movie.2024.1080p");
        fs::create_dir_all(&release).unwrap();
        write_file(&release.join("movie.mkv"), 10);
        write_file(&release.join("notes.txt"), 20);

        for ignored in [
            "sample",
            "proof",
            "BDMV",
            "bdrom",
            "CERTIFICATE",
            "VIDEO_TS",
        ] {
            let ignored_dir = release.join(ignored);
            fs::create_dir_all(&ignored_dir).unwrap();
            write_file(&ignored_dir.join("ignored.mkv"), 30);
        }

        let scanner = InventoryScanner::new(InventoryScanOptions { max_depth: 3 });
        let report = scanner.scan_media_dirs(std::slice::from_ref(&root));

        assert!(report.failures.is_empty());
        assert_eq!(1, report.items.len());
        let scanned = &report.items[0];
        assert_eq!("Movie.2024.1080p", scanned.item.display_name.as_str());
        assert_eq!(ByteSize::new(10), scanned.item.total_size);
        assert_eq!(Some(release), scanned.item.path);
        assert_eq!(1, scanned.files.len());
        assert_eq!(PathBuf::from("movie.mkv"), scanned.files[0].relative_path);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scan_media_dirs_respects_configured_depth() {
        let root = unique_temp_dir("depth");
        let shallow = root.join("Shallow");
        let deep = root.join("A/B/C/D");
        fs::create_dir_all(&shallow).unwrap();
        fs::create_dir_all(&deep).unwrap();
        write_file(&shallow.join("shallow.mkv"), 10);
        write_file(&deep.join("deep.mkv"), 10);

        let scanner = InventoryScanner::new(InventoryScanOptions { max_depth: 1 });
        let report = scanner.scan_media_dirs(std::slice::from_ref(&root));

        assert_eq!(1, report.items.len());
        assert_eq!("Shallow", report.items[0].item.display_name.as_str());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scan_media_dirs_handles_deleted_or_unreadable_roots() {
        let root = unique_temp_dir("deleted");
        let missing = root.join("missing");
        let scanner = InventoryScanner::new(InventoryScanOptions::default());
        let report = scanner.scan_media_dirs(std::slice::from_ref(&missing));

        assert!(report.items.is_empty());
        assert_eq!(1, report.failures.len());
        assert_eq!(missing, report.failures[0].path);
        assert_eq!(InventoryScanFailureKind::Metadata, report.failures[0].kind);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn scan_media_dirs_handles_large_release_directories() {
        let root = unique_temp_dir("large");
        let release = root.join("Large.Release");
        fs::create_dir_all(&release).unwrap();
        for index in 0..300 {
            write_file(&release.join(format!("episode-{index:03}.mkv")), 1);
        }

        let scanner = InventoryScanner::new(InventoryScanOptions::default());
        let report = scanner.scan_media_dirs(std::slice::from_ref(&root));

        assert_eq!(1, report.items.len());
        assert_eq!(300, report.items[0].files.len());
        assert_eq!(ByteSize::new(300), report.items[0].item.total_size);

        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn scan_media_dirs_skips_non_utf8_file_names() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let root = unique_temp_dir("non-utf8");
        let release = root.join("Release");
        fs::create_dir_all(&release).unwrap();
        write_file(&release.join("valid.mkv"), 1);
        let invalid_name = OsString::from_vec(vec![b'b', b'a', b'd', 0xff, b'.', b'm', b'k', b'v']);
        write_file(&release.join(invalid_name), 1);

        let scanner = InventoryScanner::new(InventoryScanOptions::default());
        let report = scanner.scan_media_dirs(std::slice::from_ref(&root));

        assert_eq!(1, report.items.len());
        assert_eq!(1, report.items[0].files.len());
        assert!(
            report
                .failures
                .iter()
                .any(|failure| failure.kind == InventoryScanFailureKind::NonUtf8Path)
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn scan_media_dirs_reports_permission_failures_and_continues() {
        use std::os::unix::fs::PermissionsExt;

        let root = unique_temp_dir("permission");
        let denied = root.join("Denied");
        let allowed = root.join("Allowed");
        fs::create_dir_all(&denied).unwrap();
        fs::create_dir_all(&allowed).unwrap();
        write_file(&denied.join("hidden.mkv"), 1);
        write_file(&allowed.join("visible.mkv"), 1);
        let original_permissions = fs::metadata(&denied).unwrap().permissions();
        fs::set_permissions(&denied, fs::Permissions::from_mode(0o000)).unwrap();

        let scanner = InventoryScanner::new(InventoryScanOptions::default());
        let report = scanner.scan_media_dirs(std::slice::from_ref(&root));

        fs::set_permissions(&denied, original_permissions).unwrap();

        assert_eq!(1, report.items.len());
        assert_eq!("Allowed", report.items[0].item.display_name.as_str());
        assert!(
            report
                .failures
                .iter()
                .any(|failure| failure.kind == InventoryScanFailureKind::ReadDirectory)
        );

        fs::remove_dir_all(root).unwrap();
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("sporos-inventory-test-{label}-{nanos}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn write_file(path: &Path, size: usize) {
        let mut file = File::create(path).unwrap();
        let bytes = vec![b'x'; size];
        file.write_all(&bytes).unwrap();
    }
}
