//! Torrent file cache integration.

use std::{
    fs,
    io::Write,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use filetime::FileTime;

use crate::{
    domain::{InfoHash, Metafile},
    torrent::{parse_metafile, torrent_cache_path},
};

use super::integration_error;

/// Write a valid candidate torrent into the info-hash cache.
pub fn cache_torrent_file(app_dir: &Path, bytes: &[u8]) -> crate::Result<Metafile<'static>> {
    let metafile = parse_metafile(bytes)?;
    let path = torrent_cache_path(app_dir, &metafile.info_hash);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            integration_error(format!("failed to create torrent cache: {error}"))
        })?;
    }
    write_cached_torrent_atomically(&path, bytes)?;
    Ok(metafile)
}

fn write_cached_torrent_atomically(path: &Path, bytes: &[u8]) -> crate::Result<()> {
    refuse_cache_symlink(path)?;
    let Some(parent) = path.parent() else {
        return Err(integration_error(format!(
            "cached torrent path has no parent: {}",
            path.display()
        )));
    };
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            integration_error(format!("invalid cached torrent path: {}", path.display()))
        })?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let mut temp_path = None;
    let mut temp_file = None;
    for attempt in 0..16 {
        let candidate = parent.join(format!(
            ".{file_name}.{}.{}.tmp",
            std::process::id(),
            nonce + attempt
        ));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => {
                temp_path = Some(candidate);
                temp_file = Some(file);
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(integration_error(format!(
                    "failed to create cached torrent temp file: {error}"
                )));
            }
        }
    }
    let Some(mut file) = temp_file else {
        return Err(integration_error(
            "failed to create unique cached torrent temp file",
        ));
    };
    let temp_path = temp_path.expect("temp path set with temp file");
    let write_result = file
        .write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| integration_error(format!("failed to write cached torrent: {error}")));
    drop(file);
    if let Err(error) = write_result {
        remove_temp_cached_torrent(&temp_path);
        return Err(error);
    }
    if let Err(error) = refuse_cache_symlink(path) {
        remove_temp_cached_torrent(&temp_path);
        return Err(error);
    }
    if let Err(error) = fs::rename(&temp_path, path) {
        remove_temp_cached_torrent(&temp_path);
        return Err(integration_error(format!(
            "failed to publish cached torrent: {error}"
        )));
    }
    Ok(())
}

fn remove_temp_cached_torrent(path: &Path) {
    if let Err(error) = fs::remove_file(path) {
        tracing::debug!(
            "failed to remove cached torrent temp file {}: {error}",
            path.display()
        );
    }
}

fn refuse_cache_symlink(path: &Path) -> crate::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(integration_error(format!(
            "refusing to write cached torrent through symlink: {}",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(integration_error(format!(
            "failed to inspect cached torrent path {}: {error}",
            path.display()
        ))),
    }
}

/// Read a cached torrent, update access time, and delete corrupted cache files.
pub fn get_cached_torrent(
    app_dir: &Path,
    info_hash: &InfoHash<'_>,
) -> crate::Result<Option<Metafile<'static>>> {
    let path = torrent_cache_path(app_dir, info_hash);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(integration_error(format!(
                "failed to read cached torrent: {error}"
            )));
        }
    };
    match parse_metafile(&bytes) {
        Ok(metafile) => {
            let now = FileTime::now();
            let metadata = fs::metadata(&path).map_err(|error| {
                integration_error(format!("failed to stat cached torrent: {error}"))
            })?;
            let modified = FileTime::from_last_modification_time(&metadata);
            filetime::set_file_times(&path, now, modified).map_err(|error| {
                integration_error(format!("failed to touch cached torrent: {error}"))
            })?;
            Ok(Some(metafile))
        }
        Err(error) => {
            let _cleanup = fs::remove_file(&path);
            Err(error)
        }
    }
}
