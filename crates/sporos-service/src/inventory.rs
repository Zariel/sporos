use std::{fmt, io::Read};

use serde::{Deserialize, de::DeserializeSeed};
use thiserror::Error;

const MAX_HASH_BYTES: usize = 64;
const MAX_NAME_BYTES: usize = 4_096;
const MAX_PATH_BYTES: usize = 16_384;
const MAX_LABEL_BYTES: usize = 4_096;
const MAX_STATE_BYTES: usize = 64;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct InventoryTorrent {
    pub hash: String,
    #[serde(default)]
    pub infohash_v1: String,
    #[serde(default)]
    pub infohash_v2: String,
    pub name: String,
    #[serde(default)]
    pub total_size: u64,
    pub amount_left: u64,
    pub progress: f64,
    pub state: String,
    pub save_path: String,
    pub content_path: String,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub tags: String,
    #[serde(default)]
    pub added_on: i64,
    #[serde(default)]
    pub completion_on: i64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Default)]
pub struct InventoryDelta {
    pub infohash_v1: Option<String>,
    pub infohash_v2: Option<String>,
    pub name: Option<String>,
    pub total_size: Option<u64>,
    pub amount_left: Option<u64>,
    pub progress: Option<f64>,
    pub state: Option<String>,
    pub save_path: Option<String>,
    pub content_path: Option<String>,
    pub category: Option<String>,
    pub tags: Option<String>,
    pub added_on: Option<i64>,
    pub completion_on: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InventoryChange {
    Upsert {
        qbit_id: String,
        delta: Box<InventoryDelta>,
    },
    Removed {
        qbit_id: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MainData {
    pub response_id: u64,
    pub full_update: bool,
    pub changed: usize,
    pub removed: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct InventoryFile {
    pub index: usize,
    pub name: String,
    pub size: u64,
    pub progress: f64,
}

#[derive(Debug, Error)]
pub enum InventoryParseError {
    #[error("qBittorrent inventory exceeded its byte limit")]
    LimitExceeded,
    #[error("invalid qBittorrent inventory")]
    Json(#[source] serde_json::Error),
}

pub fn parse_inventory(
    reader: impl Read,
    byte_limit: u64,
    emit: impl FnMut(InventoryTorrent) -> Result<(), String>,
) -> Result<usize, InventoryParseError> {
    let reader = LimitedReader {
        inner: reader,
        remaining: byte_limit,
    };
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let count = InventorySeed { emit }
        .deserialize(&mut deserializer)
        .map_err(classify_error)?;
    deserializer.end().map_err(classify_error)?;
    Ok(count)
}

pub fn parse_main_data(
    reader: impl Read,
    byte_limit: u64,
    mut emit: impl FnMut(InventoryChange) -> Result<(), String>,
) -> Result<MainData, InventoryParseError> {
    parse_main_data_with_header(reader, byte_limit, |_| Ok(()), &mut emit)
}

pub(crate) fn parse_main_data_with_header(
    reader: impl Read,
    byte_limit: u64,
    mut emit_header: impl FnMut(MainData) -> Result<(), String>,
    mut emit: impl FnMut(InventoryChange) -> Result<(), String>,
) -> Result<MainData, InventoryParseError> {
    let reader = LimitedReader {
        inner: reader,
        remaining: byte_limit,
    };
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let result = MainDataSeed {
        emit_header: &mut emit_header,
        emit: &mut emit,
    }
    .deserialize(&mut deserializer)
    .map_err(classify_error)?;
    deserializer.end().map_err(classify_error)?;
    Ok(result)
}

pub fn parse_files(
    reader: impl Read,
    byte_limit: u64,
    file_limit: usize,
    emit: impl FnMut(InventoryFile) -> Result<(), String>,
) -> Result<usize, InventoryParseError> {
    let reader = LimitedReader {
        inner: reader,
        remaining: byte_limit,
    };
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let count = FileSeed {
        emit,
        limit: file_limit,
    }
    .deserialize(&mut deserializer)
    .map_err(classify_error)?;
    deserializer.end().map_err(classify_error)?;
    Ok(count)
}

pub fn parse_piece_states(
    reader: impl Read,
    byte_limit: u64,
    piece_limit: usize,
    emit: impl FnMut(u8) -> Result<(), String>,
) -> Result<usize, InventoryParseError> {
    let reader = LimitedReader {
        inner: reader,
        remaining: byte_limit,
    };
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let count = PieceStateSeed {
        emit,
        limit: piece_limit,
    }
    .deserialize(&mut deserializer)
    .map_err(classify_error)?;
    deserializer.end().map_err(classify_error)?;
    Ok(count)
}

struct InventorySeed<F> {
    emit: F,
}

impl<'de, F> DeserializeSeed<'de> for InventorySeed<F>
where
    F: FnMut(InventoryTorrent) -> Result<(), String>,
{
    type Value = usize;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(InventoryVisitor { emit: self.emit })
    }
}

struct InventoryVisitor<F> {
    emit: F,
}

struct MainDataSeed<'a, H, F> {
    emit_header: &'a mut H,
    emit: &'a mut F,
}

impl<'de, H, F> DeserializeSeed<'de> for MainDataSeed<'_, H, F>
where
    H: FnMut(MainData) -> Result<(), String>,
    F: FnMut(InventoryChange) -> Result<(), String>,
{
    type Value = MainData;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(MainDataVisitor {
            emit_header: self.emit_header,
            emit: self.emit,
        })
    }
}

struct MainDataVisitor<'a, H, F> {
    emit_header: &'a mut H,
    emit: &'a mut F,
}

impl<'de, H, F> serde::de::Visitor<'de> for MainDataVisitor<'_, H, F>
where
    H: FnMut(MainData) -> Result<(), String>,
    F: FnMut(InventoryChange) -> Result<(), String>,
{
    type Value = MainData;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a qBittorrent main-data object")
    }

    fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut response_id = None;
        let mut full_update = false;
        let mut header_emitted = false;
        let mut changed = 0_usize;
        let mut removed = 0_usize;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "rid" => {
                    let value = map.next_value()?;
                    if header_emitted && response_id != Some(value) {
                        return Err(serde::de::Error::custom(
                            "main-data response ID changed after payload began",
                        ));
                    }
                    response_id = Some(value);
                }
                "full_update" => {
                    let value = map.next_value()?;
                    if header_emitted && full_update != value {
                        return Err(serde::de::Error::custom(
                            "main-data update mode changed after payload began",
                        ));
                    }
                    full_update = value;
                }
                "torrents" => {
                    emit_main_data_header(
                        &mut self.emit_header,
                        &mut header_emitted,
                        response_id,
                        full_update,
                    )?;
                    changed = map.next_value_seed(DeltaMapSeed {
                        emit: &mut self.emit,
                    })?;
                }
                "torrents_removed" => {
                    emit_main_data_header(
                        &mut self.emit_header,
                        &mut header_emitted,
                        response_id,
                        full_update,
                    )?;
                    removed = map.next_value_seed(RemovedSeed {
                        emit: &mut self.emit,
                    })?;
                }
                _ => {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
        }
        emit_main_data_header(
            &mut self.emit_header,
            &mut header_emitted,
            response_id,
            full_update,
        )?;
        Ok(MainData {
            response_id: response_id.ok_or_else(|| serde::de::Error::missing_field("rid"))?,
            full_update,
            changed,
            removed,
        })
    }
}

fn emit_main_data_header<E: serde::de::Error>(
    emit: &mut impl FnMut(MainData) -> Result<(), String>,
    emitted: &mut bool,
    response_id: Option<u64>,
    full_update: bool,
) -> Result<(), E> {
    if !*emitted {
        emit(MainData {
            response_id: response_id.ok_or_else(|| E::missing_field("rid"))?,
            full_update,
            changed: 0,
            removed: 0,
        })
        .map_err(E::custom)?;
        *emitted = true;
    }
    Ok(())
}

struct DeltaMapSeed<'a, F> {
    emit: &'a mut F,
}

impl<'de, F> DeserializeSeed<'de> for DeltaMapSeed<'_, F>
where
    F: FnMut(InventoryChange) -> Result<(), String>,
{
    type Value = usize;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(DeltaMapVisitor { emit: self.emit })
    }
}

struct DeltaMapVisitor<'a, F> {
    emit: &'a mut F,
}

impl<'de, F> serde::de::Visitor<'de> for DeltaMapVisitor<'_, F>
where
    F: FnMut(InventoryChange) -> Result<(), String>,
{
    type Value = usize;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a qBittorrent torrent change map")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut count = 0_usize;
        while let Some((qbit_id, delta)) = map.next_entry::<String, InventoryDelta>()? {
            validate_id(&qbit_id).map_err(serde::de::Error::custom)?;
            validate_delta(&delta).map_err(serde::de::Error::custom)?;
            (self.emit)(InventoryChange::Upsert {
                qbit_id,
                delta: Box::new(delta),
            })
            .map_err(serde::de::Error::custom)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| serde::de::Error::custom("inventory item count overflow"))?;
        }
        Ok(count)
    }
}

struct RemovedSeed<'a, F> {
    emit: &'a mut F,
}

impl<'de, F> DeserializeSeed<'de> for RemovedSeed<'_, F>
where
    F: FnMut(InventoryChange) -> Result<(), String>,
{
    type Value = usize;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(RemovedVisitor { emit: self.emit })
    }
}

struct RemovedVisitor<'a, F> {
    emit: &'a mut F,
}

impl<'de, F> serde::de::Visitor<'de> for RemovedVisitor<'_, F>
where
    F: FnMut(InventoryChange) -> Result<(), String>,
{
    type Value = usize;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a qBittorrent removed-torrent array")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut count = 0_usize;
        while let Some(qbit_id) = sequence.next_element::<String>()? {
            validate_id(&qbit_id).map_err(serde::de::Error::custom)?;
            (self.emit)(InventoryChange::Removed { qbit_id }).map_err(serde::de::Error::custom)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| serde::de::Error::custom("inventory item count overflow"))?;
        }
        Ok(count)
    }
}

struct FileSeed<F> {
    emit: F,
    limit: usize,
}

impl<'de, F> DeserializeSeed<'de> for FileSeed<F>
where
    F: FnMut(InventoryFile) -> Result<(), String>,
{
    type Value = usize;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(FileVisitor {
            emit: self.emit,
            limit: self.limit,
        })
    }
}

struct FileVisitor<F> {
    emit: F,
    limit: usize,
}

impl<'de, F> serde::de::Visitor<'de> for FileVisitor<F>
where
    F: FnMut(InventoryFile) -> Result<(), String>,
{
    type Value = usize;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a qBittorrent file array")
    }

    fn visit_seq<A>(mut self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut count = 0_usize;
        while let Some(file) = sequence.next_element::<InventoryFile>()? {
            if count == self.limit {
                return Err(serde::de::Error::custom("file count limit exceeded"));
            }
            if file.name.len() > MAX_PATH_BYTES
                || !file.progress.is_finite()
                || !(0.0..=1.0).contains(&file.progress)
            {
                return Err(serde::de::Error::custom("invalid torrent file"));
            }
            (self.emit)(file).map_err(serde::de::Error::custom)?;
            count += 1;
        }
        Ok(count)
    }
}

struct PieceStateSeed<F> {
    emit: F,
    limit: usize,
}

impl<'de, F> DeserializeSeed<'de> for PieceStateSeed<F>
where
    F: FnMut(u8) -> Result<(), String>,
{
    type Value = usize;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(PieceStateVisitor {
            emit: self.emit,
            limit: self.limit,
        })
    }
}

struct PieceStateVisitor<F> {
    emit: F,
    limit: usize,
}

impl<'de, F> serde::de::Visitor<'de> for PieceStateVisitor<F>
where
    F: FnMut(u8) -> Result<(), String>,
{
    type Value = usize;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a qBittorrent piece-state array")
    }

    fn visit_seq<A>(mut self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut count = 0_usize;
        while let Some(state) = sequence.next_element::<u8>()? {
            if count == self.limit {
                return Err(serde::de::Error::custom("piece count limit exceeded"));
            }
            if state > 2 {
                return Err(serde::de::Error::custom("invalid piece state"));
            }
            (self.emit)(state).map_err(serde::de::Error::custom)?;
            count += 1;
        }
        Ok(count)
    }
}

impl<'de, F> serde::de::Visitor<'de> for InventoryVisitor<F>
where
    F: FnMut(InventoryTorrent) -> Result<(), String>,
{
    type Value = usize;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a qBittorrent torrent array")
    }

    fn visit_seq<A>(mut self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut count = 0_usize;
        while let Some(torrent) = sequence.next_element::<InventoryTorrent>()? {
            validate(&torrent).map_err(serde::de::Error::custom)?;
            (self.emit)(torrent).map_err(serde::de::Error::custom)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| serde::de::Error::custom("inventory item count overflow"))?;
        }
        Ok(count)
    }
}

fn validate(torrent: &InventoryTorrent) -> Result<(), &'static str> {
    validate_id(&torrent.hash)?;
    validate_hash(&torrent.infohash_v1, 40)?;
    validate_hash(&torrent.infohash_v2, 64)?;
    if !torrent.progress.is_finite() || !(0.0..=1.0).contains(&torrent.progress) {
        return Err("invalid progress");
    }
    for (value, limit, field) in [
        (&torrent.name, MAX_NAME_BYTES, "name"),
        (&torrent.state, MAX_STATE_BYTES, "state"),
        (&torrent.save_path, MAX_PATH_BYTES, "save path"),
        (&torrent.content_path, MAX_PATH_BYTES, "content path"),
        (&torrent.category, MAX_LABEL_BYTES, "category"),
        (&torrent.tags, MAX_LABEL_BYTES, "tags"),
    ] {
        if value.len() > limit {
            return Err(field);
        }
    }
    Ok(())
}

fn validate_delta(delta: &InventoryDelta) -> Result<(), &'static str> {
    if let Some(value) = &delta.infohash_v1 {
        validate_hash(value, 40)?;
    }
    if let Some(value) = &delta.infohash_v2 {
        validate_hash(value, 64)?;
    }
    if delta
        .progress
        .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
    {
        return Err("invalid progress");
    }
    for (value, limit, field) in [
        (&delta.name, MAX_NAME_BYTES, "name"),
        (&delta.state, MAX_STATE_BYTES, "state"),
        (&delta.save_path, MAX_PATH_BYTES, "save path"),
        (&delta.content_path, MAX_PATH_BYTES, "content path"),
        (&delta.category, MAX_LABEL_BYTES, "category"),
        (&delta.tags, MAX_LABEL_BYTES, "tags"),
    ] {
        if value.as_ref().is_some_and(|value| value.len() > limit) {
            return Err(field);
        }
    }
    Ok(())
}

fn validate_id(value: &str) -> Result<(), &'static str> {
    if value.is_empty()
        || value.len() > MAX_HASH_BYTES
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("invalid qBittorrent torrent ID");
    }
    Ok(())
}

fn validate_hash(value: &str, length: usize) -> Result<(), &'static str> {
    if !value.is_empty()
        && (value.len() != length || !value.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return Err("invalid infohash");
    }
    Ok(())
}

fn classify_error(error: serde_json::Error) -> InventoryParseError {
    if error.io_error_kind() == Some(std::io::ErrorKind::FileTooLarge) {
        InventoryParseError::LimitExceeded
    } else {
        InventoryParseError::Json(error)
    }
}

struct LimitedReader<R> {
    inner: R,
    remaining: u64,
}

impl<R: Read> Read for LimitedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            let mut extra = [0_u8; 1];
            return match self.inner.read(&mut extra)? {
                0 => Ok(0),
                _ => Err(std::io::Error::from(std::io::ErrorKind::FileTooLarge)),
            };
        }
        let allowed = usize::try_from(self.remaining.min(buffer.len() as u64))
            .expect("allowed read length fits usize");
        let count = self.inner.read(&mut buffer[..allowed])?;
        self.remaining -= count as u64;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_items_without_collecting_the_array() {
        let input = br#"[
          {"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"one","amount_left":0,"progress":1.0,"state":"stoppedUP","save_path":"/data","content_path":"/data/one"},
          {"hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","name":"two","amount_left":4,"progress":0.0,"state":"stoppedDL","save_path":"/data","content_path":"/data/two","category":"video","tags":"one,two"}
        ]"#;
        let mut names = Vec::new();
        let count = parse_inventory(input.as_slice(), 4_096, |torrent| {
            names.push(torrent.name);
            Ok(())
        })
        .expect("inventory");

        assert_eq!(count, 2);
        assert_eq!(names, ["one", "two"]);
    }

    #[test]
    fn enforces_the_response_limit_while_streaming() {
        let input = br#"[{"hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"one","amount_left":0,"progress":1.0,"state":"stoppedUP","save_path":"/data","content_path":"/data/one"}]"#;
        assert!(matches!(
            parse_inventory(input.as_slice(), 16, |_| Ok(())),
            Err(InventoryParseError::LimitExceeded)
        ));
    }

    #[test]
    fn streams_partial_main_data_and_removals() {
        let input = br#"{
            "rid": 42,
            "full_update": false,
            "torrents": {
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": {
                    "amount_left": 0,
                    "progress": 1.0,
                    "state": "uploading"
                }
            },
            "torrents_removed": ["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],
            "server_state": {"connection_status":"connected"}
        }"#;
        let mut changes = Vec::new();

        let result = parse_main_data(input.as_slice(), 4_096, |change| {
            changes.push(change);
            Ok(())
        })
        .expect("main data");

        assert_eq!(
            result,
            MainData {
                response_id: 42,
                full_update: false,
                changed: 1,
                removed: 1,
            }
        );
        assert!(matches!(
            &changes[0],
            InventoryChange::Upsert { delta, .. }
                if delta.name.is_none() && delta.amount_left == Some(0)
        ));
        assert!(matches!(&changes[1], InventoryChange::Removed { .. }));
    }

    #[test]
    fn streams_bounded_file_lists() {
        let input = br#"[
            {"index":0,"name":"root/a.mkv","size":4,"progress":1.0},
            {"index":1,"name":"root/b.srt","size":2,"progress":0.5}
        ]"#;
        let mut paths = Vec::new();

        assert_eq!(
            parse_files(input.as_slice(), 4_096, 2, |file| {
                paths.push(file.name);
                Ok(())
            })
            .expect("files"),
            2
        );
        assert_eq!(paths, ["root/a.mkv", "root/b.srt"]);
        assert!(parse_files(input.as_slice(), 4_096, 1, |_| Ok(())).is_err());
    }

    #[test]
    fn streams_piece_states_with_a_count_limit() {
        let mut states = Vec::new();
        assert_eq!(
            parse_piece_states(b"[0,1,2,0]".as_slice(), 64, 4, |state| {
                states.push(state);
                Ok(())
            })
            .expect("piece states"),
            4
        );
        assert_eq!(states, [0, 1, 2, 0]);
        assert!(parse_piece_states(b"[0,1]".as_slice(), 64, 1, |_| Ok(())).is_err());
        assert!(parse_piece_states(b"[3]".as_slice(), 64, 1, |_| Ok(())).is_err());
    }
}
