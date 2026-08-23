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
    pub name: String,
    pub amount_left: u64,
    pub progress: f64,
    pub state: String,
    pub save_path: String,
    pub content_path: String,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub tags: String,
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
    if torrent.hash.is_empty() || torrent.hash.len() > MAX_HASH_BYTES {
        return Err("invalid infohash length");
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
          {"hash":"a","name":"one","amount_left":0,"progress":1.0,"state":"stoppedUP","save_path":"/data","content_path":"/data/one"},
          {"hash":"b","name":"two","amount_left":4,"progress":0.0,"state":"stoppedDL","save_path":"/data","content_path":"/data/two","category":"video","tags":"one,two"}
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
        let input = br#"[{"hash":"a","name":"one","amount_left":0,"progress":1.0,"state":"stoppedUP","save_path":"/data","content_path":"/data/one"}]"#;
        assert!(matches!(
            parse_inventory(input.as_slice(), 16, |_| Ok(())),
            Err(InventoryParseError::LimitExceeded)
        ));
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
