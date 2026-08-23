use std::io::{BufReader, Read};

use quick_xml::{Reader, events::Event};
use thiserror::Error;

const MAX_DEPTH: usize = 32;
const MAX_FIELD_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorznabResult {
    pub title: String,
    pub guid: Option<String>,
    pub download_url: String,
    pub size: Option<u64>,
}

#[derive(Debug, Error)]
pub enum TorznabParseError {
    #[error("Torznab response exceeded its byte limit")]
    LimitExceeded,
    #[error("Torznab XML exceeded its depth limit")]
    DepthExceeded,
    #[error("Torznab XML contains a prohibited declaration or entity")]
    ProhibitedXml,
    #[error("invalid Torznab XML")]
    Xml(#[source] quick_xml::Error),
    #[error("Torznab result consumer failed: {0}")]
    Consumer(String),
}

pub fn parse_torznab(
    reader: impl Read,
    byte_limit: u64,
    result_limit: usize,
    mut emit: impl FnMut(TorznabResult) -> Result<(), String>,
) -> Result<usize, TorznabParseError> {
    if result_limit == 0 {
        return Ok(0);
    }
    let limited = LimitedReader {
        inner: reader,
        remaining: byte_limit,
    };
    let mut reader = Reader::from_reader(BufReader::new(limited));
    reader.config_mut().trim_text(false);
    let mut buffer = Vec::new();
    let mut depth = 0_usize;
    let mut item = None;
    let mut field = None;
    let mut emitted = 0_usize;

    loop {
        match reader
            .read_event_into(&mut buffer)
            .map_err(classify_error)?
        {
            Event::Start(start) => {
                depth = depth
                    .checked_add(1)
                    .ok_or(TorznabParseError::DepthExceeded)?;
                if depth > MAX_DEPTH {
                    return Err(TorznabParseError::DepthExceeded);
                }
                match start.local_name().as_ref() {
                    "item" => item = Some(Item::default()),
                    "title" if item.is_some() => field = Some(Field::Title),
                    "guid" if item.is_some() => field = Some(Field::Guid),
                    "link" if item.is_some() => field = Some(Field::Link),
                    _ => {}
                }
            }
            Event::Empty(start) if item.is_some() => match start.local_name().as_ref() {
                "enclosure" => {
                    if let Some(value) = attribute(&start, "url")? {
                        item.as_mut().expect("checked above").set_url(value);
                    }
                }
                "attr" if attribute(&start, "name")?.as_deref() == Some("size") => {
                    let value = attribute(&start, "value")?;
                    let item = item.as_mut().expect("checked above");
                    match value.and_then(|value| value.parse().ok()) {
                        Some(size) => item.size = Some(size),
                        None => item.invalid = true,
                    }
                }
                _ => {}
            },
            Event::Text(text) if item.is_some() && field.is_some() => {
                let decoded = text.xml10_content();
                append(
                    item.as_mut().expect("checked above"),
                    field.expect("checked above"),
                    decoded.as_ref(),
                );
            }
            Event::CData(text) if item.is_some() && field.is_some() => append(
                item.as_mut().expect("checked above"),
                field.expect("checked above"),
                text.xml10_content().as_ref(),
            ),
            Event::GeneralRef(reference) => {
                let value = resolve_reference(&reference)?;
                if let (Some(item), Some(field)) = (&mut item, field) {
                    let mut encoded = [0_u8; 4];
                    append(item, field, value.encode_utf8(&mut encoded));
                }
            }
            Event::End(end) => {
                let name = end.local_name();
                if matches!(name.as_ref(), "title" | "guid" | "link") {
                    field = None;
                }
                if name.as_ref() == "item"
                    && let Some(result) = item.take().and_then(Item::finish)
                {
                    emit(result).map_err(TorznabParseError::Consumer)?;
                    emitted += 1;
                    if emitted == result_limit {
                        return Ok(emitted);
                    }
                }
                depth = depth.saturating_sub(1);
            }
            Event::DocType(_) | Event::PI(_) => return Err(TorznabParseError::ProhibitedXml),
            Event::Eof => return Ok(emitted),
            _ => {}
        }
        buffer.clear();
    }
}

#[derive(Clone, Copy)]
enum Field {
    Title,
    Guid,
    Link,
}

#[derive(Default)]
struct Item {
    title: String,
    guid: String,
    download_url: String,
    size: Option<u64>,
    invalid: bool,
}

impl Item {
    fn set_url(&mut self, value: String) {
        if value.len() > MAX_FIELD_BYTES {
            self.invalid = true;
        } else {
            self.download_url = value;
        }
    }

    fn finish(self) -> Option<TorznabResult> {
        let title = self.title.trim().to_owned();
        let guid = self.guid.trim().to_owned();
        let download_url = self.download_url.trim().to_owned();
        if self.invalid || title.is_empty() || download_url.is_empty() {
            return None;
        }
        Some(TorznabResult {
            title,
            guid: (!guid.is_empty()).then_some(guid),
            download_url,
            size: self.size,
        })
    }
}

fn append(item: &mut Item, field: Field, value: &str) {
    let target = match field {
        Field::Title => &mut item.title,
        Field::Guid => &mut item.guid,
        Field::Link => &mut item.download_url,
    };
    if target.len().saturating_add(value.len()) > MAX_FIELD_BYTES {
        item.invalid = true;
        return;
    }
    target.push_str(value);
}

fn attribute(
    start: &quick_xml::events::BytesStart<'_>,
    name: &str,
) -> Result<Option<String>, TorznabParseError> {
    for attribute in start.attributes() {
        let attribute = attribute
            .map_err(quick_xml::Error::from)
            .map_err(classify_error)?;
        if attribute.key.local_name().as_ref() == name {
            return attribute
                .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                .map(|value| Some(value.into_owned()))
                .map_err(classify_error);
        }
    }
    Ok(None)
}

fn resolve_reference(
    reference: &quick_xml::events::BytesRef<'_>,
) -> Result<char, TorznabParseError> {
    match reference.as_ref() {
        "lt" => Ok('<'),
        "gt" => Ok('>'),
        "amp" => Ok('&'),
        "apos" => Ok('\''),
        "quot" => Ok('"'),
        _ => reference
            .resolve_char_ref()
            .map_err(classify_error)?
            .ok_or(TorznabParseError::ProhibitedXml),
    }
}

fn classify_error(error: quick_xml::Error) -> TorznabParseError {
    if matches!(&error, quick_xml::Error::Io(source) if source.kind() == std::io::ErrorKind::FileTooLarge)
    {
        TorznabParseError::LimitExceeded
    } else {
        TorznabParseError::Xml(error)
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
    fn streams_valid_items_and_skips_malformed_ones() {
        let xml = br#"<?xml version="1.0"?><rss xmlns:torznab="http://torznab.com/schemas/2015/feed"><channel>
          <item><title>A &amp; B</title><guid>one</guid><enclosure url="https://prowlarr/api/v1/indexer/1/download?id=one"/><torznab:attr name="size" value="42"/></item>
          <item><guid>missing title</guid></item>
          <item><title>Second</title><link>https://prowlarr/download/two</link></item>
        </channel></rss>"#;
        let mut results = Vec::new();
        let count = parse_torznab(xml.as_slice(), 8_192, 100, |item| {
            results.push(item);
            Ok(())
        })
        .expect("Torznab response");

        assert_eq!(count, 2);
        assert_eq!(results[0].title, "A & B");
        assert_eq!(results[0].size, Some(42));
        assert_eq!(results[1].title, "Second");
    }

    #[test]
    fn stops_at_the_result_cap() {
        let xml = b"<rss><channel><item><title>one</title><link>one</link></item><item><title>two</title><link>two</link></item></channel></rss>";
        assert_eq!(
            parse_torznab(xml.as_slice(), 1_024, 1, |_| Ok(())).expect("capped response"),
            1
        );
    }

    #[test]
    fn rejects_dtds_and_external_entity_declarations() {
        let xml = br#"<!DOCTYPE rss [<!ENTITY xxe SYSTEM "file:///etc/passwd">]><rss><channel><item><title>&xxe;</title><link>x</link></item></channel></rss>"#;
        assert!(matches!(
            parse_torznab(xml.as_slice(), 4_096, 100, |_| Ok(())),
            Err(TorznabParseError::ProhibitedXml)
        ));
        assert!(matches!(
            parse_torznab(
                b"<rss><channel><description>&custom;</description></channel></rss>".as_slice(),
                4_096,
                100,
                |_| Ok(())
            ),
            Err(TorznabParseError::ProhibitedXml)
        ));
    }

    #[test]
    fn enforces_byte_and_depth_limits() {
        let xml = b"<rss><channel></channel></rss>";
        assert!(matches!(
            parse_torznab(xml.as_slice(), 8, 100, |_| Ok(())),
            Err(TorznabParseError::LimitExceeded)
        ));

        let nested = format!("{}{}", "<x>".repeat(33), "</x>".repeat(33));
        assert!(matches!(
            parse_torznab(nested.as_bytes(), 4_096, 100, |_| Ok(())),
            Err(TorznabParseError::DepthExceeded)
        ));
    }
}
