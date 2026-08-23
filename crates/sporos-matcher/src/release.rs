use sporos_model::{Date, NormalizedTitle, ReleaseDescriptor, VideoKind};
use unicode_normalization::UnicodeNormalization;

pub fn normalize_title(value: &str) -> NormalizedTitle {
    let mut normalized = String::new();
    let mut separator = true;
    for character in value.nfc().flat_map(char::to_lowercase) {
        if character.is_alphanumeric() {
            normalized.push(character);
            separator = false;
        } else if !separator && !normalized.is_empty() {
            normalized.push(' ');
            separator = true;
        }
    }
    if separator {
        normalized.pop();
    }
    NormalizedTitle::from_normalized(normalized)
}

pub fn parse_release(value: &str) -> ReleaseDescriptor {
    let name = leaf_without_extension(value);
    let upper = name.to_ascii_uppercase();
    let episode = find_episode(&upper);
    let date = find_date(&upper);
    let season = episode
        .as_ref()
        .map(|value| {
            (
                value.start,
                value.season,
                Some(value.episode),
                value.episode_end,
            )
        })
        .or_else(|| find_season(&upper).map(|(start, season)| (start, season, None, None)));
    let absolute = find_absolute_episode(name);

    let (kind, marker, parsed_season, parsed_episode, episode_end, air_date, absolute_episode) =
        if let Some((start, season, episode, episode_end)) = season {
            (
                if episode.is_some() {
                    VideoKind::Episode
                } else {
                    VideoKind::SeasonPack
                },
                start,
                Some(season),
                episode,
                episode_end,
                None,
                None,
            )
        } else if let Some((start, date)) = date {
            (
                VideoKind::DateEpisode,
                start,
                None,
                None,
                None,
                Some(date),
                None,
            )
        } else if let Some((start, episode)) = absolute {
            (
                VideoKind::AbsoluteEpisode,
                start,
                None,
                None,
                None,
                None,
                Some(episode),
            )
        } else if is_disc(&upper) {
            (VideoKind::Disc, name.len(), None, None, None, None, None)
        } else if let Some((start, year)) = find_year(&upper) {
            let mut descriptor = descriptor(VideoKind::Movie, &name[..start]);
            descriptor.year = Some(year);
            add_technical_metadata(&mut descriptor, name);
            return descriptor;
        } else {
            (
                VideoKind::UnknownVideo,
                technical_suffix_start(name),
                None,
                None,
                None,
                None,
                None,
            )
        };

    let mut descriptor = descriptor(kind, &name[..marker]);
    descriptor.season = parsed_season;
    descriptor.episode = parsed_episode;
    descriptor.episode_end = episode_end;
    descriptor.air_date = air_date;
    descriptor.absolute_episode = absolute_episode;
    add_technical_metadata(&mut descriptor, name);
    descriptor
}

fn descriptor(kind: VideoKind, title: &str) -> ReleaseDescriptor {
    let mut descriptor = ReleaseDescriptor::unknown(normalize_title(title));
    descriptor.kind = kind;
    descriptor
}

fn leaf_without_extension(value: &str) -> &str {
    let leaf = value.rsplit(['/', '\\']).next().unwrap_or(value);
    let Some((stem, extension)) = leaf.rsplit_once('.') else {
        return leaf;
    };
    if matches!(
        extension.to_ascii_lowercase().as_str(),
        "mkv"
            | "mp4"
            | "m4v"
            | "avi"
            | "ts"
            | "m2ts"
            | "mov"
            | "wmv"
            | "webm"
            | "iso"
            | "nfo"
            | "srt"
    ) {
        stem
    } else {
        leaf
    }
}

#[derive(Debug, Clone, Copy)]
struct EpisodeMarker {
    start: usize,
    season: u16,
    episode: u16,
    episode_end: Option<u16>,
}

fn find_episode(value: &str) -> Option<EpisodeMarker> {
    let bytes = value.as_bytes();
    for start in 0..bytes.len() {
        if bytes[start] != b'S' {
            continue;
        }
        let Some((season, after_season)) = digits(bytes, start + 1, 1, 2) else {
            continue;
        };
        if bytes.get(after_season) != Some(&b'E') {
            continue;
        }
        let Some((episode, mut end)) = digits(bytes, after_season + 1, 1, 3) else {
            continue;
        };
        let mut episode_end = None;
        if bytes.get(end) == Some(&b'-') {
            end += 1;
        }
        if bytes.get(end) == Some(&b'E')
            && let Some((last, _)) = digits(bytes, end + 1, 1, 3)
        {
            episode_end = Some(last as u16);
        }
        return Some(EpisodeMarker {
            start,
            season: season as u16,
            episode: episode as u16,
            episode_end,
        });
    }
    None
}

fn find_season(value: &str) -> Option<(usize, u16)> {
    let bytes = value.as_bytes();
    for start in 0..bytes.len() {
        if bytes[start] != b'S' {
            continue;
        }
        let Some((season, end)) = digits(bytes, start + 1, 1, 2) else {
            continue;
        };
        if bytes.get(end) != Some(&b'E') && boundary(bytes.get(end).copied()) {
            return Some((start, season as u16));
        }
    }
    None
}

fn find_date(value: &str) -> Option<(usize, Date)> {
    let bytes = value.as_bytes();
    for start in 0..bytes.len().saturating_sub(9) {
        let slice = &bytes[start..start + 10];
        if !slice[..4].iter().all(u8::is_ascii_digit)
            || !matches!(slice[4], b'.' | b'-')
            || !slice[5..7].iter().all(u8::is_ascii_digit)
            || slice[7] != slice[4]
            || !slice[8..].iter().all(u8::is_ascii_digit)
        {
            continue;
        }
        let year = parse_digits(&slice[..4]) as u16;
        let month = parse_digits(&slice[5..7]) as u8;
        let day = parse_digits(&slice[8..]) as u8;
        if (1900..=2099).contains(&year) && (1..=12).contains(&month) && valid_day(year, month, day)
        {
            return Some((start, Date { year, month, day }));
        }
    }
    None
}

fn find_absolute_episode(value: &str) -> Option<(usize, u32)> {
    let bytes = value.as_bytes();
    for start in 0..bytes.len() {
        if bytes[start] != b'[' {
            continue;
        }
        let Some((episode, end)) = digits(bytes, start + 1, 1, 4) else {
            continue;
        };
        if bytes.get(end) == Some(&b']') {
            return Some((start, episode));
        }
    }
    None
}

fn find_year(value: &str) -> Option<(usize, u16)> {
    let bytes = value.as_bytes();
    for start in 0..bytes.len().saturating_sub(3) {
        if start > 0 && bytes[start - 1].is_ascii_digit() {
            continue;
        }
        let slice = &bytes[start..start + 4];
        if slice.iter().all(u8::is_ascii_digit)
            && boundary(bytes.get(start + 4).copied())
            && (1900..=2099).contains(&parse_digits(slice))
        {
            return Some((start, parse_digits(slice) as u16));
        }
    }
    None
}

fn digits(bytes: &[u8], start: usize, minimum: usize, maximum: usize) -> Option<(u32, usize)> {
    let mut end = start;
    while end < bytes.len() && end - start < maximum && bytes[end].is_ascii_digit() {
        end += 1;
    }
    (end - start >= minimum).then(|| (parse_digits(&bytes[start..end]), end))
}

fn parse_digits(bytes: &[u8]) -> u32 {
    bytes
        .iter()
        .fold(0, |value, digit| value * 10 + u32::from(*digit - b'0'))
}

fn boundary(value: Option<u8>) -> bool {
    value.is_none_or(|value| !value.is_ascii_alphanumeric())
}

fn valid_day(year: u16, month: u8, day: u8) -> bool {
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let maximum = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    (1..=maximum).contains(&day)
}

fn is_disc(value: &str) -> bool {
    value.contains("BDMV") || value.contains("VIDEO_TS")
}

fn technical_suffix_start(value: &str) -> usize {
    value
        .char_indices()
        .find_map(|(index, _)| {
            let suffix = value[index..].to_ascii_uppercase();
            [
                "2160P", "1080P", "720P", "BLURAY", "WEB-DL", "WEBRIP", "HDTV",
            ]
            .iter()
            .any(|marker| suffix.starts_with(marker))
            .then_some(index)
        })
        .unwrap_or(value.len())
}

fn add_technical_metadata(descriptor: &mut ReleaseDescriptor, value: &str) {
    let upper = value.to_ascii_uppercase();
    descriptor.resolution = ["2160P", "1080P", "720P", "576P", "480P"]
        .into_iter()
        .find(|marker| upper.contains(marker))
        .map(str::to_owned);
    descriptor.source = ["WEB-DL", "WEBRIP", "BLURAY", "HDTV", "DVD"]
        .into_iter()
        .find(|marker| upper.contains(marker))
        .map(str::to_owned);
    descriptor.video_codec = ["X265", "HEVC", "X264", "H264", "AV1"]
        .into_iter()
        .find(|marker| upper.contains(marker))
        .map(str::to_owned);
    descriptor.hdr = ["DOLBY VISION", "DV", "HDR10+", "HDR10", "HDR"]
        .into_iter()
        .find(|marker| upper.contains(marker))
        .map(str::to_owned);
    descriptor.audio = ["TRUEHD", "DTS-HD", "DTS", "DDP", "AAC", "FLAC"]
        .into_iter()
        .find(|marker| upper.contains(marker))
        .map(str::to_owned);
    descriptor.release_group = value
        .rsplit_once('-')
        .map(|(_, group)| group.trim())
        .filter(|group| !group.is_empty() && !group.contains('/') && !group.contains('\\'))
        .map(str::to_owned);
}

#[cfg(test)]
mod tests {
    use sporos_model::{Date, VideoKind};

    use super::{normalize_title, parse_release};

    #[test]
    fn normalizes_unicode_and_separators() {
        assert_eq!(
            normalize_title("  Amélie...DIRECTOR’S CUT ").as_str(),
            "amélie director s cut"
        );
        assert_eq!(
            normalize_title("Cafe\u{301}").as_str(),
            normalize_title("Café").as_str()
        );
    }

    #[test]
    fn parses_episode_ranges() {
        let release = parse_release("Example.Show.S03E07-E08.2160p.WEB-DL-GROUP.mkv");
        assert_eq!(release.kind, VideoKind::Episode);
        assert_eq!(release.primary_title.as_str(), "example show");
        assert_eq!(release.season, Some(3));
        assert_eq!(release.episode, Some(7));
        assert_eq!(release.episode_end, Some(8));
        assert_eq!(release.resolution.as_deref(), Some("2160P"));
        assert_eq!(release.source.as_deref(), Some("WEB-DL"));
        assert_eq!(release.release_group.as_deref(), Some("GROUP"));
    }

    #[test]
    fn parses_seasons_dates_absolute_episodes_and_movies() {
        let season = parse_release("Example.Show.S02.1080p");
        assert_eq!(season.kind, VideoKind::SeasonPack);
        assert_eq!(season.season, Some(2));

        let date = parse_release("Daily.Show.2026-08-24.720p");
        assert_eq!(date.kind, VideoKind::DateEpisode);
        assert_eq!(
            date.air_date,
            Some(Date {
                year: 2026,
                month: 8,
                day: 24
            })
        );

        let absolute = parse_release("Anime Title [012] 1080p");
        assert_eq!(absolute.kind, VideoKind::AbsoluteEpisode);
        assert_eq!(absolute.absolute_episode, Some(12));

        let movie = parse_release("The.Movie.2024.1080p.BluRay");
        assert_eq!(movie.kind, VideoKind::Movie);
        assert_eq!(movie.year, Some(2024));
        assert_eq!(movie.primary_title.as_str(), "the movie");
    }

    #[test]
    fn invalid_calendar_date_remains_an_unknown_release() {
        assert_eq!(
            parse_release("Daily.Show.2025.02.29.720p").kind,
            VideoKind::Movie
        );
    }
}
