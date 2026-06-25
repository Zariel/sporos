const SECONDS_PER_DAY: i64 = 86_400;
const MILLIS_PER_SECOND: i64 = 1_000;

pub fn unix_ms_to_rfc3339_seconds(unix_ms: i64) -> String {
    let seconds = unix_ms.div_euclid(MILLIS_PER_SECOND);
    let days = seconds.div_euclid(SECONDS_PER_DAY);
    let seconds_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }

    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::unix_ms_to_rfc3339_seconds;

    #[test]
    fn unix_ms_to_rfc3339_seconds_formats_utc_without_fractional_seconds() {
        assert_eq!("1970-01-01T00:00:00Z", unix_ms_to_rfc3339_seconds(0));
        assert_eq!(
            "2023-11-14T22:13:20Z",
            unix_ms_to_rfc3339_seconds(1_700_000_000_999)
        );
        assert_eq!(
            "2026-06-24T23:22:15Z",
            unix_ms_to_rfc3339_seconds(1_782_343_335_130)
        );
    }
}
