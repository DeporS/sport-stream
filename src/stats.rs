//! Parser for the Flashscore statistics feed (`df_st_1_<match id>`), based on
//! the format handled by `_parse_flashscore_format` in value-bet-live-detection.
//!
//! Records are separated by `~`, fields by `¬`, key and value by `÷`. `SE` sets
//! the current period, `SG` is the statistic name, `SH`/`SI` are the home/away
//! values. Every `SG` row becomes one record, so new metrics, unknown period
//! names and the same metric in several periods need no code changes.
//!
//! Differences from the reference parser, which keeps only the `Match` section,
//! looks up a fixed list of names and turns a missing or unparsable value into 0:
//! here all periods are kept, a missing value is `null` (never 0), and text that
//! is not clearly a number is kept verbatim.

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Number;

use crate::parser::ParseError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueKind {
    /// No value in the feed (field absent or empty): `raw` is `null`.
    Missing,
    Integer,
    Decimal,
    /// `58%`: `value` is the percentage (58).
    Percent,
    /// `18/24`: only `numerator` and `denominator`; `value` stays `null`.
    Fraction,
    /// `75% (18/24)` or `18/24 (75%)`: `value` is the percentage.
    PercentFraction,
    /// Anything else, kept verbatim in `raw`.
    Text,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StatValue {
    /// The displayed value as sent (`value` in the JSON format); `None` when absent or blank.
    pub raw: Option<String>,
    pub kind: ValueKind,
    pub value: Option<Number>,
    pub numerator: Option<u64>,
    pub denominator: Option<u64>,
    /// JSON format only: the more precise `rawValue` (e.g. `"0.3389"` next to `"0.34"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_value: Option<String>,
    /// JSON format only: the `subValue` text (e.g. `"(80%)"`), kept verbatim.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_value: Option<String>,
}

/// One statistics response: its records and when the collector received it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StatisticsReading {
    pub observed_at: DateTime<Utc>,
    pub records: Vec<StatRecord>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StatRecord {
    /// Text format: value of the last `SE` seen before this row; `None` if there was none.
    /// JSON format: always `None`, the stage is only known by its numeric id.
    pub period: Option<String>,
    /// JSON format only: `eventStageId`. Its meaning is not documented by the source, so it is
    /// kept as an id and never turned into a period name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_stage_id: Option<i64>,
    /// Text format: original `SG` text. JSON format: `typeId`.
    pub name: String,
    pub home: StatValue,
    pub away: StatValue,
}

fn int_number(digits: &str) -> Option<Number> {
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<u64>().ok().map(Number::from)
}

fn unsigned(digits: &str) -> Option<u64> {
    int_number(digits).and_then(|n| n.as_u64())
}

/// `12`, `-3`, `1.87`, `-0.5` (no signs other than a leading `-`, no commas).
fn plain_number(s: &str) -> Option<(ValueKind, Number)> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    if let Some(n) = int_number(body) {
        let n = if neg {
            Number::from(-i64::try_from(n.as_u64()?).ok()?)
        } else {
            n
        };
        return Some((ValueKind::Integer, n));
    }
    let (int, frac) = body.split_once('.')?;
    if int.is_empty()
        || frac.is_empty()
        || !int.bytes().all(|b| b.is_ascii_digit())
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let f: f64 = s.parse().ok()?;
    Number::from_f64(f).map(|n| (ValueKind::Decimal, n))
}

fn percent(s: &str) -> Option<Number> {
    let body = s.strip_suffix('%')?.trim_end();
    if body.starts_with('-') {
        return None;
    }
    plain_number(body).map(|(_, n)| n)
}

fn fraction(s: &str) -> Option<(u64, u64)> {
    let (n, d) = s.split_once('/')?;
    Some((unsigned(n.trim())?, unsigned(d.trim())?))
}

fn percent_fraction(s: &str) -> Option<(Number, u64, u64)> {
    let inner_end = s.strip_suffix(')')?;
    let (left, inner) = inner_end.split_once('(')?;
    let (left, inner) = (left.trim(), inner.trim());
    if let (Some(p), Some((n, d))) = (percent(left), fraction(inner)) {
        return Some((p, n, d));
    }
    if let (Some((n, d)), Some(p)) = (fraction(left), percent(inner)) {
        return Some((p, n, d));
    }
    None
}

/// Interprets one raw cell. `None` (absent or blank) is `Missing`; a real `0`
/// is an `Integer` with value 0.
pub fn classify(raw: Option<&str>) -> StatValue {
    let raw = raw.map(str::trim).filter(|s| !s.is_empty());
    let mut v = StatValue {
        raw: raw.map(str::to_string),
        kind: ValueKind::Missing,
        value: None,
        numerator: None,
        denominator: None,
        raw_value: None,
        sub_value: None,
    };
    let Some(s) = raw else { return v };
    if let Some((kind, n)) = plain_number(s) {
        (v.kind, v.value) = (kind, Some(n));
    } else if let Some(p) = percent(s) {
        (v.kind, v.value) = (ValueKind::Percent, Some(p));
    } else if let Some((n, d)) = fraction(s) {
        (v.kind, v.numerator, v.denominator) = (ValueKind::Fraction, Some(n), Some(d));
    } else if let Some((p, n, d)) = percent_fraction(s) {
        v.kind = ValueKind::PercentFraction;
        (v.value, v.numerator, v.denominator) = (Some(p), Some(n), Some(d));
    } else {
        v.kind = ValueKind::Text;
    }
    v
}

/// Looks up `key` in one record. Repeated with different values -> ambiguous.
fn lookup<'a>(
    record: &'a str,
    key: &str,
    field: &'static str,
) -> Result<Option<&'a str>, ParseError> {
    let mut found: Option<&str> = None;
    for pair in record.split('¬') {
        let Some((k, v)) = pair.split_once('÷') else {
            continue;
        };
        if k.trim() != key {
            continue;
        }
        let v = v.trim();
        match found {
            Some(previous) if previous != v => return Err(ParseError::AmbiguousField { field }),
            _ => found = Some(v),
        }
    }
    Ok(found)
}

pub fn parse_stats_feed(payload: &str) -> Result<Vec<StatRecord>, ParseError> {
    if payload.trim().is_empty() {
        return Err(ParseError::EmptyPayload);
    }
    let mut period: Option<String> = None;
    let mut records = Vec::new();
    for record in payload.split('~').map(str::trim).filter(|r| !r.is_empty()) {
        if let Some(se) = lookup(record, "SE", "period (SE)")? {
            if se.is_empty() {
                return Err(ParseError::MissingField("period (SE)"));
            }
            period = Some(se.to_string());
        }
        let name = lookup(record, "SG", "statistic name (SG)")?;
        let home = lookup(record, "SH", "home value (SH)")?;
        let away = lookup(record, "SI", "away value (SI)")?;
        match name {
            Some("") => return Err(ParseError::MissingField("statistic name (SG)")),
            Some(name) => records.push(StatRecord {
                period: period.clone(),
                event_stage_id: None,
                name: name.to_string(),
                home: classify(home),
                away: classify(away),
            }),
            None if home.is_some() || away.is_some() => {
                return Err(ParseError::MissingField("statistic name (SG)"));
            }
            None => {}
        }
    }
    if records.is_empty() {
        return Err(ParseError::MissingField("statistics (SG rows)"));
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL: &str = include_str!("../fixtures/flashscore/stats_full.txt");

    fn int(n: i64) -> Option<Number> {
        Some(Number::from(n))
    }

    fn dec(f: f64) -> Option<Number> {
        Number::from_f64(f)
    }

    fn find<'a>(records: &'a [StatRecord], period: &str, name: &str) -> &'a StatRecord {
        records
            .iter()
            .find(|r| r.period.as_deref() == Some(period) && r.name == name)
            .unwrap_or_else(|| panic!("no {name} in {period}"))
    }

    #[test]
    fn classifies_values() {
        let c = |s| classify(Some(s));
        assert_eq!((c("12").kind, c("12").value), (ValueKind::Integer, int(12)));
        assert_eq!((c("-3").kind, c("-3").value), (ValueKind::Integer, int(-3)));
        assert_eq!(
            (c("1.87").kind, c("1.87").value),
            (ValueKind::Decimal, dec(1.87))
        );
        assert_eq!(
            (c("58%").kind, c("58%").value),
            (ValueKind::Percent, int(58))
        );
        assert_eq!(c("33.5%").value, dec(33.5));
        let f = c("18/24");
        assert_eq!(
            (f.kind, f.value, f.numerator, f.denominator),
            (ValueKind::Fraction, None, Some(18), Some(24))
        );
        for s in ["75% (18/24)", "18/24 (75%)"] {
            let pf = c(s);
            assert_eq!(
                (pf.kind, pf.value, pf.numerator, pf.denominator),
                (ValueKind::PercentFraction, int(75), Some(18), Some(24)),
                "{s}"
            );
            assert_eq!(pf.raw.as_deref(), Some(s));
        }
    }

    #[test]
    fn missing_is_null_and_zero_is_zero() {
        for blank in [None, Some(""), Some("  ")] {
            let m = classify(blank);
            assert_eq!((m.kind, m.raw, m.value), (ValueKind::Missing, None, None));
        }
        let z = classify(Some("0"));
        assert_eq!(
            (z.kind, z.raw.as_deref(), z.value),
            (ValueKind::Integer, Some("0"), int(0))
        );
        assert_eq!(classify(Some("0%")).value, int(0));
        assert_eq!(classify(Some("0.0")).value, dec(0.0));
    }

    #[test]
    fn unrecognised_text_is_kept_raw() {
        for s in [
            "n/a",
            "-",
            "1,5",
            "1.",
            ".5",
            "--1",
            "-5%",
            "1/",
            "/2",
            "1/2/3",
            "75% (18/x)",
            "(18/24)",
            "12 shots",
            "99999999999999999999",
        ] {
            let v = classify(Some(s));
            assert_eq!(v.kind, ValueKind::Text, "{s:?}");
            assert_eq!(v.raw.as_deref(), Some(s));
            assert_eq!((v.value, v.numerator, v.denominator), (None, None, None));
        }
    }

    #[test]
    fn parses_full_fixture() {
        let r = parse_stats_feed(FULL).unwrap();
        assert_eq!(r.len(), 9 + 3 + 2 + 1);
        let periods: Vec<_> =
            r.iter()
                .map(|x| x.period.as_deref().unwrap())
                .fold(Vec::new(), |mut acc, p| {
                    if acc.last() != Some(&p) {
                        acc.push(p);
                    }
                    acc
                });
        assert_eq!(periods, ["Match", "1st Half", "2nd Half", "Extra Time"]);

        let xg = find(&r, "Match", "Expected goals (xG)");
        assert_eq!(
            (xg.home.value.clone(), xg.away.value.clone()),
            (dec(1.87), dec(0.64))
        );
        assert_eq!(
            find(&r, "1st Half", "Expected goals (xG)").home.value,
            dec(0.92)
        );
        assert_eq!(
            find(&r, "2nd Half", "Expected goals (xG)").home.value,
            dec(0.95)
        );

        let crosses = find(&r, "Match", "Crosses");
        assert_eq!(crosses.home.kind, ValueKind::PercentFraction);
        assert_eq!(crosses.home.raw.as_deref(), Some("75% (18/24)"));
        assert_eq!(crosses.away.denominator, Some(12));

        let tackles = find(&r, "Match", "Tackles");
        assert_eq!(tackles.away.kind, ValueKind::Fraction);

        let offsides = find(&r, "Match", "Offsides");
        assert_eq!(offsides.home.value, int(3));
        assert_eq!(
            (offsides.away.kind, offsides.away.raw.clone()),
            (ValueKind::Missing, None)
        );

        let red = find(&r, "Match", "Red cards");
        assert_eq!(
            (red.home.value.clone(), red.away.value.clone()),
            (int(0), int(0))
        );

        assert_eq!(find(&r, "Match", "Throw-ins").home.value, int(21));
        assert_eq!(find(&r, "Match", "Dribbles").home.kind, ValueKind::Text);
        assert_eq!(find(&r, "Extra Time", "Total shots").home.value, int(2));
    }

    #[test]
    fn same_name_in_different_periods_gives_separate_records() {
        let r = parse_stats_feed(FULL).unwrap();
        let shots: Vec<_> = r.iter().filter(|x| x.name == "Total shots").collect();
        assert_eq!(shots.len(), 4);
        assert_eq!(shots[0].home.value, int(14));
        assert_eq!(shots[1].home.value, int(7));
    }

    #[test]
    fn repeated_row_in_one_period_is_kept_twice() {
        let r = parse_stats_feed("SE÷Match¬~SG÷Fouls¬SH÷1¬SI÷2¬~SG÷Fouls¬SH÷3¬SI÷4¬~").unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(
            (r[0].home.value.clone(), r[1].home.value.clone()),
            (int(1), int(3))
        );
    }

    #[test]
    fn unknown_period_names_and_rows_before_any_period() {
        let r = parse_stats_feed("SG÷Fouls¬SH÷1¬SI÷2¬~SE÷Penalty Shoot-out¬~SG÷Goals¬SH÷4¬SI÷3¬~")
            .unwrap();
        assert_eq!(r[0].period, None);
        assert_eq!(r[1].period.as_deref(), Some("Penalty Shoot-out"));
    }

    #[test]
    fn new_metric_needs_no_code_change() {
        let r = parse_stats_feed("SE÷Match¬~SG÷Brand New Metric 2031¬SH÷7¬SI÷1¬~").unwrap();
        assert_eq!(r[0].name, "Brand New Metric 2031");
    }

    #[test]
    fn rejects_empty_and_corrupt_payloads() {
        assert_eq!(parse_stats_feed(""), Err(ParseError::EmptyPayload));
        assert_eq!(parse_stats_feed(" \n"), Err(ParseError::EmptyPayload));
        let no_stats = Err(ParseError::MissingField("statistics (SG rows)"));
        assert_eq!(parse_stats_feed("garbage without separators"), no_stats);
        assert_eq!(parse_stats_feed("SE÷Match¬~"), no_stats);
        assert_eq!(parse_stats_feed("~~~"), no_stats);
        let no_name = Err(ParseError::MissingField("statistic name (SG)"));
        assert_eq!(parse_stats_feed("SE÷Match¬~SH÷1¬SI÷2¬~"), no_name);
        assert_eq!(parse_stats_feed("SG÷¬SH÷1¬SI÷2¬~"), no_name);
        assert_eq!(
            parse_stats_feed("SE÷¬~SG÷Fouls¬SH÷1¬SI÷2¬~"),
            Err(ParseError::MissingField("period (SE)"))
        );
        assert_eq!(
            parse_stats_feed("SG÷Fouls¬SH÷1¬SH÷2¬SI÷2¬~"),
            Err(ParseError::AmbiguousField {
                field: "home value (SH)"
            })
        );
        assert_eq!(
            parse_stats_feed("SG÷Fouls¬SG÷Cards¬SH÷1¬SI÷2¬~"),
            Err(ParseError::AmbiguousField {
                field: "statistic name (SG)"
            })
        );
    }
}
