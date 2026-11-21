//! Parser for the Flashscore "core" feed (`dc_1_<match id>`), based on the
//! format handled by `_parse_flashscore_format` in value-bet-live-detection.
//!
//! The payload is a flat list of `key÷value` pairs separated by `¬` (records
//! may end with `~`). The score lives in `DE` (home goals) and `DF` (away
//! goals); `DA`/`DB`/`DD` carry status and period start, which this stage
//! ignores. Neither this parser nor the reference one reads team names from the
//! core payload, so they are passed in as match metadata.
//!
//! Deliberately stricter than the reference parser, which defaults a missing or
//! unparsable score to 0, lets the last duplicate key win and does not split the
//! core payload on `~`: here a missing, invalid or conflicting score is an error,
//! so a wrong `0:0` can never reach Kafka.

use crate::parser::{MatchSnapshot, ParseError};

const HOME_SCORE_KEY: &str = "DE";
const AWAY_SCORE_KEY: &str = "DF";

/// Looks up a score key. Missing -> `MissingField`, non-numeric ->
/// `InvalidField`, repeated with different values -> `AmbiguousField`.
fn score_field(payload: &str, key: &str, field: &'static str) -> Result<u8, ParseError> {
    let mut found: Option<&str> = None;
    for pair in payload.split(['¬', '~']) {
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
    let raw = found.ok_or(ParseError::MissingField(field))?;
    let invalid = || ParseError::InvalidField {
        field,
        value: raw.to_string(),
    };
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    raw.parse().map_err(|_| invalid())
}

pub fn parse_core_feed(
    payload: &str,
    home_team: &str,
    away_team: &str,
) -> Result<MatchSnapshot, ParseError> {
    if payload.trim().is_empty() {
        return Err(ParseError::EmptyPayload);
    }
    Ok(MatchSnapshot {
        home_team: home_team.trim().to_string(),
        away_team: away_team.trim().to_string(),
        home_score: score_field(payload, HOME_SCORE_KEY, "home score (DE)")?,
        away_score: score_field(payload, AWAY_SCORE_KEY, "away score (DF)")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(payload: &str) -> Result<MatchSnapshot, ParseError> {
        parse_core_feed(payload, " Lech Poznań ", "Legia Warszawa")
    }

    fn score(payload: &str) -> (u8, u8) {
        let s = parse(payload).unwrap();
        (s.home_score, s.away_score)
    }

    #[test]
    fn parses_fixtures() {
        assert_eq!(
            score(include_str!("../fixtures/flashscore/core_0-0.txt")),
            (0, 0)
        );
        assert_eq!(
            score(include_str!("../fixtures/flashscore/core_1-0.txt")),
            (1, 0)
        );
        assert_eq!(
            score(include_str!("../fixtures/flashscore/core_2-1.txt")),
            (2, 1)
        );
    }

    #[test]
    fn real_zero_zero_is_a_valid_score_and_teams_come_from_metadata() {
        let s = parse("DE÷0¬DF÷0¬").unwrap();
        assert_eq!(
            s,
            MatchSnapshot {
                home_team: "Lech Poznań".into(),
                away_team: "Legia Warszawa".into(),
                home_score: 0,
                away_score: 0,
            }
        );
    }

    #[test]
    fn rejects_empty_payload() {
        assert_eq!(parse(""), Err(ParseError::EmptyPayload));
        assert_eq!(parse(" \n"), Err(ParseError::EmptyPayload));
    }

    #[test]
    fn rejects_missing_score() {
        assert_eq!(
            parse("DA÷2¬DF÷1¬"),
            Err(ParseError::MissingField("home score (DE)"))
        );
        assert_eq!(
            parse("DA÷2¬DE÷1¬"),
            Err(ParseError::MissingField("away score (DF)"))
        );
        assert!(matches!(
            parse("garbage without separators"),
            Err(ParseError::MissingField(_))
        ));
    }

    #[test]
    fn rejects_invalid_values() {
        for bad in ["", "x", "-1", "1.5", "+1", "999"] {
            let payload = format!("DE÷{bad}¬DF÷0¬");
            assert_eq!(
                parse(&payload),
                Err(ParseError::InvalidField {
                    field: "home score (DE)",
                    value: bad.to_string()
                }),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn rejects_ambiguous_score_but_accepts_repeated_equal_values() {
        assert_eq!(
            parse("DE÷1¬DE÷2¬DF÷0¬"),
            Err(ParseError::AmbiguousField {
                field: "home score (DE)"
            })
        );
        assert_eq!(score("DE÷1¬DF÷0¬~DE÷1¬DF÷0¬"), (1, 0));
    }
}
