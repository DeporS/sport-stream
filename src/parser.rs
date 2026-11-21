use std::fmt;

use scraper::{Html, Selector};
use serde::Serialize;

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct MatchSnapshot {
    pub home_team: String,
    pub away_team: String,
    pub home_score: u8,
    pub away_score: u8,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParseError {
    MissingField(&'static str),
    InvalidScore(String),
    EmptyPayload,
    InvalidField {
        field: &'static str,
        value: String,
    },
    AmbiguousField {
        field: &'static str,
    },
    /// The payload is not valid JSON.
    Json(String),
    /// The source answered with an `errors` list instead of (or next to) data.
    SourceErrors(String),
    /// One statistics entry is malformed (wrong type, unknown team, conflicting duplicate...).
    InvalidEntry(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField(name) => write!(f, "missing or empty field: {name}"),
            Self::InvalidScore(raw) => write!(f, "invalid score {raw:?}, expected format \"H:A\""),
            Self::EmptyPayload => write!(f, "empty payload"),
            Self::InvalidField { field, value } => {
                write!(
                    f,
                    "invalid {field}: {value:?} is not a non-negative integer"
                )
            }
            Self::AmbiguousField { field } => {
                write!(
                    f,
                    "ambiguous {field}: the payload contains conflicting values"
                )
            }
            Self::Json(e) => write!(f, "invalid JSON: {e}"),
            Self::SourceErrors(e) => write!(f, "the source reported errors: {e}"),
            Self::InvalidEntry(e) => write!(f, "invalid statistics entry: {e}"),
        }
    }
}

impl std::error::Error for ParseError {}

fn select_text(doc: &Html, css: &'static str, field: &'static str) -> Result<String, ParseError> {
    let selector = Selector::parse(css).expect("static selector is valid");
    let text = doc
        .select(&selector)
        .next()
        .map(|el| el.text().collect::<String>().trim().to_string())
        .unwrap_or_default();
    if text.is_empty() {
        Err(ParseError::MissingField(field))
    } else {
        Ok(text)
    }
}

fn parse_score(raw: &str) -> Result<(u8, u8), ParseError> {
    let invalid = || ParseError::InvalidScore(raw.to_string());
    let (home, away) = raw.split_once(':').ok_or_else(invalid)?;
    let home = home.trim().parse().map_err(|_| invalid())?;
    let away = away.trim().parse().map_err(|_| invalid())?;
    Ok((home, away))
}

pub fn parse_match(html: &str) -> Result<MatchSnapshot, ParseError> {
    let doc = Html::parse_document(html);
    let home_team = select_text(&doc, ".match .home-team", "home_team")?;
    let away_team = select_text(&doc, ".match .away-team", "away_team")?;
    let score = select_text(&doc, ".match .score", "score")?;
    let (home_score, away_score) = parse_score(&score)?;
    Ok(MatchSnapshot {
        home_team,
        away_team,
        home_score,
        away_score,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = include_str!("../fixtures/match.html");

    #[test]
    fn parses_valid_match() {
        let snap = parse_match(VALID).unwrap();
        assert_eq!(
            snap,
            MatchSnapshot {
                home_team: "Lech Poznań".into(),
                away_team: "Legia Warszawa".into(),
                home_score: 2,
                away_score: 1,
            }
        );
    }

    #[test]
    fn reports_missing_field() {
        let html = r#"<div class="match"><span class="home-team">A</span><span class="score">1:0</span></div>"#;
        assert_eq!(
            parse_match(html),
            Err(ParseError::MissingField("away_team"))
        );
    }

    #[test]
    fn reports_invalid_score() {
        for bad in ["abc", "2-1", "2:", "x:1", "300:1"] {
            let html = format!(
                r#"<div class="match"><span class="home-team">A</span><span class="away-team">B</span><span class="score">{bad}</span></div>"#
            );
            assert_eq!(
                parse_match(&html),
                Err(ParseError::InvalidScore(bad.to_string())),
                "score {bad:?}"
            );
        }
    }
}
