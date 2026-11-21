//! Parser for the Flashscore GraphQL team statistics response (JSON).
//!
//! Entries live under `data.findEventById.teamStats.entries`. Each entry is one value of one
//! team in one stage: `{typeId, value, subValue, rawValue, eventStageId, teamId}`. The response
//! has no home/away marker and no stage names, so:
//!  * home and away are chosen by the caller (explicit team ids), and an unknown `teamId` is an
//!    error rather than being dropped;
//!  * `eventStageId` is kept as a number (`event_stage_id`); the record's `period` stays `None`,
//!    because the source does not say what a stage id means.
//!
//! Entries are grouped by `(eventStageId, typeId)` in order of first appearance. A side with no
//! entry stays missing, identical duplicates collapse, conflicting duplicates are an error.
//! `value` is the displayed text (`StatValue.raw`, interpreted like the text format), `rawValue`
//! the more precise number and `subValue` extra text such as `(80%)`; all three are kept as sent.

use std::collections::{BTreeSet, HashMap};

use serde_json::Value;

use crate::parser::ParseError;
use crate::stats::{StatRecord, StatValue, classify};

const ENTRIES_PATH: &str = "data.findEventById.teamStats.entries";

/// The three strings of one entry exactly as sent (blank or `null` is `None`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cell {
    pub value: Option<String>,
    pub raw_value: Option<String>,
    pub sub_value: Option<String>,
}

#[derive(Default)]
struct Pair {
    home: Option<Cell>,
    away: Option<Cell>,
}

fn invalid(index: usize, what: &str) -> ParseError {
    ParseError::InvalidEntry(format!("entries[{index}]: {what}"))
}

fn required_str<'a>(
    entry: &'a serde_json::Map<String, Value>,
    key: &str,
    index: usize,
) -> Result<&'a str, ParseError> {
    match entry.get(key) {
        Some(Value::String(s)) if !s.trim().is_empty() => Ok(s),
        _ => Err(invalid(index, &format!("{key} must be a non-empty string"))),
    }
}

/// `null` / absent / blank -> `None`; a string is kept trimmed; anything else is an error.
fn optional_str(
    entry: &serde_json::Map<String, Value>,
    key: &str,
    index: usize,
) -> Result<Option<String>, ParseError> {
    match entry.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.trim().to_string()).filter(|s| !s.is_empty())),
        Some(other) => Err(invalid(
            index,
            &format!("{key} must be a string or null, got {other}"),
        )),
    }
}

fn source_errors(root: &Value) -> Option<String> {
    let errors = root.get("errors")?.as_array().filter(|e| !e.is_empty())?;
    let messages: Vec<String> = errors
        .iter()
        .map(|e| {
            e.get("message")
                .and_then(Value::as_str)
                .map_or_else(|| e.to_string(), str::to_string)
        })
        .collect();
    Some(messages.join("; "))
}

fn side_value(cell: Option<&Cell>) -> StatValue {
    let Some(cell) = cell else {
        return classify(None);
    };
    let mut v = classify(cell.value.as_deref());
    v.raw_value = cell.raw_value.clone();
    v.sub_value = cell.sub_value.clone();
    v
}

/// One validated entry of the response, before it is assigned to a side.
struct Entry {
    index: usize,
    stage: i64,
    type_id: String,
    team_id: String,
    cell: Cell,
}

/// Validates the envelope and every entry. Shared by both parsers; knows nothing about sides.
fn parse_entries(payload: &str) -> Result<Vec<Entry>, ParseError> {
    if payload.trim().is_empty() {
        return Err(ParseError::EmptyPayload);
    }
    let root: Value = serde_json::from_str(payload).map_err(|e| ParseError::Json(e.to_string()))?;
    if !root.is_object() {
        return Err(ParseError::Json("expected a JSON object".to_string()));
    }
    if let Some(messages) = source_errors(&root) {
        return Err(ParseError::SourceErrors(messages));
    }
    let entries = root
        .pointer("/data/findEventById/teamStats/entries")
        .filter(|v| !v.is_null())
        .ok_or(ParseError::MissingField(ENTRIES_PATH))?;
    let entries = entries
        .as_array()
        .ok_or_else(|| ParseError::InvalidEntry(format!("{ENTRIES_PATH} is not a list")))?;
    if entries.is_empty() {
        return Err(ParseError::MissingField("statistics entries"));
    }
    entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let entry = entry
                .as_object()
                .ok_or_else(|| invalid(index, "not an object"))?;
            Ok(Entry {
                index,
                type_id: required_str(entry, "typeId", index)?.to_string(),
                team_id: required_str(entry, "teamId", index)?.to_string(),
                stage: entry
                    .get("eventStageId")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| invalid(index, "eventStageId must be an integer"))?,
                cell: Cell {
                    value: optional_str(entry, "value", index)?,
                    raw_value: optional_str(entry, "rawValue", index)?,
                    sub_value: optional_str(entry, "subValue", index)?,
                },
            })
        })
        .collect()
}

fn conflict(entry: &Entry) -> ParseError {
    invalid(
        entry.index,
        &format!(
            "conflicting duplicate for typeId {:?}, eventStageId {}, teamId {:?}",
            entry.type_id, entry.stage, entry.team_id
        ),
    )
}

pub fn parse_stats_json(
    payload: &str,
    home_team_id: &str,
    away_team_id: &str,
) -> Result<Vec<StatRecord>, ParseError> {
    let mut order: Vec<(i64, String)> = Vec::new();
    let mut groups: HashMap<(i64, String), Pair> = HashMap::new();
    for entry in parse_entries(payload)? {
        let key = (entry.stage, entry.type_id.clone());
        if !groups.contains_key(&key) {
            order.push(key.clone());
        }
        let pair = groups.entry(key).or_default();
        let slot = if entry.team_id == home_team_id {
            &mut pair.home
        } else if entry.team_id == away_team_id {
            &mut pair.away
        } else {
            return Err(invalid(
                entry.index,
                &format!(
                    "teamId {:?} is neither the home nor the away team id",
                    entry.team_id
                ),
            ));
        };
        match slot {
            Some(existing) if *existing != entry.cell => return Err(conflict(&entry)),
            _ => *slot = Some(entry.cell),
        }
    }

    Ok(order
        .into_iter()
        .map(|key| {
            let pair = &groups[&key];
            StatRecord {
                period: None,
                event_stage_id: Some(key.0),
                name: key.1,
                home: side_value(pair.home.as_ref()),
                away: side_value(pair.away.as_ref()),
            }
        })
        .collect())
}

/// One metric of one stage; `cells` is aligned with `TeamStats::teams` (`None`: no entry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metric {
    pub type_id: String,
    pub cells: Vec<Option<Cell>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageStats {
    pub event_stage_id: i64,
    pub metrics: Vec<Metric>,
}

/// The whole response without any home/away assumption: columns are the team ids found in the
/// response (sorted, so the order says nothing about roles), stages and metrics keep the order of
/// first appearance. Nothing is hard-coded: any number of teams, stages and metrics is accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamStats {
    /// Number of entries in the response (before identical duplicates are collapsed).
    pub entries: usize,
    pub teams: Vec<String>,
    pub stages: Vec<StageStats>,
}

pub fn parse_stats_json_by_team(payload: &str) -> Result<TeamStats, ParseError> {
    let entries = parse_entries(payload)?;
    let count = entries.len();
    let teams: Vec<String> = entries
        .iter()
        .map(|e| e.team_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let column = |team: &str| {
        teams
            .iter()
            .position(|t| t == team)
            .expect("team was collected")
    };

    let mut stages: Vec<StageStats> = Vec::new();
    for entry in entries {
        let stage = match stages.iter().position(|s| s.event_stage_id == entry.stage) {
            Some(i) => &mut stages[i],
            None => {
                stages.push(StageStats {
                    event_stage_id: entry.stage,
                    metrics: Vec::new(),
                });
                stages.last_mut().expect("just pushed")
            }
        };
        let metric = match stage
            .metrics
            .iter()
            .position(|m| m.type_id == entry.type_id)
        {
            Some(i) => &mut stage.metrics[i],
            None => {
                stage.metrics.push(Metric {
                    type_id: entry.type_id.clone(),
                    cells: vec![None; teams.len()],
                });
                stage.metrics.last_mut().expect("just pushed")
            }
        };
        let slot = &mut metric.cells[column(&entry.team_id)];
        match slot {
            Some(existing) if *existing != entry.cell => return Err(conflict(&entry)),
            _ => *slot = Some(entry.cell),
        }
    }
    Ok(TeamStats {
        entries: count,
        teams,
        stages,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::ValueKind;
    use serde_json::Number;

    const FIXTURE: &str = include_str!("../fixtures/flashscore/stats_graphql.json");

    fn parse(payload: &str) -> Result<Vec<StatRecord>, ParseError> {
        parse_stats_json(payload, "synthHome", "synthAway")
    }

    fn entries(list: &str) -> String {
        format!(r#"{{"data":{{"findEventById":{{"teamStats":{{"entries":[{list}]}}}}}}}}"#)
    }

    fn entry(type_id: &str, value: &str, stage: i64, team: &str) -> String {
        format!(
            r#"{{"typeId":"{type_id}","value":{value},"subValue":null,"rawValue":null,"eventStageId":{stage},"teamId":"{team}"}}"#
        )
    }

    fn find<'a>(r: &'a [StatRecord], stage: i64, name: &str) -> &'a StatRecord {
        r.iter()
            .find(|x| x.event_stage_id == Some(stage) && x.name == name)
            .unwrap_or_else(|| panic!("no {name} in stage {stage}"))
    }

    fn num(f: f64) -> Option<Number> {
        Number::from_f64(f)
    }

    #[test]
    fn parses_fixture_with_both_teams_and_several_stages() {
        let r = parse(FIXTURE).unwrap();
        // 8 metrics in stage 74, 2 in stage 12.
        assert_eq!(r.len(), 8 + 2);
        assert!(r.iter().all(|x| x.period.is_none()));
        let xg74 = find(&r, 74, "expected_goals");
        assert_eq!(xg74.home.value, num(0.34));
        assert_eq!(xg74.away.value, num(1.23));
        let xg12 = find(&r, 12, "expected_goals");
        assert_eq!(
            (xg12.home.value.clone(), xg12.away.value.clone()),
            (num(0.2), num(1.02))
        );
        // Same metric in two stages stays two records.
        assert_eq!(r.iter().filter(|x| x.name == "expected_goals").count(), 2);
    }

    #[test]
    fn keeps_value_raw_value_and_sub_value_apart() {
        let r = parse(FIXTURE).unwrap();
        let home = &find(&r, 74, "expected_goals").home;
        assert_eq!(
            (
                home.raw.as_deref(),
                home.raw_value.as_deref(),
                home.sub_value.as_deref()
            ),
            (Some("0.34"), Some("0.3389"), None)
        );
        let passes = &find(&r, 74, "calculated_completed_passes").home;
        assert_eq!(
            (
                passes.raw.as_deref(),
                passes.raw_value.as_deref(),
                passes.sub_value.as_deref()
            ),
            (Some("160"), Some("160.0"), Some("(80%)"))
        );
        assert_eq!(passes.kind, ValueKind::Integer);
        let away = &find(&r, 74, "calculated_completed_passes").away;
        assert_eq!(away.sub_value.as_deref(), Some("(92%)"));
    }

    #[test]
    fn value_kinds_zero_missing_and_text_stay_distinct() {
        let r = parse(FIXTURE).unwrap();
        let red = &find(&r, 74, "red_cards").home;
        assert_eq!(
            (red.kind, red.raw.as_deref(), red.value.clone()),
            (ValueKind::Integer, Some("0"), Some(Number::from(0)))
        );
        let off = find(&r, 74, "offsides");
        assert_eq!(off.home.value, Some(Number::from(2)));
        assert_eq!(
            (
                off.away.kind,
                off.away.raw.clone(),
                off.away.raw_value.clone()
            ),
            (ValueKind::Missing, None, None)
        );
        let new = find(&r, 74, "brand_new_metric");
        assert_eq!(
            (new.home.kind, new.home.raw.as_deref()),
            (ValueKind::Text, Some("n/a"))
        );
        assert_eq!(new.away.value, Some(Number::from(4)));
        assert_eq!(
            find(&r, 74, "ball_possession").home.kind,
            ValueKind::Percent
        );
        let gk = &find(&r, 74, "calculated_goalkeeper_saves").home;
        assert_eq!(
            (gk.kind, gk.numerator, gk.denominator, gk.value.clone()),
            (ValueKind::Fraction, Some(0), Some(2), None)
        );
        assert_eq!(find(&r, 74, "goals_prevented").home.value, num(-0.27));
    }

    #[test]
    fn missing_side_stays_missing() {
        // touches_in_opposition_box exists only for the away team in stage 12.
        let r = parse(FIXTURE).unwrap();
        let t = find(&r, 12, "touches_in_opposition_box");
        assert_eq!(
            (t.home.kind, t.home.raw.clone()),
            (ValueKind::Missing, None)
        );
        assert_eq!(t.away.value, Some(Number::from(12)));
    }

    #[test]
    fn sides_follow_the_given_ids_not_the_order() {
        let r = parse_stats_json(FIXTURE, "synthAway", "synthHome").unwrap();
        let xg = find(&r, 74, "expected_goals");
        assert_eq!(
            (xg.home.value.clone(), xg.away.value.clone()),
            (num(1.23), num(0.34))
        );
    }

    #[test]
    fn keeps_first_seen_order() {
        let r = parse(FIXTURE).unwrap();
        let names: Vec<_> = r.iter().take(3).map(|x| x.name.as_str()).collect();
        assert_eq!(names, ["expected_goals", "ball_possession", "red_cards"]);
    }

    #[test]
    fn duplicates_identical_collapse_conflicting_fail() {
        let same = entries(&format!(
            "{},{}",
            entry("fouls", r#""3""#, 74, "synthHome"),
            entry("fouls", r#""3""#, 74, "synthHome")
        ));
        assert_eq!(parse(&same).unwrap().len(), 1);
        let conflict = entries(&format!(
            "{},{}",
            entry("fouls", r#""3""#, 74, "synthHome"),
            entry("fouls", r#""4""#, 74, "synthHome")
        ));
        assert!(
            matches!(parse(&conflict), Err(ParseError::InvalidEntry(m)) if m.contains("conflicting duplicate"))
        );
    }

    #[test]
    fn unknown_team_is_an_error() {
        let p = entries(&entry("fouls", r#""3""#, 74, "someoneElse"));
        assert!(matches!(parse(&p), Err(ParseError::InvalidEntry(m)) if m.contains("someoneElse")));
    }

    #[test]
    fn graphql_errors_and_missing_entries() {
        let err = r#"{"errors":[{"message":"boom"},{"message":"bang"}],"data":null}"#;
        assert_eq!(
            parse(err),
            Err(ParseError::SourceErrors("boom; bang".into()))
        );
        // errors win even when data is present
        let both = format!(
            r#"{{"errors":[{{"message":"partial"}}],{}"#,
            &entries(&entry("a", r#""1""#, 1, "synthHome"))[1..]
        );
        assert!(matches!(parse(&both), Err(ParseError::SourceErrors(_))));
        // "errors": null / [] are not errors
        let ok =
            entries(&entry("a", r#""1""#, 1, "synthHome")).replacen('{', r#"{"errors":null,"#, 1);
        assert_eq!(parse(&ok).unwrap().len(), 1);
        for missing in [
            r#"{"data":null}"#,
            r#"{"data":{"findEventById":null}}"#,
            r#"{"data":{"findEventById":{"teamStats":{}}}}"#,
            r#"{"data":{"findEventById":{"teamStats":{"entries":null}}}}"#,
            r#"{}"#,
        ] {
            assert_eq!(
                parse(missing),
                Err(ParseError::MissingField(ENTRIES_PATH)),
                "{missing}"
            );
        }
        assert_eq!(
            parse(&entries("")),
            Err(ParseError::MissingField("statistics entries"))
        );
    }

    #[test]
    fn rejects_empty_corrupt_and_malformed() {
        assert_eq!(parse(""), Err(ParseError::EmptyPayload));
        assert_eq!(parse("  \n"), Err(ParseError::EmptyPayload));
        assert!(matches!(parse("{not json"), Err(ParseError::Json(_))));
        assert!(matches!(parse("[1,2]"), Err(ParseError::Json(_))));
        assert!(matches!(
            parse(r#"{"data":{"findEventById":{"teamStats":{"entries":{}}}}}"#),
            Err(ParseError::InvalidEntry(_))
        ));
        for bad in [
            "1",
            r#"{"value":"1","eventStageId":1,"teamId":"synthHome"}"#,
            r#"{"typeId":"","value":"1","eventStageId":1,"teamId":"synthHome"}"#,
            r#"{"typeId":"x","value":"1","eventStageId":"74","teamId":"synthHome"}"#,
            r#"{"typeId":"x","value":"1","eventStageId":1.5,"teamId":"synthHome"}"#,
            r#"{"typeId":"x","value":"1","eventStageId":1}"#,
            r#"{"typeId":"x","value":3,"eventStageId":1,"teamId":"synthHome"}"#,
            r#"{"typeId":"x","value":"1","rawValue":1.0,"eventStageId":1,"teamId":"synthHome"}"#,
        ] {
            assert!(
                matches!(parse(&entries(bad)), Err(ParseError::InvalidEntry(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn by_team_lists_every_team_stage_and_metric_without_roles() {
        let t = parse_stats_json_by_team(FIXTURE).unwrap();
        assert_eq!(t.entries, 19);
        // sorted ids: the order carries no home/away meaning
        assert_eq!(t.teams, ["synthAway", "synthHome"]);
        let ids: Vec<_> = t.stages.iter().map(|s| s.event_stage_id).collect();
        assert_eq!(ids, [74, 12]); // first appearance, not sorted
        assert_eq!(t.stages[0].metrics.len(), 8);
        assert_eq!(t.stages[1].metrics.len(), 2);
        let xg = &t.stages[0].metrics[0];
        assert_eq!(xg.type_id, "expected_goals");
        let (away, home) = (xg.cells[0].as_ref().unwrap(), xg.cells[1].as_ref().unwrap());
        assert_eq!(
            (away.value.as_deref(), away.raw_value.as_deref()),
            (Some("1.23"), Some("1.2322"))
        );
        assert_eq!(
            (home.value.as_deref(), home.raw_value.as_deref()),
            (Some("0.34"), Some("0.3389"))
        );
        let passes = t.stages[0]
            .metrics
            .iter()
            .find(|m| m.type_id == "calculated_completed_passes")
            .unwrap();
        assert_eq!(
            passes.cells[1].as_ref().unwrap().sub_value.as_deref(),
            Some("(80%)")
        );
        // one side has no entry -> None, a null value -> a cell without value
        let touches = t.stages[1]
            .metrics
            .iter()
            .find(|m| m.type_id == "touches_in_opposition_box")
            .unwrap();
        assert_eq!(
            (touches.cells[0].is_some(), touches.cells[1].is_none()),
            (true, true)
        );
        let offsides = t.stages[0]
            .metrics
            .iter()
            .find(|m| m.type_id == "offsides")
            .unwrap();
        assert_eq!(offsides.cells[0].as_ref().unwrap().value, None);
    }

    #[test]
    fn by_team_accepts_any_number_of_teams_and_stages() {
        let list = [
            entry("a", r#""1""#, 5, "t3"),
            entry("a", r#""2""#, 5, "t1"),
            entry("a", r#""3""#, 9, "t2"),
            entry("b", r#""4""#, 9, "t2"),
            entry("a", r#""5""#, 7, "t1"),
        ]
        .join(",");
        let t = parse_stats_json_by_team(&entries(&list)).unwrap();
        assert_eq!(t.teams, ["t1", "t2", "t3"]);
        assert_eq!(
            t.stages
                .iter()
                .map(|s| s.event_stage_id)
                .collect::<Vec<_>>(),
            [5, 9, 7]
        );
        assert_eq!(t.stages[1].metrics.len(), 2);
    }

    #[test]
    fn by_team_applies_the_same_validation() {
        assert_eq!(parse_stats_json_by_team(""), Err(ParseError::EmptyPayload));
        assert!(matches!(
            parse_stats_json_by_team("{nope"),
            Err(ParseError::Json(_))
        ));
        let err = r#"{"errors":[{"message":"boom"}],"data":null}"#;
        assert_eq!(
            parse_stats_json_by_team(err),
            Err(ParseError::SourceErrors("boom".into()))
        );
        assert_eq!(
            parse_stats_json_by_team(r#"{"data":{"findEventById":{"teamStats":{}}}}"#),
            Err(ParseError::MissingField(ENTRIES_PATH))
        );
        let same = entries(&format!(
            "{},{}",
            entry("a", r#""1""#, 1, "t1"),
            entry("a", r#""1""#, 1, "t1")
        ));
        assert_eq!(
            parse_stats_json_by_team(&same).unwrap().stages[0]
                .metrics
                .len(),
            1
        );
        let conflict = entries(&format!(
            "{},{}",
            entry("a", r#""1""#, 1, "t1"),
            entry("a", r#""2""#, 1, "t1")
        ));
        assert!(
            matches!(parse_stats_json_by_team(&conflict), Err(ParseError::InvalidEntry(m)) if m.contains("conflicting"))
        );
        assert!(matches!(
            parse_stats_json_by_team(&entries(
                r#"{"typeId":"x","value":"1","eventStageId":"1","teamId":"t"}"#
            )),
            Err(ParseError::InvalidEntry(_))
        ));
    }

    #[test]
    fn new_fields_do_not_leak_into_the_text_format_json() {
        let text = crate::stats::parse_stats_feed("SE÷Match¬~SG÷Fouls¬SH÷1¬SI÷2¬~").unwrap();
        let json = serde_json::to_value(&text[0]).unwrap();
        assert!(json.get("event_stage_id").is_none());
        assert!(json["home"].get("raw_value").is_none());
        assert!(json["home"].get("sub_value").is_none());
    }
}
