//! Terminal output for `sport-stream stats`: a table per stage and an optional JSON document.
//!
//! The response does not say which team is the home team or what a stage id means, so columns
//! are headed by team ids and stages by `eventStageId`.

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value, json};

use crate::stats_graphql::{Cell, TeamStats};

/// What the user sees in a table cell: `value` plus ` subValue`, `-` when there is no value.
fn cell_text(cell: Option<&Cell>) -> String {
    let Some(cell) = cell else {
        return "-".to_string();
    };
    match (&cell.value, &cell.sub_value) {
        (Some(v), Some(sub)) => format!("{v} {sub}"),
        (Some(v), None) => v.clone(),
        (None, Some(sub)) => format!("- {sub}"),
        (None, None) => "-".to_string(),
    }
}

pub fn render_table(stats: &TeamStats, fetched_at: DateTime<Utc>, status: u16) -> String {
    let mut out = vec![
        format!(
            "Fetched at {} (HTTP {status})",
            fetched_at.to_rfc3339_opts(SecondsFormat::Millis, true)
        ),
        format!(
            "{} entries, {} teams, {} stages",
            stats.entries,
            stats.teams.len(),
            stats.stages.len()
        ),
        format!(
            "Columns are team ids (home/away roles not confirmed): {}",
            stats.teams.join(", ")
        ),
        String::new(),
    ];
    for stage in &stats.stages {
        let rows: Vec<Vec<String>> = stage
            .metrics
            .iter()
            .map(|m| {
                std::iter::once(m.type_id.clone())
                    .chain(m.cells.iter().map(|c| cell_text(c.as_ref())))
                    .collect()
            })
            .collect();
        let header: Vec<String> = std::iter::once("metric".to_string())
            .chain(stats.teams.iter().cloned())
            .collect();
        let widths: Vec<usize> = (0..header.len())
            .map(|c| {
                std::iter::once(&header)
                    .chain(rows.iter())
                    .map(|r| r[c].chars().count())
                    .max()
                    .unwrap_or(0)
            })
            .collect();
        let line = |r: &Vec<String>| {
            r.iter()
                .enumerate()
                .map(|(c, text)| {
                    if c == 0 {
                        format!("{text:<w$}", w = widths[c])
                    } else {
                        format!("{text:>w$}", w = widths[c])
                    }
                })
                .collect::<Vec<_>>()
                .join("  ")
        };
        out.push(format!(
            "-- eventStageId {} (name not confirmed), {} metrics --",
            stage.event_stage_id,
            stage.metrics.len()
        ));
        out.push(line(&header));
        out.extend(rows.iter().map(&line));
        out.push(String::new());
    }
    out.join("\n")
}

/// Everything `--watch` shows besides the table itself.
pub struct WatchView<'a> {
    pub attempt: u64,
    pub interval: std::time::Duration,
    /// The last successful read: stats, time the response was received, HTTP status.
    pub last_ok: Option<(&'a TeamStats, DateTime<Utc>, u16)>,
    /// The last attempt, if it failed: when and why.
    pub failure: Option<(DateTime<Utc>, &'a str)>,
    /// Set when the refresh loop gave up (e.g. HTTP 429); no further attempts are made.
    pub stopped: Option<&'a str>,
}

fn millis(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// One screen of `--watch`: a status banner, then the table of the last successful read.
/// A failed attempt never removes the last good data; it only adds a notice.
pub fn render_watch(view: &WatchView) -> String {
    let mut out = Vec::new();
    out.push(match view.stopped {
        Some(_) => format!("Watch stopped after attempt #{}", view.attempt),
        None => format!(
            "Watching, refresh every {} s (Ctrl+C to exit) - attempt #{}",
            view.interval.as_secs_f64(),
            view.attempt
        ),
    });
    if let Some((at, error)) = view.failure {
        out.push(format!("! LAST ATTEMPT FAILED at {}: {error}", millis(at)));
        out.push(match view.last_ok {
            Some((_, ok_at, _)) => format!(
                "  Showing the data from the last successful read at {}.",
                millis(ok_at)
            ),
            None => "  No successful read yet, nothing to show.".to_string(),
        });
    }
    if let Some(reason) = view.stopped {
        out.push(format!("! STOPPED: {reason}"));
    }
    out.push(String::new());
    if let Some((stats, ok_at, status)) = view.last_ok {
        out.push(render_table(stats, ok_at, status));
    }
    out.join("\n")
}

fn cell_json(cell: Option<&Cell>) -> Value {
    match cell {
        None => Value::Null,
        Some(c) => json!({"value": c.value, "sub_value": c.sub_value, "raw_value": c.raw_value}),
    }
}

/// JSON output: everything the table shows plus `raw_value`. A team without an entry is `null`.
pub fn render_json(
    stats: &TeamStats,
    fetched_at: DateTime<Utc>,
    status: u16,
) -> Result<String, String> {
    let stages: Vec<Value> = stats
        .stages
        .iter()
        .map(|stage| {
            let metrics: Vec<Value> = stage
                .metrics
                .iter()
                .map(|m| {
                    let values: Map<String, Value> = stats
                        .teams
                        .iter()
                        .zip(&m.cells)
                        .map(|(team, cell)| (team.clone(), cell_json(cell.as_ref())))
                        .collect();
                    json!({"type_id": m.type_id, "values": values})
                })
                .collect();
            json!({"event_stage_id": stage.event_stage_id, "metrics": metrics})
        })
        .collect();
    serde_json::to_string_pretty(&json!({
        "fetched_at": fetched_at.to_rfc3339_opts(SecondsFormat::Millis, true),
        "http_status": status,
        "entries": stats.entries,
        "teams": stats.teams,
        "stages": stages,
    }))
    .map_err(|e| format!("cannot serialize: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats_graphql::parse_stats_json_by_team;

    fn sample() -> TeamStats {
        parse_stats_json_by_team(include_str!("../fixtures/flashscore/stats_graphql.json")).unwrap()
    }

    fn at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-02T03:04:05.678Z")
            .unwrap()
            .to_utc()
    }

    #[test]
    fn table_has_time_team_ids_stages_value_and_sub_value_but_no_raw_value() {
        let t = render_table(&sample(), at(), 200);
        assert!(
            t.contains("Fetched at 2026-01-02T03:04:05.678Z (HTTP 200)"),
            "{t}"
        );
        assert!(t.contains("19 entries, 2 teams, 2 stages"));
        assert!(t.contains("synthAway, synthHome"));
        assert!(t.contains("-- eventStageId 74 (name not confirmed), 8 metrics --"));
        assert!(t.contains("-- eventStageId 12 (name not confirmed), 2 metrics --"));
        // header row: team ids, no roles
        assert!(t.lines().any(|l| l.starts_with("metric")
            && l.contains("synthAway")
            && l.contains("synthHome")));
        let xg = t.lines().find(|l| l.starts_with("expected_goals")).unwrap();
        assert!(xg.contains("1.23") && xg.contains("0.34"), "{xg}");
        let passes = t
            .lines()
            .find(|l| l.starts_with("calculated_completed_passes"))
            .unwrap();
        assert!(
            passes.contains("160 (80%)") && passes.contains("318 (92%)"),
            "{passes}"
        );
        assert!(
            !t.contains("0.3389") && !t.contains("1.2322"),
            "rawValue must not be in the table"
        );
        // a null value and an absent entry are shown as "-"
        let offsides = t.lines().find(|l| l.starts_with("offsides")).unwrap();
        assert!(
            offsides.trim_end().ends_with(" 2") && offsides.contains(" - "),
            "{offsides:?}"
        );
        let touches = t
            .lines()
            .find(|l| l.starts_with("touches_in_opposition_box"))
            .unwrap();
        assert!(
            touches.contains("12") && touches.trim_end().ends_with("-"),
            "{touches:?}"
        );
        assert!(
            !t.contains("Home") || t.contains("synthHome"),
            "no role names"
        );
    }

    fn watch(
        last_ok: Option<(&TeamStats, DateTime<Utc>, u16)>,
        failure: Option<(DateTime<Utc>, &str)>,
        stopped: Option<&str>,
    ) -> String {
        render_watch(&WatchView {
            attempt: 3,
            interval: std::time::Duration::from_secs(30),
            last_ok,
            failure,
            stopped,
        })
    }

    #[test]
    fn watch_screen_after_a_good_read_has_no_failure_notice() {
        let stats = sample();
        let t = watch(Some((&stats, at(), 200)), None, None);
        assert!(
            t.starts_with("Watching, refresh every 30 s (Ctrl+C to exit) - attempt #3"),
            "{t}"
        );
        assert!(t.contains("Fetched at 2026-01-02T03:04:05.678Z (HTTP 200)"));
        assert!(!t.contains("FAILED") && !t.contains("STOPPED"));
        assert!(t.contains("expected_goals"));
    }

    #[test]
    fn watch_screen_keeps_the_last_good_data_when_the_last_attempt_failed() {
        let stats = sample();
        let later = at() + chrono::Duration::seconds(30);
        let t = watch(
            Some((&stats, at(), 200)),
            Some((later, "request to u returned HTTP status 500")),
            None,
        );
        assert!(t.contains("! LAST ATTEMPT FAILED at 2026-01-02T03:04:35.678Z: request to u returned HTTP status 500"), "{t}");
        assert!(t.contains(
            "Showing the data from the last successful read at 2026-01-02T03:04:05.678Z."
        ));
        assert!(t.contains("Fetched at 2026-01-02T03:04:05.678Z (HTTP 200)"));
        assert!(
            t.contains("expected_goals"),
            "last good table is still there"
        );
    }

    #[test]
    fn watch_screen_without_any_good_read_and_when_stopped() {
        let t = watch(None, Some((at(), "connection refused")), None);
        assert!(
            t.contains("No successful read yet") && !t.contains("expected_goals"),
            "{t}"
        );
        let stats = sample();
        let t = watch(
            Some((&stats, at(), 200)),
            Some((at(), "HTTP status 429")),
            Some("HTTP 429 Too Many Requests, not retrying"),
        );
        assert!(t.starts_with("Watch stopped after attempt #3"), "{t}");
        assert!(t.contains("! STOPPED: HTTP 429 Too Many Requests, not retrying"));
        assert!(t.contains("expected_goals"));
    }

    #[test]
    fn json_keeps_raw_value_and_marks_missing_teams_as_null() {
        let v: Value = serde_json::from_str(&render_json(&sample(), at(), 200).unwrap()).unwrap();
        assert_eq!(
            (v["http_status"].clone(), v["entries"].clone()),
            (json!(200), json!(19))
        );
        assert_eq!(v["fetched_at"], "2026-01-02T03:04:05.678Z");
        assert_eq!(v["teams"], json!(["synthAway", "synthHome"]));
        let stage74 = &v["stages"][0];
        assert_eq!(stage74["event_stage_id"], 74);
        let xg = &stage74["metrics"][0]["values"];
        assert_eq!(
            xg["synthHome"],
            json!({"value": "0.34", "sub_value": null, "raw_value": "0.3389"})
        );
        assert_eq!(xg["synthAway"]["raw_value"], "1.2322");
        let touches = &v["stages"][1]["metrics"][1];
        assert_eq!(touches["type_id"], "touches_in_opposition_box");
        assert!(touches["values"]["synthHome"].is_null());
    }
}
