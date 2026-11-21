//! Read-only HTTP API over the stored match observations.

use std::{env, process::ExitCode};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, postgres::PgPoolOptions};

const DEFAULT_LISTEN: &str = "127.0.0.1:8080";
const DEFAULT_LIMIT: u32 = 50;
const MAX_LIMIT: u32 = 200;
const USAGE: &str = "usage: api [--listen <host:port>]   (default 127.0.0.1:8080)
       DATABASE_URL must be set in the environment";

// Stable order everywhere: newest first; identical timestamps are ordered by
// the position of the Kafka message that carried them.

#[derive(FromRow)]
struct Row {
    match_id: String,
    observed_at: DateTime<Utc>,
    home_team: String,
    away_team: String,
    home_score: i16,
    away_score: i16,
}

#[derive(Serialize)]
struct ObservationJson {
    match_id: String,
    observed_at: DateTime<Utc>,
    snapshot: SnapshotJson,
}

#[derive(Serialize)]
struct SnapshotJson {
    home_team: String,
    away_team: String,
    home_score: i16,
    away_score: i16,
}

impl From<Row> for ObservationJson {
    fn from(r: Row) -> Self {
        Self {
            match_id: r.match_id,
            observed_at: r.observed_at,
            snapshot: SnapshotJson {
                home_team: r.home_team,
                away_team: r.away_team,
                home_score: r.home_score,
                away_score: r.away_score,
            },
        }
    }
}

#[derive(FromRow)]
struct StatisticsRow {
    match_id: String,
    observed_at: DateTime<Utc>,
    statistics_observed_at: DateTime<Utc>,
    statistics: serde_json::Value,
}

#[derive(Serialize)]
struct StatisticsJson {
    match_id: String,
    /// When the score of the same observation was read.
    observed_at: DateTime<Utc>,
    /// When the statistics response was read; this is what `at` is compared with.
    statistics_observed_at: DateTime<Utc>,
    /// Ordered `{period, name, home, away}` records exactly as the collector sent them.
    statistics: serde_json::Value,
}

impl From<StatisticsRow> for StatisticsJson {
    fn from(r: StatisticsRow) -> Self {
        Self {
            match_id: r.match_id,
            observed_at: r.observed_at,
            statistics_observed_at: r.statistics_observed_at,
            statistics: r.statistics,
        }
    }
}

enum ApiError {
    BadRequest(String),
    NotFound(String),
    Internal(sqlx::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            Self::NotFound(m) => (StatusCode::NOT_FOUND, m),
            Self::Internal(e) => {
                // Details stay on the server; the client gets a generic message.
                eprintln!("error: database query failed: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
        };
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

#[derive(Deserialize)]
struct HistoryParams {
    limit: Option<String>,
}

#[derive(Deserialize)]
struct StateParams {
    at: Option<String>,
}

fn parse_limit(raw: Option<&str>) -> Result<u32, ApiError> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_LIMIT);
    };
    match raw.parse::<u32>() {
        Ok(n) if (1..=MAX_LIMIT).contains(&n) => Ok(n),
        _ => Err(ApiError::BadRequest(format!(
            "limit must be an integer between 1 and {MAX_LIMIT}"
        ))),
    }
}

fn parse_at(raw: Option<&str>) -> Result<Option<DateTime<Utc>>, ApiError> {
    raw.map(|v| {
        DateTime::parse_from_rfc3339(v)
            .map(|t| t.to_utc())
            .map_err(|_| {
                ApiError::BadRequest(
                    "at must be an RFC 3339 timestamp, e.g. 2026-01-02T03:04:05Z \
                     (encode '+' in offsets as %2B)"
                        .to_string(),
                )
            })
    })
    .transpose()
}

async fn history(
    State(pool): State<PgPool>,
    Path(match_id): Path<String>,
    Query(params): Query<HistoryParams>,
) -> Result<Json<Vec<ObservationJson>>, ApiError> {
    let limit = parse_limit(params.limit.as_deref())?;
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT match_id, observed_at, home_team, away_team, home_score, away_score
         FROM match_observations WHERE match_id = $1
         ORDER BY observed_at DESC, topic DESC, kafka_partition DESC, kafka_offset DESC
         LIMIT $2",
    )
    .bind(&match_id)
    .bind(i64::from(limit))
    .fetch_all(&pool)
    .await
    .map_err(ApiError::Internal)?;
    Ok(Json(rows.into_iter().map(Into::into).collect()))
}

async fn state(
    State(pool): State<PgPool>,
    Path(match_id): Path<String>,
    Query(params): Query<StateParams>,
) -> Result<Json<ObservationJson>, ApiError> {
    let at = parse_at(params.at.as_deref())?;
    // A NULL bound means "no upper limit", i.e. the newest observation.
    let row: Option<Row> = sqlx::query_as(
        "SELECT match_id, observed_at, home_team, away_team, home_score, away_score
         FROM match_observations
         WHERE match_id = $1 AND ($2::timestamptz IS NULL OR observed_at <= $2)
         ORDER BY observed_at DESC, topic DESC, kafka_partition DESC, kafka_offset DESC
         LIMIT 1",
    )
    .bind(&match_id)
    .bind(at)
    .fetch_optional(&pool)
    .await
    .map_err(ApiError::Internal)?;
    row.map(|o| Json(o.into())).ok_or_else(|| {
        ApiError::NotFound(format!(
            "no observation of match {match_id} at the given time"
        ))
    })
}

async fn statistics(
    State(pool): State<PgPool>,
    Path(match_id): Path<String>,
    Query(params): Query<StateParams>,
) -> Result<Json<StatisticsJson>, ApiError> {
    let at = parse_at(params.at.as_deref())?;
    // Only observations that carry statistics count: a newer score-only row is skipped.
    // No `at` means "no upper limit" (infinity), i.e. the latest available reading. The bound
    // is written as COALESCE(..) rather than `$2 IS NULL OR ..` so that it stays an index
    // condition on (match_id, statistics_observed_at) instead of a filter over newer rows.
    let row: Option<StatisticsRow> = sqlx::query_as(
        "SELECT match_id, observed_at, statistics_observed_at, statistics
         FROM match_observations
         WHERE match_id = $1 AND statistics IS NOT NULL
           AND statistics_observed_at <= COALESCE($2::timestamptz, 'infinity'::timestamptz)
         ORDER BY statistics_observed_at DESC, topic DESC, kafka_partition DESC, kafka_offset DESC
         LIMIT 1",
    )
    .bind(&match_id)
    .bind(at)
    .fetch_optional(&pool)
    .await
    .map_err(ApiError::Internal)?;
    row.map(|o| Json(o.into())).ok_or_else(|| {
        ApiError::NotFound(format!(
            "no statistics of match {match_id} at the given time"
        ))
    })
}

fn app(pool: PgPool) -> Router {
    Router::new()
        .route("/matches/{match_id}/observations", get(history))
        .route("/matches/{match_id}/state", get(state))
        .route("/matches/{match_id}/statistics", get(statistics))
        .with_state(pool)
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<String, String> {
    let mut args = args.into_iter();
    let mut listen = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => {
                let v = args
                    .next()
                    .filter(|v| !v.trim().is_empty())
                    .ok_or(format!("--listen requires a value\n{USAGE}"))?;
                if listen.replace(v).is_some() {
                    return Err(format!("--listen given more than once\n{USAGE}"));
                }
            }
            other => return Err(format!("unexpected argument {other}\n{USAGE}")),
        }
    }
    Ok(listen.unwrap_or_else(|| DEFAULT_LISTEN.to_string()))
}

async fn run() -> Result<(), String> {
    let listen = parse_args(env::args().skip(1))?;
    let database_url =
        env::var("DATABASE_URL").map_err(|_| format!("DATABASE_URL is not set\n{USAGE}"))?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .map_err(|e| format!("cannot connect to PostgreSQL: {e}"))?;
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .map_err(|e| format!("cannot listen on {listen}: {e}"))?;
    eprintln!("listening on http://{listen}");
    axum::serve(listener, app(pool))
        .await
        .map_err(|e| format!("server error: {e}"))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("error: {msg}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_defaults_and_bounds() {
        assert_eq!(parse_limit(None).ok(), Some(50));
        assert_eq!(parse_limit(Some("1")).ok(), Some(1));
        assert_eq!(parse_limit(Some("200")).ok(), Some(200));
        for bad in ["0", "201", "-1", "abc", "", "1.5"] {
            assert!(parse_limit(Some(bad)).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn at_parses_rfc3339() {
        assert_eq!(parse_at(None).ok(), Some(None));
        let t = parse_at(Some("2026-01-02T04:04:05+01:00"))
            .ok()
            .flatten()
            .unwrap();
        assert_eq!(t.to_rfc3339(), "2026-01-02T03:04:05+00:00");
        for bad in ["yesterday", "2026-01-02", "2026-13-01T00:00:00Z", ""] {
            assert!(parse_at(Some(bad)).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn parses_args() {
        assert_eq!(parse_args([]).unwrap(), DEFAULT_LISTEN);
        assert_eq!(
            parse_args(["--listen".into(), "0.0.0.0:1".into()]).unwrap(),
            "0.0.0.0:1"
        );
        assert!(parse_args(["--listen".into()]).is_err());
        assert!(parse_args(["--x".into()]).is_err());
    }
}
