//! Kafka consumer that stores match observations in PostgreSQL.
//!
//! Offsets are committed manually, only after the row is stored (or already
//! exists). Any error stops the consumer without committing that message.

use std::{env, process::ExitCode};

use chrono::{DateTime, Utc};
use rdkafka::{
    ClientConfig, Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
};
use serde::Deserialize;
use sqlx::{PgPool, postgres::PgPoolOptions};

const DEFAULT_TOPIC: &str = "sport.match.observations.v1";
const DEFAULT_BROKERS: &str = "127.0.0.1:9092";
const SUPPORTED_SCHEMA_VERSION: u32 = 1;
const USAGE: &str =
    "usage: consumer --group-id <id> [--brokers <host:port>] [--topic <name>] [--max-messages <N>]
       DATABASE_URL must be set in the environment";

#[derive(Debug, PartialEq, Eq)]
struct Args {
    brokers: String,
    group_id: String,
    topic: String,
    max_messages: Option<u64>,
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let mut args = args.into_iter();
    let (mut brokers, mut group_id, mut topic, mut max_messages) = (None, None, None, None);
    while let Some(flag) = args.next() {
        let mut value = || {
            args.next()
                .filter(|v| !v.trim().is_empty())
                .ok_or(format!("{flag} requires a non-empty value\n{USAGE}"))
        };
        let slot = match flag.as_str() {
            "--brokers" => &mut brokers,
            "--group-id" => &mut group_id,
            "--topic" => &mut topic,
            "--max-messages" => {
                let v = value()?;
                match v.parse::<u64>() {
                    Ok(n) if n > 0 => max_messages = Some(n),
                    _ => {
                        return Err(format!(
                            "--max-messages must be a positive integer, got {v:?}"
                        ));
                    }
                }
                continue;
            }
            other => return Err(format!("unexpected argument {other}\n{USAGE}")),
        };
        *slot = Some(value()?);
    }
    Ok(Args {
        brokers: brokers.unwrap_or_else(|| DEFAULT_BROKERS.to_string()),
        group_id: group_id.ok_or(format!("--group-id is required\n{USAGE}"))?,
        topic: topic.unwrap_or_else(|| DEFAULT_TOPIC.to_string()),
        max_messages,
    })
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
struct Observation {
    schema_version: u32,
    match_id: String,
    observed_at: DateTime<Utc>,
    snapshot: Snapshot,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
struct Snapshot {
    home_team: String,
    away_team: String,
    home_score: u8,
    away_score: u8,
}

fn decode(payload: &[u8]) -> Result<Observation, String> {
    let observation: Observation =
        serde_json::from_slice(payload).map_err(|e| format!("cannot deserialize message: {e}"))?;
    if observation.schema_version != SUPPORTED_SCHEMA_VERSION {
        return Err(format!(
            "unsupported schema_version {} (supported: {SUPPORTED_SCHEMA_VERSION})",
            observation.schema_version
        ));
    }
    Ok(observation)
}

/// Returns true if a new row was inserted, false if it already existed.
async fn store(
    pool: &PgPool,
    observation: &Observation,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "INSERT INTO match_observations
            (match_id, observed_at, home_team, away_team, home_score, away_score,
             schema_version, topic, kafka_partition, kafka_offset)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING",
    )
    .bind(&observation.match_id)
    .bind(observation.observed_at)
    .bind(&observation.snapshot.home_team)
    .bind(&observation.snapshot.away_team)
    .bind(i16::from(observation.snapshot.home_score))
    .bind(i16::from(observation.snapshot.away_score))
    .bind(observation.schema_version as i32)
    .bind(topic)
    .bind(partition)
    .bind(offset)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

async fn run() -> Result<(), String> {
    let args = parse_args(env::args().skip(1))?;
    let database_url =
        env::var("DATABASE_URL").map_err(|_| format!("DATABASE_URL is not set\n{USAGE}"))?;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .map_err(|e| format!("cannot connect to PostgreSQL: {e}"))?;
    sqlx::migrate!()
        .run(&pool)
        .await
        .map_err(|e| format!("cannot run migrations: {e}"))?;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &args.brokers)
        .set("group.id", &args.group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .map_err(|e| format!("cannot create Kafka consumer: {e}"))?;
    consumer
        .subscribe(&[&args.topic])
        .map_err(|e| format!("cannot subscribe to {}: {e}", args.topic))?;
    eprintln!(
        "consuming {} from {} as group {}",
        args.topic, args.brokers, args.group_id
    );

    let mut processed = 0u64;
    while args.max_messages.is_none_or(|max| processed < max) {
        let message = consumer
            .recv()
            .await
            .map_err(|e| format!("Kafka error: {e}"))?;
        let (partition, offset) = (message.partition(), message.offset());
        let at = format!("{}[{partition}]@{offset}", message.topic());
        let payload = message
            .payload()
            .ok_or(format!("message {at} has no payload"))?;
        let observation = decode(payload).map_err(|e| format!("message {at}: {e}"))?;
        let inserted = store(&pool, &observation, message.topic(), partition, offset)
            .await
            .map_err(|e| format!("cannot store message {at}: {e}"))?;
        // Commit only after the row is stored, also if it already existed.
        consumer
            .commit_message(&message, CommitMode::Sync)
            .map_err(|e| format!("cannot commit offset of message {at}: {e}"))?;
        processed += 1;
        println!(
            "{} {at} match_id={} {}",
            if inserted { "stored" } else { "duplicate" },
            observation.match_id,
            observation.observed_at.to_rfc3339(),
        );
    }
    Ok(())
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

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    const VALID: &str = r#"{"schema_version":1,"match_id":"demo","observed_at":"2026-01-02T03:04:05Z","snapshot":{"home_team":"A","away_team":"B","home_score":2,"away_score":1}}"#;

    #[test]
    fn decodes_valid_message() {
        let o = decode(VALID.as_bytes()).unwrap();
        assert_eq!(o.match_id, "demo");
        assert_eq!(o.snapshot.home_score, 2);
        assert_eq!(o.observed_at.to_rfc3339(), "2026-01-02T03:04:05+00:00");
    }

    #[test]
    fn rejects_bad_messages() {
        assert!(decode(b"not json").is_err());
        assert!(decode(br#"{"schema_version":1}"#).is_err());
        let v2 = VALID.replace("\"schema_version\":1", "\"schema_version\":2");
        assert!(
            decode(v2.as_bytes())
                .unwrap_err()
                .contains("unsupported schema_version 2")
        );
    }

    #[test]
    fn parses_args() {
        assert_eq!(
            parse_args(args(&["--group-id", "g"])).unwrap(),
            Args {
                brokers: DEFAULT_BROKERS.into(),
                group_id: "g".into(),
                topic: DEFAULT_TOPIC.into(),
                max_messages: None
            }
        );
        let a = parse_args(args(&[
            "--group-id",
            "g",
            "--brokers",
            "b:1",
            "--topic",
            "t",
            "--max-messages",
            "3",
        ]))
        .unwrap();
        assert_eq!(
            (a.brokers.as_str(), a.topic.as_str(), a.max_messages),
            ("b:1", "t", Some(3))
        );
    }

    #[test]
    fn rejects_bad_args() {
        for bad in [
            &[][..],
            &["--brokers", "b:1"],
            &["--group-id"],
            &["--group-id", ""],
            &["--group-id", "g", "--max-messages", "0"],
            &["--group-id", "g", "--bogus"],
        ] {
            assert!(parse_args(args(bad)).is_err(), "{bad:?}");
        }
    }
}
