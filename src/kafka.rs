use std::time::Duration;

use chrono::{DateTime, Utc};
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use serde::Serialize;

use crate::parser::MatchSnapshot;

pub const TOPIC: &str = "sport.match.observations.v1";
const SCHEMA_VERSION: u32 = 1;
/// Upper bound for one delivery attempt (librdkafka gives up after this).
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);
const QUEUE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq, Eq)]
pub struct KafkaConfig {
    pub brokers: String,
    pub match_id: String,
}

#[derive(Serialize)]
struct KafkaObservation<'a> {
    schema_version: u32,
    match_id: &'a str,
    observed_at: DateTime<Utc>,
    snapshot: &'a MatchSnapshot,
}

pub fn render_message(
    match_id: &str,
    snapshot: &MatchSnapshot,
    observed_at: DateTime<Utc>,
) -> Result<String, String> {
    serde_json::to_string(&KafkaObservation {
        schema_version: SCHEMA_VERSION,
        match_id,
        observed_at,
        snapshot,
    })
    .map_err(|e| format!("cannot serialize Kafka message: {e}"))
}

pub struct Publisher {
    producer: FutureProducer,
    match_id: String,
}

impl Publisher {
    pub fn new(config: &KafkaConfig) -> Result<Self, String> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("acks", "all")
            .set("allow.auto.create.topics", "false")
            .set(
                "message.timeout.ms",
                DELIVERY_TIMEOUT.as_millis().to_string(),
            )
            .create()
            .map_err(|e| format!("cannot create Kafka producer: {e}"))?;
        Ok(Self {
            producer,
            match_id: config.match_id.clone(),
        })
    }

    /// Publishes one observation and waits for the broker acknowledgement.
    pub async fn publish(
        &self,
        snapshot: &MatchSnapshot,
        observed_at: DateTime<Utc>,
    ) -> Result<(), String> {
        let payload = render_message(&self.match_id, snapshot, observed_at)?;
        let record = FutureRecord::to(TOPIC)
            .key(&self.match_id)
            .payload(&payload);
        self.producer
            .send(record, Timeout::After(QUEUE_TIMEOUT))
            .await
            .map(|_| ())
            .map_err(|(e, _)| format!("Kafka delivery to topic {TOPIC} failed: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_has_expected_shape() {
        let snap = MatchSnapshot {
            home_team: "A".into(),
            away_team: "B".into(),
            home_score: 1,
            away_score: 0,
        };
        let at = DateTime::parse_from_rfc3339("2026-01-02T03:04:05Z")
            .unwrap()
            .to_utc();
        assert_eq!(
            render_message("demo", &snap, at).unwrap(),
            r#"{"schema_version":1,"match_id":"demo","observed_at":"2026-01-02T03:04:05Z","snapshot":{"home_team":"A","away_team":"B","home_score":1,"away_score":0}}"#
        );
    }
}
