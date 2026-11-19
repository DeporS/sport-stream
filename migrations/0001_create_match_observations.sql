CREATE TABLE match_observations (
    id             BIGSERIAL PRIMARY KEY,
    match_id       TEXT        NOT NULL,
    observed_at    TIMESTAMPTZ NOT NULL,
    home_team      TEXT        NOT NULL,
    away_team      TEXT        NOT NULL,
    home_score     SMALLINT    NOT NULL,
    away_score     SMALLINT    NOT NULL,
    schema_version INTEGER     NOT NULL,
    topic          TEXT        NOT NULL,
    kafka_partition INTEGER    NOT NULL,
    kafka_offset   BIGINT      NOT NULL,
    -- Idempotency for re-reading the same message from the same topic.
    CONSTRAINT match_observations_kafka_position_key
        UNIQUE (topic, kafka_partition, kafka_offset)
);

-- Match history queries: WHERE match_id = $1 ORDER BY observed_at
CREATE INDEX match_observations_match_id_observed_at_idx
    ON match_observations (match_id, observed_at);
