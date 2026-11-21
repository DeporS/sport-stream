-- Optional team statistics read together with a score observation.
-- Rows written before this migration, and messages that carry only a score,
-- keep both columns NULL.
ALTER TABLE match_observations
    -- When the collector received the statistics response (not the score response).
    ADD COLUMN statistics_observed_at TIMESTAMPTZ,
    -- Ordered list of {period, name, home, away}; repeated names are kept.
    ADD COLUMN statistics JSONB,
    ADD CONSTRAINT match_observations_statistics_together
        CHECK ((statistics IS NULL) = (statistics_observed_at IS NULL)),
    ADD CONSTRAINT match_observations_statistics_is_array
        CHECK (statistics IS NULL OR jsonb_typeof(statistics) = 'array');

-- Statistics queries: WHERE match_id = $1 AND statistics IS NOT NULL ORDER BY statistics_observed_at
CREATE INDEX match_observations_match_id_statistics_observed_at_idx
    ON match_observations (match_id, statistics_observed_at)
    WHERE statistics IS NOT NULL;
