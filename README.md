# Sport Stream

A Data Engineering portfolio project in Rust. The goal is to collect football
match results, stats and odds, stream the observations through Kafka and store
their history in PostgreSQL.

This MVP parses HTML from a local file or an HTTP URL, can poll it, and can publish observations to Kafka into a `MatchSnapshot` and prints it as JSON.

## Run

```sh
cargo run -- fixtures/match.html
```

Example output:

```json
{
  "home_team": "Lech Poznań",
  "away_team": "Legia Warszawa",
  "home_score": 2,
  "away_score": 1
}
```

Errors (bad arguments, unreadable file, connection failure, timeout, non-2xx status, missing field, invalid score) go to
stderr and the exit code is non-zero.

### Fetch over HTTP

Serve the fixtures locally and fetch the page (one request per run, 10 s timeout):

```sh
python3 -m http.server 8000 --bind 127.0.0.1 --directory fixtures
cargo run -- --url http://127.0.0.1:8000/match.html
```

The output is identical to the file mode.

### Poll one match

```sh
cargo run -- --url http://127.0.0.1:8000/match.html --poll-seconds 2 --max-polls 3
```

The first fetch happens immediately; each next one starts N seconds after the
previous attempt finished. Every successful attempt prints one JSON line to
stdout (unchanged scores are printed too):

```json
{"observed_at":"2026-11-19T12:00:00.123456Z","snapshot":{"home_team":"Lech Poznań","away_team":"Legia Warszawa","home_score":2,"away_score":1}}
```

Failed attempts are logged to stderr and polling continues. `--max-polls`
(optional, requires `--poll-seconds`) counts all attempts; without it the
program runs until Ctrl+C. With a limit, the exit code is 0 if at least one
attempt succeeded and 1 if all failed.

### Publish observations to Kafka (optional)

Polling mode can also publish every successful observation to a local Kafka
broker (single-node KRaft, pinned image `apache/kafka:3.9.1`, published only on
`127.0.0.1:9092`). Full local scenario:

```sh
# 1. Start the broker
docker compose up -d

# 2. Create the topic explicitly (one partition; auto-creation is disabled)
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create \
  --topic sport.match.observations.v1 --partitions 1 --replication-factor 1

# 3. Serve the HTML fixture
python3 -m http.server 8000 --bind 127.0.0.1 --directory fixtures &

# 4. Collect three observations
cargo run -- --url http://127.0.0.1:8000/match.html --poll-seconds 1 --max-polls 3 \
  --kafka-brokers 127.0.0.1:9092 --match-id demo-lech-legia

# 5. Read the topic from the beginning (prints key and value)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic sport.match.observations.v1 \
  --from-beginning --max-messages 3 --property print.key=true

# 6. Clean up
kill %1
docker compose down
```

`--match-id` is required with `--kafka-brokers` and is used as the message key,
because team names alone cannot tell two meetings of the same teams apart.
Each message value looks like:

```json
{"schema_version":1,"match_id":"demo-lech-legia","observed_at":"2026-11-19T12:00:00.123456Z","snapshot":{"home_team":"Lech Poznań","away_team":"Legia Warszawa","home_score":2,"away_score":1}}
```

An attempt counts as successful only after the broker acknowledged the message
(delivery times out after 10 s); a failed publish is logged to stderr and
counted as a failed attempt. With Kafka enabled, the NDJSON line is printed to
stdout after that acknowledgement.

## Test

```sh
cargo test
```
