# Sport Stream

A Data Engineering portfolio project in Rust. The goal is to collect football
match results, stats and odds, stream the observations through Kafka and store
their history in PostgreSQL.

This MVP parses HTML from a local file or an HTTP URL, can poll it, can publish observations to Kafka, stores them in PostgreSQL with a consumer, and serves the history over a read-only HTTP API into a `MatchSnapshot` and prints it as JSON.

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

### Store the history in PostgreSQL

`compose.yaml` also starts PostgreSQL (`postgres:17.6`, published only on
`127.0.0.1:5432`, data in the named volume `sport_stream_pgdata`). Local demo
credentials: user `sport`, password `sport`, database `sport_stream`.

The `consumer` binary reads the topic and stores **one row per observation**
(identical scores are kept: every row is something the collector really saw).
It reads `DATABASE_URL` from the environment, applies the SQLx migrations in
`migrations/` on start, and runs until Ctrl+C (or `--max-messages N`).

```sh
# 1-3. Start Kafka and PostgreSQL, create the topic, serve the HTML (see above)
docker compose up -d
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create \
  --topic sport.match.observations.v1 --partitions 1 --replication-factor 1
python3 -m http.server 8000 --bind 127.0.0.1 --directory fixtures &

# 4. Produce three observations
cargo run -- --url http://127.0.0.1:8000/match.html --poll-seconds 1 --max-polls 3 \
  --kafka-brokers 127.0.0.1:9092 --match-id demo-lech-legia

# 5. Consume them into PostgreSQL
export DATABASE_URL=postgres://sport:sport@127.0.0.1:5432/sport_stream
cargo run --bin consumer -- --group-id demo --max-messages 3

# 6. Query the history of one match
docker compose exec postgres psql -U sport -d sport_stream -c \
  "SELECT observed_at, home_team, home_score, away_score, away_team, kafka_offset
   FROM match_observations WHERE match_id = 'demo-lech-legia' ORDER BY observed_at"

# 7. Clean up (-v also removes the demo database volume)
kill %1
docker compose down -v
```

Consumer options: `--group-id` (required), `--brokers` (default
`127.0.0.1:9092`), `--topic` (default `sport.match.observations.v1`) and
`--max-messages`.

Delivery semantics: auto-commit is off and messages are handled one at a time.
The offset is committed in Kafka only after the row was written (or already
existed). A deserialization error, an unsupported `schema_version` or a
database error stops the consumer with an error and the offset of that message
is not committed, so it is retried on the next start.

Deduplication uses a unique `(topic, kafka_partition, kafka_offset)` with
`ON CONFLICT DO NOTHING`. It protects against **re-reading the same message
from the same topic** (for example with a new consumer group): the row count
does not change. It does not identify the same observation across a recreated
topic, because a topic recreated from scratch can reuse the same offsets for
different messages; clear the table (or `docker compose down -v`) when you
reset the topic.

### Read the history over HTTP

The read-only `api` binary (Axum + SQLx) serves the stored observations. It
reads `DATABASE_URL` and listens on `127.0.0.1:8080` by default
(`--listen <host:port>` changes that):

```sh
export DATABASE_URL=postgres://sport:sport@127.0.0.1:5432/sport_stream
cargo run --bin api

# Observations, newest first (limit 1-200, default 50; unknown match -> [])
curl "http://127.0.0.1:8080/matches/demo-lech-legia/observations?limit=10"

# Latest known state
curl "http://127.0.0.1:8080/matches/demo-lech-legia/state"

# State as last observed at or before a given moment (404 if there is none)
curl "http://127.0.0.1:8080/matches/demo-lech-legia/state?at=2026-11-19T13:31:31Z"
```

Example response:

```json
{"match_id":"demo-lech-legia","observed_at":"2026-11-19T13:31:30.079627Z","snapshot":{"home_team":"Lech Poznań","away_team":"Legia Warszawa","home_score":1,"away_score":0}}
```

Invalid `limit` or `at` gives `400`, database problems give a generic `500`
(details are logged by the server). In URLs, encode `+` in an offset as `%2B`.
Observations with the same `observed_at` are ordered by their Kafka position
(newest message first).

`at` means *the state last observed by the collector up to that moment*, not
the true state of the match at that moment. The collector only sees the match
when it polls: if a goal was scored between two polls, the API cannot know its
exact time and returns the older score until the first poll that saw the new
one. Timestamps are stored with microsecond precision.

## One-command demo

```sh
./scripts/demo.sh            # HTML replay; `./scripts/demo.sh flashscore` for the feed replay
```

The script starts Kafka and PostgreSQL in its own Compose project
(`sport-stream-demo`), serves a deterministic HTML page (scores `0:0`, `1:0`,
`2:1` for the first three collector requests; `/healthz` does not advance the
match), runs collector → Kafka → consumer → PostgreSQL, starts the API and
checks the history (`[2:1, 1:0, 0:0]`), the latest state (`2:1`) and the state
at a moment between the second and third observation (`1:0`, computed from the
real `observed_at` values). Example output:

```
history (newest first): ['2:1', '1:0', '0:0']
latest state:           2:1
state at 2026-11-19T13:37:07.749193Z: 1:0
RESULT: OK
```

It exits non-zero on any mismatch. It always cleans up (success, error, Ctrl+C):
it stops only the processes it started and removes only the Compose project
`sport-stream-demo` with its volume. If port 8000, 8080, 9092 or 5432 is in use
it exits with a message and stops nothing.

Requirements (WSL Ubuntu): Docker with the Compose plugin (and a running
daemon, e.g. Docker Desktop with WSL integration), a Rust toolchain (`cargo`),
`python3` and `curl`. The first run downloads the Kafka and PostgreSQL images
and compiles the project.

### Flashscore core feed (offline replay)

`--format flashscore` reads a Flashscore *core feed* payload (the `dc_1_<match id>`
endpoint of the reference project
[value-bet-live-detection](https://github.com/DeporS/value-bet-live-detection))
instead of HTML. The payload is a list of `key÷value` pairs separated by `¬`.
The score comes from `DE` (home goals) and `DF` (away goals). This code
does not read team names from the payload (the reference parser does not either), so they are passed as match metadata with
`--home-team` and `--away-team` (required with this format, rejected without it).
Status fields (`DA`, `DB`, `DD`) and the statistics/odds feeds are not read yet.

A missing, empty, non-numeric or conflicting (`DE` given twice with different
values) score and an empty payload are errors; a real `0:0` is valid. The
`MatchSnapshot` JSON, the Kafka message and `observed_at` (time the collector
received the response) are unchanged. This stage never contacts Flashscore;
it replays synthetic payloads from `fixtures/flashscore/`:

```sh
cargo run -- fixtures/flashscore/core_2-1.txt --format flashscore \
  --home-team "Lech Poznań" --away-team "Legia Warszawa"

# replay over HTTP with polling (the demo server serves 0:0, 1:0, 2:1)
python3 scripts/demo_server.py 8000 &
cargo run -- --url http://127.0.0.1:8000/feed/dc_1_demo --poll-seconds 1 --max-polls 3 \
  --format flashscore --home-team "Lech Poznań" --away-team "Legia Warszawa"
kill %1

# whole pipeline (Kafka -> consumer -> PostgreSQL -> API) with the Flashscore replay
./scripts/demo.sh flashscore
```

## Test

```sh
cargo test
```
