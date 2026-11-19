# Sport Stream

A Data Engineering portfolio project in Rust. The goal is to collect football
match results, stats and odds, stream the observations through Kafka and store
their history in PostgreSQL.

This MVP parses HTML from a local file or an HTTP URL into a `MatchSnapshot` and prints it as JSON.

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

## Test

```sh
cargo test
```
