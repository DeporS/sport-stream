# Sport Stream

A Data Engineering portfolio project in Rust. The goal is to collect football
match results, stats and odds, stream the observations through Kafka and store
their history in PostgreSQL.

This first MVP parses a local HTML file into a `MatchSnapshot` and prints it as JSON.

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

Errors (missing argument, unreadable file, missing field, invalid score) go to
stderr and the exit code is non-zero.

## Test

```sh
cargo test
```
