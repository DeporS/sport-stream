mod flashscore;
mod kafka;
mod parser;
mod source;
mod stats;
mod stats_graphql;
mod stats_view;

use std::{env, process::ExitCode, time::Duration};

use chrono::{DateTime, Utc};
use serde::Serialize;

use kafka::{KafkaConfig, Publisher};
use parser::{MatchSnapshot, ParseError, parse_match};
use stats::{StatRecord, StatisticsReading};

const STATS_USAGE: &str = "usage: sport-stream stats --stats-url <url> [--json]
       Fetches the Flashscore team statistics (GraphQL JSON) once and prints them; it needs no
       match score, Docker, Kafka or local server. --json prints JSON including rawValue.
       (A local file literally named `stats` must be given as ./stats.)";

const USAGE: &str = "usage: sport-stream <path-to-html>
       sport-stream --url <http-url> [--poll-seconds <N> [--max-polls <N>]
                    [--kafka-brokers <host:port> --match-id <id>]]
       add --format flashscore --home-team <name> --away-team <name> to read a
       Flashscore core feed instead of HTML (default: --format html)
       sport-stream <core-feed-file> --format flashscore --home-team <name> --away-team <name>
                    --stats-file <stats-feed-file>   (local replay: score + statistics as one JSON)
       sport-stream <core-feed-file> --format flashscore --home-team <name> --away-team <name>
                    --stats-file <graphql-response.json> --stats-format json
                    --home-team-id <id> --away-team-id <id>   (statistics in the JSON format)
       sport-stream stats --stats-url <url> [--json]   (print real statistics once, see below)
       sport-stream --url <core-url> --poll-seconds <N> --format flashscore ...
                    --stats-url <stats-feed-url>     (also fetch statistics in every poll;
                    add --stats-format json --home-team-id <id> --away-team-id <id> for GraphQL JSON)";

/// How the fetched text is turned into a snapshot.
#[derive(Debug, PartialEq, Eq)]
enum Format {
    Html,
    Flashscore {
        home_team: String,
        away_team: String,
    },
}

impl Format {
    fn parse(&self, body: &str) -> Result<MatchSnapshot, ParseError> {
        match self {
            Self::Html => parse_match(body),
            Self::Flashscore {
                home_team,
                away_team,
            } => flashscore::parse_core_feed(body, home_team, away_team),
        }
    }
}

/// Format of the statistics feed read with `--stats-file`.
#[derive(Debug, PartialEq, Eq)]
enum StatsFormat {
    /// Historical text feed (`SE`/`SG`/`SH`/`SI`).
    Text,
    /// GraphQL JSON response; sides are chosen by explicit team ids.
    Json {
        home_team_id: String,
        away_team_id: String,
    },
}

impl StatsFormat {
    fn parse(&self, body: &str) -> Result<Vec<StatRecord>, ParseError> {
        match self {
            Self::Text => stats::parse_stats_feed(body),
            Self::Json {
                home_team_id,
                away_team_id,
            } => stats_graphql::parse_stats_json(body, home_team_id, away_team_id),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Cli {
    input: Input,
    format: Format,
    stats_format: StatsFormat,
    /// Local replay: statistics feed read from a file (file input only).
    stats_file: Option<String>,
    /// Polling: statistics feed fetched together with the score in every attempt.
    stats_url: Option<String>,
}

/// Splits off `--format`, `--home-team`, `--away-team`, `--stats-file` and `--stats-url`;
/// the rest goes to `parse_args`.
fn parse_cli(args: impl IntoIterator<Item = String>) -> Result<Cli, String> {
    let mut args = args.into_iter();
    let (mut format, mut home, mut away) = (None, None, None);
    let (mut stats_file, mut stats_url) = (None, None);
    let (mut stats_format, mut home_id, mut away_id) = (None, None, None);
    let mut rest = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--format" => once(&mut format, &arg, text(&arg, args.next())?)?,
            "--home-team" => once(&mut home, &arg, text(&arg, args.next())?)?,
            "--away-team" => once(&mut away, &arg, text(&arg, args.next())?)?,
            "--stats-file" => once(&mut stats_file, &arg, text(&arg, args.next())?)?,
            "--stats-url" => once(&mut stats_url, &arg, text(&arg, args.next())?)?,
            "--stats-format" => once(&mut stats_format, &arg, text(&arg, args.next())?)?,
            "--home-team-id" => once(&mut home_id, &arg, text(&arg, args.next())?)?,
            "--away-team-id" => once(&mut away_id, &arg, text(&arg, args.next())?)?,
            _ => rest.push(arg),
        }
    }
    let format = match (format.as_deref(), home, away) {
        (None | Some("html"), None, None) => Format::Html,
        (None | Some("html"), _, _) => {
            return Err(format!(
                "--home-team and --away-team require --format flashscore\n{USAGE}"
            ));
        }
        (Some("flashscore"), Some(home_team), Some(away_team)) => Format::Flashscore {
            home_team: home_team.trim().to_string(),
            away_team: away_team.trim().to_string(),
        },
        (Some("flashscore"), _, _) => {
            return Err(format!(
                "--format flashscore requires --home-team and --away-team\n{USAGE}"
            ));
        }
        (Some(other), _, _) => {
            return Err(format!(
                "unknown format {other:?} (expected html or flashscore)\n{USAGE}"
            ));
        }
    };
    let input = parse_args(rest)?;
    let flashscore = format != Format::Html;
    if stats_file.is_some() && stats_url.is_some() {
        return Err(format!(
            "--stats-file and --stats-url cannot be combined\n{USAGE}"
        ));
    }
    if stats_file.is_some() && !(matches!(input, Input::File(_)) && flashscore) {
        return Err(format!(
            "--stats-file needs a local core feed file and --format flashscore\n{USAGE}"
        ));
    }
    if stats_url.is_some() && !(matches!(input, Input::Poll { .. }) && flashscore) {
        return Err(format!(
            "--stats-url needs --url with --poll-seconds and --format flashscore\n{USAGE}"
        ));
    }
    let stats_format = match (stats_format.as_deref(), home_id, away_id) {
        (None | Some("text"), None, None) => StatsFormat::Text,
        (None | Some("text"), _, _) => {
            return Err(format!(
                "--home-team-id and --away-team-id require --stats-format json\n{USAGE}"
            ));
        }
        (Some("json"), Some(home), Some(away)) => {
            let (home_team_id, away_team_id) = (home.trim().to_string(), away.trim().to_string());
            if home_team_id == away_team_id {
                return Err("--home-team-id and --away-team-id must differ".to_string());
            }
            if stats_file.is_none() && stats_url.is_none() {
                return Err(format!(
                    "--stats-format json needs --stats-file (local replay) or --stats-url (polling)\n{USAGE}"
                ));
            }
            StatsFormat::Json {
                home_team_id,
                away_team_id,
            }
        }
        (Some("json"), _, _) => {
            return Err(format!(
                "--stats-format json requires --home-team-id and --away-team-id\n{USAGE}"
            ));
        }
        (Some(other), _, _) => {
            return Err(format!(
                "unknown statistics format {other:?} (expected text or json)\n{USAGE}"
            ));
        }
    };
    Ok(Cli {
        input,
        format,
        stats_format,
        stats_file,
        stats_url,
    })
}

#[derive(Debug, PartialEq, Eq)]
enum Input {
    File(String),
    Url(String),
    Poll {
        url: String,
        interval: Duration,
        max_polls: Option<u64>,
        kafka: Option<KafkaConfig>,
    },
}

#[derive(Serialize)]
struct Observation<'a> {
    observed_at: DateTime<Utc>,
    snapshot: &'a MatchSnapshot,
    // Printed only when statistics were collected (the full list goes to Kafka).
    #[serde(skip_serializing_if = "Option::is_none")]
    statistics_observed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    statistics_count: Option<usize>,
}

fn positive(flag: &str, value: Option<String>) -> Result<u64, String> {
    let value = value.ok_or(format!("{flag} requires a value\n{USAGE}"))?;
    match value.parse::<u64>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err(format!("{flag} must be a positive integer, got {value:?}")),
    }
}

fn text(flag: &str, value: Option<String>) -> Result<String, String> {
    match value {
        Some(v) if !v.trim().is_empty() => Ok(v),
        Some(_) => Err(format!("{flag} must not be empty")),
        None => Err(format!("{flag} requires a value\n{USAGE}")),
    }
}

fn once<T>(slot: &mut Option<T>, flag: &str, value: T) -> Result<(), String> {
    if slot.replace(value).is_some() {
        return Err(format!("{flag} given more than once\n{USAGE}"));
    }
    Ok(())
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Input, String> {
    let mut args = args.into_iter();
    let mut file = None;
    let mut url = None;
    let mut poll = None;
    let mut max_polls = None;
    let mut brokers = None;
    let mut match_id = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => once(&mut url, &arg, text(&arg, args.next())?)?,
            "--poll-seconds" => once(&mut poll, &arg, positive(&arg, args.next())?)?,
            "--max-polls" => once(&mut max_polls, &arg, positive(&arg, args.next())?)?,
            "--kafka-brokers" => once(&mut brokers, &arg, text(&arg, args.next())?)?,
            "--match-id" => once(&mut match_id, &arg, text(&arg, args.next())?)?,
            a if a.starts_with("--") => return Err(format!("unknown option {a}\n{USAGE}")),
            _ => {
                if file.replace(arg.clone()).is_some() {
                    return Err(format!("unexpected argument {arg}\n{USAGE}"));
                }
            }
        }
    }
    let has_poll_options =
        poll.is_some() || max_polls.is_some() || brokers.is_some() || match_id.is_some();
    match (file, url) {
        (None, None) if !has_poll_options => Err(USAGE.to_string()),
        (None, None) => Err(format!("polling options require --url\n{USAGE}")),
        (Some(_), Some(_)) => Err(format!(
            "a file path cannot be combined with --url\n{USAGE}"
        )),
        (Some(_), None) if has_poll_options => {
            Err(format!("polling options require --url\n{USAGE}"))
        }
        (Some(path), None) => Ok(Input::File(path)),
        (None, Some(url)) if !has_poll_options => Ok(Input::Url(url)),
        (None, Some(url)) => {
            let secs = poll.ok_or(format!(
                "--max-polls, --kafka-brokers and --match-id require --poll-seconds\n{USAGE}"
            ))?;
            let kafka = match (brokers, match_id) {
                (Some(brokers), Some(match_id)) => Some(KafkaConfig { brokers, match_id }),
                (None, None) => None,
                (Some(_), None) => {
                    return Err(format!("--kafka-brokers requires --match-id\n{USAGE}"));
                }
                (None, Some(_)) => {
                    return Err(format!("--match-id requires --kafka-brokers\n{USAGE}"));
                }
            };
            Ok(Input::Poll {
                url,
                interval: Duration::from_secs(secs),
                max_polls,
                kafka,
            })
        }
    }
}

/// Local replay output: the score snapshot fields plus every statistics row.
#[derive(Serialize)]
struct Report<'a> {
    #[serde(flatten)]
    snapshot: &'a MatchSnapshot,
    statistics: &'a [StatRecord],
}

fn render_report(snapshot: &MatchSnapshot, statistics: &[StatRecord]) -> Result<String, String> {
    serde_json::to_string_pretty(&Report {
        snapshot,
        statistics,
    })
    .map_err(|e| format!("cannot serialize: {e}"))
}

fn render(snapshot: &MatchSnapshot) -> Result<String, String> {
    serde_json::to_string_pretty(snapshot).map_err(|e| format!("cannot serialize: {e}"))
}

fn render_observation(
    snapshot: &MatchSnapshot,
    observed_at: DateTime<Utc>,
    statistics: Option<&StatisticsReading>,
) -> Result<String, String> {
    let observation = Observation {
        observed_at,
        snapshot,
        statistics_observed_at: statistics.map(|s| s.observed_at),
        statistics_count: statistics.map(|s| s.records.len()),
    };
    serde_json::to_string(&observation).map_err(|e| format!("cannot serialize: {e}"))
}

/// Turns a fetched statistics response into a reading; `observed_at` is the time the
/// statistics response itself was received.
fn statistics_reading(
    fetched: Result<(String, DateTime<Utc>), String>,
    url: &str,
    format: &StatsFormat,
) -> Result<StatisticsReading, String> {
    let (body, observed_at) = fetched?;
    let records = format
        .parse(&body)
        .map_err(|e| format!("cannot parse {url}: {e}"))?;
    Ok(StatisticsReading {
        observed_at,
        records,
    })
}

/// One polling attempt. The score is mandatory: if it cannot be fetched or parsed nothing
/// is published. Statistics are optional: if they fail, the score is still published
/// without them and the error is reported on stderr.
async fn poll_once(
    client: &reqwest::Client,
    publisher: Option<&Publisher>,
    format: &Format,
    url: &str,
    stats: Option<(&str, &StatsFormat)>,
) -> Result<String, String> {
    let (body, observed_at) = source::fetch_url(client, url).await?;
    let snapshot = format
        .parse(&body)
        .map_err(|e| format!("cannot parse {url}: {e}"))?;
    let statistics = match stats {
        Some((stats_url, stats_format)) => {
            let fetched = source::fetch_url(client, stats_url).await;
            match statistics_reading(fetched, stats_url, stats_format) {
                Ok(reading) => Some(reading),
                Err(e) => {
                    eprintln!("error: statistics skipped, publishing the score only: {e}");
                    None
                }
            }
        }
        None => None,
    };
    if let Some(publisher) = publisher {
        publisher
            .publish(&snapshot, observed_at, statistics.as_ref())
            .await?;
    }
    render_observation(&snapshot, observed_at, statistics.as_ref())
}

/// Polls sequentially: the next attempt starts `interval` after the previous one finished.
/// With Kafka enabled, a line is printed only after the broker acknowledged the message.
async fn poll(
    format: &Format,
    url: &str,
    stats: Option<(&str, &StatsFormat)>,
    interval: Duration,
    max_polls: Option<u64>,
    kafka: Option<KafkaConfig>,
) -> Result<(), String> {
    let client = source::build_client()?;
    let publisher = kafka.as_ref().map(Publisher::new).transpose()?;
    let (mut attempts, mut successes) = (0u64, 0u64);
    loop {
        attempts += 1;
        match poll_once(&client, publisher.as_ref(), format, url, stats).await {
            Ok(line) => {
                successes += 1;
                println!("{line}");
            }
            Err(msg) => eprintln!("error: {msg}"),
        }
        if max_polls.is_some_and(|max| attempts >= max) {
            break;
        }
        tokio::time::sleep(interval).await;
    }
    if successes == 0 {
        Err(format!("all {attempts} poll attempts failed"))
    } else {
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
struct StatsCommand {
    url: String,
    json: bool,
}

fn parse_stats_command(args: impl IntoIterator<Item = String>) -> Result<StatsCommand, String> {
    let mut args = args.into_iter();
    let (mut url, mut json) = (None, false);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--stats-url" => once(&mut url, &arg, text(&arg, args.next())?)?,
            "--json" if !json => json = true,
            "--json" => return Err(format!("--json given more than once\n{STATS_USAGE}")),
            other => return Err(format!("unexpected argument {other}\n{STATS_USAGE}")),
        }
    }
    Ok(StatsCommand {
        url: url.ok_or(format!("--stats-url is required\n{STATS_USAGE}"))?,
        json,
    })
}

/// One GET, then the shared GraphQL parser. Returns the stats, the time the response was received
/// and the HTTP status. Failed HTTP, GraphQL `errors` and an invalid payload are separate errors.
async fn fetch_team_stats(
    client: &reqwest::Client,
    url: &str,
) -> Result<(stats_graphql::TeamStats, DateTime<Utc>, u16), String> {
    let (body, fetched_at, status) = source::fetch_url_with_status(client, url).await?;
    let stats = stats_graphql::parse_stats_json_by_team(&body)
        .map_err(|e| format!("cannot use the response from {url}: {e}"))?;
    Ok((stats, fetched_at, status))
}

async fn run_stats(command: StatsCommand) -> Result<(), String> {
    let client = source::build_client()?;
    let (stats, fetched_at, status) = fetch_team_stats(&client, &command.url).await?;
    if command.json {
        println!("{}", stats_view::render_json(&stats, fetched_at, status)?);
    } else {
        print!("{}", stats_view::render_table(&stats, fetched_at, status));
    }
    Ok(())
}

async fn run() -> Result<(), String> {
    let mut argv: Vec<String> = env::args().skip(1).collect();
    if argv.first().map(String::as_str) == Some("stats") {
        argv.remove(0);
        return run_stats(parse_stats_command(argv)?).await;
    }
    let Cli {
        input,
        format,
        stats_format,
        stats_file,
        stats_url,
    } = parse_cli(argv)?;
    let (body, origin) = match input {
        Input::File(path) => (source::read_file(&path)?, path),
        Input::Url(url) => {
            let client = source::build_client()?;
            (source::fetch_url(&client, &url).await?.0, url)
        }
        Input::Poll {
            url,
            interval,
            max_polls,
            kafka,
        } => {
            return poll(
                &format,
                &url,
                stats_url.as_deref().map(|url| (url, &stats_format)),
                interval,
                max_polls,
                kafka,
            )
            .await;
        }
    };
    let snapshot = format
        .parse(&body)
        .map_err(|e| format!("cannot parse {origin}: {e}"))?;
    if let Some(stats_path) = stats_file {
        let stats_body = source::read_file(&stats_path)?;
        let statistics = stats_format
            .parse(&stats_body)
            .map_err(|e| format!("cannot parse {stats_path}: {e}"))?;
        println!("{}", render_report(&snapshot, &statistics)?);
        return Ok(());
    }
    println!("{}", render(&snapshot)?);
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

    #[test]
    fn parses_file_and_url_args() {
        assert_eq!(
            parse_args(args(&["a.html"])),
            Ok(Input::File("a.html".into()))
        );
        assert_eq!(
            parse_args(args(&["--url", "http://x"])),
            Ok(Input::Url("http://x".into()))
        );
    }

    #[test]
    fn parses_format_options() {
        let cli = parse_cli(args(&["a.html"])).unwrap();
        assert_eq!(
            cli,
            Cli {
                input: Input::File("a.html".into()),
                format: Format::Html,
                stats_format: StatsFormat::Text,
                stats_file: None,
                stats_url: None
            }
        );
        let Cli {
            input,
            format,
            stats_file,
            stats_url,
            ..
        } = parse_cli(args(&[
            "--url",
            "http://x",
            "--format",
            "flashscore",
            "--home-team",
            " A ",
            "--away-team",
            "B",
        ]))
        .unwrap();
        assert_eq!(input, Input::Url("http://x".into()));
        assert_eq!((stats_file, stats_url), (None, None));
        assert_eq!(
            format,
            Format::Flashscore {
                home_team: "A".into(),
                away_team: "B".into()
            }
        );
    }

    #[test]
    fn rejects_bad_format_options() {
        for bad in [
            &["a", "--format", "flashscore"][..],
            &["a", "--format", "flashscore", "--home-team", "A"],
            &["a", "--home-team", "A", "--away-team", "B"],
            &["a", "--format", "xml"],
            &[
                "a",
                "--format",
                "html",
                "--home-team",
                "A",
                "--away-team",
                "B",
            ],
            &["a", "--format"],
        ] {
            assert!(parse_cli(args(bad)).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn stats_file_needs_flashscore_and_a_local_file() {
        let flash = [
            "--format",
            "flashscore",
            "--home-team",
            "A",
            "--away-team",
            "B",
        ];
        let with = |extra: &[&str]| {
            let all: Vec<&str> = flash.iter().chain(extra).copied().collect();
            parse_cli(args(&all))
        };
        let cli = with(&["core.txt", "--stats-file", "stats.txt"]).unwrap();
        assert_eq!(cli.input, Input::File("core.txt".into()));
        assert_eq!(cli.stats_file.as_deref(), Some("stats.txt"));
        assert!(with(&["--url", "http://x", "--stats-file", "s"]).is_err());
        assert!(with(&["core.txt", "--stats-file"]).is_err());
        assert!(with(&["core.txt", "--stats-file", "a", "--stats-file", "b"]).is_err());
        assert!(parse_cli(args(&["core.txt", "--stats-file", "s"])).is_err());
    }

    #[test]
    fn stats_format_json_needs_ids_and_a_stats_file() {
        let flash = [
            "--format",
            "flashscore",
            "--home-team",
            "A",
            "--away-team",
            "B",
        ];
        let with = |extra: &[&str]| {
            let all: Vec<&str> = flash.iter().chain(extra).copied().collect();
            parse_cli(args(&all))
        };
        let cli = with(&[
            "core.txt",
            "--stats-file",
            "s.json",
            "--stats-format",
            "json",
            "--home-team-id",
            " h1 ",
            "--away-team-id",
            "a1",
        ])
        .unwrap();
        assert_eq!(
            cli.stats_format,
            StatsFormat::Json {
                home_team_id: "h1".into(),
                away_team_id: "a1".into()
            }
        );
        // text stays the default and may be chosen explicitly
        assert_eq!(
            with(&["core.txt", "--stats-file", "s"])
                .unwrap()
                .stats_format,
            StatsFormat::Text
        );
        assert_eq!(
            with(&["core.txt", "--stats-file", "s", "--stats-format", "text"])
                .unwrap()
                .stats_format,
            StatsFormat::Text
        );
        for bad in [
            // ids missing / one id / same ids / ids without json / unknown format
            &["core.txt", "--stats-file", "s", "--stats-format", "json"][..],
            &[
                "core.txt",
                "--stats-file",
                "s",
                "--stats-format",
                "json",
                "--home-team-id",
                "h",
            ],
            &[
                "core.txt",
                "--stats-file",
                "s",
                "--stats-format",
                "json",
                "--home-team-id",
                "x",
                "--away-team-id",
                "x",
            ],
            &[
                "core.txt",
                "--stats-file",
                "s",
                "--home-team-id",
                "h",
                "--away-team-id",
                "a",
            ],
            &["core.txt", "--stats-file", "s", "--stats-format", "xml"],
            // json needs a statistics source (--stats-file or --stats-url)
            &[
                "core.txt",
                "--stats-format",
                "json",
                "--home-team-id",
                "h",
                "--away-team-id",
                "a",
            ],
            &["core.txt", "--stats-file", "s", "--stats-format"],
        ] {
            assert!(with(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn stats_command_arguments() {
        let ok = parse_stats_command(args(&["--stats-url", "http://x/gql"])).unwrap();
        assert_eq!(
            ok,
            StatsCommand {
                url: "http://x/gql".into(),
                json: false
            }
        );
        let ok = parse_stats_command(args(&["--json", "--stats-url", "http://x/gql"])).unwrap();
        assert!(ok.json);
        for bad in [
            &[][..],
            &["--json"],
            &["--stats-url"],
            &["--stats-url", ""],
            &["--stats-url", "a", "--stats-url", "b"],
            &["--stats-url", "a", "--json", "--json"],
            &["--stats-url", "a", "--bogus"],
            &["file.txt"],
        ] {
            assert!(parse_stats_command(args(bad)).is_err(), "{bad:?}");
        }
    }

    /// Serves one canned HTTP response on a local port; `None` accepts and never answers.
    async fn serve_once(response: Option<(&'static str, String)>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/pq_graphql?_hash=dts",
            listener.local_addr().unwrap()
        );
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 2048];
            let _ = socket.read(&mut buf).await;
            match response {
                Some((status, body)) => {
                    let reply = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(reply.as_bytes()).await;
                }
                None => tokio::time::sleep(Duration::from_secs(30)).await,
            }
        });
        url
    }

    #[tokio::test]
    async fn fetches_and_parses_the_local_graphql_fixture() {
        let body = include_str!("../fixtures/flashscore/stats_graphql.json").to_string();
        let url = serve_once(Some(("200 OK", body))).await;
        let client = source::build_client().unwrap();
        let (stats, _, status) = fetch_team_stats(&client, &url).await.unwrap();
        assert_eq!(
            (status, stats.entries, stats.teams.len(), stats.stages.len()),
            (200, 19, 2, 2)
        );
    }

    #[tokio::test]
    async fn reports_http_graphql_and_payload_errors_readably() {
        let client = source::build_client().unwrap();
        let url = serve_once(Some(("503 Service Unavailable", "{}".into()))).await;
        let e = fetch_team_stats(&client, &url).await.unwrap_err();
        assert!(e.contains("returned HTTP status 503"), "{e}");

        let gql = r#"{"errors":[{"message":"PersistedQueryNotFound"}],"data":null}"#;
        let url = serve_once(Some(("200 OK", gql.into()))).await;
        let e = fetch_team_stats(&client, &url).await.unwrap_err();
        assert!(
            e.contains("the source reported errors: PersistedQueryNotFound"),
            "{e}"
        );

        let url = serve_once(Some(("200 OK", "<html>blocked</html>".into()))).await;
        let e = fetch_team_stats(&client, &url).await.unwrap_err();
        assert!(e.contains("invalid JSON"), "{e}");

        let url = serve_once(Some((
            "200 OK",
            r#"{"data":{"findEventById":null}}"#.into(),
        )))
        .await;
        let e = fetch_team_stats(&client, &url).await.unwrap_err();
        assert!(e.contains("data.findEventById.teamStats.entries"), "{e}");

        let e = fetch_team_stats(&client, "http://127.0.0.1:1/x")
            .await
            .unwrap_err();
        assert!(e.contains("failed"), "{e}");
    }

    #[tokio::test]
    async fn times_out_instead_of_hanging() {
        let url = serve_once(None).await;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(300))
            .build()
            .unwrap();
        let started = std::time::Instant::now();
        let e = fetch_team_stats(&client, &url).await.unwrap_err();
        assert!(e.contains("timed out"), "{e}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn stats_url_accepts_the_json_format() {
        let cli = parse_cli(args(&[
            "--url",
            "http://x/core",
            "--poll-seconds",
            "2",
            "--format",
            "flashscore",
            "--home-team",
            "A",
            "--away-team",
            "B",
            "--stats-url",
            "http://x/gql",
            "--stats-format",
            "json",
            "--home-team-id",
            "h1",
            "--away-team-id",
            "a1",
        ]))
        .unwrap();
        assert_eq!(cli.stats_url.as_deref(), Some("http://x/gql"));
        assert_eq!(
            cli.stats_format,
            StatsFormat::Json {
                home_team_id: "h1".into(),
                away_team_id: "a1".into()
            }
        );
    }

    #[test]
    fn json_statistics_reading_uses_the_json_parser_and_reports_errors() {
        let at = Utc::now();
        let fmt = StatsFormat::Json {
            home_team_id: "h".into(),
            away_team_id: "a".into(),
        };
        let ok = r#"{"data":{"findEventById":{"teamStats":{"entries":[{"typeId":"expected_goals","value":"0.34","subValue":null,"rawValue":"0.3389","eventStageId":74,"teamId":"h"}]}}}}"#;
        let reading = statistics_reading(Ok((ok.into(), at)), "u", &fmt).unwrap();
        assert_eq!((reading.observed_at, reading.records.len()), (at, 1));
        assert_eq!(reading.records[0].event_stage_id, Some(74));
        // The text parser rejects the same body, the JSON parser rejects text and GraphQL errors.
        assert!(statistics_reading(Ok((ok.into(), at)), "u", &StatsFormat::Text).is_err());
        let text = "SE÷Match¬~SG÷Fouls¬SH÷1¬SI÷2¬~";
        assert!(statistics_reading(Ok((text.into(), at)), "u", &fmt).is_err());
        let gql_error = r#"{"errors":[{"message":"boom"}],"data":null}"#;
        let e = statistics_reading(Ok((gql_error.into(), at)), "u", &fmt).unwrap_err();
        assert!(e.contains("boom"), "{e}");
    }

    #[test]
    fn stats_url_needs_flashscore_polling() {
        let flash = [
            "--format",
            "flashscore",
            "--home-team",
            "A",
            "--away-team",
            "B",
        ];
        let with = |extra: &[&str]| {
            let all: Vec<&str> = flash.iter().chain(extra).copied().collect();
            parse_cli(args(&all))
        };
        let cli = with(&[
            "--url",
            "http://x/core",
            "--poll-seconds",
            "2",
            "--stats-url",
            "http://x/st",
        ])
        .unwrap();
        assert_eq!(cli.stats_url.as_deref(), Some("http://x/st"));
        assert_eq!(cli.stats_file, None);
        // Kafka and --max-polls still combine with it.
        assert!(
            with(&[
                "--url",
                "http://x/core",
                "--poll-seconds",
                "2",
                "--max-polls",
                "3",
                "--kafka-brokers",
                "b:1",
                "--match-id",
                "m",
                "--stats-url",
                "http://x/st",
            ])
            .is_ok()
        );
        // Not without polling, not for a file, not combined with --stats-file, not for HTML.
        assert!(with(&["--url", "http://x/core", "--stats-url", "http://x/st"]).is_err());
        assert!(with(&["core.txt", "--stats-url", "http://x/st"]).is_err());
        assert!(
            with(&[
                "--url",
                "u",
                "--poll-seconds",
                "2",
                "--stats-url",
                "s",
                "--stats-file",
                "f"
            ])
            .is_err()
        );
        assert!(with(&["--url", "u", "--poll-seconds", "2", "--stats-url"]).is_err());
        assert!(
            parse_cli(args(&[
                "--url",
                "u",
                "--poll-seconds",
                "2",
                "--stats-url",
                "s"
            ]))
            .is_err()
        );
    }

    #[test]
    fn failed_statistics_do_not_produce_a_reading() {
        let at = Utc::now();
        assert!(
            statistics_reading(Err("boom".into()), "u", &StatsFormat::Text)
                .unwrap_err()
                .contains("boom")
        );
        for body in ["", "garbage", "SE÷Match¬~"] {
            assert!(
                statistics_reading(Ok((body.into(), at)), "u", &StatsFormat::Text).is_err(),
                "{body:?}"
            );
        }
        let ok = statistics_reading(
            Ok(("SE÷Match¬~SG÷Fouls¬SH÷1¬SI÷2¬~".into(), at)),
            "u",
            &StatsFormat::Text,
        )
        .unwrap();
        assert_eq!((ok.observed_at, ok.records.len()), (at, 1));
    }

    #[test]
    fn observation_line_mentions_statistics_only_when_present() {
        let snap = flashscore::parse_core_feed("DE÷1¬DF÷0¬", "A", "B").unwrap();
        let at = Utc::now();
        let reading = StatisticsReading {
            observed_at: at,
            records: stats::parse_stats_feed("SE÷Match¬~SG÷Fouls¬SH÷1¬SI÷2¬~").unwrap(),
        };
        let with: serde_json::Value =
            serde_json::from_str(&render_observation(&snap, at, Some(&reading)).unwrap()).unwrap();
        assert_eq!(with["statistics_count"], 1);
        assert!(with["statistics_observed_at"].is_string());
        let without: serde_json::Value =
            serde_json::from_str(&render_observation(&snap, at, None).unwrap()).unwrap();
        assert!(without.get("statistics_count").is_none());
    }

    #[test]
    fn report_has_score_and_statistics() {
        let snap = flashscore::parse_core_feed("DE÷1¬DF÷0¬", "A", "B").unwrap();
        let rows = stats::parse_stats_feed("SE÷Match¬~SG÷Fouls¬SH÷0¬SI÷¬~").unwrap();
        let json: serde_json::Value =
            serde_json::from_str(&render_report(&snap, &rows).unwrap()).unwrap();
        assert_eq!(json["home_score"], 1);
        assert_eq!(json["statistics"][0]["home"]["value"], 0);
        assert!(json["statistics"][0]["away"]["raw"].is_null());
    }

    #[test]
    fn format_dispatches_to_the_right_parser() {
        let flash = Format::Flashscore {
            home_team: "A".into(),
            away_team: "B".into(),
        };
        assert_eq!(flash.parse("DE÷3¬DF÷2¬").unwrap().home_score, 3);
        assert!(Format::Html.parse("DE÷3¬DF÷2¬").is_err());
    }

    #[test]
    fn parses_poll_args() {
        assert_eq!(
            parse_args(args(&[
                "--url",
                "http://x",
                "--poll-seconds",
                "2",
                "--max-polls",
                "3"
            ])),
            Ok(Input::Poll {
                url: "http://x".into(),
                interval: Duration::from_secs(2),
                max_polls: Some(3),
                kafka: None
            })
        );
        assert_eq!(
            parse_args(args(&["--poll-seconds", "5", "--url", "http://x"])),
            Ok(Input::Poll {
                url: "http://x".into(),
                interval: Duration::from_secs(5),
                max_polls: None,
                kafka: None
            })
        );
    }

    #[test]
    fn parses_kafka_args() {
        assert_eq!(
            parse_args(args(&[
                "--url",
                "http://x",
                "--poll-seconds",
                "1",
                "--kafka-brokers",
                "127.0.0.1:9092",
                "--match-id",
                "demo"
            ])),
            Ok(Input::Poll {
                url: "http://x".into(),
                interval: Duration::from_secs(1),
                max_polls: None,
                kafka: Some(KafkaConfig {
                    brokers: "127.0.0.1:9092".into(),
                    match_id: "demo".into()
                }),
            })
        );
    }

    #[test]
    fn rejects_bad_kafka_args() {
        let base = ["--url", "http://x", "--poll-seconds", "1"];
        for extra in [
            &["--kafka-brokers", "b:9092"][..],
            &["--match-id", "m"],
            &["--kafka-brokers", "", "--match-id", "m"],
            &["--kafka-brokers", "b:9092", "--match-id", " "],
            &["--kafka-brokers", "b:9092", "--match-id"],
        ] {
            let mut v = args(&base);
            v.extend(args(extra));
            assert!(parse_args(v).is_err(), "{extra:?}");
        }
        // Kafka options without polling, or with a file path.
        assert!(
            parse_args(args(&[
                "--url",
                "http://x",
                "--kafka-brokers",
                "b",
                "--match-id",
                "m"
            ]))
            .is_err()
        );
        assert!(parse_args(args(&["a.html", "--kafka-brokers", "b", "--match-id", "m"])).is_err());
    }

    #[test]
    fn rejects_bad_args() {
        for bad in [
            &[][..],
            &["--url"],
            &["a.html", "b.html"],
            &["--url", "http://x", "extra"],
            &["--bogus"],
            &["--url", "http://x", "--max-polls", "3"],
            &["a.html", "--poll-seconds", "2"],
            &["--poll-seconds", "2"],
            &["--url", "http://x", "--poll-seconds", "0"],
            &["--url", "http://x", "--poll-seconds", "-1"],
            &["--url", "http://x", "--poll-seconds", "abc"],
            &["--url", "http://x", "--poll-seconds"],
            &[
                "--url",
                "http://x",
                "--poll-seconds",
                "1",
                "--max-polls",
                "0",
            ],
            &[
                "--url",
                "http://x",
                "--poll-seconds",
                "1",
                "--poll-seconds",
                "2",
            ],
        ] {
            assert!(parse_args(args(bad)).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn observation_line_has_expected_shape() {
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
            render_observation(&snap, at, None).unwrap(),
            r#"{"observed_at":"2026-01-02T03:04:05Z","snapshot":{"home_team":"A","away_team":"B","home_score":1,"away_score":0}}"#
        );
    }
}
