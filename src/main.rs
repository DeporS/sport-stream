mod flashscore;
mod kafka;
mod parser;
mod source;
mod stats;

use std::{env, process::ExitCode, time::Duration};

use chrono::{DateTime, Utc};
use serde::Serialize;

use kafka::{KafkaConfig, Publisher};
use parser::{MatchSnapshot, ParseError, parse_match};
use stats::StatRecord;

const USAGE: &str = "usage: sport-stream <path-to-html>
       sport-stream --url <http-url> [--poll-seconds <N> [--max-polls <N>]
                    [--kafka-brokers <host:port> --match-id <id>]]
       add --format flashscore --home-team <name> --away-team <name> to read a
       Flashscore core feed instead of HTML (default: --format html)
       sport-stream <core-feed-file> --format flashscore --home-team <name> --away-team <name>
                    --stats-file <stats-feed-file>   (local replay: score + statistics as one JSON)";

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

/// Splits off `--format`, `--home-team`, `--away-team` and `--stats-file`; the rest goes to
/// `parse_args`. The third element is the optional statistics feed file for local replay.
fn parse_cli(
    args: impl IntoIterator<Item = String>,
) -> Result<(Input, Format, Option<String>), String> {
    let mut args = args.into_iter();
    let (mut format, mut home, mut away, mut stats_file) = (None, None, None, None);
    let mut rest = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--format" => once(&mut format, &arg, text(&arg, args.next())?)?,
            "--home-team" => once(&mut home, &arg, text(&arg, args.next())?)?,
            "--away-team" => once(&mut away, &arg, text(&arg, args.next())?)?,
            "--stats-file" => once(&mut stats_file, &arg, text(&arg, args.next())?)?,
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
    if stats_file.is_some() && !(matches!(input, Input::File(_)) && format != Format::Html) {
        return Err(format!(
            "--stats-file needs a local core feed file and --format flashscore\n{USAGE}"
        ));
    }
    Ok((input, format, stats_file))
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
) -> Result<String, String> {
    let observation = Observation {
        observed_at,
        snapshot,
    };
    serde_json::to_string(&observation).map_err(|e| format!("cannot serialize: {e}"))
}

async fn poll_once(
    client: &reqwest::Client,
    publisher: Option<&Publisher>,
    format: &Format,
    url: &str,
) -> Result<String, String> {
    let (body, observed_at) = source::fetch_url(client, url).await?;
    let snapshot = format
        .parse(&body)
        .map_err(|e| format!("cannot parse {url}: {e}"))?;
    if let Some(publisher) = publisher {
        publisher.publish(&snapshot, observed_at).await?;
    }
    render_observation(&snapshot, observed_at)
}

/// Polls sequentially: the next attempt starts `interval` after the previous one finished.
/// With Kafka enabled, a line is printed only after the broker acknowledged the message.
async fn poll(
    format: &Format,
    url: &str,
    interval: Duration,
    max_polls: Option<u64>,
    kafka: Option<KafkaConfig>,
) -> Result<(), String> {
    let client = source::build_client()?;
    let publisher = kafka.as_ref().map(Publisher::new).transpose()?;
    let (mut attempts, mut successes) = (0u64, 0u64);
    loop {
        attempts += 1;
        match poll_once(&client, publisher.as_ref(), format, url).await {
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

async fn run() -> Result<(), String> {
    let (input, format, stats_file) = parse_cli(env::args().skip(1))?;
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
        } => return poll(&format, &url, interval, max_polls, kafka).await,
    };
    let snapshot = format
        .parse(&body)
        .map_err(|e| format!("cannot parse {origin}: {e}"))?;
    if let Some(stats_path) = stats_file {
        let stats_body = source::read_file(&stats_path)?;
        let statistics = stats::parse_stats_feed(&stats_body)
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
        let (input, format, stats) = parse_cli(args(&["a.html"])).unwrap();
        assert_eq!(
            (input, format, stats),
            (Input::File("a.html".into()), Format::Html, None)
        );
        let (input, format, stats) = parse_cli(args(&[
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
        assert_eq!(stats, None);
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
        let (input, _, stats) = with(&["core.txt", "--stats-file", "stats.txt"]).unwrap();
        assert_eq!(input, Input::File("core.txt".into()));
        assert_eq!(stats.as_deref(), Some("stats.txt"));
        assert!(with(&["--url", "http://x", "--stats-file", "s"]).is_err());
        assert!(with(&["core.txt", "--stats-file"]).is_err());
        assert!(with(&["core.txt", "--stats-file", "a", "--stats-file", "b"]).is_err());
        assert!(parse_cli(args(&["core.txt", "--stats-file", "s"])).is_err());
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
            render_observation(&snap, at).unwrap(),
            r#"{"observed_at":"2026-01-02T03:04:05Z","snapshot":{"home_team":"A","away_team":"B","home_score":1,"away_score":0}}"#
        );
    }
}
