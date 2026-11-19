mod parser;
mod source;

use std::{env, process::ExitCode, time::Duration};

use chrono::{DateTime, Utc};
use serde::Serialize;

use parser::{MatchSnapshot, parse_match};

const USAGE: &str = "usage: sport-stream <path-to-html>\n       sport-stream --url <http-url> [--poll-seconds <N> [--max-polls <N>]]";

#[derive(Debug, PartialEq, Eq)]
enum Input {
    File(String),
    Url(String),
    Poll {
        url: String,
        interval: Duration,
        max_polls: Option<u64>,
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

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Input, String> {
    let mut args = args.into_iter();
    let mut file = None;
    let mut url = None;
    let mut poll = None;
    let mut max_polls = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => {
                let value = args
                    .next()
                    .ok_or(format!("--url requires a value\n{USAGE}"))?;
                if url.replace(value).is_some() {
                    return Err(format!("--url given more than once\n{USAGE}"));
                }
            }
            "--poll-seconds" => {
                if poll.replace(positive(&arg, args.next())?).is_some() {
                    return Err(format!("--poll-seconds given more than once\n{USAGE}"));
                }
            }
            "--max-polls" => {
                if max_polls.replace(positive(&arg, args.next())?).is_some() {
                    return Err(format!("--max-polls given more than once\n{USAGE}"));
                }
            }
            a if a.starts_with("--") => return Err(format!("unknown option {a}\n{USAGE}")),
            _ => {
                if file.replace(arg.clone()).is_some() {
                    return Err(format!("unexpected argument {arg}\n{USAGE}"));
                }
            }
        }
    }
    match (file, url, poll, max_polls) {
        (None, None, None, None) => Err(USAGE.to_string()),
        (Some(_), Some(_), ..) => Err(format!(
            "a file path cannot be combined with --url\n{USAGE}"
        )),
        (Some(path), None, None, None) => Ok(Input::File(path)),
        (Some(_), None, ..) => Err(format!("polling options require --url\n{USAGE}")),
        (None, None, ..) => Err(format!("polling options require --url\n{USAGE}")),
        (None, Some(url), None, None) => Ok(Input::Url(url)),
        (None, Some(_), None, Some(_)) => {
            Err(format!("--max-polls requires --poll-seconds\n{USAGE}"))
        }
        (None, Some(url), Some(secs), max_polls) => Ok(Input::Poll {
            url,
            interval: Duration::from_secs(secs),
            max_polls,
        }),
    }
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

async fn poll_once(client: &reqwest::Client, url: &str) -> Result<String, String> {
    let (html, observed_at) = source::fetch_url(client, url).await?;
    let snapshot = parse_match(&html).map_err(|e| format!("cannot parse {url}: {e}"))?;
    render_observation(&snapshot, observed_at)
}

/// Polls sequentially: the next attempt starts `interval` after the previous one finished.
async fn poll(url: &str, interval: Duration, max_polls: Option<u64>) -> Result<(), String> {
    let client = source::build_client()?;
    let (mut attempts, mut successes) = (0u64, 0u64);
    loop {
        attempts += 1;
        match poll_once(&client, url).await {
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
    let (html, origin) = match parse_args(env::args().skip(1))? {
        Input::File(path) => (source::read_file(&path)?, path),
        Input::Url(url) => {
            let client = source::build_client()?;
            (source::fetch_url(&client, &url).await?.0, url)
        }
        Input::Poll {
            url,
            interval,
            max_polls,
        } => return poll(&url, interval, max_polls).await,
    };
    let snapshot = parse_match(&html).map_err(|e| format!("cannot parse {origin}: {e}"))?;
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
                max_polls: Some(3)
            })
        );
        assert_eq!(
            parse_args(args(&["--poll-seconds", "5", "--url", "http://x"])),
            Ok(Input::Poll {
                url: "http://x".into(),
                interval: Duration::from_secs(5),
                max_polls: None
            })
        );
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
