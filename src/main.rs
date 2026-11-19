mod parser;

use std::{env, fs, process::ExitCode};

use parser::{MatchSnapshot, parse_match};

fn read_input(path: &str) -> Result<String, String> {
    fs::read_to_string(path).map_err(|e| format!("cannot read {path}: {e}"))
}

fn render(snapshot: &MatchSnapshot) -> Result<String, String> {
    serde_json::to_string_pretty(snapshot).map_err(|e| format!("cannot serialize: {e}"))
}

fn run() -> Result<(), String> {
    let path = env::args()
        .nth(1)
        .ok_or("usage: sport-stream <path-to-html>")?;
    let html = read_input(&path)?;
    let snapshot = parse_match(&html).map_err(|e| format!("cannot parse {path}: {e}"))?;
    println!("{}", render(&snapshot)?);
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("error: {msg}");
            ExitCode::FAILURE
        }
    }
}
