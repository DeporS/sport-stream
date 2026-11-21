#!/usr/bin/env bash
# Repeatable sport-stream demo: HTML -> collector -> Kafka -> consumer -> PostgreSQL -> API.
# Uses its own Docker Compose project (sport-stream-demo); it never touches other containers.
#
# Usage: scripts/demo.sh [html|flashscore|stats|live]   (default: html)
#   html        replays a synthetic HTML page
#   flashscore  replays a synthetic Flashscore core feed (offline, nothing is sent to Flashscore)
#   stats       flashscore replay with statistics: xG 0.42, then 1.20, then a poll whose statistics
#               request fails (score only), plus one hand-written score-only message in the
#               old format; checks GET /matches/{id}/statistics (offline, nothing is sent to Flashscore)
#   live        terminal view: a collector polls a local server (score + GraphQL JSON statistics every
#               few seconds), the data goes Kafka -> consumer -> PostgreSQL -> API and scripts/live_view.py
#               redraws the tables from the API until Ctrl+C (offline, nothing is sent to Flashscore).
#               LIVE_POLL_SECONDS (default 4) sets the interval; LIVE_UPDATES=N exits after N readings.
set -euo pipefail

cd "$(dirname "$0")/.."

MODE=${1:-html}
EXPECTED_MESSAGES=3
case "$MODE" in
  html) SOURCE_PATH=match.html; COLLECTOR_FORMAT=() ;;
  flashscore)
    SOURCE_PATH=feed/dc_1_demo
    COLLECTOR_FORMAT=(--format flashscore --home-team "Lech Poznań" --away-team "Legia Warszawa") ;;
  stats)
    SOURCE_PATH=feed/dc_1_demo
    COLLECTOR_FORMAT=(--format flashscore --home-team "Lech Poznań" --away-team "Legia Warszawa"
      --stats-url "http://127.0.0.1:8000/feed/df_st_demo")
    EXPECTED_MESSAGES=4 ;;
  live)
    SOURCE_PATH=feed/dc_1_live
    COLLECTOR_FORMAT=(--format flashscore --home-team "Lech Poznań" --away-team "Legia Warszawa"
      --stats-url "http://127.0.0.1:8000/graphql/team-stats" --stats-format json
      --home-team-id synthHome --away-team-id synthAway) ;;
  *) printf 'usage: %s [html|flashscore|stats|live]\n' "$0" >&2; exit 2 ;;
esac

PROJECT=sport-stream-demo
HTML_PORT=8000
API_PORT=8080
KAFKA_PORT=9092
PG_PORT=5432
MATCH_ID=demo-lech-legia
export DATABASE_URL="postgres://sport:sport@127.0.0.1:${PG_PORT}/sport_stream"

compose() { docker compose -p "$PROJECT" -f compose.yaml "$@"; }
log() { printf '==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

STARTED_COMPOSE=0
PIDS=()
WORK=$(mktemp -d)

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [ "$status" -ne 0 ]; then
    for f in "$WORK"/*.log; do
      [ -s "$f" ] && { printf -- '--- %s ---\n' "$(basename "$f")" >&2; tail -n 15 "$f" >&2; }
    done
  fi
  log "cleaning up"
  for pid in "${PIDS[@]:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  for pid in "${PIDS[@]:-}"; do
    [ -n "$pid" ] && wait "$pid" 2>/dev/null || true
  done
  if [ "$STARTED_COMPOSE" -eq 1 ]; then
    compose down -v >"$WORK/down.log" 2>&1 || printf 'warning: docker compose down failed, see project %s\n' "$PROJECT" >&2
  fi
  rm -rf "$WORK"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# --- requirements -----------------------------------------------------------
for tool in docker cargo python3 curl; do
  command -v "$tool" >/dev/null || die "required tool not found: $tool"
done
docker compose version >/dev/null 2>&1 || die "docker compose plugin not available"
docker info >/dev/null 2>&1 || die "Docker daemon is not reachable (is Docker Desktop running with WSL integration?)"

port_in_use() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
for port in "$HTML_PORT" "$API_PORT" "$KAFKA_PORT" "$PG_PORT"; do
  if port_in_use "$port"; then
    die "port $port is already in use; stop whatever uses it and retry (nothing was stopped)"
  fi
done

# wait_for <description> <timeout-seconds> <command...>
wait_for() {
  local what=$1 timeout=$2 i
  shift 2
  for ((i = 0; i < timeout; i++)); do
    "$@" >/dev/null 2>&1 && return 0
    sleep 1
  done
  die "timed out after ${timeout}s waiting for $what"
}

# --- build ------------------------------------------------------------------
log "building binaries"
cargo build --quiet --bins
BIN=target/debug

# --- infrastructure ---------------------------------------------------------
log "starting Kafka and PostgreSQL (project $PROJECT)"
STARTED_COMPOSE=1
compose up -d >"$WORK/compose-up.log" 2>&1 || die "docker compose up failed"
KT="compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"
wait_for "Kafka" 90 $KT --list
wait_for "PostgreSQL" 60 compose exec -T postgres pg_isready -U sport -d sport_stream
log "creating topic"
$KT --create --topic sport.match.observations.v1 --partitions 1 --replication-factor 1 >"$WORK/topic.log" 2>&1 \
  || die "cannot create topic"

# --- deterministic HTML server ----------------------------------------------
log "starting HTML server on :$HTML_PORT"
python3 scripts/demo_server.py "$HTML_PORT" >"$WORK/html-server.log" 2>&1 &
PIDS+=($!)
wait_for "HTML server" 10 curl -sf "http://127.0.0.1:$HTML_PORT/healthz"

# --- live view (live mode only) ----------------------------------------------
if [ "$MODE" = live ]; then
  POLL=${LIVE_POLL_SECONDS:-4}
  log "starting the consumer (it also creates the schema)"
  "$BIN/consumer" --group-id "demo-live-$$-$(date +%s)" >"$WORK/consumer.log" 2>&1 &
  PIDS+=($!)
  wait_for "database schema" 60 compose exec -T postgres psql -U sport -d sport_stream -Atc "SELECT 1 FROM match_observations LIMIT 1"
  log "starting the API on :$API_PORT"
  "$BIN/api" --listen "127.0.0.1:$API_PORT" >"$WORK/api.log" 2>&1 &
  PIDS+=($!)
  wait_for "API" 15 curl -sf "http://127.0.0.1:$API_PORT/matches/$MATCH_ID/observations"
  log "starting the collector: 4 readings, one every ${POLL}s (score + statistics)"
  "$BIN/sport-stream" --url "http://127.0.0.1:$HTML_PORT/$SOURCE_PATH" "${COLLECTOR_FORMAT[@]}" \
    --poll-seconds "$POLL" --max-polls 4 \
    --kafka-brokers "127.0.0.1:$KAFKA_PORT" --match-id "$MATCH_ID" >"$WORK/collector.log" 2>&1 &
  PIDS+=($!)
  # The last table stays on screen until Ctrl+C; cleanup then stops only what this script started.
  VIEW=(python3 scripts/live_view.py --api "http://127.0.0.1:$API_PORT" --match-id "$MATCH_ID")
  if [ -z "${LIVE_UPDATES:-}" ]; then
    "${VIEW[@]}"
    exit $?
  fi
  # Scripted run: stop after N redraws, then check what reached the API and what the viewer showed.
  "${VIEW[@]}" --updates "$LIVE_UPDATES" --no-clear | tee "$WORK/live-view.out"
  API="http://127.0.0.1:$API_PORT/matches/$MATCH_ID"
  LIVE_STATS=$(curl -sf "$API/statistics") || die "statistics request failed"
  LIVE_STATE=$(curl -sf "$API/state") || die "state request failed"
  LIVE_HISTORY=$(curl -sf "$API/observations") || die "history request failed"
  grep -q "statistics skipped" "$WORK/collector.log" || die "collector did not report the failed statistics request"
  echo "collector stderr: $(grep 'statistics skipped' "$WORK/collector.log" | head -1)"
  LIVE_STATS="$LIVE_STATS" LIVE_STATE="$LIVE_STATE" LIVE_HISTORY="$LIVE_HISTORY" LIVE_VIEW_OUT="$WORK/live-view.out" python3 - <<'PY'
import json, os, sys
stats, state, hist = (json.loads(os.environ[k]) for k in ("LIVE_STATS", "LIVE_STATE", "LIVE_HISTORY"))
view = open(os.environ["LIVE_VIEW_OUT"], encoding="utf-8").read()
last_screen = view.split("=" * 72)[-1]
recs = stats["statistics"]
xg = next(r for r in recs if r["name"] == "expected_goals" and r["event_stage_id"] == 74)
passes = next(r for r in recs if r["name"] == "calculated_completed_passes" and r["event_stage_id"] == 74)
throw = next(r for r in recs if r["name"] == "throw_in" and r["event_stage_id"] == 74)
scores = [f'{o["snapshot"]["home_score"]}:{o["snapshot"]["away_score"]}' for o in hist]
print("API /statistics xG (stage 74):", json.dumps({s: {k: xg[s][k] for k in ("raw", "raw_value")} for s in ("home", "away")}))
print("API /statistics completed passes home:", json.dumps({k: passes["home"][k] for k in ("raw", "raw_value", "sub_value")}))
print("API /state:", f'{state["snapshot"]["home_score"]}:{state["snapshot"]["away_score"]}', "read at", state["observed_at"],
      "| /statistics observation read at", stats["observed_at"])
checks = {
  "4 observations, score 0:0 -> 1:0 -> 2:1 -> 3:1": scores == ["3:1", "2:1", "1:0", "0:0"],
  "newest observation has no statistics: /statistics returns an earlier one": stats["observed_at"] < state["observed_at"],
  "18 records, 9 per eventStageId (74 and 12), period null": len(recs) == 18 and {r["event_stage_id"] for r in recs} == {74, 12} and all(r["period"] is None for r in recs),
  "raw_value reached the API (last xG 1.20 / 1.2000)": xg["home"]["raw"] == "1.20" and xg["home"]["raw_value"] == "1.2000",
  "sub_value reached the API": passes["home"]["sub_value"] == "(86%)",
  "missing value stays missing (throw_in away)": throw["away"]["raw"] is None and throw["away"]["kind"] == "missing",
  "viewer: last screen shows the NEWEST score 3 : 1": "Lech Poznań  3 : 1  Legia Warszawa" in last_screen,
  "viewer: last screen marks the statistics as an earlier reading (score then 2 : 1)": "STATISTICS ARE FROM AN EARLIER READING" in last_screen and "when the score was 2 : 1" in last_screen,
  "viewer: earlier screens carry no such mark": view.count("STATISTICS ARE FROM AN EARLIER READING") == 1,
}
for name, ok in checks.items():
    print(("ok   " if ok else "FAIL ") + name)
print("RESULT:", "OK" if all(checks.values()) else "MISMATCH")
sys.exit(0 if all(checks.values()) else 1)
PY
  exit $?
fi

# --- pipeline ---------------------------------------------------------------
log "collecting three observations (mode: $MODE)"
"$BIN/sport-stream" --url "http://127.0.0.1:$HTML_PORT/$SOURCE_PATH" "${COLLECTOR_FORMAT[@]}" \
  --poll-seconds 2 --max-polls 3 \
  --kafka-brokers "127.0.0.1:$KAFKA_PORT" --match-id "$MATCH_ID" >"$WORK/collector.log" 2>&1 \
  || die "collector failed"

if [ "$MODE" = stats ]; then
  log "publishing one score-only message in the old format (another match)"
  LEGACY='{"schema_version":1,"match_id":"demo-legacy","observed_at":"2026-01-02T03:04:05Z","snapshot":{"home_team":"Old Home","away_team":"Old Away","home_score":3,"away_score":2}}'
  printf '%s\n' "$LEGACY" | compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic sport.match.observations.v1 >"$WORK/legacy-producer.log" 2>&1 \
    || die "cannot publish the old-format message"
fi

log "consuming into PostgreSQL"
timeout 60 "$BIN/consumer" --group-id "demo-$$-$(date +%s)" --max-messages "$EXPECTED_MESSAGES" >"$WORK/consumer.log" 2>&1 \
  || die "consumer failed"

log "starting API on :$API_PORT"
"$BIN/api" --listen "127.0.0.1:$API_PORT" >"$WORK/api.log" 2>&1 &
PIDS+=($!)
API="http://127.0.0.1:$API_PORT/matches/$MATCH_ID"
wait_for "API" 15 curl -sf "$API/observations"

# --- verification -----------------------------------------------------------
HISTORY=$(curl -sf "$API/observations") || die "history request failed"

# Moment between the 2nd and 3rd observation, from the real observed_at values.
AT=$(HISTORY="$HISTORY" python3 - <<'PY'
import json, os
from datetime import datetime
obs = json.loads(os.environ["HISTORY"])          # newest first
t = [datetime.fromisoformat(o["observed_at"].replace("Z", "+00:00")) for o in obs]
mid = t[1] + (t[0] - t[1]) / 2                   # between 2nd (t[1]) and 3rd (t[0])
print(mid.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
PY
) || die "cannot compute the intermediate time"

LATEST=$(curl -sf "$API/state") || die "latest state request failed"
PAST=$(curl -sf "$API/state?at=$AT") || die "historical state request failed"

HISTORY="$HISTORY" LATEST="$LATEST" PAST="$PAST" AT="$AT" python3 - <<'PY'
import json, os, sys
score = lambda o: f'{o["snapshot"]["home_score"]}:{o["snapshot"]["away_score"]}'
history = [score(o) for o in json.loads(os.environ["HISTORY"])]
latest = score(json.loads(os.environ["LATEST"]))
past = score(json.loads(os.environ["PAST"]))
print(f"history (newest first): {history}")
print(f"latest state:           {latest}")
print(f"state at {os.environ['AT']}: {past}")
ok = history == ["2:1", "1:0", "0:0"] and latest == "2:1" and past == "1:0"
print("RESULT:", "OK" if ok else "MISMATCH")
sys.exit(0 if ok else 1)
PY

# --- statistics (stats mode only) --------------------------------------------
[ "$MODE" = stats ] || exit 0

log "checking statistics"
# The statistics reading times as stored (the collector's observed_at of the statistics responses).
STAT_TIMES=$(compose exec -T postgres psql -U sport -d sport_stream -Atc \
  "SELECT to_char(statistics_observed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')
     FROM match_observations
    WHERE match_id = '$MATCH_ID' AND statistics IS NOT NULL ORDER BY statistics_observed_at") \
  || die "cannot read statistics times from PostgreSQL"
read -r T1 T2 EXTRA <<<"$(printf '%s' "$STAT_TIMES" | tr '\n' ' ')"
[ -n "${T1:-}" ] && [ -n "${T2:-}" ] && [ -z "${EXTRA:-}" ] || die "expected exactly two statistics readings, got: $STAT_TIMES"

read -r MID BEFORE <<<"$(T1="$T1" T2="$T2" python3 - <<'PY'
import os
from datetime import datetime, timedelta
p = lambda v: datetime.fromisoformat(v.replace("Z", "+00:00"))
f = lambda d: d.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
t1, t2 = p(os.environ["T1"]), p(os.environ["T2"])
print(f(t1 + (t2 - t1) / 2), f(t1 - timedelta(seconds=1)))
PY
)"

STATS="$API/statistics"
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
LATEST_STATS=$(curl -sf "$STATS") || die "latest statistics request failed"
MID_STATS=$(curl -sf "$STATS?at=$MID") || die "statistics between the readings failed"
AT_T1_STATS=$(curl -sf "$STATS?at=$T1") || die "statistics at the first reading failed"
BEFORE_CODE=$(code "$STATS?at=$BEFORE")
BAD_CODE=$(code "$STATS?at=yesterday")
NO_SUCH_CODE=$(code "http://127.0.0.1:$API_PORT/matches/no-such-match/statistics")
LEGACY_STATE=$(curl -sf "http://127.0.0.1:$API_PORT/matches/demo-legacy/state") || die "old-format match not stored"
LEGACY_STATS_CODE=$(code "http://127.0.0.1:$API_PORT/matches/demo-legacy/statistics")

grep -q "statistics skipped" "$WORK/collector.log" || die "collector did not report the failed statistics request"
echo "collector stderr:   $(grep 'statistics skipped' "$WORK/collector.log" | head -1)"
echo "at=$BEFORE:  HTTP $BEFORE_CODE $(curl -s "$STATS?at=$BEFORE")"
echo "at=yesterday:       HTTP $BAD_CODE $(curl -s "$STATS?at=yesterday")"
echo "xG at $MID: $(printf '%s' "$MID_STATS" | python3 -c 'import json,sys; r=[r for r in json.load(sys.stdin)["statistics"] if r["name"]=="Expected goals (xG)"][0]; print(r["home"]["raw"], "(home)")')"
echo "latest statistics:  $(printf '%s' "$LATEST_STATS" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(json.dumps({k: d[k] for k in ("match_id","observed_at","statistics_observed_at")}), "records:", len(d["statistics"]))')"
echo "latest xG record:   $(printf '%s' "$LATEST_STATS" | python3 -c 'import json,sys; print(json.dumps([r for r in json.load(sys.stdin)["statistics"] if r["name"]=="Expected goals (xG)"][0]))')"

HISTORY="$HISTORY" LATEST="$LATEST" LATEST_STATS="$LATEST_STATS" MID_STATS="$MID_STATS" \
AT_T1_STATS="$AT_T1_STATS" LEGACY_STATE="$LEGACY_STATE" T1="$T1" T2="$T2" MID="$MID" BEFORE="$BEFORE" \
BEFORE_CODE="$BEFORE_CODE" BAD_CODE="$BAD_CODE" NO_SUCH_CODE="$NO_SUCH_CODE" LEGACY_STATS_CODE="$LEGACY_STATS_CODE" \
python3 - <<'PY'
import json, os, sys
e = os.environ
load = lambda k: json.loads(e[k])
xg = lambda d, period="Match": next(r for r in d["statistics"] if r["name"] == "Expected goals (xG)" and r["period"] == period)
rec = lambda d, name: next(r for r in d["statistics"] if r["name"] == name)
latest, mid, at_t1 = load("LATEST_STATS"), load("MID_STATS"), load("AT_T1_STATS")
history = load("HISTORY")          # newest first: 2:1 (no statistics), 1:0, 0:0
state = load("LATEST")
legacy = load("LEGACY_STATE")
sc = lambda o: f'{o["snapshot"]["home_score"]}:{o["snapshot"]["away_score"]}'

checks = {
  "latest statistics xG home is 1.20 (raw '1.20')": xg(latest)["home"]["value"] == 1.2 and xg(latest)["home"]["raw"] == "1.20",
  "latest statistics time is the second reading": latest["statistics_observed_at"].rstrip("Z")[:26] == e["T2"].rstrip("Z")[:26],
  f"at={e['MID']} (between readings) gives 0.42": xg(mid)["home"]["value"] == 0.42,
  "at=<first reading> gives 0.42 (inclusive)": xg(at_t1)["home"]["value"] == 0.42,
  f"at={e['BEFORE']} (before the first reading) is 404": e["BEFORE_CODE"] == "404",
  "at=yesterday is 400": e["BAD_CODE"] == "400",
  "unknown match is 404": e["NO_SUCH_CODE"] == "404",
  "newest observation is score-only (2:1) yet statistics stay available": sc(state) == "2:1" and latest["observed_at"] == history[1]["observed_at"] and latest["observed_at"] < history[0]["observed_at"],
  "periods are kept (xG in Match and 1st Half)": xg(latest, "1st Half")["home"]["value"] == 1.2,
  "ordered list with all periods": [r["period"] for r in latest["statistics"]] == ["Match"] * 6 + ["1st Half"],
  "missing / zero / n/a stay distinct": (rec(latest, "Offsides")["away"]["raw"] is None and rec(latest, "Offsides")["away"]["kind"] == "missing"
      and rec(latest, "Red cards")["home"]["raw"] == "0" and rec(latest, "Red cards")["home"]["value"] == 0
      and rec(latest, "Dribbles")["home"]["raw"] == "n/a" and rec(latest, "Dribbles")["home"]["kind"] == "text"),
  "75% (18/24) keeps percent, numerator, denominator": (lambda v: (v["value"], v["numerator"], v["denominator"], v["raw"]))(rec(latest, "Crosses")["home"]) == (75, 18, 24, "75% (18/24)"),
  "old-format message stored (3:2) and has no statistics": sc(legacy) == "3:2" and e["LEGACY_STATS_CODE"] == "404",
}
for name, ok in checks.items():
    print(("ok   " if ok else "FAIL ") + name)
print("RESULT:", "OK" if all(checks.values()) else "MISMATCH")
sys.exit(0 if all(checks.values()) else 1)
PY
