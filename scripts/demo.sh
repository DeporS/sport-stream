#!/usr/bin/env bash
# Repeatable sport-stream demo: HTML -> collector -> Kafka -> consumer -> PostgreSQL -> API.
# Uses its own Docker Compose project (sport-stream-demo); it never touches other containers.
set -euo pipefail

cd "$(dirname "$0")/.."

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

# --- pipeline ---------------------------------------------------------------
log "collecting three observations"
"$BIN/sport-stream" --url "http://127.0.0.1:$HTML_PORT/match.html" \
  --poll-seconds 2 --max-polls 3 \
  --kafka-brokers "127.0.0.1:$KAFKA_PORT" --match-id "$MATCH_ID" >"$WORK/collector.log" 2>&1 \
  || die "collector failed"

log "consuming into PostgreSQL"
timeout 60 "$BIN/consumer" --group-id "demo-$$-$(date +%s)" --max-messages 3 >"$WORK/consumer.log" 2>&1 \
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
