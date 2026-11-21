#!/usr/bin/env python3
"""Terminal view of what actually went through the pipeline, read from the sport-stream API.

Polls GET /matches/{id}/state (newest score) and GET /matches/{id}/statistics (newest statistics)
and redraws whenever either changes: a table `metric / home / away` per period. Periods without a
confirmed name are labelled with their eventStageId. If the newest observation came without
statistics (their request failed), the newest score is still shown and the statistics are clearly
marked as coming from an earlier reading, with the score of that reading. Ctrl+C exits.

usage: live_view.py --api http://127.0.0.1:8080 --match-id ID [--interval 1] [--updates N] [--no-clear]
  --updates N  exit after N redraws (for scripted runs); default: run until Ctrl+C
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def get(url):
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def cell(v):
    if v["raw"] is None:
        return "-"          # missing value (not a zero)
    return v["raw"] + (f" {v['sub_value']}" if v.get("sub_value") else "")


def period_label(record):
    if record.get("period") is not None:
        return str(record["period"])
    if record.get("event_stage_id") is not None:
        return f"eventStageId {record['event_stage_id']} (name not confirmed)"
    return "(no period)"


def render(match_id, number, stats, state, state_then):
    """state: newest observation; stats: newest statistics (or None); state_then: the observation
    the statistics were read with (== state when the newest observation has statistics)."""
    snap = state["snapshot"]
    lines = [
        f"sport-stream live view - match {match_id} - update #{number}",
        f"  {snap['home_team']}  {snap['home_score']} : {snap['away_score']}  {snap['away_team']}",
        f"  score read at       {state['observed_at']}",
    ]
    if stats is None:
        lines += ["  statistics          none received yet (no statistics reading stored)", ""]
        lines.append("polling the API - Ctrl+C to exit")
        return "\n".join(lines)
    lines.append(f"  statistics read at  {stats['statistics_observed_at']}")
    if stats["observed_at"] != state["observed_at"]:
        then = state_then["snapshot"]
        lines += [
            "  ! STATISTICS ARE FROM AN EARLIER READING, NOT THE NEWEST ONE.",
            f"    The newest reading (score {snap['home_score']} : {snap['away_score']}, read at {state['observed_at']})",
            "    came without statistics (the statistics request failed). The tables below were read at",
            f"    {stats['statistics_observed_at']}, when the score was {then['home_score']} : {then['away_score']}.",
        ]
    lines.append("")
    groups = {}
    for r in stats["statistics"]:
        groups.setdefault(period_label(r), []).append(r)
    for label, records in groups.items():
        rows = [(r["name"], cell(r["home"]), cell(r["away"])) for r in records]
        w0 = max(len("metric"), *(len(r[0]) for r in rows))
        w1 = max(len(snap["home_team"]), *(len(r[1]) for r in rows))
        w2 = max(len(snap["away_team"]), *(len(r[2]) for r in rows))
        lines.append(f"-- {label} --")
        lines.append(f"{'metric':<{w0}}  {snap['home_team']:>{w1}}  {snap['away_team']:>{w2}}")
        lines += [f"{n:<{w0}}  {h:>{w1}}  {a:>{w2}}" for n, h, a in rows]
        lines.append("")
    recs = stats["statistics"]
    count = lambda f: sum(1 for r in recs for side in ("home", "away") if f(r[side]))
    lines.append(
        f"via API: {len(recs)} records, event_stage_id on {sum('event_stage_id' in r for r in recs)}, "
        f"raw_value on {count(lambda v: v.get('raw_value') is not None)} values, "
        f"sub_value on {count(lambda v: v.get('sub_value') is not None)} values"
    )
    lines.append("polling the API - Ctrl+C to exit")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://127.0.0.1:8080")
    ap.add_argument("--match-id", required=True)
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--updates", type=int, default=0)
    ap.add_argument("--no-clear", action="store_true")
    ap.add_argument("--wait", type=float, default=90.0, help="give up if no reading appears in this many seconds")
    a = ap.parse_args()
    base = f"{a.api}/matches/{urllib.parse.quote(a.match_id)}"
    clear = sys.stdout.isatty() and not a.no_clear
    seen, last, started, waiting = 0, None, time.time(), False
    try:
        while True:
            try:
                # statistics first: if a newer observation arrives in between, the state is newer
                # than the statistics and is (correctly) reported as "earlier reading" this round
                stats = get(f"{base}/statistics")
                state = get(f"{base}/state")
            except OSError as e:
                stats = state = None
                if time.time() - started > a.wait:
                    print(f"error: API not reachable: {e}", file=sys.stderr)
                    return 1
            if state is None:
                if last is None and time.time() - started > a.wait:
                    print("error: no observation appeared", file=sys.stderr)
                    return 1
                if last is None and not waiting:
                    print("waiting for the first reading ...", flush=True)
                    waiting = True
            else:
                key = (state["observed_at"], stats["statistics_observed_at"] if stats else None)
                if key != last:
                    state_then = state
                    if stats is not None and stats["observed_at"] != state["observed_at"]:
                        state_then = get(f"{base}/state?at={urllib.parse.quote(stats['observed_at'])}")
                        if state_then is None:
                            print("error: score of the statistics reading not found", file=sys.stderr)
                            return 1
                    last = key
                    seen += 1
                    if clear:
                        sys.stdout.write("\033[H\033[2J")
                    else:
                        print("=" * 72)
                    print(render(a.match_id, seen, stats, state, state_then), flush=True)
                    if a.updates and seen >= a.updates:
                        return 0
            time.sleep(a.interval)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
