#!/usr/bin/env python3
"""Deterministic demo server for sport-stream.

GET /match.html returns the fixture with the score 0:0, 1:0 and 2:1 for the
first, second and third request (and 2:1 afterwards).
GET /feed/dc_1_demo replays the same three scores as a synthetic Flashscore
core feed (fixtures/flashscore/core_*.txt), with its own counter.
GET /feed/df_st_demo returns synthetic statistics (fixtures/flashscore/stats_demo_*.txt):
home xG 0.42 on the first request, 1.20 on the second, and HTTP 500 afterwards (so the
third poll has a score but no statistics).
GET /feed/dc_1_live returns a synthetic core feed whose score changes on every request:
0:0, 1:0, 2:1 and 3:1 (then 3:1 again).
GET /graphql/team-stats returns synthetic GraphQL team statistics (the shape of
data.findEventById.teamStats.entries; team ids synthHome / synthAway, stages 74 and 12) that change
on every request: three readings with different xG, shots, passes ..., and HTTP 500 from the
fourth request on. So in the live demo the fourth poll has a new score but no statistics.
GET /healthz is a readiness probe and does not advance the match.
"""
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

SCORES = ["0:0", "1:0", "2:1"]
TEMPLATE = (Path(__file__).resolve().parent.parent / "fixtures" / "match.html").read_text(encoding="utf-8")
assert ">2:1<" in TEMPLATE, "fixture must contain the placeholder score 2:1"

FEED_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "flashscore"
FEEDS = [(FEED_DIR / f"core_{s.replace(':', '-')}.txt").read_bytes() for s in SCORES]

STATS = [(FEED_DIR / f"stats_demo_{i}.txt").read_bytes() for i in (1, 2)]

LIVE_SCORES = [(0, 0), (1, 0), (2, 1), (3, 1)]
served = {"html": 0, "flashscore": 0, "stats": 0, "graphql": 0, "live": 0}

# --- synthetic GraphQL team statistics -----------------------------------------------------------
# One row per reading: xG (home, away), possession % (home), shots on goal, completed/total passes,
# goalkeeper saves, goals prevented. Made-up numbers, not taken from any real match.
READINGS = [
    dict(xg=(0.12, 0.05), poss=52, sog=(0, 0), passes=((38, 45), (30, 40)), saves=((0, 0), (0, 0)), gp=(0.00, 0.00)),
    dict(xg=(0.42, 0.31), poss=55, sog=(1, 1), passes=((120, 140), (96, 118)), saves=((0, 1), (0, 0)), gp=(-0.31, 0.00)),
    dict(xg=(1.20, 0.35), poss=58, sog=(3, 1), passes=((214, 250), (150, 190)), saves=((1, 2), (1, 1)), gp=(-0.31, 0.12)),
]


def entry(type_id, value, raw, stage, team, sub=None):
    return {"__typename": "TeamStatsEntry", "typeId": type_id, "value": value, "subValue": sub,
            "rawValue": raw, "eventStageId": stage, "teamId": team}


def pct(n, d):
    return f"({round(100 * n / d)}%)" if d else "(0%)"


def graphql_response(index):
    r = READINGS[min(index, len(READINGS) - 1)]
    entries = []
    for stage, scale in ((74, 1.0), (12, 0.45)):   # stage 12: smaller numbers, meaning unknown
        for side, team in ((0, "synthHome"), (1, "synthAway")):
            xg = round(r["xg"][side] * scale, 2)
            poss = r["poss"] if side == 0 else 100 - r["poss"]
            sog = round(r["sog"][side] * scale)
            done, total = (round(v * scale) for v in r["passes"][side])
            saved, faced = (round(v * scale) for v in r["saves"][side])
            gp = round(r["gp"][side] * scale, 2)
            entries += [
                entry("expected_goals", f"{xg:.2f}", f"{r['xg'][side] * scale:.4f}", stage, team),
                entry("ball_possession", f"{poss}%", f"{poss}.0", stage, team),
                entry("shots_on_goal", str(sog), f"{sog}.0", stage, team),
                entry("total_passes", str(total), f"{total}.0", stage, team),
                entry("calculated_completed_passes", str(done), f"{done}.0", stage, team, pct(done, total)),
                entry("calculated_goalkeeper_saves", f"{saved}/{faced}", f"{saved}.0", stage, team),
                entry("goals_prevented", f"{gp:.2f}", f"{gp:.4f}", stage, team),
                entry("red_cards", "0", "0.0", stage, team),
                # absent for the away team: a missing value, not a zero
                entry("throw_in", None if side else "9", None if side else "9.0", stage, team),
            ]
    return json.dumps({"data": {"findEventById": {
        "__typename": "TeamStatsEvent", "shouldUpdate": True,
        "teamStats": {"__typename": "TeamStats", "entries": entries,
                      "subscriptionSubjects": {"__typename": "SubscriptionSubjects",
                                               "updateTeamStatsSubscriptionSubjects": []}}}}}).encode("utf-8")


def next_index(kind):
    i = min(served[kind], len(SCORES) - 1)
    served[kind] += 1
    return i


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            body, ctype = b"ok", "text/plain"
        elif self.path == "/match.html":
            score = SCORES[next_index("html")]
            body = TEMPLATE.replace(">2:1<", f">{score}<").encode("utf-8")
            ctype = "text/html; charset=utf-8"
        elif self.path == "/feed/dc_1_demo":
            body, ctype = FEEDS[next_index("flashscore")], "text/plain; charset=utf-8"
        elif self.path == "/feed/df_st_demo":
            served["stats"] += 1
            if served["stats"] > len(STATS):
                self.send_error(500)
                return
            body, ctype = STATS[served["stats"] - 1], "text/plain; charset=utf-8"
        elif self.path == "/feed/dc_1_live":
            home, away = LIVE_SCORES[min(served["live"], len(LIVE_SCORES) - 1)]
            served["live"] += 1
            body = f"DA÷2¬DB÷13¬DD÷1700003000¬DE÷{home}¬DF÷{away}¬~".encode("utf-8")
            ctype = "text/plain; charset=utf-8"
        elif self.path == "/graphql/team-stats":
            served["graphql"] += 1
            if served["graphql"] > len(READINGS):
                self.send_error(500)
                return
            body, ctype = graphql_response(served["graphql"] - 1), "application/json"
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
