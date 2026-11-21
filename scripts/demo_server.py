#!/usr/bin/env python3
"""Deterministic demo server for sport-stream.

GET /match.html returns the fixture with the score 0:0, 1:0 and 2:1 for the
first, second and third request (and 2:1 afterwards).
GET /feed/dc_1_demo replays the same three scores as a synthetic Flashscore
core feed (fixtures/flashscore/core_*.txt), with its own counter.
GET /healthz is a readiness probe and does not advance the match.
"""
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

SCORES = ["0:0", "1:0", "2:1"]
TEMPLATE = (Path(__file__).resolve().parent.parent / "fixtures" / "match.html").read_text(encoding="utf-8")
assert ">2:1<" in TEMPLATE, "fixture must contain the placeholder score 2:1"

FEED_DIR = Path(__file__).resolve().parent.parent / "fixtures" / "flashscore"
FEEDS = [(FEED_DIR / f"core_{s.replace(':', '-')}.txt").read_bytes() for s in SCORES]

served = {"html": 0, "flashscore": 0}


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
