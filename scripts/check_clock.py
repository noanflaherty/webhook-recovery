#!/usr/bin/env python3
"""Verify the derived virtual clock through the API.

This is the Phase 0 check worth doing carefully: the clock is the only output
that three separate processes have to agree on, and a wrong answer is invisible
until Phase 3, where it presents as a *fairness* bug rather than a clock bug.

**Latency-aware on purpose.** The naive version of this check -- read, sleep one
second, read, assert the delta is ~20 -- only holds on localhost. Every read is
a round trip, and at a 20x multiplier a 265ms RTT is worth five virtual seconds,
so against a deployed URL the naive check fails on a clock that is perfectly
correct. Since the plan calls for running these checks against Railway, the
assertions are written against the invariant that actually holds everywhere:

    virtual elapsed  ==  wall elapsed  x  speed_multiplier

Each sample is timestamped at the *midpoint* of its request window, which bounds
the attribution error to half an RTT rather than a whole one.

    ./scripts/check_clock.py [base-url]
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request

DEFAULT_BASE = "http://localhost:8000"
SPEED = 20.0
#: Real seconds to let the clock run between samples. Long enough that the
#: measurement dominates the noise, short enough to keep the check snappy.
SAMPLE_GAP_S = 1.0
GREEN, RED, RESET = "\033[32m", "\033[31m", "\033[0m"

failures = 0


def ok(msg: str) -> None:
    print(f"   {GREEN}OK{RESET}  {msg}")


def fail(msg: str) -> None:
    global failures
    failures += 1
    print(f"   {RED}FAIL{RESET} {msg}")


def request(base: str, method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, data=data, method=method, headers={"content-type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        result: dict = json.load(resp)
        return result


def timed(base: str, method: str, path: str, body: dict | None = None) -> tuple[dict, float]:
    """Perform a request and timestamp it at the midpoint of its window."""
    start = time.monotonic()
    payload = request(base, method, path, body)
    end = time.monotonic()
    return payload, (start + end) / 2.0


def main() -> int:
    base = (sys.argv[1] if len(sys.argv) > 1 else DEFAULT_BASE).rstrip("/")

    sim = request(base, "POST", "/api/simulation", {})["id"]
    print(f"   simulation {sim}")
    path = f"/api/simulation/{sim}"

    request(base, "PATCH", path, {"speed_multiplier": SPEED})

    # --- Rate: virtual elapsed tracks wall elapsed times the multiplier ----
    first, t_first = timed(base, "GET", path)
    time.sleep(SAMPLE_GAP_S)
    second, t_second = timed(base, "GET", path)

    wall = t_second - t_first
    virtual = second["virtual_now_s"] - first["virtual_now_s"]
    expected = wall * SPEED
    # Half an RTT of attribution error at each end, plus a little slack for
    # scheduling jitter on a shared host.
    tolerance = max(2.0, 0.15 * expected)
    print(f"   {virtual:.2f} virtual s over {wall:.3f} real s (expected ~{expected:.2f})")
    if abs(virtual - expected) <= tolerance:
        ok(f"virtual time advances at {SPEED:g}x")
    else:
        fail(f"expected ~{expected:.2f} virtual s, got {virtual:.2f}")

    # --- Pause: the clock is frozen, exactly -----------------------------
    request(base, "PATCH", path, {"status": "paused"})
    paused_a = request(base, "GET", path)["virtual_now_s"]
    time.sleep(SAMPLE_GAP_S)
    paused_b = request(base, "GET", path)["virtual_now_s"]
    # Exact: a paused clock returns a stored value, not a computation.
    if paused_b == paused_a:
        ok("frozen while paused")
    else:
        fail(f"clock moved while paused: {paused_a:.3f} -> {paused_b:.3f}")

    # --- Resume: the paused interval contributes zero virtual seconds -----
    # The delta after resuming is real time since the resume, times the
    # multiplier -- NOT the pause duration. Comparing against the elapsed wall
    # time is what makes this hold over a network as well as on localhost.
    _, t_resume = timed(base, "PATCH", path, {"status": "running"})
    resumed, t_resumed = timed(base, "GET", path)

    jump = resumed["virtual_now_s"] - paused_b
    expected_jump = (t_resumed - t_resume) * SPEED
    naive_jump = SAMPLE_GAP_S * SPEED  # what a broken rebase would have added
    print(f"   resumed +{jump:.2f} virtual s (expected ~{expected_jump:.2f})")
    if abs(jump - expected_jump) <= max(2.0, 0.5 * expected_jump):
        ok("no jump across the pause")
    elif jump >= naive_jump:
        fail(f"the pause interval leaked into virtual time: +{jump:.2f} virtual s")
    else:
        fail(f"expected ~{expected_jump:.2f} virtual s since resume, got {jump:.2f}")

    return failures


if __name__ == "__main__":
    try:
        sys.exit(main())
    except urllib.error.URLError as exc:
        print(f"   {RED}FAIL{RESET} could not reach the API: {exc}")
        sys.exit(1)
