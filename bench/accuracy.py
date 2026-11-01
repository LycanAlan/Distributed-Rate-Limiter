#!/usr/bin/env python3
"""Accuracy across 3 nodes (plan Task C10).

One user sends 3x their limit (200 per 2 s, sliding_counter) for 5 windows,
round-robin over the 3 gateway nodes. In local mode each node enforces the
limit on its own, so the cluster admits about 3x. redis and hybrid share one
count in Redis and should stay at the limit.

Usage: python3 bench/accuracy.py      (BUILD_DIR as for run_cluster.sh)
"""

import asyncio
import time
import uuid

from common import cluster_down, cluster_up, paced_load

LIMIT = 200  # the "accuracy" rule in rules.bench.conf
WINDOW_S = 2.0
WINDOWS = 5
LOAD_FACTOR = 3


def admitted_per_window(admitted_at: list[float], start: float) -> list[int]:
    """Admissions in each fixed, epoch-aligned window (the windows Redis counts in)."""
    counts = [0] * WINDOWS
    for t in admitted_at:
        index = int((t - start) // WINDOW_S)
        if 0 <= index < WINDOWS:
            counts[index] += 1
    return counts


def most_in_any_window(admitted_at: list[float]) -> int:
    """Most admissions inside any WINDOW_S span, wherever it starts: the strict
    reading of "LIMIT per window", which a fixed window can break at its edges."""
    times = sorted(admitted_at)
    most = first = 0
    for last in range(len(times)):
        while times[last] - times[first] >= WINDOW_S:
            first += 1
        most = max(most, last - first + 1)
    return most


def measure(mode: str) -> tuple[list[int], int]:
    """Returns (admitted per fixed window, most admitted in any sliding window)."""
    cluster_up(mode)
    try:
        user = f"acc-{uuid.uuid4().hex[:8]}"
        admitted_at: list[float] = []

        def on_status(status: int) -> None:
            if status == 200:
                admitted_at.append(time.time())

        start = (time.time() // WINDOW_S + 1) * WINDOW_S  # the next window boundary
        time.sleep(start - time.time())
        rate = LOAD_FACTOR * LIMIT / WINDOW_S
        asyncio.run(paced_load("/accuracy", rate, WINDOWS * WINDOW_S, lambda: user, on_status))
        return admitted_per_window(admitted_at, start), most_in_any_window(admitted_at)
    finally:
        cluster_down()


def main() -> None:
    print(f"1 user, {LOAD_FACTOR}x the limit of {LIMIT} per {WINDOW_S:g} s, 3 nodes, {WINDOWS} windows\n")
    print(f"{'mode':<7} {'admitted per window':<32} {'mean':>5}  {'max in any 2 s':>14}  over the limit")
    for mode in ("local", "redis", "hybrid"):
        per_window, most = measure(mode)
        mean = sum(per_window) / len(per_window)
        over = (most - LIMIT) * 100 / LIMIT
        print(f"{mode:<7} {str(per_window):<32} {mean:>5.0f}  {most:>14}  {over:+.0f}%")


if __name__ == "__main__":
    main()
