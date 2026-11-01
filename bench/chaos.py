#!/usr/bin/env python3
"""A Redis shard dies under load (plan Task C10).

redis mode, 2 shards, 40 users sending 500 req/s in total. At t=5 s shard :6380
is shut down; at t=12 s it is started again. /metrics on every node is scraped
every 250 ms. Prints a per-second timeline and how long the circuit breakers
took to open and to close again.

The "chaos" rule fails over to a local limiter (limit / 3 per node), so users
on the dead shard keep being limited instead of all being let through.

Usage: python3 bench/chaos.py      (BUILD_DIR as for run_cluster.sh)
"""

import asyncio
import random
import time
import uuid
from collections import Counter

from common import GATEWAY_PORTS, cluster, cluster_down, cluster_up, paced_load, scrape_metrics

RATE = 500
USERS = 40
DURATION_S = 18
KILL_AT_S = 5
RESTART_AT_S = 12
SCRAPE_EVERY_S = 0.25
DEAD_SHARD = "127.0.0.1:6380"
EVENTS = {KILL_AT_S: "shard killed", RESTART_AT_S: "shard restarted"}


async def cluster_snapshot() -> dict[str, int]:
    per_node = await asyncio.gather(*(asyncio.to_thread(scrape_metrics, port) for port in GATEWAY_PORTS))

    def total(prefix: str) -> int:
        return sum(value for metrics in per_node for series, value in metrics.items() if series.startswith(prefix))

    return {
        "allowed": total('rl_requests_total{decision="allowed"}'),
        "denied": total('rl_requests_total{decision="denied"}'),
        "fallback": total("rl_fallback_total"),
        "breakers_open": total(f'rl_breaker_open{{shard="{DEAD_SHARD}"}}'),
    }


async def run() -> tuple[list[tuple[float, dict[str, int]]], Counter]:
    """Returns (metrics samples as (seconds since start, snapshot), reply statuses)."""
    run_id = uuid.uuid4().hex[:6]
    users = [f"chaos-{run_id}-{i}" for i in range(USERS)]
    statuses: Counter = Counter()
    samples: list[tuple[float, dict[str, int]]] = []
    start = time.monotonic()

    async def at(second: float) -> None:
        await asyncio.sleep(max(0.0, start + second - time.monotonic()))

    async def sample_metrics() -> None:
        while (elapsed := time.monotonic() - start) < DURATION_S:
            samples.append((elapsed, await cluster_snapshot()))
            await asyncio.sleep(SCRAPE_EVERY_S)

    async def kill_and_restart_shard() -> None:
        await at(KILL_AT_S)
        await asyncio.to_thread(cluster, "shard-down")
        await at(RESTART_AT_S)
        await asyncio.to_thread(cluster, "shard-up")

    await asyncio.gather(
        sample_metrics(),
        kill_and_restart_shard(),
        paced_load("/chaos", RATE, DURATION_S, lambda: random.choice(users), lambda status: statuses.update([status])),
    )
    return samples, statuses


def first_time_after(samples, after_s: float, condition) -> float | None:
    for elapsed, snapshot in samples:
        if elapsed >= after_s and condition(snapshot):
            return elapsed - after_s
    return None


def print_timeline(samples) -> None:
    print(f"{'t (s)':>5}  {'allowed/s':>9}  {'denied/s':>8}  {'fallback/s':>10}  nodes with :6380 breaker open")
    previous = samples[0][1]
    next_second = 1.0
    for elapsed, snapshot in samples:
        if elapsed < next_second:
            continue
        delta = {name: snapshot[name] - previous[name] for name in ("allowed", "denied", "fallback")}
        event = EVENTS.get(next_second)  # row N ends at the first sample after N s, so it includes an event at N
        print(f"{next_second:>5.0f}  {delta['allowed']:>9}  {delta['denied']:>8}  {delta['fallback']:>10}"
              f"  {snapshot['breakers_open']}" + (f"   <- {event}" if event else ""))
        previous = snapshot
        next_second += 1


def main() -> None:
    cluster_up("redis")
    try:
        samples, statuses = asyncio.run(run())
    finally:
        cluster_down()

    print(f"redis mode, 2 shards, {USERS} users, {RATE} req/s; :6380 down from t={KILL_AT_S}s to t={RESTART_AT_S}s\n")
    print_timeline(samples)

    nodes = len(GATEWAY_PORTS)
    opened = first_time_after(samples, KILL_AT_S, lambda s: s["breakers_open"] == nodes)
    closed = first_time_after(samples, RESTART_AT_S, lambda s: s["breakers_open"] == 0)
    failed = sum(count for status, count in statuses.items() if status not in (200, 429))
    print()
    print(f"breakers open on all {nodes} nodes: {opened * 1000:.0f} ms after the kill" if opened is not None
          else "breakers never opened on every node")
    print(f"breakers closed on all nodes: {closed * 1000:.0f} ms after the restart" if closed is not None
          else "breakers did not close before the run ended")
    print(f"replies: {statuses[200]} allowed, {statuses[429]} denied, {failed} failed (not 200/429)")
    print(f"(metrics are scraped every {SCRAPE_EVERY_S * 1000:.0f} ms, so times are accurate to that)")


if __name__ == "__main__":
    main()
