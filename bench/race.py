#!/usr/bin/env python3
"""Race demo (plan Task C8).

60 clients hammer one user's limit of 100 through 3 gateway nodes at once.
naive mode does GET, then SET (two round trips), so concurrent requests read
the same count and all get admitted: the lost-update race. redis mode runs the
check-and-increment as one atomic Lua script, so exactly the limit gets through.

Usage: python3 bench/race.py      (BUILD_DIR as for run_cluster.sh; default ~/rl-build)
"""

import asyncio
import uuid

from common import GATEWAY_PORTS, Connection, cluster_down, cluster_total, cluster_up, wait_for_fresh_window

CLIENTS = 60
ATTEMPTS = 600
LIMIT = 100  # the "race" rule in rules.bench.conf
WINDOW_MS = 10_000
MIN_WINDOW_LEFT_MS = 5_000  # the whole burst must land inside one fixed window


async def fire_burst(user: str) -> int:
    """Sends ATTEMPTS requests for `user` from CLIENTS connections; returns how many got 200."""
    attempts_left = ATTEMPTS
    admitted = 0

    async def client(port: int) -> None:
        nonlocal attempts_left, admitted
        connection = await Connection.open(port)
        try:
            while attempts_left > 0:
                attempts_left -= 1  # safe: asyncio runs one coroutine at a time
                if await connection.get("/race", {"X-User-Id": user}) == 200:
                    admitted += 1
        finally:
            connection.close()

    nodes = [GATEWAY_PORTS[i % len(GATEWAY_PORTS)] for i in range(CLIENTS)]
    await asyncio.gather(*(client(port) for port in nodes))
    return admitted


def measure(mode: str) -> tuple[int, int]:
    """Returns (admitted, fallbacks) for one run in `mode`."""
    cluster_up(mode)
    try:
        wait_for_fresh_window(WINDOW_MS, MIN_WINDOW_LEFT_MS)
        user = f"race-{uuid.uuid4().hex[:8]}"  # a fresh key: nothing left over from earlier runs
        admitted = asyncio.run(fire_burst(user))
        return admitted, cluster_total("rl_fallback_total")
    finally:
        cluster_down()


def main() -> None:
    print(f"{CLIENTS} clients, {ATTEMPTS} attempts, {len(GATEWAY_PORTS)} nodes, "
          f"limit {LIMIT} per {WINDOW_MS // 1000} s window\n")
    print(f"{'mode':<6} {'admitted':>8}  over the limit")
    for mode in ("naive", "redis"):
        admitted, fallbacks = measure(mode)
        over = admitted - LIMIT
        line = f"{mode:<6} {admitted:>8}  {over:+d} ({over * 100 / LIMIT:+.0f}%)"
        if fallbacks:
            line += f"   [{fallbacks} decisions used the fail mode: is lua/fixed_window.lua implemented?]"
        print(line)


if __name__ == "__main__":
    main()
