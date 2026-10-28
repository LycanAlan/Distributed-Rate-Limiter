"""Shared helpers for the benchmark scripts: a tiny keep-alive HTTP client,
/metrics scraping, and starting/stopping the local cluster. Stdlib only."""

import asyncio
import subprocess
import time
import urllib.request
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
GATEWAY_PORTS = [8081, 8082, 8083]


class Connection:
    """One keep-alive HTTP/1.1 connection; requests go one at a time."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer

    @classmethod
    async def open(cls, port: int, host: str = "127.0.0.1") -> "Connection":
        reader, writer = await asyncio.open_connection(host, port)
        return cls(reader, writer)

    async def get(self, path: str, headers: dict | None = None) -> int:
        """Sends GET and returns the status code (the body is read and dropped)."""
        lines = [f"GET {path} HTTP/1.1", "Host: bench"]
        lines += [f"{name}: {value}" for name, value in (headers or {}).items()]
        self.writer.write(("\r\n".join(lines) + "\r\n\r\n").encode())
        await self.writer.drain()

        status = int((await self.reader.readline()).split()[1])
        body_length = 0
        while (line := await self.reader.readline()) not in (b"\r\n", b""):
            name, _, value = line.decode().partition(":")
            if name.strip().lower() == "content-length":
                body_length = int(value)
        await self.reader.readexactly(body_length)
        return status

    def close(self) -> None:
        self.writer.close()


def scrape_metrics(port: int) -> dict[str, int]:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2) as response:
        text = response.read().decode()
    metrics = {}
    for line in text.splitlines():
        if line and not line.startswith("#"):
            series, _, value = line.rpartition(" ")
            metrics[series] = int(float(value))
    return metrics


def cluster_total(prefix: str, ports: list[int] = GATEWAY_PORTS) -> int:
    """Sum of every series starting with `prefix`, across all gateway nodes."""
    return sum(value for port in ports for series, value in scrape_metrics(port).items()
               if series.startswith(prefix))


def cluster_up(mode: str, rules: Path | None = None) -> None:
    args = [str(BENCH_DIR / "run_cluster.sh"), "up", mode] + ([str(rules)] if rules else [])
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL)


def cluster_down() -> None:
    subprocess.run([str(BENCH_DIR / "run_cluster.sh"), "down"], check=True, stdout=subprocess.DEVNULL)


def wait_for_fresh_window(window_ms: int, min_left_ms: int) -> None:
    """Windows are epoch-aligned: sleep until at least `min_left_ms` of the current one is left."""
    left_ms = window_ms - (time.time() * 1000) % window_ms
    if left_ms < min_left_ms:
        time.sleep((left_ms + 5) / 1000)
