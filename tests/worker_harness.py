"""Standalone qler worker process — managed by procler during M3 integration tests.

This script is spawned as a subprocess by procler's ProcessManager.
It shares a qler SQLite DB with the test process via QLER_DB_PATH.

Usage (via procler):
    procler define --name qler-worker --command "uv run python tests/worker_harness.py"
    procler start qler-worker

Environment variables:
    QLER_DB_PATH     (required) Path to shared qler SQLite database
    QLER_QUEUES      (optional) Comma-separated queue names (default: "default")
    QLER_CONCURRENCY (optional) Worker concurrency level (default: 1)
    QLER_LOG_PATH    (optional) Path for logler JsonHandler output
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

from qler import Queue, Worker, task

# ---------------------------------------------------------------------------
# Task functions — must be module-level for qler's @task decorator
# ---------------------------------------------------------------------------


async def echo_task(message: str) -> str:
    """Simple echo — proves basic job execution through procler."""
    return f"echo:{message}"


async def slow_task(seconds: float = 1.0) -> str:
    """Sleeps for N seconds — proves long-running job handling."""
    await asyncio.sleep(seconds)
    return f"slept:{seconds}"


async def fail_task(message: str) -> str:
    """Always raises — proves error handling through the stack."""
    raise RuntimeError(f"intentional failure: {message}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

_worker: Worker | None = None


def _handle_signal(signum, frame):
    """Graceful shutdown on SIGTERM/SIGINT."""
    if _worker is not None:
        _worker._running = False


async def main():
    global _worker

    db_path = os.environ.get("QLER_DB_PATH")
    if not db_path:
        print("ERROR: QLER_DB_PATH environment variable is required", file=sys.stderr)
        sys.exit(1)

    queues = os.environ.get("QLER_QUEUES", "default").split(",")
    concurrency = int(os.environ.get("QLER_CONCURRENCY", "1"))
    log_path = os.environ.get("QLER_LOG_PATH")

    # Optional logler structured logging
    if log_path:
        from logler.context import CorrelationFilter, JsonHandler

        handler = JsonHandler(filename=log_path)
        handler.addFilter(CorrelationFilter())
        logger = logging.getLogger("worker_harness")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        logger.info("Worker harness starting", extra={"db_path": db_path, "queues": queues})

    # Initialize queue and register tasks
    q = Queue(db_path)
    await q.init_db()

    task(q)(echo_task)
    task(q)(slow_task)
    task(q)(fail_task)

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # Run worker
    _worker = Worker(q, queues=queues, concurrency=concurrency)
    try:
        await _worker.run()
    finally:
        await q.close()


if __name__ == "__main__":
    asyncio.run(main())
