"""
proofler koyeb_budget — Prove the -ler stack fits in Koyeb's 512 MB free tier.

Runs a FastAPI server with an in-process qler Worker and SQLite, then measures
RSS through four phases: idle, burst, archival, and sustained soak.

Usage:
    uv run python tests/koyeb_budget.py                     # Full test (~70 min)
    uv run python tests/koyeb_budget.py --quick              # Phases 1-3 only (~5 min)
    uv run python tests/koyeb_budget.py --soak-minutes 30    # Custom phase 4 duration
    uv run python tests/koyeb_budget.py --memory-ceiling 512 # Custom ceiling (default 512)
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import resource
import socket
import sys
import tempfile
import time
from pathlib import Path

import httpx
import uvicorn
from fastapi import FastAPI

import sqler
from sqler import AsyncSQLerDB

import qler
from qler import Job, JobAttempt, Queue, Worker, task

# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------

passed = 0
failed = 0
errors: list[str] = []


def check(name: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        msg = f"  FAIL  {name}"
        if detail:
            msg += f" — {detail}"
        print(msg)
        errors.append(f"{name}: {detail}" if detail else name)


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# RSS helpers
# ---------------------------------------------------------------------------

def get_current_rss_mb() -> float:
    """Current RSS in MB via /proc/self/status (Linux). Falls back to peak."""
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except (OSError, ValueError, IndexError):
        pass
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


class RSSTracker:
    """Sample RSS periodically during async execution."""

    def __init__(self, interval: float = 2.0):
        self.interval = interval
        self.samples: list[tuple[float, float]] = []
        self._task: asyncio.Task | None = None
        self._start: float = 0

    def start(self):
        self._start = time.time()
        self.samples.append((0.0, get_current_rss_mb()))
        self._task = asyncio.create_task(self._sample_loop())

    async def _sample_loop(self):
        try:
            while True:
                await asyncio.sleep(self.interval)
                elapsed = time.time() - self._start
                self.samples.append((elapsed, get_current_rss_mb()))
        except asyncio.CancelledError:
            elapsed = time.time() - self._start
            self.samples.append((elapsed, get_current_rss_mb()))

    def stop(self):
        if self._task and not self._task.done():
            self._task.cancel()

    @property
    def peak_mb(self) -> float:
        return max(s[1] for s in self.samples) if self.samples else 0

    @property
    def final_mb(self) -> float:
        return self.samples[-1][1] if self.samples else 0

    @property
    def growth_mb(self) -> float:
        if len(self.samples) < 2:
            return 0
        return self.samples[-1][1] - self.samples[0][1]


# ---------------------------------------------------------------------------
# Module-level task functions (qler @task rejects nested functions)
# ---------------------------------------------------------------------------

async def budget_process_order(order_id: int) -> dict:
    """Simulated order processing task."""
    await asyncio.sleep(0.01)
    return {"order_id": order_id, "status": "processed"}


# ---------------------------------------------------------------------------
# Globals set during setup
# ---------------------------------------------------------------------------

_queue: Queue | None = None
_worker: Worker | None = None
_worker_task: asyncio.Task | None = None
_process_order_tw = None
_db_path: str = ""


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    global _queue, _worker, _worker_task, _process_order_tw, _db_path

    # Create temp DB
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    _db_path = tmp.name

    # Init queue with constrained PRAGMAs
    _queue = Queue(_db_path)
    await _queue.init_db()

    # Apply constrained PRAGMAs (Koyeb-friendly: 8 MB cache, no mmap)
    for pragma in [
        "PRAGMA cache_size=-8000",  # 8 MB
        "PRAGMA mmap_size=0",
    ]:
        cur = await _queue._db.adapter.execute(pragma)
        await cur.close()

    # Register task
    _process_order_tw = task(_queue, max_retries=1, retry_delay=1)(budget_process_order)

    # Start worker in-process
    _worker = Worker(
        _queue,
        concurrency=1,
        poll_interval=0.1,
        shutdown_timeout=5.0,
        archive_interval=60,
        archive_after=300,
        memory_limit_mb=400,
    )
    _worker_task = asyncio.create_task(_worker.run())

    yield

    # Shutdown
    if _worker:
        _worker._running = False
    if _worker_task:
        try:
            await asyncio.wait_for(_worker_task, timeout=10.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            _worker_task.cancel()
            try:
                await _worker_task
            except asyncio.CancelledError:
                pass
    if _queue:
        await _queue.close()

    # Cleanup DB files
    for p in (_db_path,):
        if p and os.path.exists(p):
            try:
                os.unlink(p)
            except OSError:
                pass
            for suffix in ("-wal", "-shm"):
                wp = p + suffix
                if os.path.exists(wp):
                    try:
                        os.unlink(wp)
                    except OSError:
                        pass


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    rss = get_current_rss_mb()
    worker_running = _worker_task is not None and not _worker_task.done()
    return {"status": "healthy", "worker": "running" if worker_running else "stopped", "rss_mb": rss}


@app.post("/enqueue")
async def enqueue():
    job = await _process_order_tw.enqueue(int(time.time() * 1000) % 1_000_000)
    return {"job_id": job.ulid, "status": "enqueued"}


@app.get("/status/{job_id}")
async def job_status(job_id: str):
    from sqler import F
    job = await Job.query().filter(F("ulid") == job_id).first()
    if job is None:
        return {"job_id": job_id, "status": "not_found"}
    return {"job_id": job_id, "status": job.status}


# ---------------------------------------------------------------------------
# Helper: find a free port
# ---------------------------------------------------------------------------

def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

async def run_budget_test(args: argparse.Namespace):
    ceiling = args.memory_ceiling

    print(f"\n{'='*60}")
    print(f"  KOYEB MEMORY BUDGET TEST")
    sqler_ver = getattr(sqler, "__version__", "dev")
    qler_ver = getattr(qler, "__version__", "dev")
    print(f"  sqler {sqler_ver} + qler {qler_ver}")
    print(f"  Memory ceiling: {ceiling} MB")
    if args.quick:
        print(f"  Mode: quick (phases 1-3)")
    else:
        print(f"  Mode: full (phases 1-4, soak {args.soak_minutes} min)")
    print(f"{'='*60}")

    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"

    # Suppress uvicorn access logs
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    # Start uvicorn in background
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    # Wait for server to be ready
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
        for _ in range(50):
            try:
                resp = await client.get("/health")
                if resp.status_code == 200:
                    break
            except httpx.ConnectError:
                pass
            await asyncio.sleep(0.1)
        else:
            print("  ERROR: Server failed to start")
            server.should_exit = True
            await server_task
            sys.exit(1)

        # RSS tracker
        rss = RSSTracker(interval=5.0)
        rss.start()

        # Track phase peaks
        idle_rss = 0.0
        burst_peak = 0.0
        post_archival_rss = 0.0
        soak_peak = 0.0

        # -------------------------------------------------------------------
        # Phase 1: Idle
        # -------------------------------------------------------------------
        section("Phase 1: Idle baseline")

        await asyncio.sleep(5)
        resp = await client.get("/health")
        data = resp.json()
        idle_rss = data["rss_mb"]
        check("Server healthy", data["status"] == "healthy",
              f"got {data['status']}")
        check("Worker running", data["worker"] == "running",
              f"got {data['worker']}")
        check(f"Idle RSS < 200 MB", idle_rss < 200,
              f"got {idle_rss:.0f} MB")
        print(f"  Idle RSS: {idle_rss:.0f} MB")

        # -------------------------------------------------------------------
        # Phase 2: 1K job burst
        # -------------------------------------------------------------------
        section("Phase 2: 1K job burst")

        job_ids: list[str] = []
        enqueue_start = time.time()
        for i in range(1000):
            resp = await client.post("/enqueue")
            data = resp.json()
            job_ids.append(data["job_id"])
            if (i + 1) % 250 == 0:
                print(f"  Enqueued {i + 1}/1000")
        enqueue_elapsed = time.time() - enqueue_start
        print(f"  Enqueue: {enqueue_elapsed:.1f}s ({1000/enqueue_elapsed:.0f} jobs/s)")

        # Wait for completion — last-enqueued job finishes last with FIFO + concurrency=1
        wait_start = time.time()
        while time.time() - wait_start < 120:
            resp = await client.get(f"/status/{job_ids[-1]}")
            if resp.json()["status"] in ("completed", "failed"):
                break
            await asyncio.sleep(1)

        burst_peak = get_current_rss_mb()
        check(f"Burst peak RSS < 300 MB", burst_peak < 300,
              f"got {burst_peak:.0f} MB")

        # Verify all completed
        completed = 0
        for jid in job_ids[:20]:  # Sample first 20
            resp = await client.get(f"/status/{jid}")
            if resp.json()["status"] == "completed":
                completed += 1
        check("Sample jobs completed (20/20)",
              completed == 20, f"got {completed}/20")
        print(f"  Burst peak RSS: {burst_peak:.0f} MB")

        # -------------------------------------------------------------------
        # Phase 3: Archival + 1K more
        # -------------------------------------------------------------------
        section("Phase 3: Archival + 1K more")

        # Trigger archival (archive jobs > 5s old for test speed)
        await _queue.archive_jobs(older_than_seconds=5)
        archived = await _queue.archive_count()
        check("Archival moved jobs to archive", archived > 0,
              f"got {archived}")
        print(f"  Archived: {archived} jobs")

        # Process 1K more
        batch2_ids: list[str] = []
        for i in range(1000):
            resp = await client.post("/enqueue")
            batch2_ids.append(resp.json()["job_id"])
            if (i + 1) % 250 == 0:
                print(f"  Enqueued {i + 1}/1000 (batch 2)")

        # Wait for completion (poll last job, FIFO with concurrency=1)
        wait_start = time.time()
        while time.time() - wait_start < 120:
            resp = await client.get(f"/status/{batch2_ids[-1]}")
            if resp.json()["status"] in ("completed", "failed"):
                break
            await asyncio.sleep(1)

        post_archival_rss = get_current_rss_mb()
        rss_return = abs(post_archival_rss - idle_rss)
        check(f"Post-archival RSS within 50 MB of idle",
              rss_return <= 50,
              f"idle={idle_rss:.0f}, now={post_archival_rss:.0f}, "
              f"delta={rss_return:.0f} MB")
        print(f"  Post-archival RSS: {post_archival_rss:.0f} MB "
              f"(delta from idle: {rss_return:+.0f} MB)")

        # -------------------------------------------------------------------
        # Phase 4: Sustained soak
        # -------------------------------------------------------------------
        if not args.quick:
            section(f"Phase 4: Soak ({args.soak_minutes} min)")

            soak_start = time.time()
            soak_end = soak_start + args.soak_minutes * 60
            jobs_enqueued = 0
            last_progress = soak_start
            target_per_min = 50  # ~50 jobs/min trickle

            while time.time() < soak_end:
                # Enqueue ~50/min in small bursts
                burst = 5
                for _ in range(burst):
                    await client.post("/enqueue")
                    jobs_enqueued += 1

                # Wait to maintain rate
                await asyncio.sleep(60.0 / (target_per_min / burst))

                # Progress every 5 minutes or every 500 jobs
                now = time.time()
                if jobs_enqueued % 500 == 0 or now - last_progress >= 300:
                    elapsed_min = (now - soak_start) / 60
                    current_rss = get_current_rss_mb()
                    print(f"  Soak: {elapsed_min:.0f}m | "
                          f"{jobs_enqueued} jobs | RSS {current_rss:.0f} MB")
                    last_progress = now

            # Final drain
            await asyncio.sleep(15)

            soak_peak = rss.peak_mb
            check(f"Soak peak RSS < 400 MB",
                  soak_peak < 400,
                  f"got {soak_peak:.0f} MB")
            print(f"  Soak total: {jobs_enqueued} jobs in {args.soak_minutes} min")
            print(f"  Soak peak RSS: {soak_peak:.0f} MB")

        # Stop RSS tracker
        rss.stop()
        try:
            await rss._task
        except asyncio.CancelledError:
            pass

    # Shutdown server
    server.should_exit = True
    try:
        await asyncio.wait_for(server_task, timeout=10.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass

    # -------------------------------------------------------------------
    # Final Report
    # -------------------------------------------------------------------
    overall_peak = rss.peak_mb
    headroom = ceiling - overall_peak

    section("KOYEB BUDGET REPORT")
    print(f"  Phase 1 (idle):   {idle_rss:.0f} MB   "
          f"[{'PASS' if idle_rss < 200 else 'FAIL'} < 200 MB]")
    print(f"  Phase 2 (1K):     {burst_peak:.0f} MB   "
          f"[{'PASS' if burst_peak < 300 else 'FAIL'} < 300 MB]")
    print(f"  Phase 3 (arch):   {post_archival_rss:.0f} MB   "
          f"[{'PASS' if abs(post_archival_rss - idle_rss) <= 50 else 'FAIL'} "
          f"< idle+50 MB]")
    if not args.quick:
        print(f"  Phase 4 (soak):   {soak_peak:.0f} MB   "
              f"[{'PASS' if soak_peak < 400 else 'FAIL'} < 400 MB]")
    print(f"  Headroom:         {headroom:.0f} MB  ({ceiling} - {overall_peak:.0f})")
    verdict = "FITS" if overall_peak < ceiling else "DOES NOT FIT"
    print(f"  Verdict:          {verdict}")
    print()
    print(f"  Checks: {passed}/{passed + failed} passed")
    print(f"{'='*60}")

    if failed > 0:
        print(f"\n  FAILURES:")
        for e in errors:
            print(f"    - {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="proofler koyeb_budget — Koyeb 512 MB memory budget test")
    parser.add_argument(
        "--quick", action="store_true",
        help="Phases 1-3 only (~5 min)")
    parser.add_argument(
        "--soak-minutes", type=int, default=60,
        help="Phase 4 soak duration in minutes (default 60)")
    parser.add_argument(
        "--memory-ceiling", type=int, default=512,
        help="Memory ceiling in MB (default 512)")
    args = parser.parse_args()
    asyncio.run(run_budget_test(args))


if __name__ == "__main__":
    main()
