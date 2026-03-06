"""
proofler stress — Scalable chaos soak test for the -ler order pipeline.

Runs thousands of orders through an 8-task dependency pipeline with seeded
chaos, worker churn, abrupt kills, and synthetic zombie injection.

Usage:
    uv run python tests/stress.py                    # 1000 orders (~5-7 min)
    uv run python tests/stress.py --orders 5000      # 5000 orders (~25 min)
    uv run python tests/stress.py --soak             # 30 min continuous waves
    uv run python tests/stress.py --soak --minutes 60
    uv run python tests/stress.py --seed 42          # reproducible run
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import logging
import os
import random
import sqlite3
import sys
import tempfile
import time
import tracemalloc
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Stack imports
# ---------------------------------------------------------------------------

import resource

import sqler
from sqler import AsyncSQLerDB, F

import qler
from qler import Job, JobAttempt, JobStatus, AttemptStatus, Queue, Worker, task, current_job
from qler.exceptions import JobFailedError, JobCancelledError

from logler.context import correlation_context, CorrelationFilter, JsonHandler
from logler.investigate import Investigator, extract_metrics
from logler.db_source import db_to_jsonl, qler_job_mapping, qler_attempt_mapping

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
# Pipeline task definitions (module-level — @task rejects nested functions)
# ---------------------------------------------------------------------------

_stress_timing_db: str = ""  # Separate DB for timing data (avoids lock contention with qler)
_task_log = logging.getLogger("stress.pipeline")

# Map function names to short task keys used throughout the test
TASK_SHORT_NAMES = {
    "validate_order": "validate",
    "check_fraud": "fraud",
    "charge_payment": "charge",
    "send_confirmation": "confirm",
    "update_inventory": "inventory",
    "notify_warehouse": "warehouse",
    "generate_invoice": "invoice",
    "update_analytics": "analytics",
}


def _timing_start(task_name: str, order_id: int):
    """Record task start in the stress_timing SQLite table (sync, <1ms)."""
    conn = sqlite3.connect(_stress_timing_db)
    conn.execute(
        "INSERT OR REPLACE INTO stress_timing (job_ulid, task_name, order_id, started, finished)"
        " VALUES (?,?,?,?,NULL)",
        (current_job().ulid, task_name, order_id, time.time()),
    )
    conn.commit()
    conn.close()


def _timing_end():
    """Record task end in the stress_timing SQLite table (sync, <1ms)."""
    conn = sqlite3.connect(_stress_timing_db)
    conn.execute(
        "UPDATE stress_timing SET finished=? WHERE job_ulid=?",
        (time.time(), current_job().ulid),
    )
    conn.commit()
    conn.close()


async def validate_order(order_id: int, seed: int) -> dict:
    rng = random.Random(seed)
    _timing_start("validate", order_id)
    _task_log.info("Validating order %d", order_id,
                   extra={"step": "validate", "order_id": order_id})
    await asyncio.sleep(rng.uniform(0.005, 0.015))
    if rng.random() < 0.03:
        _task_log.error("Order %d: invalid address", order_id,
                        extra={"step": "validate", "order_id": order_id, "result": "failed"})
        raise ValueError(f"Order {order_id}: invalid address")
    _timing_end()
    _task_log.info("Order %d validated", order_id,
                   extra={"step": "validate", "order_id": order_id, "result": "ok"})
    return {"order_id": order_id, "task": "validate", "status": "ok"}


async def check_fraud(order_id: int, seed: int) -> dict:
    rng = random.Random(seed + 500)
    _timing_start("fraud", order_id)
    _task_log.info("Checking fraud for order %d", order_id,
                   extra={"step": "fraud", "order_id": order_id})
    await asyncio.sleep(rng.uniform(0.010, 0.030))
    if rng.random() < 0.05:
        _task_log.error("Order %d: fraud flagged", order_id,
                        extra={"step": "fraud", "order_id": order_id, "result": "failed"})
        raise ValueError(f"Order {order_id}: fraud flagged")
    _timing_end()
    _task_log.info("Order %d fraud check passed", order_id,
                   extra={"step": "fraud", "order_id": order_id, "result": "ok"})
    return {"order_id": order_id, "task": "fraud", "status": "ok"}


async def charge_payment(order_id: int, amount: float, seed: int) -> dict:
    rng = random.Random(seed + 1000)
    _timing_start("charge", order_id)
    _task_log.info("Charging order %d ($%.2f)", order_id, amount,
                   extra={"step": "charge", "order_id": order_id, "amount": amount})
    await asyncio.sleep(rng.uniform(0.030, 0.100))
    if rng.random() < 0.12:
        _task_log.error("Order %d: payment declined", order_id,
                        extra={"step": "charge", "order_id": order_id, "result": "failed"})
        raise ValueError(f"Order {order_id}: payment declined")
    _timing_end()
    _task_log.info("Order %d charged $%.2f", order_id, amount,
                   extra={"step": "charge", "order_id": order_id, "result": "ok", "amount": amount})
    return {"order_id": order_id, "task": "charge", "status": "ok", "amount": amount}


async def send_confirmation(order_id: int, email: str) -> dict:
    rng = random.Random(order_id + 2000)
    _timing_start("confirm", order_id)
    _task_log.info("Sending confirmation for order %d", order_id,
                   extra={"step": "confirm", "order_id": order_id})
    await asyncio.sleep(rng.uniform(0.010, 0.040))
    _timing_end()
    _task_log.info("Order %d confirmation sent", order_id,
                   extra={"step": "confirm", "order_id": order_id, "result": "ok"})
    return {"order_id": order_id, "task": "confirm", "status": "ok"}


async def update_inventory(order_id: int, items: int) -> dict:
    rng = random.Random(order_id + 3000)
    _timing_start("inventory", order_id)
    _task_log.info("Updating inventory for order %d (%d items)", order_id, items,
                   extra={"step": "inventory", "order_id": order_id, "items": items})
    await asyncio.sleep(rng.uniform(0.010, 0.040))
    _timing_end()
    _task_log.info("Order %d inventory updated", order_id,
                   extra={"step": "inventory", "order_id": order_id, "result": "ok"})
    return {"order_id": order_id, "task": "inventory", "status": "ok"}


async def notify_warehouse(order_id: int) -> dict:
    rng = random.Random(order_id + 4000)
    _timing_start("warehouse", order_id)
    _task_log.info("Notifying warehouse for order %d", order_id,
                   extra={"step": "warehouse", "order_id": order_id})
    await asyncio.sleep(rng.uniform(0.010, 0.040))
    _timing_end()
    _task_log.info("Order %d warehouse notified", order_id,
                   extra={"step": "warehouse", "order_id": order_id, "result": "ok"})
    return {"order_id": order_id, "task": "warehouse", "status": "ok"}


async def generate_invoice(order_id: int, amount: float) -> dict:
    rng = random.Random(order_id + 5000)
    _timing_start("invoice", order_id)
    _task_log.info("Generating invoice for order %d ($%.2f)", order_id, amount,
                   extra={"step": "invoice", "order_id": order_id, "amount": amount})
    await asyncio.sleep(rng.uniform(0.020, 0.040))
    _timing_end()
    _task_log.info("Order %d invoice generated", order_id,
                   extra={"step": "invoice", "order_id": order_id, "result": "ok"})
    return {"order_id": order_id, "task": "invoice", "status": "ok"}


async def update_analytics(order_id: int) -> dict:
    rng = random.Random(order_id + 6000)
    _timing_start("analytics", order_id)
    _task_log.info("Updating analytics for order %d", order_id,
                   extra={"step": "analytics", "order_id": order_id})
    await asyncio.sleep(rng.uniform(0.010, 0.030))
    _timing_end()
    _task_log.info("Order %d analytics updated", order_id,
                   extra={"step": "analytics", "order_id": order_id, "result": "ok"})
    return {"order_id": order_id, "task": "analytics", "status": "ok"}


# ---------------------------------------------------------------------------
# Task wrapper globals (set during registration)
# ---------------------------------------------------------------------------

validate_order_tw = None
check_fraud_tw = None
charge_payment_tw = None
send_confirmation_tw = None
update_inventory_tw = None
notify_warehouse_tw = None
generate_invoice_tw = None
update_analytics_tw = None

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TASKS_PER_ORDER = 8


def get_rss_mb() -> float:
    """Get current RSS in MB."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


class RSSTracker:
    """Sample RSS periodically during async execution."""

    def __init__(self, interval: float = 2.0):
        self.interval = interval
        self.samples: list[tuple[float, float]] = []  # (elapsed_s, rss_mb)
        self._task: asyncio.Task | None = None
        self._start: float = 0

    def start(self):
        self._start = time.time()
        self.samples.append((0.0, get_rss_mb()))
        self._task = asyncio.create_task(self._sample_loop())

    async def _sample_loop(self):
        try:
            while True:
                await asyncio.sleep(self.interval)
                elapsed = time.time() - self._start
                self.samples.append((elapsed, get_rss_mb()))
        except asyncio.CancelledError:
            # Final sample
            elapsed = time.time() - self._start
            self.samples.append((elapsed, get_rss_mb()))

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


async def raw_query(q: Queue, sql: str, params=()) -> list[tuple]:
    """Run raw SQL on the queue's DB adapter and return rows."""
    cursor = await q._db.adapter.execute(sql, list(params))
    rows = await cursor.fetchall()
    await cursor.close()
    await q._db.adapter.auto_commit()
    return rows


async def raw_count(q: Queue, where: str, params=()) -> int:
    """Count rows in qler_jobs matching a WHERE clause."""
    rows = await raw_query(q, f"SELECT COUNT(*) FROM qler_jobs WHERE {where}", params)
    return rows[0][0]


# ---------------------------------------------------------------------------
# Memory profiling helpers
# ---------------------------------------------------------------------------

_prev_snapshot: tracemalloc.Snapshot | None = None


def _tracemalloc_report(label: str):
    """Print tracemalloc top allocators and diff from previous snapshot."""
    global _prev_snapshot
    snap = tracemalloc.take_snapshot()
    snap = snap.filter_traces([
        tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
        tracemalloc.Filter(False, "<frozen importlib._bootstrap_external>"),
        tracemalloc.Filter(False, "<unknown>"),
    ])

    rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    current_mb, peak_mb = tracemalloc.get_traced_memory()
    print(f"\n  === MEMORY SNAPSHOT: {label} ===")
    print(f"  RSS: {rss_mb:.0f} MB | traced current: {current_mb / 1024**2:.1f} MB | "
          f"traced peak: {peak_mb / 1024**2:.1f} MB")

    # Top 15 by cumulative size
    print(f"  Top 15 allocators (cumulative):")
    for stat in snap.statistics("lineno")[:15]:
        print(f"    {stat}")

    # Diff from previous snapshot
    if _prev_snapshot is not None:
        print(f"  Top 15 growth since last snapshot:")
        prev_filtered = _prev_snapshot.filter_traces([
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, "<frozen importlib._bootstrap_external>"),
            tracemalloc.Filter(False, "<unknown>"),
        ])
        for stat in snap.compare_to(prev_filtered, "lineno")[:15]:
            print(f"    {stat}")

    _prev_snapshot = snap
    print(flush=True)


async def get_order_jobs(cid: str) -> dict[str, Job]:
    """Fetch all jobs for a correlation_id, keyed by short task name."""
    jobs = await Job.query().filter(F("correlation_id") == cid).all()
    result = {}
    for j in jobs:
        # j.task is like "__main__.charge_payment" — extract function name
        func_name = j.task.rsplit(".", 1)[-1] if "." in j.task else j.task
        short = TASK_SHORT_NAMES.get(func_name, func_name)
        result[short] = j
    return result


async def run_worker_for(worker: Worker, seconds: float, timeout: float = 10.0):
    """Run a worker for a fixed duration, then stop it gracefully."""
    worker_task = asyncio.create_task(worker.run())
    await asyncio.sleep(seconds)
    worker._running = False
    try:
        await asyncio.wait_for(worker_task, timeout=timeout)
    except asyncio.TimeoutError:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass


async def abrupt_kill_worker(worker: Worker, seconds: float):
    """Run a worker then cancel it abruptly (simulates crash)."""
    worker_task = asyncio.create_task(worker.run())
    await asyncio.sleep(seconds)
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass
    # Brief drain: let aiosqlite release WAL locks from cancelled transactions.
    # At 10K+ scale, the WAL is large and lock release after cancellation isn't
    # instantaneous — without this, subsequent DB ops hit "database is locked".
    await asyncio.sleep(0.5)


async def enqueue_order(
    q: Queue, i: int, base_seed: int, pipe_logger: logging.Logger,
) -> int:
    """Enqueue a single order's 8 jobs with the dependency chain. Returns job count."""
    cid = f"order-{i:06d}"
    amount = round(49.99 + i * 0.5, 2)
    email = f"customer-{i}@example.com"
    items = i % 5 + 1

    with correlation_context(cid):
        pipe_logger.info(f"Processing order {i}")

        job_v = await validate_order_tw.enqueue(
            i, base_seed + i, _correlation_id=cid)
        job_f = await check_fraud_tw.enqueue(
            i, base_seed + i,
            _correlation_id=cid, _depends_on=[job_v.ulid])
        job_c = await charge_payment_tw.enqueue(
            i, amount, base_seed + i,
            _correlation_id=cid, _depends_on=[job_f.ulid])
        await send_confirmation_tw.enqueue(
            i, email,
            _correlation_id=cid, _depends_on=[job_c.ulid])
        job_inv = await update_inventory_tw.enqueue(
            i, items,
            _correlation_id=cid, _depends_on=[job_c.ulid])
        await notify_warehouse_tw.enqueue(
            i,
            _correlation_id=cid, _depends_on=[job_inv.ulid])
        await generate_invoice_tw.enqueue(
            i, amount,
            _correlation_id=cid, _depends_on=[job_c.ulid])
        await update_analytics_tw.enqueue(
            i,
            _correlation_id=cid, _depends_on=[job_c.ulid])

        pipe_logger.info(f"Enqueued order {i}: {TASKS_PER_ORDER} jobs")

    return TASKS_PER_ORDER


async def count_pipeline_remaining(q: Queue) -> int:
    """Count pipeline jobs still pending or running (all jobs in DB are pipeline jobs)."""
    return await raw_count(q, "status IN ('pending','running')")


async def inject_zombies(
    q: Queue, rng: random.Random, count: int,
) -> int:
    """Inject synthetic zombie jobs by setting random pending jobs to expired-running state."""
    pending = await Job.query().filter(F("status") == "pending").all()
    if not pending:
        return 0

    targets = rng.sample(pending, min(count, len(pending)))
    now = int(time.time())
    injected = 0
    for j in targets:
        updated = await Job.query().filter(
            F("ulid") == j.ulid
        ).update_one(status="running", lease_expires_at=now - 10)
        if updated:
            injected += 1
    return injected


# ---------------------------------------------------------------------------
# Main stress test
# ---------------------------------------------------------------------------

async def run_stress(args: argparse.Namespace):
    global validate_order_tw, check_fraud_tw, charge_payment_tw
    global send_confirmation_tw, update_inventory_tw, notify_warehouse_tw
    global generate_invoice_tw, update_analytics_tw
    global _stress_timing_db

    # Start tracemalloc for memory profiling (1 frame = minimal overhead)
    tracemalloc.start(1)

    seed = args.seed if args.seed is not None else random.randint(0, 2**31)
    rng = random.Random(seed)

    print(f"\n{'='*60}")
    print(f"  -ler Stress Test")
    sqler_ver = getattr(sqler, "__version__", "dev")
    qler_ver = getattr(qler, "__version__", "dev")
    print(f"  sqler {sqler_ver} + qler {qler_ver} + logler")
    print(f"  Seed: {seed}")
    if args.soak:
        print(f"  Mode: soak ({args.minutes} min)")
    else:
        print(f"  Mode: batch ({args.orders} orders)")
    print(f"  Workers: {args.workers} × concurrency {args.concurrency} "
          f"= {args.workers * args.concurrency} total coroutines (pool_size=4, WAL)")
    print(f"{'='*60}")

    # --- Setup ---

    tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp_db.close()
    db_path = tmp_db.name

    # Separate DB for timing data — avoids lock contention with qler's async aiosqlite
    tmp_timing_db = tempfile.NamedTemporaryFile(suffix="-timing.db", delete=False)
    tmp_timing_db.close()
    timing_db_path = tmp_timing_db.name
    _stress_timing_db = timing_db_path

    # Create stress_timing table
    conn = sqlite3.connect(timing_db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stress_timing (
            job_ulid TEXT PRIMARY KEY,
            task_name TEXT,
            order_id INTEGER,
            started REAL,
            finished REAL
        )
    """)
    conn.commit()
    conn.close()

    tmp_log = tempfile.NamedTemporaryFile(suffix=".log", delete=False, mode="w")
    tmp_log.close()
    log_path = tmp_log.name

    q = Queue(db_path)
    await q.init_db()

    # Register 8 pipeline tasks
    validate_order_tw = task(q, max_retries=0)(validate_order)
    check_fraud_tw = task(q, max_retries=0)(check_fraud)
    charge_payment_tw = task(q, max_retries=2, retry_delay=1, timeout=2)(charge_payment)
    send_confirmation_tw = task(q, max_retries=1, retry_delay=1)(send_confirmation)
    update_inventory_tw = task(q, max_retries=1, retry_delay=1)(update_inventory)
    notify_warehouse_tw = task(q, max_retries=1, retry_delay=1)(notify_warehouse)
    generate_invoice_tw = task(q, max_retries=1, retry_delay=1)(generate_invoice)
    update_analytics_tw = task(q, max_retries=1, retry_delay=1)(update_analytics)

    # Pipeline logging (app + task logs)
    pipe_logger = logging.getLogger("stress")
    pipe_logger.setLevel(logging.DEBUG)
    pipe_handler = JsonHandler(filename=log_path)
    pipe_handler.addFilter(CorrelationFilter())
    pipe_logger.addHandler(pipe_handler)

    # Lifecycle logging (qler M24 structured events)
    tmp_lifecycle = tempfile.NamedTemporaryFile(suffix=".log", delete=False, mode="w")
    tmp_lifecycle.close()
    lifecycle_log_path = tmp_lifecycle.name

    lifecycle_handler = JsonHandler(filename=lifecycle_log_path)
    lifecycle_handler.addFilter(CorrelationFilter())
    lc_logger = logging.getLogger("qler.lifecycle")
    lc_logger.setLevel(logging.DEBUG)
    lc_logger.addHandler(lifecycle_handler)

    pipeline_start = time.time()

    # RSS memory tracking
    rss = RSSTracker(interval=5.0)
    rss.start()

    # -----------------------------------------------------------------------
    # Phase 1: Enqueue
    # -----------------------------------------------------------------------
    section("Phase 1: Enqueue")

    if args.soak:
        # Soak: enqueue in waves via background task
        orders_enqueued = 0
        wave_size = 25
        wave_interval = 10.0
        soak_deadline = time.time() + args.minutes * 60

        async def soak_enqueuer():
            nonlocal orders_enqueued
            while time.time() < soak_deadline:
                for _ in range(wave_size):
                    i = orders_enqueued
                    await enqueue_order(q, i, seed + i, pipe_logger)
                    orders_enqueued += 1
                print(f"  Wave: enqueued {orders_enqueued} orders "
                      f"({orders_enqueued * TASKS_PER_ORDER} jobs)")
                await asyncio.sleep(wave_interval)

        enqueue_task = asyncio.create_task(soak_enqueuer())
        # Give the first wave time to land before starting workers
        await asyncio.sleep(wave_interval + 1)
        num_orders = orders_enqueued  # updated as soak proceeds
    else:
        # Batch: enqueue all upfront
        num_orders = args.orders
        for i in range(num_orders):
            await enqueue_order(q, i, seed + i, pipe_logger)
            if (i + 1) % 100 == 0:
                print(f"  Enqueued {i + 1}/{num_orders} orders "
                      f"({(i + 1) * TASKS_PER_ORDER} jobs)")
        enqueue_task = None

    total_enqueued = num_orders * TASKS_PER_ORDER
    print(f"  Total: {total_enqueued} jobs for {num_orders} orders")

    # -----------------------------------------------------------------------
    # Phase 2: Execution with Worker Churn
    # -----------------------------------------------------------------------
    section("Phase 2: Execution (worker churn + chaos)")

    _tracemalloc_report("Phase 2 BASELINE (before any processing)")

    worker_cycles = 0
    graceful_stops = 0
    abrupt_kills = 0
    total_zombies_injected = 0
    total_zombies_recovered = 0
    jobs_resolved_per_cycle: list[int] = []

    # Pick one random cycle for abrupt kill
    abrupt_kill_cycle = rng.randint(1, 6)

    while True:
        worker_cycles += 1
        cycle_start = time.time()

        # Count pre-cycle state
        pre_remaining = await count_pipeline_remaining(q)
        if pre_remaining == 0 and enqueue_task is None:
            break

        # Determine run duration
        duration = rng.uniform(30, 90)

        # Create worker(s) (pool_size=4 via sqler connection pool + WAL mode)
        num_workers = args.workers
        workers = [
            Worker(q, concurrency=args.concurrency, poll_interval=0.01, shutdown_timeout=5.0)
            for _ in range(num_workers)
        ]

        if worker_cycles == abrupt_kill_cycle:
            # Abrupt kill: cancel all worker tasks directly
            label = f"{num_workers}w" if num_workers > 1 else ""
            print(f"  Cycle {worker_cycles}: ABRUPT KILL {label}after "
                  f"{duration:.0f}s (pre: {pre_remaining} remaining)")
            await asyncio.gather(*(abrupt_kill_worker(w, duration) for w in workers))
            abrupt_kills += 1
        else:
            # Graceful stop
            label = f"{num_workers}w " if num_workers > 1 else ""
            print(f"  Cycle {worker_cycles}: {label}graceful run {duration:.0f}s "
                  f"(pre: {pre_remaining} remaining)")
            await asyncio.gather(*(run_worker_for(w, duration) for w in workers))
            graceful_stops += 1

        # Recover expired leases
        recovered = await q.recover_expired_leases()
        total_zombies_recovered += recovered

        # Inject synthetic zombies
        zombie_count = rng.randint(3, 5)
        injected = await inject_zombies(q, rng, zombie_count)
        total_zombies_injected += injected

        # Recover the injected zombies
        recovered2 = await q.recover_expired_leases()
        total_zombies_recovered += recovered2

        # Count post-cycle state
        # In soak mode, update counts from enqueuer
        if args.soak:
            num_orders = orders_enqueued
            total_enqueued = orders_enqueued * TASKS_PER_ORDER

        post_remaining = await count_pipeline_remaining(q)
        resolved_this_cycle = pre_remaining - post_remaining
        jobs_resolved_per_cycle.append(max(0, resolved_this_cycle))

        cycle_elapsed = time.time() - cycle_start
        print(f"         resolved ~{resolved_this_cycle} jobs in "
              f"{cycle_elapsed:.1f}s | zombies: +{injected}/-{recovered + recovered2} | "
              f"remaining: {post_remaining}")

        pipe_logger.info(
            f"Cycle {worker_cycles}: resolved ~{resolved_this_cycle}, "
            f"remaining {post_remaining}, zombies injected {injected}")

        # Memory profiling: snapshot every 5 cycles
        if worker_cycles % 5 == 0:
            _tracemalloc_report(f"Phase 2, Cycle {worker_cycles}, {post_remaining} remaining")

        # Drain detection
        if post_remaining == 0:
            if enqueue_task is None:
                break
            # Soak mode: check if enqueue is done
            if enqueue_task.done():
                # Final drain pass
                drain_workers = [
                    Worker(q, concurrency=args.concurrency, poll_interval=0.01, shutdown_timeout=5.0)
                    for _ in range(args.workers)
                ]
                await asyncio.gather(*(run_worker_for(dw, 30.0) for dw in drain_workers))
                await q.recover_expired_leases()
                break

        # Safety valve: don't run forever in batch mode
        # Scale with order count — logging I/O lowers throughput at scale
        max_cycles = max(30, num_orders // 10)
        if not args.soak and worker_cycles >= max_cycles:
            print(f"  WARNING: hit {max_cycles} cycle limit, stopping")
            break

        # Soak time limit
        if args.soak and time.time() > (pipeline_start + (args.minutes + 5) * 60):
            print(f"  Soak time limit reached, draining...")
            if not enqueue_task.done():
                enqueue_task.cancel()
                try:
                    await enqueue_task
                except asyncio.CancelledError:
                    pass
            # Final drain
            drain_workers2 = [
                Worker(q, concurrency=args.concurrency, poll_interval=0.01, shutdown_timeout=5.0)
                for _ in range(args.workers)
            ]
            await asyncio.gather(*(run_worker_for(dw, 300.0, timeout=310.0) for dw in drain_workers2))
            await q.recover_expired_leases()
            break

    # If soak enqueuer is still running, stop it
    if enqueue_task is not None and not enqueue_task.done():
        enqueue_task.cancel()
        try:
            await enqueue_task
        except asyncio.CancelledError:
            pass

    # Final counts
    if args.soak:
        num_orders = orders_enqueued
    total_enqueued = num_orders * TASKS_PER_ORDER
    pipeline_elapsed = time.time() - pipeline_start

    print(f"\n  Execution complete: {worker_cycles} cycles "
          f"({graceful_stops} graceful, {abrupt_kills} abrupt) "
          f"in {pipeline_elapsed:.1f}s")

    _tracemalloc_report("Phase 2 FINAL (after all processing)")

    # Stop RSS tracker
    rss.stop()
    try:
        await rss._task
    except asyncio.CancelledError:
        pass

    # -----------------------------------------------------------------------
    # Phase 3: Correctness Assertions
    # -----------------------------------------------------------------------
    section("Phase 3: Correctness Assertions")

    _tracemalloc_report("Phase 3 START")

    # 1. No lost jobs — all jobs should be in a terminal state
    pending_running = await raw_count(q, "status IN ('pending','running')")
    check("No lost jobs (0 pending/running)",
          pending_running == 0, f"{pending_running} unresolved")

    # 2. No double execution
    double_rows = await raw_query(q, """
        SELECT job_ulid, COUNT(*) as c FROM qler_job_attempts
        WHERE status='completed' GROUP BY job_ulid HAVING c > 1
    """)
    check("No double execution",
          len(double_rows) == 0,
          f"{len(double_rows)} jobs completed >1 time")

    # 3. Total jobs
    total_jobs = await raw_count(q, "1=1")
    expected_total = num_orders * TASKS_PER_ORDER
    check(f"Total jobs = {expected_total}",
          total_jobs == expected_total, f"got {total_jobs}")

    # 4. Success rate — count orders where all 8 tasks completed
    successful_cid_rows = await raw_query(q, """
        SELECT json_extract(data, '$.correlation_id') as cid
        FROM qler_jobs
        GROUP BY cid
        HAVING COUNT(*) = 8
           AND SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) = 8
    """)
    successful_cids = [r[0] for r in successful_cid_rows]
    success_pct = len(successful_cids) / num_orders * 100 if num_orders > 0 else 0
    check("Success rate 65-95%",
          65 <= success_pct <= 95,
          f"{success_pct:.1f}% ({len(successful_cids)}/{num_orders})")

    # 5. Cascade: failed charge → downstream cancelled
    failed_charge_rows = await raw_query(q, """
        SELECT json_extract(data, '$.correlation_id') as cid
        FROM qler_jobs
        WHERE json_extract(data, '$.task') LIKE '%charge_payment'
          AND status = 'failed'
    """)
    failed_charge_cids = [r[0] for r in failed_charge_rows]
    check("Charge failures exist for cascade test",
          len(failed_charge_cids) >= 5,
          f"got {len(failed_charge_cids)}")
    sample_charge = rng.sample(
        failed_charge_cids, min(20, len(failed_charge_cids)))
    cascade_charge_ok = True
    cascade_charge_detail: list[str] = []
    for cid in sample_charge:
        jobs = await get_order_jobs(cid)
        for stage in ("confirm", "inventory", "warehouse", "invoice", "analytics"):
            dep = jobs.get(stage)
            if dep is None:
                cascade_charge_ok = False
                cascade_charge_detail.append(f"{cid} {stage}=MISSING")
            elif dep.status != "cancelled":
                cascade_charge_ok = False
                cascade_charge_detail.append(f"{cid} {stage}={dep.status}")
            elif "Dependency" not in (dep.last_error or ""):
                cascade_charge_ok = False
                cascade_charge_detail.append(
                    f"{cid} {stage} missing 'Dependency' in last_error")
    check(f"Cascade: {len(sample_charge)} failed-charge orders → downstream cancelled",
          cascade_charge_ok, "; ".join(cascade_charge_detail[:5]))

    # 6. Cascade: failed validate/fraud → all downstream cancelled
    failed_early_rows = await raw_query(q, """
        SELECT DISTINCT json_extract(data, '$.correlation_id') as cid
        FROM qler_jobs
        WHERE (json_extract(data, '$.task') LIKE '%validate_order'
               OR json_extract(data, '$.task') LIKE '%check_fraud')
          AND status = 'failed'
    """)
    failed_early_cids = [r[0] for r in failed_early_rows]
    check("Validate/fraud failures exist for cascade test",
          len(failed_early_cids) >= 3,
          f"got {len(failed_early_cids)}")
    sample_early = rng.sample(
        failed_early_cids, min(10, len(failed_early_cids)))
    cascade_early_ok = True
    cascade_early_detail: list[str] = []
    for cid in sample_early:
        jobs = await get_order_jobs(cid)
        # Determine which downstream should be cancelled
        if jobs.get("validate") and jobs["validate"].status == "failed":
            downstream = ["fraud", "charge", "confirm", "inventory",
                          "warehouse", "invoice", "analytics"]
        else:  # fraud failed
            downstream = ["charge", "confirm", "inventory",
                          "warehouse", "invoice", "analytics"]
        for stage in downstream:
            dep = jobs.get(stage)
            if dep is None:
                cascade_early_ok = False
                cascade_early_detail.append(f"{cid} {stage}=MISSING")
            elif dep.status != "cancelled":
                cascade_early_ok = False
                cascade_early_detail.append(f"{cid} {stage}={dep.status}")
            elif "Dependency" not in (dep.last_error or ""):
                cascade_early_ok = False
                cascade_early_detail.append(
                    f"{cid} {stage} missing 'Dependency' in last_error")
    check(f"Cascade: {len(sample_early)} failed-validate/fraud → downstream cancelled",
          cascade_early_ok, "; ".join(cascade_early_detail[:5]))

    # 7. Retry exhaustion: failed charges have exactly 3 attempts
    sample_retry = rng.sample(
        failed_charge_cids, min(10, len(failed_charge_cids)))
    check("Retry sample non-empty",
          len(sample_retry) >= 1,
          f"got {len(sample_retry)}")
    retry_ok = True
    retry_detail: list[str] = []
    for cid in sample_retry:
        jobs = await get_order_jobs(cid)
        charge_job = jobs.get("charge")
        if charge_job is None:
            retry_ok = False
            retry_detail.append(f"{cid}: charge job missing")
            continue
        charge_attempts = await JobAttempt.query().filter(
            F("job_ulid") == charge_job.ulid
        ).all()
        if len(charge_attempts) != 3:
            retry_ok = False
            retry_detail.append(
                f"{cid}: {len(charge_attempts)} attempts (expected 3)")
    check(f"Retry exhaustion: {len(sample_retry)} failed charges have 3 attempts",
          retry_ok, "; ".join(retry_detail[:5]))

    # 8. Dependency ordering — query stress_timing table
    sample_success_cids = rng.sample(
        successful_cids, min(20, len(successful_cids)))
    ordering_ok = True
    ordering_detail: list[str] = []
    ordering_verified = 0
    ordering_skipped = 0

    # Read all timing data in one query for sampled orders
    timing_conn = sqlite3.connect(timing_db_path)
    for cid in sample_success_cids:
        jobs = await get_order_jobs(cid)
        # Build ULID→timing lookup for this order
        ulids = [j.ulid for j in jobs.values()]
        placeholders = ",".join("?" for _ in ulids)
        rows = timing_conn.execute(
            f"SELECT job_ulid, started, finished FROM stress_timing "
            f"WHERE job_ulid IN ({placeholders})", ulids
        ).fetchall()
        timing = {r[0]: {"start": r[1], "end": r[2]} for r in rows}

        v_ulid = jobs["validate"].ulid if "validate" in jobs else None
        f_ulid = jobs["fraud"].ulid if "fraud" in jobs else None
        c_ulid = jobs["charge"].ulid if "charge" in jobs else None
        inv_ulid = jobs["inventory"].ulid if "inventory" in jobs else None
        wh_ulid = jobs["warehouse"].ulid if "warehouse" in jobs else None

        def _end(ulid):
            t = timing.get(ulid)
            return t["end"] if t and t["end"] is not None else None

        def _start(ulid):
            t = timing.get(ulid)
            return t["start"] if t and t["start"] is not None else None

        # validate.end < fraud.start
        v_end, f_start = _end(v_ulid), _start(f_ulid)
        if v_end is not None and f_start is not None:
            ordering_verified += 1
            if v_end > f_start:
                ordering_ok = False
                ordering_detail.append(f"{cid}: validate.end > fraud.start")
        else:
            ordering_skipped += 1

        # fraud.end < charge.start
        f_end, c_start = _end(f_ulid), _start(c_ulid)
        if f_end is not None and c_start is not None:
            if f_end > c_start:
                ordering_ok = False
                ordering_detail.append(f"{cid}: fraud.end > charge.start")
        else:
            ordering_skipped += 1

        # charge.end < [confirm, inventory, warehouse, invoice, analytics].start
        c_end = _end(c_ulid)
        if c_end is not None:
            for stage in ("confirm", "inventory", "warehouse", "invoice", "analytics"):
                s_ulid = jobs[stage].ulid if stage in jobs else None
                s_start = _start(s_ulid)
                if s_start is not None and c_end > s_start:
                    ordering_ok = False
                    ordering_detail.append(f"{cid}: charge.end > {stage}.start")

        # inventory.end < warehouse.start
        inv_end, wh_start = _end(inv_ulid), _start(wh_ulid)
        if inv_end is not None and wh_start is not None:
            if inv_end > wh_start:
                ordering_ok = False
                ordering_detail.append(f"{cid}: inventory.end > warehouse.start")

    timing_conn.close()

    check("Dependency ordering respected",
          ordering_ok, "; ".join(ordering_detail[:5]))
    min_required = min(10, len(sample_success_cids))
    check(f"Ordering check covered >= {min_required} orders",
          ordering_verified >= min_required,
          f"verified {ordering_verified}, skipped {ordering_skipped} (incomplete timing)")

    # 9. Worker churn continuity
    multi_cycle_work = sum(1 for r in jobs_resolved_per_cycle if r > 0)
    min_active_cycles = max(2, worker_cycles // 2)
    check(f"Worker churn: >= {min_active_cycles} of {worker_cycles} cycles resolved jobs",
          multi_cycle_work >= min_active_cycles,
          f"only {multi_cycle_work}/{worker_cycles} cycles resolved jobs")

    # 10. Zombie recovery
    min_zombies = max(1, worker_cycles * 2)
    check(f"Synthetic zombies injected >= {min_zombies}",
          total_zombies_injected >= min_zombies,
          f"injected {total_zombies_injected}")
    check("Zombie recovery rate >= 80%",
          total_zombies_recovered >= total_zombies_injected * 0.8,
          f"recovered {total_zombies_recovered}/{total_zombies_injected}")

    # -----------------------------------------------------------------------
    # Phase 4: Observability (logler — full integration)
    # -----------------------------------------------------------------------
    section("Phase 4: Observability (logler)")

    _tracemalloc_report("Phase 4 START")

    # Compute final stats via SQL (no in-memory dicts)
    final_completed = await raw_count(q, "status='completed'")
    final_cancelled = await raw_count(q, "status='cancelled'")
    final_failed = await raw_count(q, "status='failed'")

    # --- C.1: Setup ---
    pipe_handler.close()
    pipe_logger.removeHandler(pipe_handler)
    lifecycle_handler.close()
    lc_logger.removeHandler(lifecycle_handler)

    # Convert DB to JSONL (verifies DB is readable)
    _tracemalloc_report("Phase 4 BEFORE db_to_jsonl()")
    jsonl_path = db_to_jsonl(db_path, [qler_job_mapping(), qler_attempt_mapping()])
    _tracemalloc_report("Phase 4 AFTER db_to_jsonl()")
    jsonl_size = os.path.getsize(jsonl_path) if os.path.exists(jsonl_path) else 0
    check("DB-to-JSONL produced non-empty file",
          jsonl_size > 0, f"got {jsonl_size} bytes")

    # Load 3 sources into Investigator (disk-backed DuckDB for 10K+ scale)
    # DuckDB must create the file itself — pre-existing empty file is rejected
    duckdb_path = os.path.join(tempfile.gettempdir(), f"stress-{os.getpid()}.duckdb")
    _tracemalloc_report("Phase 4 BEFORE Investigator()")
    inv = Investigator(sql_db_path=duckdb_path)
    _tracemalloc_report("Phase 4 AFTER Investigator()")
    inv.load_files([log_path, lifecycle_log_path])
    _tracemalloc_report("Phase 4 AFTER load_files()")
    inv.load_from_db(db_path)
    _tracemalloc_report("Phase 4 AFTER load_from_db()")

    # Report variables (set in Phase 4, used in final report)
    p50_ms = p95_ms = p99_ms = 0.0
    traceable_count = 0
    traceable_total = 0
    diagnosed_count = 0
    diagnosed_total = 0
    xcorr_count = 0
    xcorr_total = 0
    obs_entry_count = 0

    try:
        # --- C.2: Lifecycle Event Verification ---
        # Use count_only=True to get total_matches without materializing results.
        # Without this, search() deserializes every match through json.loads
        # (12M objects at 10K orders → 6 GB RSS spike).
        enqueue_result = inv.search(query="job.enqueued", count_only=True)
        enqueue_count = enqueue_result.get("total_matches", 0)
        check("Lifecycle: enqueue events captured",
              enqueue_count > 0 and abs(enqueue_count - total_enqueued) / total_enqueued < 0.20,
              f"expected ~{total_enqueued}, got {enqueue_count}")

        _tracemalloc_report("Phase 4 AFTER lifecycle count_only searches")
        complete_result = inv.search(query="job.completed", count_only=True)
        complete_count = complete_result.get("total_matches", 0)
        check("Lifecycle: completion events captured",
              complete_count > 0 and abs(complete_count - final_completed) / max(1, final_completed) < 0.20,
              f"expected ~{final_completed}, got {complete_count}")

        # --- C.3: Latency Metrics from Lifecycle Events ---
        # Filter to "job.executed" events — only these carry duration= values.
        # Without the query filter, extract_metrics(limit=0) materializes ALL
        # lifecycle entries (~80K at 10K orders) when we only need ~10K executed
        # events.  This is correct API usage, not a workaround.
        latency_metrics = extract_metrics(
            [lifecycle_log_path], fields=["duration"], query="job.executed")
        dur_fields = latency_metrics.get("fields", {}).get("duration", {})
        dur_stats = dur_fields.get("stats", {})
        dur_count = dur_fields.get("count", 0)

        min_dur_events = int(final_completed * 0.8)
        check(f"Latency: extracted duration from >= {min_dur_events} events",
              dur_count >= min_dur_events,
              f"got {dur_count}")

        p50_ms = dur_stats.get("median", 0) * 1000
        p95_ms = dur_stats.get("p95", 0) * 1000
        p99_ms = dur_stats.get("p99", 0) * 1000

        check("Latency p50 > 0ms (guard)", p50_ms > 0, f"p50={p50_ms:.1f}ms")
        check("Latency p50 < 500ms", p50_ms < 500, f"p50={p50_ms:.1f}ms")
        check("Latency p95 < 2000ms", p95_ms < 2000, f"p95={p95_ms:.1f}ms")
        check("Latency p99 < 5000ms", p99_ms < 5000, f"p99={p99_ms:.1f}ms")

        # --- C.4: Order Tracing (enhanced — expect >= 20 entries) ---
        sample_obs_cids = rng.sample(
            successful_cids, min(10, len(successful_cids)))
        traceable_total = len(sample_obs_cids)
        for cid in sample_obs_cids:
            results = inv.search(correlation_id=cid)
            r_count = len(results.get("results", []))
            obs_entry_count += r_count
            ok = r_count >= 20
            if ok:
                traceable_count += 1
            check(f"Trace {cid}: >= 20 entries",
                  ok, f"got {r_count}")

        # --- C.5: Failed Order Diagnosis (enhanced) ---
        sample_fail_cids = rng.sample(
            failed_charge_cids, min(3, len(failed_charge_cids)))
        diagnosed_total = len(sample_fail_cids)
        root_cause_keywords = ("declined", "flagged", "invalid")
        for cid in sample_fail_cids:
            results = inv.search(correlation_id=cid, level="ERROR")
            entries = results.get("results", [])
            has_error = len(entries) > 0
            has_diagnosis = any(
                any(kw in (e["entry"].get("message", "") or "").lower()
                    for kw in root_cause_keywords)
                for e in entries
            )
            ok = has_error and has_diagnosis
            if ok:
                diagnosed_count += 1
            check(f"Diagnose {cid}: ERROR with root cause",
                  ok,
                  f"errors={len(entries)}, diagnosis={'yes' if has_diagnosis else 'no'}")

        # --- C.6: SQL Analytics ---
        # Query 1: Level breakdown — error rate 1-30%
        level_counts = inv.sql_query(
            "SELECT level, COUNT(*) as cnt FROM logs GROUP BY level")
        total_sql = sum(row["cnt"] for row in level_counts)
        error_sql = sum(
            row["cnt"] for row in level_counts if row["level"] == "ERROR")
        obs_entry_count = max(obs_entry_count, total_sql)
        if total_sql > 0:
            error_pct = error_sql / total_sql * 100
            check("SQL: error rate 1-30%",
                  1 <= error_pct <= 30,
                  f"{error_pct:.1f}% ({error_sql}/{total_sql})")
        else:
            check("SQL: log view has rows", False, f"got {total_sql}")

        # Query 2: Per-event breakdown — prove multiple event types
        event_counts = inv.sql_query(
            "SELECT message, COUNT(*) as cnt FROM logs "
            "WHERE message LIKE 'job.%' GROUP BY message ORDER BY cnt DESC")
        event_types = {row["message"].split(":")[0].strip() for row in event_counts}
        expected_events = {"job.enqueued", "job.completed"}
        found_expected = expected_events & event_types
        check("SQL: multiple lifecycle event types",
              len(found_expected) >= 2,
              f"found {event_types}")

        # --- C.7: Thread Follow (enhanced — >= 20 entries) ---
        sample_thread_cids = rng.sample(
            successful_cids, min(3, len(successful_cids)))
        for cid in sample_thread_cids:
            thread = inv.follow_thread(correlation_id=cid)
            t_count = len(thread.get("entries", []))
            check(f"Thread {cid}: >= 20 entries",
                  t_count >= 20, f"got {t_count}")
            # Verify chronological order
            if t_count >= 2:
                timestamps: list[datetime] = []
                parse_failures = 0
                for e in thread["entries"]:
                    raw = e.get("timestamp", "")
                    try:
                        timestamps.append(datetime.fromisoformat(raw))
                    except (ValueError, TypeError):
                        parse_failures += 1
                check(f"Thread {cid}: all timestamps parseable",
                      parse_failures == 0, f"{parse_failures} unparseable")
                if parse_failures == 0:
                    is_sorted = all(
                        timestamps[j] <= timestamps[j + 1]
                        for j in range(len(timestamps) - 1))
                    check(f"Thread {cid}: chronological order",
                          is_sorted, "out of order")

        # --- C.8: Cross-Correlation Proof ---
        # Detect sources by message content patterns (field names vary by parser):
        #   App log:   task functions log "Validating order", "Order N validated", etc.
        #   Lifecycle: qler emits "job.enqueued: ...", "job.completed: ...", etc.
        #   DB:        db_to_jsonl produces "[job] task (ulid) status=..." messages
        _app_patterns = ("validating", "checking fraud", "charging", "confirmation",
                         "inventory", "warehouse", "invoice", "analytics",
                         "processing order", "enqueued order")
        sample_xcorr_cids = rng.sample(
            successful_cids, min(3, len(successful_cids)))
        xcorr_total = len(sample_xcorr_cids)
        for cid in sample_xcorr_cids:
            results = inv.search(correlation_id=cid)
            entries = results.get("results", [])
            has_app = any(
                any(pat in (e["entry"].get("message", "") or "").lower()
                    for pat in _app_patterns)
                for e in entries)
            has_lifecycle = any(
                (e["entry"].get("message", "") or "").startswith("job.")
                for e in entries)
            has_db = any(
                (e["entry"].get("message", "") or "").startswith("[job]")
                or (e["entry"].get("message", "") or "").startswith("[attempt]")
                for e in entries)
            sources = sum([has_app, has_lifecycle, has_db])
            ok = sources >= 2
            if ok:
                xcorr_count += 1
            check(f"Cross-source {cid}: >= 2 sources",
                  ok,
                  f"app={has_app}, lifecycle={has_lifecycle}, db={has_db}")

    finally:
        inv.close()

    # -----------------------------------------------------------------------
    # Final Report — Production Viability
    # -----------------------------------------------------------------------

    # Count per-task failure rates via SQL
    task_fail_rows = await raw_query(q, """
        SELECT json_extract(data, '$.task') as t, status, COUNT(*) as c
        FROM qler_jobs
        GROUP BY t, status
    """)
    # Build lookup: {task_suffix: {status: count}}
    task_stats: dict[str, dict[str, int]] = {}
    for row in task_fail_rows:
        t_name = row[0].rsplit(".", 1)[-1] if row[0] and "." in row[0] else (row[0] or "")
        short = TASK_SHORT_NAMES.get(t_name, t_name)
        if short not in task_stats:
            task_stats[short] = {}
        task_stats[short][row[1]] = row[2]

    validate_fails = task_stats.get("validate", {}).get("failed", 0)
    fraud_fails = task_stats.get("fraud", {}).get("failed", 0)
    charge_fails = task_stats.get("charge", {}).get("failed", 0)
    fraud_completed = task_stats.get("fraud", {}).get("completed", 0)
    charge_eligible = fraud_completed  # charge is attempted only if fraud passed

    throughput = total_enqueued / pipeline_elapsed if pipeline_elapsed > 0 else 0
    jobs_per_min = throughput * 60

    v_pct = validate_fails / num_orders * 100 if num_orders else 0
    f_pct = fraud_fails / num_orders * 100 if num_orders else 0
    c_pct = charge_fails / charge_eligible * 100 if charge_eligible else 0

    # Format duration
    dur_min = int(pipeline_elapsed // 60)
    dur_sec = int(pipeline_elapsed % 60)
    if dur_min > 0:
        dur_str = f"{dur_min}m {dur_sec}s"
    else:
        dur_str = f"{dur_sec}s"

    total_checks = passed + failed

    section("STRESS TEST: PRODUCTION VIABILITY REPORT")

    print(f"  Duration:     {dur_str}")
    print(f"  Orders:       {num_orders:,} "
          f"({len(successful_cids):,} succeeded, "
          f"{num_orders - len(successful_cids):,} failed)")
    print(f"  Jobs:         {total_enqueued:,} "
          f"({final_completed:,} completed, "
          f"{final_cancelled:,} cancelled, "
          f"{final_failed:,} failed)")
    print(f"  Throughput:   {throughput:.1f} jobs/s sustained "
          f"({jobs_per_min:,.0f} jobs/min)")
    print()
    print(f"  Latency (from qler lifecycle events via logler):")
    print(f"    p50: {p50_ms:.0f}ms    p95: {p95_ms:.0f}ms    p99: {p99_ms:.0f}ms")
    print()
    print(f"  Error budget:")
    print(f"    validate: {v_pct:.1f}%  fraud: {f_pct:.1f}%  charge: {c_pct:.1f}%")
    print(f"    Overall order success: "
          f"{len(successful_cids) / max(1, num_orders) * 100:.1f}%")
    print()
    print(f"  Observability (logler):")
    print(f"    Log entries:     ~{obs_entry_count:,} (app + lifecycle + DB)")
    print(f"    Unique CIDs:     {num_orders:,}")
    print(f"    Traceable:       "
          f"{'YES' if traceable_count == traceable_total else 'PARTIAL'} "
          f"({traceable_count}/{traceable_total} sampled)")
    print(f"    Error diagnosis: "
          f"{'YES' if diagnosed_count == diagnosed_total else 'PARTIAL'} "
          f"({diagnosed_count}/{diagnosed_total} failed orders diagnosed)")
    print(f"    Cross-source:    "
          f"{'YES' if xcorr_count == xcorr_total else 'PARTIAL'} "
          f"({xcorr_count}/{xcorr_total} orders traced across sources)")
    print()
    print(f"  Reliability:")
    print(f"    Workers:       {args.workers} × concurrency {args.concurrency} "
          f"= {args.workers * args.concurrency} coroutines (pool_size=4, WAL)")
    print(f"    Worker churn:  {worker_cycles} cycles "
          f"({graceful_stops} graceful, {abrupt_kills} abrupt kill)")
    print(f"    Zombies:       {total_zombies_injected} injected, "
          f"{total_zombies_recovered} recovered")
    print(f"    Data loss:     {pending_running} jobs")
    print()
    print(f"  Memory (RSS):")
    print(f"    Peak:          {rss.peak_mb:.0f} MB")
    print(f"    Final:         {rss.final_mb:.0f} MB")
    print(f"    Growth:        {rss.growth_mb:+.0f} MB over {len(rss.samples)} samples")
    if rss.peak_mb > 400:
        print(f"    WARNING:       Peak RSS exceeds 400 MB (Koyeb 512 MB limit)")
    print()
    print(f"  Production equivalence:")
    print(f"    ~{jobs_per_min:,.0f} background jobs/min")
    print()
    print(f"  Checks: {passed}/{total_checks} passed")
    print(f"{'='*60}")

    # Cleanup
    await q.close()
    for p in (db_path, timing_db_path, duckdb_path, log_path, lifecycle_log_path, jsonl_path):
        if p and os.path.exists(p):
            try:
                os.unlink(p)
            except OSError:
                pass
        for suffix in ("-wal", "-shm"):
            wp = p + suffix
            if wp and os.path.exists(wp):
                try:
                    os.unlink(wp)
                except OSError:
                    pass

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
        description="proofler stress — scalable chaos soak test")
    parser.add_argument(
        "--orders", type=int, default=1000,
        help="Number of orders (default 1000, ignored in soak mode)")
    parser.add_argument(
        "--soak", action="store_true",
        help="Continuous wave enqueue for --minutes duration")
    parser.add_argument(
        "--minutes", type=int, default=30,
        help="Soak duration in minutes (default 30, only with --soak)")
    parser.add_argument(
        "--seed", type=int, default=None,
        help="RNG seed for reproducibility (default: random)")
    parser.add_argument(
        "--concurrency", type=int, default=2,
        help="Worker concurrency level (default 2, optimal for SQLite+WAL)")
    parser.add_argument(
        "--workers", type=int, default=1,
        help="Number of concurrent Worker instances sharing the same Queue (default 1)")
    args = parser.parse_args()
    asyncio.run(run_stress(args))


if __name__ == "__main__":
    main()
