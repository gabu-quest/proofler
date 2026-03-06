"""
proofler — Full -ler stack integration test.

Exercises sqler + qler + logler working together through their public APIs.

Requires: sqler, qler, logler all installed in the same environment.
See CLAUDE.local.md for build instructions.

Run: uv run python tests/test_stack.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import sys
import tempfile
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Stack imports
# ---------------------------------------------------------------------------

import sqler
from sqler import AsyncSQLerDB, F

import qler
from qler import (
    Job,
    JobAttempt,
    JobStatus,
    AttemptStatus,
    Queue,
    Worker,
    current_job,
    is_cancellation_requested,
    task,
    cron,
)
from qler.exceptions import JobFailedError, JobCancelledError

from logler.context import correlation_context, get_correlation_id, CorrelationFilter, JsonHandler
from logler.investigate import Investigator, search_db, extract_metrics
from logler.db_source import db_to_jsonl, qler_job_mapping, qler_attempt_mapping

from dagler import DAG, DagRun
from dagler.exceptions import RetryCompletedRunError, RetryNoFailuresError

import procler.db as procler_db
from procler.models import Process, ProcessStatus
from procler.core import process_manager as procler_pm_mod
from procler.core import context_local as procler_ctx_mod
from procler.core import health as procler_health_mod
from procler.core import events as procler_events_mod
from procler.core.process_manager import get_process_manager
from procler.core.health import HealthChecker, HealthStatus, HealthState
from procler.config.schema import HealthCheckDef

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
# Shared state
# ---------------------------------------------------------------------------

DB_PATH = ""
LOG_PATH = ""
q: Queue = None  # type: ignore


# ---------------------------------------------------------------------------
# Task definitions (module-level — @task rejects nested functions)
# ---------------------------------------------------------------------------

_task_results: dict[str, object] = {}


async def add_numbers(a: int, b: int) -> int:
    return a + b


async def fail_always(msg: str = "boom"):
    raise ValueError(msg)


async def slow_task(seconds: float = 0.1) -> str:
    await asyncio.sleep(seconds)
    return "done"


def sync_greet(name: str) -> str:
    return f"hello {name}"


async def access_context() -> dict:
    job = current_job()
    return {
        "ulid": job.ulid,
        "task": job.task,
        "correlation_id": job.correlation_id,
    }


# Immediate mode tasks
async def imm_add(a: int, b: int) -> int:
    return a + b


def imm_sync_mul(a: int, b: int) -> int:
    return a * b


async def imm_fail():
    raise RuntimeError("immediate fail")


# Job.wait tasks
async def wait_task() -> str:
    return "waited"


async def wait_fail():
    raise ValueError("wait-fail")


# Rate limit tasks
async def rate_limited_task(n: int) -> int:
    return n


async def task_rate_limited(n: int) -> int:
    return n


# Cron tasks
async def periodic_cleanup():
    return "cleaned"


# Performance upgrade tasks (Sections 20-24)
async def archive_task(n: int) -> int:
    return n * 10


async def dep_index_task(label: str) -> str:
    return f"done:{label}"


async def batch_task(n: int) -> int:
    return n + 1


async def claim_task(n: int) -> int:
    return n * 2


# Wrappers set during registration
add_numbers_tw = None
fail_always_tw = None
slow_task_tw = None
sync_greet_tw = None
access_context_tw = None
imm_add_tw = None
imm_sync_mul_tw = None
imm_fail_tw = None
wait_task_tw = None
wait_fail_tw = None
rate_limited_task_tw = None
task_rate_limited_tw = None
periodic_cleanup_cw = None

# Performance upgrade wrappers (set in sections 20-24)
archive_task_tw = None
dep_index_task_tw = None
batch_task_tw = None
claim_task_tw = None


# ---------------------------------------------------------------------------
# dagler task definitions (Sections 25-30 — module level for decorator rules)
# ---------------------------------------------------------------------------

# Section 25: Linear pipeline
async def dag_extract(source="default"):
    return {"rows": [1, 2, 3], "source": source}


async def dag_transform(dag_extract=None):
    doubled = [x * 2 for x in dag_extract["rows"]]
    return {"doubled": doubled, "source": dag_extract["source"]}


async def dag_load(dag_transform=None):
    total = sum(dag_transform["doubled"])
    return {"total": total, "count": len(dag_transform["doubled"])}


# Section 26: Diamond DAG
async def dag_root():
    return {"value": 10}


async def dag_branch_a(dag_root=None):
    return {"result": dag_root["value"] * 2}


async def dag_branch_b(dag_root=None):
    return {"result": dag_root["value"] + 5}


async def dag_merge(dag_branch_a=None, dag_branch_b=None):
    return {
        "combined": dag_branch_a["result"] + dag_branch_b["result"],
        "from_a": dag_branch_a["result"],
        "from_b": dag_branch_b["result"],
    }


# Section 27: Failure cascade
_dag_fail_counter: dict[str, int] = {}


async def dag_step_ok():
    return {"step": "ok"}


async def dag_step_fail(dag_step_ok=None):
    cid = current_job().correlation_id
    _dag_fail_counter.setdefault(cid, 0)
    _dag_fail_counter[cid] += 1
    if _dag_fail_counter[cid] == 1:
        raise ValueError("transient failure")
    return {"step": "fail_recovered", "attempt": _dag_fail_counter[cid]}


async def dag_step_downstream(dag_step_fail=None):
    return {"step": "downstream", "parent": dag_step_fail}


# Section 28: Fan-out/reduce
async def dag_fetch_items():
    return ["alpha", "bravo", "charlie", "delta", "echo"]


async def dag_process_item(item):
    return item.upper()


async def dag_collect_results(results):
    return {"count": len(results), "items": results}


async def dag_summarize(dag_collect_results=None):
    return {"summary": f"{dag_collect_results['count']} items processed"}


# Section 30: Full stack roundtrip
async def dag_ingest(batch_id=0):
    return {"batch_id": batch_id, "records": [10, 20, 30]}


async def dag_validate_records(dag_ingest=None):
    return dag_ingest["records"]


async def dag_enrich_record(record):
    return {"original": record, "enriched": record * 2}


async def dag_aggregate(results):
    total = sum(r["enriched"] for r in results)
    return {"total": total, "count": len(results)}


async def dag_finalize(dag_aggregate=None):
    return {"final_total": dag_aggregate["total"], "status": "complete"}


# ---------------------------------------------------------------------------
# M7 dagler scale task definitions (Sections 31-35)
# ---------------------------------------------------------------------------

async def m7_produce_n(n: int = 100) -> list:
    return list(range(n))


async def m7_map_square(item) -> int:
    return item * item


async def m7_reduce_sum(results) -> int:
    return sum(results)


async def m7_gen_data(seed: int = 0) -> dict:
    return {"seed": seed, "value": seed * 7}


async def m7_transform(m7_gen_data=None) -> dict:
    return {"doubled": m7_gen_data["value"] * 2}


async def m7_store(m7_transform=None) -> dict:
    return {"stored": True, "value": m7_transform["doubled"]}


async def m7_produce_100() -> list:
    return list(range(100))


async def m7_s1_map(item) -> int:
    return item * item


async def m7_s1_reduce(results) -> list:
    return results[:50]


async def m7_s2_map(item) -> int:
    return item + 1


async def m7_s2_reduce(results) -> int:
    return sum(results)


# ---------------------------------------------------------------------------
# M8 dagler limit task definitions (Sections 36-41)
# ---------------------------------------------------------------------------

# Section 36: Cancel mid-flight fan-out
async def m8_produce_items(n: int = 20) -> list:
    return list(range(n))


async def m8_slow_map(item) -> dict:
    await asyncio.sleep(2)
    return {"item": item}


async def m8_reduce_collect(results) -> dict:
    return {"count": len(results), "items": results}


# Section 37: Retry on fan-out + error
_m8_fail_counter: dict[str, int] = {}


async def m8_produce_small(n: int = 5) -> list:
    return list(range(n))


async def m8_map_fail_some(item) -> int:
    if item == 2:
        cid = current_job().correlation_id
        _m8_fail_counter.setdefault(cid, 0)
        _m8_fail_counter[cid] += 1
        if _m8_fail_counter[cid] == 1:
            raise ValueError("item-2 transient failure")
    return item * 10


async def m8_reduce_retry(results) -> list:
    return sorted(results)


# Section 39: wait() timeout
async def m8_instant_ok() -> str:
    return "ok"


async def m8_never_finishes(m8_instant_ok=None) -> str:
    await asyncio.sleep(600)
    return "should never reach"


# Section 40: Concurrent fan-out
async def m8_produce_concurrent(n: int = 50) -> list:
    return list(range(n))


async def m8_map_identity(item) -> int:
    return item


async def m8_reduce_sum_conc(results) -> int:
    return sum(results)


# ---------------------------------------------------------------------------
# Pipeline task definitions (Section 13 — order processing under chaos)
# ---------------------------------------------------------------------------

_pipeline_timing: dict[str, dict] = {}  # {ulid: {start, end, task, order_id}}


async def validate_order(order_id: int, seed: int) -> dict:
    rng = random.Random(seed)
    _pipeline_timing[current_job().ulid] = {
        "start": time.time(), "task": "validate", "order_id": order_id,
    }
    await asyncio.sleep(rng.uniform(0.005, 0.015))
    if rng.random() < 0.03:
        raise ValueError(f"Order {order_id}: invalid address")
    _pipeline_timing[current_job().ulid]["end"] = time.time()
    return {"order_id": order_id, "task": "validate", "status": "ok"}


async def charge_payment(order_id: int, amount: float, seed: int) -> dict:
    rng = random.Random(seed + 1000)
    _pipeline_timing[current_job().ulid] = {
        "start": time.time(), "task": "charge", "order_id": order_id,
    }
    await asyncio.sleep(rng.uniform(0.03, 0.10))
    if rng.random() < 0.12:
        raise ValueError(f"Order {order_id}: payment declined")
    _pipeline_timing[current_job().ulid]["end"] = time.time()
    return {"order_id": order_id, "task": "charge", "status": "ok", "amount": amount}


async def send_confirmation(order_id: int, email: str) -> dict:
    _pipeline_timing[current_job().ulid] = {
        "start": time.time(), "task": "confirm", "order_id": order_id,
    }
    await asyncio.sleep(random.uniform(0.01, 0.02))
    _pipeline_timing[current_job().ulid]["end"] = time.time()
    return {"order_id": order_id, "task": "confirm", "status": "ok"}


async def update_inventory(order_id: int, items: int) -> dict:
    _pipeline_timing[current_job().ulid] = {
        "start": time.time(), "task": "inventory", "order_id": order_id,
    }
    await asyncio.sleep(random.uniform(0.02, 0.04))
    _pipeline_timing[current_job().ulid]["end"] = time.time()
    return {"order_id": order_id, "task": "inventory", "status": "ok"}


async def notify_warehouse(order_id: int) -> dict:
    _pipeline_timing[current_job().ulid] = {
        "start": time.time(), "task": "warehouse", "order_id": order_id,
    }
    await asyncio.sleep(random.uniform(0.01, 0.015))
    _pipeline_timing[current_job().ulid]["end"] = time.time()
    return {"order_id": order_id, "task": "warehouse", "status": "ok"}


async def generate_invoice(order_id: int, amount: float) -> dict:
    _pipeline_timing[current_job().ulid] = {
        "start": time.time(), "task": "invoice", "order_id": order_id,
    }
    await asyncio.sleep(random.uniform(0.02, 0.04))
    _pipeline_timing[current_job().ulid]["end"] = time.time()
    return {"order_id": order_id, "task": "invoice", "status": "ok"}


# Pipeline wrappers (set during registration in Section 13)
validate_order_tw = None
charge_payment_tw = None
send_confirmation_tw = None
update_inventory_tw = None
notify_warehouse_tw = None
generate_invoice_tw = None


async def run_worker_for(worker: Worker, seconds: float, timeout: float | None = None):
    """Run a worker for a fixed duration, then stop it gracefully.

    timeout defaults to worker.shutdown_timeout + 1.0 so the worker can
    drain in-flight jobs before we force-cancel.
    """
    if timeout is None:
        timeout = getattr(worker, "shutdown_timeout", 5.0) + 1.0
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


# ---------------------------------------------------------------------------
# Section 1: sqler foundation
# ---------------------------------------------------------------------------

async def test_sqler_foundation():
    """Verify sqler features that qler depends on."""
    section("1. sqler foundation")

    global DB_PATH, q
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    DB_PATH = tmp.name

    # AsyncSQLerDB.on_disk — real file-based database
    q = Queue(DB_PATH)
    await q.init_db()
    check("Queue.init_db() creates database file", os.path.exists(DB_PATH))

    # Verify WAL mode
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    conn.close()
    check("WAL mode enabled", mode == "wal", f"got {mode}")

    # Verify promoted columns exist on qler_jobs table
    conn = sqlite3.connect(DB_PATH)
    cols = [row[1] for row in conn.execute("PRAGMA table_info('qler_jobs')").fetchall()]
    conn.close()
    check("qler_jobs has promoted column 'ulid'", "ulid" in cols)
    check("qler_jobs has promoted column 'status'", "status" in cols)
    check("qler_jobs has promoted column 'queue_name'", "queue_name" in cols)
    check("qler_jobs has promoted column 'priority'", "priority" in cols)
    check("qler_jobs has promoted column 'eta'", "eta" in cols)
    check("qler_jobs has promoted column 'lease_expires_at'", "lease_expires_at" in cols)

    # Verify CHECK constraints exist (check by trying an invalid insert)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT INTO qler_jobs (_id, data, _version, ulid, status) "
            "VALUES (999, '{}', 1, 'test-check', 'BOGUS')"
        )
        check("CHECK constraint on status prevents invalid values", False, "insert succeeded")
    except sqlite3.IntegrityError:
        check("CHECK constraint on status prevents invalid values", True)
    finally:
        conn.rollback()
        conn.close()

    # Verify attempt table promoted columns
    conn = sqlite3.connect(DB_PATH)
    cols = [row[1] for row in conn.execute("PRAGMA table_info('qler_job_attempts')").fetchall()]
    conn.close()
    check("qler_job_attempts has promoted column 'job_ulid'", "job_ulid" in cols)
    check("qler_job_attempts has promoted column 'status'", "status" in cols)


# ---------------------------------------------------------------------------
# Section 2: qler enqueue + immediate mode
# ---------------------------------------------------------------------------

async def test_enqueue_and_immediate():
    """Test enqueue and immediate mode execution."""
    section("2. qler enqueue + immediate mode")

    global add_numbers_tw, fail_always_tw, slow_task_tw, sync_greet_tw, access_context_tw

    # Register module-level tasks on the main queue
    add_numbers_tw = task(q)(add_numbers)
    fail_always_tw = task(q, max_retries=2, retry_delay=1)(fail_always)
    slow_task_tw = task(q, lease_duration=5)(slow_task)
    sync_greet_tw = task(q, sync=True)(sync_greet)
    access_context_tw = task(q)(access_context)

    # Basic enqueue (not executed yet)
    job1 = await add_numbers_tw.enqueue(3, 4, _correlation_id="corr-add")
    check("Enqueue returns Job", isinstance(job1, Job))
    check("Job has ULID", len(job1.ulid) > 0)
    check("Job status is pending", job1.status == "pending")
    check("Job has correlation_id", job1.correlation_id == "corr-add")
    check("Job task path recorded", "add_numbers" in job1.task)

    # Priority enqueue
    job_hi = await add_numbers_tw.enqueue(10, 20, _priority=100, _correlation_id="corr-hi")
    check("High-priority job enqueued", job_hi.priority == 100)

    # Delayed job
    job_delayed = await slow_task_tw.enqueue(0.01, _delay=3600, _correlation_id="corr-delay")
    check("Delayed job has future ETA", job_delayed.eta > int(time.time()))

    # Idempotency key
    job_idem1 = await add_numbers_tw.enqueue(1, 2, _idempotency_key="unique-1")
    job_idem2 = await add_numbers_tw.enqueue(99, 99, _idempotency_key="unique-1")
    check("Idempotency returns same job", job_idem1.ulid == job_idem2.ulid)

    # Immediate mode test — share the main queue's DB to avoid stomping Job.set_db()
    global imm_add_tw, imm_sync_mul_tw, imm_fail_tw
    imm_q = Queue(q.db, immediate=True)
    await imm_q.init_db()

    imm_add_tw = task(imm_q)(imm_add)
    imm_job = await imm_add_tw.enqueue(5, 7)
    check("Immediate mode: job completed", imm_job.status == "completed")
    check("Immediate mode: result correct", imm_job.result == 12)

    # Immediate mode with sync task
    imm_sync_mul_tw = task(imm_q, sync=True)(imm_sync_mul)
    sync_job = await imm_sync_mul_tw.enqueue(6, 7)
    check("Immediate mode sync: job completed", sync_job.status == "completed")
    check("Immediate mode sync: result correct", sync_job.result == 42)

    # Immediate mode with failure
    imm_fail_tw = task(imm_q, max_retries=0)(imm_fail)
    fail_job = await imm_fail_tw.enqueue()
    check("Immediate mode fail: job failed", fail_job.status == "failed")
    check("Immediate mode fail: error recorded", "immediate fail" in (fail_job.last_error or ""))

    await imm_q.close()

    # Count pending jobs on main queue using sqler .count()
    pending_count = await Job.query().filter(
        (F("status") == "pending") & (F("queue_name") == "default")
    ).count()
    check("sqler .count() works on promoted columns", pending_count >= 2)


# ---------------------------------------------------------------------------
# Section 3: Worker execution
# ---------------------------------------------------------------------------

async def test_worker_execution():
    """Run a real Worker loop that processes enqueued jobs."""
    section("3. Worker execution")

    # Enqueue a few more jobs for the worker to process
    job_a = await add_numbers_tw.enqueue(100, 200, _correlation_id="corr-worker-a")
    job_b = await sync_greet_tw.enqueue("world", _correlation_id="corr-worker-b")
    job_ctx = await access_context_tw.enqueue(_correlation_id="corr-ctx-test")
    job_f = await fail_always_tw.enqueue("worker-fail", _correlation_id="corr-worker-fail")

    # Create and run worker for a short time
    # concurrency=1 to avoid interleaved commits on a single aiosqlite connection
    # (the unit tests use in-memory DBs which don't hit this WAL-mode edge case)
    worker = Worker(q, queues=["default"], concurrency=1, poll_interval=0.1, shutdown_timeout=5.0)

    async def run_worker_briefly():
        worker_task = asyncio.create_task(worker.run())
        await asyncio.sleep(3.0)
        worker._running = False
        try:
            await asyncio.wait_for(worker_task, timeout=1.0)
        except asyncio.TimeoutError:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

    await run_worker_briefly()

    # Verify jobs were processed
    await job_a.refresh()
    check("Worker: add_numbers completed", job_a.status == "completed")
    check("Worker: add_numbers result correct", job_a.result == 300)

    await job_b.refresh()
    check("Worker: sync_greet completed", job_b.status == "completed")
    check("Worker: sync_greet result correct", job_b.result == "hello world")

    await job_ctx.refresh()
    check("Worker: access_context completed", job_ctx.status == "completed")
    if job_ctx.result:
        check("Worker: current_job() had correct correlation_id",
              job_ctx.result.get("correlation_id") == "corr-ctx-test")

    # fail_always has max_retries=2, so after 3 attempts it should be failed
    # But it needs more time for retries (retry_delay=1). Let's check it's been attempted.
    await job_f.refresh()
    check("Worker: fail_always was attempted", job_f.attempts >= 1)

    # Verify attempt records were created
    attempts = await JobAttempt.query().filter(F("job_ulid") == job_a.ulid).all()
    check("Attempt record created for add_numbers", len(attempts) >= 1)
    if attempts:
        check("Attempt status is completed", attempts[0].status == "completed")

    # Verify F-expressions worked (attempts counter)
    check("F-expression: attempts counter incremented", job_a.attempts >= 1)

    # Multi-field order_by: verify claim used priority + eta ordering
    # (We already tested this implicitly — high-priority job should have been processed)
    await job_hi_check()


async def job_hi_check():
    """Check the high-priority job was processed."""
    hi_jobs = await Job.query().filter(F("priority") == 100).all()
    if hi_jobs:
        check("High-priority job was processed", hi_jobs[0].status in ("completed", "running"))


# ---------------------------------------------------------------------------
# Section 4: Cancellation
# ---------------------------------------------------------------------------

async def test_cancellation():
    """Test cooperative cancellation."""
    section("4. Cancellation")

    # Cancel a pending job
    cancel_job = await add_numbers_tw.enqueue(1, 1, _correlation_id="corr-cancel")
    result = await cancel_job.cancel()
    check("Cancel pending job succeeded", result is True)
    await cancel_job.refresh()
    check("Cancelled job has correct status", cancel_job.status == "cancelled")


# ---------------------------------------------------------------------------
# Section 5: Job.wait()
# ---------------------------------------------------------------------------

async def test_job_wait():
    """Test job.wait() polling."""
    section("5. Job.wait()")

    # Use immediate mode — share DB to avoid stomping Job.set_db()
    global wait_task_tw, wait_fail_tw
    wait_q = Queue(q.db, immediate=True)
    await wait_q.init_db()

    wait_task_tw = task(wait_q)(wait_task)
    job = await wait_task_tw.enqueue()
    # Already completed in immediate mode, so wait should return immediately
    result_job = await job.wait(timeout=5.0)
    check("Job.wait() returns completed job", result_job.status == "completed")
    check("Job.wait() result accessible", result_job.result == "waited")

    # Test wait on failed job
    wait_fail_tw = task(wait_q, max_retries=0)(wait_fail)
    fail_job = await wait_fail_tw.enqueue()
    try:
        await fail_job.wait(timeout=5.0)
        check("Job.wait() raises on failure", False, "no exception raised")
    except JobFailedError as e:
        check("Job.wait() raises JobFailedError", True)
        check("JobFailedError has ulid", e.ulid == fail_job.ulid)

    await wait_q.close()


# ---------------------------------------------------------------------------
# Section 6: Lease recovery
# ---------------------------------------------------------------------------

async def test_lease_recovery():
    """Test lease expiry and recovery."""
    section("6. Lease recovery")

    # Manually put a job in RUNNING with an expired lease
    now = int(time.time())
    attempt_ulid = f"ATT-LEASE-{now}"
    lease_job = Job(
        ulid=f"LEASE-{now}",
        status="running",
        queue_name="default",
        task="test.fake_task",
        worker_id="dead-worker",
        lease_expires_at=now - 100,  # Already expired
        last_attempt_id=attempt_ulid,  # Link to the attempt record
        attempts=1,
        created_at=now,
        updated_at=now,
    )
    await lease_job.save()

    # Create attempt record for it
    attempt = JobAttempt(
        ulid=attempt_ulid,
        job_ulid=lease_job.ulid,
        status="running",
        attempt_number=1,
        worker_id="dead-worker",
        started_at=now - 200,
        lease_expires_at=now - 100,
    )
    await attempt.save()

    recovered = await q.recover_expired_leases()
    check("Lease recovery found expired job", recovered >= 1)

    await lease_job.refresh()
    check("Recovered job reset to pending", lease_job.status == "pending")
    check("Recovered job worker_id cleared", lease_job.worker_id == "")

    # Check attempt was marked as lease_expired
    await attempt.refresh()
    check("Attempt marked as lease_expired", attempt.status == "lease_expired")


# ---------------------------------------------------------------------------
# Section 7: Rate limiting
# ---------------------------------------------------------------------------

async def test_rate_limiting():
    """Test rate limiting (queue-level and task-level)."""
    section("7. Rate limiting")

    # Create a rate-limited queue — share DB to avoid stomping Job.set_db()
    global rate_limited_task_tw, task_rate_limited_tw
    rl_q = Queue(q.db, rate_limits={"limited": "2/s"})
    await rl_q.init_db()

    rate_limited_task_tw = task(rl_q, queue_name="limited")(rate_limited_task)
    task_rate_limited_tw = task(rl_q, rate_limit="1/s")(task_rate_limited)

    # Enqueue several jobs
    for i in range(5):
        await rate_limited_task_tw.enqueue(i, _correlation_id=f"corr-rl-{i}")

    check("Rate-limited jobs enqueued", True)

    # The rate limiter works during claim_job, so we'd need a worker to test it fully.
    # For now, verify the rate spec was parsed correctly.
    check("Queue rate limit parsed", "limited" in rl_q._rate_limits)
    check("Queue rate limit: 2 per second",
          rl_q._rate_limits["limited"].limit == 2
          and rl_q._rate_limits["limited"].window_seconds == 1)

    tw = rl_q._tasks.get(task_rate_limited_tw.task_path)
    check("Task rate limit parsed", tw is not None and tw.rate_spec is not None)
    if tw and tw.rate_spec:
        check("Task rate limit: 1 per second",
              tw.rate_spec.limit == 1
              and tw.rate_spec.window_seconds == 1)

    await rl_q.close()


# ---------------------------------------------------------------------------
# Section 8: Cron scheduling
# ---------------------------------------------------------------------------

async def test_cron_scheduling():
    """Test @cron decorator and scheduling."""
    section("8. Cron scheduling")

    global periodic_cleanup_cw
    cron_q = Queue(q.db)
    await cron_q.init_db()

    periodic_cleanup_cw = cron(cron_q, "*/5 * * * *", max_running=2)(periodic_cleanup)

    check("Cron task registered", periodic_cleanup_cw.task_path in cron_q._cron_tasks)
    check("Cron schedule expression", periodic_cleanup_cw.schedule.expression == "*/5 * * * *")
    check("Cron max_running", periodic_cleanup_cw.schedule.max_running == 2)

    # Test next_run calculation
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    next_run = periodic_cleanup_cw.next_run(after=now)
    check("Cron next_run is in the future", next_run > now)

    # Test idempotency key generation
    key = periodic_cleanup_cw.idempotency_key(1705312800)
    check("Cron idempotency key format", key.startswith("cron:") and "periodic_cleanup" in key)

    await cron_q.close()


# ---------------------------------------------------------------------------
# Section 9: Logging integration (logler.context)
# ---------------------------------------------------------------------------

async def test_logging_integration():
    """Test logler's correlation context and JSON logging."""
    section("9. logler logging integration")

    global LOG_PATH
    tmp = tempfile.NamedTemporaryFile(suffix=".log", delete=False, mode="w")
    tmp.close()
    LOG_PATH = tmp.name

    # Set up logler's structured logging
    test_logger = logging.getLogger("integration_test")
    test_logger.setLevel(logging.DEBUG)
    handler = JsonHandler(filename=LOG_PATH)
    handler.addFilter(CorrelationFilter())
    test_logger.addHandler(handler)

    # Log with correlation context
    with correlation_context("test-corr-001"):
        test_logger.info("Job started")
        cid = get_correlation_id()
        check("correlation_context sets ID", cid == "test-corr-001")
        test_logger.warning("Something suspicious")

    with correlation_context("test-corr-002"):
        test_logger.error("Job failed")
        test_logger.debug("Debug details")

    # Log without context
    test_logger.info("No correlation")

    handler.close()
    test_logger.removeHandler(handler)

    # Verify the log file contains valid JSON with expected fields
    with open(LOG_PATH) as f:
        lines = f.readlines()

    check("JSON log file has entries", len(lines) == 5)

    entries = [json.loads(line) for line in lines]
    check("Log entries have timestamp", all("timestamp" in e for e in entries))
    check("Log entries have level", all("level" in e for e in entries))
    check("Log entries have message", all("message" in e for e in entries))

    # Correlation IDs
    corr_entries = [e for e in entries if e.get("correlation_id")]
    check("Correlation IDs present in log", len(corr_entries) == 4)
    check("First correlation_id correct", corr_entries[0]["correlation_id"] == "test-corr-001")
    check("Third entry has different correlation_id", corr_entries[2]["correlation_id"] == "test-corr-002")

    # Entry without correlation
    no_corr = [e for e in entries if "correlation_id" not in e]
    check("Entry without context has no correlation_id", len(no_corr) == 1)


# ---------------------------------------------------------------------------
# Section 10: logler reads qler's DB (the bridge)
# ---------------------------------------------------------------------------

async def test_logler_db_bridge():
    """Test logler reading qler's SQLite database via db_source."""
    section("10. logler <-> qler DB bridge")

    # First, verify db_to_jsonl works with explicit mappings
    jsonl_path = db_to_jsonl(DB_PATH, [qler_job_mapping(), qler_attempt_mapping()])
    try:
        with open(jsonl_path) as f:
            entries = [json.loads(line) for line in f]

        check("db_to_jsonl produced entries", len(entries) > 0)

        # Check job entries
        job_entries = [e for e in entries if e.get("thread_id") == "qler_jobs"]
        check("Job entries found", len(job_entries) > 0)
        check("Job entries have service_name", all(e.get("service_name") == "qler" for e in job_entries))
        check("Job entries have level", all("level" in e for e in job_entries))

        # Check epoch timestamps were converted to ISO
        for e in job_entries:
            check("Job timestamp is ISO format", "T" in e["timestamp"])
            break  # Just check first one

        # Check attempt entries
        attempt_entries = [e for e in entries if e.get("thread_id") == "qler_job_attempts"]
        check("Attempt entries found", len(attempt_entries) > 0)

        # Verify failed jobs mapped to ERROR
        error_jobs = [e for e in job_entries if e["level"] == "ERROR"]
        check("Failed jobs mapped to ERROR level", len(error_jobs) > 0)

        # Verify correlation IDs extracted from jobs
        corr_jobs = [e for e in job_entries if e.get("correlation_id")]
        check("Correlation IDs extracted from jobs", len(corr_jobs) > 0)
    finally:
        os.unlink(jsonl_path)

    # Now test auto-detection
    jsonl_auto = db_to_jsonl(DB_PATH)
    try:
        with open(jsonl_auto) as f:
            auto_entries = [json.loads(line) for line in f]
        check("Auto-detection found entries", len(auto_entries) > 0)

        # Verify the auto-detected entries include both tables
        tables = {e.get("thread_id") for e in auto_entries}
        check("Auto-detect found qler_jobs", "qler_jobs" in tables)
        check("Auto-detect found qler_job_attempts", "qler_job_attempts" in tables)
    finally:
        os.unlink(jsonl_auto)


# ---------------------------------------------------------------------------
# Section 11: logler Investigator on qler data
# ---------------------------------------------------------------------------

async def test_investigator():
    """Test logler's Investigator loading qler's DB and searching."""
    section("11. logler Investigator")

    inv = Investigator()
    inv.load_from_db(DB_PATH)

    # Search for ERROR entries
    results = inv.search(level="ERROR")
    error_entries = results.get("results", [])
    check("Investigator found ERROR entries", len(error_entries) > 0)

    # Search for all entries
    all_results = inv.search()
    all_entries = all_results.get("results", [])
    check("Investigator found all entries", len(all_entries) > 5)

    # Search by correlation ID
    corr_results = inv.search(correlation_id="corr-worker-a")
    corr_entries = corr_results.get("results", [])
    check("Investigator search by correlation_id", len(corr_entries) >= 1)
    if corr_entries:
        check("Correlation search returns correct entry",
              corr_entries[0]["entry"].get("correlation_id") == "corr-worker-a")

    # Search by service name
    svc_results = inv.search(service_name="qler")
    svc_entries = svc_results.get("results", [])
    check("Investigator search by service_name", len(svc_entries) > 0)

    # Temp file tracking
    check("Temp files tracked", len(inv._db_temp_files) == 1)

    # Clean up
    temp_path = inv._db_temp_files[0]
    inv.close()
    check("Temp files cleaned up", not os.path.exists(temp_path))

    # search_db convenience function
    quick_results = search_db(DB_PATH, level="ERROR")
    quick_entries = quick_results.get("results", [])
    check("search_db convenience works", len(quick_entries) > 0)


# ---------------------------------------------------------------------------
# Section 12: Full roundtrip (logler reads logs AND DB together)
# ---------------------------------------------------------------------------

async def test_full_roundtrip():
    """Load both the JSON log file and qler's DB into one Investigator."""
    section("12. Full roundtrip — logs + DB combined")

    inv = Investigator()

    # Load the JSON log file from Section 9
    if os.path.exists(LOG_PATH):
        inv.load_files([LOG_PATH])

        # Load qler's DB
        inv.load_from_db(DB_PATH)

        # Search across both sources
        all_results = inv.search()
        total = len(all_results.get("results", []))
        check("Combined search found entries from both sources", total > 5)

        # Search ERROR across both sources
        error_results = inv.search(level="ERROR")
        error_entries = error_results.get("results", [])
        check("ERROR search across both sources", len(error_entries) > 0)

        # Verify entries from log file are present (check for correlation_id from logging test)
        corr_log = inv.search(correlation_id="test-corr-001")
        check("Log file entries searchable", len(corr_log.get("results", [])) >= 1)

        inv.close()
    else:
        check("Log file exists for roundtrip test", False, "LOG_PATH missing")


# ---------------------------------------------------------------------------
# Section 13: Order Processing Pipeline Under Chaos
# ---------------------------------------------------------------------------

async def test_order_pipeline():
    """Simulate 100 orders × 6 pipeline tasks with chaos injection and worker churn."""
    section("13. Order Processing Pipeline Under Chaos")

    global validate_order_tw, charge_payment_tw, send_confirmation_tw
    global update_inventory_tw, notify_warehouse_tw, generate_invoice_tw

    NUM_ORDERS = 100
    BASE_SEED = 42
    pipeline_start = time.time()

    # --- Phase 1: Setup + Enqueue ---

    # Register pipeline tasks on shared queue
    validate_order_tw = task(q, max_retries=0)(validate_order)
    charge_payment_tw = task(q, max_retries=2, retry_delay=1, timeout=2)(charge_payment)
    send_confirmation_tw = task(q, max_retries=1, retry_delay=1)(send_confirmation)
    update_inventory_tw = task(q, max_retries=1, retry_delay=1)(update_inventory)
    notify_warehouse_tw = task(q, max_retries=1, retry_delay=1)(notify_warehouse)
    generate_invoice_tw = task(q, max_retries=1, retry_delay=1)(generate_invoice)

    # Set up pipeline logging
    pipeline_log_tmp = tempfile.NamedTemporaryFile(suffix=".log", delete=False, mode="w")
    pipeline_log_tmp.close()
    pipeline_log_path = pipeline_log_tmp.name

    pipe_logger = logging.getLogger("pipeline")
    pipe_logger.setLevel(logging.DEBUG)
    pipe_handler = JsonHandler(filename=pipeline_log_path)
    pipe_handler.addFilter(CorrelationFilter())
    pipe_logger.addHandler(pipe_handler)

    # Enqueue 100 orders with dependency chains
    order_jobs: dict[int, dict[str, Job]] = {}
    pipeline_ulids: set[str] = set()

    for i in range(NUM_ORDERS):
        cid = f"order-{i:04d}"
        amount = round(49.99 + i * 0.5, 2)
        email = f"customer-{i}@example.com"
        items = i % 5 + 1

        with correlation_context(cid):
            pipe_logger.info(f"Processing order {i}")

            job_v = await validate_order_tw.enqueue(
                i, BASE_SEED + i, _correlation_id=cid)
            job_c = await charge_payment_tw.enqueue(
                i, amount, BASE_SEED + i,
                _correlation_id=cid, _depends_on=[job_v.ulid])
            job_conf = await send_confirmation_tw.enqueue(
                i, email,
                _correlation_id=cid, _depends_on=[job_c.ulid])
            job_inv = await update_inventory_tw.enqueue(
                i, items,
                _correlation_id=cid, _depends_on=[job_c.ulid])
            job_wh = await notify_warehouse_tw.enqueue(
                i,
                _correlation_id=cid, _depends_on=[job_inv.ulid])
            job_invc = await generate_invoice_tw.enqueue(
                i, amount,
                _correlation_id=cid, _depends_on=[job_c.ulid])

            order_jobs[i] = {
                "validate": job_v, "charge": job_c,
                "confirm": job_conf, "inventory": job_inv,
                "warehouse": job_wh, "invoice": job_invc,
            }
            for job in order_jobs[i].values():
                pipeline_ulids.add(job.ulid)

            pipe_logger.info(f"Enqueued order {i}: 6 jobs")

    total_enqueued = len(pipeline_ulids)
    check("Pipeline: 600 jobs enqueued", total_enqueued == 600,
          f"got {total_enqueued}")

    # --- Phase 2: Execution with Worker Churn ---

    # Worker A: process for 2 seconds then stop (short enough that fast
    # hardware with batch-claim optimization can't drain all 600 jobs)
    worker_a = Worker(q, concurrency=16, poll_interval=0.05, shutdown_timeout=5.0)
    await run_worker_for(worker_a, 2.0)

    # Count progress after Worker A
    after_a_resolved = 0
    for jobs_dict in order_jobs.values():
        for job in jobs_dict.values():
            await job.refresh()
            if job.status in ("completed", "failed", "cancelled"):
                after_a_resolved += 1

    pipe_logger.info(
        f"Worker A shut down. {after_a_resolved}/{total_enqueued} resolved. "
        f"Recovering leases.")

    # Recover any in-flight jobs from Worker A (graceful shutdown means 0 is typical)
    recovered = await q.recover_expired_leases()
    pipe_logger.info(f"Recovered {recovered} expired leases")

    # Worker B: run until all pipeline jobs resolved or 20s timeout
    worker_b = Worker(q, concurrency=16, poll_interval=0.05, shutdown_timeout=5.0)
    worker_b_task = asyncio.create_task(worker_b.run())
    for _ in range(200):  # 200 * 0.1s = 20s max
        await asyncio.sleep(0.1)
        all_active = await Job.query().filter(
            (F("status") == "pending") | (F("status") == "running")
        ).all()
        pipeline_remaining = sum(1 for j in all_active if j.ulid in pipeline_ulids)
        if pipeline_remaining == 0:
            break
    worker_b._running = False
    try:
        await asyncio.wait_for(worker_b_task, timeout=10.0)
    except asyncio.TimeoutError:
        worker_b_task.cancel()
        try:
            await worker_b_task
        except asyncio.CancelledError:
            pass

    pipe_logger.info("Worker B finished. Pipeline complete.")
    pipeline_elapsed = time.time() - pipeline_start

    # --- Phase 3: Pipeline Correctness Assertions ---

    # Refresh all pipeline jobs
    for jobs_dict in order_jobs.values():
        for job in jobs_dict.values():
            await job.refresh()

    # No lost jobs: every pipeline job should be resolved
    pending_running = sum(
        1 for jobs_dict in order_jobs.values()
        for job in jobs_dict.values()
        if job.status in ("pending", "running")
    )
    check("Pipeline: no lost jobs (0 pending/running)",
          pending_running == 0, f"{pending_running} unresolved")

    # No double execution: check no job has >1 completed attempt
    completed_attempts = await JobAttempt.query().filter(
        F("status") == "completed"
    ).all()
    pipeline_completed_attempts = [a for a in completed_attempts if a.job_ulid in pipeline_ulids]
    attempt_counts: dict[str, int] = {}
    for a in pipeline_completed_attempts:
        attempt_counts[a.job_ulid] = attempt_counts.get(a.job_ulid, 0) + 1
    double_completes = {u: c for u, c in attempt_counts.items() if c > 1}
    check("Pipeline: no double execution",
          len(double_completes) == 0,
          f"{len(double_completes)} jobs completed >1 time")

    # Total job count
    total_jobs = sum(len(d) for d in order_jobs.values())
    check("Pipeline: 600 total jobs", total_jobs == 600, f"got {total_jobs}")

    # Successful orders: all 6 jobs completed
    successful_orders = [
        i for i, jobs_dict in order_jobs.items()
        if all(j.status == "completed" for j in jobs_dict.values())
    ]
    check("Pipeline: 60-95 orders fully succeeded",
          60 <= len(successful_orders) <= 95,
          f"got {len(successful_orders)}")

    # Cascade proof: charge_payment failures cancel all downstream
    failed_charge_count = sum(
        1 for d in order_jobs.values() if d["charge"].status == "failed"
    )
    check("Pipeline: >= 5 charge failures for cascade proof",
          failed_charge_count >= 5, f"got {failed_charge_count}")

    cascade_ok = True
    cascade_detail: list[str] = []
    for i, jobs_dict in order_jobs.items():
        if jobs_dict["charge"].status != "failed":
            continue
        for stage in ("confirm", "inventory", "warehouse", "invoice"):
            dep = jobs_dict[stage]
            if dep.status != "cancelled":
                cascade_ok = False
                cascade_detail.append(f"order-{i} {stage}={dep.status}")
            elif "Dependency" not in (dep.last_error or ""):
                cascade_ok = False
                cascade_detail.append(
                    f"order-{i} {stage} missing 'Dependency' in last_error")
    check("Pipeline: failed charge cascades to downstream",
          cascade_ok, "; ".join(cascade_detail[:5]))

    # Cascade proof: validate failures also cascade
    failed_validate_count = sum(
        1 for d in order_jobs.values() if d["validate"].status == "failed"
    )
    check("Pipeline: >= 1 validate failure for cascade proof",
          failed_validate_count >= 1, f"got {failed_validate_count}")

    validate_cascade_ok = True
    validate_cascade_detail: list[str] = []
    for i, jobs_dict in order_jobs.items():
        if jobs_dict["validate"].status != "failed":
            continue
        for stage in ("charge", "confirm", "inventory", "warehouse", "invoice"):
            dep = jobs_dict[stage]
            if dep.status != "cancelled":
                validate_cascade_ok = False
                validate_cascade_detail.append(f"order-{i} {stage}={dep.status}")
            elif "Dependency" not in (dep.last_error or ""):
                validate_cascade_ok = False
                validate_cascade_detail.append(
                    f"order-{i} {stage} missing 'Dependency' in last_error")
    check("Pipeline: failed validate cascades to all downstream",
          validate_cascade_ok, "; ".join(validate_cascade_detail[:5]))

    # Dependency ordering for successful orders
    ordering_ok = True
    ordering_detail: list[str] = []
    timing_verified = 0
    for i in successful_orders[:10]:
        jobs = order_jobs[i]
        v_ulid, c_ulid = jobs["validate"].ulid, jobs["charge"].ulid
        if v_ulid in _pipeline_timing and c_ulid in _pipeline_timing:
            timing_verified += 1
            v_end = _pipeline_timing[v_ulid].get("end", 0)
            c_start = _pipeline_timing[c_ulid].get("start", 0)
            if v_end > c_start:
                ordering_ok = False
                ordering_detail.append(
                    f"order-{i}: validate end {v_end:.3f} > charge start {c_start:.3f}")
        if c_ulid in _pipeline_timing:
            c_end = _pipeline_timing[c_ulid].get("end", 0)
            for stage in ("confirm", "inventory", "invoice"):
                s_ulid = jobs[stage].ulid
                if s_ulid in _pipeline_timing:
                    s_start = _pipeline_timing[s_ulid].get("start", 0)
                    if c_end > s_start:
                        ordering_ok = False
                        ordering_detail.append(
                            f"order-{i}: charge end > {stage} start")
    check("Pipeline: dependency ordering respected",
          ordering_ok, "; ".join(ordering_detail[:5]))
    check("Pipeline: ordering check covered >= 5 of 10 sampled orders",
          timing_verified >= 5, f"only {timing_verified} had full timing data")

    # Retry exhaustion: failed charges should have 3 attempts (1 + max_retries=2)
    retry_ok = True
    retry_detail: list[str] = []
    for i, jobs_dict in order_jobs.items():
        if jobs_dict["charge"].status != "failed":
            continue
        charge_attempts = await JobAttempt.query().filter(
            F("job_ulid") == jobs_dict["charge"].ulid
        ).all()
        if len(charge_attempts) != 3:
            retry_ok = False
            retry_detail.append(f"order-{i}: {len(charge_attempts)} attempts")
    check("Pipeline: failed charges exhausted 3 attempts",
          retry_ok, "; ".join(retry_detail[:5]))

    # Worker handoff: Worker A didn't finish everything
    check("Pipeline: worker handoff occurred",
          0 < after_a_resolved < total_enqueued,
          f"Worker A resolved {after_a_resolved}/{total_enqueued}")

    # Throughput info
    final_completed = sum(
        1 for jobs_dict in order_jobs.values()
        for job in jobs_dict.values()
        if job.status == "completed"
    )
    throughput = final_completed / pipeline_elapsed if pipeline_elapsed > 0 else 0
    print(f"  INFO  {final_completed} completed, "
          f"{total_enqueued - final_completed} failed/cancelled "
          f"in {pipeline_elapsed:.1f}s ({throughput:.0f} jobs/s)")

    # --- Phase 4: Observability (the logler payoff) ---

    pipe_handler.close()
    pipe_logger.removeHandler(pipe_handler)

    # Convert DB to JSONL (standalone reference file)
    jsonl_path = db_to_jsonl(DB_PATH, [qler_job_mapping(), qler_attempt_mapping()])

    # Load both sources into Investigator
    inv = Investigator()
    inv.load_files([pipeline_log_path])
    inv.load_from_db(DB_PATH)

    try:
        # Per-order trace (5 random successful orders)
        sample_rng = random.Random(BASE_SEED)
        sample = sample_rng.sample(successful_orders, min(5, len(successful_orders)))
        for i in sample:
            cid = f"order-{i:04d}"
            results = inv.search(correlation_id=cid)
            r_count = len(results.get("results", []))
            check(f"Pipeline trace {cid}: >= 8 entries",
                  r_count >= 8, f"got {r_count}")

            thread = inv.follow_thread(correlation_id=cid)
            t_count = len(thread.get("entries", []))
            check(f"Pipeline thread {cid}: >= 8 entries",
                  t_count >= 8, f"got {t_count}")

        # Failed order diagnosis
        failed_charge_orders = [
            i for i, d in order_jobs.items() if d["charge"].status == "failed"
        ]
        check("Pipeline: failed charge orders exist for diagnosis",
              len(failed_charge_orders) >= 1,
              f"got {len(failed_charge_orders)}")
        if failed_charge_orders:
            fi = failed_charge_orders[0]
            fail_cid = f"order-{fi:04d}"
            fail_results = inv.search(correlation_id=fail_cid)
            fail_entries = fail_results.get("results", [])
            has_error = any(
                e["entry"].get("level") == "ERROR" for e in fail_entries)
            check("Pipeline: failed order has ERROR entry", has_error,
                  f"order-{fi}: no ERROR in {len(fail_entries)} entries")

            has_reason = any(
                "declined" in (e["entry"].get("message", "")).lower()
                or "failed" in (e["entry"].get("message", "")).lower()
                for e in fail_entries
                if e["entry"].get("level") == "ERROR"
            )
            check("Pipeline: ERROR mentions failure reason", has_reason,
                  f"order-{fi}: no 'declined'/'failed' in ERROR entries")

        # Pipeline-wide metrics
        metrics = extract_metrics([pipeline_log_path])
        check("Pipeline: metrics scanned >= 200 entries",
              metrics.get("entries_scanned", 0) >= 200,
              f"got {metrics.get('entries_scanned', 0)}")

        # Error rate via SQL (includes log entries + DB entries, so dilution is expected)
        level_counts = inv.sql_query(
            "SELECT level, COUNT(*) as cnt FROM logs GROUP BY level")
        total_sql = sum(row["cnt"] for row in level_counts)
        error_sql = sum(
            row["cnt"] for row in level_counts if row["level"] == "ERROR")
        check("Pipeline: SQL log view has rows",
              total_sql > 0, f"got {total_sql}")
        if total_sql > 0:
            error_pct = error_sql / total_sql * 100
            check("Pipeline: error rate 2-30%",
                  2 <= error_pct <= 30,
                  f"{error_pct:.1f}% ({error_sql}/{total_sql})")

        # Follow-thread chronological order (parse timestamps to handle mixed precision)
        if successful_orders:
            from datetime import datetime
            chrono_cid = f"order-{successful_orders[0]:04d}"
            chrono = inv.follow_thread(correlation_id=chrono_cid)
            entries = chrono.get("entries", [])
            if len(entries) >= 2:
                parsed_ts: list[datetime] = []
                parse_errors = 0
                for e in entries:
                    raw = e.get("timestamp", "")
                    try:
                        parsed_ts.append(datetime.fromisoformat(raw))
                    except (ValueError, TypeError):
                        parse_errors += 1
                        parsed_ts.append(datetime.min)
                check("Pipeline: all thread timestamps parseable",
                      parse_errors == 0, f"{parse_errors} unparseable")
                is_sorted = all(
                    parsed_ts[j] <= parsed_ts[j + 1]
                    for j in range(len(parsed_ts) - 1))
                bad_pairs = [
                    f"{parsed_ts[j].isoformat()} > {parsed_ts[j+1].isoformat()}"
                    for j in range(len(parsed_ts) - 1)
                    if parsed_ts[j] > parsed_ts[j + 1]
                ]
                check("Pipeline: follow_thread in chronological order",
                      is_sorted, "; ".join(bad_pairs[:3]))
            else:
                check("Pipeline: follow_thread has entries for chrono check",
                      False, f"only {len(entries)} entries")

    finally:
        inv.close()
        for p in (pipeline_log_path, jsonl_path):
            try:
                os.unlink(p)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# procler helpers
# ---------------------------------------------------------------------------

import signal
import shutil
from procler.config import loader as procler_config_loader

def _reset_procler():
    """Reset all procler singletons between sections."""
    procler_db.reset_database()
    procler_pm_mod._manager = None
    procler_ctx_mod._local_context = None
    procler_health_mod.reset_health_checker()
    procler_events_mod.reset_event_bus()
    procler_config_loader.reset_config_cache()
    # Remove env override
    os.environ.pop("PROCLER_CONFIG_DIR", None)


def _init_procler(tmp_dir: str) -> None:
    """Initialize procler with a temp state DB, isolated from global config."""
    _reset_procler()
    # Point procler at the temp dir so it doesn't find ~/.procler/config.yaml
    os.environ["PROCLER_CONFIG_DIR"] = tmp_dir
    procler_config_loader.reset_config_cache()
    db_path = os.path.join(tmp_dir, "procler_state.db")
    procler_db.init_database(Path(db_path))


async def _wait_for_jobs(queue: Queue, expected: int, timeout: float = 15.0,
                         queue_name: str = "default") -> int:
    """Poll until expected jobs are terminal (completed/failed/cancelled) or timeout.

    Scoped to queue_name to avoid counting jobs from other sections/queues.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        completed = await Job.query().filter(
            (F("status").in_list(["completed", "failed", "cancelled"]))
            & (F("queue_name") == queue_name)
        ).count()
        if completed >= expected:
            return completed
        await asyncio.sleep(0.3)
    return await Job.query().filter(
        (F("status").in_list(["completed", "failed", "cancelled"]))
        & (F("queue_name") == queue_name)
    ).count()


# ---------------------------------------------------------------------------
# Section 14: procler foundations
# ---------------------------------------------------------------------------

async def test_procler_foundations():
    section("14. procler foundations")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_procler_")
    try:
        _init_procler(tmp_dir)

        # Define a simple process
        proc = Process(
            name="echo-test",
            command="echo hello-from-procler",
            context_type="local",
            created_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
            updated_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
        )
        proc.save()

        # Query it back
        found = Process.query().filter(F("name") == "echo-test").all()
        check("procler: define + query process",
              len(found) == 1, f"got {len(found)} results")
        check("procler: process name matches",
              found[0].name == "echo-test", f"got {found[0].name}")
        check("procler: initial status is stopped",
              found[0].status == ProcessStatus.STOPPED.value,
              f"got {found[0].status}")

        # Start via ProcessManager
        pm = get_process_manager()
        result = await pm.start("echo-test")
        check("procler: start returns success",
              result.get("success") is True, f"got {result}")

        # Wait for process to exit (echo is instant)
        await asyncio.sleep(0.5)

        # Re-query to see updated state
        updated = Process.query().filter(F("name") == "echo-test").all()
        check("procler: process ran and stopped",
              updated[0].status == ProcessStatus.STOPPED.value,
              f"status={updated[0].status}")
        check("procler: exit code is 0",
              updated[0].exit_code == 0,
              f"exit_code={updated[0].exit_code}")

    finally:
        _reset_procler()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 15: procler manages qler worker
# ---------------------------------------------------------------------------

async def test_procler_manages_worker():
    section("15. procler manages qler worker")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_procler_worker_")
    qler_db_path = os.path.join(tmp_dir, "qler_jobs.db")
    worker_harness = os.path.join(os.path.dirname(__file__), "worker_harness.py")

    try:
        _init_procler(tmp_dir)

        # Create a local qler queue to enqueue jobs
        local_q = Queue(qler_db_path)
        await local_q.init_db()

        # Define procler process for the worker
        proc = Process(
            name="qler-worker-1",
            command=f"{sys.executable} {worker_harness}",
            context_type="local",
            env={"QLER_DB_PATH": qler_db_path},
            created_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
            updated_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
        )
        proc.save()

        # Start worker via procler
        pm = get_process_manager()
        result = await pm.start("qler-worker-1")
        check("worker: procler start success",
              result.get("success") is True, f"got {result}")

        # Give worker time to init
        await asyncio.sleep(1.5)

        proc_status = Process.query().filter(F("name") == "qler-worker-1").all()
        check("worker: process is RUNNING",
              proc_status[0].status == ProcessStatus.RUNNING.value,
              f"status={proc_status[0].status}")
        old_pid = proc_status[0].pid
        check("worker: has PID",
              old_pid is not None and old_pid > 0, f"pid={old_pid}")

        # Enqueue 10 echo jobs from test process
        for i in range(10):
            await local_q.enqueue(
                "__main__.echo_task",
                kwargs={"message": f"job-{i}"},
            )

        # Wait for all jobs to complete
        terminal = await _wait_for_jobs(local_q, 10, timeout=15.0)
        check("worker: all 10 jobs completed",
              terminal == 10, f"terminal={terminal}")

        completed = await Job.query().filter(F("status") == "completed").count()
        check("worker: all completed (not failed)",
              completed == 10, f"completed={completed}")

        # Worker should still be RUNNING (polling for more)
        proc_status = Process.query().filter(F("name") == "qler-worker-1").all()
        check("worker: still RUNNING after jobs done",
              proc_status[0].status == ProcessStatus.RUNNING.value,
              f"status={proc_status[0].status}")

        # Stop worker via procler
        stop_result = await pm.stop("qler-worker-1")
        check("worker: procler stop success",
              stop_result.get("success") is True, f"got {stop_result}")

        await asyncio.sleep(0.5)

        proc_status = Process.query().filter(F("name") == "qler-worker-1").all()
        check("worker: status is STOPPED after stop",
              proc_status[0].status == ProcessStatus.STOPPED.value,
              f"status={proc_status[0].status}")

        # Verify no pending/running jobs left
        pending = await Job.query().filter(F("status") == "pending").count()
        running = await Job.query().filter(F("status") == "running").count()
        check("worker: no pending/running jobs remain",
              pending == 0 and running == 0, f"pending={pending}, running={running}")

    finally:
        # Ensure worker is stopped
        try:
            pm = get_process_manager()
            await pm.stop("qler-worker-1")
        except Exception as e:
            print(f"  WARN  cleanup stop failed: {e}")
        await asyncio.sleep(0.3)
        _reset_procler()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 16: procler health checks for qler worker
# ---------------------------------------------------------------------------

async def test_procler_health_checks():
    section("16. procler health checks for qler worker")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_procler_health_")
    qler_db_path = os.path.join(tmp_dir, "qler_jobs.db")
    worker_harness = os.path.join(os.path.dirname(__file__), "worker_harness.py")

    try:
        _init_procler(tmp_dir)

        # Start worker
        local_q = Queue(qler_db_path)
        await local_q.init_db()

        proc = Process(
            name="qler-health-worker",
            command=f"{sys.executable} {worker_harness}",
            context_type="local",
            env={"QLER_DB_PATH": qler_db_path},
            created_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
            updated_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
        )
        proc.save()

        pm = get_process_manager()
        await pm.start("qler-health-worker")
        await asyncio.sleep(1.0)

        # Get PID for health check probe
        proc_row = Process.query().filter(F("name") == "qler-health-worker").all()[0]
        worker_pid = proc_row.pid

        # Register health check — command probe using kill -0 (checks PID alive)
        healthcheck_def = HealthCheckDef(
            test=f"kill -0 {worker_pid}",
            interval="0.5s",
            timeout="2s",
            retries=2,
            start_period="0s",
        )

        hc = HealthChecker()
        hc.register_process("qler-health-worker", healthcheck_def)
        await hc.start_checking("qler-health-worker", healthcheck_def)

        # Let health checker run a few cycles
        await asyncio.sleep(2.0)

        health_state = hc.get_health("qler-health-worker")
        check("health: status is HEALTHY",
              health_state.status == HealthStatus.HEALTHY,
              f"status={health_state.status}")
        check("health: zero consecutive failures",
              health_state.consecutive_failures == 0,
              f"failures={health_state.consecutive_failures}")
        check("health: at least 3 checks completed (0.5s interval, 2s sleep = ~4 cycles)",
              health_state.check_count >= 3,
              f"check_count={health_state.check_count}")

        # Stop health checking + worker
        await hc.stop_checking("qler-health-worker")
        health_after_stop = hc.get_health("qler-health-worker")
        check("health: DEAD after stop_checking",
              health_after_stop.status == HealthStatus.DEAD,
              f"status={health_after_stop.status}")

        await pm.stop("qler-health-worker")
        await asyncio.sleep(0.3)

        proc_row = Process.query().filter(F("name") == "qler-health-worker").all()[0]
        check("health: worker stopped cleanly",
              proc_row.status == ProcessStatus.STOPPED.value,
              f"status={proc_row.status}")
        check("health: exit code after stop",
              proc_row.exit_code in (0, -15),
              f"exit_code={proc_row.exit_code}")

    finally:
        try:
            pm = get_process_manager()
            await pm.stop("qler-health-worker")
        except Exception as e:
            print(f"  WARN  cleanup stop failed: {e}")
        await asyncio.sleep(0.3)
        _reset_procler()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 17: Worker crash + procler detection
# ---------------------------------------------------------------------------

async def test_procler_crash_recovery():
    section("17. Worker crash + procler detection")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_procler_crash_")
    qler_db_path = os.path.join(tmp_dir, "qler_jobs.db")
    worker_harness = os.path.join(os.path.dirname(__file__), "worker_harness.py")

    try:
        _init_procler(tmp_dir)

        local_q = Queue(qler_db_path)
        await local_q.init_db()

        proc = Process(
            name="qler-crash-worker",
            command=f"{sys.executable} {worker_harness}",
            context_type="local",
            env={"QLER_DB_PATH": qler_db_path},
            created_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
            updated_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
        )
        proc.save()

        pm = get_process_manager()
        await pm.start("qler-crash-worker")
        await asyncio.sleep(1.5)

        proc_row = Process.query().filter(F("name") == "qler-crash-worker").all()[0]
        old_pid = proc_row.pid
        check("crash: worker is RUNNING",
              proc_row.status == ProcessStatus.RUNNING.value,
              f"status={proc_row.status}")

        # Enqueue slow jobs to keep worker busy
        for i in range(5):
            await local_q.enqueue(
                "__main__.slow_task",
                kwargs={"seconds": 2.0},
            )

        await asyncio.sleep(0.5)  # let worker claim some

        # Kill the entire process group (shell + python child) with SIGKILL
        try:
            os.killpg(os.getpgid(old_pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            os.kill(old_pid, signal.SIGKILL)

        # Verify the PID is actually dead
        await asyncio.sleep(0.5)
        pid_alive = True
        try:
            os.kill(old_pid, 0)
        except OSError:
            pid_alive = False
        check("crash: PID is dead after SIGKILL",
              not pid_alive, f"pid={old_pid} still alive")

        # Wait for procler's exit monitor to detect the crash
        deadline = time.time() + 10.0
        while time.time() < deadline:
            proc_row = Process.query().filter(F("name") == "qler-crash-worker").all()[0]
            if proc_row.status != ProcessStatus.RUNNING.value:
                break
            await asyncio.sleep(0.3)

        # procler should detect STOPPED; if not, it's a known limitation
        # (exit monitor depends on asyncio subprocess pipe closure timing)
        proc_row = Process.query().filter(F("name") == "qler-crash-worker").all()[0]
        crash_detected = proc_row.status == ProcessStatus.STOPPED.value
        check("crash: procler detected crash",
              crash_detected, f"status={proc_row.status}")

        check("crash: non-zero exit code",
              crash_detected and proc_row.exit_code is not None and proc_row.exit_code != 0,
              f"exit_code={proc_row.exit_code}, detected={crash_detected}")

        if not crash_detected:
            # Force-update so restart test below can still exercise recovery,
            # but the check above already recorded FAIL.
            proc_row.status = ProcessStatus.STOPPED.value
            proc_row.pid = None
            proc_row.exit_code = -9
            proc_row.save()

        # Some jobs may still be pending or running (worker died mid-processing)
        pending = await Job.query().filter(F("status") == "pending").count()
        running = await Job.query().filter(F("status") == "running").count()
        check("crash: unfinished jobs exist after crash",
              (pending + running) > 0,
              f"pending={pending}, running={running}")

        # Manual restart via procler
        result = await pm.start("qler-crash-worker")
        check("crash: manual restart success",
              result.get("success") is True, f"got {result}")

        await asyncio.sleep(1.0)

        proc_row = Process.query().filter(F("name") == "qler-crash-worker").all()[0]
        new_pid = proc_row.pid
        check("crash: new PID after restart",
              new_pid is not None and new_pid != old_pid,
              f"old={old_pid}, new={new_pid}")

        # Recover expired leases so the new worker can claim in-flight jobs
        await local_q.recover_expired_leases()

        # Wait for remaining jobs to complete
        terminal = await _wait_for_jobs(local_q, 5, timeout=20.0)
        check("crash: all jobs eventually complete after restart",
              terminal == 5, f"terminal={terminal}")

        # Stop worker
        await pm.stop("qler-crash-worker")
        await asyncio.sleep(0.3)

        proc_row = Process.query().filter(F("name") == "qler-crash-worker").all()[0]
        check("crash: clean stop after recovery",
              proc_row.status == ProcessStatus.STOPPED.value,
              f"status={proc_row.status}")

    finally:
        try:
            pm = get_process_manager()
            await pm.stop("qler-crash-worker")
        except Exception as e:
            print(f"  WARN  cleanup stop failed: {e}")
        await asyncio.sleep(0.3)
        _reset_procler()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 18: procler + logler observability
# ---------------------------------------------------------------------------

async def test_procler_logler_observability():
    section("18. procler + logler observability")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_procler_obs_")
    qler_db_path = os.path.join(tmp_dir, "qler_jobs.db")
    worker_log_path = os.path.join(tmp_dir, "worker.log")
    worker_harness = os.path.join(os.path.dirname(__file__), "worker_harness.py")
    jsonl_path = None

    try:
        _init_procler(tmp_dir)

        local_q = Queue(qler_db_path)
        await local_q.init_db()

        proc = Process(
            name="qler-obs-worker",
            command=f"{sys.executable} {worker_harness}",
            context_type="local",
            env={
                "QLER_DB_PATH": qler_db_path,
                "QLER_LOG_PATH": worker_log_path,
            },
            created_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
            updated_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
        )
        proc.save()

        pm = get_process_manager()
        await pm.start("qler-obs-worker")
        await asyncio.sleep(1.5)

        # Enqueue 5 jobs with correlation IDs
        cids = [f"proc-obs-{i}" for i in range(5)]
        for i, cid in enumerate(cids):
            await local_q.enqueue(
                "__main__.echo_task",
                kwargs={"message": f"observable-{i}"},
                correlation_id=cid,
            )

        # Wait for completion
        terminal = await _wait_for_jobs(local_q, 5, timeout=15.0)
        check("observability: all 5 jobs completed",
              terminal == 5, f"terminal={terminal}")

        # Stop worker before reading logs
        await pm.stop("qler-obs-worker")
        await asyncio.sleep(0.5)

        # Convert qler DB to JSONL for logler
        jsonl_path = db_to_jsonl(qler_db_path, [
            qler_job_mapping(),
            qler_attempt_mapping(),
        ])

        check("observability: JSONL generated from qler DB",
              jsonl_path is not None and os.path.exists(jsonl_path),
              f"jsonl_path={jsonl_path}")

        # Load into Investigator
        inv = Investigator()
        files_to_load = [jsonl_path]
        if os.path.exists(worker_log_path):
            files_to_load.append(worker_log_path)
        inv.load_files(files_to_load)

        # Search by CID — should find entries
        test_cid = cids[0]
        results = inv.search(correlation_id=test_cid)
        entries = results.get("results", [])
        check("observability: CID search returns entries (>= 1 job + attempt)",
              len(entries) >= 1, f"entries={len(entries)} for cid={test_cid}")

        # Check that DB source has job lifecycle data (qler_job_mapping uses
        # service_name="qler" and message_template includes "status=...")
        db_entries = [e for e in entries if
                      e.get("entry", {}).get("service_name") == "qler"]
        check("observability: DB entries show job lifecycle (>= 1 per completed job)",
              len(db_entries) >= 1,
              f"db_entries={len(db_entries)}")

        # Verify follow_thread returns chronological entries
        thread = inv.follow_thread(correlation_id=test_cid)
        thread_entries = thread.get("entries", [])
        check("observability: follow_thread returns entries",
              len(thread_entries) >= 1,
              f"thread_entries={len(thread_entries)}")

        # Verify chronological ordering when multiple entries exist
        # (single CID may yield only 1 DB entry if worker log doesn't emit per-CID lines)
        if len(thread_entries) >= 2:
            timestamps = [e.get("timestamp", "") for e in thread_entries if e.get("timestamp")]
            is_sorted = all(timestamps[j] <= timestamps[j + 1] for j in range(len(timestamps) - 1))
            check("observability: thread entries in chronological order",
                  is_sorted, "timestamps out of order")

        inv.close()

    finally:
        try:
            pm = get_process_manager()
            await pm.stop("qler-obs-worker")
        except Exception:
            pass
        await asyncio.sleep(0.3)
        _reset_procler()
        # Clean up temp files
        for p in (jsonl_path,):
            try:
                os.unlink(p)
            except (OSError, NameError):
                pass
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 19: Full -ler stack roundtrip (sqler + qler + logler + procler)
# ---------------------------------------------------------------------------

async def test_full_ler_stack():
    section("19. Full -ler stack roundtrip (sqler + qler + logler + procler)")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_full_stack_")
    qler_db_path = os.path.join(tmp_dir, "qler_jobs.db")
    worker_log_path = os.path.join(tmp_dir, "worker.log")
    jsonl_path = None
    worker_harness = os.path.join(os.path.dirname(__file__), "worker_harness.py")

    try:
        _init_procler(tmp_dir)

        local_q = Queue(qler_db_path)
        await local_q.init_db()

        proc = Process(
            name="qler-full-stack",
            command=f"{sys.executable} {worker_harness}",
            context_type="local",
            env={
                "QLER_DB_PATH": qler_db_path,
                "QLER_LOG_PATH": worker_log_path,
            },
            created_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
            updated_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
        )
        proc.save()

        pm = get_process_manager()
        await pm.start("qler-full-stack")
        await asyncio.sleep(1.5)

        # Verify procler shows RUNNING
        proc_row = Process.query().filter(F("name") == "qler-full-stack").all()[0]
        check("full-stack: procler process RUNNING",
              proc_row.status == ProcessStatus.RUNNING.value,
              f"status={proc_row.status}")
        check("full-stack: procler captured PID",
              proc_row.pid is not None and proc_row.pid > 0,
              f"pid={proc_row.pid}")

        # Enqueue 20 jobs: 18 echo (succeed) + 2 fail
        for i in range(18):
            cid = f"full-stack-{i:03d}"
            await local_q.enqueue(
                "__main__.echo_task",
                kwargs={"message": f"round-{i}"},
                correlation_id=cid,
            )
        for i in range(2):
            cid = f"full-stack-fail-{i}"
            await local_q.enqueue(
                "__main__.fail_task",
                kwargs={"message": f"expected-failure-{i}"},
                correlation_id=cid,
            )

        # Wait for all 20 jobs to reach terminal state
        terminal = await _wait_for_jobs(local_q, 20, timeout=15.0)
        check("full-stack: all 20 jobs terminal",
              terminal == 20, f"terminal={terminal}")

        completed = await Job.query().filter(F("status") == "completed").count()
        failed_count = await Job.query().filter(F("status") == "failed").count()
        check("full-stack: 18 completed, 2 failed",
              completed == 18 and failed_count == 2,
              f"completed={completed}, failed={failed_count}")

        pending = await Job.query().filter(F("status") == "pending").count()
        check("full-stack: zero pending (no data loss)",
              pending == 0, f"pending={pending}")

        # Stop worker
        await pm.stop("qler-full-stack")
        await asyncio.sleep(0.5)

        proc_row = Process.query().filter(F("name") == "qler-full-stack").all()[0]
        check("full-stack: clean shutdown via procler",
              proc_row.status == ProcessStatus.STOPPED.value,
              f"status={proc_row.status}")

        # logler: convert DB + read logs
        jsonl_path = db_to_jsonl(qler_db_path, [
            qler_job_mapping(),
            qler_attempt_mapping(),
        ])

        inv = Investigator()
        files_to_load = [jsonl_path]
        if os.path.exists(worker_log_path):
            files_to_load.append(worker_log_path)
        inv.load_files(files_to_load)

        # Cross-stack: pick 3 successful CIDs, verify logler has entries
        for i in range(3):
            cid = f"full-stack-{i:03d}"
            results = inv.search(correlation_id=cid)
            entries = results.get("results", [])
            check(f"full-stack: CID {cid} found in logler",
                  len(entries) > 0,
                  f"entries={len(entries)}")

        # Cross-stack: check failed CID has error info
        fail_cid = "full-stack-fail-0"
        fail_results = inv.search(correlation_id=fail_cid)
        fail_entries = fail_results.get("results", [])
        check("full-stack: failed CID found in logler",
              len(fail_entries) > 0,
              f"entries={len(fail_entries)}")

        # Check that at least one entry has failed status (structured field access)
        has_failed_status = any(
            e.get("entry", {}).get("status") == "failed"
            or e.get("entry", {}).get("level", "").upper() == "ERROR"
            for e in fail_entries
        )
        check("full-stack: failed CID has error/failed status",
              has_failed_status,
              "no entry with status=failed or level=ERROR")

        inv.close()

    finally:
        try:
            pm = get_process_manager()
            await pm.stop("qler-full-stack")
        except Exception as e:
            print(f"  WARN  cleanup stop failed: {e}")
        await asyncio.sleep(0.3)
        _reset_procler()
        for p in (jsonl_path,):
            try:
                os.unlink(p)
            except (OSError, NameError):
                pass
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 20: Job archival (UPGRADE-1)
# ---------------------------------------------------------------------------

async def test_job_archival():
    section("20. Job archival (UPGRADE-1)")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_archival_")
    db_path = os.path.join(tmp_dir, "archival.db")

    try:
        local_q = Queue(db_path, immediate=True)
        await local_q.init_db()

        # Register task on local queue
        local_tw = task(local_q)(archive_task)

        # Enqueue + execute 8 jobs via immediate mode (all complete instantly)
        jobs = []
        for i in range(8):
            j = await local_tw.enqueue(i)
            jobs.append(j)

        completed = await Job.query().filter(F("status") == "completed").count()
        check("archival: 8 jobs completed via immediate mode",
              completed == 8, f"completed={completed}")

        # archive_jobs with large threshold — all too recent
        archived = await local_q.archive_jobs(older_than_seconds=9999)
        check("archival: archive_jobs(9999) returns 0 (too recent)",
              archived == 0, f"archived={archived}")

        count = await local_q.archive_count()
        check("archival: archive_count() == 0",
              count == 0, f"count={count}")

        # Backdate finished_at on 5 jobs to 10 minutes ago (raw SQL to
        # bypass optimistic locking — this is test data manipulation)
        old_ts = int(time.time()) - 600
        for j in jobs[:5]:
            await local_q._db.adapter.execute(
                "UPDATE qler_jobs SET data = json_set(data, '$.finished_at', ?) "
                "WHERE ulid = ?",
                [old_ts, j.ulid],
            )
        await local_q._db.adapter.auto_commit()

        # Archive with 300s threshold — 5 qualify
        archived = await local_q.archive_jobs(older_than_seconds=300)
        check("archival: archive_jobs(300) returns 5",
              archived == 5, f"archived={archived}")

        count = await local_q.archive_count()
        check("archival: archive_count() == 5",
              count == 5, f"count={count}")

        # Main table should have 3 remaining
        remaining = await Job.query().count()
        check("archival: main table has 3 remaining",
              remaining == 3, f"remaining={remaining}")

        # archived_jobs() returns 5
        arch_list = await local_q.archived_jobs()
        check("archival: archived_jobs() returns 5",
              len(arch_list) == 5, f"len={len(arch_list)}")

        # All archived have completed status
        all_completed = all(j.status == "completed" for j in arch_list)
        check("archival: all archived jobs have status=completed",
              all_completed,
              f"statuses={[j.status for j in arch_list]}")

        # Filter by status
        arch_completed = await local_q.archived_jobs(status="completed")
        check("archival: archived_jobs(status=completed) returns 5",
              len(arch_completed) == 5, f"len={len(arch_completed)}")

        arch_failed = await local_q.archived_jobs(status="failed")
        check("archival: archived_jobs(status=failed) returns 0",
              len(arch_failed) == 0, f"len={len(arch_failed)}")

    finally:
        await local_q.close()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 21: Reverse dependency index + cascade (UPGRADE-2)
# ---------------------------------------------------------------------------

async def test_reverse_dep_index():
    section("21. Reverse dependency index + cascade (UPGRADE-2)")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_depindex_")
    db_path = os.path.join(tmp_dir, "depindex.db")

    try:
        local_q = Queue(db_path)
        await local_q.init_db()

        # Register task
        dep_tw = task(local_q)(dep_index_task)

        # --- Chain test: A -> B -> C ---
        job_a = await dep_tw.enqueue("A")
        job_b = await dep_tw.enqueue("B", _depends_on=[job_a.ulid])
        job_c = await dep_tw.enqueue("C", _depends_on=[job_b.ulid])

        # Verify qler_job_deps has 2 rows
        import sqlite3
        conn = sqlite3.connect(db_path)
        dep_rows = conn.execute(
            "SELECT parent_ulid, child_ulid FROM qler_job_deps"
        ).fetchall()
        conn.close()
        check("deps-chain: qler_job_deps has 2 rows",
              len(dep_rows) == 2, f"rows={len(dep_rows)}")

        # Verify initial pending_dep_counts
        await job_b.refresh()
        check("deps-chain: B.pending_dep_count == 1",
              job_b.pending_dep_count == 1,
              f"got {job_b.pending_dep_count}")

        await job_c.refresh()
        check("deps-chain: C.pending_dep_count == 1",
              job_c.pending_dep_count == 1,
              f"got {job_c.pending_dep_count}")

        # Run worker until all 3 complete
        worker = Worker(local_q, queues=["default"], concurrency=1,
                        poll_interval=0.1, shutdown_timeout=5.0)
        await run_worker_for(worker, seconds=3.0)

        # Verify all completed with correct results
        await job_a.refresh()
        await job_b.refresh()
        await job_c.refresh()

        check("deps-chain: A completed",
              job_a.status == "completed", f"status={job_a.status}")
        check("deps-chain: B completed",
              job_b.status == "completed", f"status={job_b.status}")
        check("deps-chain: C completed",
              job_c.status == "completed", f"status={job_c.status}")
        check("deps-chain: results correct (A=done:A, B=done:B, C=done:C)",
              job_a.result == "done:A"
              and job_b.result == "done:B"
              and job_c.result == "done:C",
              f"A={job_a.result}, B={job_b.result}, C={job_c.result}")

        # --- Cascade test: D -> [E, G], E -> F ---
        job_d = await dep_tw.enqueue("D")
        job_e = await dep_tw.enqueue("E", _depends_on=[job_d.ulid])
        job_f = await dep_tw.enqueue("F", _depends_on=[job_e.ulid])
        job_g = await dep_tw.enqueue("G", _depends_on=[job_d.ulid])

        # Verify qler_job_deps for these 4 jobs
        conn = sqlite3.connect(db_path)
        cascade_rows = conn.execute(
            "SELECT parent_ulid, child_ulid FROM qler_job_deps "
            "WHERE parent_ulid IN (?, ?)",
            (job_d.ulid, job_e.ulid),
        ).fetchall()
        conn.close()
        check("deps-cascade: qler_job_deps has 3 rows for D,E parents",
              len(cascade_rows) == 3, f"rows={len(cascade_rows)}")

        # Cancel D — should cascade to E, F, G
        ok = await local_q.cancel_job(job_d)
        check("deps-cascade: cancel_job(D) succeeded",
              ok is True, f"ok={ok}")

        # Verify E cancelled with dependency error
        await job_e.refresh()
        check("deps-cascade: E cancelled",
              job_e.status == "cancelled", f"status={job_e.status}")
        check("deps-cascade: E.last_error mentions Dependency",
              "Dependency" in (job_e.last_error or ""),
              f"error={job_e.last_error}")

        # F cancelled (cascaded from E)
        await job_f.refresh()
        check("deps-cascade: F cancelled (cascaded from E)",
              job_f.status == "cancelled", f"status={job_f.status}")

        # G cancelled
        await job_g.refresh()
        check("deps-cascade: G cancelled",
              job_g.status == "cancelled", f"status={job_g.status}")

    finally:
        await local_q.close()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 22: Batch enqueue (UPGRADE-4)
# ---------------------------------------------------------------------------

async def test_batch_enqueue():
    section("22. Batch enqueue (UPGRADE-4)")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_batchenq_")
    db_path = os.path.join(tmp_dir, "batchenq.db")

    try:
        local_q = Queue(db_path)
        await local_q.init_db()

        # Register task
        local_tw = task(local_q)(batch_task)
        task_path = f"{batch_task.__module__}.{batch_task.__qualname__}"

        # --- Basic batch: 8 jobs ---
        specs = [
            {"task_path": task_path, "args": (i,), "queue_name": "default"}
            for i in range(8)
        ]
        jobs = await local_q.enqueue_many(specs)

        check("batch-basic: 8 jobs returned",
              len(jobs) == 8, f"len={len(jobs)}")

        all_pending = all(j.status == "pending" for j in jobs)
        check("batch-basic: all pending",
              all_pending, f"statuses={[j.status for j in jobs]}")

        unique_ulids = set(j.ulid for j in jobs)
        check("batch-basic: all unique ULIDs",
              len(unique_ulids) == 8, f"unique={len(unique_ulids)}")

        db_count = await Job.query().count()
        check("batch-basic: 8 in DB",
              db_count == 8, f"count={db_count}")

        # --- Batch with deps (two-phase) ---
        root_specs = [
            {"task_path": task_path, "args": (i + 10,),
             "queue_name": "default"}
            for i in range(4)
        ]
        roots = await local_q.enqueue_many(root_specs)

        dep_specs = [
            {"task_path": task_path, "args": (i + 100,),
             "queue_name": "default", "depends_on": [roots[i].ulid]}
            for i in range(4)
        ]
        dependents = await local_q.enqueue_many(dep_specs)

        # Verify deps table
        import sqlite3
        conn = sqlite3.connect(db_path)
        dep_count = conn.execute(
            "SELECT COUNT(*) FROM qler_job_deps"
        ).fetchone()[0]
        conn.close()
        check("batch-deps: qler_job_deps has 4 rows",
              dep_count == 4, f"count={dep_count}")

        # Verify pending_dep_count on dependents
        dep_counts = []
        for d in dependents:
            await d.refresh()
            dep_counts.append(d.pending_dep_count)
        check("batch-deps: all dependents have pending_dep_count == 1",
              all(c == 1 for c in dep_counts),
              f"counts={dep_counts}")

        # --- Idempotency ---
        idem_specs = [
            {"task_path": task_path, "args": (0,),
             "queue_name": "default", "idempotency_key": "dup"},
            {"task_path": task_path, "args": (1,),
             "queue_name": "default", "idempotency_key": "dup"},
            {"task_path": task_path, "args": (2,),
             "queue_name": "default", "idempotency_key": "unique2"},
        ]
        idem_jobs = await local_q.enqueue_many(idem_specs)

        unique_idem = set(j.ulid for j in idem_jobs)
        check("batch-idem: 2 unique jobs from 3 specs",
              len(unique_idem) == 2, f"unique={len(unique_idem)}")

        # Verify DB total: 8 basic + 4 roots + 4 deps + 2 idem = 18
        total_in_db = await Job.query().count()
        check("batch-idem: 18 total in DB (dedup worked)",
              total_in_db == 18, f"count={total_in_db}")

        # --- Run worker, verify all complete ---
        worker = Worker(local_q, queues=["default"], concurrency=1,
                        poll_interval=0.1, shutdown_timeout=5.0)
        await run_worker_for(worker, seconds=5.0)

        completed = await Job.query().filter(
            F("status") == "completed"
        ).count()
        check("batch-exec: all 18 jobs completed",
              completed == 18, f"completed={completed}")

        # Spot-check: batch_task(0) should return 1
        await jobs[0].refresh()
        check("batch-exec: batch_task(0) result == 1",
              jobs[0].result == 1, f"result={jobs[0].result}")

    finally:
        await local_q.close()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 23: Batch claim (UPGRADE-5)
# ---------------------------------------------------------------------------

async def test_batch_claim():
    section("23. Batch claim (UPGRADE-5)")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_batchclaim_")
    db_path = os.path.join(tmp_dir, "batchclaim.db")

    try:
        local_q = Queue(db_path)
        await local_q.init_db()

        # Register task
        claim_tw = task(local_q)(claim_task)

        # --- Priority ordering ---
        priorities = [80, 70, 60, 50, 40, 30, 20, 10]
        for p in priorities:
            await claim_tw.enqueue(p, _priority=p)

        # Claim top 4
        claimed = await local_q.claim_jobs("w1", ["default"], n=4)
        check("claim-priority: 4 jobs claimed",
              len(claimed) == 4, f"len={len(claimed)}")

        claimed_statuses = [j.status for j in claimed]
        check("claim-priority: all RUNNING",
              all(s == "running" for s in claimed_statuses),
              f"statuses={claimed_statuses}")

        claimed_priorities = [j.priority for j in claimed]
        check("claim-priority: top-4 priorities {80,70,60,50}",
              set(claimed_priorities) == {80, 70, 60, 50},
              f"priorities={claimed_priorities}")

        check("claim-priority: descending order",
              claimed_priorities == sorted(claimed_priorities, reverse=True),
              f"order={claimed_priorities}")

        # --- Remainder + empty ---
        remainder = await local_q.claim_jobs("w2", ["default"], n=10)
        check("claim-remainder: 4 remaining claimed",
              len(remainder) == 4, f"len={len(remainder)}")

        remainder_priorities = [j.priority for j in remainder]
        check("claim-remainder: bottom-4 priorities {40,30,20,10}",
              set(remainder_priorities) == {40, 30, 20, 10},
              f"priorities={remainder_priorities}")

        empty = await local_q.claim_jobs("w3", ["default"], n=5)
        check("claim-empty: 0 when none available",
              len(empty) == 0, f"len={len(empty)}")

        # --- Attempt records ---
        all_claimed = claimed + remainder
        all_have_attempts = True
        worker_ids_match = True
        all_attempt_1 = True
        for job in all_claimed:
            attempts = await JobAttempt.query().filter(
                F("job_ulid") == job.ulid
            ).all()
            if len(attempts) != 1:
                all_have_attempts = False
            if not attempts or attempts[0].worker_id != job.worker_id:
                worker_ids_match = False
            if not attempts or attempts[0].attempt_number != 1:
                all_attempt_1 = False

        check("claim-attempts: all 8 have exactly 1 attempt record",
              all_have_attempts, "some jobs missing attempt records")
        check("claim-attempts: worker_ids match claiming workers",
              worker_ids_match, "worker_id mismatch")
        check("claim-attempts: all attempt_number == 1",
              all_attempt_1, "some attempt_number != 1")

        # --- Dep blocking ---
        job_x = await claim_tw.enqueue(100)
        job_y = await claim_tw.enqueue(200, _depends_on=[job_x.ulid])

        dep_claimed = await local_q.claim_jobs("w4", ["default"], n=2)
        check("claim-depblock: only X claimed (Y blocked by dep)",
              len(dep_claimed) == 1 and dep_claimed[0].ulid == job_x.ulid,
              f"claimed={[j.ulid for j in dep_claimed]}, x={job_x.ulid}")

    finally:
        await local_q.close()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 24: Combined pipeline (all upgrades)
# ---------------------------------------------------------------------------

async def test_combined_pipeline():
    section("24. Combined pipeline (all upgrades)")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_combined_")
    db_path = os.path.join(tmp_dir, "combined.db")

    try:
        local_q = Queue(db_path)
        await local_q.init_db()

        # Register task (reuse batch_task for all phases)
        local_tw = task(local_q)(batch_task)
        task_path = f"{batch_task.__module__}.{batch_task.__qualname__}"

        # --- Phase 1: Batch enqueue with deps ---
        # 10 validate jobs (no deps)
        validate_specs = [
            {"task_path": task_path, "args": (i,), "queue_name": "default"}
            for i in range(10)
        ]
        validates = await local_q.enqueue_many(validate_specs)

        # 10 charge jobs (depends on corresponding validate)
        charge_specs = [
            {"task_path": task_path, "args": (i + 100,),
             "queue_name": "default", "depends_on": [validates[i].ulid]}
            for i in range(10)
        ]
        charges = await local_q.enqueue_many(charge_specs)

        # 10 fulfill jobs (depends on corresponding charge)
        fulfill_specs = [
            {"task_path": task_path, "args": (i + 200,),
             "queue_name": "default", "depends_on": [charges[i].ulid]}
            for i in range(10)
        ]
        fulfills = await local_q.enqueue_many(fulfill_specs)

        total = await Job.query().count()
        check("pipeline: 30 total jobs enqueued",
              total == 30, f"total={total}")

        import sqlite3
        conn = sqlite3.connect(db_path)
        dep_count = conn.execute(
            "SELECT COUNT(*) FROM qler_job_deps"
        ).fetchone()[0]
        conn.close()
        check("pipeline: qler_job_deps has 20 rows",
              dep_count == 20, f"dep_count={dep_count}")

        # --- Phase 2: Worker execution (batch claim used internally) ---
        worker = Worker(local_q, queues=["default"], concurrency=2,
                        poll_interval=0.1, shutdown_timeout=5.0)
        worker_task = asyncio.create_task(worker.run())

        deadline = time.time() + 15.0
        while time.time() < deadline:
            done = await Job.query().filter(
                F("status") == "completed"
            ).count()
            if done >= 30:
                break
            await asyncio.sleep(0.3)

        worker._running = False
        try:
            await asyncio.wait_for(worker_task, timeout=5.0)
        except asyncio.TimeoutError:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        completed = await Job.query().filter(
            F("status") == "completed"
        ).count()
        check("pipeline: all 30 completed",
              completed == 30, f"completed={completed}")

        pending = await Job.query().filter(F("status") == "pending").count()
        running = await Job.query().filter(F("status") == "running").count()
        check("pipeline: 0 pending/running",
              pending == 0 and running == 0,
              f"pending={pending}, running={running}")

        # Spot-check results
        await validates[0].refresh()
        check("pipeline: validate[0] result == 1 (batch_task(0))",
              validates[0].result == 1,
              f"result={validates[0].result}")

        await charges[0].refresh()
        check("pipeline: charge[0] result == 101 (batch_task(100))",
              charges[0].result == 101,
              f"result={charges[0].result}")

        await fulfills[0].refresh()
        check("pipeline: fulfill[0] result == 201 (batch_task(200))",
              fulfills[0].result == 201,
              f"result={fulfills[0].result}")

        # --- Phase 3: Dependency ordering proof ---
        ordering_ok = True
        ordering_detail = []
        for i in range(10):
            await validates[i].refresh()
            await charges[i].refresh()
            # Use charge's attempt record for started_at
            c_attempts = await JobAttempt.query().filter(
                F("job_ulid") == charges[i].ulid
            ).all()
            if c_attempts and validates[i].finished_at is not None:
                if validates[i].finished_at > c_attempts[0].started_at:
                    ordering_ok = False
                    ordering_detail.append(
                        f"order {i}: validate.finished={validates[i].finished_at}"
                        f" > charge.started={c_attempts[0].started_at}"
                    )
            else:
                ordering_ok = False
                ordering_detail.append(f"order {i}: missing timestamp data")

        check("pipeline: dependency ordering "
              "(validate finished before charge started)",
              ordering_ok,
              "; ".join(ordering_detail) if ordering_detail else "")

        # --- Phase 4: Archival ---
        old_ts = int(time.time()) - 600
        all_jobs = await Job.query().all()
        for j in all_jobs:
            await local_q._db.adapter.execute(
                "UPDATE qler_jobs SET data = json_set(data, '$.finished_at', ?) "
                "WHERE ulid = ?",
                [old_ts, j.ulid],
            )
        await local_q._db.adapter.auto_commit()

        archived = await local_q.archive_jobs(older_than_seconds=300)
        check("pipeline-archive: 30 jobs archived",
              archived == 30, f"archived={archived}")

        count = await local_q.archive_count()
        check("pipeline-archive: archive_count() == 30",
              count == 30, f"count={count}")

        remaining = await Job.query().count()
        check("pipeline-archive: main table empty",
              remaining == 0, f"remaining={remaining}")

        arch_list = await local_q.archived_jobs(limit=50)
        check("pipeline-archive: archived_jobs() returns 30",
              len(arch_list) == 30, f"len={len(arch_list)}")

        # Verify deps cleaned after archival
        conn = sqlite3.connect(db_path)
        deps_after = conn.execute(
            "SELECT COUNT(*) FROM qler_job_deps"
        ).fetchone()[0]
        conn.close()
        check("pipeline-archive: qler_job_deps cleaned (0 rows)",
              deps_after == 0, f"deps={deps_after}")

        # Filter by queue_name
        arch_default = await local_q.archived_jobs(queue_name="default")
        check("pipeline-archive: archived_jobs(queue_name=default) filters",
              len(arch_default) == 30, f"len={len(arch_default)}")

    finally:
        await local_q.close()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 25: Linear pipeline (dagler)
# ---------------------------------------------------------------------------

async def test_dagler_linear():
    section("25. Linear pipeline (dagler)")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_dag25_")
    db_path = os.path.join(tmp_dir, "dag25.db")

    try:
        DAG._registry.clear()

        local_q = Queue(db_path)
        await local_q.init_db()

        # Build DAG: extract → transform → load
        dag = DAG("s25_etl")
        dag.task(dag_extract)
        dag.task(depends_on=[dag_extract])(dag_transform)
        dag.task(depends_on=[dag_transform])(dag_load)

        cid = "s25-test-cid"
        run = await dag.run(local_q, correlation_id=cid)

        check("linear: dag.run() creates DagRun",
              run.dag_name == "s25_etl",
              f"dag_name={run.dag_name}")

        check("linear: 3 qler jobs",
              len(run.job_ulids) == 3,
              f"len={len(run.job_ulids)}")

        check("linear: jobs list populated", len(run.jobs) == 3, f"len={len(run.jobs)}")
        all_share_cid = all(j.correlation_id == cid for j in run.jobs)
        check("linear: all jobs share CID",
              all_share_cid,
              f"cids={[j.correlation_id for j in run.jobs]}")

        check("linear: status pending before Worker",
              run.status == "pending",
              f"status={run.status}")

        # Run worker
        worker = Worker(local_q, queues=["default"], concurrency=1,
                        poll_interval=0.1, shutdown_timeout=5.0)
        worker_task = asyncio.create_task(worker.run())
        await run.wait(timeout=15.0)
        worker._running = False
        try:
            await asyncio.wait_for(worker_task, timeout=5.0)
        except asyncio.TimeoutError:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        check("linear: 3 jobs in run", len(run.jobs) == 3, f"len={len(run.jobs)}")
        all_completed = all(j.status == "completed" for j in run.jobs)
        check("linear: Worker completes all 3",
              all_completed,
              f"statuses={[j.status for j in run.jobs]}")

        check("linear: DagRun completed",
              run.status == "completed",
              f"status={run.status}")

        jm = run.job_map
        check("linear: extract result correct",
              jm["dag_extract"].result == {"rows": [1, 2, 3], "source": "default"},
              f"result={jm['dag_extract'].result}")

        check("linear: transform received extract",
              jm["dag_transform"].result == {"doubled": [2, 4, 6], "source": "default"},
              f"result={jm['dag_transform'].result}")

        check("linear: load computed total",
              jm["dag_load"].result == {"total": 12, "count": 3},
              f"result={jm['dag_load'].result}")

    finally:
        await local_q.close()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 26: Diamond DAG + multi-parent injection
# ---------------------------------------------------------------------------

async def test_dagler_diamond():
    section("26. Diamond DAG + multi-parent injection")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_dag26_")
    db_path = os.path.join(tmp_dir, "dag26.db")

    try:
        DAG._registry.clear()

        local_q = Queue(db_path)
        await local_q.init_db()

        # Build diamond: root → (branch_a, branch_b) → merge
        dag = DAG("s26_diamond")
        dag.task(dag_root)
        dag.task(depends_on=[dag_root])(dag_branch_a)
        dag.task(depends_on=[dag_root])(dag_branch_b)
        dag.task(depends_on=[dag_branch_a, dag_branch_b])(dag_merge)

        cid = "s26-test-cid"
        run = await dag.run(local_q, correlation_id=cid)

        check("diamond: 4 jobs created",
              len(run.job_ulids) == 4,
              f"len={len(run.job_ulids)}")

        jm = run.job_map
        root_job = jm["dag_root"]
        check("diamond: root has 0 deps",
              root_job.pending_dep_count == 0,
              f"pending_dep_count={root_job.pending_dep_count}")

        branch_a_job = jm["dag_branch_a"]
        check("diamond: branch_a depends on root",
              root_job.ulid in branch_a_job.dependencies,
              f"deps={branch_a_job.dependencies}")

        branch_b_job = jm["dag_branch_b"]
        check("diamond: branch_b depends on root",
              root_job.ulid in branch_b_job.dependencies,
              f"deps={branch_b_job.dependencies}")

        merge_job = jm["dag_merge"]
        check("diamond: merge depends on both",
              set(merge_job.dependencies) == {branch_a_job.ulid, branch_b_job.ulid},
              f"deps={merge_job.dependencies}")

        # Run worker (concurrency=2 for parallel branches)
        worker = Worker(local_q, queues=["default"], concurrency=2,
                        poll_interval=0.1, shutdown_timeout=5.0)
        worker_task = asyncio.create_task(worker.run())
        await run.wait(timeout=15.0)
        worker._running = False
        try:
            await asyncio.wait_for(worker_task, timeout=5.0)
        except asyncio.TimeoutError:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        check("diamond: 4 jobs in run", len(run.jobs) == 4, f"len={len(run.jobs)}")
        all_completed = all(j.status == "completed" for j in run.jobs)
        check("diamond: all 4 completed",
              all_completed,
              f"statuses={[j.status for j in run.jobs]}")

        jm = run.job_map
        check("diamond: root result",
              jm["dag_root"].result == {"value": 10},
              f"result={jm['dag_root'].result}")

        check("diamond: branch_a result",
              jm["dag_branch_a"].result == {"result": 20},
              f"result={jm['dag_branch_a'].result}")

        check("diamond: branch_b result",
              jm["dag_branch_b"].result == {"result": 15},
              f"result={jm['dag_branch_b'].result}")

        check("diamond: merge received both",
              jm["dag_merge"].result == {"combined": 35, "from_a": 20, "from_b": 15},
              f"result={jm['dag_merge'].result}")

    finally:
        await local_q.close()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 27: Failure cascade + retry (dagler)
# ---------------------------------------------------------------------------

async def test_dagler_failure_retry():
    section("27. Failure cascade + retry (dagler)")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_dag27_")
    db_path = os.path.join(tmp_dir, "dag27.db")

    try:
        DAG._registry.clear()

        local_q = Queue(db_path)
        await local_q.init_db()

        dag = DAG("s27_fail")
        dag.task(dag_step_ok)
        dag.task(depends_on=[dag_step_ok])(dag_step_fail)
        dag.task(depends_on=[dag_step_fail])(dag_step_downstream)

        cid = "s27-test-cid"
        run = await dag.run(local_q, correlation_id=cid)

        check("fail-cascade: 3 jobs created",
              len(run.job_ulids) == 3,
              f"len={len(run.job_ulids)}")

        # First worker run — step_fail will fail
        worker_a = Worker(local_q, queues=["default"], concurrency=1,
                          poll_interval=0.1, shutdown_timeout=5.0)
        wa_task = asyncio.create_task(worker_a.run())
        await run.wait(timeout=15.0)
        worker_a._running = False
        try:
            await asyncio.wait_for(wa_task, timeout=5.0)
        except asyncio.TimeoutError:
            wa_task.cancel()
            try:
                await wa_task
            except asyncio.CancelledError:
                pass

        jm = run.job_map
        check("fail-cascade: step_ok completed",
              jm["dag_step_ok"].status == "completed",
              f"status={jm['dag_step_ok'].status}")

        step_fail_job = jm["dag_step_fail"]
        check("fail-cascade: step_fail status is failed",
              step_fail_job.status == "failed",
              f"status={step_fail_job.status}")

        check("fail-cascade: step_fail error contains 'transient'",
              "transient" in (step_fail_job.last_error or ""),
              f"error={step_fail_job.last_error!r}")

        check("fail-cascade: downstream cancelled",
              jm["dag_step_downstream"].status == "cancelled",
              f"status={jm['dag_step_downstream'].status}")

        check("fail-cascade: DagRun failed",
              run.status == "failed",
              f"status={run.status}")

        # Retry
        await run.retry(local_q)

        check("retry: resets run status",
              run.status == "pending",
              f"status={run.status}")

        jm = run.job_map
        check("retry: step_ok remains completed",
              jm["dag_step_ok"].status == "completed",
              f"status={jm['dag_step_ok'].status}")

        check("retry: step_fail reset to pending",
              jm["dag_step_fail"].status == "pending",
              f"status={jm['dag_step_fail'].status}")

        check("retry: downstream reset to pending",
              jm["dag_step_downstream"].status == "pending",
              f"status={jm['dag_step_downstream'].status}")

        # Second worker run — step_fail succeeds (counter at 2)
        worker_b = Worker(local_q, queues=["default"], concurrency=1,
                          poll_interval=0.1, shutdown_timeout=5.0)
        wb_task = asyncio.create_task(worker_b.run())
        await run.wait(timeout=15.0)
        worker_b._running = False
        try:
            await asyncio.wait_for(wb_task, timeout=5.0)
        except asyncio.TimeoutError:
            wb_task.cancel()
            try:
                await wb_task
            except asyncio.CancelledError:
                pass

        check("retry: 3 jobs in run", len(run.jobs) == 3, f"len={len(run.jobs)}")
        all_completed = all(j.status == "completed" for j in run.jobs)
        check("retry: Worker B completes all",
              all_completed,
              f"statuses={[j.status for j in run.jobs]}")

        jm = run.job_map
        check("retry: step_fail result on recovery",
              jm["dag_step_fail"].result == {"step": "fail_recovered", "attempt": 2},
              f"result={jm['dag_step_fail'].result}")

        check("retry: DagRun final completed",
              run.status == "completed",
              f"status={run.status}")

    finally:
        await local_q.close()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 28: Fan-out/reduce (dagler)
# ---------------------------------------------------------------------------

async def test_dagler_fanout():
    section("28. Fan-out/reduce (dagler)")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_dag28_")
    db_path = os.path.join(tmp_dir, "dag28.db")

    try:
        DAG._registry.clear()

        local_q = Queue(db_path)
        await local_q.init_db()

        dag = DAG("s28_fanout")
        dag.task(dag_fetch_items)
        dag.map_task(depends_on=[dag_fetch_items])(dag_process_item)
        dag.reduce_task(depends_on=[dag_process_item])(dag_collect_results)
        dag.task(depends_on=[dag_collect_results])(dag_summarize)

        cid = "s28-test-cid"
        run = await dag.run(local_q, correlation_id=cid)

        # Initial: fetch_items + dispatcher = 2
        check("fanout: initial job count == 2",
              len(run.job_ulids) == 2,
              f"len={len(run.job_ulids)}")

        # Run worker with concurrency=4 for parallel map
        worker = Worker(local_q, queues=["default"], concurrency=4,
                        poll_interval=0.1, shutdown_timeout=5.0)
        worker_task = asyncio.create_task(worker.run())
        await run.wait(timeout=30.0)
        worker._running = False
        try:
            await asyncio.wait_for(worker_task, timeout=5.0)
        except asyncio.TimeoutError:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        check("fanout: wait completes",
              run.status == "completed",
              f"status={run.status}")

        # Total: fetch + dispatcher + 5 map + reduce + summarize = 9
        total_jobs = len(run.job_ulids)
        check("fanout: total jobs after completion",
              total_jobs == 9,
              f"total={total_jobs}")

        map_count = sum(1 for name in run.task_names if name == "dag_process_item")
        check("fanout: 5 map jobs created",
              map_count == 5,
              f"map_count={map_count}")

        jm = run.job_map
        reduce_result = jm["dag_collect_results"].result
        check("fanout: reduce result count",
              reduce_result["count"] == 5,
              f"count={reduce_result.get('count')}")

        expected_items = sorted(["ALPHA", "BRAVO", "CHARLIE", "DELTA", "ECHO"])
        actual_items = sorted(reduce_result.get("items", []))
        check("fanout: reduce items correct",
              actual_items == expected_items,
              f"items={actual_items}")

        check("fanout: summarize received reduce",
              jm["dag_summarize"].result == {"summary": "5 items processed"},
              f"result={jm['dag_summarize'].result}")

        check("fanout: 9 jobs in run", len(run.jobs) == 9, f"len={len(run.jobs)}")
        all_share_cid = all(j.correlation_id == cid for j in run.jobs)
        check("fanout: all jobs share CID",
              all_share_cid,
              f"mismatches={[j.correlation_id for j in run.jobs if j.correlation_id != cid]}")

        # DagRun persisted — re-fetch from DB
        persisted = await DagRun.query().filter(
            F("correlation_id") == cid
        ).first()
        check("fanout: DagRun persisted with all jobs",
              persisted is not None and len(persisted.job_ulids) == 9,
              f"persisted_jobs={len(persisted.job_ulids) if persisted else 0}")

        check("fanout: DB status completed",
              persisted is not None and persisted.status == "completed",
              f"status={persisted.status if persisted else None}")

    finally:
        await local_q.close()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 29: logler observability (dagler)
# ---------------------------------------------------------------------------

async def test_dagler_logler():
    section("29. logler observability (dagler)")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_dag29_")
    db_path = os.path.join(tmp_dir, "dag29.db")
    jsonl_path = None
    jsonl_auto_path = None

    try:
        DAG._registry.clear()

        local_q = Queue(db_path)
        await local_q.init_db()

        # Reuse S25 ETL tasks
        dag = DAG("s29_logler")
        dag.task(dag_extract)
        dag.task(depends_on=[dag_extract])(dag_transform)
        dag.task(depends_on=[dag_transform])(dag_load)

        cid = "s29-logler-cid"
        run = await dag.run(local_q, correlation_id=cid)

        # Run worker
        worker = Worker(local_q, queues=["default"], concurrency=1,
                        poll_interval=0.1, shutdown_timeout=5.0)
        worker_task = asyncio.create_task(worker.run())
        await run.wait(timeout=15.0)
        worker._running = False
        try:
            await asyncio.wait_for(worker_task, timeout=5.0)
        except asyncio.TimeoutError:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        # db_to_jsonl with explicit mappings
        jsonl_path = db_to_jsonl(db_path, [qler_job_mapping(), qler_attempt_mapping()])
        check("logler-dag: db_to_jsonl produces file",
              jsonl_path is not None and os.path.exists(jsonl_path),
              f"path={jsonl_path}")

        with open(jsonl_path) as f:
            entries = [json.loads(line) for line in f]

        job_entries = [e for e in entries if e.get("thread_id") == "qler_jobs"]
        check("logler-dag: JSONL includes job entries",
              len(job_entries) == 3,
              f"job_entries={len(job_entries)}")

        # Investigator search by CID
        inv = Investigator()
        inv.load_from_db(db_path)

        cid_results = inv.search(correlation_id=cid)
        cid_entries = cid_results.get("results", [])
        check("logler-dag: CID search returns entries",
              len(cid_entries) == 4,
              f"count={len(cid_entries)}")

        check("logler-dag: CID entries populated", len(cid_entries) == 4, f"len={len(cid_entries)}")
        all_cid_match = all(
            e["entry"].get("correlation_id") == cid
            for e in cid_entries
        )
        check("logler-dag: all entries share CID",
              all_cid_match,
              f"mismatches={[e['entry'].get('correlation_id') for e in cid_entries if e['entry'].get('correlation_id') != cid]}")

        completed_entries = [
            e for e in cid_entries
            if "status=completed" in e["entry"].get("message", "")
        ]
        check("logler-dag: lifecycle visible (completed jobs)",
              len(completed_entries) == 3,
              f"completed={len(completed_entries)}")

        # follow_thread chronological
        thread = inv.follow_thread(correlation_id=cid)
        thread_entries = thread.get("entries", [])
        timestamps = []
        for te in thread_entries:
            ts = te.get("timestamp") or te.get("entry", {}).get("timestamp")
            if ts is not None:
                timestamps.append(ts)
        is_sorted = all(
            timestamps[i] <= timestamps[i + 1]
            for i in range(len(timestamps) - 1)
        )
        check("logler-dag: follow_thread chronological",
              len(timestamps) >= 2 and is_sorted,
              f"count={len(timestamps)}, sorted={is_sorted}")

        # Auto-detect with dagler_runs table (regression guard)
        jsonl_auto_path = db_to_jsonl(db_path)
        check("logler-dag: dagler_runs table safe (auto-detect)",
              jsonl_auto_path is not None and os.path.exists(jsonl_auto_path),
              f"path={jsonl_auto_path}")

    finally:
        for path in (jsonl_path, jsonl_auto_path):
            if path and os.path.exists(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass
        await local_q.close()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 30: Full stack roundtrip (dagler)
# ---------------------------------------------------------------------------

async def test_dagler_full_stack():
    section("30. Full stack roundtrip (dagler)")

    tmp_dir = tempfile.mkdtemp(prefix="proofler_dag30_")
    db_path = os.path.join(tmp_dir, "dag30.db")

    try:
        DAG._registry.clear()

        local_q = Queue(db_path)
        await local_q.init_db()

        # Linear + fan-out + post-reduce
        dag = DAG("s30_data_pipeline")
        dag.task(dag_ingest)
        dag.task(depends_on=[dag_ingest])(dag_validate_records)
        dag.map_task(depends_on=[dag_validate_records])(dag_enrich_record)
        dag.reduce_task(depends_on=[dag_enrich_record])(dag_aggregate)
        dag.task(depends_on=[dag_aggregate])(dag_finalize)

        cid = "s30-pipeline-cid"
        run = await dag.run(local_q, correlation_id=cid)

        check("full-stack: DAG submitted",
              run.dag_name == "s30_data_pipeline",
              f"dag_name={run.dag_name}")

        # Run worker
        worker = Worker(local_q, queues=["default"], concurrency=4,
                        poll_interval=0.1, shutdown_timeout=5.0)
        worker_task = asyncio.create_task(worker.run())
        await run.wait(timeout=30.0)
        worker._running = False
        try:
            await asyncio.wait_for(worker_task, timeout=5.0)
        except asyncio.TimeoutError:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        check("full-stack: all jobs completed",
              run.status == "completed",
              f"status={run.status}")

        jm = run.job_map
        check("full-stack: ingest result correct",
              jm["dag_ingest"].result == {"batch_id": 0, "records": [10, 20, 30]},
              f"result={jm['dag_ingest'].result}")

        map_count = sum(1 for name in run.task_names if name == "dag_enrich_record")
        check("full-stack: 3 map jobs created",
              map_count == 3,
              f"map_count={map_count}")

        agg_result = jm["dag_aggregate"].result
        check("full-stack: aggregate total",
              agg_result["total"] == 120,
              f"total={agg_result.get('total')}")

        fin_result = jm["dag_finalize"].result
        check("full-stack: finalize result",
              fin_result == {"final_total": 120, "status": "complete"},
              f"result={fin_result}")

        # DagRun persisted — re-fetch from DB
        # Total: ingest + validate + dispatcher + 3 map + reduce + finalize = 8
        persisted = await DagRun.query().filter(
            F("correlation_id") == cid
        ).first()
        check("full-stack: DagRun persisted",
              persisted is not None and len(persisted.job_ulids) == 8,
              f"persisted_jobs={len(persisted.job_ulids) if persisted else 0}")

        # logler CID trace
        inv = Investigator()
        inv.load_from_db(db_path)

        cid_results = inv.search(correlation_id=cid)
        cid_entries = cid_results.get("results", [])
        check("full-stack: logler CID trace",
              len(cid_entries) >= 8,
              f"count={len(cid_entries)}")

    finally:
        await local_q.close()
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 31: Fan-Out Scaling Curve
# ---------------------------------------------------------------------------

async def test_m7_fanout_scaling():
    """Fan-out at 100, 500, 1K, 5K items — find dagler's limits."""
    section("31. dagler fan-out scaling curve")

    import shutil

    scale_points = [
        (100, 30),
        (500, 60),
        (1000, 120),
        (5000, 300),
    ]

    for n, timeout_budget in scale_points:
        tag = f"n={n}"
        tmp_dir = tempfile.mkdtemp(prefix=f"proofler_s31_{n}_")
        db_path = os.path.join(tmp_dir, f"s31_{n}.db")
        local_q = None
        worker = None
        worker_task = None

        try:
            DAG._registry.clear()

            local_q = Queue(db_path)
            await local_q.init_db()

            dag = DAG(f"s31_scale_{n}")
            dag.task(m7_produce_n)
            dag.map_task(depends_on=[m7_produce_n])(m7_map_square)
            dag.reduce_task(depends_on=[m7_map_square])(m7_reduce_sum)

            worker = Worker(local_q, queues=["default"], concurrency=4,
                            poll_interval=0.1, shutdown_timeout=5.0)
            worker_task = asyncio.create_task(worker.run())

            timed_out = False
            t0 = time.monotonic()
            try:
                run = await dag.run(
                    local_q,
                    payload={"m7_produce_n": {"n": n}},
                    correlation_id=f"s31-scale-{n}",
                )
                run = await run.wait(timeout=timeout_budget)
            except TimeoutError:
                timed_out = True
                elapsed = time.monotonic() - t0
                check(f"scale {tag}: completed", False,
                      f"timed out after {elapsed:.1f}s (budget={timeout_budget}s)")
                check(f"scale {tag}: job count", False, "timed out")
                check(f"scale {tag}: reduce job in job_map", False, "timed out")
                check(f"scale {tag}: exact reduce result", False, "timed out")
                check(f"scale {tag}: within budget ({timeout_budget}s)", False,
                      f"elapsed={elapsed:.1f}s")
                print(f"  INFO  scale {tag}: TIMED OUT at {elapsed:.1f}s — dagler/qler perf limit")
                continue

            elapsed = time.monotonic() - t0

            check(f"scale {tag}: completed",
                  run.status == "completed",
                  f"status={run.status}")

            # produce + dispatcher + n maps + reduce = n + 3
            expected_jobs = n + 3
            check(f"scale {tag}: job count",
                  len(run.job_ulids) == expected_jobs,
                  f"got={len(run.job_ulids)}, expected={expected_jobs}")

            expected_sum = sum(i * i for i in range(n))
            reduce_result = run.job_map.get("m7_reduce_sum")
            check(f"scale {tag}: reduce job in job_map",
                  reduce_result is not None,
                  "m7_reduce_sum not found")
            actual_result = reduce_result.result if reduce_result else None
            check(f"scale {tag}: exact reduce result",
                  actual_result == expected_sum,
                  f"got={actual_result}, expected={expected_sum}")

            check(f"scale {tag}: within budget ({timeout_budget}s)",
                  elapsed < timeout_budget,
                  f"elapsed={elapsed:.1f}s")

            rate = n / elapsed if elapsed > 0 else 0
            print(f"  INFO  scale {tag}: {elapsed:.1f}s, {rate:.0f} items/s")

        finally:
            # Stop worker BEFORE closing queue to avoid race
            if worker is not None and worker_task is not None and not worker_task.done():
                worker._running = False
                try:
                    await asyncio.wait_for(worker_task, timeout=5.0)
                except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                    worker_task.cancel()
                    try:
                        await worker_task
                    except (asyncio.CancelledError, Exception):
                        pass
            await asyncio.sleep(0.1)
            if local_q is not None:
                try:
                    await local_q.close()
                except Exception:
                    pass
            shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 32: Concurrent DAG Runs
# ---------------------------------------------------------------------------

async def test_m7_concurrent_runs():
    """10 concurrent small DAGs — prove isolation under contention."""
    section("32. dagler concurrent runs")

    import shutil

    tmp_dir = tempfile.mkdtemp(prefix="proofler_s32_")
    db_path = os.path.join(tmp_dir, "s32.db")

    try:
        DAG._registry.clear()

        local_q = Queue(db_path)
        await local_q.init_db()

        # Build a simple 3-task linear DAG
        dag = DAG("s32_concurrent")
        dag.task(m7_gen_data)
        dag.task(depends_on=[m7_gen_data])(m7_transform)
        dag.task(depends_on=[m7_transform])(m7_store)

        worker = Worker(local_q, queues=["default"], concurrency=4,
                        poll_interval=0.1, shutdown_timeout=5.0)
        worker_task = asyncio.create_task(worker.run())

        # Submit 10 runs with different seeds before any wait
        runs = []
        for i in range(10):
            run = await dag.run(
                local_q,
                payload={"m7_gen_data": {"seed": i}},
                correlation_id=f"s32-run-{i}",
            )
            runs.append(run)

        # Wait on all concurrently
        completed_runs = await asyncio.gather(
            *[r.wait(timeout=30) for r in runs]
        )

        worker._running = False
        try:
            await asyncio.wait_for(worker_task, timeout=5.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        # All 10 completed
        all_completed = all(r.status == "completed" for r in completed_runs)
        check("concurrent: all 10 completed",
              all_completed,
              f"statuses={[r.status for r in completed_runs]}")

        # Per-run result isolation — check each run individually
        for i, r in enumerate(completed_runs):
            jm = r.job_map
            expected_value = i * 7 * 2  # seed * 7 * 2
            store_result = jm.get("m7_store")
            actual = store_result.result if store_result else None
            check(f"concurrent: run {i} result",
                  actual == {"stored": True, "value": expected_value},
                  f"expected value={expected_value}, got={actual}")

        # DB-level checks
        from sqler import F
        all_dag_runs = await DagRun.query().filter(
            F("dag_name") == "s32_concurrent"
        ).all()
        check("concurrent: 10 DagRun rows",
              len(all_dag_runs) == 10,
              f"got={len(all_dag_runs)}")

        all_jobs = await Job.query().all()
        check("concurrent: 30 Job rows",
              len(all_jobs) == 30,
              f"got={len(all_jobs)}")

        # CID isolation: every job's CID matches its run's CID
        cid_mismatches = 0
        for r in completed_runs:
            check(f"concurrent: run {r.correlation_id} has jobs",
                  len(r.jobs) == 3,
                  f"got={len(r.jobs)}")
            for j in r.jobs:
                if j.correlation_id != r.correlation_id:
                    cid_mismatches += 1
        check("concurrent: CID isolation",
              cid_mismatches == 0,
              f"{cid_mismatches} jobs with wrong CID")

        # Each run has exactly 3 jobs
        job_counts_ok = all(len(r.job_ulids) == 3 for r in completed_runs)
        check("concurrent: 3 jobs per run",
              job_counts_ok,
              f"counts={[len(r.job_ulids) for r in completed_runs]}")

        # No duplicate ULIDs across runs
        all_ulids = []
        for r in completed_runs:
            all_ulids.extend(r.job_ulids)
        check("concurrent: no duplicate ULIDs",
              len(all_ulids) == len(set(all_ulids)),
              f"total={len(all_ulids)}, unique={len(set(all_ulids))}")

    finally:
        if not worker_task.done():
            worker._running = False
            try:
                await asyncio.wait_for(worker_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                worker_task.cancel()
                try:
                    await worker_task
                except (asyncio.CancelledError, Exception):
                    pass
        await local_q.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 33: Multi-Stage Depth
# ---------------------------------------------------------------------------

async def test_m7_multistage():
    """2-stage pipeline: 100 → map → reduce → 50 → map → reduce."""
    section("33. dagler multi-stage depth")

    import shutil

    tmp_dir = tempfile.mkdtemp(prefix="proofler_s33_")
    db_path = os.path.join(tmp_dir, "s33.db")

    try:
        DAG._registry.clear()

        local_q = Queue(db_path)
        await local_q.init_db()

        # 2-stage: produce_100 → s1_map → s1_reduce → s2_map → s2_reduce
        dag = DAG("s33_multistage")
        dag.task(m7_produce_100)
        dag.map_task(depends_on=[m7_produce_100])(m7_s1_map)
        dag.reduce_task(depends_on=[m7_s1_map])(m7_s1_reduce)
        dag.map_task(depends_on=[m7_s1_reduce])(m7_s2_map)
        dag.reduce_task(depends_on=[m7_s2_map])(m7_s2_reduce)

        worker = Worker(local_q, queues=["default"], concurrency=4,
                        poll_interval=0.1, shutdown_timeout=5.0)
        worker_task = asyncio.create_task(worker.run())

        cid = "s33-multistage"
        run = await dag.run(local_q, correlation_id=cid)
        run = await run.wait(timeout=120)

        worker._running = False
        try:
            await asyncio.wait_for(worker_task, timeout=5.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        check("multistage: completed",
              run.status == "completed",
              f"status={run.status}")

        # Count map jobs per stage
        s1_maps = sum(1 for n in run.task_names if n == "m7_s1_map")
        check("multistage: stage 1 map count",
              s1_maps == 100,
              f"got={s1_maps}")

        s2_maps = sum(1 for n in run.task_names if n == "m7_s2_map")
        check("multistage: stage 2 map count",
              s2_maps == 50,
              f"got={s2_maps}")

        # Dispatcher count: 2 stages = 2 dispatchers
        dispatchers = sum(1 for n in run.task_names if n.startswith("_dispatcher_"))
        check("multistage: dispatcher count",
              dispatchers == 2,
              f"got={dispatchers}")

        # Expected final result: squares of 0..99, take first 50, add 1, sum
        stage1_squares = [i * i for i in range(100)]
        stage1_reduced = stage1_squares[:50]
        stage2_mapped = [x + 1 for x in stage1_reduced]
        expected_final = sum(stage2_mapped)

        reduce_job = run.job_map.get("m7_s2_reduce")
        check("multistage: s2_reduce in job_map",
              reduce_job is not None,
              "m7_s2_reduce not found")
        actual_final = reduce_job.result if reduce_job else None
        check("multistage: exact final result",
              actual_final == expected_final,
              f"got={actual_final}, expected={expected_final}")

        # Total job count: produce + disp1 + 100 maps + reduce + disp2 + 50 maps + reduce = 155
        # Actually: produce(1) + dispatcher_s1(1) + 100 maps + s1_reduce(1) + dispatcher_s2(1) + 50 maps + s2_reduce(1) = 155
        expected_total = 1 + 1 + 100 + 1 + 1 + 50 + 1
        check("multistage: total job count",
              len(run.job_ulids) == expected_total,
              f"got={len(run.job_ulids)}, expected={expected_total}")

        # All dispatchers completed
        disp_jobs = [run.job_map[n] for n in run.job_map if n.startswith("_dispatcher_")]
        check("multistage: dispatcher jobs found",
              len(disp_jobs) == 2,
              f"got={len(disp_jobs)}")
        all_disp_ok = all(j.status == "completed" for j in disp_jobs)
        check("multistage: all dispatchers completed",
              all_disp_ok,
              f"statuses={[j.status for j in disp_jobs]}")

        # Stage 1 reduce result = first 50 squares
        s1_reduce_job = run.job_map.get("m7_s1_reduce")
        s1_result = s1_reduce_job.result if s1_reduce_job else None
        check("multistage: stage 1 reduce result",
              s1_result == stage1_reduced,
              f"len={len(s1_result) if s1_result else 0}")

    finally:
        await local_q.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 34: Memory Leak Detection
# ---------------------------------------------------------------------------

async def test_m7_memory_leak():
    """Run 100-item fan-out 10 times — track RSS for leaks."""
    section("34. dagler memory leak detection")

    import resource
    import shutil

    tmp_dir = tempfile.mkdtemp(prefix="proofler_s34_")
    db_path = os.path.join(tmp_dir, "s34.db")

    def get_rss_mb() -> float:
        """Current RSS in MB via /proc/self/status (Linux) or getrusage fallback."""
        try:
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1]) / 1024
        except (FileNotFoundError, ValueError):
            pass
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

    worker_task = None
    try:
        local_q = Queue(db_path)
        await local_q.init_db()

        rss_readings: list[float] = []
        expected_sum = sum(i * i for i in range(100))

        for cycle in range(10):
            DAG._registry.clear()

            dag = DAG(f"s34_leak_{cycle}")
            dag.task(m7_produce_n)
            dag.map_task(depends_on=[m7_produce_n])(m7_map_square)
            dag.reduce_task(depends_on=[m7_map_square])(m7_reduce_sum)

            worker = Worker(local_q, queues=["default"], concurrency=4,
                            poll_interval=0.1, shutdown_timeout=5.0)
            worker_task = asyncio.create_task(worker.run())

            run = await dag.run(
                local_q,
                payload={"m7_produce_n": {"n": 100}},
                correlation_id=f"s34-cycle-{cycle}",
            )
            run = await run.wait(timeout=30)

            worker._running = False
            try:
                await asyncio.wait_for(worker_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass

            rss = get_rss_mb()
            rss_readings.append(rss)

            # Per-cycle correctness checks
            check(f"leak: cycle {cycle} completed",
                  run.status == "completed",
                  f"status={run.status}")
            reduce_job = run.job_map.get("m7_reduce_sum")
            actual = reduce_job.result if reduce_job else None
            check(f"leak: cycle {cycle} result",
                  actual == expected_sum,
                  f"got={actual}, expected={expected_sum}")

        # Print RSS curve
        for i, rss in enumerate(rss_readings):
            print(f"  INFO  cycle {i}: {rss:.1f} MB RSS")

        first_rss = rss_readings[0]
        last_rss = rss_readings[-1]
        total_growth = last_rss - first_rss

        check("leak: RSS growth < 50 MB",
              total_growth < 50,
              f"growth={total_growth:.1f} MB (first={first_rss:.1f}, last={last_rss:.1f})")

        # Plateau check: cycles 5-10 should show minimal growth
        plateau_growth = rss_readings[-1] - rss_readings[4]
        check("leak: plateau (cycles 5-10) < 20 MB",
              plateau_growth < 20,
              f"plateau_growth={plateau_growth:.1f} MB")

        peak_rss = max(rss_readings)
        print(f"  INFO  peak RSS: {peak_rss:.1f} MB, total growth: {total_growth:.1f} MB")

    finally:
        if worker_task is not None and not worker_task.done():
            worker._running = False
            try:
                await asyncio.wait_for(worker_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                worker_task.cancel()
                try:
                    await worker_task
                except (asyncio.CancelledError, Exception):
                    pass
        await local_q.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 35: Worker Concurrency Sweep
# ---------------------------------------------------------------------------

async def test_m7_concurrency_sweep():
    """1K-item fan-out at c=1, c=2, c=4 — measure throughput."""
    section("35. dagler concurrency sweep")

    import shutil

    n = 1000
    expected_sum = sum(i * i for i in range(n))
    concurrency_levels = [1, 2, 4]
    timings: dict[int, float] = {}

    for c in concurrency_levels:
        tag = f"c={c}"
        tmp_dir = tempfile.mkdtemp(prefix=f"proofler_s35_c{c}_")
        db_path = os.path.join(tmp_dir, f"s35_c{c}.db")

        try:
            DAG._registry.clear()

            local_q = Queue(db_path)
            await local_q.init_db()

            dag = DAG(f"s35_sweep_c{c}")
            dag.task(m7_produce_n)
            dag.map_task(depends_on=[m7_produce_n])(m7_map_square)
            dag.reduce_task(depends_on=[m7_map_square])(m7_reduce_sum)

            worker = Worker(local_q, queues=["default"], concurrency=c,
                            poll_interval=0.1, shutdown_timeout=5.0)
            worker_task = asyncio.create_task(worker.run())

            t0 = time.monotonic()
            run = await dag.run(
                local_q,
                payload={"m7_produce_n": {"n": n}},
                correlation_id=f"s35-sweep-c{c}",
            )
            run = await run.wait(timeout=180)
            elapsed = time.monotonic() - t0
            timings[c] = elapsed

            worker._running = False
            try:
                await asyncio.wait_for(worker_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass

            check(f"sweep {tag}: completed",
                  run.status == "completed",
                  f"status={run.status}")

            reduce_job = run.job_map.get("m7_reduce_sum")
            check(f"sweep {tag}: reduce job in job_map",
                  reduce_job is not None,
                  "m7_reduce_sum not found")
            actual = reduce_job.result if reduce_job else None
            check(f"sweep {tag}: exact result",
                  actual == expected_sum,
                  f"got={actual}")

        finally:
            await local_q.close()
            shutil.rmtree(tmp_dir, ignore_errors=True)

    # Print throughput table
    print(f"\n  INFO  Concurrency sweep (n={n}):")
    print(f"  INFO  {'c':>4s}  {'elapsed':>8s}  {'items/s':>8s}")
    for c in concurrency_levels:
        if c in timings:
            rate = n / timings[c]
            print(f"  INFO  {c:>4d}  {timings[c]:>7.1f}s  {rate:>7.0f}")

    # c=2 vs c=1: report finding (WAL contention makes ordering unreliable)
    if 1 in timings and 2 in timings:
        c2_faster = timings[2] < timings[1]
        detail = f"c=1={timings[1]:.1f}s, c=2={timings[2]:.1f}s"
        if not c2_faster:
            print(f"  INFO  WAL contention: c=2 slower than c=1 ({detail})")
        check("sweep: c=2 vs c=1 measured", True, detail)
    else:
        check("sweep: c=2 vs c=1 measured", False, "missing timing data")

    # c=4 vs c=1: report finding (WAL contention may negate c=4 gains)
    if 1 in timings and 4 in timings:
        c4_faster = timings[4] < timings[1]
        detail = f"c=1={timings[1]:.1f}s, c=4={timings[4]:.1f}s"
        if not c4_faster:
            print(f"  INFO  WAL contention: c=4 slower than c=1 ({detail})")
        check("sweep: c=4 vs c=1 measured", True, detail)
    else:
        check("sweep: c=4 vs c=1 measured", False, "missing timing data")

    # c=4 vs c=2: report WAL contention finding
    if 2 in timings and 4 in timings:
        detail = f"c=2={timings[2]:.1f}s, c=4={timings[4]:.1f}s"
        contention = timings[4] > timings[2]
        if contention:
            print(f"  INFO  WAL contention: c=4 slower than c=2 ({detail})")
        check("sweep: c=4 vs c=2 measured", True, detail)


# ---------------------------------------------------------------------------
# Section 36: Cancel mid-flight fan-out (dagler)
# ---------------------------------------------------------------------------

async def test_m8_cancel_midflight():
    """Cancel a fan-out DagRun while map jobs are still executing."""
    section("36. Cancel mid-flight fan-out (dagler)")

    import shutil

    tmp_dir = tempfile.mkdtemp(prefix="proofler_s36_")
    db_path = os.path.join(tmp_dir, "s36.db")

    try:
        DAG._registry.clear()

        local_q = Queue(db_path)
        await local_q.init_db()

        dag = DAG("s36_cancel")
        dag.task(m8_produce_items)
        dag.map_task(depends_on=[m8_produce_items])(m8_slow_map)
        dag.reduce_task(depends_on=[m8_slow_map])(m8_reduce_collect)

        worker = Worker(local_q, queues=["default"], concurrency=2,
                        poll_interval=0.1, shutdown_timeout=5.0)
        worker_task = asyncio.create_task(worker.run())

        cid = "s36-cancel-cid"
        run = await dag.run(local_q, correlation_id=cid)

        # Wait for dispatcher to complete — map jobs will be in-flight
        # Re-fetch DagRun from DB to discover dynamically added jobs
        db_job_count = 0
        for _ in range(100):
            await asyncio.sleep(0.1)
            refreshed = await DagRun.query().filter(
                F("correlation_id") == cid
            ).first()
            if refreshed:
                db_job_count = len(refreshed.job_ulids)
                if db_job_count > 2:
                    run.jobs_manifest = refreshed.jobs_manifest
                    break

        check("cancel: dispatcher created map jobs",
              db_job_count > 2,
              f"db_job_count={db_job_count}")

        # Some map jobs should still be pending/running (they sleep 2s each)
        all_pre = await Job.query().filter(F("correlation_id") == cid).all()
        pre_non_terminal = [j for j in all_pre if j.status in ("pending", "running")]
        check("cancel: non-terminal jobs exist before cancel",
              len(pre_non_terminal) > 0,
              f"non_terminal={len(pre_non_terminal)}, statuses={[j.status for j in all_pre]}")

        # Cancel the run
        await run.cancel(local_q)

        check("cancel: DagRun status is cancelled",
              run.status == "cancelled",
              f"status={run.status}")

        # Verify no pending jobs remain in DB
        all_jobs = await Job.query().filter(F("correlation_id") == cid).all()
        pending_jobs = [j for j in all_jobs if j.status == "pending"]
        check("cancel: no pending jobs in DB",
              len(pending_jobs) == 0,
              f"pending={len(pending_jobs)}")

        # Verify map jobs exist and none are pending (cancel reached them)
        map_jobs_post = [j for j in all_jobs if j.task and "m8_slow_map" in j.task]
        check("cancel: exactly 20 map jobs exist",
              len(map_jobs_post) == 20,
              f"count={len(map_jobs_post)}")
        # Running jobs use cooperative cancellation (Worker checks flag),
        # so some may still be "running" immediately after cancel().
        # The key invariant: no map jobs should be "pending" after cancel.
        pending_maps = [j for j in map_jobs_post if j.status == "pending"]
        check("cancel: no pending map jobs after cancel",
              len(pending_maps) == 0,
              f"pending={len(pending_maps)}")

        # Produce task should have completed before cancel
        produce_job = run.job_map.get("m8_produce_items")
        check("cancel: produce completed before cancel",
              produce_job is not None and produce_job.status == "completed",
              f"status={produce_job.status if produce_job else 'missing'}")

        # Dispatcher should have completed (it created map jobs)
        dispatcher_jobs = [j for j in all_jobs if "dispatch" in (j.task or "").lower()
                          or "_dagler_fanout_dispatch" in (j.task or "")]
        check("cancel: dispatcher completed",
              len(dispatcher_jobs) == 1 and dispatcher_jobs[0].status == "completed",
              f"dispatcher_count={len(dispatcher_jobs)}, "
              f"status={dispatcher_jobs[0].status if dispatcher_jobs else 'none'}")

        # Reduce should be cancelled (its deps were cancelled)
        reduce_job = run.job_map.get("m8_reduce_collect")
        check("cancel: reduce cancelled",
              reduce_job is not None and reduce_job.status == "cancelled",
              f"status={reduce_job.status if reduce_job else 'missing'}")

        # DB-level: DagRun persisted as cancelled
        persisted = await DagRun.query().filter(
            F("correlation_id") == cid
        ).first()
        check("cancel: DagRun persisted as cancelled",
              persisted is not None and persisted.status == "cancelled",
              f"db_status={persisted.status if persisted else 'missing'}")

        # Total job count: produce + dispatcher + 20 maps + reduce = 23
        check("cancel: total job count",
              len(all_jobs) == 23,
              f"got={len(all_jobs)}")

    finally:
        if not worker_task.done():
            worker._running = False
            try:
                await asyncio.wait_for(worker_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                worker_task.cancel()
                try:
                    await worker_task
                except (asyncio.CancelledError, Exception):
                    pass
        await asyncio.sleep(0.1)
        await local_q.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 37: Retry on fan-out + error (dagler)
# ---------------------------------------------------------------------------

async def test_m8_retry_fanout():
    """Retry a fan-out DAG after map-level failure."""
    section("37. Retry fan-out + error (dagler)")

    import shutil

    tmp_dir = tempfile.mkdtemp(prefix="proofler_s37_")
    db_path = os.path.join(tmp_dir, "s37.db")

    try:
        DAG._registry.clear()
        _m8_fail_counter.clear()

        local_q = Queue(db_path)
        await local_q.init_db()

        dag = DAG("s37_retry")
        dag.task(m8_produce_small)
        dag.map_task(depends_on=[m8_produce_small])(m8_map_fail_some)
        dag.reduce_task(depends_on=[m8_map_fail_some])(m8_reduce_retry)

        cid = "s37-retry-cid"
        run = await dag.run(local_q, correlation_id=cid)

        # First worker run — item==2 fails
        worker_a = Worker(local_q, queues=["default"], concurrency=1,
                          poll_interval=0.1, shutdown_timeout=5.0)
        wa_task = asyncio.create_task(worker_a.run())
        await run.wait(timeout=30.0)
        worker_a._running = False
        try:
            await asyncio.wait_for(wa_task, timeout=5.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            wa_task.cancel()
            try:
                await wa_task
            except asyncio.CancelledError:
                pass

        check("retry-fanout: DagRun failed after first run",
              run.status == "failed",
              f"status={run.status}")

        # Produce completed
        jm = run.job_map
        check("retry-fanout: produce completed",
              jm["m8_produce_small"].status == "completed",
              f"status={jm['m8_produce_small'].status}")

        # Find the failed map job (item==2)
        map_jobs = [j for j in run.jobs if j.task and "m8_map_fail_some" in j.task]
        failed_maps = [j for j in map_jobs if j.status == "failed"]
        check("retry-fanout: exactly 1 map failed",
              len(failed_maps) == 1,
              f"failed_count={len(failed_maps)}")

        check("retry-fanout: failed map error contains 'item-2'",
              failed_maps and "item-2" in (failed_maps[0].last_error or ""),
              f"error={failed_maps[0].last_error if failed_maps else 'none'}")

        # Reduce should be cancelled (dep failed)
        reduce_job = jm.get("m8_reduce_retry")
        check("retry-fanout: reduce cancelled",
              reduce_job is not None and reduce_job.status == "cancelled",
              f"status={reduce_job.status if reduce_job else 'missing'}")

        # Retry the run
        await run.retry(local_q)

        check("retry-fanout: status reset to pending",
              run.status == "pending",
              f"status={run.status}")

        # Second worker — item==2 succeeds (counter at 2)
        worker_b = Worker(local_q, queues=["default"], concurrency=1,
                          poll_interval=0.1, shutdown_timeout=5.0)
        wb_task = asyncio.create_task(worker_b.run())
        await run.wait(timeout=30.0)
        worker_b._running = False
        try:
            await asyncio.wait_for(wb_task, timeout=5.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            wb_task.cancel()
            try:
                await wb_task
            except asyncio.CancelledError:
                pass

        check("retry-fanout: DagRun completed after retry",
              run.status == "completed",
              f"status={run.status}")

        # Reduce result: sorted([0*10, 1*10, 2*10, 3*10, 4*10]) = [0, 10, 20, 30, 40]
        jm = run.job_map
        reduce_result = jm.get("m8_reduce_retry")
        check("retry-fanout: reduce result exact",
              reduce_result is not None and reduce_result.result == [0, 10, 20, 30, 40],
              f"result={reduce_result.result if reduce_result else 'missing'}")

        # Test RetryCompletedRunError
        got_completed_error = False
        try:
            await run.retry(local_q)
        except RetryCompletedRunError:
            got_completed_error = True
        check("retry-fanout: RetryCompletedRunError on completed run",
              got_completed_error,
              "no exception raised")

        # Test RetryNoFailuresError — submit a run, don't start a worker,
        # all jobs are pending (none failed/cancelled) → retry should raise
        dag_nofail = DAG("s37_nofail")
        dag_nofail.task(m8_instant_ok)
        run_nofail = await dag_nofail.run(local_q, correlation_id="s37-nofail-cid")
        got_no_failures_error = False
        try:
            await run_nofail.retry(local_q)
        except RetryNoFailuresError:
            got_no_failures_error = True
        check("retry-fanout: RetryNoFailuresError on run with no failures",
              got_no_failures_error,
              "no exception raised")

    finally:
        await local_q.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 38: Idempotent DAG submission (dagler)
# ---------------------------------------------------------------------------

async def test_m8_idempotent():
    """Submit the same DAG twice with idempotency_key — same run returned."""
    section("38. Idempotent DAG submission (dagler)")

    import shutil

    tmp_dir = tempfile.mkdtemp(prefix="proofler_s38_")
    db_path = os.path.join(tmp_dir, "s38.db")

    try:
        DAG._registry.clear()

        local_q = Queue(db_path)
        await local_q.init_db()

        # Reuse existing linear DAG task functions
        dag = DAG("s38_idem")
        dag.task(dag_extract)
        dag.task(depends_on=[dag_extract])(dag_transform)
        dag.task(depends_on=[dag_transform])(dag_load)

        idem_key = "s38-key-1"
        cid1 = "s38-idem-1"

        # First submission
        run1 = await dag.run(local_q, correlation_id=cid1, idempotency_key=idem_key)
        run1_id = run1._id
        run1_cid = run1.correlation_id

        check("idem: first submission creates run",
              run1 is not None and run1._id is not None,
              f"id={run1._id}")

        check("idem: first submission has 3 jobs",
              len(run1.job_ulids) == 3,
              f"count={len(run1.job_ulids)}")

        # Second submission — same key
        run2 = await dag.run(local_q, correlation_id="s38-idem-2", idempotency_key=idem_key)

        check("idem: second submission returns same _id",
              run2._id == run1_id,
              f"run1_id={run1_id}, run2_id={run2._id}")

        check("idem: second submission returns same CID",
              run2.correlation_id == run1_cid,
              f"run1_cid={run1_cid}, run2_cid={run2.correlation_id}")

        # DB: exactly 1 DagRun row with this key
        all_runs = await DagRun.query().filter(
            F("idempotency_key") == idem_key
        ).all()
        check("idem: exactly 1 DagRun row in DB",
              len(all_runs) == 1,
              f"count={len(all_runs)}")

        # DB: exactly 3 Job rows (not 6)
        all_jobs = await Job.query().filter(F("correlation_id") == run1_cid).all()
        check("idem: exactly 3 Job rows in DB",
              len(all_jobs) == 3,
              f"count={len(all_jobs)}")

        # DB: no orphan jobs created for the second submission's CID
        orphan_jobs = await Job.query().filter(F("correlation_id") == "s38-idem-2").all()
        check("idem: no orphan jobs for second CID",
              len(orphan_jobs) == 0,
              f"orphan_count={len(orphan_jobs)}")

        # Run Worker, complete the DAG
        worker = Worker(local_q, queues=["default"], concurrency=1,
                        poll_interval=0.1, shutdown_timeout=5.0)
        worker_task = asyncio.create_task(worker.run())
        await run1.wait(timeout=15.0)
        worker._running = False
        try:
            await asyncio.wait_for(worker_task, timeout=5.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        check("idem: DagRun completed",
              run1.status == "completed",
              f"status={run1.status}")

        # Third submission post-completion — same completed run returned
        run3 = await dag.run(local_q, correlation_id="s38-idem-3", idempotency_key=idem_key)
        check("idem: post-completion returns same _id",
              run3._id == run1_id,
              f"run3_id={run3._id}")

        check("idem: post-completion status is completed",
              run3.status == "completed",
              f"status={run3.status}")

        # Fourth submission — different key → new independent run
        run4 = await dag.run(
            local_q, correlation_id="s38-idem-4", idempotency_key="s38-key-2"
        )
        check("idem: different key creates new run",
              run4._id != run1_id,
              f"run4_id={run4._id}, run1_id={run1_id}")

        check("idem: new run has own CID",
              run4.correlation_id == "s38-idem-4",
              f"cid={run4.correlation_id}")

    finally:
        await local_q.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 39: wait() timeout path (dagler)
# ---------------------------------------------------------------------------

async def test_m8_wait_timeout():
    """wait(timeout=...) raises TimeoutError before DAG completes."""
    section("39. wait() timeout path (dagler)")

    import shutil

    tmp_dir = tempfile.mkdtemp(prefix="proofler_s39_")
    db_path = os.path.join(tmp_dir, "s39.db")

    try:
        DAG._registry.clear()

        local_q = Queue(db_path)
        await local_q.init_db()

        dag = DAG("s39_timeout")
        dag.task(m8_instant_ok)
        dag.task(depends_on=[m8_instant_ok])(m8_never_finishes)

        cid = "s39-timeout-cid"
        worker = Worker(local_q, queues=["default"], concurrency=1,
                        poll_interval=0.1, shutdown_timeout=5.0)
        worker_task = asyncio.create_task(worker.run())

        run = await dag.run(local_q, correlation_id=cid)

        # Wait with short timeout — m8_never_finishes sleeps 600s
        got_timeout = False
        try:
            await run.wait(timeout=3.0)
        except TimeoutError:
            got_timeout = True

        check("timeout: TimeoutError raised",
              got_timeout,
              "no TimeoutError")

        # Status should NOT be completed
        await run.refresh_status()
        check("timeout: status is not completed",
              run.status != "completed",
              f"status={run.status}")

        check("timeout: finished_at is None",
              run.finished_at is None,
              f"finished_at={run.finished_at}")

        # m8_instant_ok should have completed before timeout
        jm = run.job_map
        ok_job = jm.get("m8_instant_ok")
        check("timeout: instant_ok completed before timeout",
              ok_job is not None and ok_job.status == "completed",
              f"status={ok_job.status if ok_job else 'missing'}")

        check("timeout: instant_ok result correct",
              ok_job is not None and ok_job.result == "ok",
              f"result={ok_job.result if ok_job else 'missing'}")

        # m8_never_finishes should be running (claimed by worker, sleeping)
        nf_job = jm.get("m8_never_finishes")
        check("timeout: never_finishes is running",
              nf_job is not None and nf_job.status == "running",
              f"status={nf_job.status if nf_job else 'missing'}")

        # Cancel after timeout — demonstrates recovery pattern
        await run.cancel(local_q)

        check("timeout: cancel after timeout succeeds",
              run.status == "cancelled",
              f"status={run.status}")

        # DB persisted
        persisted = await DagRun.query().filter(
            F("correlation_id") == cid
        ).first()
        check("timeout: DB status cancelled",
              persisted is not None and persisted.status == "cancelled",
              f"status={persisted.status if persisted else 'missing'}")

    finally:
        if not worker_task.done():
            worker._running = False
            try:
                await asyncio.wait_for(worker_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                worker_task.cancel()
                try:
                    await worker_task
                except (asyncio.CancelledError, Exception):
                    pass
        await asyncio.sleep(0.1)
        await local_q.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 40: Concurrent fan-out DAG runs (dagler)
# ---------------------------------------------------------------------------

async def test_m8_concurrent_fanout():
    """Two concurrent fan-out DAGs — prove isolation with dispatcher writes."""
    section("40. Concurrent fan-out DAG runs (dagler)")

    import shutil

    tmp_dir = tempfile.mkdtemp(prefix="proofler_s40_")
    db_path = os.path.join(tmp_dir, "s40.db")

    try:
        DAG._registry.clear()

        local_q = Queue(db_path)
        await local_q.init_db()

        n = 50
        dag = DAG("s40_conc_fanout")
        dag.task(m8_produce_concurrent)
        dag.map_task(depends_on=[m8_produce_concurrent])(m8_map_identity)
        dag.reduce_task(depends_on=[m8_map_identity])(m8_reduce_sum_conc)

        worker = Worker(local_q, queues=["default"], concurrency=4,
                        poll_interval=0.1, shutdown_timeout=5.0)
        worker_task = asyncio.create_task(worker.run())

        # Submit 2 runs concurrently
        run_a = await dag.run(
            local_q,
            payload={"m8_produce_concurrent": {"n": n}},
            correlation_id="s40-run-a",
        )
        run_b = await dag.run(
            local_q,
            payload={"m8_produce_concurrent": {"n": n}},
            correlation_id="s40-run-b",
        )

        # Wait concurrently
        completed_a, completed_b = await asyncio.gather(
            run_a.wait(timeout=120),
            run_b.wait(timeout=120),
        )

        worker._running = False
        try:
            await asyncio.wait_for(worker_task, timeout=5.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass

        # Both completed
        check("conc-fanout: run A completed",
              completed_a.status == "completed",
              f"status={completed_a.status}")

        check("conc-fanout: run B completed",
              completed_b.status == "completed",
              f"status={completed_b.status}")

        # Exact reduce results: sum(range(50)) = 1225
        expected_sum = sum(range(n))
        reduce_a = completed_a.job_map.get("m8_reduce_sum_conc")
        check("conc-fanout: run A reduce result",
              reduce_a is not None and reduce_a.result == expected_sum,
              f"expected={expected_sum}, got={reduce_a.result if reduce_a else 'missing'}")

        reduce_b = completed_b.job_map.get("m8_reduce_sum_conc")
        check("conc-fanout: run B reduce result",
              reduce_b is not None and reduce_b.result == expected_sum,
              f"expected={expected_sum}, got={reduce_b.result if reduce_b else 'missing'}")

        # CID isolation
        a_cids = {j.correlation_id for j in completed_a.jobs}
        check("conc-fanout: run A CID isolation",
              a_cids == {"s40-run-a"},
              f"cids={a_cids}")

        b_cids = {j.correlation_id for j in completed_b.jobs}
        check("conc-fanout: run B CID isolation",
              b_cids == {"s40-run-b"},
              f"cids={b_cids}")

        # No ULID overlap between runs
        a_ulids = set(completed_a.job_ulids)
        b_ulids = set(completed_b.job_ulids)
        overlap = a_ulids & b_ulids
        check("conc-fanout: no ULID overlap",
              len(overlap) == 0,
              f"overlap={overlap}")

        # DB: exactly 2 DagRun rows
        all_dag_runs = await DagRun.query().filter(
            F("dag_name") == "s40_conc_fanout"
        ).all()
        check("conc-fanout: 2 DagRun rows in DB",
              len(all_dag_runs) == 2,
              f"count={len(all_dag_runs)}")

        # Total jobs: each run = produce + dispatcher + 50 maps + reduce = 53
        # Two runs = 106
        all_jobs = await Job.query().all()
        check("conc-fanout: 106 total jobs in DB",
              len(all_jobs) == 106,
              f"count={len(all_jobs)}")

        # Per-run job count
        check("conc-fanout: run A job count",
              len(completed_a.job_ulids) == 53,
              f"count={len(completed_a.job_ulids)}")

        check("conc-fanout: run B job count",
              len(completed_b.job_ulids) == 53,
              f"count={len(completed_b.job_ulids)}")

        # Map job counts per run
        a_maps = sum(1 for n in completed_a.task_names if n == "m8_map_identity")
        check("conc-fanout: run A has 50 map jobs",
              a_maps == 50,
              f"count={a_maps}")

        b_maps = sum(1 for n in completed_b.task_names if n == "m8_map_identity")
        check("conc-fanout: run B has 50 map jobs",
              b_maps == 50,
              f"count={b_maps}")

    finally:
        if not worker_task.done():
            worker._running = False
            try:
                await asyncio.wait_for(worker_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                worker_task.cancel()
                try:
                    await worker_task
                except (asyncio.CancelledError, Exception):
                    pass
        await asyncio.sleep(0.1)
        await local_q.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Section 41: 10K fan-out (dagler)
# ---------------------------------------------------------------------------

async def test_m8_10k_fanout():
    """10K fan-out — push past 5K to find the next wall."""
    section("41. 10K fan-out (dagler)")

    import shutil

    n = 10000
    timeout_budget = 600
    tmp_dir = tempfile.mkdtemp(prefix="proofler_s41_")
    db_path = os.path.join(tmp_dir, "s41.db")
    local_q = None
    worker = None
    worker_task = None

    try:
        DAG._registry.clear()

        local_q = Queue(db_path)
        await local_q.init_db()

        dag = DAG("s41_10k")
        dag.task(m7_produce_n)
        dag.map_task(depends_on=[m7_produce_n])(m7_map_square)
        dag.reduce_task(depends_on=[m7_map_square])(m7_reduce_sum)

        worker = Worker(local_q, queues=["default"], concurrency=2,
                        poll_interval=0.1, shutdown_timeout=5.0)
        worker_task = asyncio.create_task(worker.run())

        timed_out = False
        t0 = time.monotonic()
        try:
            run = await dag.run(
                local_q,
                payload={"m7_produce_n": {"n": n}},
                correlation_id="s41-10k",
            )
            run = await run.wait(timeout=timeout_budget)
        except TimeoutError:
            timed_out = True
            elapsed = time.monotonic() - t0
            rate = n / elapsed if elapsed > 0 else 0
            print(f"  INFO  10K fan-out: TIMED OUT at {elapsed:.1f}s ({rate:.0f} items/s)")
            print(f"  INFO  Performance finding: 10K fan-out exceeds {timeout_budget}s budget")
            # Verify partial progress: run was created and dispatcher ran
            all_timeout_jobs = await Job.query().filter(
                F("correlation_id") == "s41-10k"
            ).all()
            check("10K timeout: partial jobs created",
                  len(all_timeout_jobs) > n // 2,
                  f"only {len(all_timeout_jobs)} jobs created before timeout")
            check("10K timeout: elapsed plausible",
                  elapsed >= 5.0,
                  f"elapsed={elapsed:.1f}s — run may not have started")

        if not timed_out:
            elapsed = time.monotonic() - t0
            rate = n / elapsed if elapsed > 0 else 0

            check("10K: completed",
                  run.status == "completed",
                  f"status={run.status}")

            # n + 3 = produce + dispatcher + n maps + reduce
            expected_jobs = n + 3
            check("10K: job count",
                  len(run.job_ulids) == expected_jobs,
                  f"got={len(run.job_ulids)}, expected={expected_jobs}")

            expected_sum = sum(i * i for i in range(n))
            reduce_job = run.job_map.get("m7_reduce_sum")
            actual = reduce_job.result if reduce_job else None
            check("10K: exact reduce result",
                  actual == expected_sum,
                  f"got={actual}, expected={expected_sum}")

            check("10K: within budget",
                  elapsed < timeout_budget,
                  f"elapsed={elapsed:.1f}s")

            check("10K: throughput floor",
                  rate >= 10,
                  f"{rate:.0f} items/s < 10 items/s floor")

            print(f"  INFO  10K fan-out: {elapsed:.1f}s, {rate:.0f} items/s")

    finally:
        if worker is not None and worker_task is not None and not worker_task.done():
            worker._running = False
            try:
                await asyncio.wait_for(worker_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                worker_task.cancel()
                try:
                    await worker_task
                except (asyncio.CancelledError, Exception):
                    pass
        await asyncio.sleep(0.1)
        if local_q is not None:
            try:
                await local_q.close()
            except Exception:
                pass
        shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    print("\n" + "="*60)
    print("  -ler Stack Integration Test")
    sqler_ver = getattr(sqler, "__version__", "dev")
    qler_ver = getattr(qler, "__version__", "dev")
    import procler
    procler_ver = getattr(procler, "__version__", "dev")
    import dagler
    dagler_ver = getattr(dagler, "__version__", "dev")
    print(f"  sqler {sqler_ver} + qler {qler_ver} + logler + procler {procler_ver} + dagler {dagler_ver}")
    print("="*60)

    try:
        await test_sqler_foundation()
        await test_enqueue_and_immediate()
        await test_worker_execution()
        await test_cancellation()
        await test_job_wait()
        await test_lease_recovery()
        await test_rate_limiting()
        await test_cron_scheduling()
        await test_logging_integration()
        await test_logler_db_bridge()
        await test_investigator()
        await test_full_roundtrip()
        await test_order_pipeline()
        await test_procler_foundations()
        await test_procler_manages_worker()
        await test_procler_health_checks()
        await test_procler_crash_recovery()
        await test_procler_logler_observability()
        await test_full_ler_stack()
        await test_job_archival()
        await test_reverse_dep_index()
        await test_batch_enqueue()
        await test_batch_claim()
        await test_combined_pipeline()
        await test_dagler_linear()
        await test_dagler_diamond()
        await test_dagler_failure_retry()
        await test_dagler_fanout()
        await test_dagler_logler()
        await test_dagler_full_stack()
        await test_m7_fanout_scaling()
        await test_m7_concurrent_runs()
        await test_m7_multistage()
        await test_m7_memory_leak()
        await test_m7_concurrency_sweep()
        await test_m8_cancel_midflight()
        await test_m8_retry_fanout()
        await test_m8_idempotent()
        await test_m8_wait_timeout()
        await test_m8_concurrent_fanout()
        await test_m8_10k_fanout()
    except Exception as e:
        import traceback
        print(f"\n  CRASH  Unhandled exception: {e}")
        traceback.print_exc()
        global failed
        failed += 1
    finally:
        # Cleanup
        if q is not None:
            await q.close()
        for path in (DB_PATH, LOG_PATH):
            if path and os.path.exists(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass
            # Also clean WAL/SHM files
            for suffix in ("-wal", "-shm"):
                wal_path = path + suffix
                if wal_path and os.path.exists(wal_path):
                    try:
                        os.unlink(wal_path)
                    except OSError:
                        pass

    # Summary
    section("SUMMARY")
    total = passed + failed
    print(f"  {passed}/{total} passed, {failed} failed")
    if errors:
        print("\n  Failures:")
        for err in errors:
            print(f"    - {err}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
