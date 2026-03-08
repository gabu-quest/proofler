# M3: procler Integration

## What This Is

An integration test proving that procler can manage qler workers as real OS processes with health monitoring and full logler observability. This is the final integration layer: sqler stores data, qler processes jobs, logler traces everything, and procler orchestrates the workers.

Someone reading this thinks: "I can use procler to manage my job workers in production and logler will tell me what happened when something goes wrong."

---

## The Architecture

```
procler (ProcessManager)
    |
    ├── defines "qler-worker-1" as a local process
    ├── starts subprocess: uv run python tests/worker_harness.py
    ├── monitors PID, status, exit code
    └── health checks via command probe
         |
         v
worker_harness.py (subprocess)
    |
    ├── opens shared qler DB (via QLER_DB_PATH env var)
    ├── registers task functions (echo_task, slow_task, fail_task)
    ├── runs Worker loop (poll → claim → execute → complete)
    ├── writes structured logs via logler JsonHandler
    └── graceful shutdown on SIGTERM
         |
         v
test_stack.py (test process)
    |
    ├── creates qler DB (AsyncSQLerDB)
    ├── enqueues jobs with correlation IDs
    ├── polls for completion
    ├── reads logler log files + DB via Investigator
    └── verifies: procler status + qler jobs + logler traces all agree
```

Two separate SQLite databases:
- **procler state DB** — process definitions, runtime status, logs
- **qler job DB** — jobs, attempts (shared between test process and worker subprocess)

---

## Worker Harness

`tests/worker_harness.py` — a standalone Python script that procler spawns as a subprocess.

Configuration via environment variables:
- `QLER_DB_PATH` (required) — path to shared qler SQLite DB
- `QLER_QUEUES` (default: "default") — comma-separated queue names
- `QLER_CONCURRENCY` (default: 1) — worker concurrency
- `QLER_LOG_PATH` (optional) — logler JsonHandler output file
- `QLER_CID_PREFIX` (optional) — correlation ID prefix

Task functions (module-level):
```python
async def echo_task(message: str) -> str:
    """Simple echo — proves basic job execution."""
    return f"echo:{message}"

async def slow_task(seconds: float = 1.0) -> str:
    """Sleeps for N seconds — proves long-running job handling."""
    await asyncio.sleep(seconds)
    return f"slept:{seconds}"

async def fail_task(message: str) -> str:
    """Always raises — proves error handling."""
    raise RuntimeError(f"intentional failure: {message}")
```

---

## Test Sections

### Section 14: procler Foundations (~6 checks)

Prove procler's core API works in lerproof's environment.

1. Import procler components (db, models, ProcessManager)
2. Initialize procler with temp state DB
3. Define a simple process (`echo hello`)
4. Query it back, verify fields
5. Start it, verify status transitions
6. Check exit_code == 0 after completion

### Section 15: procler Manages qler Worker (~10 checks)

The core integration seam: procler spawns a real qler worker.

1. Create shared qler DB
2. Define procler process pointing to worker_harness.py
3. Start worker via ProcessManager
4. Enqueue 10 echo jobs from test process
5. Poll until all complete (max 15s)
6. Verify all 10 completed
7. Verify worker process is RUNNING
8. Stop worker via ProcessManager
9. Verify STOPPED status, exit_code == 0
10. Verify no pending/running jobs remain

### Section 16: procler Health Checks (~6 checks)

Health monitoring for a qler worker process.

1. Start worker, register command-based health check
2. Health check probes: `kill -0 <pid>` (is process alive?)
3. Wait for health checker to run 2+ cycles
4. Verify HEALTHY status, 0 consecutive failures
5. Stop worker + health checking
6. Verify health transitions to DEAD

### Section 17: Worker Crash + Recovery (~8 checks)

Prove procler detects crashes and workers can be manually restarted.

1. Start worker, enqueue slow jobs
2. Kill worker with SIGKILL (simulating crash)
3. Wait for procler to detect exit
4. Verify STOPPED status, non-zero exit code
5. Verify pending jobs remain in queue
6. Restart worker via ProcessManager
7. New worker picks up remaining jobs
8. All jobs eventually complete, new PID != old PID

### Section 18: procler + logler Observability (~8 checks)

Thread logler through the procler-managed pipeline.

1. Start worker with QLER_LOG_PATH for structured logging
2. Enqueue 5 jobs with correlation IDs
3. Wait for completion
4. Convert qler DB to JSONL via db_to_jsonl()
5. Load log file + JSONL into Investigator
6. Search by CID returns entries from both sources
7. At least one CID has full lifecycle trace
8. follow_thread returns chronological entries

### Section 19: Full -ler Stack Roundtrip (~10 checks)

All four libraries in one flow.

1. procler manages worker with logler logging
2. Enqueue 20 jobs (18 echo, 2 fail) with CIDs
3. Worker processes all jobs
4. Verify procler: process status, PID
5. Verify qler: 18 completed, 2 failed, 0 pending
6. Verify logler: investigator traces jobs through log + DB
7. Cross-stack: 3 successful CIDs appear in both qler DB and logler log
8. Cross-stack: 1 failed CID shows error diagnosis in logler
9. Stop worker via procler, clean shutdown
10. Verify no data loss

---

## Success Criteria

- All sections pass deterministically (no timing flakiness)
- Each section completes in < 20s (no long polls)
- procler manages real OS processes (not mocked)
- Shared qler DB works across process boundary
- logler traces thread through the full stack
- Total: ~48 checks across 6 sections

---

## Non-Goals

- No Docker context testing (LocalContext only)
- No auto_restart testing (field exists but not implemented in procler)
- No procler groups/recipes (future milestone)
- No concurrent multi-worker procler management (single worker per section)
- No performance benchmarking (covered in BENCHMARKS.md)
