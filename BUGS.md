# Cross-Stack Bugs and Improvements

Discovered during integration testing on 2026-02-20.

---

## Bugs

### BUG-2: Job.set_db() is class-level global state (sqler design issue)

**Status:** Open (design limitation)
**Where:** sqler model system
**Severity:** Medium — footgun for multi-queue applications

**Symptoms:** Creating multiple `Queue(path)` instances pointing to the same DB file causes the second `init_db()` to overwrite `Job.set_db()`, pointing the model class at the new connection. If the second Queue is closed, `Job` points to a dead connection.

**Workaround:** Share the main queue's DB object: `Queue(q.db, immediate=True)` instead of `Queue(DB_PATH, immediate=True)`. The secondary queue's `close()` becomes a no-op since it doesn't own the DB.

**Proper fix:** sqler models should support per-instance DB binding instead of class-level `set_db()`. This is a significant architectural change.

---

## Fixed Bugs

### BUG-1: Worker concurrency > 1 fails on on-disk databases (FIXED)

**Fixed:** 2026-02-25
**Where:** sqler `AsyncSQLiteAdapter` in `adapter/asynchronous.py`

**Was:** `sqlite3.OperationalError: cannot commit transaction - SQL statements in progress` during `claim_job` when Worker runs with `concurrency=2` on a file-based database. Single shared `aiosqlite.Connection` caused interleaved commits.

**Fix:** sqler replaced the single connection with an `asyncio.Queue`-based connection pool. Per-task connection pinning via `ContextVar`. `on_disk()` defaults to `pool_size=4` with WAL mode. Benchmarked up to `concurrency=8` with zero operational errors.

**Verified:** stress.py at c=2 achieves 1,230 jobs/min (200 orders) and 265 jobs/min (1000 orders) with zero data loss. See `BENCHMARKS.md`.

---

### BUG-3: sqler .count() ignores promoted columns (FIXED)

**Fixed:** 2026-02-20
**Where:** sqler `_build_aggregate_query()` in both `async_query.py` and `query.py`
**Commits:** On sqler branch `feat/qler-prerequisites`

`_build_aggregate_query()` was not calling `_rewrite_promoted_refs()`, so WHERE clauses on promoted columns used `json_extract(data, '$.status')` instead of the direct column. Since promoted fields are stripped from the JSON blob on save, json_extract returned NULL and aggregates returned 0.

Fix: Added `_rewrite_promoted_refs()` call and promoted field detection to `_build_aggregate_query()`. 10 regression tests added.

---

### BUG-4: logler db_source qler mappings wrong (FIXED)

**Fixed:** 2026-02-20
**Where:** logler `src/logler/db_source.py`
**Commits:** On logler branch `feat/sqler-bridge`

All three mapping functions (`qler_job_mapping`, `qler_attempt_mapping`, `_auto_detect_mappings`) had wrong table names, field names, status values, and timestamp formats — they were written against the spec before qler was implemented.

| What | Was (wrong) | Is (correct) |
|------|-------------|--------------|
| Job table | `jobs` | `qler_jobs` |
| Attempt table | `job_attempts` | `qler_job_attempts` |
| Task field | `task_name` | `task` |
| Queue field | `queue` | `queue_name` |
| Attempts field | `attempt_count` | `attempts` |
| Timestamps | `iso` | `epoch` |
| Statuses | `success`/`claimed`/`dead` | `completed`/`running`/`cancelled` |
| Attempt statuses | `success`/`failure`/`timeout`/`retry` | `running`/`completed`/`failed`/`lease_expired` |

---

### BUG-5: logler_rs __init__.py empty — shadows Rust .so (FIXED)

**Fixed:** 2026-02-20
**Where:** logler `src/logler_rs/__init__.py`
**Commits:** On logler branch `feat/sqler-bridge`

The maturin build creates `logler_rs/logler_rs.cpython-312-x86_64-linux-gnu.so` inside the package directory. The `__init__.py` was empty, so `import logler_rs` imported the empty stub instead of the `.so`. Fix: `from .logler_rs import *`.

---

## Improvements Identified

### IMP-1: sqler needs async connection pooling ✅

Shipped. sqler `on_disk()` now uses `asyncio.Queue`-based pool with `pool_size=4` and ContextVar pinning. See BUG-1 fix above.

### IMP-2: logler db_source should validate mappings

`db_to_jsonl` silently produces empty output if table names don't match. It should warn or error when a mapping references a table that doesn't exist in the database.

### IMP-3: Integration test should be pytest-based

The current test uses a custom `check()` framework. Converting to pytest would give better failure reporting, parametrization, and fixture management.

### IMP-4: procler integration

procler should be able to define and manage qler workers as named processes. This is the next integration layer to test.
