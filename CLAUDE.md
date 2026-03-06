# proofler — Cross-Stack Integration Tests for the -ler Ecosystem

**Purpose:** Prove that sqler, qler, logler, procler, and dagler work together correctly through their public APIs. This repo owns the integration tests and shared test infrastructure for the -ler stack.

---

## What This Repo Is

proofler is NOT a library. It's an integration test suite and quality gate for the -ler stack:

| Layer | Library | Responsibility |
|-------|---------|----------------|
| Storage | **sqler** | SQLite ORM, async, optimistic locking, promoted columns |
| Queues | **qler** | Background job queue, workers, cron, rate limiting |
| Logs | **logler** | Log investigation, Rust backend, correlation IDs, DB bridge |
| Process | **procler** | Process management, health checks, crash detection |
| Pipelines | **dagler** | DAG pipeline orchestration on qler + SQLite |

Each library has its own repo and unit tests. proofler tests **the seams** — where one library's output becomes another's input.

---

## Architecture Decisions

### Why a separate repo?

- Each -ler library can evolve independently with its own test suite
- Integration tests catch **interface mismatches** that unit tests miss (e.g., logler's DB mappings vs qler's actual schema)
- A new contributor can set up `deps/` symlinks and run `uv run python tests/test_stack.py` to verify everything works together without touching individual repos

### What we test

1. **sqler foundations** — Promoted columns, CHECK constraints, WAL mode (qler depends on these)
2. **qler job lifecycle** — Enqueue, claim, execute, complete/fail through the real Worker
3. **qler features** — Immediate mode, cancellation, job.wait(), lease recovery, rate limiting, cron
4. **logler logging** — correlation_context, CorrelationFilter, JsonHandler
5. **logler DB bridge** — db_to_jsonl reads qler's SQLite DB, auto-detection, epoch timestamp conversion
6. **logler Investigator** — Search by level, correlation_id, service_name across both logs and DB
7. **Full roundtrip** — Job executes with correlation ID → logler reads both the log file and DB → correlation ID threads through
8. **procler process management** — ProcessManager spawns/stops qler workers, health checks, crash detection + recovery
9. **procler + logler observability** — CID traces through procler-managed worker logs + qler DB
10. **Full -ler stack roundtrip** — procler starts worker → qler processes jobs → logler traces everything
11. **Performance upgrades** — Job archival, reverse dep index + cascade, batch enqueue with idempotency, batch claim with priority, combined 30-job pipeline
12. **dagler pipelines** — DAG definition, task result passing, fan-out/reduce, DagRun tracking

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.12+ | Runtime |
| uv | Package management, editable local installs |

---

## Project Structure

```
proofler/
├── CLAUDE.md              # This file — project context for any session
├── CLAUDE.local.md        # Gitignored — local paths, Rust PATH, session state
├── pyproject.toml         # Dependencies on sqler, qler, logler, procler, dagler
├── deps/                  # Symlinks to sibling repos (gitignored)
├── tests/
│   ├── test_stack.py      # The main integration test (41 sections, 432 checks)
│   ├── worker_harness.py  # Standalone qler worker for procler to manage
│   └── stress.py          # Scalable chaos soak test
└── .gitignore
```

---

## Running the Tests

```bash
# 1. Set up deps/ symlinks (see README for details)
# 2. uv sync
# 3. Run
uv run python tests/test_stack.py
```

The test prints a section-by-section report with PASS/FAIL for each check.

---

## Key Technical Notes

### Job.set_db() is class-level global state
Creating multiple `Queue(path)` instances overwrites the global `Job`/`JobAttempt` model DB reference. Secondary Queue instances should share the main queue's DB object: `Queue(q.db, immediate=True)` instead of `Queue(DB_PATH, immediate=True)`.

### @task and @cron reject nested functions
Task functions must be defined at module level. Register them on queues using `task(queue)(module_level_fn)`.

### sqler editable installs
Use `editable = true` in uv sources: `sqler = { path = "deps/sqler", editable = true }`. Without it, source changes aren't visible until re-lock.

### Worker concurrency on on-disk DBs
`concurrency=1` is safe. `concurrency>1` causes `sqlite3.OperationalError: cannot commit transaction - SQL statements in progress` due to interleaved commits on a single aiosqlite connection. This is a known sqler/aiosqlite limitation.

---

## Active Roadmaps

None currently active.

---

## Rules

### Fix bugs at the source (NON-NEGOTIABLE)
When a dependency has a bug, fix it in that dependency's repo. NEVER wrap in try/except, skip tests, or add fallback paths. The whole point of proofler is to catch these bugs.

### NEVER work around a slow or broken dependency API (NON-NEGOTIABLE)
If a library's API is slow, broken, or OOMs — **that is the bug**. Document it, report it to the library's CLAUDE.local.md, and fix it there. NEVER:
- Replace a library API call with a faster alternative to avoid the slow path
- Skip testing a library feature because it's too slow at scale
- Substitute raw queries for a library's query API to dodge a performance bug
- Weaken assertions because the library returns wrong/incomplete results
- **Kill a stuck test and "work around it" to get it to complete** — if the test hangs, the library has a performance bug. Stop, document it, and hand it off.

If `inv.sql_query()` takes 35 minutes at 10K orders, that is a logler performance bug. The test MUST still call `inv.sql_query()`. Fix logler's `_get_sql_engine()` instead.

If `inv.search()` OOMs without a limit, that is a logler memory bug. Use the proper API (`count_only=True`, `limit=N`) — but NEVER remove the `search()` test entirely.

If `_get_sql_engine()` hangs on offset-based pagination at scale, that is a logler algorithmic bug. The answer is to fix the O(N²) scan in logler, NOT to skip the SQL section or find a different path in the test.

**The protocol when a dependency bug blocks the test:**
1. Kill the test (it's not going to finish)
2. Document the bug in the dependency's `CLAUDE.local.md` with full context, measurements, root cause analysis, and suggested fix approaches
3. Tell the user — they run the fix in a separate session on that dependency
4. Rebuild, reinstall, retest

**proofler exists to find these bugs. Working around them defeats the entire purpose. There is no "for now" — every workaround is a betrayal of the mission.**

### Test through public APIs
Import from the library's public API, not internal modules. If a test needs an internal, that's a signal the public API has a gap.

### No raw SQL
All database operations go through sqler's model API. If sqler can't express it, fix sqler.

### No manual log parsing
All log reading goes through logler. If logler can't read it, fix logler.

### Document improvement opportunities
When a dependency is slow, memory-hungry, or missing a feature — even if it technically works — document it in that library's `CLAUDE.local.md` with:
1. What was observed (concrete numbers: timing, RSS, scale)
2. What the root cause is (exact file/line if possible)
3. Suggested fix approaches (prioritized)

Every proofler run at scale is an opportunity to find improvements across the -ler stack. Capture them all.
