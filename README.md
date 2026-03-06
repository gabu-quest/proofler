# proofler

**Cross-stack integration tests for the -ler ecosystem.**

proofler proves that [sqler](https://github.com/gabu-quest/sqler), [qler](https://github.com/gabu-quest/qler), [logler](https://github.com/gabu-quest/logler), [procler](https://github.com/gabu-quest/procler), and [dagler](https://github.com/gabu-quest/dagler) work together correctly through their public APIs. Each library has its own unit tests — proofler tests **the seams**.

```
432 checks across 41 sections. 425 passing.
Zero workarounds. Every bug fixed at the source.
```

---

## The Stack

| Layer | Library | What It Does |
|-------|---------|-------------|
| Storage | **sqler** | SQLite ORM — async, optimistic locking, promoted columns, connection pool |
| Queues | **qler** | Background job queue — workers, cron, rate limiting, dependencies, batch ops |
| Logs | **logler** | Log investigation — Rust backend, correlation IDs, DB bridge, DuckDB analytics |
| Process | **procler** | Process management — health checks, crash detection, recovery |
| Pipelines | **dagler** | DAG orchestration — fan-out/reduce, retry, cancel, idempotent submission |

All backed by SQLite. Zero external dependencies.

---

## Quick Start

```bash
# Prerequisites: Python 3.12+, Rust stable (for logler_rs)

# Clone all repos
git clone git@github.com:gabu-quest/proofler.git
git clone git@github.com:gabu-quest/sqler.git
git clone git@github.com:gabu-quest/qler.git
git clone git@github.com:gabu-quest/procler.git
git clone git@github.com:gabu-quest/dagler.git

# Symlink dependencies into proofler
cd proofler
mkdir deps
ln -s ../sqler deps/sqler
ln -s ../qler deps/qler
ln -s ../procler deps/procler
ln -s ../dagler deps/dagler

# Install (logler installs automatically from PyPI)
uv sync

# Run
uv run python tests/test_stack.py
```

---

## What Gets Tested

### Foundations (S1-S12) — sqler + qler + logler

The base layer: promoted columns, CHECK constraints, WAL mode, job lifecycle (enqueue/claim/execute/complete/fail), cancellation, `job.wait()`, lease recovery, rate limiting, cron scheduling, logler correlation context, JSON handler, DB bridge, cross-stack investigation.

### Order Pipeline Under Chaos (S13) — Full stack

100 orders x 6 pipeline tasks = 600 jobs with seeded chaos. Worker churn, lease recovery, cascade cancellation, retry exhaustion, logler per-order tracing and error diagnosis.

### procler Integration (S14-S19)

Real OS subprocess management: spawn/stop workers, health checks (STARTING -> HEALTHY -> DEAD), crash detection via SIGKILL, automatic recovery, CID tracing through procler-managed workers, full -ler stack roundtrip (procler -> qler -> logler).

### Performance Upgrades (S20-S24)

Validates qler's five performance optimizations: job archival, reverse dependency index, batch enqueue with idempotency, batch claim with priority ordering, and a combined 30-job dependency pipeline.

### dagler Pipelines (S25-S30)

DAG orchestration end-to-end: linear pipelines with result injection, diamond DAGs with multi-parent merging, failure cascade + retry, fan-out/reduce with dynamic dispatcher, logler observability, full stack roundtrip.

### dagler Scale (S31-S35)

Push dagler to its limits: fan-out scaling curve (100 -> 5K items), 10 concurrent DAG runs, multi-stage depth (2-stage cascading fan-out), memory leak detection (10 cycles, stable RSS), concurrency sweep with WAL contention analysis.

### dagler Limits (S36-S41)

Operational edge cases: cancel mid-flight fan-out (cooperative cancellation), retry with map-level failures, idempotent submission dedup, `wait()` timeout + cancel recovery, concurrent fan-out contention, 10K fan-out (performance cliff documented).

---

## Performance

Throughput characterization from `tests/stress.py` (order processing pipeline with chaos):

| Scale | Throughput | Latency p50 | Notes |
|-------|-----------|-------------|-------|
| 200 orders (1,600 jobs) | 2,445 jobs/min | 30ms | Post-upgrade baseline |
| 1,000 orders (8,000 jobs) | 3,909 jobs/min | 34ms | c=8 optimal |
| 10,000 orders (80,000 jobs) | 1,523 jobs/min | 49ms | 2 workers, zero data loss |

dagler fan-out throughput:

| Fan-out Size | Elapsed | items/s |
|-------------|---------|---------|
| 100 items | 1.2s | 81 |
| 500 items | 4.3s | 116 |
| 1,000 items | 8.3s | 120 |
| 5,000 items | 69.8s | 72 |
| 10,000 items | >600s | 17 (throughput cliff) |

---

## Project Structure

```
proofler/
├── tests/
│   ├── test_stack.py      # 41 sections, 432 checks
│   ├── worker_harness.py  # Standalone qler worker for procler
│   └── stress.py          # Scalable chaos soak test (up to 10K orders)
├── deps/                  # Symlinks to sibling repos (gitignored)
└── CLAUDE.md              # AI-assisted development context
```

---

## Philosophy

proofler follows one rule above all others:

> **Fix bugs at the source.**

When a dependency has a bug, we fix it in that dependency's repo. No try/except wrappers, no skipped tests, no workarounds. The whole point of proofler is to catch these bugs — working around them defeats the mission.

Over 8 milestones, this approach uncovered and fixed:
- 3 sqler bugs (connection pool, count aggregation, promoted column handling)
- 4 qler bugs (connection leaks, stale attempt IDs, batch dedup, crash contention)
- 5 logler bugs (wrong DB mappings, empty init.py, search OOM, SQL engine perf, streaming)
- 3 dagler bugs (cancel missing dynamic jobs, schema timing, cancel status)
- 1 procler bug (DDL execution path)

Every fix has a regression test that fails if the bug comes back.

---

## License

MIT
