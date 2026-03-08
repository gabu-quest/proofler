# M4: Performance Upgrade Validation + Re-Benchmark

## Context

M1-M3 complete (19 sections, 165 checks). qler shipped 5 performance upgrades:

| # | Upgrade | API |
|---|---------|-----|
| 1 | Job archival | `archive_jobs()`, `archived_jobs()`, `archive_count()` |
| 2 | Reverse dependency index | `qler_job_deps` table, O(1) child lookup |
| 3 | Dependency resolution partial index | `idx_qler_jobs_pending_deps` |
| 4 | Batch INSERT | `enqueue_many()` via `Job.asave_many()` |
| 5 | Batch claim | `claim_jobs(worker_id, queues, n)` via `update_n()` |

sqler shipped `asave_many()` (batch INSERT) and `update_n()` (batch UPDATE).

## Goal

Prove all 5 upgrades work end-to-end via integration tests, then re-benchmark to measure throughput improvement.

**Baseline:** 265 jobs/min at 1000 orders (4.6x degradation from 200-order peak of 1,230 jobs/min).

## Deliverables

### Part A: Integration Tests (sections 20-24)

Each section creates its own temp DB + Queue for isolation.

| Section | Upgrade | Checks |
|---------|---------|--------|
| 20 | Job archival | ~12 |
| 21 | Reverse dep index + cascade | ~14 |
| 22 | Batch enqueue | ~12 |
| 23 | Batch claim | ~12 |
| 24 | Combined pipeline | ~14 |

**Total new checks:** ~64
**Grand total:** ~229 (165 existing + 64 new)

### Part B: Re-Benchmark

Run `stress.py` at 200 and 1000 orders with `--concurrency 2 --seed 42`.

Record throughput, latency percentiles, degradation factor.
Update BENCHMARKS.md with before/after comparison.

## Technical Decisions

- **Temp DB isolation per section**: Same pattern as M3 sections (14-19)
- **Raw SQL reads for qler_job_deps**: No sqler model for this internal table; read-only introspection via sqlite3
- **finished_at backdating**: `job.finished_at = old_ts; await job.asave()` updates JSON blob correctly
- **No chaos in section 24**: Clean happy path; chaos testing covered by stress.py
- **claim_jobs() tested directly**: Section 23 tests API in isolation; section 24 uses Worker (which calls claim_jobs internally)

## Acceptance Criteria

1. All sections 1-24 pass (`uv run python tests/test_stack.py`)
2. stress.py completes at both 200 and 1000 orders
3. BENCHMARKS.md updated with post-upgrade results
4. ROADMAP.md updated with M4 status
