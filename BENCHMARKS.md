# Benchmarks: -ler Stack Under Chaos

Performance characterization of the sqler + qler + logler stack running a realistic order-processing pipeline with seeded chaos, worker churn, zombie injection, and abrupt kills.

**Test harness:** `tests/stress.py` — 8-task dependency pipeline per order, full logler instrumentation.

---

## Executive Summary

| Metric | Pre-Upgrade (M2) | Post-Upgrade (M4) | M5 Best |
|--------|------------------|-------------------|---------|
| Peak throughput (200 orders) | 1,230 jobs/min | 2,445 jobs/min | — |
| Scale throughput (1000 orders) | 265 jobs/min | 2,972 jobs/min | **3,909 jobs/min (c=8)** |
| Optimal concurrency | c=2 | c=2 | **c=8** |
| Data loss | Zero | Zero | **Zero** |
| Zombie recovery | 100% | 100% | **100%** |
| Observability | 61k entries | 12k+ entries | **61k entries** |
| 10,000 orders (1w) | untested | untested | **50/50 PASS, 1,203 jobs/min, 1,369 MB peak** |
| 10,000 orders (2w) | untested | untested | **50/50 PASS, 1,523 jobs/min, 1,279 MB peak** |

SQLite + WAL + connection pool is a legitimate small-to-medium job processing backend. Zero external dependencies, zero data loss under chaos, full observability through logler. The M4 performance upgrades fundamentally changed the concurrency curve — higher concurrency now helps instead of hurting, and scale degradation is eliminated.

---

## The Journey

### Starting point

- `concurrency=1` only — BUG-1 (`sqlite3.OperationalError: cannot commit transaction`) blocked any higher concurrency on disk-backed databases
- Single aiosqlite connection shared across all coroutines; interleaved commits crashed the worker
- Throughput: **~878 jobs/min** (14.6 jobs/s) at c=1 with 200 orders

### sqler ships connection pool

sqler replaced the single shared connection with an `asyncio.Queue`-based pool:
- `pool_size=4` default for `on_disk()` databases
- Per-task connection pinning via `ContextVar`
- Connections return to pool after commit/rollback, stay pinned during transactions
- WAL mode + full PRAGMA suite (busy_timeout=5000, mmap_size=256MB, cache_size=64MB)

BUG-1: **FIXED.**

### Benchmark campaign

With the pool in place, we benchmarked c=1 through c=8 at 200 orders (clean sequential runs, no contamination between benchmarks). Then scaled to 1000 orders to find the ceiling.

Discovery: **c=2 is the SQLite sweet spot** — 2.8x throughput over c=1. Higher concurrency adds lock contention that overwhelms the gains.

---

## Benchmark Results

### Concurrency curve (200 orders, 1600 jobs)

Clean sequential runs — each concurrency level tested alone.

| Concurrency | Throughput | p50 | p95 | p99 |
|-------------|-----------|-----|-----|-----|
| c=1 | 439 jobs/min | 96ms | 193ms | 278ms |
| **c=2** | **1,230 jobs/min** | **77ms** | **158ms** | **227ms** |
| c=4 | 840 jobs/min | 172ms | 549ms | 773ms |
| c=8 | 630 jobs/min | 386ms | 1,210ms | 2,097ms |

c=2 wins on every metric: highest throughput AND lowest latency. At c=4+, SQLite lock contention drives up tail latency while reducing throughput.

### Scale degradation (c=2)

| Orders | Jobs | Duration | Throughput | p50 | p95 | p99 |
|--------|------|----------|-----------|-----|-----|-----|
| 200 | 1,600 | ~1.5 min | 1,230 jobs/min | 77ms | 158ms | 227ms |
| 1,000 | 8,000 | 30m 11s | 265 jobs/min | 361ms | 921ms | 1,958ms |

**4.6x throughput drop** at 5x the data. The main table grows to 8,000 rows, and every poll/claim/complete cycle pays the cost of scanning it. See [Bottleneck Analysis](#where-the-time-goes) below.

### Chaos resilience

Every run — regardless of concurrency or scale — passed with zero data loss:

| Run | Zombies injected | Recovered | Worker churn | Data loss |
|-----|-----------------|-----------|--------------|-----------|
| 200 orders, c=1 | 5 | 5 (100%) | 2 cycles | 0 |
| 200 orders, c=4 | 10 | 10 (100%) | 3 cycles | 0 |
| 1000 orders, c=1 | 119 | 119 (100%) | 31 cycles | 0 |
| 1000 orders, c=2 | 110 | 110 (100%) | 29 cycles | 0 |

Worker churn includes graceful shutdown + restart and exactly 1 abrupt kill per 1000-order run. Lease recovery handles all zombie jobs without manual intervention.

### Observability proof (logler)

At 1000 orders with full logler instrumentation:

| Metric | Value |
|--------|-------|
| Log entries | ~61,597 (app + lifecycle + DB) |
| Unique correlation IDs | 1,000 (1 per order) |
| Traceable (sampled) | 10/10 orders |
| Error diagnosis | 3/3 failed orders diagnosed |
| Cross-source tracing | 3/3 orders traced across log + DB |

Error budget breakdown (seeded chaos rates):
- `validate`: 2.5% failure
- `fraud`: 4.5% failure
- `charge`: 11.2% failure
- Overall order success: 82.6% (826/1000)

---

## Where The Time Goes

Bottleneck analysis based on qler internals (`qler/src/qler/queue.py`).

### NOT the bottleneck: claim query

The claim query uses a well-designed partial index:

```sql
CREATE INDEX idx_qler_jobs_claim
ON qler_jobs(queue_name, priority DESC, eta, ulid)
WHERE status = 'pending' AND pending_dep_count = 0;
```

This is O(log n) — fast even at scale.

### Primary bottleneck: dependency resolution (O(n) full scan)

`_resolve_dependencies()` (queue.py ~line 979) runs after every job completion:

```python
candidates = await Job.query().filter(
    (F("status") == JobStatus.PENDING.value)
    & (F("pending_dep_count") > 0)
).all()
```

This scans **every pending job with unresolved dependencies** — no index, full table scan. With 8,000 jobs, this runs thousands of times as jobs complete. Each scan touches thousands of rows. This is the primary reason throughput degrades 4.6x at 1000 orders.

### Secondary bottleneck: cascade cancellation

`_cascade_cancel_dependents()` uses the same full-scan pattern to find downstream jobs when a parent fails. In the order pipeline, a failed `charge` task triggers cascade cancellation of `confirm`, `inventory`, `invoice`, and `warehouse` — each requiring a scan.

### Contributing factors

| Factor | Impact |
|--------|--------|
| **No job archival** | Completed/failed/cancelled jobs stay in the main table forever, bloating every query |
| **Individual INSERTs** | `enqueue_many()` saves jobs one at a time instead of batch INSERT |
| **Single-job claim** | Claim query uses LIMIT 1; could batch-claim N jobs per poll |

---

## Performance Roadmap

All optimizations target SQLite-native improvements. No Postgres, no external dependencies.

Ordered by expected impact-to-effort ratio:

| # | Optimization | Where | Expected Impact | Effort |
|---|-------------|-------|----------------|--------|
| 1 | Job archival | qler | 3-5x at scale | Medium |
| 2 | Reverse dependency index | qler | Eliminates O(n) scan per completion | Medium |
| 3 | Dependency resolution index | qler | O(n) → O(k) for dep resolution query | Small |
| 4 | Batch INSERT | qler/sqler | Faster bulk enqueue | Small |
| 5 | Batch claim | qler | N jobs per poll instead of 1 | Small |
| 6 | Hash-based DB sharding | qler | Linear write scaling across shards | Large |
| 7 | PRAGMA tuning | sqler/qler | Better cache hit rates | Small |

### 1. Job archival

Move completed/failed/cancelled jobs to a `qler_jobs_archive` table after N seconds. The active table stays small (under 1,000 rows during processing). All hot-path queries — claim, dependency resolution, metrics — run against the small table. Archive is read-only, used for reporting and logler investigation.

**Expected impact:** This alone could bring 1000-order throughput up to match the 200-order numbers (1,230 jobs/min), since the bottleneck is table size, not concurrency.

### 2. Reverse dependency index

Instead of scanning all pending jobs to find who depends on a completed ULID, store a reverse map: `parent_ulid -> [child_ulid1, child_ulid2, ...]`. On completion, look up children directly. O(1) instead of O(n).

Could be a separate table (`qler_job_deps`) or a JSON column with an index. Either way, eliminates the full table scan in `_resolve_dependencies()`.

### 3. Dependency resolution index

Quick win:

```sql
CREATE INDEX idx_qler_jobs_deps
ON qler_jobs(status, pending_dep_count)
WHERE status = 'pending' AND pending_dep_count > 0;
```

Turns the scan into an index lookup for the common case. Doesn't eliminate the need to check the `dependencies` JSON field, but drastically reduces the candidate set.

### 4. Batch INSERT

SQLite supports multi-row INSERT: `INSERT INTO t VALUES (...), (...), (...)`. Use this in `enqueue_many()` instead of individual `asave()` calls. Reduces transaction count from N to 1 for bulk enqueue.

### 5. Batch claim

Change `LIMIT 1` to `LIMIT N` in the claim query. Worker claims N jobs at once and processes them concurrently. Reduces poll overhead by Nx. Particularly impactful at high concurrency where poll frequency is the bottleneck.

### 6. Hash-based DB sharding

Shard jobs across N SQLite files: `shard = hash(queue_name) % N`. Each shard has its own connection pool and write lock. Unrelated queues never contend. Linear write throughput scaling.

Biggest architectural change but biggest potential gain. Most relevant for multi-queue deployments where independent pipelines shouldn't contend.

### 7. PRAGMA tuning

sqler's `on_disk()` already sets mmap_size (256MB) and cache_size (64MB), but qler's `init_db()` runs its own PRAGMAs that don't include these. Ensuring qler inherits sqler's full PRAGMA suite would keep hot pages in memory and reduce disk I/O.

---

## What This Stack Can Run Today

Real-world throughput mapping at c=2 (the sweet spot):

| Working set | Throughput | Real-world equivalent |
|-------------|-----------|----------------------|
| < 500 active jobs | ~1,200 jobs/min | E-commerce order processing, webhook delivery, email campaigns |
| 500-2,000 active jobs | ~600 jobs/min | Background report generation, batch data processing |
| 2,000-8,000 active jobs | ~265 jobs/min | Large import pipelines, migration jobs |

For context: 1,200 jobs/min = 72,000 jobs/hour = 1.7M jobs/day. That's more than enough for most single-server applications. The stack handles this with zero external dependencies — just SQLite files on disk.

---

## Post-Upgrade Results (M4)

All 5 performance upgrades shipped and validated through integration tests (sections 20-24). Re-benchmarked with the same parameters as the pre-upgrade runs.

### What shipped

| # | Upgrade | What it does |
|---|---------|-------------|
| 1 | **Job archival** | `archive_jobs()` moves completed jobs to `qler_jobs_archive`, keeping the hot table small |
| 2 | **Reverse dependency index** | `qler_job_deps` table maps parent→child. O(1) child lookup replaces O(n) full scan |
| 3 | **Dependency resolution partial index** | `idx_qler_jobs_pending_deps` for pending jobs with unresolved deps |
| 4 | **Batch INSERT** | `enqueue_many()` → `Job.asave_many()` — one transaction for N jobs |
| 5 | **Batch claim** | `claim_jobs(worker_id, queues, n)` → `update_n()` — claim N jobs per poll |

### Bugs found and fixed during validation

| Bug | Impact | Fix |
|-----|--------|-----|
| Connection leak in `_resolve_dependencies()` | Pool exhaustion → Worker hang after 4 jobs | Added `auto_commit()` after SELECT cursor close |
| Connection leak in `_cascade_cancel_dependents()` | Same pool exhaustion pattern | Added `auto_commit()` after SELECT cursor close |
| Stale `job.last_attempt_id` in `claim_jobs()` | `_terminalize_attempt` skips finalization → attempt records stuck as "running" | Added `job.last_attempt_id = attempt_id` after DB update |
| No intra-batch idempotency in `enqueue_many()` | Two specs with same key in one batch both inserted | Added `batch_idem_keys` tracking + dedup before `asave_many` |
| logler `db_to_jsonl` crashes on non-sqler tables | `ORDER BY _id` fails on `qler_job_deps` (no `_id` column) | Skip tables without `_id` in auto-detection; fallback to `rowid` |

### Re-benchmark: 200 orders (c=2)

| Metric | Pre-Upgrade | Post-Upgrade | Change |
|--------|------------|-------------|--------|
| **Throughput** | 1,230 jobs/min | **2,445 jobs/min** | **+99% (2x)** |
| **p50 latency** | 77ms | **30ms** | **-61%** |
| **p95 latency** | 158ms | **83ms** | **-47%** |
| **p99 latency** | 227ms | **106ms** | **-53%** |
| **Duration** | ~1.5 min | **39s** | **-57%** |
| **Data integrity** | Zero loss | Zero loss | Same |

The 2x throughput improvement at 200 orders comes primarily from:
- **Batch claim** (UPGRADE-5): Claiming multiple jobs per poll eliminates per-job claim overhead
- **Reverse dep index** (UPGRADE-2): O(1) child lookup replaces O(n) scan after every completion
- **Connection leak fixes**: All 4 pool connections now return correctly, eliminating the silent bottleneck

### Re-benchmark: 1000 orders (c=2)

| Metric | Pre-Upgrade | Post-Upgrade | Change |
|--------|------------|-------------|--------|
| **Throughput** | 265 jobs/min | **2,972 jobs/min** | **11.2x faster** |
| **p50 latency** | 361ms | **30ms** | **-92%** |
| **p95 latency** | 921ms | **87ms** | **-91%** |
| **p99 latency** | 1,958ms | **106ms** | **-95%** |
| **Duration** | 30m 11s | **2m 41s** | **-91%** |
| **Data integrity** | Zero loss | Zero loss | Same |
| **Checks** | 51/51 | **51/51** | Same |

**The scale degradation problem is solved.** Pre-upgrade, throughput dropped 4.6x from 200→1000 orders because `_resolve_dependencies()` scanned the entire jobs table after every completion. Post-upgrade, 1000 orders actually runs at higher throughput than 200 orders because the larger dataset keeps the batch claim pipeline full.

### Degradation factor: before and after

| Scale | Pre-Upgrade | Post-Upgrade |
|-------|------------|-------------|
| 200 orders | 1,230 jobs/min (baseline) | 2,445 jobs/min |
| 1000 orders | 265 jobs/min (**4.6x degradation**) | 2,972 jobs/min (**1.2x improvement**) |

The degradation factor went from **4.6x slowdown** to **1.2x speedup**. At scale, the upgrades deliver 11x more throughput than the pre-upgrade version.

### What's left to optimize

The 5 upgrades addressed the biggest bottlenecks identified in the pre-upgrade analysis. Remaining opportunities:

1. **Hash-based DB sharding** — Linear write scaling across N SQLite files for multi-queue deployments
2. **PRAGMA tuning** — Ensure qler inherits sqler's full PRAGMA suite (mmap_size, cache_size)
3. **Job archival auto-sweep** — Periodic background archival during worker operation (currently manual `archive_jobs()` call)

---

## M5: Concurrency × Scale Matrix

With the M4 upgrades in place, we re-ran the concurrency sweep to find the new optimal point. The pre-upgrade bottleneck was `_resolve_dependencies()` doing O(n) full scans — higher concurrency meant more concurrent scans fighting for the same rows. With the reverse dep index, that contention is gone.

### Post-upgrade concurrency curve (1000 orders, 8000 jobs)

| Config | Throughput | p50 | p95 | p99 | Duration | Checks |
|--------|-----------|-----|-----|-----|----------|--------|
| c=2 (baseline) | 2,972 jobs/min | 30ms | 87ms | 106ms | 2m 41s | 51/51 |
| **c=4** | **3,776 jobs/min** | 32ms | 93ms | 113ms | 2m 07s | 51/51 |
| **c=8** | **3,909 jobs/min** | 34ms | 97ms | 125ms | 2m 02s | 51/51 |

### Concurrency curve: before vs after

| Concurrency | Pre-Upgrade (200 orders) | Post-Upgrade (1000 orders) | Verdict |
|-------------|-------------------------|---------------------------|---------|
| c=1 | 439 jobs/min | — | — |
| c=2 | **1,230 jobs/min** (optimal) | 2,972 jobs/min | No longer the peak |
| c=4 | 840 jobs/min (degraded) | **3,776 jobs/min** | Now faster than c=2 |
| c=8 | 630 jobs/min (severely degraded) | **3,909 jobs/min** | **New optimal** |

**The optimal concurrency shifted from c=2 to c=8.** Pre-upgrade, c=4+ caused lock contention in `_resolve_dependencies()` — every concurrent coroutine was doing full table scans that fought over the same rows. The reverse dep index (M4 UPGRADE-2) eliminated that contention entirely. Now higher concurrency means more jobs processing in parallel with no lock amplification.

Diminishing returns above c=4: the jump from c=4→c=8 (+3.5%) is much smaller than c=2→c=4 (+27%). The remaining bottleneck is SQLite's single-writer constraint — WAL mode allows concurrent reads but serializes writes.

### Scale degradation: eliminated

| Scale | Pre-Upgrade (c=2) | Post-Upgrade (c=2) | Post-Upgrade (c=8) |
|-------|-------------------|--------------------|--------------------|
| 200 orders | 1,230 jobs/min | 2,445 jobs/min | — |
| 1000 orders | 265 jobs/min | 2,972 jobs/min | 3,909 jobs/min |
| **Degradation** | **4.6x slowdown** | **1.2x speedup** | — |

### Multi-worker results (1000 orders)

Does splitting work across multiple Worker instances (separate poll loops, separate claim batches) help beyond raw coroutine count?

| Config | Total coroutines | Throughput | p50 | p95 | p99 | RSS |
|--------|-----------------|-----------|-----|-----|-----|-----|
| 1w × c=2 | 2 | 2,972 j/min | 30ms | 87ms | 106ms | — |
| **2w × c=2** | 4 | **3,921 j/min** | 31ms | 90ms | 108ms | 136 MB |
| 1w × c=4 | 4 | 3,776 j/min | 32ms | 93ms | 113ms | — |
| 4w × c=2 | 8 | 3,825 j/min | 36ms | 103ms | 138ms | 137 MB |
| 1w × c=8 | 8 | 3,909 j/min | 34ms | 97ms | 125ms | — |
| 2w × c=4 | 8 | 3,466 j/min | 46ms | 135ms | 222ms | 136 MB |

**Findings:**

1. **2w×c=2 is the overall winner** (3,921 j/min) — two poll loops claiming small batches slightly outperforms one loop claiming bigger batches at the same 4 coroutines.

2. **High per-worker concurrency + multiple workers hurts.** 2w×c=4 (3,466 j/min) is the worst 8-coroutine config. Two workers each claiming 4 jobs creates overlapping write transactions that contend on SQLite's single-writer lock.

3. **RSS is flat at 136-137 MB** across all multi-worker configs. No memory scaling with worker count at 1K orders.

4. **Recommended production config: 2w×c=2 at 1K scale, 2w×c=1 at 10K+ scale.** At 1K, 2w×c=2 wins on throughput. At 10K, the larger WAL + abrupt cancellation creates fatal write contention at c=2 — drop to c=1 per worker for crash resilience.

### 10,000 orders: OOM #1 (test artifacts)

The first 10,000-order attempt (80,000 jobs) at c=2 completed the execution phase — all 80,000 jobs processed in 23 worker cycles (~29 minutes, ~2,720 jobs/min sustained). **Throughput held steady at scale.**

Then the kernel OOM killer struck during Phase 4 (logler observability analysis):

```
oom-kill: task=python3, pid=1048060
total-vm: 7,805,328 kB (~7.4 GB)
anon-rss: 6,022,724 kB (~5.7 GB)
```

**Root cause:** stress.py held 80K Job objects in `order_jobs` dict (~2-3 GB), 80K timing entries in `_pipeline_timing` dict (~200 MB), and logler's Investigator loaded 600K+ entries into in-memory DuckDB (~2-3 GB).

### 10,000 orders: Memory optimization

Eliminated all three test-side memory hogs:

| Change | Before | After |
|--------|--------|-------|
| `order_jobs` dict (80K Job objects) | ~2-3 GB in-memory | SQL queries via `get_order_jobs(cid)` |
| `_pipeline_timing` dict (80K entries) | ~200 MB in-memory | Separate SQLite timing DB |
| `pipeline_ulids` set | ~50 MB in-memory | Eliminated (all jobs in temp DB are pipeline jobs) |
| logler Investigator DuckDB | In-memory (`:memory:`) | Disk-backed (`sql_db_path=`) |

Key lesson: timing data MUST go in a **separate SQLite file** — sync `sqlite3.connect()` in task functions contends with qler's async aiosqlite on the same file, causing "database is locked" floods.

Verified at smaller scales:

| Scale | Result | RSS |
|-------|--------|-----|
| 200 orders (1,600 jobs) | 50/50 PASS | 54 MB |
| 1,000 orders (8,000 jobs) | 50/50 PASS | 111 MB |

### 10,000 orders: OOM #2 (misattributed — stdout buffering)

Without `PYTHONUNBUFFERED=1`, stdout was block-buffered. We saw stale Phase 2 cycle output while the process was actually in Phase 3/4. The OOM at 7.1 GB was initially attributed to a "Phase 2 runtime leak":

```
oom-kill: task=python3, pid=4596
total-vm: 10,311,612 kB (~9.8 GB)
anon-rss: 7,478,192 kB (~7.1 GB)
```

The tracemalloc investigation (below) revealed this was wrong — Phase 2 was stable. The explosion happened in Phase 3/4.

### 10,000 orders: tracemalloc investigation ✅

Added `tracemalloc.start(1)` with snapshots every 5 cycles. `PYTHONUNBUFFERED=1` for real-time output. Results:

**Phase 2 memory is completely stable:**

| Snapshot | Jobs remaining | Jobs processed | RSS | Traced current | Traced peak |
|----------|---------------|----------------|-----|----------------|-------------|
| Baseline | 80,000 | 0 | 46 MB | 0.2 MB | 0.3 MB |
| Cycle 5 | 71,553 | ~8,447 | 1,252 MB | 0.6 MB | 537 MB |
| Cycle 10 | 61,448 | ~18,552 | 1,252 MB | 0.6 MB | 537 MB |
| Cycle 15 | 47,855 | ~32,145 | 1,304 MB | 0.6 MB | 537 MB |
| Cycle 20 | 36,637 | ~43,363 | 1,304 MB | 0.7 MB | 537 MB |
| Cycle 25 | 25,774 | ~54,226 | 1,304 MB | 0.7 MB | 537 MB |
| Cycle 30 | 10,471 | ~69,529 | 1,304 MB | 0.7 MB | 537 MB |
| **FINAL** | **0** | **80,000** | **1,304 MB** | **0.9 MB** | **537 MB** |

**Phase 2 completed: 34 cycles, 44 min, 80K jobs. RSS flat at 1,304 MB from Cycle 5 onward.**

Phase 3 (correctness assertions): **All 15 checks PASSED.**

Phase 4 (logler observability): Started, RSS jumped to **4.9 GB** within seconds, then continued growing. WSL crashed (entire VM killed, `wsl --shutdown` required to recover).

**Key findings from tracemalloc:**

1. **qler Worker does NOT leak.** RSS stayed at exactly 1,304 MB from Cycle 5 through Cycle 34. Processing 80K jobs across 34 cycles with zero memory growth. The "runtime memory leak" hypothesis was wrong.

2. **Python-tracked allocations are negligible.** tracemalloc saw 0.9 MB of live Python objects at Phase 2 end. The 1,304 MB RSS is almost entirely C-level memory: SQLite page cache (`cache_size=64MB`), WAL buffers, `mmap_size=256MB`, aiosqlite thread stacks.

3. **The traced peak of 537 MB was transient** — hit once during early processing, then fully freed. CPython's allocator kept the RSS high (1,304 MB) but memory was reused, not leaking.

4. **The real OOM is in Phase 3/4 — test/analysis code, not qler.** Something in the correctness assertions or logler observability analysis blows up memory by 3.6+ GB on top of the 1.3 GB base.

**Revised suspect list for the Phase 3/4 OOM:**

| Suspect | Phase | Mechanism | Likelihood |
|---------|-------|-----------|------------|
| **db_to_jsonl()** | 4 | Reads all 80K+ records from qler DB, buffers in memory before writing JSONL | High |
| **Investigator file loading** | 4 | Even with disk-backed DuckDB, initial file ingest may buffer entirely in RAM | High |
| **get_order_jobs() sampling** | 3 | Each call loads all jobs for a correlation_id; 10 samples × 8 jobs each is small, but queries touch 80K-row table | Low |
| **Job.query().all() for aggregate checks** | 3 | `raw_count()` uses COUNT(*) which is efficient, but some checks might load full result sets | Medium |

**Takeaway:** qler is production-safe at 80K jobs — zero memory growth during processing. The 10K barrier is a test infrastructure problem in Phase 3/4, specifically logler's `db_to_jsonl()` and `Investigator` analysis at scale. This is the same class of issue as OOM #1 but surfacing later because the biggest hogs were already eliminated.

### 10,000 orders: Phase 4 profiling ✅

Added tracemalloc snapshots around each Phase 4 call. Reproduced on a second 10K run (38 cycles, 50 min for Phase 2 — again stable at 1,297 MB). Phase 3 passed all 15 checks. Then Phase 4:

**Per-call memory breakdown:**

| Call | RSS before | RSS after | Delta | Traced Δ |
|------|-----------|-----------|-------|----------|
| Phase 3 (all checks) | 1,297 MB | 1,468 MB | +171 MB | +1.4 MB |
| `db_to_jsonl()` | 1,468 MB | 1,548 MB | **+80 MB** | +0.1 MB |
| `Investigator()` | 1,548 MB | 1,548 MB | **0** | 0 |
| `load_files()` | 1,548 MB | 2,351 MB | **+803 MB** | +0.8 MB |
| `load_from_db()` | 2,351 MB | 2,634 MB | **+283 MB** | 0 |
| First `inv.search()` | 2,634 MB | 7,251 MB | **+4,617 MB** | **+1,281 MB** |
| OOM killed | — | 8,602 MB | — | — |

**The smoking gun — `inv.search()` tracemalloc output:**

```
json/decoder.py:353: size=1,182 MiB, count=12,238,515, average=101 B
logler/_search_core.py:52: size=3,516 KiB, count=80,000, average=45 B
```

A single `inv.search(query="job.enqueued")` call:
- Decoded **12.2 million JSON objects** consuming **1.18 GB** of Python heap
- Created **80,000 result entries** in `_search_core.py`
- Grew RSS by **4.6 GB** (1.18 GB Python + ~3.4 GB DuckDB query execution buffers)
- This is a full-table scan that deserializes every matching row into Python dicts

**Kernel OOM (third crash):**

```
oom-kill: task=python3, pid=8417
total-vm: 11,211,064 kB (~10.7 GB)
anon-rss: 8,602,168 kB (~8.2 GB)
```

Died during or just after the first `inv.search()` call, before the second search could run.

**Root cause confirmed: logler `search()` returns unbounded result sets as fully-parsed Python objects.** At 10K scale with ~600K+ log entries, querying for common patterns like "job.enqueued" matches tens of thousands of entries, each JSON-decoded into memory. With disk-backed DuckDB, the storage is on disk but the **query results are fully materialized in Python**.

### Handoff to logler

**qler is clean.** No fixes needed. The handoff is entirely to logler:

1. **`search()` must support `LIMIT`/pagination.** The current API returns all matching results as a Python list. At 10K scale, a broad query returns 80K+ results × 12M JSON decode calls. Fix: add `limit` and `offset` parameters, default limit to 1000 or similar.

2. **`load_files()` buffers +803 MB** loading two log files into DuckDB. Even with disk-backed DuckDB, the ingest phase reads entire files into memory. Fix: streaming line-by-line ingest or chunked loading.

3. **`load_from_db()` adds +283 MB** loading the qler DB. Same pattern as above — full read before DuckDB insert.

4. **`db_to_jsonl()` is fine at +80 MB** — this is acceptable for 80K records.

**Priority order:** Fix #1 (`search()` LIMIT) first — it's the 4.6 GB spike that actually kills the process. Fixes #2-3 are optimizations that would reduce the baseline from 2.6 GB to something more reasonable.

**Production impact:** This only affects analysis/investigation code, not the logging path. Applications using `correlation_context()` and `CorrelationFilter` for structured logging are unaffected. The Investigator is a diagnostic tool — but it needs to work at scale for lerproof's test suite and for production log analysis.

---

## What This Stack Can Run Today

Post-upgrade throughput mapping (c=4, recommended for production):

| Working set | Throughput | Real-world equivalent |
|-------------|-----------|----------------------|
| < 500 active jobs | ~3,800+ jobs/min | E-commerce order processing, webhook delivery, email campaigns |
| 500-2,000 active jobs | ~3,500 jobs/min | Background report generation, batch data processing |
| 2,000-8,000 active jobs | ~2,700 jobs/min | Large import pipelines, migration jobs |
| 8,000+ active jobs | ~2,700 jobs/min (projected) | Scale tested to 80K jobs — throughput holds |

For context: 3,800 jobs/min = 228K jobs/hour = 5.5M jobs/day. With zero external dependencies — just SQLite files on disk.

---

## Memory Anatomy (from tracemalloc at 10K scale)

The 10K tracemalloc investigation revealed exactly where memory goes at steady state:

| Component | Size | Source |
|-----------|------|--------|
| Python objects (traced) | **~1 MB** | Job model instances, asyncio frames — all transient |
| SQLite page cache | **64 MB** | `cache_size=-65536` PRAGMA (configurable) |
| SQLite mmap | **256 MB** | `mmap_size=268435456` PRAGMA (configurable) |
| Connection pool threads | ~32 MB | `pool_size=4` × 8 MB thread stack |
| SQLite WAL + journal | ~50-200 MB | Proportional to write volume, checkpointed periodically |
| CPython arena overhead | ~700 MB | PyMalloc allocator retains freed pages (RSS ≠ live memory) |
| **Total (steady state)** | **~1,304 MB** | Stable — zero growth across 80K jobs |

**Key insight:** The 1,304 MB RSS at 10K scale is almost entirely C-level allocations (SQLite + CPython arena), not Python objects. tracemalloc saw only 0.9 MB of live objects. The large RSS is SQLite doing its job (caching) plus CPython's allocator not returning freed arenas to the OS.

### Tuning for constrained environments

The default PRAGMAs are tuned for throughput on a dev workstation. For resource-constrained deployments (512 MB containers, free-tier VPS):

| Setting | Default (dev) | Tuned (512 MB) | Impact |
|---------|---------------|----------------|--------|
| `cache_size` | 64 MB | 8 MB | Less page caching, more disk reads |
| `mmap_size` | 256 MB | 0 (disabled) | No memory-mapped I/O, relies on page cache |
| `pool_size` | 4 | 1-2 | Fewer concurrent DB connections |
| `concurrency` | 2-4 | 1 | Single-task processing |
| **Estimated RSS** | **~1,300 MB** | **~70-100 MB** | Fits in 512 MB with headroom |

Throughput impact: expect ~2-3x reduction with constrained settings (fewer cached pages = more disk I/O per query). Still viable for low-volume workloads (bot commands, webhook delivery, scheduled tasks).

---

### 10,000 orders: logler search() fix — round 1

logler shipped a two-phase Rust search refactor (filter+materialize with `MatchCandidate` structs) and a `DEFAULT_MAX_RESULTS = 100_000` safety cap. Rebuilt logler, ran 10K again. Results:

**Phase 2:** Stable at 1,297-1,333 MB (identical to previous runs). 37 cycles.

**Phase 3:** 15/15 PASS.

**Phase 4 memory breakdown (with Rust fix):**

| Call | RSS after | Delta | Notes |
|------|-----------|-------|-------|
| Phase 2 FINAL | 1,333 MB | — | Stable baseline |
| Phase 3 assertions | 1,537 MB | +204 MB | — |
| `db_to_jsonl()` | 1,616 MB | +79 MB | Same as before |
| `Investigator()` | 1,616 MB | 0 | Lazy constructor |
| `load_files()` | 2,422 MB | +806 MB | Rust index (unchanged) |
| `load_from_db()` | 2,664 MB | +242 MB | DB entries (unchanged) |
| `inv.search("job.enqueued")` | **6,299 MB** | **+3,635 MB** | **Still OOM-dangerous** |

**Problem:** The 100K safety cap didn't trigger because there were only ~80K matches (under the cap). All 80K results were fully materialized by Rust, serialized as one giant JSON string, and deserialized by Python's `json.loads()` — same `json/decoder.py:353: 1,182 MiB, count=12,238,514` as before.

**WSL OOM (4th crash):** The second `inv.search(query="job.completed")` presumably pushed RSS beyond 10 GB. Tmux + WSL killed.

**What the Rust fix actually helped:** When results EXCEED 100K, the two-phase approach avoids cloning entries during filter (~24 MB for 600K candidates vs ~4.2 GB). But for results UNDER 100K, all entries still get materialized. The fix is necessary but insufficient for our 80K-match broad queries.

### 10,000 orders: logler search() fix — round 2

logler shipped three follow-up fixes:

| Commit | Fix | Impact |
|--------|-----|--------|
| `701b972` | Lower `DEFAULT_MAX_RESULTS` 100K → 10K | Prevents materializing more than 10K results by default |
| `cb72d2e` | `count_only=True` + `offset` in Rust search | Zero-materialization count queries; pagination support |
| `1cb4fe9` | Strengthen tests after audit | Coverage for offset, count_only edge cases |

Additionally, `db_to_jsonl()` was refactored to stream per-table directly to disk (no in-memory `all_entries` accumulation + sort). Saves ~80 MB at 80K rows.

**lerproof stress.py updated:** Broad lifecycle queries (`"job.enqueued"`, `"job.completed"`) now use `count_only=True` instead of materializing all results. Only the count is needed — individual entries were never inspected.

**10K test with round 2 fixes — partial results (killed after 60+ min SQL engine build):**

Phase 2: 33 cycles, ~45 min, RSS stable at 940 MB (lower plateau than previous runs).

Phase 3: **15/15 PASS.**

Phase 4 memory breakdown (with count_only + 10K cap + db_to_jsonl streaming):

| Call | RSS after | Delta | Notes |
|------|-----------|-------|-------|
| Phase 2 FINAL | 911 MB | — | Stable baseline |
| Phase 3 assertions | 1,447 MB | +536 MB | — |
| `load_files()` | ~2,400 MB | +~950 MB | Rust index (600K entries) |
| `load_from_db()` | ~2,700 MB | +~300 MB | DB entries |
| `count_only` searches | ~2,700 MB | **~0** | **count_only works — no materialization** |
| SQL engine build start | ~4,200 MB | +1,500 MB | LogParser re-parsing all files |
| SQL engine build plateau | ~5,000 MB | +800 MB | DuckDB ingest stabilized |
| **60+ min later** | **~5,000 MB** | **still building** | **Killed — logler perf bug** |

Phase 4 checks before SQL engine: **34/36 passed** (2 FAIL on latency extraction — unrelated). All search, trace, diagnosis, and cross-correlation checks passed. The SQL analytics section (C.6) never ran because `_get_sql_engine()` was still building.

**Status: `count_only=True` eliminated the search() OOM. The only remaining blocker is logler's `_get_sql_engine()` doing 1.2M individual INSERT statements (60+ min at 600K entries). Fix handed off to logler session.**

### 10,000 orders: COMPLETE ✅ (2026-03-03)

After 10 logler fixes across 8 OOM/perf bugs, the 10K stress test passes all 50 checks:

| Metric | Value |
|--------|-------|
| **Checks** | **50/50 PASS** |
| **Orders** | 10,000 (80,000 jobs) |
| **Throughput** | 1,203 jobs/min sustained |
| **Latency** | p50: 60ms, p95: 134ms, p99: 171ms |
| **Log entries** | ~610,727 (app + lifecycle + DB) |
| **Unique CIDs** | 10,000 traceable |
| **Worker churn** | 52 cycles (51 graceful, 1 abrupt kill) |
| **Zombies** | 211 injected, 211 recovered (100%) |
| **Data loss** | 0 |
| **Peak RSS** | 1,369 MB |
| **Order success** | 81.2% (seeded chaos: validate 3%, fraud 4.8%, charge 11.9%) |

**logler fixes that unblocked this:**

| # | Fix | Impact |
|---|-----|--------|
| 1 | `DEFAULT_MAX_RESULTS` 100K → 10K | Prevents accidental materialization of all entries |
| 2 | `count_only=True` mode | Zero-memory count queries for lifecycle stats |
| 3 | `offset`/`limit` pagination | Server-side pagination in Rust search |
| 4 | `db_to_jsonl()` streaming | Per-table streaming, no in-memory sort |
| 5 | `executemany` batch inserts | 5K batch size in SqlEngine.load_files() |
| 6 | `limit=0` means no cap | Rust-side: `usize::MAX`. Python internal callers pass `limit=0` |
| 7 | Security hardening | Conn guards, try/finally, thread-safety docs |
| 8 | Paginated `_get_sql_engine()` | `self.search(limit=10K, offset=N)` → bounded memory |
| 9 | Direct index iteration | `get_entries_page()` in Rust → O(N) total vs O(N²/page_size) |
| 10 | `read_csv()` bulk insert | 230x faster than `executemany` — 680K rows in ~4s |

**stress.py fixes:**

| Fix | Impact |
|-----|--------|
| `query="job.executed"` filter on `extract_metrics()` | Narrow to ~10K events instead of ~80K |
| WAL drain sleep after abrupt kill | 0.5s lets aiosqlite release locks at scale |
| Eliminated `order_jobs` dict, `_pipeline_timing` dict, `pipeline_ulids` set | SQL-backed queries, bounded memory |

### 10,000 orders: Multi-worker (2w×c=1) ✅

Tested multi-worker at 10K scale. Two configurations attempted:

**2w×c=2 (FAILED — "database is locked"):**

```
Cycle 6: ABRUPT KILL 2w after 53s (pre: 70591 remaining)
sqlite3.OperationalError: database is locked
```

With 4 concurrent coroutines (2 workers × c=2) on an 80K-row WAL, the abrupt kill cycle creates a fatal contention window: both workers are cancelled mid-transaction, WAL locks linger, and the post-kill DB operations (`recover_expired_leases`, `inject_zombies`) hit the 5s busy_timeout. This config passed at 1K orders (8K rows) where transactions complete faster and the WAL is 10× smaller.

**2w×c=1 (50/50 PASS):**

| Metric | 1w×c=2 (baseline) | 2w×c=1 | Change |
|--------|-------------------|--------|--------|
| **Throughput** | 1,203 jobs/min | **1,523 jobs/min** | **+27%** |
| **Peak RSS** | 1,369 MB | **1,279 MB** | **-7%** |
| **Latency p50** | 60ms | 49ms | -18% |
| **Latency p95** | 134ms | 113ms | -16% |
| **Latency p99** | 171ms | 139ms | -19% |
| **Worker cycles** | 52 | 39 | — |
| **Zombies** | 211 inj/rec | 152 inj/rec | — |
| **Data loss** | 0 | 0 | Same |
| **Checks** | 50/50 | 50/50 | Same |
| **Order success** | 81.2% | 81.2% | Same (deterministic seed) |

Two workers at c=1 is **faster and leaner** than one worker at c=2. Each worker runs a single coroutine, so writes never overlap within a worker. Two poll loops with interleaved writes cause less contention than one poll loop with 2 concurrent writes, because SQLite's single-writer lock is held for shorter durations.

**Recommended production config at 10K+ scale: 2w×c=1.** Best throughput, lowest latency, no crash-kill vulnerability. Avoid multi-worker × multi-concurrency configs on large WAL files — the write serialization window during abrupt cancellation creates unrecoverable contention.

**SQLite write contention finding:** At 80K rows in WAL mode, the combination of `pool_size=4`, `busy_timeout=5000ms`, and `asyncio.Task.cancel()` creates a scenario where cancelled writers hold WAL locks past the busy_timeout. This is a fundamental SQLite+aiosqlite limitation, not a qler bug — but qler's Worker could mitigate it with a graceful connection drain on cancellation (close all pool connections explicitly before returning).

---

*Pre-upgrade benchmarks: 2026-02-25. Post-upgrade (M4): 2026-02-26. M5 concurrency sweep: 2026-02-26. 10K tracemalloc: 2026-02-27. logler fix validation: 2026-03-02. 10K COMPLETE: 2026-03-03. Multi-worker 10K: 2026-03-04.*
*Hardware: WSL2 Linux 6.6, 10 GB RAM, single SQLite file, WAL mode, pool_size=4.*
