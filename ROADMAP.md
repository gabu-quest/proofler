# Roadmap: lerproof

## Milestones

### M1: Foundation + Feature Coverage ✅
- sqler foundations (promoted columns, CHECK, WAL)
- qler job lifecycle (enqueue, worker, cancel, wait, lease, rate limit, cron)
- logler logging pipeline (correlation context, JSON handler, DB bridge)
- Cross-stack roundtrip (log + DB combined search)
- Concurrency stress (200 jobs, 16 workers)
- Advanced features (dependencies, DLQ, batch, unique, progress, timeout, retry)
- logler API surface (SQL query, hierarchy, follow_thread, metrics, formats)
- Cross-stack traces (DB investigation, lifecycle trace with CID)
- 33 sections, 279 checks

### M2: Order Processing Pipeline Under Chaos ✅
- 100 orders × 6 pipeline tasks = 600 jobs with seeded chaos
- Dependency chains: validate → charge → [confirm, inventory, invoice], inventory → warehouse
- Worker churn: Worker A (8s) → lease recovery → Worker B (until done)
- Cascade proof: failed charge/validate → all downstream cancelled with "Dependency" error
- Retry exhaustion: failed charges verified at exactly 3 attempts
- logler observability: per-order CID traces, failed order diagnosis, SQL error rates
- 39 checks, hardened per test-auditor (no vacuous passes)
- **stress.py**: Scalable soak test — 1000+ orders × 8 tasks, worker churn, zombie injection, abrupt kills
- **Benchmarks**: Peak 1,230 jobs/min (c=2, 200 orders), 265 jobs/min at 1000 orders. BUG-1 fixed (sqler connection pool). See [BENCHMARKS.md](./BENCHMARKS.md)
- See [spec](./specs/m2-order-pipeline.md)

### M3: procler Integration ✅
- procler manages qler workers as real OS subprocesses via ProcessManager
- Health checks: command probe (kill -0), status transitions (STARTING → HEALTHY → DEAD)
- Crash detection: SIGKILL → procler detects exit → manual restart → worker recovers jobs
- logler observability: CID tracing through procler-managed worker logs + qler DB
- Full -ler stack roundtrip: procler starts worker → qler processes 20 jobs → logler traces everything
- 47 checks across 6 sections (14-19)
- Fixed procler db.py: `execute_sql()` → `adapter.execute()` for DDL (sqler read-only restriction)
- See [spec](./specs/m3-procler-integration.md)

### M4: Performance Upgrade Validation + Re-Benchmark ✅
- Validates all 5 qler performance upgrades end-to-end
- Job archival: `archive_jobs()`, `archived_jobs()`, `archive_count()` with backdating proof
- Reverse dependency index: `qler_job_deps` population, O(1) dep resolution, cascade cancellation
- Batch enqueue: `enqueue_many()` with deps, intra-batch idempotency dedup
- Batch claim: `claim_jobs(worker_id, queues, n)` with priority ordering, dep blocking
- Combined pipeline: 30-job dependency chain → batch claim via Worker → archival
- 58 checks across 5 sections (20-24), hardened per test-auditor
- Re-benchmark: 200 orders → 2,445 jobs/min (was 1,230 — 2x improvement)
- Fixed 3 qler bugs: connection leak in `_resolve_dependencies`/`_cascade_cancel_dependents`, stale `last_attempt_id` in `claim_jobs`, intra-batch idempotency dedup in `enqueue_many`
- Fixed logler bug: `db_to_jsonl` auto-detection crash on non-sqler tables (e.g., `qler_job_deps`)
- See [spec](./specs/m4-upgrade-validation.md)

### M5: Scale Stress Testing + Production Readiness ✅
Proved the -ler stack survives as a long-running production process. Three phases:

**Phase 1: Scale stress testing** ✅
- ✅ 1000 orders @ c=4: **3,776 jobs/min**, 51/51 checks
- ✅ 1000 orders @ c=8: **3,909 jobs/min** (new optimal), 51/51 checks
- ✅ **10,000 orders @ c=2: 50/50 PASS** — 80K jobs, 1,203 jobs/min sustained, 1,369 MB peak RSS
- ✅ Concurrency curve inverted: c=8 now fastest (was c=2 pre-upgrade)
- ✅ Multi-worker support added to stress.py (`--workers N`)
- ✅ Multi-worker benchmarks: 1K done (2w×c=2 optimal at 3,921 j/min), **10K done (2w×c=1: 1,523 j/min, 50/50)**
- ✅ 10K multi-worker finding: 2w×c=2 hung (SQLite write contention during abrupt kill at 80K rows)

**Phase 2: Memory profiling + leak detection** ✅
- ✅ RSS tracking added to stress.py (`RSSTracker` class, 5s sampling)
- ✅ OOM root cause identified: logler Investigator + test object accumulation, NOT qler runtime
- ✅ tracemalloc profiling: Python allocations negligible (0.9 MB), 1,304 MB RSS is C-level (SQLite/WAL/mmap)
- ✅ qler Worker confirmed stable: no growth across 52 cycles / 80K jobs
- ✅ RSS plateaus at ~1,020-1,120 MB during Phase 2, stable across all cycles

**Phase 3: Production readiness fixes (qler + logler)** ✅
- ✅ **logler: disk-backed DuckDB** — `Investigator(sql_db_path=path)` spills to disk
- ✅ **logler: streaming file loading** — paginated `get_entries_page()` + `read_csv()` bulk insert
- ✅ **logler: search() memory safety** — DEFAULT_MAX_RESULTS=10K, count_only mode, limit=0 semantics
- ✅ **logler: SQL engine O(N) build** — direct index iteration (fix #9) + read_csv (fix #10)
- ✅ **logler: extract_metrics query filter** — callers narrow with `query=` to avoid materializing all entries
- ✅ **qler: automatic archival sweep** — `--archive-interval` / `--archive-after` on `qler worker` CLI
- ✅ **qler: memory watchdog** — `--memory-limit-mb` RSS-based emergency archival
- ✅ **qler: connection pool health check** — `Queue.pool_health()` + `pool` field in Worker `/health`

**Deferred (not blocking):**
- logler Rust index memory (+1,086 MB at 10K) — optimization, not needed for production (logler won't run on Koyeb)
- Log rotation — production deploys to stdout on Koyeb; local dev uses stdlib `RotatingFileHandler` if needed
- Koyeb viability — qler Worker proven stable at 80K jobs; constrained PRAGMAs fit 512 MB (see BENCHMARKS.md tuning table)

### M6: dagler Integration ✅
Proved dagler's DAG pipeline orchestration works end-to-end on top of qler + sqler with real on-disk SQLite.

- ✅ Linear pipeline: extract → transform → load with result injection (S25, 11 checks)
- ✅ Diamond DAG: multi-parent injection, concurrent branches (S26, 12 checks)
- ✅ Failure cascade + retry: cascade cancel, DagRun.retry(), recovery (S27, 14 checks)
- ✅ Fan-out/reduce: dispatcher creates 5 map + reduce + trailing dynamically (S28, 11 checks)
- ✅ logler observability: db_to_jsonl, Investigator CID search, dagler_runs table safe (S29, 8 checks)
- ✅ Full stack roundtrip: linear + fan-out + logler CID trace in one DAG (S30, 8 checks)
- 63 checks across 6 sections, hardened per test-auditor
- Sibling project: `../dagler-test`
- See [spec](./specs/m6-dagler-integration.md)

### M7: dagler Scale Stress Testing ✅
Push dagler to its limits: fan-out scaling, concurrent runs, multi-stage pipelines, memory leak detection, concurrency sweep.

**Pre-milestone fixes:**
- ✅ Replace private qler imports (`qler._context`, `qler._time`) with public API
- ✅ Unify parallel `job_ulids`/`task_names` arrays into `jobs_manifest` field with derived `@property` accessors
- ✅ Fix dispatcher payload race: pre-generate ULIDs before `enqueue_many()` (qler + dagler)

**Scale testing results:**
- ✅ Fan-out scaling: 100, 500, 1K, **5K** — all exact results (S31, 17 checks)
- ✅ **5K fan-out: 69.8s, 72 items/s** — resolved by qler batch dep edges + pre-fetch keys + sqler save_many dedup
- ✅ Concurrent runs: 10 simultaneous DAGs, result isolation verified, no CID cross-contamination (S32, 22 checks)
- ✅ Multi-stage depth: 2-stage pipeline (100→map→reduce→50→map→reduce), exact cascading results (S33, 10 checks)
- ✅ Memory leak detection: 10 cycles, stable RSS (~161 MB), zero growth in plateau (S34, 21 checks)
- ✅ Concurrency sweep: c=1, c=2, c=4 — WAL contention documented, all results exact (S35, 13 checks)
- 376/376 checks passing (S16 procler health check flake fixed)
- Hardened per test-auditor (per-cycle checks, RSS via /proc, guard assertions, worker cleanup)
- See [spec](./specs/m7-dagler-scale.md)

**Resolved: 5K fan-out performance wall**
- **Before qler fixes**: 5K exceeded 600s (timeout)
- **After qler fixes** (batch dep edges, pre-fetch keys, sqler save_many dedup): **69.8s, 72 items/s**
- Scaling curve: 100→1.1s (90/s), 500→4.1s (122/s), 1K→8.6s (116/s), **5K→69.8s (72/s)**
- Throughput drops at 5K (72 vs 116 items/s at 1K) — likely WAL pressure at scale, but well within budget

**Known finding: WAL contention at c=4 (dagler fan-out only)**
- Concurrency sweep shows c=2 is not reliably faster than c=1 at n=1000 fan-out
- c=4 often slower than c=1 due to SQLite WAL lock contention
- **This does NOT apply to qler direct**: M5 proved c=4→3,776 j/min, c=8→3,909 j/min for order pipeline jobs
- The difference is dagler's fan-out pattern: `enqueue_many(1000)` is a massive batch write, then 1000 map jobs all write results simultaneously — far more write contention than qler's steady claim→execute→complete cycle
- Optimal concurrency: **qler pipelines → c=4 or c=8**, **dagler fan-out → c=1 or c=2**

### M8: dagler Limit Testing ✅
Pushed dagler into operational edge cases real pipelines hit.

- ✅ Cancel mid-flight fan-out: cancel running DagRun with in-flight map jobs (S36, 11 checks)
- ✅ Retry on fan-out: fail/retry cycle with map-level failures, RetryCompletedRunError (S37, 10 checks)
- ✅ Idempotent DAG submission: idempotency_key dedup, post-completion re-submit (S38, 12 checks)
- ✅ wait() timeout: TimeoutError path, cancel-after-timeout recovery (S39, 9 checks)
- ✅ Concurrent fan-out: 2 runs × 50 items, dispatcher write contention (S40, 11 checks)
- ✅ 10K fan-out: timed out at 600s (17 items/s) — throughput cliff documented (S41, 2 checks)
- 55 checks across 6 sections, hardened per test-auditor
- Fixed 3 dagler bugs: cancel() missing dynamic jobs, schema before idempotency query, cancel status force-set
- See [spec](./specs/m8-dagler-limits.md)

### M9: Koyeb Free Tier Viability ✅
Proved the -ler stack runs on Koyeb free tier (512 MB RAM, 0.1 vCPU). No logler on Koyeb — it stays local/CI only.

- ✅ HTTP server baseline: FastAPI + qler Worker + SQLite = 67 MB idle, 77 MB peak under 60-min soak
- ✅ Constrained PRAGMAs: cache_size=-8000 (8 MB), mmap_size=0 for 512 MB budget
- ✅ koyeb_budget.py: 4-phase test (idle, burst, archival, soak) — 8/8 checks pass, 435 MB headroom
- ✅ Soak under cgroup constraints (512 MB RAM, 0.1 vCPU CPUQuota=10%)
- ✅ Real throughput at 0.1 vCPU: ~320 jobs/min sustained
- ✅ stress.py soak mode: stable worker (no churn), configurable trickle rate, hourly metrics, archival
- ✅ 6h soak: 163K jobs, 453 j/min sustained, throughput stable across all 6 hours
- ✅ RSS: 102→197 MB over 6h (orphan accumulation, not runtime leak — archival keeps main table bounded)
- ⬚ Deploy to actual Koyeb free tier (deferred — viability proven, deployment is operational)

### M10: laneler — Async MOBA on the -ler Stack ✅
A real 1v1 game server proving sqler + qler + FastAPI works together as a complete product.

- ✅ 7 sqler models: Player, Game, Hero, Creep, Base, Command, GameTick
- ✅ 3 hero types: Blade (assassin), Bolt (mage), Bastion (tank) with 6 unique skills
- ✅ Tick engine: self-chaining qler jobs, command resolution, creep AI, combat, economy
- ✅ FastAPI: 16 endpoints — auth, lobby, game state, commands, tick replay, full history
- ✅ Auth: argon2 passphrase hashing, bearer tokens, rate limiting
- ✅ Lobby flow: create → join → assign heroes → start (full state machine)
- ✅ Game mechanics: movement, abilities, cooldowns, creep spawning, base damage, AFK forfeit
- ✅ Economy: passive gold, kill bounties, XP, hero leveling, creep upgrades, potions
- ✅ Win conditions: base destroyed, tick cap (20), AFK forfeit
- ✅ Single-page HTML UI: CSS grid board, emoji units, fetch polling, command panel
- ✅ 79 checks across 10 sections, hardened per test-auditor + security-auditor
- ✅ Bot soak: 2 games, 0 errors, 67→69 MB RSS (+2 MB), avg tick interval matches target
- ✅ Security fixes: hero_id=0 auth bypass, safe int parsing in tick resolvers
- See [spec](./specs/m10-laneler.md)

**How to run:**

```bash
# Integration tests (3s ticks, ~3 min)
uv run python tests/test_laneler.py

# Bot soak test (2 games, 60s)
uv run python tests/laneler_bot.py --games 2 --duration 60 --tick-delay 3

# Standalone server (browser UI at http://localhost:PORT)
LANELER_TICK_DELAY=30 uv run python -m tests.laneler

# Koyeb-constrained soak
systemd-run --user --scope -p MemoryMax=512M -p CPUQuota=10% \
  uv run python tests/laneler_bot.py --games 2 --duration 60
```

**Audit findings (documented, not blocking M10):**

Security (from security-auditor):
- CRITICAL-1: Login/register scans all players for argon2 verification (O(N) per auth). Acceptable for game scale (<1K players). Would need username-indexed lookup for real scale.
- HIGH-1: Rate limiting uses `request.client.host` — needs proxy header config for Koyeb deployment.
- HIGH-2: Any authenticated player can read any game's state (spectator by default). Not a problem for this game but noted.
- HIGH-4: No optimistic locking on lobby state transitions — double-start race possible under concurrent requests.

Test quality (from test-auditor):
- Damage values, cooldown enforcement, respawn mechanics are exercised but not asserted with exact values.
- Storm skill (AoE) has no dedicated test coverage.
- Many HTTP-200-as-proxy-for-correctness checks could be strengthened with post-tick state assertions.
- Coverage is ~55% of API surface, ~20% of game-mechanic logic.

### M11: laneler Balance — Automated Game Tuning ✅
Co-evolutionary balancing: genetic algorithm tunes ~30 game constants while bot archetypes play minimax tournaments. Two nested loops — inner loop plays games, outer loop mutates stats and selects for game quality.

**Phase 1: Infrastructure** ✅
- Pure game engine (`engine.py`): tick resolution as stateless functions, StatVector with 30 tunable params
- Bot AI (`bot.py`): depth-2 minimax with beam search, archetype weight profiles
- GA framework (`balance.py`): tournament runner, fitness function, mutation/crossover/selection
- CLI (`laneler_balance.py`): demo mode, generation reporting, JSON output
- Smoke tested: 3 gen x 3 pop x 6 games runs to completion, fitness scoring works

**Phase 2: Tuning & Iteration** ✅
- Added greedy bot mode (~3x faster per game for GA evaluation)
- Widened initial population diversity (multi-round mutation, extreme seeds)
- Greedy GA → minimax GA → 120-game validation iteration loop (4 GA runs total)
- Farmer archetype dropped: structurally unviable (28.7% win rate at 120 games, creep economy 7x less profitable than hero bounties)
- Command fairness fix: randomized tie-breaking prevents team A first-mover advantage
- **Final result (2 archetypes: aggro + splitter):**
  - 120-game minimax validation: **4/4 criteria passing**
  - Win rates: aggro 50.8%, splitter 49.2%
  - Median ticks: 14.0, decisive: 100%, comeback: 30.8%
  - Key stat changes: blade glass cannon (HP 6→4, execute 6→9), bolt tanky (HP 6→9), tougher creeps/bases, higher dmg/level
- Report: `tests/balance_report.json`
- See [spec](./specs/m11-laneler-balance.md)

---

## Memory Leak Roadmap

### Confirmed findings (from M5 10K-order OOM)

The 10,000-order test OOMed at **5.7 GB RSS** on a 10 GB machine. Root cause breakdown:

| Consumer | Estimated size | Is production risk? | Fix location |
|----------|---------------|--------------------|----|
| **logler Investigator** (in-memory DuckDB) | ~2-3 GB | **YES** — any code using Investigator at scale | **logler** — disk-backed DuckDB, streaming load |
| **Test `order_jobs` dict** (80K Job objects) | ~2-3 GB | No — test artifact only | stress.py — stream assertions, don't hold all |
| **Test `_pipeline_timing` dict** | ~200 MB | No — test artifact only | stress.py — drop after verification |
| Python + asyncio overhead | ~500 MB | Low — fixed cost | — |

**Key insight:** qler itself processed 80,000 jobs at sustained ~2,720 jobs/min with no throughput degradation. The OOM came from logler analysis + test infrastructure, not from qler's runtime.

### Known risks (profiling still needed for qler runtime)

| Risk | Source | Severity | Fix |
|------|--------|----------|-----|
| **logler Investigator memory** | All-in-memory DuckDB, no streaming | **CONFIRMED HIGH** | Disk-backed DuckDB, streaming file load |
| **Connection pool leaks** | Any new raw `execute()` without `auto_commit()` | High | Pool health check assertion |
| **Job object accumulation** | Main table grows without archival | Medium | Automatic archival sweep |
| **asyncio task references** | Worker creates tasks per job; cancellation cleanup | Medium | Audit `_execute_job` lifecycle |
| **logler log growth** | Log entries grow without bound | Medium | Log rotation or max-size handler |
| **SQLite mmap pressure** | `mmap_size=256MB` default | Low | Make configurable |

### Measurement plan

1. **Baseline RSS**: stress.py now tracks RSS (RSSTracker class, 5s intervals) — data collected automatically
2. **Growth curve**: Run 10×100-order batches sequentially (same process), measure RSS after each
3. **Leak detection**: If RSS grows > 20% between batch 1 and batch 10, there's a leak
4. **tracemalloc snapshot**: Capture top allocators at batch 1 vs batch 10

### Success criteria for Koyeb free tier (512MB)

- qler Worker RSS stays under 200MB for 1000+ orders processed (without logler analysis)
- logler Investigator RSS stays under 300MB with disk-backed DuckDB
- No OOM after 24h of continuous operation (soak test)
- Automatic archival keeps main table under 1000 rows
- Pool health check passes after every Worker shutdown
