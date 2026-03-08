# M7: dagler Scale Stress Testing

## Goal

Find dagler's limits on local on-disk WAL-mode SQLite. Push fan-out from 100 items (M6 max) to 5K. Track memory. Sweep concurrency. Prove isolation under contention.

## Sections

### Section 31: Fan-Out Scaling Curve (~16 checks)

Fan-out at 100, 500, 1K, 5K items. Same pattern: produce → map(square) → reduce(sum). Per scale point:

- `run.status == "completed"`
- `len(run.job_ulids) == n + 3` (produce + dispatcher + n maps + reduce)
- Exact reduce result: `sum(i*i for i in range(n))`
- Elapsed within timeout budget (100→30s, 500→60s, 1K→120s, 5K→300s)

### Section 32: Concurrent DAG Runs (~8 checks)

10 concurrent small DAGs (3 linear tasks each), all submitted before any wait. `asyncio.gather()` on all waits.

- All 10 completed
- Per-run result isolation (exact seed-derived values)
- DB-level: exactly 10 DagRun rows, 30 Job rows
- CID isolation: no job shares CID across runs

### Section 33: Multi-Stage Depth (~8 checks)

2-stage pipeline: produce 100 items → map → reduce → produce 50 items from reduce → map → reduce. Proves cascading dispatcher waves work at scale.

- Stage 1 map count == 100
- Stage 2 map count == 50
- Exact final reduce result
- All dispatchers completed
- Total job count exact

### Section 34: Memory Leak Detection (~5 checks)

Run 100-item fan-out DAG 10 times sequentially, same process, same DB. Track RSS via `resource.getrusage()`.

- All 10 cycles complete with correct results
- RSS growth first→last < 50 MB
- Plateau check: cycles 5-10 growth < 20 MB
- Print RSS curve as INFO

### Section 35: Worker Concurrency Sweep (~8 checks)

1K-item fan-out at c=1, c=2, c=4. Fresh DagRun per concurrency level.

- All 3 complete with exact result
- c=4 faster than c=1 (proves concurrency helps)
- Print throughput table as INFO

## Task Functions (m7_ prefix)

- `m7_produce_n(n=100)` — return `list(range(n))`
- `m7_map_square(item)` — return `item * item`
- `m7_reduce_sum(results)` — return `sum(results)`
- `m7_gen_data(seed=0)` — return `{"seed": seed, "value": seed * 7}`
- `m7_transform(m7_gen_data=None)` — return `{"doubled": m7_gen_data["value"] * 2}`
- `m7_store(m7_transform=None)` — return `{"stored": True, "value": m7_transform["doubled"]}`
- `m7_produce_100()` — return `list(range(100))`
- `m7_s1_map(item)` — return `item * item`
- `m7_s1_reduce(results)` — return first 50 items of results
- `m7_s2_map(item)` — return `item + 1`
- `m7_s2_reduce(results)` — return `sum(results)`

## Success Criteria

- All 5 sections pass with exact value assertions
- 5K fan-out completes within 300s
- No memory leak detected (RSS growth < 50 MB over 10 cycles)
- c=4 outperforms c=1 at 1K items
