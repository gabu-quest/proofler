# Roadmap: proofler

## Milestones

### M1: Initial Release + Benchmark Baseline ✅
- Lifted from lerproof, public repo at gabu-quest/proofler
- deps/ symlink approach for dependency paths (zero personal paths in tracked files)
- logler installs from PyPI (no maturin build step needed)
- 41 sections, 430+ checks, all passing
- Benchmark baseline on current hardware:
  - 200 orders: 860 jobs/min, 62 MB RSS
  - 1,000 orders: 2,145 jobs/min, 152 MB RSS
  - 10,000 orders: 1,245 jobs/min, 1,370 MB RSS (processing), 50/50 checks

### M2: Soak Test (24h continuous operation) ⬚
Prove the stack survives long-running production-like workloads.

- Add `--soak` mode to stress.py with realistic trickle rates (not burst)
- Enable qler automatic archival (`--archive-interval`, `--archive-after`)
- Run 24h soak: steady ~100 jobs/hour, archival keeping active table under 1K rows
- Track RSS every 60s, assert no growth over 24h
- Track throughput per hour, assert no degradation
- Track archival table size, assert bounded active table
- Success criteria: zero data loss, zero memory growth, zero throughput degradation over 24h

### M3: Koyeb Memory Budget Test (512 MB ceiling) ⬚
Prove the stack fits in Koyeb free tier constraints.

- Minimal HTTP server (FastAPI or Starlette; both already in dep tree) with:
  - `GET /health` — returns server + qler worker status
  - `POST /enqueue` — submits a job
  - `GET /status/:id` — returns job status
- qler Worker running in-process alongside the HTTP server
- Constrained PRAGMAs: `cache_size=8MB`, `mmap_size=0`, `pool_size=1-2`, `concurrency=1`
- RSS budget: server + worker < 200 MB idle, < 400 MB under load
- Test sequence:
  1. Measure idle RSS (server + worker, no jobs)
  2. Process 1K jobs, measure peak RSS
  3. Enable archival, process another 1K, verify RSS returns to idle baseline
  4. 1h soak at ~50 jobs/min, verify RSS stays under 400 MB
- If too heavy: evaluate Starlette alone (drop FastAPI/Pydantic overhead) or a minimal "servler"
- Success criteria: entire stack fits in 512 MB with headroom for the OS

### M4: Koyeb Deploy + 24h Soak ⬚
Actually deploy to Koyeb free tier and run the soak test on real infrastructure.

- Dockerfile or Procfile for Koyeb deployment
- Health check endpoint for Koyeb's probe
- Deploy: HTTP server + qler worker + SQLite on persistent volume
- 24h soak test within memory budget
- Monitor via Koyeb dashboard: RSS, restarts, health check failures
- Success criteria: zero restarts, zero OOMs, zero data loss over 24h
