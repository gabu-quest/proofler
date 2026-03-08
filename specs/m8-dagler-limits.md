# M8: dagler Limit Testing

## Goal

Push dagler into operational edge cases that real pipelines will hit: cancellation, retry on fan-out, idempotent submission, timeout handling, concurrent fan-out contention, and 10K+ scale.

## Sections

### S36: Cancel Mid-Flight Fan-Out (~10 checks)
- DAG: produce 20 items → slow_map (2s each) → reduce
- Cancel while map jobs are in-flight
- Assert: cancelled status, pending maps cancelled, dispatcher completed, DB consistent

### S37: Retry on Fan-Out + Error (~12 checks)
- DAG: produce 5 items → map (item==2 fails first attempt) → reduce
- Run → fail → retry → complete
- Assert: exact reduce result after recovery, RetryCompletedRunError tested

### S38: Idempotent DAG Submission (~11 checks)
- DAG: 3-task linear (reuses S25 task functions)
- Submit twice with same idempotency_key → same DagRun returned
- DB: exactly 1 DagRun, exactly 3 Jobs (not doubled)
- Post-completion re-submission returns completed run
- Different key → new independent run

### S39: wait() Timeout Path (~9 checks)
- DAG: instant_ok → never_finishes (600s sleep)
- wait(timeout=3.0) → TimeoutError
- Assert: not completed, finished_at is None, instant_ok completed
- Cancel after timeout → cancelled (recovery pattern)

### S40: Concurrent Fan-Out DAG Runs (~13 checks)
- Same DAG, 2 concurrent runs (50 items each), shared Worker
- asyncio.gather on both waits
- Assert: both completed, exact reduce results, CID isolation, no ULID overlap
- DB: 2 DagRun rows, 106 total Jobs

### S41: 10K Fan-Out (~6 checks)
- Reuses M7 task functions (produce_n, map_square, reduce_sum)
- 600s budget, concurrency=2
- If timeout: recorded as performance finding (not failure)
- If complete: exact result, job count, throughput

## Known Bug

**cancel() misses dynamic fan-out jobs** — `cancel()` iterates `self._jobs` (static only) without re-fetching dynamic jobs from DB. Fix needed in dagler `run.py:cancel()`: call `refresh_status()` before iterating.

## Dependencies

- dagler cancel() fix required for S36
- No other dependency changes expected

## Success Criteria

- All non-timeout checks pass
- S36 exposes and validates the cancel() fix
- S41 records 10K throughput (pass or finding)
