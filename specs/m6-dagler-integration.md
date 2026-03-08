# M6: dagler Integration Tests

## Goal

Prove dagler's DAG pipeline orchestration works end-to-end on top of qler + sqler with real on-disk WAL-mode SQLite. Test the **seams**: dagler submission → qler Worker execution → result injection through qler's dependency resolution → logler observability via correlation_id.

## What we test (the seams)

- Real on-disk WAL-mode SQLite (not in-memory)
- `result_json` round-trip through qler Worker
- DagRun lifecycle from Worker-driven transitions
- `db_to_jsonl()` on a DB with `dagler_runs` table
- `correlation_id` threading dagler → qler → logler

## What we DON'T re-test

dagler's unit tests already cover cycle detection, validation, decorators, DagRun status computation, cancel/retry edge cases, fan-out dispatcher mechanics, and CLI.

## Sections (6 sections, ~54 checks)

### Section 25: Linear Pipeline (9 checks)
`dag_extract → dag_transform → dag_load` — validates basic DAG submission, Worker execution, result injection between tasks, and DagRun lifecycle.

### Section 26: Diamond DAG + Multi-Parent Injection (10 checks)
`dag_root → (dag_branch_a, dag_branch_b) → dag_merge` — validates diamond dependency wiring, multi-parent result injection, and concurrent branch execution.

### Section 27: Failure Cascade + Retry (10 checks)
`dag_step_ok → dag_step_fail → dag_step_downstream` — validates failure propagation (cascade cancel), DagRun failure state, `run.retry()` reset, and recovery via second Worker.

### Section 28: Fan-Out/Reduce (10 checks)
`dag_fetch_items → (map)dag_process_item → (reduce)dag_collect_results → dag_summarize` — validates dynamic job creation by dispatcher, map/reduce result collection, and trailing task execution.

### Section 29: logler Observability (7 checks)
Reuses ETL tasks. Validates `db_to_jsonl()` on dagler DB, Investigator CID search, chronological thread ordering, and `dagler_runs` table auto-detection safety.

### Section 30: Full Stack Roundtrip (8 checks)
`dag_ingest → dag_validate_records → (map)dag_enrich_record → (reduce)dag_aggregate → dag_finalize` — validates linear + fan-out + post-reduce in one DAG, with logler CID tracing.

## Key API patterns

- `dag.run(queue, correlation_id=...)` → DagRun (calls `register()` + `validate()` internally)
- `run.wait(timeout=...)` → blocks until all jobs terminal (multi-wave for fan-out)
- `run.retry(queue)` → resets failed/cancelled jobs to pending
- `run.job_map["task_name"].result` → deserialized task return value
- `DAG._registry.clear()` between sections to avoid name collisions
- Fan-out DAGs start with N static + 1 dispatcher jobs; dynamic jobs added at runtime
