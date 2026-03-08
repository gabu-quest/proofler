# M2: Order Processing Pipeline Under Chaos

## What This Is

A realistic integration test that simulates an e-commerce order processing backend. 100+ orders flow through a multi-stage pipeline with dependency chains, injected failures, timeouts, and worker churn. At the end, logler reconstructs the full story of every order from correlation IDs alone.

This is the test that makes someone reading the blog post think: "that's my checkout flow."

---

## The Pipeline

Each order spawns a chain of dependent jobs:

```
validate_order ──→ charge_payment ──┬──→ send_confirmation
                                    ├──→ update_inventory ──→ notify_warehouse
                                    └──→ generate_invoice
```

- `validate_order`: Checks order data. ~10ms. Fails ~3% (bad address, missing fields).
- `charge_payment`: Processes payment. ~50-150ms. Fails ~12% (declined, timeout). This is the main chaos injector.
- `send_confirmation`: Sends email. ~20ms. Depends on payment. Fails ~2%.
- `update_inventory`: Decrements stock. ~30ms. Depends on payment. Fails ~1%.
- `notify_warehouse`: Sends pick list. ~15ms. Depends on inventory.
- `generate_invoice`: Creates PDF. ~40ms. Depends on payment. Fails ~1%.

Total: 6 tasks per order. 100 orders = 600 jobs minimum (more with retries).

---

## Chaos Injection

### Failure modes
- **Payment decline**: `charge_payment` raises `PaymentDeclinedError`. ~12% of orders. Terminal — no retry.
- **Transient failure**: Random tasks raise `TransientError`. Auto-retried (max_retries=2, retry_delay=0.5).
- **Timeout**: `charge_payment` occasionally sleeps past its timeout (timeout=2s). ~3% of orders.

### Worker churn
- Start worker A (concurrency=16)
- After ~40% of jobs claimed, shut down worker A gracefully
- Start worker B (concurrency=16) — picks up where A left off via lease recovery
- Proves: no jobs lost, no double-execution, in-flight jobs recovered

### What we DON'T inject
- No SQLite corruption, no disk failures, no OOM — those are infrastructure problems, not application-level chaos.

---

## Correlation ID Strategy

Each order gets a CID: `order-{order_number}` (e.g., `order-0042`).

All 6 jobs for that order are enqueued inside `correlation_context(f"order-{n}")`. The CID flows:
1. From the enqueue call into `Job._correlation_id`
2. Through the logler `JsonHandler` during enqueue logging
3. Into the qler DB as a field on the Job record
4. Through `db_to_jsonl()` into logler-readable JSONL
5. Into the Investigator's search index

At the end, `inv.search(correlation_id="order-0042")` should return every log entry AND every job record for that order — the full trace.

---

## Assertions (What We Prove)

### Pipeline correctness
1. **Dependency ordering**: No downstream job executes before its parent completes (timestamp proof, not just status)
2. **Cascade cancellation**: When `charge_payment` fails terminally, ALL downstream jobs (`send_confirmation`, `update_inventory`, `notify_warehouse`, `generate_invoice`) are cancelled
3. **Successful orders**: Orders where all tasks succeed have exactly 6 completed jobs
4. **Partial failures**: Orders with transient failures that recovered have completed jobs + retry attempts logged

### Scale
5. **Throughput**: 600+ jobs processed in < 30s wall time with concurrency=16
6. **No lost jobs**: `pending_count == 0` after full run (everything either completed, failed, or cancelled)
7. **No double execution**: Each job's ULID appears exactly once in the results
8. **Worker handoff**: Jobs that were in-flight when worker A stopped are completed by worker B

### Observability (the logler payoff)
9. **Per-order trace**: Pick 5 random successful orders — `inv.search(correlation_id=cid)` returns entries from both log file and DB
10. **Per-order trace depth**: Each successful order's trace has >= 6 entries (one per task) plus log entries
11. **Failed order diagnosis**: Pick a failed order — the trace shows exactly which task failed and why (error message in the entry)
12. **Cross-source consistency**: Log file entries and DB entries for the same CID agree on timeline (log timestamps < job completion timestamps, roughly)
13. **Pipeline-wide stats**: `extract_metrics()` on the log file shows duration distributions across task types
14. **Error rate verification**: logler SQL query `SELECT level, COUNT(*)` matches expected ~15% failure rate (within tolerance)
15. **Follow thread**: `follow_thread(correlation_id=cid)` for a successful order returns entries in chronological order matching the dependency chain

---

## Test Structure

Single new section in `test_stack.py`:

```
Section 34: Order Processing Pipeline Under Chaos
```

### Setup phase (~1s)
- Create 100 orders with deterministic seed (reproducible chaos)
- Register all 6 task functions at module level
- Set up JsonHandler + CorrelationFilter for pipeline logging
- Enqueue all 600+ jobs with dependency chains and CIDs

### Execution phase (~15-25s)
- Start worker A (concurrency=16), run for ~8s
- Shut down worker A gracefully
- Call `recover_expired_leases()` for any in-flight jobs
- Start worker B (concurrency=16), run until all jobs resolved or 20s timeout

### Verification phase (~2s)
- Pipeline correctness assertions (1-4)
- Scale assertions (5-8)

### Observability phase (~3s)
- Convert qler DB to JSONL
- Load log file + JSONL into Investigator
- Observability assertions (9-15)

### Cleanup
- Close handlers, remove temp files

---

## Module-Level Task Functions

```python
# Deterministic chaos via seeded random per order
def validate_order(order_id: int, seed: int) -> dict: ...
def charge_payment(order_id: int, amount: float, seed: int) -> dict: ...
def send_confirmation(order_id: int, email: str) -> dict: ...
def update_inventory(order_id: int, items: list) -> dict: ...
def notify_warehouse(order_id: int, warehouse: str) -> dict: ...
def generate_invoice(order_id: int, amount: float) -> dict: ...
```

Each returns a dict with `{"order_id": N, "task": "task_name", "status": "ok"}` on success or raises on failure. The seed ensures the same orders fail every run.

---

## Success Criteria

- All assertions pass deterministically (seeded RNG, no flaky timing)
- Wall time < 30s for the full section
- Blog-post reader can follow the narrative: "100 orders → chaos → logler shows me what happened"
- No new dependencies needed (everything uses existing sqler/qler/logler APIs)

---

## Non-Goals

- No HTTP layer (that's procler/botler territory)
- No real payment processing or external APIs
- No persistent state between test runs
- No benchmarking (we measure throughput but don't optimize for it)
