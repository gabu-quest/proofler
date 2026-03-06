#!/bin/bash
# Multi-worker benchmark suite — tests with --workers flag
# Run AFTER _benchmark_suite.sh completes (shares same DB patterns)
set -e
cd "$(dirname "$0")/.."

echo "============================================================"
echo "  MULTI-WORKER BENCHMARK SUITE — $(date)"
echo "============================================================"
echo ""

# --- Test 1: 1000 orders, 2 workers × c=2 (4 total coroutines) ---
echo ">>> [1/3] 1000 orders, 2 workers × concurrency=2 (4 coroutines), seed=42"
echo "    Started: $(date)"
t0=$(date +%s)
uv run python tests/stress.py --orders 1000 --workers 2 --concurrency 2 --seed 42 2>/dev/null
t1=$(date +%s)
echo "    Finished: $(date) ($(( t1 - t0 ))s)"
echo ""

# --- Test 2: 1000 orders, 4 workers × c=2 (8 total coroutines) ---
echo ">>> [2/3] 1000 orders, 4 workers × concurrency=2 (8 coroutines), seed=42"
echo "    Started: $(date)"
t0=$(date +%s)
uv run python tests/stress.py --orders 1000 --workers 4 --concurrency 2 --seed 42 2>/dev/null
t1=$(date +%s)
echo "    Finished: $(date) ($(( t1 - t0 ))s)"
echo ""

# --- Test 3: 1000 orders, 2 workers × c=4 (8 total coroutines) ---
echo ">>> [3/3] 1000 orders, 2 workers × concurrency=4 (8 coroutines), seed=42"
echo "    Started: $(date)"
t0=$(date +%s)
uv run python tests/stress.py --orders 1000 --workers 2 --concurrency 4 --seed 42 2>/dev/null
t1=$(date +%s)
echo "    Finished: $(date) ($(( t1 - t0 ))s)"
echo ""

echo "============================================================"
echo "  MULTI-WORKER SUITE COMPLETE — $(date)"
echo "============================================================"
