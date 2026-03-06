#!/bin/bash
# Sequential benchmark suite — run all stress tests back-to-back
# Each test gets its own temp DB so no contamination
set -e
cd "$(dirname "$0")/.."

echo "============================================================"
echo "  BENCHMARK SUITE — $(date)"
echo "============================================================"
echo ""

# --- Test 1: 1000 orders, c=4 ---
echo ">>> [1/4] 1000 orders, concurrency=4, seed=42"
echo "    Started: $(date)"
t0=$(date +%s)
uv run python tests/stress.py --orders 1000 --concurrency 4 --seed 42 2>/dev/null
t1=$(date +%s)
echo "    Finished: $(date) ($(( t1 - t0 ))s)"
echo ""

# --- Test 2: 1000 orders, c=8 ---
echo ">>> [2/4] 1000 orders, concurrency=8, seed=42"
echo "    Started: $(date)"
t0=$(date +%s)
uv run python tests/stress.py --orders 1000 --concurrency 8 --seed 42 2>/dev/null
t1=$(date +%s)
echo "    Finished: $(date) ($(( t1 - t0 ))s)"
echo ""

# --- Test 3: 10000 orders, c=2 ---
echo ">>> [3/4] 10000 orders, concurrency=2, seed=42"
echo "    Started: $(date)"
t0=$(date +%s)
uv run python tests/stress.py --orders 10000 --concurrency 2 --seed 42 2>/dev/null
t1=$(date +%s)
echo "    Finished: $(date) ($(( t1 - t0 ))s)"
echo ""

# --- Test 4: 10000 orders, c=4 ---
echo ">>> [4/4] 10000 orders, concurrency=4, seed=42"
echo "    Started: $(date)"
t0=$(date +%s)
uv run python tests/stress.py --orders 10000 --concurrency 4 --seed 42 2>/dev/null
t1=$(date +%s)
echo "    Finished: $(date) ($(( t1 - t0 ))s)"
echo ""

echo "============================================================"
echo "  SUITE COMPLETE — $(date)"
echo "============================================================"
