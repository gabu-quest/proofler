# laneler / proofler task runner

# Start laneler game server on LAN (port 8888)
game tick_delay="30":
    LANELER_TICK_DELAY={{tick_delay}} LANELER_PORT=8888 LANELER_HOST=0.0.0.0 uv run python -m tests.laneler

# Run laneler integration tests (79 checks)
test-game:
    uv run python tests/test_laneler.py

# Run main stack integration tests (430+ checks)
test:
    uv run python tests/test_stack.py

# Bot soak test (2 games, minimax bots)
soak games="2" duration="60" tick_delay="3":
    uv run python tests/laneler_bot.py --games {{games}} --duration {{duration}} --tick-delay {{tick_delay}}

# Balance demo: watch aggro vs splitter play a game
demo:
    uv run python tests/laneler_balance.py --demo aggro splitter --seed 42

# Run balance GA (minimax, ~2 hours)
balance generations="15" population="10" games="30":
    uv run python tests/laneler_balance.py --generations {{generations}} --population {{population}} --games-per-eval {{games}}
