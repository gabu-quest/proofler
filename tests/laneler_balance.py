"""CLI entry point for laneler balance tuning.

Usage:
    # Quick smoke (5 min)
    uv run python tests/laneler_balance.py --generations 5 --population 5 --games-per-eval 10

    # Demo: single game with tick-by-tick output
    uv run python tests/laneler_balance.py --demo aggro splitter

    # Full run
    uv run python tests/laneler_balance.py --generations 100 --population 20 --games-per-eval 50

"""

from __future__ import annotations

import argparse
import json
import sys
import os

# Ensure tests/ is on path for package imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from laneler.engine import StatVector, create_game, resolve_tick
from laneler.bot import Bot, ARCHETYPES
from laneler.balance import BalanceRunner, play_game, _random_assignment


def run_demo(arch_a: str, arch_b: str, seed: int | None = None) -> None:
    """Play a single game and print tick-by-tick events."""
    import random
    if seed is not None:
        random.seed(seed)

    stats = StatVector()
    bot_a = Bot(arch_a)
    bot_b = Bot(arch_b)
    assign_a = _random_assignment()
    assign_b = _random_assignment()

    print(f"=== Demo: {arch_a} (A) vs {arch_b} (B) ===")
    print(f"Team A: {assign_a}")
    print(f"Team B: {assign_b}")
    print()

    game = create_game(stats, assign_a, assign_b)

    while not game.finished:
        cmds_a = bot_a.decide(game, "a")
        cmds_b = bot_b.decide(game, "b")
        events = resolve_tick(game, cmds_a + cmds_b)

        print(f"--- Tick {game.tick} ---")
        for cmd in cmds_a:
            print(f"  A: {cmd.action} hero={cmd.hero_id} {cmd.args}")
        for cmd in cmds_b:
            print(f"  B: {cmd.action} hero={cmd.hero_id} {cmd.args}")

        for e in events:
            etype = e.get("type", "?")
            if etype in ("move", "dash"):
                print(f"  >> {etype}: {e['hero']} -> tile {e['to']}")
            elif etype == "damage":
                print(f"  >> damage: {e['target']} takes {e['amount']} (HP={e['remaining_hp']})")
            elif etype == "death":
                print(f"  >> DEATH: {e['hero']} ({e['team']})")
            elif etype == "respawn":
                print(f"  >> respawn: {e['hero']} ({e['team']})")
            elif etype in ("execute", "zap", "storm"):
                print(f"  >> {etype}: {e['hero']} dmg={e.get('dmg', '?')}")
            elif etype == "shield":
                print(f"  >> shield: {e['hero']}")
            elif etype == "fortify":
                print(f"  >> fortify: {e['hero']}")
            elif etype == "base_damage":
                print(f"  >> base damage: {e['team']} {e['lane']} HP={e['hp']}")
            elif etype == "base_destroyed":
                print(f"  >> BASE DESTROYED: {e['team']} {e['lane']}")
            elif etype == "game_over":
                print(f"  >> GAME OVER: winner={e['winner']} reason={e['reason']}")
            elif etype == "level_up":
                print(f"  >> LEVEL UP: {e['hero']} ({e['team']}) -> lv{e['level']}")

        # Board summary
        for h in game.heroes:
            marker = " (DEAD)" if h.is_dead else ""
            print(f"    {h.team}.{h.name}: {h.lane} tile={h.tile} HP={h.hp}/{h.max_hp} lv{h.level}{marker}")
        print(f"    Gold: A={game.gold['a']} B={game.gold['b']}  XP: A={game.xp['a']} B={game.xp['b']}")
        print(f"    Kills: A={game.kills['a']} B={game.kills['b']}")
        print()

    print(f"=== Result: {'Team A' if game.winner == 'a' else 'Team B'} wins in {game.tick} ticks ===")
    base_hp_a = sum(b.hp for b in game.bases if b.team == "a")
    base_hp_b = sum(b.hp for b in game.bases if b.team == "b")
    print(f"Base HP: A={base_hp_a} B={base_hp_b}")
    print(f"Final kills: A={game.kills['a']} B={game.kills['b']}")


def run_balance(args: argparse.Namespace) -> None:
    """Run the GA balance loop."""
    def on_generation(gen: int, entry: dict) -> None:
        wr = entry["win_rates"]
        wr_str = " ".join(f"{k}={v:.0%}" for k, v in wr.items())
        print(
            f"Gen {gen:3d}/{args.generations}  "
            f"fitness={entry['best_fitness']:.4f}  "
            f"ticks={entry['median_ticks']:.1f}  "
            f"decisive={entry['decisive_pct']:.0%}  "
            f"comeback={entry['comeback_pct']:.0%}  "
            f"{wr_str}  "
            f"({entry['elapsed_s']:.1f}s)"
        )

    runner = BalanceRunner(
        pop_size=args.population,
        games_per_eval=args.games_per_eval,
        generations=args.generations,
        seed=args.seed,
        on_generation=on_generation,
        greedy=args.greedy,
    )

    mode = "greedy" if args.greedy else "minimax"
    print(f"Starting balance run: {args.generations} generations, "
          f"{args.population} candidates, {args.games_per_eval} games/eval, {mode} bots")
    print()

    report = runner.run()

    # Save report
    output = args.output
    with open(output, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nReport saved to {output}")

    # Summary
    print(f"\n=== Final Results ===")
    print(f"Best fitness: {report['best_fitness']}")
    t = report["tournament"]
    print(f"Median ticks: {t['median_ticks']}")
    print(f"Decisive: {t['decisive_pct']:.0%}")
    print(f"Win rates: {t['win_rates']}")
    print(f"Comeback: {t['comeback_pct']:.0%}")
    print(f"Total time: {report['total_elapsed_s']:.0f}s")

    # Show tuned stats diff from defaults
    defaults = StatVector()
    tuned = report["stats"]
    print(f"\nStat changes from defaults:")
    for key, val in tuned.items():
        default_val = getattr(defaults, key, None)
        if default_val is not None and val != default_val:
            print(f"  {key}: {default_val} -> {val}")


def main() -> None:
    parser = argparse.ArgumentParser(description="laneler balance tuning")
    parser.add_argument("--generations", type=int, default=100)
    parser.add_argument("--population", type=int, default=20)
    parser.add_argument("--games-per-eval", type=int, default=50)
    parser.add_argument("--output", type=str, default="balance_report.json")
    # --resume: future feature (checkpoint save/restore)
    parser.add_argument("--demo", nargs=2, metavar=("ARCH_A", "ARCH_B"),
                        help="Play single demo game (e.g., --demo aggro splitter)")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--greedy", action="store_true",
                        help="Use greedy bots (faster) instead of minimax")

    args = parser.parse_args()

    if args.demo:
        arch_a, arch_b = args.demo
        if arch_a not in ARCHETYPES or arch_b not in ARCHETYPES:
            print(f"Unknown archetype. Available: {list(ARCHETYPES.keys())}")
            sys.exit(1)
        run_demo(arch_a, arch_b, seed=args.seed)
    else:
        run_balance(args)


if __name__ == "__main__":
    main()
