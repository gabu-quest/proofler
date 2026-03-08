"""Genetic algorithm for laneler balance tuning.

Evolves StatVector parameters across generations, using bot tournaments
as the fitness function. Produces a tuned stat set and balance report.
"""

from __future__ import annotations

import json
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, fields, field
from statistics import median

from .engine import StatVector, GameState, create_game, resolve_tick, STAT_BOUNDS, HERO_NAMES, LANES
from .bot import Bot, BotWeights, ARCHETYPES


# ---------------------------------------------------------------------------
# Tournament results
# ---------------------------------------------------------------------------

@dataclass
class TournamentResults:
    games_played: int = 0
    median_ticks: float = 0.0
    decisive_pct: float = 0.0
    win_rates: dict[str, float] = field(default_factory=dict)
    comeback_pct: float = 0.0
    lane_activity: dict[str, float] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Fitness scoring helpers
# ---------------------------------------------------------------------------

def _length_score(median_ticks: float) -> float:
    """1.0 if median in [10, 16], drops off outside."""
    if 10 <= median_ticks <= 16:
        return 1.0
    if median_ticks < 5 or median_ticks > 20:
        return 0.0
    if median_ticks < 10:
        return (median_ticks - 5) / 5
    return (20 - median_ticks) / 4


def _balance_score(win_rates: dict[str, float]) -> float:
    """1.0 if all archetypes between 40-60% win rate."""
    if not win_rates:
        return 0.0
    max_deviation = max(abs(wr - 0.5) for wr in win_rates.values())
    return max(0.0, 1.0 - max_deviation * 5)


def _comeback_score(comeback_pct: float) -> float:
    """1.0 if 20-40%, drops outside."""
    if 0.2 <= comeback_pct <= 0.4:
        return 1.0
    if comeback_pct < 0.1 or comeback_pct > 0.6:
        return 0.0
    if comeback_pct < 0.2:
        return (comeback_pct - 0.1) / 0.1
    return (0.6 - comeback_pct) / 0.2


# ---------------------------------------------------------------------------
# Single game runner
# ---------------------------------------------------------------------------

def play_game(
    stats: StatVector,
    bot_a: Bot,
    bot_b: Bot,
    assign_a: dict[str, str] | None = None,
    assign_b: dict[str, str] | None = None,
    greedy: bool = False,
) -> dict:
    """Play a single game between two bots. Returns game summary."""
    game = create_game(stats, assign_a, assign_b)

    while not game.finished:
        if greedy:
            cmds_a = bot_a.decide_greedy(game, "a")
            cmds_b = bot_b.decide_greedy(game, "b")
        else:
            cmds_a = bot_a.decide(game, "a")
            cmds_b = bot_b.decide(game, "b")
        resolve_tick(game, cmds_a + cmds_b)

    return {
        "winner": game.winner,
        "ticks": game.tick,
        "decisive": any(b.hp <= 0 for b in game.bases),
        "kills": dict(game.kills),
        "first_kill": game.first_kill,
        "gold": dict(game.gold),
        "lane_heroes": _lane_activity(game),
    }


def _lane_activity(game: GameState) -> dict[str, int]:
    """Count heroes per lane at game end."""
    counts: dict[str, int] = {"top": 0, "mid": 0, "bot": 0}
    for h in game.heroes:
        counts[h.lane] = counts.get(h.lane, 0) + 1
    return counts


def _random_assignment() -> dict[str, str]:
    """Random hero-to-lane assignment."""
    lanes = list(LANES)
    random.shuffle(lanes)
    return dict(zip(HERO_NAMES, lanes))


# ---------------------------------------------------------------------------
# Balance runner (GA)
# ---------------------------------------------------------------------------

class BalanceRunner:
    def __init__(
        self,
        pop_size: int = 20,
        games_per_eval: int = 50,
        generations: int = 100,
        mutation_rate: float = 0.15,
        elite_count: int = 2,
        seed: int | None = None,
        on_generation: Callable | None = None,
        greedy: bool = False,
    ):
        self.pop_size = pop_size
        self.games_per_eval = games_per_eval
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.elite_count = elite_count
        self.on_generation = on_generation
        self.greedy = greedy
        if seed is not None:
            random.seed(seed)

    def _init_population(self) -> list[StatVector]:
        """Create diverse initial population: defaults + multi-round mutations + extremes."""
        population = [StatVector()]  # slot 0 = baseline defaults

        # Extreme seeds: all-low, all-high, random-uniform
        if self.pop_size >= 4:
            low = StatVector()
            high = StatVector()
            rand = StatVector()
            for f in fields(StatVector):
                bounds = STAT_BOUNDS.get(f.name)
                if bounds is None:
                    continue
                lo, hi = bounds
                if isinstance(getattr(low, f.name), float):
                    setattr(low, f.name, round(lo, 2))
                    setattr(high, f.name, round(hi, 2))
                    setattr(rand, f.name, round(random.uniform(lo, hi), 2))
                else:
                    setattr(low, f.name, int(lo))
                    setattr(high, f.name, int(hi))
                    setattr(rand, f.name, random.randint(int(lo), int(hi)))
            population.extend([low, high, rand])

        # Fill rest with multi-round mutations from defaults
        while len(population) < self.pop_size:
            candidate = StatVector()
            rounds = random.randint(1, 5)
            for _ in range(rounds):
                candidate = self.mutate(candidate)
            population.append(candidate)

        return population[:self.pop_size]

    def run(self) -> dict:
        """Run the full balance loop. Returns final report dict."""
        population = self._init_population()

        history: list[dict] = []
        best_overall = None
        difficulty_snapshots: dict[str, dict] = {}
        start = time.monotonic()

        for gen in range(self.generations):
            gen_start = time.monotonic()

            fitness_scores: list[tuple[float, StatVector, TournamentResults]] = []
            for candidate in population:
                results = self.run_tournament(candidate)
                score = self.fitness(results)
                fitness_scores.append((score, candidate, results))

            fitness_scores.sort(key=lambda x: -x[0])
            best_score, best_candidate, best_results = fitness_scores[0]

            gen_elapsed = time.monotonic() - gen_start
            entry = {
                "gen": gen,
                "best_fitness": round(best_score, 4),
                "median_ticks": round(best_results.median_ticks, 1),
                "decisive_pct": round(best_results.decisive_pct, 2),
                "win_rates": {k: round(v, 2) for k, v in best_results.win_rates.items()},
                "comeback_pct": round(best_results.comeback_pct, 2),
                "elapsed_s": round(gen_elapsed, 1),
            }
            history.append(entry)

            if best_overall is None or best_score > best_overall[0]:
                best_overall = (best_score, best_candidate, best_results)

            # Difficulty snapshots at 20%, 50%, 100%
            pct = (gen + 1) / self.generations
            if pct >= 0.2 and "easy" not in difficulty_snapshots:
                difficulty_snapshots["easy"] = {"generation": gen, "stats": best_candidate.to_dict()}
            if pct >= 0.5 and "medium" not in difficulty_snapshots:
                difficulty_snapshots["medium"] = {"generation": gen, "stats": best_candidate.to_dict()}

            if self.on_generation:
                self.on_generation(gen, entry)

            # Next generation
            next_pop = [fs[1] for fs in fitness_scores[:self.elite_count]]
            while len(next_pop) < self.pop_size:
                parent_a = self._tournament_select(fitness_scores)
                parent_b = self._tournament_select(fitness_scores)
                child = self.crossover(parent_a, parent_b)
                child = self.mutate(child)
                next_pop.append(child)
            population = next_pop

        total_elapsed = time.monotonic() - start

        if best_overall:
            difficulty_snapshots["hard"] = {
                "generation": self.generations - 1,
                "stats": best_overall[1].to_dict(),
            }

        return self._final_report(best_overall, history, difficulty_snapshots, total_elapsed)

    def run_tournament(self, stats: StatVector) -> TournamentResults:
        """Play round-robin between archetypes."""
        matchups = [
            ("aggro", "splitter"),
            ("splitter", "aggro"),
        ]

        games_per_matchup = max(1, self.games_per_eval // len(matchups))
        results_list: list[dict] = []
        wins: dict[str, int] = {"aggro": 0, "splitter": 0}
        games_as: dict[str, int] = {"aggro": 0, "splitter": 0}

        for arch_a, arch_b in matchups:
            bot_a = Bot(arch_a)
            bot_b = Bot(arch_b)

            for _ in range(games_per_matchup):
                assign_a = _random_assignment()
                assign_b = _random_assignment()
                result = play_game(stats, bot_a, bot_b, assign_a, assign_b, greedy=self.greedy)
                results_list.append({**result, "arch_a": arch_a, "arch_b": arch_b})

                games_as[arch_a] += 1
                games_as[arch_b] += 1
                if result["winner"] == "a":
                    wins[arch_a] += 1
                elif result["winner"] == "b":
                    wins[arch_b] += 1

        # Aggregate
        ticks = [r["ticks"] for r in results_list]
        decisive_count = sum(1 for r in results_list if r["decisive"])
        total_games = len(results_list)

        win_rates = {}
        for arch in ("aggro", "splitter"):
            if games_as[arch] > 0:
                win_rates[arch] = wins[arch] / games_as[arch]
            else:
                win_rates[arch] = 0.5

        # Comeback: games where first_kill loser still wins
        comeback_count = 0
        comeback_eligible = 0
        for r in results_list:
            if r["first_kill"]:
                comeback_eligible += 1
                if r["first_kill"] != r["winner"]:
                    comeback_count += 1
        comeback_pct = comeback_count / comeback_eligible if comeback_eligible > 0 else 0.0

        # Lane activity
        lane_totals: dict[str, int] = {"top": 0, "mid": 0, "bot": 0}
        for r in results_list:
            for lane, count in r.get("lane_heroes", {}).items():
                lane_totals[lane] = lane_totals.get(lane, 0) + count
        lane_activity = {
            lane: total / total_games if total_games > 0 else 0
            for lane, total in lane_totals.items()
        }

        return TournamentResults(
            games_played=total_games,
            median_ticks=median(ticks) if ticks else 0,
            decisive_pct=decisive_count / total_games if total_games > 0 else 0,
            win_rates=win_rates,
            comeback_pct=comeback_pct,
            lane_activity=lane_activity,
        )

    def fitness(self, r: TournamentResults) -> float:
        """Weighted fitness score."""
        length = _length_score(r.median_ticks)
        decisive = r.decisive_pct
        balance = _balance_score(r.win_rates)
        comeback = _comeback_score(r.comeback_pct)

        # Lane activity: score based on how many lanes have hero presence
        lane_vals = list(r.lane_activity.values()) if r.lane_activity else [0, 0, 0]
        active_lanes = sum(1 for v in lane_vals if v >= 1.0)
        lanes = min(1.0, active_lanes / 3)

        return (0.25 * length + 0.25 * decisive + 0.25 * balance +
                0.15 * comeback + 0.10 * lanes)

    def mutate(self, parent: StatVector) -> StatVector:
        """Pick 3-5 random stats, mutate by +/-1 (int) or +/-0.1 (float)."""
        child = StatVector(**parent.to_dict())
        stat_fields = [f for f in fields(StatVector)]
        n_mutations = random.randint(3, 5)
        to_mutate = random.sample(stat_fields, min(n_mutations, len(stat_fields)))

        for f in to_mutate:
            val = getattr(child, f.name)
            bounds = STAT_BOUNDS.get(f.name, (0, 100))

            if isinstance(val, float):
                delta = random.choice([-0.1, 0.1])
                new_val = max(bounds[0], min(bounds[1], val + delta))
                setattr(child, f.name, round(new_val, 2))
            elif isinstance(val, int):
                delta = random.choice([-2, -1, 1, 2])
                new_val = max(int(bounds[0]), min(int(bounds[1]), val + delta))
                setattr(child, f.name, new_val)

        return child

    def crossover(self, a: StatVector, b: StatVector) -> StatVector:
        """Uniform crossover: 50/50 per gene from each parent."""
        d = {}
        for f in fields(StatVector):
            d[f.name] = getattr(a, f.name) if random.random() < 0.5 else getattr(b, f.name)
        return StatVector(**d)

    def _tournament_select(
        self, fitness_scores: list[tuple[float, StatVector, TournamentResults]], k: int = 3,
    ) -> StatVector:
        """Tournament selection: pick k random, return best."""
        contestants = random.sample(fitness_scores, min(k, len(fitness_scores)))
        return max(contestants, key=lambda x: x[0])[1]

    def _final_report(
        self,
        best: tuple[float, StatVector, TournamentResults] | None,
        history: list[dict],
        difficulty_snapshots: dict,
        elapsed: float,
    ) -> dict:
        if best is None:
            return {"error": "no generations completed"}

        score, stats, results = best
        return {
            "best_fitness": round(score, 4),
            "stats": stats.to_dict(),
            "tournament": {
                "games_played": results.games_played,
                "median_ticks": round(results.median_ticks, 1),
                "decisive_pct": round(results.decisive_pct, 2),
                "win_rates": {k: round(v, 2) for k, v in results.win_rates.items()},
                "comeback_pct": round(results.comeback_pct, 2),
                "lane_activity": {k: round(v, 2) for k, v in results.lane_activity.items()},
            },
            "bot_weights": {name: w.to_dict() for name, w in ARCHETYPES.items()},
            "difficulty_snapshots": difficulty_snapshots,
            "evolution_history": history,
            "total_elapsed_s": round(elapsed, 1),
        }
