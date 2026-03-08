# M11: laneler Balance — Automated Game Tuning

## Overview

A long-running Python script that co-evolves game stats and bot AI to find balanced configurations. Two nested optimization loops: an inner loop where bots play games, and an outer loop where a genetic algorithm mutates stat vectors and selects for interesting, decisive games.

The script produces three artifacts: a tuned stat table, trained bot weights (usable as AI opponents), and a balance report.

## Why This Matters

M10 shipped with hand-picked constants (Blade HP=6, Execute dmg=6, etc.). These are educated guesses. Without automated testing, we can't know if games stalemate at tick 20, if one hero dominates, or if a specific opening is unbeatable. The balance script answers these questions by running thousands of games.

**Bonus:** The trained bots become AI opponents for single-player mode. Three difficulty levels fall out naturally from evolution snapshots (early/mid/final generation).

---

## Architecture

### Two Populations, Co-Evolving

```
┌─────────────────────────────────────────────────┐
│              Outer Loop (Genetic)                │
│                                                  │
│  Population: stat vectors (HP, dmg, costs, ...)  │
│  Fitness: game quality metrics                   │
│                                                  │
│  For each candidate stat set:                    │
│    ┌─────────────────────────────────────┐       │
│    │       Inner Loop (Tournament)       │       │
│    │                                     │       │
│    │  3 bot archetypes play round-robin  │       │
│    │  N games per matchup                │       │
│    │  Collect: length, decisiveness,     │       │
│    │           win rates, comebacks      │       │
│    └─────────────────────────────────────┘       │
│                                                  │
│  Select top candidates → mutate → next gen       │
└─────────────────────────────────────────────────┘
```

### Direct Engine, No HTTP

The balance script imports tick resolution logic directly — no FastAPI, no HTTP, no qler jobs. Games run as pure function calls: state in, state out. This keeps each game simulation under 200ms.

```python
# Pseudocode
game = create_game(stat_vector)
while not game.finished:
    commands_a = bot_a.decide(game, team="a")
    commands_b = bot_b.decide(game, team="b")
    game = resolve_tick(game, commands_a + commands_b)
return game.result
```

This means extracting the tick resolution core from `tests/laneler/tick.py` into pure functions that don't depend on sqler models or qler TaskWrappers.

---

## Bot AI

### Three Archetypes

Each bot has an evaluation function that scores board positions. The archetype determines the weight vector.

| Archetype | Playstyle | Overweights | Underweights |
|-----------|-----------|-------------|--------------|
| **Aggro** | Push forward, take fights | Hero kills, tile advancement, execute damage | Gold efficiency, defense |
| **Farmer** | Avoid fights, accumulate | Gold/XP lead, creep upgrades, level advantage | Tile position, hero kills |
| **Splitter** | Push undefended lanes | Base damage, lane spread, uncontested tiles | Hero kills, group fights |

### Evaluation Weights

Each bot has ~10 tunable weights:

```python
@dataclass
class BotWeights:
    hero_kill_value: float = 1.0      # value of killing an enemy hero
    death_penalty: float = 1.0        # cost of own hero dying
    tile_advance: float = 1.0         # value per tile toward enemy base
    base_damage: float = 1.0          # value of dealing base damage
    gold_lead: float = 1.0            # value of gold advantage
    xp_lead: float = 1.0             # value of XP advantage
    hp_preservation: float = 1.0      # value of keeping heroes healthy
    creep_advantage: float = 1.0      # value of upgraded creeps
    lane_spread: float = 1.0         # value of contesting multiple lanes
    ability_efficiency: float = 1.0   # value of abilities hitting targets
```

Default weights per archetype:

- **Aggro**: `hero_kill_value=3.0, tile_advance=2.0, death_penalty=0.5`
- **Farmer**: `gold_lead=3.0, xp_lead=2.0, hp_preservation=2.0, hero_kill_value=0.5`
- **Splitter**: `base_damage=3.0, lane_spread=2.0, hero_kill_value=0.3`

### Decision Engine: Minimax (Depth 2)

Turn-based with discrete tiles — minimax is tractable.

- **Branching factor**: 3 heroes x ~5 actions each = ~125 possible command sets per player per tick. With alpha-beta pruning and move ordering, effective branching ~20-30.
- **Depth 2**: My commands + opponent's response. Sufficient because the evaluation function does the heavy lifting, not the search depth.
- **Speed target**: <50ms per decision → <100ms per tick → <2s per full game (20 ticks).

```python
def decide(game: GameState, team: str, weights: BotWeights) -> list[Command]:
    best_score = -inf
    best_commands = []
    for my_commands in generate_legal_commands(game, team):
        game_after_mine = simulate_tick(game, my_commands, [])
        worst_opponent_score = inf
        for opp_commands in generate_legal_commands(game_after_mine, other_team):
            game_after_both = simulate_tick(game, my_commands, opp_commands)
            score = evaluate(game_after_both, team, weights)
            worst_opponent_score = min(worst_opponent_score, score)
        if worst_opponent_score > best_score:
            best_score = worst_opponent_score
            best_commands = my_commands
    return best_commands
```

The full 125x125 = 15,625 evaluations per tick is expensive. Pruning strategies:
- Alpha-beta cuts ~80% of branches
- Only evaluate top-5 opponent responses (beam search)
- Skip clearly dominated moves (e.g., moving away from all action)
- Effective: ~200-500 evaluations per tick

### Bot Weight Evolution (Optional, Phase 2)

Initially, archetype weights are fixed. In phase 2, bot weights become a second genetic population:
- **Fitness**: win rate against other archetypes in the current stat environment
- **Goal**: rock-paper-scissors balance (no archetype >60% vs all others)
- **Constraint**: archetypes must remain distinct (enforce minimum weight divergence)

---

## Genetic Algorithm

### Stat Vector

The GA evolves these game constants:

```python
@dataclass
class StatVector:
    # Hero stats
    blade_hp: int = 6
    blade_speed: int = 2
    blade_execute_dmg: int = 6
    blade_dash_range: int = 2
    bolt_hp: int = 6
    bolt_speed: int = 1
    bolt_zap_dmg: int = 3
    bolt_zap_range: int = 2
    bolt_storm_dmg: int = 4
    bolt_storm_range: int = 2
    bastion_hp: int = 12
    bastion_speed: int = 1
    bastion_fortify_reduction: float = 0.5

    # Economy
    passive_gold: int = 1
    creep_kill_gold: int = 2
    hero_kill_gold: int = 5
    creep_kill_xp: int = 1
    hero_kill_xp: int = 3
    upgrade_creeps_cost: int = 5
    potion_cost: int = 3
    potion_heal: int = 3
    level_up_xp_2: int = 5
    level_up_xp_3: int = 10
    level_up_xp_4: int = 15
    hp_per_level: int = 2
    dmg_per_level: int = 1

    # Creeps & bases
    creep_hp: int = 2
    creep_atk: int = 1
    base_hp: int = 10
```

~30 dimensions. Integers mostly, one float (fortify_reduction).

### Mutation

Per-generation mutation:
- Pick 3-5 random stats to mutate
- Integer stats: +/- 1 or 2 (bounded by reasonable min/max)
- Float stats: +/- 0.1
- Crossover: uniform crossover between two parents (50/50 per gene)

Bounds:
- HP: 1-20
- Damage: 1-15
- Speed: 1-3
- Gold/XP costs: 1-20
- Base HP: 5-30

### Selection

Tournament selection (k=3):
1. Pick 3 random candidates
2. Best fitness wins, becomes a parent
3. Repeat to fill next generation

Elitism: top 2 candidates survive unchanged into the next generation.

### Population

- **Size**: 20 candidates per generation
- **Games per evaluation**: 50 (6 matchups x ~8 games each, both sides)
- **Generations**: 100-200

### Fitness Function

```python
def fitness(tournament_results: TournamentResults) -> float:
    scores = {
        # Game length: target median 10-16 ticks
        "length": length_score(tournament_results.median_ticks),
        # Decisiveness: % ending by base destroy (not timeout)
        "decisive": tournament_results.decisive_pct,
        # No dominant archetype: max win rate < 60%
        "balance": archetype_balance_score(tournament_results.win_rates),
        # Comeback potential: 20-40% of losing-first-hero games still win
        "comeback": comeback_score(tournament_results.comeback_pct),
        # Lane diversity: all 3 lanes see combat
        "lanes": lane_diversity_score(tournament_results.lane_activity),
    }

    weights = {
        "length": 0.25,
        "decisive": 0.25,
        "balance": 0.25,
        "comeback": 0.15,
        "lanes": 0.10,
    }

    return sum(scores[k] * weights[k] for k in scores)
```

Individual scoring functions:

```python
def length_score(median_ticks: float) -> float:
    """1.0 if median in [10, 16], drops off outside."""
    if 10 <= median_ticks <= 16:
        return 1.0
    if median_ticks < 5 or median_ticks > 20:
        return 0.0
    if median_ticks < 10:
        return (median_ticks - 5) / 5
    return (20 - median_ticks) / 4

def archetype_balance_score(win_rates: dict[str, float]) -> float:
    """1.0 if all archetypes between 40-60% win rate."""
    max_deviation = max(abs(wr - 0.5) for wr in win_rates.values())
    return max(0, 1.0 - max_deviation * 5)  # 0.0 at 70%, 1.0 at 50%
```

---

## Output

### Per-Generation Log

```
=== Generation 42 / 100 ===
Best fitness: 0.78 (prev: 0.74)
Best candidate #7:
  Blade:   HP=7  speed=2  execute=5  dash=2
  Bolt:    HP=5  speed=1  zap=4     storm=3  ranges=2
  Bastion: HP=14 speed=1  fortify=0.5
  Economy: passive=1  creep_gold=2  hero_gold=4  potion=3/3  upgrade=6
  Creeps:  HP=2  ATK=1  Base HP=12

Tournament results:
  Aggro vs Farmer:   52% / 48%  (24 games)
  Aggro vs Splitter:  45% / 55%  (24 games)
  Farmer vs Splitter: 57% / 43%  (24 games)
  Median ticks: 13.5   Decisive: 82%   Comeback: 28%

Elapsed: 4m12s  Games played: 1,200 total
```

### Final Report (JSON + human-readable)

```json
{
  "generation": 100,
  "best_fitness": 0.87,
  "stats": { "blade_hp": 7, "blade_speed": 2, ... },
  "bot_weights": {
    "aggro": { "hero_kill_value": 3.2, ... },
    "farmer": { "gold_lead": 2.8, ... },
    "splitter": { "base_damage": 3.5, ... }
  },
  "tournament": {
    "games_played": 5000,
    "median_ticks": 13,
    "decisive_pct": 0.85,
    "archetype_win_rates": { "aggro": 0.51, "farmer": 0.48, "splitter": 0.52 },
    "comeback_pct": 0.31
  },
  "evolution_history": [ ... ]  // fitness curve per generation
}
```

### Difficulty Snapshots

```json
{
  "easy": { "generation": 20, "weights": { ... } },
  "medium": { "generation": 50, "weights": { ... } },
  "hard": { "generation": 100, "weights": { ... } }
}
```

---

## File Structure

```
tests/laneler/
├── engine.py          # Pure game engine (no sqler/qler deps)  ~300 lines
├── bot.py             # Bot AI: minimax + evaluation function  ~250 lines
├── balance.py         # GA + tournament runner + fitness       ~300 lines
└── ...existing files...
tests/laneler_balance.py   # CLI entry point                   ~100 lines
```

~950 new lines.

### engine.py — Pure Game Engine

Extracts tick resolution from `tick.py` into pure functions operating on dataclasses (not sqler models). No async, no DB, no side effects.

```python
@dataclass
class GameState:
    heroes: list[HeroState]
    creeps: list[CreepState]
    bases: list[BaseState]
    gold: dict[str, int]
    xp: dict[str, int]
    tick: int
    stats: StatVector
    finished: bool = False
    winner: str = ""

def create_game(stats: StatVector, assign_a: dict, assign_b: dict) -> GameState: ...
def resolve_tick(game: GameState, commands: list[Command]) -> GameState: ...
def legal_commands(game: GameState, team: str) -> list[list[Command]]: ...
```

### bot.py — Bot AI

```python
class Bot:
    def __init__(self, archetype: str, weights: BotWeights): ...
    def decide(self, game: GameState, team: str) -> list[Command]: ...
    def evaluate(self, game: GameState, team: str) -> float: ...
```

### balance.py — Genetic Algorithm

```python
class BalanceRunner:
    def __init__(self, population_size=20, games_per_eval=50, generations=100): ...
    def run(self) -> BalanceReport: ...
    def evaluate_candidate(self, stats: StatVector) -> float: ...
    def run_tournament(self, stats: StatVector) -> TournamentResults: ...
    def mutate(self, parent: StatVector) -> StatVector: ...
    def crossover(self, a: StatVector, b: StatVector) -> StatVector: ...
```

---

## CLI

```bash
# Full balance run (~3 hours)
uv run python tests/laneler_balance.py \
  --generations 100 \
  --population 20 \
  --games-per-eval 50 \
  --output balance_report.json

# Quick smoke test (~5 minutes)
uv run python tests/laneler_balance.py \
  --generations 5 \
  --population 5 \
  --games-per-eval 10

# Resume from checkpoint
uv run python tests/laneler_balance.py \
  --resume balance_checkpoint.json

# Bot vs bot demo (single game with current stats)
uv run python tests/laneler_balance.py --demo aggro farmer
```

---

## Implementation Order

1. **engine.py** — Extract pure game engine from tick.py
2. **bot.py** — Heuristic bot (random-legal-move baseline), then minimax
3. **balance.py** — GA framework, tournament runner, fitness function
4. **laneler_balance.py** — CLI entry point with progress reporting
5. **Integration** — Wire bot weights back into app.py for AI opponent mode

---

## Overfitting Mitigation

The main risk: stats tuned to beat 3 specific bot archetypes but unbalanced for humans.

Mitigations:
1. **3 distinct archetypes** — forces stats to work across playstyles, not just one
2. **Rock-paper-scissors fitness** — penalizes any archetype dominating all others
3. **Archetype divergence constraint** — bots must maintain distinct weight profiles (if they converge, they're not testing different strategies)
4. **Random assignment variety** — each tournament game randomizes hero-to-lane assignment (not always the "optimal" assignment)
5. **Human playtesting final pass** — the script gets 80% of the way; humans tune the last 20% by playing and adjusting fitness weights

---

## Success Criteria

1. Balance script runs to completion (100 generations) without crashes
2. Final fitness > 0.75 (games are decisive, length is reasonable, archetypes are balanced)
3. No archetype has >60% win rate against all others
4. Median game length is 10-16 ticks
5. >75% of games end by base destruction (not timeout)
6. Bot plays competitively enough to be a reasonable single-player opponent
7. Tuned stat set feels meaningfully different from the hand-picked defaults

---

## Phase 2 (Future)

- Co-evolve bot weights alongside stats (second genetic population)
- Add more bot archetypes (e.g., "deathball" — group all heroes in one lane)
- Monte Carlo Tree Search as alternative to minimax
- ELO rating system for bot variants
- Export AI weights for in-game bot opponents (vs-AI mode)
