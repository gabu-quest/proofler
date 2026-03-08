"""Bot AI for laneler balance tuning.

Minimax depth-2 with beam search over heuristic-pruned candidates.
Two archetypes with distinct evaluation weights: aggro (kill-focused)
and splitter (lane-spread / base damage).
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, fields

from .engine import (
    GameState, CommandInput, legal_commands, resolve_tick,
    _base_tile, _enemy_base_tile,
)


# ---------------------------------------------------------------------------
# Bot evaluation weights
# ---------------------------------------------------------------------------

@dataclass
class BotWeights:
    hero_kill_value: float = 1.0
    death_penalty: float = 1.0
    tile_advance: float = 1.0
    base_damage: float = 1.0
    gold_lead: float = 1.0
    xp_lead: float = 1.0
    hp_preservation: float = 1.0
    creep_advantage: float = 1.0
    lane_spread: float = 1.0
    ability_efficiency: float = 1.0

    def to_dict(self) -> dict:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_dict(cls, d: dict) -> BotWeights:
        return cls(**{k: v for k, v in d.items() if k in {f.name for f in fields(cls)}})


ARCHETYPES: dict[str, BotWeights] = {
    "aggro": BotWeights(hero_kill_value=3.0, tile_advance=2.0, death_penalty=0.5),
    "splitter": BotWeights(base_damage=3.0, lane_spread=2.0, hero_kill_value=0.3),
}


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------

class Bot:
    def __init__(self, archetype: str, weights: BotWeights | None = None):
        self.archetype = archetype
        self.weights = weights or ARCHETYPES[archetype]

    def decide(self, game: GameState, team: str, beam: int = 10) -> list[CommandInput]:
        """Minimax depth-2: pick best command set assuming opponent responds optimally.

        beam: max opponent responses to evaluate (top N by quick heuristic).
        """
        my_candidates = legal_commands(game, team)
        opp_team = "b" if team == "a" else "a"

        if not my_candidates or my_candidates == [[]]:
            return []

        best_score = float("-inf")
        best_cmds = my_candidates[0]

        # Pre-generate opponent candidates once (they don't change much between our moves)
        opp_candidates = legal_commands(game, opp_team)
        if not opp_candidates or opp_candidates == [[]]:
            opp_candidates = [[]]

        for my_cmds in my_candidates:
            worst_score = float("inf")

            for opp_cmds in opp_candidates[:beam]:
                game_copy = deepcopy(game)
                all_cmds = list(my_cmds) + list(opp_cmds)
                resolve_tick(game_copy, all_cmds)
                score = self.evaluate(game_copy, team)
                worst_score = min(worst_score, score)

            if worst_score > best_score:
                best_score = worst_score
                best_cmds = my_cmds

        return list(best_cmds)

    def decide_greedy(self, game: GameState, team: str) -> list[CommandInput]:
        """Greedy: pick best command set by evaluating each in isolation.

        ~10x faster than minimax — no opponent modeling, 1 deepcopy per candidate.
        Used for GA evaluation where speed matters more than perfect play.
        """
        my_candidates = legal_commands(game, team)

        if not my_candidates or my_candidates == [[]]:
            return []

        best_score = float("-inf")
        best_cmds = my_candidates[0]

        for my_cmds in my_candidates:
            game_copy = deepcopy(game)
            resolve_tick(game_copy, list(my_cmds))
            score = self.evaluate(game_copy, team)

            if score > best_score:
                best_score = score
                best_cmds = my_cmds

        return list(best_cmds)

    def evaluate(self, game: GameState, team: str) -> float:
        """Score a board position from team's perspective."""
        w = self.weights
        opp = "b" if team == "a" else "a"
        score = 0.0

        # Hero kills (cumulative)
        score += w.hero_kill_value * game.kills[team]
        score -= w.death_penalty * game.kills[opp]

        # Tile advancement
        my_heroes = [h for h in game.heroes if h.team == team and not h.is_dead]
        enemy_base = _enemy_base_tile(team)
        my_base = _base_tile(team)
        lane_range = abs(enemy_base - my_base) or 1

        for h in my_heroes:
            if team == "a":
                advance = h.tile / lane_range
            else:
                advance = (6 - h.tile) / lane_range
            score += w.tile_advance * advance

        # Base damage dealt
        opp_base_hp = sum(b.hp for b in game.bases if b.team == opp)
        max_opp_base_hp = game.stats.base_hp * 3
        if max_opp_base_hp > 0:
            score += w.base_damage * (max_opp_base_hp - opp_base_hp) / max_opp_base_hp * 10

        # Gold/XP lead (normalized)
        gold_diff = game.gold[team] - game.gold[opp]
        score += w.gold_lead * gold_diff * 0.1

        xp_diff = game.xp[team] - game.xp[opp]
        score += w.xp_lead * xp_diff * 0.1

        # HP preservation
        for h in my_heroes:
            if h.max_hp > 0:
                score += w.hp_preservation * (h.hp / h.max_hp)

        # Creep upgrades
        my_upgrades = sum(game.creep_upgrades[team].values())
        opp_upgrades = sum(game.creep_upgrades[opp].values())
        score += w.creep_advantage * (my_upgrades - opp_upgrades)

        # Lane spread
        my_lanes = len(set(h.lane for h in my_heroes))
        score += w.lane_spread * my_lanes

        # Game over bonus
        if game.finished:
            if game.winner == team:
                score += 100
            elif game.winner == opp:
                score -= 100

        return score
