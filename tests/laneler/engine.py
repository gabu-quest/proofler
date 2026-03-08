"""Pure game engine for laneler balance tuning.

No sqler, no async, no DB — just dataclasses and functions.
Mirrors tick.py logic but parameterized by StatVector.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field, fields


# ---------------------------------------------------------------------------
# Constants (non-tunable)
# ---------------------------------------------------------------------------

LANES = ("top", "mid", "bot")
TILE_A_BASE = 0
TILE_B_BASE = 6
TILE_THRONE = 3
TILES_PER_LANE = 7
HERO_NAMES = ("blade", "bolt", "bastion")
MAX_CREEP_UPGRADES_PER_LANE = 3


# ---------------------------------------------------------------------------
# Tunable stat vector
# ---------------------------------------------------------------------------

# Bounds for mutation: (min, max) per field
STAT_BOUNDS: dict[str, tuple[float, float]] = {
    "blade_hp": (3, 20),
    "blade_speed": (1, 3),
    "blade_execute_dmg": (2, 15),
    "blade_dash_range": (1, 4),
    "bolt_hp": (3, 20),
    "bolt_speed": (1, 3),
    "bolt_zap_dmg": (1, 10),
    "bolt_zap_range": (1, 4),
    "bolt_storm_dmg": (1, 10),
    "bolt_storm_range": (1, 4),
    "bastion_hp": (5, 20),
    "bastion_speed": (1, 3),
    "bastion_fortify_reduction": (0.2, 0.8),
    "passive_gold": (0, 5),
    "creep_kill_gold": (1, 10),
    "hero_kill_gold": (1, 15),
    "creep_kill_xp": (1, 5),
    "hero_kill_xp": (1, 10),
    "upgrade_creeps_cost": (1, 15),
    "potion_cost": (1, 10),
    "potion_heal": (1, 10),
    "level_up_xp_2": (2, 15),
    "level_up_xp_3": (5, 25),
    "level_up_xp_4": (8, 35),
    "hp_per_level": (1, 5),
    "dmg_per_level": (1, 3),
    "creep_hp": (1, 6),
    "creep_atk": (1, 4),
    "base_hp": (5, 30),
    "max_tick": (10, 30),
}


@dataclass
class StatVector:
    """~30 tunable game constants."""
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
    creep_hp: int = 2
    creep_atk: int = 1
    base_hp: int = 10
    max_tick: int = 20

    def to_dict(self) -> dict:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    @classmethod
    def from_dict(cls, d: dict) -> StatVector:
        return cls(**{k: v for k, v in d.items() if k in {f.name for f in fields(cls)}})

    def level_up_xp(self) -> dict[int, int]:
        return {2: self.level_up_xp_2, 3: self.level_up_xp_3, 4: self.level_up_xp_4}


# ---------------------------------------------------------------------------
# Game state dataclasses
# ---------------------------------------------------------------------------

@dataclass
class HeroState:
    id: int
    team: str
    name: str
    lane: str
    tile: int
    hp: int
    max_hp: int
    level: int = 1
    big_skill_cd: int = 0
    is_dead: int = 0
    shielded: int = 0
    fortified: int = 0


@dataclass
class CreepState:
    id: int
    team: str
    lane: str
    tile: int
    hp: int
    atk: int
    is_dead: int = 0


@dataclass
class BaseState:
    team: str
    lane: str
    hp: int


@dataclass
class CommandInput:
    team: str
    hero_id: int = 0
    action: str = ""
    args: dict = field(default_factory=dict)
    order: int = 0


@dataclass
class GameState:
    stats: StatVector
    tick: int = 0
    heroes: list[HeroState] = field(default_factory=list)
    creeps: list[CreepState] = field(default_factory=list)
    bases: list[BaseState] = field(default_factory=list)
    gold: dict = field(default_factory=lambda: {"a": 0, "b": 0})
    xp: dict = field(default_factory=lambda: {"a": 0, "b": 0})
    creep_upgrades: dict = field(default_factory=lambda: {
        "a": {"top": 0, "mid": 0, "bot": 0},
        "b": {"top": 0, "mid": 0, "bot": 0},
    })
    finished: bool = False
    winner: str = ""
    next_creep_id: int = 7
    kills: dict = field(default_factory=lambda: {"a": 0, "b": 0})
    first_kill: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HERO_STAT_MAP: dict[tuple[str, str], str] = {
    ("blade", "hp"): "blade_hp",
    ("blade", "speed"): "blade_speed",
    ("blade", "execute_dmg"): "blade_execute_dmg",
    ("blade", "dash_range"): "blade_dash_range",
    ("bolt", "hp"): "bolt_hp",
    ("bolt", "speed"): "bolt_speed",
    ("bolt", "zap_dmg"): "bolt_zap_dmg",
    ("bolt", "zap_range"): "bolt_zap_range",
    ("bolt", "storm_dmg"): "bolt_storm_dmg",
    ("bolt", "storm_range"): "bolt_storm_range",
    ("bastion", "hp"): "bastion_hp",
    ("bastion", "speed"): "bastion_speed",
    ("bastion", "fortify_reduction"): "bastion_fortify_reduction",
}


def hero_stat(stats: StatVector, hero_name: str, stat_name: str) -> int | float:
    key = (hero_name, stat_name)
    attr = _HERO_STAT_MAP.get(key)
    if attr is None:
        raise KeyError(f"No stat mapping for {key}")
    return getattr(stats, attr)


def _hero_level_dmg(hero: HeroState, stats: StatVector) -> int:
    return (hero.level - 1) * stats.dmg_per_level


def _distance(a: int, b: int) -> int:
    return abs(a - b)


def _base_tile(team: str) -> int:
    return TILE_A_BASE if team == "a" else TILE_B_BASE


def _enemy_base_tile(team: str) -> int:
    return TILE_B_BASE if team == "a" else TILE_A_BASE


def _move_direction(team: str) -> int:
    return 1 if team == "a" else -1


def _safe_int(val, default=None) -> int | None:
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Command resolvers
# ---------------------------------------------------------------------------

def _resolve_move(hero: HeroState, args: dict, game: GameState, events: list) -> bool:
    target = _safe_int(args.get("target_tile"))
    if target is None:
        return False
    target = max(0, min(TILES_PER_LANE - 1, target))
    dist = _distance(hero.tile, target)
    speed = hero_stat(game.stats, hero.name, "speed")
    if dist == 0:
        return False
    step = min(dist, speed)
    direction = 1 if target > hero.tile else -1
    hero.tile += step * direction
    events.append({"type": "move", "hero": hero.name, "team": hero.team, "to": hero.tile})
    return True


def _resolve_dash(hero: HeroState, args: dict, game: GameState, events: list) -> bool:
    if hero.name != "blade":
        return False
    target = _safe_int(args.get("target_tile"))
    if target is None:
        return False
    target = max(0, min(TILES_PER_LANE - 1, target))
    dist = _distance(hero.tile, target)
    if dist == 0:
        return False
    dash_range = hero_stat(game.stats, hero.name, "dash_range")
    step = min(dist, dash_range)
    direction = 1 if target > hero.tile else -1
    hero.tile += step * direction
    events.append({"type": "dash", "hero": hero.name, "team": hero.team, "to": hero.tile})
    return True


def _apply_damage(
    target_hero: HeroState, damage: int, stats: StatVector,
    all_heroes: list[HeroState], events: list,
) -> dict | None:
    if target_hero.shielded:
        target_hero.shielded = 0
        events.append({"type": "shield_block", "hero": target_hero.name, "team": target_hero.team})
        return None
    if target_hero.fortified:
        damage = max(1, int(damage * stats.bastion_fortify_reduction))
    target_hero.hp -= damage
    events.append({
        "type": "damage", "target": target_hero.name, "target_team": target_hero.team,
        "amount": damage, "remaining_hp": target_hero.hp,
    })
    if target_hero.hp <= 0:
        target_hero.hp = 0
        target_hero.is_dead = 1
        events.append({"type": "death", "hero": target_hero.name, "team": target_hero.team})
        return {"type": "hero_kill", "victim": target_hero.name, "victim_team": target_hero.team}
    return None


def _resolve_execute(
    hero: HeroState, args: dict, game: GameState,
    heroes: list[HeroState], events: list,
) -> dict | None:
    if hero.name != "blade":
        return None
    if hero.big_skill_cd > 0:
        return None
    target_id = _safe_int(args.get("target_hero_id"))
    if target_id is None:
        return None
    target = next((h for h in heroes if h.id == target_id and not h.is_dead), None)
    if target is None:
        return None
    if hero.lane != target.lane or _distance(hero.tile, target.tile) > 1:
        return None
    base_dmg = game.stats.blade_execute_dmg + _hero_level_dmg(hero, game.stats)
    if target.hp < target.max_hp / 2:
        base_dmg *= 2
    hero.big_skill_cd = 1
    events.append({"type": "execute", "hero": hero.name, "team": hero.team, "target": target.name, "dmg": base_dmg})
    return _apply_damage(target, base_dmg, game.stats, heroes, events)


def _resolve_zap(
    hero: HeroState, args: dict, game: GameState,
    heroes: list[HeroState], events: list,
) -> dict | None:
    if hero.name != "bolt":
        return None
    target_id = _safe_int(args.get("target_hero_id"))
    if target_id is None:
        return None
    target = next((h for h in heroes if h.id == target_id and not h.is_dead), None)
    if target is None:
        return None
    zap_range = game.stats.bolt_zap_range
    if hero.lane != target.lane or _distance(hero.tile, target.tile) > zap_range:
        return None
    dmg = game.stats.bolt_zap_dmg + _hero_level_dmg(hero, game.stats)
    events.append({"type": "zap", "hero": hero.name, "team": hero.team, "target": target.name, "dmg": dmg})
    return _apply_damage(target, dmg, game.stats, heroes, events)


def _resolve_storm(
    hero: HeroState, args: dict, game: GameState,
    heroes: list[HeroState], creeps: list[CreepState], events: list,
) -> list[dict]:
    kills = []
    if hero.name != "bolt":
        return kills
    if hero.big_skill_cd > 0:
        return kills
    target_tile = _safe_int(args.get("target_tile"))
    if target_tile is None:
        return kills
    storm_range = game.stats.bolt_storm_range
    if _distance(hero.tile, target_tile) > storm_range:
        return kills
    dmg = game.stats.bolt_storm_dmg + _hero_level_dmg(hero, game.stats)
    hero.big_skill_cd = 1
    events.append({"type": "storm", "hero": hero.name, "team": hero.team, "tile": target_tile, "dmg": dmg})

    for h in heroes:
        if h.is_dead or h.team == hero.team:
            continue
        if h.lane == hero.lane and _distance(h.tile, target_tile) <= storm_range:
            kill = _apply_damage(h, dmg, game.stats, heroes, events)
            if kill:
                kills.append(kill)

    enemy_team = "b" if hero.team == "a" else "a"
    for c in creeps:
        if c.is_dead or c.team != enemy_team:
            continue
        if c.lane == hero.lane and _distance(c.tile, target_tile) <= storm_range:
            c.hp -= dmg
            if c.hp <= 0:
                c.hp = 0
                c.is_dead = 1
                kills.append({"type": "creep_kill", "team": c.team, "lane": c.lane})

    return kills


def _resolve_shield(hero: HeroState, args: dict, game: GameState, events: list) -> bool:
    if hero.name != "bastion":
        return False
    hero.shielded = 1
    events.append({"type": "shield", "hero": hero.name, "team": hero.team})
    return True


def _resolve_fortify(
    hero: HeroState, args: dict, game: GameState,
    heroes: list[HeroState], events: list,
) -> bool:
    if hero.name != "bastion":
        return False
    if hero.big_skill_cd > 0:
        return False
    hero.big_skill_cd = 1
    for h in heroes:
        if h.team != hero.team:
            continue
        if h.lane == hero.lane and _distance(h.tile, hero.tile) <= 1 and not h.is_dead:
            h.fortified = 1
    events.append({"type": "fortify", "hero": hero.name, "team": hero.team})
    return True


def _resolve_upgrade_creeps(team: str, args: dict, game: GameState, events: list) -> bool:
    lane = args.get("lane")
    if lane not in LANES:
        return False
    if game.gold[team] < game.stats.upgrade_creeps_cost:
        return False
    upgrades = game.creep_upgrades[team][lane]
    if upgrades >= MAX_CREEP_UPGRADES_PER_LANE:
        return False
    game.gold[team] -= game.stats.upgrade_creeps_cost
    game.creep_upgrades[team][lane] = upgrades + 1
    events.append({"type": "upgrade_creeps", "team": team, "lane": lane, "level": upgrades + 1})
    return True


def _resolve_potion(hero: HeroState, team: str, game: GameState, events: list) -> bool:
    if game.gold[team] < game.stats.potion_cost:
        return False
    heal = min(game.stats.potion_heal, hero.max_hp - hero.hp)
    if heal <= 0:
        return False
    game.gold[team] -= game.stats.potion_cost
    hero.hp += heal
    events.append({"type": "potion", "hero": hero.name, "team": team, "heal": heal, "hp": hero.hp})
    return True


# ---------------------------------------------------------------------------
# Game setup
# ---------------------------------------------------------------------------

def create_game(
    stats: StatVector,
    assign_a: dict[str, str] | None = None,
    assign_b: dict[str, str] | None = None,
) -> GameState:
    """Create a fresh game state.

    assign_a/b map hero name to lane, e.g. {"blade": "top", "bolt": "mid", "bastion": "bot"}.
    Defaults to blade=top, bolt=mid, bastion=bot.
    """
    if assign_a is None:
        assign_a = {"blade": "top", "bolt": "mid", "bastion": "bot"}
    if assign_b is None:
        assign_b = {"blade": "top", "bolt": "mid", "bastion": "bot"}

    game = GameState(stats=deepcopy(stats))

    hero_id = 1
    for assign, team in [(assign_a, "a"), (assign_b, "b")]:
        for name in HERO_NAMES:
            lane = assign[name]
            hp = hero_stat(stats, name, "hp")
            game.heroes.append(HeroState(
                id=hero_id, team=team, name=name, lane=lane,
                tile=_base_tile(team), hp=hp, max_hp=hp,
            ))
            hero_id += 1

    for lane in LANES:
        for team in ("a", "b"):
            game.bases.append(BaseState(team=team, lane=lane, hp=stats.base_hp))

    return game


# ---------------------------------------------------------------------------
# Core tick resolution
# ---------------------------------------------------------------------------

def resolve_tick(game: GameState, commands: list[CommandInput]) -> list[dict]:
    """Resolve one tick. Mutates game in-place. Returns events list."""
    game.tick += 1
    events: list[dict] = []
    kills: list[dict] = []
    stats = game.stats
    heroes = game.heroes
    creeps = game.creeps
    bases = game.bases

    # Sort commands by order, randomizing ties to prevent first-mover advantage
    import random as _rand
    commands = sorted(commands, key=lambda c: (c.order, _rand.random()))

    # Reset per-tick shields/fortify
    for h in heroes:
        h.shielded = 0
        h.fortified = 0

    # --- Resolve commands ---
    hero_by_id = {h.id: h for h in heroes}

    for cmd in commands:
        hero = hero_by_id.get(cmd.hero_id)

        if cmd.action == "upgrade_creeps":
            _resolve_upgrade_creeps(cmd.team, cmd.args, game, events)
            continue

        if cmd.action == "potion":
            if hero is None or hero.is_dead:
                continue
            _resolve_potion(hero, cmd.team, game, events)
            continue

        if hero is None or hero.is_dead:
            continue
        if hero.team != cmd.team:
            continue

        kill_event = None

        if cmd.action == "move":
            _resolve_move(hero, cmd.args, game, events)
        elif cmd.action == "dash":
            _resolve_dash(hero, cmd.args, game, events)
        elif cmd.action == "execute":
            kill_event = _resolve_execute(hero, cmd.args, game, heroes, events)
        elif cmd.action == "zap":
            kill_event = _resolve_zap(hero, cmd.args, game, heroes, events)
        elif cmd.action == "storm":
            storm_kills = _resolve_storm(hero, cmd.args, game, heroes, creeps, events)
            kills.extend(storm_kills)
        elif cmd.action == "shield":
            _resolve_shield(hero, cmd.args, game, events)
        elif cmd.action == "fortify":
            _resolve_fortify(hero, cmd.args, game, heroes, events)

        if kill_event:
            kills.append(kill_event)

    # --- Creep phase ---
    # Spawn new creeps
    for lane in LANES:
        for team in ("a", "b"):
            upgrades = game.creep_upgrades[team][lane]
            creep = CreepState(
                id=game.next_creep_id, team=team, lane=lane,
                tile=_base_tile(team),
                hp=stats.creep_hp + upgrades,
                atk=stats.creep_atk + upgrades,
            )
            game.next_creep_id += 1
            creeps.append(creep)

    # Move existing creeps
    for c in creeps:
        if c.is_dead:
            continue
        direction = _move_direction(c.team)
        new_tile = c.tile + direction
        if 0 <= new_tile < TILES_PER_LANE:
            c.tile = new_tile

    # Creep auto-attack
    for c in creeps:
        if c.is_dead:
            continue
        enemy_team = "b" if c.team == "a" else "a"

        # Attack adjacent enemy heroes
        attacked = False
        for h in heroes:
            if h.is_dead or h.team == c.team:
                continue
            if h.lane == c.lane and _distance(h.tile, c.tile) <= 1:
                kill = _apply_damage(h, c.atk, stats, heroes, events)
                if kill:
                    kills.append(kill)
                attacked = True
                break

        if attacked:
            continue

        # Attack adjacent enemy creeps
        for ec in creeps:
            if ec.is_dead or ec.team == c.team:
                continue
            if ec.lane == c.lane and _distance(ec.tile, c.tile) <= 1:
                ec.hp -= c.atk
                if ec.hp <= 0:
                    ec.hp = 0
                    ec.is_dead = 1
                    kills.append({"type": "creep_kill", "team": ec.team, "lane": ec.lane})
                attacked = True
                break

        if attacked:
            continue

        # Attack enemy base if adjacent
        enemy_base_tile = _enemy_base_tile(c.team)
        if c.tile == enemy_base_tile:
            base = next((b for b in bases if b.team == enemy_team and b.lane == c.lane), None)
            if base and base.hp > 0:
                base.hp -= c.atk
                events.append({"type": "base_damage", "team": enemy_team, "lane": c.lane, "dmg": c.atk, "hp": base.hp})

    # --- Respawns ---
    for h in heroes:
        if h.is_dead:
            h.tile = _base_tile(h.team)
            h.hp = h.max_hp
            h.is_dead = 0
            h.shielded = 0
            h.fortified = 0
            events.append({"type": "respawn", "hero": h.name, "team": h.team, "tile": h.tile})

    # --- Economy ---
    game.gold["a"] += stats.passive_gold
    game.gold["b"] += stats.passive_gold

    for kill in kills:
        if kill.get("type") == "hero_kill":
            victim_team = kill["victim_team"]
            killer_team = "b" if victim_team == "a" else "a"
            game.gold[killer_team] += stats.hero_kill_gold
            game.xp[killer_team] += stats.hero_kill_xp
            game.kills[killer_team] += 1
            if not game.first_kill:
                game.first_kill = killer_team
        elif kill.get("type") == "creep_kill":
            killed_team = kill["team"]
            killer_team = "b" if killed_team == "a" else "a"
            game.gold[killer_team] += stats.creep_kill_gold
            game.xp[killer_team] += stats.creep_kill_xp

    # --- Level-ups ---
    level_thresholds = stats.level_up_xp()
    for team in ("a", "b"):
        team_xp = game.xp[team]
        for h in heroes:
            if h.team != team:
                continue
            for lvl in (2, 3, 4):
                if h.level < lvl and team_xp >= level_thresholds[lvl]:
                    h.level = lvl
                    h.max_hp += stats.hp_per_level
                    h.hp += stats.hp_per_level
                    events.append({"type": "level_up", "hero": h.name, "team": team, "level": lvl})

    # --- Cooldowns ---
    for h in heroes:
        if h.big_skill_cd > 0:
            h.big_skill_cd -= 1

    # --- Prune dead creeps ---
    game.creeps = [c for c in creeps if not c.is_dead]

    # --- Win check ---
    winner = None
    for base in bases:
        if base.hp <= 0:
            winner = "b" if base.team == "a" else "a"
            events.append({"type": "base_destroyed", "team": base.team, "lane": base.lane})
            break

    if winner:
        game.finished = True
        game.winner = winner
        events.append({"type": "game_over", "winner": winner, "reason": "base_destroyed"})

    # --- Tick cap ---
    if not winner and game.tick >= stats.max_tick:
        a_kills = game.kills["a"]
        b_kills = game.kills["b"]
        if a_kills > b_kills:
            winner = "a"
        elif b_kills > a_kills:
            winner = "b"
        else:
            a_hp = sum(b.hp for b in bases if b.team == "a")
            b_hp = sum(b.hp for b in bases if b.team == "b")
            winner = "a" if a_hp >= b_hp else "b"
        game.finished = True
        game.winner = winner
        events.append({"type": "game_over", "winner": winner, "reason": "tick_cap"})

    return events


# ---------------------------------------------------------------------------
# Legal command generation (heuristic pruning)
# ---------------------------------------------------------------------------

def legal_commands(game: GameState, team: str) -> list[list[CommandInput]]:
    """Generate candidate command sets (3 commands) for a team.

    Heuristic pruning: ~20-30 candidates instead of exhaustive ~455.
    """
    my_heroes = [h for h in game.heroes if h.team == team and not h.is_dead]
    if not my_heroes:
        return [[]]

    opp_team = "b" if team == "a" else "a"
    opp_heroes = [h for h in game.heroes if h.team == opp_team and not h.is_dead]

    # Generate per-hero action options
    hero_options: list[list[CommandInput]] = []

    for h in my_heroes:
        options: list[CommandInput] = []

        # Move toward enemy base
        fwd = _enemy_base_tile(team)
        options.append(CommandInput(team=team, hero_id=h.id, action="move", args={"target_tile": fwd}))

        # Move toward throne
        options.append(CommandInput(team=team, hero_id=h.id, action="move", args={"target_tile": TILE_THRONE}))

        # Move backward (retreat to own base)
        back = _base_tile(team)
        options.append(CommandInput(team=team, hero_id=h.id, action="move", args={"target_tile": back}))

        # Hero-specific skills
        if h.name == "blade":
            # Dash toward enemy base
            options.append(CommandInput(team=team, hero_id=h.id, action="dash", args={"target_tile": fwd}))
            # Execute adjacent enemies
            if h.big_skill_cd == 0:
                for opp in opp_heroes:
                    if opp.lane == h.lane and _distance(h.tile, opp.tile) <= 1:
                        options.append(CommandInput(
                            team=team, hero_id=h.id, action="execute",
                            args={"target_hero_id": opp.id},
                        ))

        elif h.name == "bolt":
            # Zap enemies in range
            for opp in opp_heroes:
                if opp.lane == h.lane and _distance(h.tile, opp.tile) <= game.stats.bolt_zap_range:
                    options.append(CommandInput(
                        team=team, hero_id=h.id, action="zap",
                        args={"target_hero_id": opp.id},
                    ))
            # Storm at best tile
            if h.big_skill_cd == 0:
                for opp in opp_heroes:
                    if opp.lane == h.lane and _distance(h.tile, opp.tile) <= game.stats.bolt_storm_range:
                        options.append(CommandInput(
                            team=team, hero_id=h.id, action="storm",
                            args={"target_tile": opp.tile},
                        ))

        elif h.name == "bastion":
            options.append(CommandInput(team=team, hero_id=h.id, action="shield"))
            if h.big_skill_cd == 0:
                options.append(CommandInput(team=team, hero_id=h.id, action="fortify"))

        # Potion if wounded
        if h.hp < h.max_hp and game.gold[team] >= game.stats.potion_cost:
            options.append(CommandInput(team=team, hero_id=h.id, action="potion"))

        hero_options.append(options)

    # Combine: pick one action per hero, generating candidate sets
    # To keep count manageable, use a greedy approach:
    # For each hero, keep top-5 options (by simple heuristic priority)
    # Then combine: up to 5^3 = 125, but we cap at 30
    candidates: list[list[CommandInput]] = []

    def _priority(cmd: CommandInput) -> int:
        """Lower is better. Prioritize attacks > skills > movement."""
        prio = {"execute": 0, "zap": 1, "storm": 1, "shield": 2, "fortify": 2,
                "dash": 3, "potion": 4, "move": 5, "upgrade_creeps": 6}
        return prio.get(cmd.action, 10)

    trimmed: list[list[CommandInput]] = []
    for opts in hero_options:
        opts.sort(key=_priority)
        trimmed.append(opts[:6])

    # Add upgrade_creeps as a standalone option
    upgrade_cmds: list[CommandInput] = []
    if game.gold[team] >= game.stats.upgrade_creeps_cost:
        for lane in LANES:
            if game.creep_upgrades[team][lane] < MAX_CREEP_UPGRADES_PER_LANE:
                upgrade_cmds.append(CommandInput(
                    team=team, hero_id=0, action="upgrade_creeps", args={"lane": lane},
                ))

    # Generate combinations
    if len(trimmed) == 0:
        return [[]]
    elif len(trimmed) == 1:
        for a in trimmed[0]:
            candidates.append([a])
    elif len(trimmed) == 2:
        for a in trimmed[0]:
            for b in trimmed[1]:
                candidates.append([a, b])
                if len(candidates) >= 30:
                    break
            if len(candidates) >= 30:
                break
    else:
        for a in trimmed[0]:
            for b in trimmed[1]:
                for c in trimmed[2]:
                    candidates.append([a, b, c])
                    if len(candidates) >= 30:
                        break
                if len(candidates) >= 30:
                    break
            if len(candidates) >= 30:
                break

    # Add a few upgrade variants
    if upgrade_cmds and candidates:
        for uc in upgrade_cmds[:2]:
            variant = candidates[0][:2] + [uc] if len(candidates[0]) >= 2 else [uc]
            candidates.append(variant)

    # Number commands
    for cmd_set in candidates:
        for i, cmd in enumerate(cmd_set):
            cmd.order = i

    return candidates if candidates else [[]]
