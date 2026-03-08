"""Tick resolution engine for laneler.

Three module-level async functions registered as qler tasks:
- tick_resolve: Core game loop (self-chaining)
- end_game: Hard cap at MAX_TICK
- cleanup_game: Archive finished game data
"""

from __future__ import annotations

import time

from sqler import F

from .models import (
    Game, Hero, Creep, Base, Command, GameTick,
    LANES, TILE_A_BASE, TILE_B_BASE, TILE_THRONE, TILES_PER_LANE,
    HERO_STATS, MAX_TICK, AFK_FORFEIT_TICKS,
    PASSIVE_GOLD, CREEP_KILL_GOLD, HERO_KILL_GOLD,
    CREEP_KILL_XP, HERO_KILL_XP,
    CREEP_BASE_HP, CREEP_BASE_ATK,
    UPGRADE_CREEPS_COST, POTION_COST, POTION_HEAL,
    HP_PER_LEVEL, DMG_PER_LEVEL, LEVEL_UP_XP,
    MAX_CREEP_UPGRADES_PER_LANE,
)

# ---------------------------------------------------------------------------
# TaskWrapper refs — set by configure() from app.py lifespan
# ---------------------------------------------------------------------------

_tick_tw = None
_end_tw = None
_cleanup_tw = None
_tick_delay: int = 30


def configure(tick_tw, end_tw, cleanup_tw, tick_delay: int = 30):
    global _tick_tw, _end_tw, _cleanup_tw, _tick_delay
    _tick_tw = tick_tw
    _end_tw = end_tw
    _cleanup_tw = cleanup_tw
    _tick_delay = tick_delay


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hero_level_dmg(hero: Hero) -> int:
    """Bonus damage from hero level."""
    return (hero.level - 1) * DMG_PER_LEVEL


def _distance(a: int, b: int) -> int:
    return abs(a - b)


def _team_for_player(game: Game, player_id: str) -> str:
    return "a" if player_id == game.player_a else "b"


def _base_tile(team: str) -> int:
    return TILE_A_BASE if team == "a" else TILE_B_BASE


def _enemy_base_tile(team: str) -> int:
    return TILE_B_BASE if team == "a" else TILE_A_BASE


def _move_direction(team: str) -> int:
    """Direction toward enemy base: +1 for team A, -1 for team B."""
    return 1 if team == "a" else -1


# ---------------------------------------------------------------------------
# Command resolvers
# ---------------------------------------------------------------------------

def _safe_int(val, default=None) -> int | None:
    """Parse value as int, returning default on failure."""
    if val is None:
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _resolve_move(hero: Hero, args: dict, game: Game, events: list) -> bool:
    """Move hero up to speed tiles toward target_tile."""
    target = _safe_int(args.get("target_tile"))
    if target is None:
        return False
    target = max(0, min(TILES_PER_LANE - 1, target))
    dist = _distance(hero.tile, target)
    speed = HERO_STATS[hero.name]["speed"]
    if dist == 0:
        return False
    step = min(dist, speed)
    direction = 1 if target > hero.tile else -1
    hero.tile += step * direction
    events.append({"type": "move", "hero": hero.name, "player": hero.player_id, "to": hero.tile})
    return True


def _resolve_dash(hero: Hero, args: dict, game: Game, events: list) -> bool:
    """Blade's small skill: move 2 extra tiles."""
    if hero.name != "blade":
        return False
    target = _safe_int(args.get("target_tile"))
    if target is None:
        return False
    target = max(0, min(TILES_PER_LANE - 1, target))
    dist = _distance(hero.tile, target)
    if dist == 0:
        return False
    step = min(dist, 2)
    direction = 1 if target > hero.tile else -1
    hero.tile += step * direction
    events.append({"type": "dash", "hero": hero.name, "player": hero.player_id, "to": hero.tile})
    return True


def _apply_damage(target_hero: Hero, damage: int, all_heroes: list[Hero], events: list) -> dict | None:
    """Apply damage to a hero, respecting shield/fortify. Returns kill event or None."""
    if target_hero.shielded:
        target_hero.shielded = 0
        events.append({"type": "shield_block", "hero": target_hero.name, "player": target_hero.player_id})
        return None
    if target_hero.fortified:
        damage = max(1, damage // 2)
    target_hero.hp -= damage
    events.append({
        "type": "damage", "target": target_hero.name, "target_player": target_hero.player_id,
        "amount": damage, "remaining_hp": target_hero.hp,
    })
    if target_hero.hp <= 0:
        target_hero.hp = 0
        target_hero.is_dead = 1
        events.append({"type": "death", "hero": target_hero.name, "player": target_hero.player_id})
        return {"type": "hero_kill", "victim": target_hero.name, "victim_player": target_hero.player_id}
    return None


def _resolve_execute(hero: Hero, args: dict, game: Game, heroes: list[Hero], events: list) -> dict | None:
    """Blade's big skill: 6 dmg adjacent, 2x if target <50% HP."""
    if hero.name != "blade":
        return None
    if hero.big_skill_cd > 0:
        return None
    target_id = _safe_int(args.get("target_hero_id"))
    if target_id is None:
        return None
    target = next((h for h in heroes if h._id == target_id and not h.is_dead), None)
    if target is None:
        return None
    if hero.lane != target.lane or _distance(hero.tile, target.tile) > 1:
        return None
    base_dmg = 6 + _hero_level_dmg(hero)
    if target.hp < target.max_hp / 2:
        base_dmg *= 2
    hero.big_skill_cd = 1
    events.append({"type": "execute", "hero": hero.name, "player": hero.player_id, "target": target.name, "dmg": base_dmg})
    return _apply_damage(target, base_dmg, heroes, events)


def _resolve_zap(hero: Hero, args: dict, game: Game, heroes: list[Hero], events: list) -> dict | None:
    """Bolt's small skill: 3 dmg within 2 tiles."""
    if hero.name != "bolt":
        return None
    target_id = _safe_int(args.get("target_hero_id"))
    if target_id is None:
        return None
    target = next((h for h in heroes if h._id == target_id and not h.is_dead), None)
    if target is None:
        return None
    if hero.lane != target.lane or _distance(hero.tile, target.tile) > 2:
        return None
    dmg = 3 + _hero_level_dmg(hero)
    events.append({"type": "zap", "hero": hero.name, "player": hero.player_id, "target": target.name, "dmg": dmg})
    return _apply_damage(target, dmg, heroes, events)


def _resolve_storm(hero: Hero, args: dict, game: Game, heroes: list[Hero], creeps: list[Creep], events: list) -> list[dict]:
    """Bolt's big skill: 4 dmg AoE within 2 tiles of target tile."""
    kills = []
    if hero.name != "bolt":
        return kills
    if hero.big_skill_cd > 0:
        return kills
    target_tile = _safe_int(args.get("target_tile"))
    if target_tile is None:
        return kills
    if _distance(hero.tile, target_tile) > 2:
        return kills
    dmg = 4 + _hero_level_dmg(hero)
    hero.big_skill_cd = 1
    team = _team_for_player(game, hero.player_id)
    events.append({"type": "storm", "hero": hero.name, "player": hero.player_id, "tile": target_tile, "dmg": dmg})

    # Hit enemy heroes in same lane within 2 tiles of target_tile
    for h in heroes:
        if h.is_dead or h.player_id == hero.player_id:
            continue
        if h.lane == hero.lane and _distance(h.tile, target_tile) <= 2:
            kill = _apply_damage(h, dmg, heroes, events)
            if kill:
                kills.append(kill)

    # Hit enemy creeps in same lane within 2 tiles of target_tile
    enemy_team = "b" if team == "a" else "a"
    for c in creeps:
        if c.is_dead or c.team != enemy_team:
            continue
        if c.lane == hero.lane and _distance(c.tile, target_tile) <= 2:
            c.hp -= dmg
            if c.hp <= 0:
                c.hp = 0
                c.is_dead = 1
                kills.append({"type": "creep_kill", "team": c.team, "lane": c.lane})

    return kills


def _resolve_shield(hero: Hero, args: dict, game: Game, events: list) -> bool:
    """Bastion's small skill: block next damage this tick."""
    if hero.name != "bastion":
        return False
    hero.shielded = 1
    events.append({"type": "shield", "hero": hero.name, "player": hero.player_id})
    return True


def _resolve_fortify(hero: Hero, args: dict, game: Game, heroes: list[Hero], events: list) -> bool:
    """Bastion's big skill: allies within 1 tile take half damage this tick."""
    if hero.name != "bastion":
        return False
    if hero.big_skill_cd > 0:
        return False
    hero.big_skill_cd = 1
    team = _team_for_player(game, hero.player_id)
    for h in heroes:
        if h.player_id != hero.player_id:
            continue
        if h.lane == hero.lane and _distance(h.tile, hero.tile) <= 1 and not h.is_dead:
            h.fortified = 1
    events.append({"type": "fortify", "hero": hero.name, "player": hero.player_id})
    return True


def _resolve_upgrade_creeps(player_id: str, args: dict, game: Game, events: list) -> bool:
    """Spend 5 gold to upgrade one lane's creeps (+1 HP, +1 ATK)."""
    lane = args.get("lane")
    if lane not in LANES:
        return False
    team = _team_for_player(game, player_id)
    gold_attr = f"gold_{team}"
    gold = getattr(game, gold_attr)
    if gold < UPGRADE_CREEPS_COST:
        return False
    upgrades = game.creep_upgrades[team][lane]
    if upgrades >= MAX_CREEP_UPGRADES_PER_LANE:
        return False
    setattr(game, gold_attr, gold - UPGRADE_CREEPS_COST)
    game.creep_upgrades[team][lane] = upgrades + 1
    events.append({"type": "upgrade_creeps", "player": player_id, "team": team, "lane": lane, "level": upgrades + 1})
    return True


def _resolve_potion(hero: Hero, player_id: str, game: Game, events: list) -> bool:
    """Spend 3 gold to heal a hero +3 HP (capped at max_hp)."""
    team = _team_for_player(game, player_id)
    gold_attr = f"gold_{team}"
    gold = getattr(game, gold_attr)
    if gold < POTION_COST:
        return False
    heal = min(POTION_HEAL, hero.max_hp - hero.hp)
    if heal <= 0:
        return False
    setattr(game, gold_attr, gold - POTION_COST)
    hero.hp += heal
    events.append({"type": "potion", "hero": hero.name, "player": player_id, "heal": heal, "hp": hero.hp})
    return True


# ---------------------------------------------------------------------------
# Core tick resolution
# ---------------------------------------------------------------------------

async def tick_resolve(game_id: str, tick_num: int) -> dict:
    """Resolve one game tick. Self-chains to next tick."""
    game = await Game.query().filter(F("ulid") == game_id).first()
    if game is None or game.status != "active":
        return {"skipped": True, "reason": "game not active"}

    if tick_num > MAX_TICK:
        return {"skipped": True, "reason": "past max tick"}

    events: list[dict] = []
    kills: list[dict] = []

    # Load state
    heroes = await Hero.query().filter(F("game_id") == game_id).all()
    creeps = await Creep.query().filter((F("game_id") == game_id) & (F("is_dead") == 0)).all()
    bases = await Base.query().filter(F("game_id") == game_id).all()
    commands = await Command.query().filter(
        (F("game_id") == game_id) & (F("tick") == tick_num) & (F("resolved") == 0)
    ).all()

    # Sort commands by submitted_at
    commands.sort(key=lambda c: c.submitted_at)

    # Reset per-tick shields/fortify
    for h in heroes:
        h.shielded = 0
        h.fortified = 0

    # --- AFK check ---
    a_cmds = [c for c in commands if c.player_id == game.player_a]
    b_cmds = [c for c in commands if c.player_id == game.player_b]

    if len(a_cmds) == 0 and tick_num > 1:
        game.afk_counter_a += 1
    else:
        game.afk_counter_a = 0

    if len(b_cmds) == 0 and tick_num > 1:
        game.afk_counter_b += 1
    else:
        game.afk_counter_b = 0

    if game.afk_counter_a >= AFK_FORFEIT_TICKS:
        game.status = "finished"
        game.winner = game.player_b
        events.append({"type": "afk_forfeit", "player": game.player_a})
        await _save_tick_and_game(game, heroes, creeps, bases, tick_num, events)
        return {"finished": True, "winner": game.player_b, "reason": "afk"}

    if game.afk_counter_b >= AFK_FORFEIT_TICKS:
        game.status = "finished"
        game.winner = game.player_a
        events.append({"type": "afk_forfeit", "player": game.player_b})
        await _save_tick_and_game(game, heroes, creeps, bases, tick_num, events)
        return {"finished": True, "winner": game.player_a, "reason": "afk"}

    # --- Resolve commands ---
    hero_by_id = {h._id: h for h in heroes}

    for cmd in commands:
        cmd.resolved = 1
        hero = hero_by_id.get(cmd.hero_id)

        # Non-hero actions
        if cmd.action == "upgrade_creeps":
            if not _resolve_upgrade_creeps(cmd.player_id, cmd.args, game, events):
                cmd.fizzled = 1
                cmd.fizzle_reason = "insufficient gold or max upgrades"
            await cmd.save()
            continue

        if cmd.action == "potion":
            if hero is None or hero.is_dead:
                cmd.fizzled = 1
                cmd.fizzle_reason = "hero dead or not found"
                await cmd.save()
                continue
            if not _resolve_potion(hero, cmd.player_id, game, events):
                cmd.fizzled = 1
                cmd.fizzle_reason = "insufficient gold or full hp"
            await cmd.save()
            continue

        # Hero-targeted actions
        if hero is None or hero.is_dead:
            cmd.fizzled = 1
            cmd.fizzle_reason = "hero dead or not found"
            await cmd.save()
            continue

        if hero.player_id != cmd.player_id:
            cmd.fizzled = 1
            cmd.fizzle_reason = "not your hero"
            await cmd.save()
            continue

        resolved = False
        kill_event = None

        if cmd.action == "move":
            resolved = _resolve_move(hero, cmd.args, game, events)
        elif cmd.action == "dash":
            resolved = _resolve_dash(hero, cmd.args, game, events)
        elif cmd.action == "execute":
            kill_event = _resolve_execute(hero, cmd.args, game, heroes, events)
            resolved = True  # execute always resolves (damage may be 0 if fizzle)
        elif cmd.action == "zap":
            kill_event = _resolve_zap(hero, cmd.args, game, heroes, events)
            resolved = kill_event is not None or True  # zap resolves even if missed
        elif cmd.action == "storm":
            storm_kills = _resolve_storm(hero, cmd.args, game, heroes, creeps, events)
            kills.extend(storm_kills)
            resolved = True
        elif cmd.action == "shield":
            resolved = _resolve_shield(hero, cmd.args, game, events)
        elif cmd.action == "fortify":
            resolved = _resolve_fortify(hero, cmd.args, game, heroes, events)
        else:
            cmd.fizzled = 1
            cmd.fizzle_reason = f"unknown action: {cmd.action}"

        if not resolved and not cmd.fizzled:
            cmd.fizzled = 1
            cmd.fizzle_reason = "action could not resolve"

        if kill_event:
            kills.append(kill_event)

        await cmd.save()

    # --- Creep phase ---
    # Spawn new creeps (1 per lane per team)
    for lane in LANES:
        for team in ("a", "b"):
            upgrades = game.creep_upgrades.get(team, {}).get(lane, 0)
            creep = Creep(
                game_id=game_id,
                team=team,
                lane=lane,
                tile=_base_tile(team),
                hp=CREEP_BASE_HP + upgrades,
                atk=CREEP_BASE_ATK + upgrades,
            )
            await creep.save()
            creeps.append(creep)

    # Move existing creeps (1 tile toward enemy base)
    for c in creeps:
        if c.is_dead:
            continue
        direction = _move_direction(c.team)
        new_tile = c.tile + direction
        if 0 <= new_tile < TILES_PER_LANE:
            c.tile = new_tile

    # Creep auto-attack: attack adjacent enemies (heroes, creeps, bases)
    for c in creeps:
        if c.is_dead:
            continue
        enemy_team = "b" if c.team == "a" else "a"

        # Attack adjacent enemy heroes
        for h in heroes:
            if h.is_dead or _team_for_player(game, h.player_id) == c.team:
                continue
            if h.lane == c.lane and _distance(h.tile, c.tile) <= 1:
                kill = _apply_damage(h, c.atk, heroes, events)
                if kill:
                    kills.append(kill)
                break  # one target per creep

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
                break

        # Attack enemy base if adjacent
        enemy_base_tile = _enemy_base_tile(c.team)
        if c.lane and c.tile == enemy_base_tile:
            base = next((b for b in bases if b.team == enemy_team and b.lane == c.lane), None)
            if base and base.hp > 0:
                base.hp -= c.atk
                events.append({"type": "base_damage", "team": enemy_team, "lane": c.lane, "dmg": c.atk, "hp": base.hp})

    # --- Damage resolution: respawns ---
    for h in heroes:
        if h.is_dead:
            team = _team_for_player(game, h.player_id)
            h.tile = _base_tile(team)
            h.hp = h.max_hp
            h.is_dead = 0
            h.shielded = 0
            h.fortified = 0
            events.append({"type": "respawn", "hero": h.name, "player": h.player_id, "tile": h.tile})

    # --- Economy ---
    # Passive gold
    game.gold_a += PASSIVE_GOLD
    game.gold_b += PASSIVE_GOLD

    # Kill rewards
    for kill in kills:
        if kill.get("type") == "hero_kill":
            victim_player = kill["victim_player"]
            if victim_player == game.player_a:
                game.gold_b += HERO_KILL_GOLD
                game.xp_b += HERO_KILL_XP
            else:
                game.gold_a += HERO_KILL_GOLD
                game.xp_a += HERO_KILL_XP
        elif kill.get("type") == "creep_kill":
            if kill["team"] == "a":
                game.gold_b += CREEP_KILL_GOLD
                game.xp_b += CREEP_KILL_XP
            else:
                game.gold_a += CREEP_KILL_GOLD
                game.xp_a += CREEP_KILL_XP

    # --- Cooldowns ---
    for h in heroes:
        if h.big_skill_cd > 0:
            h.big_skill_cd -= 1

    # --- Win check ---
    winner = None
    for base in bases:
        if base.hp <= 0:
            winner_team = "b" if base.team == "a" else "a"
            winner = game.player_a if winner_team == "a" else game.player_b
            events.append({"type": "base_destroyed", "team": base.team, "lane": base.lane})
            break

    if winner:
        game.status = "finished"
        game.winner = winner
        events.append({"type": "game_over", "winner": winner, "reason": "base_destroyed"})

    # --- Tick cap check ---
    if not winner and tick_num >= MAX_TICK:
        # Most hero kills wins; tie = most base HP
        a_kills = sum(1 for k in kills if k.get("type") == "hero_kill" and k.get("victim_player") == game.player_b)
        b_kills = sum(1 for k in kills if k.get("type") == "hero_kill" and k.get("victim_player") == game.player_a)
        if a_kills > b_kills:
            winner = game.player_a
        elif b_kills > a_kills:
            winner = game.player_b
        else:
            a_hp = sum(b.hp for b in bases if b.team == "a")
            b_hp = sum(b.hp for b in bases if b.team == "b")
            winner = game.player_a if a_hp >= b_hp else game.player_b
        game.status = "finished"
        game.winner = winner
        events.append({"type": "game_over", "winner": winner, "reason": "tick_cap"})

    # --- Save state ---
    game.tick = tick_num
    await _save_tick_and_game(game, heroes, creeps, bases, tick_num, events)

    # --- Self-chain next tick ---
    if game.status == "active" and _tick_tw:
        await _tick_tw.enqueue(game_id, tick_num + 1, _delay=_tick_delay)

    return {
        "game_id": game_id,
        "tick": tick_num,
        "events": len(events),
        "finished": game.status == "finished",
        "winner": game.winner or None,
    }


async def _save_tick_and_game(
    game: Game, heroes: list[Hero], creeps: list[Creep],
    bases: list[Base], tick_num: int, events: list[dict],
):
    """Save all state + create GameTick snapshot."""
    # Build board snapshot
    snapshot = {
        "heroes": [
            {"id": h._id, "name": h.name, "player": h.player_id, "lane": h.lane,
             "tile": h.tile, "hp": h.hp, "max_hp": h.max_hp, "level": h.level,
             "is_dead": h.is_dead, "big_skill_cd": h.big_skill_cd}
            for h in heroes
        ],
        "creeps": [
            {"team": c.team, "lane": c.lane, "tile": c.tile, "hp": c.hp, "is_dead": c.is_dead}
            for c in creeps if not c.is_dead
        ],
        "bases": [
            {"team": b.team, "lane": b.lane, "hp": b.hp}
            for b in bases
        ],
        "gold": {"a": game.gold_a, "b": game.gold_b},
        "xp": {"a": game.xp_a, "b": game.xp_b},
    }

    # Save GameTick
    gt = GameTick(game_id=game.ulid, tick=tick_num, events=events, board_snapshot=snapshot)
    await gt.save()

    # Save all entities
    await game.save()
    for h in heroes:
        await h.save()
    for c in creeps:
        await c.save()
    for b in bases:
        await b.save()


async def end_game(game_id: str) -> dict:
    """Hard cap: force-end the game if still active."""
    game = await Game.query().filter(F("ulid") == game_id).first()
    if game is None or game.status != "active":
        return {"skipped": True}

    game.status = "finished"
    # Whoever has more base HP wins
    bases = await Base.query().filter(F("game_id") == game_id).all()
    a_hp = sum(b.hp for b in bases if b.team == "a")
    b_hp = sum(b.hp for b in bases if b.team == "b")
    game.winner = game.player_a if a_hp >= b_hp else game.player_b
    await game.save()

    gt = GameTick(
        game_id=game_id, tick=game.tick + 1,
        events=[{"type": "game_over", "winner": game.winner, "reason": "time_limit"}],
        board_snapshot={},
    )
    await gt.save()

    return {"finished": True, "winner": game.winner}


async def cleanup_game(game_id: str) -> dict:
    """Archive finished game data (delete creeps to free space)."""
    creeps = await Creep.query().filter(F("game_id") == game_id).all()
    for c in creeps:
        await c.delete()
    return {"cleaned": True, "creeps_removed": len(creeps)}
