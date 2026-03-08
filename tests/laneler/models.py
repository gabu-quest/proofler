"""sqler models for laneler game state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from sqler import AsyncSQLerLiteModel, AsyncSQLerDB


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LANES = ("top", "mid", "bot")

# Tile layout per lane: 0=A base, 1-2=A approach, 3=throne, 4-5=B approach, 6=B base
TILE_A_BASE = 0
TILE_B_BASE = 6
TILE_THRONE = 3
TILES_PER_LANE = 7

HERO_NAMES = ("blade", "bolt", "bastion")

HERO_STATS: dict[str, dict] = {
    "blade": {
        "hp": 6, "max_hp": 6, "speed": 2,
        "small_skill": "dash", "big_skill": "execute",
        "small_desc": "move 2 extra tiles",
        "big_desc": "6 dmg adjacent, 2x if target <50% HP",
    },
    "bolt": {
        "hp": 6, "max_hp": 6, "speed": 1,
        "small_skill": "zap", "big_skill": "storm",
        "small_desc": "3 dmg within 2 tiles",
        "big_desc": "4 dmg AoE within 2 tiles of target tile",
    },
    "bastion": {
        "hp": 12, "max_hp": 12, "speed": 1,
        "small_skill": "shield", "big_skill": "fortify",
        "small_desc": "block next damage this tick (self)",
        "big_desc": "allies within 1 tile take half damage this tick",
    },
}

# Economy
PASSIVE_GOLD = 1
CREEP_KILL_GOLD = 2
HERO_KILL_GOLD = 5
CREEP_KILL_XP = 1
HERO_KILL_XP = 3
UPGRADE_CREEPS_COST = 5
POTION_COST = 3
POTION_HEAL = 3
LEVEL_UP_XP = {2: 5, 3: 10, 4: 15}  # level -> cumulative XP cost
HP_PER_LEVEL = 2
DMG_PER_LEVEL = 1

# Creeps
CREEP_BASE_HP = 2
CREEP_BASE_ATK = 1

# Bases
BASE_HP = 10

# Game limits
MAX_TICK = 20
MAX_COMMANDS_PER_TICK = 3
AFK_FORFEIT_TICKS = 3
MAX_CREEP_UPGRADES_PER_LANE = 3


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

@dataclass
class Player(AsyncSQLerLiteModel):
    __tablename__ = "laneler_players"

    __promoted__: ClassVar[dict[str, str]] = {
        "ulid": "TEXT UNIQUE",
        "token": "TEXT",
        "nickname": "TEXT",
    }

    ulid: str = ""
    nickname: str = ""
    passphrase_hash: str = ""
    token: str = ""
    token_expires_at: int = 0


@dataclass
class Game(AsyncSQLerLiteModel):
    __tablename__ = "laneler_games"

    __promoted__: ClassVar[dict[str, str]] = {
        "ulid": "TEXT UNIQUE",
        "status": "TEXT NOT NULL DEFAULT 'waiting'",
        "player_a": "TEXT",
        "player_b": "TEXT",
    }
    __checks__: ClassVar[dict[str, str]] = {
        "status_valid": "status IN ('waiting', 'ready', 'assigning', 'active', 'finished')",
    }

    ulid: str = ""
    status: str = "waiting"
    player_a: str = ""
    player_b: str = ""
    tick: int = 0
    winner: str = ""
    gold_a: int = 0
    gold_b: int = 0
    xp_a: int = 0
    xp_b: int = 0
    afk_counter_a: int = 0
    afk_counter_b: int = 0
    created_at: float = 0.0
    tick_job_id: str = ""
    end_job_id: str = ""
    creep_upgrades: dict = field(default_factory=lambda: {
        "a": {"top": 0, "mid": 0, "bot": 0},
        "b": {"top": 0, "mid": 0, "bot": 0},
    })


@dataclass
class Hero(AsyncSQLerLiteModel):
    __tablename__ = "laneler_heroes"

    __promoted__: ClassVar[dict[str, str]] = {
        "game_id": "TEXT",
        "player_id": "TEXT",
        "name": "TEXT",
        "is_dead": "INTEGER NOT NULL DEFAULT 0",
    }

    game_id: str = ""
    player_id: str = ""
    name: str = ""
    lane: str = ""
    tile: int = 0
    hp: int = 0
    max_hp: int = 0
    level: int = 1
    big_skill_cd: int = 0
    is_dead: int = 0
    shielded: int = 0
    fortified: int = 0


@dataclass
class Creep(AsyncSQLerLiteModel):
    __tablename__ = "laneler_creeps"

    __promoted__: ClassVar[dict[str, str]] = {
        "game_id": "TEXT",
        "team": "TEXT",
        "is_dead": "INTEGER NOT NULL DEFAULT 0",
    }

    game_id: str = ""
    team: str = ""
    lane: str = ""
    tile: int = 0
    hp: int = CREEP_BASE_HP
    atk: int = CREEP_BASE_ATK
    is_dead: int = 0


@dataclass
class Base(AsyncSQLerLiteModel):
    __tablename__ = "laneler_bases"

    __promoted__: ClassVar[dict[str, str]] = {
        "game_id": "TEXT",
        "team": "TEXT",
        "lane": "TEXT",
    }

    game_id: str = ""
    team: str = ""
    lane: str = ""
    hp: int = BASE_HP


@dataclass
class Command(AsyncSQLerLiteModel):
    __tablename__ = "laneler_commands"

    __promoted__: ClassVar[dict[str, str]] = {
        "game_id": "TEXT",
        "player_id": "TEXT",
        "tick": "INTEGER",
        "resolved": "INTEGER NOT NULL DEFAULT 0",
    }

    game_id: str = ""
    player_id: str = ""
    hero_id: int = 0
    tick: int = 0
    action: str = ""
    args: dict = field(default_factory=dict)
    submitted_at: float = 0.0
    resolved: int = 0
    fizzled: int = 0
    fizzle_reason: str = ""


@dataclass
class GameTick(AsyncSQLerLiteModel):
    __tablename__ = "laneler_game_ticks"

    __promoted__: ClassVar[dict[str, str]] = {
        "game_id": "TEXT",
        "tick": "INTEGER",
    }

    game_id: str = ""
    tick: int = 0
    events: list = field(default_factory=list)
    board_snapshot: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Init helper
# ---------------------------------------------------------------------------

ALL_MODELS = [Player, Game, Hero, Creep, Base, Command, GameTick]


async def init_models(db: AsyncSQLerDB) -> None:
    """Bind all laneler models to the given DB and ensure tables exist."""
    for model in ALL_MODELS:
        model.set_db(db)
        # Ensure table exists (with promoted columns if defined)
        table = model.__tablename__
        promoted = getattr(model, "__promoted__", None)
        if promoted:
            checks = getattr(model, "__checks__", None)
            await db._ensure_table_with_promoted(table, promoted, checks)
        else:
            await db._ensure_table(table)
