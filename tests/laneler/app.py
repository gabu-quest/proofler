"""FastAPI app for laneler — async tile MOBA on the -ler stack.

Lifespan follows koyeb_budget.py pattern: Queue + Worker in-process.

Run standalone:
    LANELER_TICK_DELAY=3 uv run python -m tests.laneler.app
"""

from __future__ import annotations

import contextlib
import logging
import os
import resource
import socket
import tempfile
import time
from pathlib import Path

from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ulid import ULID

import sqler
from sqler import AsyncSQLerDB, F

import qler
from qler import Queue, Worker, task

from . import tick as tick_mod
from .models import (
    init_models, Game, Hero, Creep, Base, Command, GameTick, Player,
    LANES, HERO_NAMES, HERO_STATS, TILE_A_BASE, TILE_B_BASE, BASE_HP,
    MAX_COMMANDS_PER_TICK, LEVEL_UP_XP, HP_PER_LEVEL, MAX_TICK,
)
from .auth import (
    hash_passphrase, verify_passphrase, generate_token, token_expiry,
    get_current_player, check_request_rate, check_token_rate,
)
from .nicknames import nickname_suggestions


# ---------------------------------------------------------------------------
# RSS helper (same as koyeb_budget.py)
# ---------------------------------------------------------------------------

def get_current_rss_mb() -> float:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except (OSError, ValueError, IndexError):
        pass
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------

_queue: Queue | None = None
_worker: Worker | None = None
_worker_task = None
_db_path: str = ""

# TaskWrappers (set in lifespan)
_tick_resolve_tw = None
_end_game_tw = None
_cleanup_game_tw = None


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    global _queue, _worker, _worker_task, _db_path
    global _tick_resolve_tw, _end_game_tw, _cleanup_game_tw
    import asyncio

    # DB path from env or temp
    _db_path = os.environ.get("LANELER_DB_PATH", "")
    if not _db_path:
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        _db_path = tmp.name

    tick_delay = int(os.environ.get("LANELER_TICK_DELAY", "30"))

    # Init queue
    _queue = Queue(_db_path)
    await _queue.init_db()

    # Constrained PRAGMAs (Koyeb-friendly)
    for pragma in ["PRAGMA cache_size=-8000", "PRAGMA mmap_size=0"]:
        cur = await _queue._db.adapter.execute(pragma)
        await cur.close()

    # Bind models
    await init_models(_queue._db)

    # Register tasks
    _tick_resolve_tw = task(_queue, max_retries=0)(tick_mod.tick_resolve)
    _end_game_tw = task(_queue, max_retries=0)(tick_mod.end_game)
    _cleanup_game_tw = task(_queue, max_retries=0)(tick_mod.cleanup_game)

    # Configure tick module with TaskWrapper refs
    tick_mod.configure(_tick_resolve_tw, _end_game_tw, _cleanup_game_tw, tick_delay)

    # Start worker
    _worker = Worker(
        _queue,
        concurrency=1,
        poll_interval=0.5,
        shutdown_timeout=5.0,
        archive_interval=120,
        archive_after=300,
    )
    _worker_task = asyncio.create_task(_worker.run())

    yield

    # Shutdown
    if _worker:
        _worker._running = False
    if _worker_task:
        try:
            await asyncio.wait_for(_worker_task, timeout=10.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            _worker_task.cancel()
            try:
                await _worker_task
            except asyncio.CancelledError:
                pass
    if _queue:
        await _queue.close()

    # Cleanup temp DB
    if _db_path and os.path.exists(_db_path):
        try:
            os.unlink(_db_path)
        except OSError:
            pass
        for suffix in ("-wal", "-shm"):
            wp = _db_path + suffix
            if os.path.exists(wp):
                try:
                    os.unlink(wp)
                except OSError:
                    pass


app = FastAPI(lifespan=lifespan)


# ---------------------------------------------------------------------------
# Middleware: IP rate limiting
# ---------------------------------------------------------------------------

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    ip = request.client.host if request.client else "unknown"
    if not check_request_rate(ip):
        return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded"})
    response = await call_next(request)
    return response


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    rss = get_current_rss_mb()
    worker_running = _worker_task is not None and not _worker_task.done()
    active_games = await Game.query().filter(F("status") == "active").count()
    return {
        "status": "healthy",
        "worker": "running" if worker_running else "stopped",
        "rss_mb": round(rss, 1),
        "active_games": active_games,
    }


# ---------------------------------------------------------------------------
# Static
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index():
    static_dir = Path(__file__).parent / "static"
    html_path = static_dir / "index.html"
    if not html_path.exists():
        return HTMLResponse("<h1>laneler</h1><p>No frontend found.</p>")
    return HTMLResponse(html_path.read_text())


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

@app.post("/auth/register")
async def register(request: Request):
    body = await request.json()
    passphrase = body.get("passphrase", "")
    nickname = body.get("nickname", "")

    if not passphrase or len(passphrase) < 4:
        raise HTTPException(400, "Passphrase must be at least 4 characters")

    ip = request.client.host if request.client else "unknown"
    if not check_token_rate(ip):
        raise HTTPException(429, "Too many registrations from this IP")

    # Check if passphrase already exists (hash + verify all players)
    existing = await Player.query().all()
    for p in existing:
        if verify_passphrase(p.passphrase_hash, passphrase):
            raise HTTPException(409, "Passphrase already registered — use /auth/login")

    if not nickname:
        nickname = nickname_suggestions(1)[0]

    token = generate_token()
    player = Player(
        ulid=str(ULID()),
        nickname=nickname,
        passphrase_hash=hash_passphrase(passphrase),
        token=token,
        token_expires_at=token_expiry(),
    )
    await player.save()

    return {"token": token, "player_id": player.ulid, "nickname": player.nickname}


@app.post("/auth/login")
async def login(request: Request):
    body = await request.json()
    passphrase = body.get("passphrase", "")

    if not passphrase:
        raise HTTPException(400, "Passphrase required")

    # Find player by passphrase (must verify against all hashes)
    players = await Player.query().all()
    player = None
    for p in players:
        if verify_passphrase(p.passphrase_hash, passphrase):
            player = p
            break

    if player is None:
        raise HTTPException(401, "Invalid passphrase")

    # Rotate token
    player.token = generate_token()
    player.token_expires_at = token_expiry()
    await player.save()

    return {"token": player.token, "player_id": player.ulid, "nickname": player.nickname}


@app.get("/auth/nicknames")
async def get_nicknames():
    return {"nicknames": nickname_suggestions(3)}


# ---------------------------------------------------------------------------
# Lobby
# ---------------------------------------------------------------------------

@app.post("/lobby/create")
async def lobby_create(player: Player = Depends(get_current_player)):
    game = Game(
        ulid=str(ULID()),
        status="waiting",
        player_a=player.ulid,
        created_at=time.time(),
    )
    await game.save()
    return {"game_id": game.ulid, "status": game.status}


@app.get("/lobby/{game_id}")
async def lobby_state(game_id: str):
    game = await Game.query().filter(F("ulid") == game_id).first()
    if game is None:
        raise HTTPException(404, "Game not found")

    heroes = await Hero.query().filter(F("game_id") == game_id).all()
    a_heroes = [{"name": h.name, "lane": h.lane} for h in heroes if h.player_id == game.player_a]
    b_heroes = [{"name": h.name, "lane": h.lane} for h in heroes if h.player_id == game.player_b]

    return {
        "game_id": game.ulid,
        "status": game.status,
        "player_a": game.player_a,
        "player_b": game.player_b,
        "heroes_a": a_heroes,
        "heroes_b": b_heroes,
    }


@app.post("/lobby/{game_id}/join")
async def lobby_join(game_id: str, player: Player = Depends(get_current_player)):
    game = await Game.query().filter(F("ulid") == game_id).first()
    if game is None:
        raise HTTPException(404, "Game not found")
    if game.status != "waiting":
        raise HTTPException(400, "Game is not waiting for players")
    if game.player_a == player.ulid:
        raise HTTPException(400, "Cannot join your own game")

    game.player_b = player.ulid
    game.status = "ready"
    await game.save()

    return {"game_id": game.ulid, "status": game.status}


@app.post("/lobby/{game_id}/assign")
async def lobby_assign(game_id: str, request: Request, player: Player = Depends(get_current_player)):
    game = await Game.query().filter(F("ulid") == game_id).first()
    if game is None:
        raise HTTPException(404, "Game not found")
    if game.status not in ("ready", "assigning"):
        raise HTTPException(400, f"Cannot assign heroes in {game.status} state")
    if player.ulid not in (game.player_a, game.player_b):
        raise HTTPException(403, "Not a player in this game")

    body = await request.json()
    # Expect: {"blade": "top", "bolt": "mid", "bastion": "bot"}
    assignments = {}
    for hero_name in HERO_NAMES:
        lane = body.get(hero_name)
        if lane not in LANES:
            raise HTTPException(400, f"Invalid lane for {hero_name}: {lane}")
        assignments[hero_name] = lane

    # Check no duplicate lanes
    if len(set(assignments.values())) != len(HERO_NAMES):
        raise HTTPException(400, "Each hero must be in a different lane")

    # Delete existing assignments for this player in this game
    existing = await Hero.query().filter(
        (F("game_id") == game_id) & (F("player_id") == player.ulid)
    ).all()
    for h in existing:
        await h.delete()

    # Create heroes
    team = "a" if player.ulid == game.player_a else "b"
    base_tile = TILE_A_BASE if team == "a" else TILE_B_BASE

    for hero_name, lane in assignments.items():
        stats = HERO_STATS[hero_name]
        hero = Hero(
            game_id=game_id,
            player_id=player.ulid,
            name=hero_name,
            lane=lane,
            tile=base_tile,
            hp=stats["hp"],
            max_hp=stats["max_hp"],
            level=1,
        )
        await hero.save()

    # Transition to assigning if both haven't assigned yet
    if game.status == "ready":
        game.status = "assigning"
        await game.save()

    return {"game_id": game.ulid, "status": game.status, "assigned": True}


@app.post("/lobby/{game_id}/start")
async def lobby_start(game_id: str, player: Player = Depends(get_current_player)):
    game = await Game.query().filter(F("ulid") == game_id).first()
    if game is None:
        raise HTTPException(404, "Game not found")
    if game.player_a != player.ulid:
        raise HTTPException(403, "Only the host can start the game")
    if game.status != "assigning":
        raise HTTPException(400, f"Cannot start game in {game.status} state")

    # Verify both players have assigned heroes
    a_heroes = await Hero.query().filter(
        (F("game_id") == game_id) & (F("player_id") == game.player_a)
    ).count()
    b_heroes = await Hero.query().filter(
        (F("game_id") == game_id) & (F("player_id") == game.player_b)
    ).count()

    if a_heroes != 3 or b_heroes != 3:
        raise HTTPException(400, "Both players must assign 3 heroes before starting")

    # Create bases (3 per team)
    for team in ("a", "b"):
        for lane in LANES:
            base = Base(game_id=game_id, team=team, lane=lane, hp=BASE_HP)
            await base.save()

    # Start the game
    game.status = "active"
    game.tick = 0
    await game.save()

    # Enqueue first tick + hard cap end_game
    tick_delay = int(os.environ.get("LANELER_TICK_DELAY", "30"))
    tick_job = await _tick_resolve_tw.enqueue(game_id, 1, _delay=tick_delay)
    end_job = await _end_game_tw.enqueue(game_id, _delay=tick_delay * MAX_TICK)

    game.tick_job_id = tick_job.ulid
    game.end_job_id = end_job.ulid
    await game.save()

    return {"game_id": game.ulid, "status": "active", "tick_delay": tick_delay}


# ---------------------------------------------------------------------------
# Game
# ---------------------------------------------------------------------------

@app.get("/game/{game_id}/state")
async def game_state(game_id: str, player: Player = Depends(get_current_player)):
    game = await Game.query().filter(F("ulid") == game_id).first()
    if game is None:
        raise HTTPException(404, "Game not found")

    heroes = await Hero.query().filter(F("game_id") == game_id).all()
    creeps = await Creep.query().filter(
        (F("game_id") == game_id) & (F("is_dead") == 0)
    ).all()
    bases = await Base.query().filter(F("game_id") == game_id).all()

    return {
        "game_id": game.ulid,
        "status": game.status,
        "tick": game.tick,
        "winner": game.winner,
        "gold": {"a": game.gold_a, "b": game.gold_b},
        "xp": {"a": game.xp_a, "b": game.xp_b},
        "heroes": [
            {
                "id": h._id, "name": h.name, "player_id": h.player_id,
                "lane": h.lane, "tile": h.tile, "hp": h.hp, "max_hp": h.max_hp,
                "level": h.level, "big_skill_cd": h.big_skill_cd, "is_dead": h.is_dead,
            }
            for h in heroes
        ],
        "creeps": [
            {"team": c.team, "lane": c.lane, "tile": c.tile, "hp": c.hp}
            for c in creeps
        ],
        "bases": [
            {"team": b.team, "lane": b.lane, "hp": b.hp}
            for b in bases
        ],
    }


@app.post("/game/{game_id}/command")
async def submit_command(game_id: str, request: Request, player: Player = Depends(get_current_player)):
    game = await Game.query().filter(F("ulid") == game_id).first()
    if game is None:
        raise HTTPException(404, "Game not found")
    if game.status != "active":
        raise HTTPException(400, "Game is not active")
    if player.ulid not in (game.player_a, game.player_b):
        raise HTTPException(403, "Not a player in this game")

    # Check command limit for next tick
    next_tick = game.tick + 1
    existing_cmds = await Command.query().filter(
        (F("game_id") == game_id) & (F("player_id") == player.ulid) & (F("tick") == next_tick)
    ).count()

    if existing_cmds >= MAX_COMMANDS_PER_TICK:
        raise HTTPException(429, f"Max {MAX_COMMANDS_PER_TICK} commands per tick")

    body = await request.json()
    action = body.get("action", "")
    hero_id = body.get("hero_id", 0)
    args = body.get("args", {})

    valid_actions = ["move", "dash", "execute", "zap", "storm", "shield", "fortify",
                     "upgrade_creeps", "potion"]
    if action not in valid_actions:
        raise HTTPException(400, f"Invalid action: {action}")

    # Validate hero ownership for hero-targeted actions
    if action not in ("upgrade_creeps",):
        if not hero_id or int(hero_id) <= 0:
            raise HTTPException(400, "hero_id required for this action")
        my_heroes = await Hero.query().filter(
            (F("game_id") == game_id) & (F("player_id") == player.ulid)
        ).all()
        hero_ids = [h._id for h in my_heroes]
        if int(hero_id) not in hero_ids:
            raise HTTPException(403, "Not your hero")

    cmd = Command(
        game_id=game_id,
        player_id=player.ulid,
        hero_id=int(hero_id) if hero_id else 0,
        tick=next_tick,
        action=action,
        args=args,
        submitted_at=time.time(),
    )
    await cmd.save()

    return {"command_id": cmd._id, "tick": next_tick, "action": action}


@app.post("/game/{game_id}/level_up")
async def level_up(game_id: str, request: Request, player: Player = Depends(get_current_player)):
    """Spend XP to level up a hero (free action, not a tick command)."""
    game = await Game.query().filter(F("ulid") == game_id).first()
    if game is None:
        raise HTTPException(404, "Game not found")
    if game.status != "active":
        raise HTTPException(400, "Game is not active")

    body = await request.json()
    hero_id = body.get("hero_id")
    if not hero_id:
        raise HTTPException(400, "hero_id required")

    hero = await Hero.from_id(int(hero_id))
    if hero is None or hero.game_id != game_id or hero.player_id != player.ulid:
        raise HTTPException(403, "Not your hero")

    next_level = hero.level + 1
    if next_level > 4:
        raise HTTPException(400, "Already max level")

    xp_cost = LEVEL_UP_XP[next_level]
    team = "a" if player.ulid == game.player_a else "b"
    xp_attr = f"xp_{team}"
    current_xp = getattr(game, xp_attr)

    if current_xp < xp_cost:
        raise HTTPException(400, f"Need {xp_cost} XP, have {current_xp}")

    setattr(game, xp_attr, current_xp - xp_cost)
    hero.level = next_level
    hero.max_hp += HP_PER_LEVEL
    hero.hp += HP_PER_LEVEL  # Heal on level-up
    await hero.save()
    await game.save()

    return {"hero": hero.name, "level": hero.level, "hp": hero.hp, "max_hp": hero.max_hp}


@app.get("/game/{game_id}/tick/{tick_num}")
async def get_tick(game_id: str, tick_num: int):
    gt = await GameTick.query().filter(
        (F("game_id") == game_id) & (F("tick") == tick_num)
    ).first()
    if gt is None:
        raise HTTPException(404, "Tick not found")
    return {"game_id": game_id, "tick": tick_num, "events": gt.events, "board": gt.board_snapshot}


@app.get("/game/{game_id}/history")
async def game_history(game_id: str):
    ticks = await GameTick.query().filter(F("game_id") == game_id).all()
    ticks.sort(key=lambda t: t.tick)
    return {
        "game_id": game_id,
        "ticks": [
            {"tick": t.tick, "events": t.events, "board": t.board_snapshot}
            for t in ticks
        ],
    }


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


if __name__ == "__main__":
    import asyncio
    import uvicorn

    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    port = int(os.environ.get("LANELER_PORT", str(_free_port())))
    print(f"laneler starting on http://127.0.0.1:{port}")
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    asyncio.run(server.serve())
