"""
laneler bot soak test — spawn bot pairs that play random valid moves.

Usage:
    uv run python tests/laneler_bot.py --games 5 --duration 30
    uv run python tests/laneler_bot.py --games 2 --duration 60 --tick-delay 3
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import random
import resource
import socket
import sys
import time

import httpx
import uvicorn


# ---------------------------------------------------------------------------
# RSS helper
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


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# Bot logic
# ---------------------------------------------------------------------------

HERO_ACTIONS = {
    "blade": ["move", "dash", "execute"],
    "bolt": ["move", "zap", "storm"],
    "bastion": ["move", "shield", "fortify"],
}

ASSIGN = {"blade": "top", "bolt": "mid", "bastion": "bot"}


async def run_bot_pair(client: httpx.AsyncClient, game_num: int, stats: dict):
    """Create a game between two bots and play random moves until game ends."""
    pp_a = f"bot-alpha-{game_num}-{time.time()}"
    pp_b = f"bot-beta-{game_num}-{time.time()}"

    # Register
    ra = await client.post("/auth/register", json={"passphrase": pp_a})
    rb = await client.post("/auth/register", json={"passphrase": pp_b})
    if ra.status_code != 200 or rb.status_code != 200:
        stats["errors"] += 1
        return
    token_a = ra.json()["token"]
    token_b = rb.json()["token"]
    player_a = ra.json()["player_id"]
    player_b = rb.json()["player_id"]
    ha = {"Authorization": f"Bearer {token_a}"}
    hb = {"Authorization": f"Bearer {token_b}"}

    # Create + join + assign + start
    r = await client.post("/lobby/create", headers=ha)
    if r.status_code != 200:
        stats["errors"] += 1
        return
    gid = r.json()["game_id"]

    await client.post(f"/lobby/{gid}/join", headers=hb)
    await client.post(f"/lobby/{gid}/assign", headers=ha, json=ASSIGN)
    await client.post(f"/lobby/{gid}/assign", headers=hb, json=ASSIGN)
    r = await client.post(f"/lobby/{gid}/start", headers=ha)
    if r.status_code != 200:
        stats["errors"] += 1
        return

    stats["games_started"] += 1
    print(f"  Game {game_num}: started ({gid[:8]})")

    # Play until game ends
    last_tick = 0
    tick_times: list[float] = []

    while True:
        await asyncio.sleep(1)

        r = await client.get(f"/game/{gid}/state", headers=ha)
        if r.status_code != 200:
            continue
        state = r.json()

        if state["status"] == "finished":
            stats["games_finished"] += 1
            winner_label = "A" if state["winner"] == player_a else "B"
            print(f"  Game {game_num}: finished at tick {state['tick']} (winner: {winner_label})")
            break

        if state["tick"] > last_tick:
            tick_times.append(time.time())
            last_tick = state["tick"]

            # Submit 3 random commands per player
            for token_h, pid in [(ha, player_a), (hb, player_b)]:
                my_heroes = [h for h in state["heroes"] if h["player_id"] == pid and not h["is_dead"]]
                for _ in range(3):
                    if not my_heroes:
                        break
                    hero = random.choice(my_heroes)
                    actions = HERO_ACTIONS.get(hero["name"], ["move"])
                    action = random.choice(actions)

                    args = {}
                    if action == "move":
                        # Move toward enemy base
                        direction = 1 if pid == player_a else -1
                        target = hero["tile"] + direction * 2
                        target = max(0, min(6, target))
                        args = {"target_tile": target}
                    elif action == "dash":
                        direction = 1 if pid == player_a else -1
                        target = hero["tile"] + direction * 2
                        target = max(0, min(6, target))
                        args = {"target_tile": target}
                    elif action in ("execute", "zap"):
                        enemies = [h for h in state["heroes"]
                                   if h["player_id"] != pid and not h["is_dead"]]
                        if enemies:
                            target = random.choice(enemies)
                            args = {"target_hero_id": str(target["id"])}
                        else:
                            action = "move"
                            args = {"target_tile": 3}
                    elif action == "storm":
                        args = {"target_tile": 3}
                    elif action == "shield":
                        args = {}
                    elif action == "fortify":
                        args = {}

                    await client.post(
                        f"/game/{gid}/command", headers=token_h,
                        json={"action": action, "hero_id": hero["id"], "args": args}
                    )
                    stats["commands_sent"] += 1

    # Report tick timing
    if len(tick_times) >= 2:
        intervals = [tick_times[i+1] - tick_times[i] for i in range(len(tick_times)-1)]
        avg = sum(intervals) / len(intervals)
        stats["avg_tick_interval"] = avg


async def run_soak(args: argparse.Namespace):
    tick_delay = args.tick_delay
    os.environ["LANELER_TICK_DELAY"] = str(tick_delay)

    from laneler.app import app

    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"

    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    # Wait for server
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
        for _ in range(50):
            try:
                resp = await client.get("/health")
                if resp.status_code == 200:
                    break
            except httpx.ConnectError:
                pass
            await asyncio.sleep(0.1)
        else:
            print("ERROR: Server failed to start")
            server.should_exit = True
            await server_task
            sys.exit(1)

        print(f"\n{'='*60}")
        print(f"  LANELER BOT SOAK TEST")
        print(f"  Games: {args.games} | Duration: {args.duration}s | Tick delay: {tick_delay}s")
        print(f"{'='*60}")

        stats = {
            "games_started": 0,
            "games_finished": 0,
            "commands_sent": 0,
            "errors": 0,
            "avg_tick_interval": 0,
        }

        start = time.time()
        rss_start = get_current_rss_mb()

        # Launch bot pairs
        tasks = []
        for i in range(args.games):
            tasks.append(asyncio.create_task(run_bot_pair(client, i + 1, stats)))
            await asyncio.sleep(0.5)  # Stagger starts

        # Wait for all games or duration
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=args.duration,
            )
        except asyncio.TimeoutError:
            print(f"\n  Duration limit reached ({args.duration}s)")

        elapsed = time.time() - start
        rss_end = get_current_rss_mb()

        # Report
        print(f"\n{'='*60}")
        print(f"  SOAK REPORT")
        print(f"{'='*60}")
        print(f"  Duration:       {elapsed:.1f}s")
        print(f"  Games started:  {stats['games_started']}")
        print(f"  Games finished: {stats['games_finished']}")
        print(f"  Commands sent:  {stats['commands_sent']}")
        print(f"  Errors:         {stats['errors']}")
        print(f"  RSS start:      {rss_start:.0f} MB")
        print(f"  RSS end:        {rss_end:.0f} MB")
        print(f"  RSS delta:      {rss_end - rss_start:+.0f} MB")
        if stats['avg_tick_interval'] > 0:
            print(f"  Avg tick interval: {stats['avg_tick_interval']:.1f}s (target: {tick_delay}s)")
        print(f"{'='*60}")

    server.should_exit = True
    try:
        await asyncio.wait_for(server_task, timeout=10.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass


def main():
    parser = argparse.ArgumentParser(description="laneler bot soak test")
    parser.add_argument("--games", type=int, default=5, help="Number of bot game pairs")
    parser.add_argument("--duration", type=int, default=120, help="Max duration in seconds")
    parser.add_argument("--tick-delay", type=int, default=3, help="Tick delay in seconds")
    args = parser.parse_args()
    asyncio.run(run_soak(args))


if __name__ == "__main__":
    main()
