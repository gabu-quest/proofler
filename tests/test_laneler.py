"""
laneler integration tests — async tile MOBA on the -ler stack.

Same harness as test_stack.py. Exercises auth, lobby, game mechanics,
tick resolution, combat, economy, and concurrent games.

Run: uv run python tests/test_laneler.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import sys
import time

import httpx
import uvicorn

# ---------------------------------------------------------------------------
# Test infrastructure (same as test_stack.py)
# ---------------------------------------------------------------------------

passed = 0
failed = 0
errors: list[str] = []


def check(name: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        msg = f"  FAIL  {name}"
        if detail:
            msg += f" -- {detail}"
        print(msg)
        errors.append(f"{name}: {detail}" if detail else name)


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# Main test
# ---------------------------------------------------------------------------

async def run_tests():
    os.environ["LANELER_TICK_DELAY"] = "3"

    # Import app after setting env (laneler is in tests/ which is on sys.path)
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
            print("  ERROR: Server failed to start")
            server.should_exit = True
            await server_task
            sys.exit(1)

        # ===========================================================
        # S1: Server + Health
        # ===========================================================
        section("S1: Server + Health")

        resp = await client.get("/health")
        data = resp.json()
        check("S1.1 Health endpoint returns 200", resp.status_code == 200)
        check("S1.2 Status is healthy", data["status"] == "healthy", f"got {data['status']}")
        check("S1.3 Worker running", data["worker"] == "running", f"got {data['worker']}")
        check("S1.4 RSS reported", data["rss_mb"] > 0, f"got {data['rss_mb']}")
        check("S1.5 Active games starts at 0", data["active_games"] == 0)

        # ===========================================================
        # S2: Auth
        # ===========================================================
        section("S2: Auth")

        # Register player A
        resp = await client.post("/auth/register", json={"passphrase": "secret-alpha-pass", "nickname": "AlphaPlayer"})
        check("S2.1 Register returns 200", resp.status_code == 200)
        a_data = resp.json()
        token_a = a_data["token"]
        player_a = a_data["player_id"]
        check("S2.2 Token returned", len(token_a) > 10, f"token len={len(token_a)}")
        check("S2.3 Nickname matches", a_data["nickname"] == "AlphaPlayer")

        # Register player B
        resp = await client.post("/auth/register", json={"passphrase": "secret-beta-pass", "nickname": "BetaPlayer"})
        check("S2.4 Player B registers", resp.status_code == 200)
        b_data = resp.json()
        token_b = b_data["token"]
        player_b = b_data["player_id"]

        # Login player A (token rotation)
        resp = await client.post("/auth/login", json={"passphrase": "secret-alpha-pass"})
        check("S2.5 Login returns 200", resp.status_code == 200)
        login_data = resp.json()
        check("S2.6 Token rotated", login_data["token"] != token_a)
        token_a = login_data["token"]  # use rotated token

        # Bad passphrase
        resp = await client.post("/auth/login", json={"passphrase": "wrong-pass"})
        check("S2.7 Bad passphrase -> 401", resp.status_code == 401)

        # Nicknames
        resp = await client.get("/auth/nicknames")
        check("S2.8 Nicknames returns 3", len(resp.json()["nicknames"]) == 3)

        headers_a = {"Authorization": f"Bearer {token_a}"}
        headers_b = {"Authorization": f"Bearer {token_b}"}

        # ===========================================================
        # S3: Lobby
        # ===========================================================
        section("S3: Lobby")

        # Create game
        resp = await client.post("/lobby/create", headers=headers_a)
        check("S3.1 Create game", resp.status_code == 200)
        game_id = resp.json()["game_id"]
        check("S3.2 Game ID returned", len(game_id) > 10)
        check("S3.3 Status is waiting", resp.json()["status"] == "waiting")

        # Lobby state
        resp = await client.get(f"/lobby/{game_id}")
        check("S3.4 Lobby state", resp.status_code == 200)
        check("S3.5 Player A is host", resp.json()["player_a"] == player_a)

        # Join
        resp = await client.post(f"/lobby/{game_id}/join", headers=headers_b)
        check("S3.6 Join game", resp.status_code == 200)
        check("S3.7 Status -> ready", resp.json()["status"] == "ready")

        # Assign heroes
        assign_a = {"blade": "top", "bolt": "mid", "bastion": "bot"}
        resp = await client.post(f"/lobby/{game_id}/assign", headers=headers_a, json=assign_a)
        check("S3.8 Player A assigns", resp.status_code == 200)
        check("S3.9 Status -> assigning", resp.json()["status"] == "assigning")

        assign_b = {"blade": "bot", "bolt": "top", "bastion": "mid"}
        resp = await client.post(f"/lobby/{game_id}/assign", headers=headers_b, json=assign_b)
        check("S3.10 Player B assigns", resp.status_code == 200)

        # Start
        resp = await client.post(f"/lobby/{game_id}/start", headers=headers_a)
        check("S3.11 Start game", resp.status_code == 200)
        check("S3.12 Status -> active", resp.json()["status"] == "active")

        # ===========================================================
        # S4: Commands
        # ===========================================================
        section("S4: Commands")

        # Get state to find hero IDs
        resp = await client.get(f"/game/{game_id}/state", headers=headers_a)
        state = resp.json()
        check("S4.1 Game state returned", resp.status_code == 200)
        check("S4.2 6 heroes exist", len(state["heroes"]) == 6, f"got {len(state['heroes'])}")

        a_heroes = [h for h in state["heroes"] if h["player_id"] == player_a]
        blade_a = next(h for h in a_heroes if h["name"] == "blade")

        # Submit 3 commands (max per tick)
        resp = await client.post(f"/game/{game_id}/command", headers=headers_a,
                                 json={"action": "move", "hero_id": blade_a["id"], "args": {"target_tile": 1}})
        check("S4.3 Command 1 accepted", resp.status_code == 200)

        bolt_a = next(h for h in a_heroes if h["name"] == "bolt")
        resp = await client.post(f"/game/{game_id}/command", headers=headers_a,
                                 json={"action": "move", "hero_id": bolt_a["id"], "args": {"target_tile": 1}})
        check("S4.4 Command 2 accepted", resp.status_code == 200)

        bastion_a = next(h for h in a_heroes if h["name"] == "bastion")
        resp = await client.post(f"/game/{game_id}/command", headers=headers_a,
                                 json={"action": "move", "hero_id": bastion_a["id"], "args": {"target_tile": 1}})
        check("S4.5 Command 3 accepted", resp.status_code == 200)

        # 4th command -> 429
        resp = await client.post(f"/game/{game_id}/command", headers=headers_a,
                                 json={"action": "move", "hero_id": blade_a["id"], "args": {"target_tile": 2}})
        check("S4.6 4th command -> 429", resp.status_code == 429)

        # Invalid action (use player B who hasn't exhausted commands)
        resp = await client.post(f"/game/{game_id}/command", headers=headers_b,
                                 json={"action": "invalid_action", "hero_id": 0, "args": {}})
        check("S4.6b Invalid action -> 400", resp.status_code == 400)

        # Wrong hero ownership (use player B trying to command A's hero)
        b_heroes = [h for h in state["heroes"] if h["player_id"] == player_b]
        blade_b = next(h for h in b_heroes if h["name"] == "blade")
        resp = await client.post(f"/game/{game_id}/command", headers=headers_b,
                                 json={"action": "move", "hero_id": blade_a["id"], "args": {"target_tile": 1}})
        check("S4.7 Wrong hero -> 403", resp.status_code == 403)

        # B also submits commands
        resp = await client.post(f"/game/{game_id}/command", headers=headers_b,
                                 json={"action": "move", "hero_id": blade_b["id"], "args": {"target_tile": 5}})
        check("S4.8 Player B command accepted", resp.status_code == 200)

        # ===========================================================
        # S5: Tick Resolution
        # ===========================================================
        section("S5: Tick Resolution")

        # Wait for tick 1 to resolve (tick_delay=3s)
        print("  Waiting for tick 1 to resolve...")
        await asyncio.sleep(5)

        resp = await client.get(f"/game/{game_id}/state", headers=headers_a)
        state = resp.json()
        check("S5.1 Tick advanced", state["tick"] >= 1, f"tick={state['tick']}")

        # Check heroes moved
        a_heroes_after = [h for h in state["heroes"] if h["player_id"] == player_a]
        blade_after = next(h for h in a_heroes_after if h["name"] == "blade")
        # Blade speed=2, target_tile=1 from base(0): moves min(1, 2)=1 tile
        check("S5.2 Blade A moved to tile 1", blade_after["tile"] == 1, f"tile={blade_after['tile']}")

        # 6 creeps spawned per tick (1/lane/team, 3 lanes * 2 teams)
        check("S5.3 Creeps spawned", len(state["creeps"]) >= 6, f"creeps={len(state['creeps'])}")

        # Passive gold: exactly 1 per tick (may have kill bonuses too)
        check("S5.4 Gold A >= 1", state["gold"]["a"] >= 1, f"gold_a={state['gold']['a']}")
        check("S5.5 Gold B >= 1", state["gold"]["b"] >= 1, f"gold_b={state['gold']['b']}")

        # Check tick replay
        resp = await client.get(f"/game/{game_id}/tick/1")
        check("S5.6 Tick 1 replay exists", resp.status_code == 200)
        tick_data = resp.json()
        check("S5.7 Events recorded", len(tick_data["events"]) > 0)
        check("S5.8 Board snapshot exists", "heroes" in tick_data["board"])

        # Wait for tick 2 (self-chaining)
        print("  Waiting for tick 2...")
        await asyncio.sleep(4)

        resp = await client.get(f"/game/{game_id}/state", headers=headers_a)
        state2 = resp.json()
        check("S5.9 Tick 2 resolved", state2["tick"] >= 2, f"tick={state2['tick']}")

        # More creeps after tick 2
        check("S5.10 More creeps after tick 2", len(state2["creeps"]) > len(state["creeps"]),
              f"before={len(state['creeps'])} after={len(state2['creeps'])}")

        # Bases still intact
        bases_ok = all(b["hp"] == 10 for b in state2["bases"])
        check("S5.11 Bases intact after tick 2", bases_ok)

        # History
        resp = await client.get(f"/game/{game_id}/history")
        check("S5.12 History returns ticks", len(resp.json()["ticks"]) >= 2,
              f"ticks={len(resp.json()['ticks'])}")

        # ===========================================================
        # S6: Combat
        # ===========================================================
        section("S6: Combat")

        # Push heroes toward center for combat
        # Move blade to tile 3 (throne) and B's blade to tile 3
        # Submit move commands for tick 3
        resp = await client.post(f"/game/{game_id}/command", headers=headers_a,
                                 json={"action": "move", "hero_id": blade_a["id"], "args": {"target_tile": 3}})
        check("S6.1 Move blade toward throne", resp.status_code == 200)

        # Also send dash to get blade further
        resp = await client.post(f"/game/{game_id}/command", headers=headers_a,
                                 json={"action": "dash", "hero_id": blade_a["id"], "args": {"target_tile": 4}})
        check("S6.2 Dash command accepted", resp.status_code == 200)

        # Shield bastion
        resp = await client.post(f"/game/{game_id}/command", headers=headers_a,
                                 json={"action": "shield", "hero_id": bastion_a["id"], "args": {}})
        check("S6.3 Shield command accepted", resp.status_code == 200)

        # B moves blade toward center
        resp = await client.post(f"/game/{game_id}/command", headers=headers_b,
                                 json={"action": "move", "hero_id": blade_b["id"], "args": {"target_tile": 3}})
        check("S6.4 B moves blade toward throne", resp.status_code == 200)

        print("  Waiting for tick 3...")
        await asyncio.sleep(4)

        resp = await client.get(f"/game/{game_id}/state", headers=headers_a)
        state3 = resp.json()
        check("S6.5 Tick 3 resolved", state3["tick"] >= 3, f"tick={state3['tick']}")

        # Check tick replay has events (commands and/or creep combat)
        resp = await client.get(f"/game/{game_id}/tick/{state3['tick']}")
        tick3 = resp.json()
        event_types = set(e["type"] for e in tick3["events"])
        check("S6.6 Tick has game events", len(event_types) > 0,
              f"events={event_types}")

        # Try execute on next tick if heroes are adjacent
        a_heroes_3 = [h for h in state3["heroes"] if h["player_id"] == player_a]
        blade_a_3 = next(h for h in a_heroes_3 if h["name"] == "blade")
        b_heroes_3 = [h for h in state3["heroes"] if h["player_id"] == player_b]
        blade_b_3 = next(h for h in b_heroes_3 if h["name"] == "blade")

        # Submit execute command
        resp = await client.post(f"/game/{game_id}/command", headers=headers_a,
                                 json={"action": "execute", "hero_id": blade_a["id"],
                                        "args": {"target_hero_id": str(blade_b["id"])}})
        check("S6.7 Execute command accepted", resp.status_code == 200)

        # B uses fortify
        resp = await client.post(f"/game/{game_id}/command", headers=headers_b,
                                 json={"action": "fortify", "hero_id": next(h for h in b_heroes if h['name'] == 'bastion')["id"], "args": {}})
        check("S6.8 Fortify command accepted", resp.status_code == 200)

        print("  Waiting for tick 4...")
        await asyncio.sleep(4)

        resp = await client.get(f"/game/{game_id}/state", headers=headers_a)
        state4 = resp.json()
        check("S6.9 Tick 4 resolved", state4["tick"] >= 4, f"tick={state4['tick']}")

        # Check tick replay for damage/combat events
        resp = await client.get(f"/game/{game_id}/tick/{state4['tick']}")
        tick4 = resp.json()
        check("S6.10 Tick 4 has events", len(tick4["events"]) > 0)

        # ===========================================================
        # S7: Economy
        # ===========================================================
        section("S7: Economy")

        # Check gold accumulated
        check("S7.1 Gold A accumulated", state4["gold"]["a"] >= 4,
              f"gold_a={state4['gold']['a']}")
        check("S7.2 Gold B accumulated", state4["gold"]["b"] >= 4,
              f"gold_b={state4['gold']['b']}")

        # Try potion (heal a hero)
        resp = await client.post(f"/game/{game_id}/command", headers=headers_a,
                                 json={"action": "potion", "hero_id": blade_a["id"], "args": {}})
        check("S7.3 Potion command accepted", resp.status_code == 200)

        # Try upgrade_creeps
        resp = await client.post(f"/game/{game_id}/command", headers=headers_a,
                                 json={"action": "upgrade_creeps", "hero_id": 0, "args": {"lane": "mid"}})
        check("S7.4 Upgrade creeps accepted", resp.status_code == 200)

        # Try zap
        bolt_a_4 = next(h for h in state4["heroes"] if h["player_id"] == player_a and h["name"] == "bolt")
        resp = await client.post(f"/game/{game_id}/command", headers=headers_a,
                                 json={"action": "zap", "hero_id": bolt_a["id"],
                                        "args": {"target_hero_id": str(next(h for h in state4['heroes'] if h['player_id'] == player_b and h['name'] == 'bolt')['id'])}})
        check("S7.5 Zap command accepted", resp.status_code == 200)

        print("  Waiting for tick 5...")
        await asyncio.sleep(4)

        resp = await client.get(f"/game/{game_id}/state", headers=headers_a)
        state5 = resp.json()
        check("S7.6 Tick 5 resolved", state5["tick"] >= 5, f"tick={state5['tick']}")

        # Level up test
        xp_a = state5["xp"]["a"]
        if xp_a >= 5:
            resp = await client.post(f"/game/{game_id}/level_up", headers=headers_a,
                                     json={"hero_id": blade_a["id"]})
            check("S7.7 Level up accepted", resp.status_code == 200)
            lvl_data = resp.json()
            check("S7.8 Hero leveled to 2", lvl_data["level"] == 2)
        else:
            # Not enough XP from combat — test level_up rejection instead
            resp = await client.post(f"/game/{game_id}/level_up", headers=headers_a,
                                     json={"hero_id": blade_a["id"]})
            check("S7.7 Level up rejected (insufficient XP)", resp.status_code == 400,
                  f"xp_a={xp_a}, status={resp.status_code}")
            check("S7.8 Rejection message mentions XP",
                  "XP" in resp.json().get("detail", ""),
                  f"detail={resp.json().get('detail', '')}")

        # ===========================================================
        # S8: Game End
        # ===========================================================
        section("S8: Game End")

        # Let the game run through remaining ticks or wait for natural end
        # We'll use storm and execute aggressively to try to end the game
        # If not, the tick cap at 20 will end it

        # Submit aggressive commands for remaining ticks
        for tick_i in range(6):
            # A attacks with everything
            await client.post(f"/game/{game_id}/command", headers=headers_a,
                             json={"action": "move", "hero_id": blade_a["id"], "args": {"target_tile": 6}})
            await client.post(f"/game/{game_id}/command", headers=headers_a,
                             json={"action": "dash", "hero_id": blade_a["id"], "args": {"target_tile": 6}})
            await client.post(f"/game/{game_id}/command", headers=headers_a,
                             json={"action": "move", "hero_id": bolt_a["id"], "args": {"target_tile": 6}})

            # B also attacks
            await client.post(f"/game/{game_id}/command", headers=headers_b,
                             json={"action": "move", "hero_id": blade_b["id"], "args": {"target_tile": 0}})
            await client.post(f"/game/{game_id}/command", headers=headers_b,
                             json={"action": "dash", "hero_id": blade_b["id"], "args": {"target_tile": 0}})
            await client.post(f"/game/{game_id}/command", headers=headers_b,
                             json={"action": "move", "hero_id": next(h for h in b_heroes if h['name'] == 'bolt')["id"],
                                    "args": {"target_tile": 0}})

            await asyncio.sleep(4)  # Wait for tick

            resp = await client.get(f"/game/{game_id}/state", headers=headers_a)
            if resp.status_code != 200:
                continue
            st = resp.json()
            if st.get("status") == "finished":
                break

        # Check final state
        resp = await client.get(f"/game/{game_id}/state", headers=headers_a)
        final = resp.json()

        # If not finished yet, wait for remaining ticks (up to tick 20)
        wait_count = 0
        while final.get("status") != "finished" and wait_count < 60:
            await asyncio.sleep(4)
            resp = await client.get(f"/game/{game_id}/state", headers=headers_a)
            if resp.status_code != 200:
                wait_count += 1
                continue
            final = resp.json()
            wait_count += 1
            if final.get("tick", 0) >= 20:
                await asyncio.sleep(5)  # Give end_game job time
                resp = await client.get(f"/game/{game_id}/state", headers=headers_a)
                final = resp.json()
                break

        check("S8.1 Game finished", final["status"] == "finished", f"status={final['status']}")
        check("S8.2 Winner declared", len(final["winner"]) > 0, f"winner={final['winner']}")
        check("S8.3 Winner is valid player",
              final["winner"] in (player_a, player_b),
              f"winner={final['winner']}")

        # Full history
        resp = await client.get(f"/game/{game_id}/history")
        history = resp.json()
        check("S8.4 History has ticks", len(history["ticks"]) >= 1,
              f"ticks={len(history['ticks'])}")

        # Check health after game
        resp = await client.get("/health")
        check("S8.5 Server still healthy", resp.json()["status"] == "healthy")

        # ===========================================================
        # S9: AFK
        # ===========================================================
        section("S9: AFK")

        # Create a new game where one player goes AFK
        resp = await client.post("/lobby/create", headers=headers_a)
        game2_id = resp.json()["game_id"]
        resp = await client.post(f"/lobby/{game2_id}/join", headers=headers_b)
        check("S9.1 AFK game created", resp.status_code == 200)

        # Both assign
        await client.post(f"/lobby/{game2_id}/assign", headers=headers_a, json=assign_a)
        await client.post(f"/lobby/{game2_id}/assign", headers=headers_b, json=assign_b)
        resp = await client.post(f"/lobby/{game2_id}/start", headers=headers_a)
        check("S9.2 AFK game started", resp.status_code == 200)

        # Player A sends commands, player B does nothing (AFK)
        # Need to get hero IDs for game2
        resp = await client.get(f"/game/{game2_id}/state", headers=headers_a)
        g2_state = resp.json()
        g2_blade_a = next(h for h in g2_state["heroes"] if h["player_id"] == player_a and h["name"] == "blade")

        # Wait through 3+ ticks with A sending commands, B silent
        # AFK counter increments from tick 2 onward (tick_num > 1 guard)
        # After 3 consecutive empty ticks, B is forfeit
        for i in range(5):
            await client.post(f"/game/{game2_id}/command", headers=headers_a,
                             json={"action": "move", "hero_id": g2_blade_a["id"], "args": {"target_tile": 3}})
            await asyncio.sleep(4)
            resp = await client.get(f"/game/{game2_id}/state", headers=headers_a)
            if resp.status_code == 200 and resp.json().get("status") == "finished":
                break

        resp = await client.get(f"/game/{game2_id}/state", headers=headers_a)
        g2_final = resp.json()
        check("S9.3 AFK game finished", g2_final["status"] == "finished",
              f"status={g2_final['status']} tick={g2_final['tick']}")
        check("S9.4 AFK player lost", g2_final["winner"] == player_a,
              f"winner={g2_final['winner']}")

        # ===========================================================
        # S10: Concurrent Games
        # ===========================================================
        section("S10: Concurrent Games")

        # Register 2 more players for a second concurrent game
        resp = await client.post("/auth/register", json={"passphrase": "gamma-secret-pass", "nickname": "GammaPlayer"})
        token_c = resp.json()["token"]
        player_c = resp.json()["player_id"]
        headers_c = {"Authorization": f"Bearer {token_c}"}

        resp = await client.post("/auth/register", json={"passphrase": "delta-secret-pass", "nickname": "DeltaPlayer"})
        token_d = resp.json()["token"]
        player_d = resp.json()["player_id"]
        headers_d = {"Authorization": f"Bearer {token_d}"}

        # Create and start game 3
        resp = await client.post("/lobby/create", headers=headers_c)
        game3_id = resp.json()["game_id"]
        await client.post(f"/lobby/{game3_id}/join", headers=headers_d)
        await client.post(f"/lobby/{game3_id}/assign", headers=headers_c, json=assign_a)
        await client.post(f"/lobby/{game3_id}/assign", headers=headers_d, json=assign_b)
        resp = await client.post(f"/lobby/{game3_id}/start", headers=headers_c)
        check("S10.1 Concurrent game started", resp.status_code == 200)

        # Both games should tick independently
        resp = await client.get(f"/game/{game3_id}/state", headers=headers_c)
        g3_state = resp.json()
        check("S10.2 Game 3 is active", g3_state["status"] == "active")

        # Submit commands in game 3
        g3_blade_c = next(h for h in g3_state["heroes"] if h["player_id"] == player_c and h["name"] == "blade")
        resp = await client.post(f"/game/{game3_id}/command", headers=headers_c,
                                 json={"action": "move", "hero_id": g3_blade_c["id"], "args": {"target_tile": 2}})
        check("S10.3 Command in game 3 accepted", resp.status_code == 200)

        await asyncio.sleep(4)

        resp = await client.get(f"/game/{game3_id}/state", headers=headers_c)
        g3_after = resp.json()
        check("S10.4 Game 3 ticked", g3_after["tick"] >= 1, f"tick={g3_after['tick']}")

        # Verify game 3 heroes are independent from game 1/2
        g3_heroes = g3_after["heroes"]
        g3_players = set(h["player_id"] for h in g3_heroes)
        check("S10.5 Game 3 has correct players",
              player_c in g3_players and player_d in g3_players)

        # Health shows active games
        resp = await client.get("/health")
        health = resp.json()
        check("S10.6 Active games tracked",
              isinstance(health["active_games"], int) and health["active_games"] >= 1,
              f"active_games={health['active_games']}")

    # Shutdown
    server.should_exit = True
    try:
        await asyncio.wait_for(server_task, timeout=10.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass

    # Final report
    section("LANELER TEST REPORT")
    print(f"  Checks: {passed}/{passed + failed} passed")
    print(f"{'='*60}")

    if failed > 0:
        print(f"\n  FAILURES:")
        for e in errors:
            print(f"    - {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(run_tests())
