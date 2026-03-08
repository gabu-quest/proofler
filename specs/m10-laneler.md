# M10: laneler — Async Tile MOBA on the -ler Stack

## Overview

1v1 async tile MOBA. Each player controls 3 heroes (assassin, tank, mage) in a mirror match across 3 lanes. 30-second ticks, 3 actions per tick, no auto-attack. Games last 5-10 minutes. The final proof that the -ler stack can run a real public game server on Koyeb's free tier.

## Why This Is Still proofler

This isn't a separate game project — it's the ultimate integration test. sqler holds game state, qler resolves ticks, FastAPI serves the API. If it fits in 512 MB on 0.1 vCPU and survives an 8h bot soak, the stack is proven.

---

## Game Design

### Board

```
Team A side          Center         Team B side
[Base] [1] [2]      [Throne]      [2] [1] [Base]
[Base] [1] [2]      [Throne]      [2] [1] [Base]
[Base] [1] [2]      [Throne]      [2] [1] [Base]
```

- 3 lanes (top, mid, bot)
- 7 tiles per lane: Base — 2 approach tiles — Throne — 2 approach tiles — Base
- 21 tiles total. Entire game visible at a glance.
- Throne is the contested center tile of each lane.

### Players & Heroes

- **1v1 only.** Each player controls 3 heroes.
- **Mirror match.** Both players get the same 3 heroes: Blade, Bolt, Bastion.
- **Initial placement:** Before tick 1, each player assigns one hero per lane. That's your first strategic decision.
- No draft, no roster selection. Strategy comes from positioning and action economy, not hero picks.

### The Three Heroes

**Blade** — *the assassin*
- HP: 6 | Speed: 2 tiles | No auto-attack
- `Dash` (small, 0 CD) — move 2 extra tiles in one direction
- `Execute` (big, 1 tick CD) — deal 6 damage to adjacent target. 2x if target below half HP.
- *Fast, fragile, lethal. Gets in, kills, gets out.*

**Bolt** — *the mage*
- HP: 6 | Speed: 1 tile | No auto-attack
- `Zap` (small, 0 CD) — 3 damage to target within 2 tiles
- `Storm` (big, 1 tick CD) — 4 damage to ALL enemies within 2 tiles of target tile (AoE)
- *Slow, squishy, but controls space. Storm punishes anyone clumped with creeps.*

**Bastion** — *the tank*
- HP: 12 | Speed: 1 tile | No auto-attack
- `Shield` (small, 0 CD) — block the next instance of damage this tick (self only)
- `Fortify` (big, 1 tick CD) — all allies within 1 tile take half damage for 1 tick
- *Slow, tanky, protects others. Does almost no damage alone but keeps Bolt alive.*

### No Auto-Attack

Heroes do nothing unless commanded. If you don't spend an action on a hero, it stands still. This is the core constraint — you have 3 heroes but only 3 actions, so every action matters.

### Actions & Ticks

- **30-second ticks.** ~10 seconds per action to think.
- **3 actions per tick** across all heroes (not per hero).
- Action types:
  - `move` (1 action) — move hero up to its speed in tiles
  - `small skill` (1 action) — use the hero's small skill
  - `big skill` (1 action) — use the hero's big skill (1 tick CD after use)
- You can stack actions on one hero: `move Blade, dash Blade, execute Blade` = all-in assassination turn.
- Or spread them: `move Blade, zap Bolt, shield Bastion` = each hero does one thing.
- `move + move + move` = pure repositioning turn, no damage dealt.

### Cooldowns

- **Small skills:** 0 CD. Usable every tick.
- **Big skills:** 1 tick CD. Use it this tick, can't use it next tick. Available again the tick after.
- Simple to track: big skill is either "ready" or "on cooldown."

### Creeps

- Spawn 1 per lane per tick from each base (6 total per tick)
- Walk toward enemy base, 1 tile per tick
- Attack nearest enemy for 1 damage when adjacent (hero, creep, or base)
- 2 HP each. Die in one hero ability or two creep hits.
- No player commands needed — creeps are automatic.

### Death & Respawn

- Heroes respawn at their base tile on the **next tick.** No death timer.
- Dying costs position (sent back to base) + gives opponent gold and XP.
- Killing someone deep in their territory is less valuable — they respawn close to where they were.
- Killing someone pushed into YOUR territory is devastating — they lose 4-5 tiles of position.

### Economy

**Gold** (earned per tick):
- Passive: +1 gold per tick
- Creep kill: +2 gold (last hit with any ability)
- Hero kill: +5 gold

**Gold spending** (submit as an action alternative):
- `Upgrade Creeps` (5 gold) — one lane's creeps get +1 HP and +1 damage for the rest of the game. Stacks up to 3 times per lane.
- `Potion` (3 gold) — one hero gets +3 HP instantly next tick (can't exceed max HP)

**XP** (global bucket):
- Creep kill: +1 XP
- Hero kill: +3 XP

**XP spending** (submit between ticks, free — not an action):
- Level up one hero: costs 5/10/15 XP for levels 2/3/4
- Each level: +2 max HP (healed on level-up) + skill damage scales (+1 per level)
- Max level 4 per hero. Leveling is a strategic choice — dump all XP into Blade for a carry, or spread evenly?

### Win Condition

1. **Destroy enemy base** in any lane — bases have 10 HP, take damage from heroes and creeps
2. If no base destroyed after 20 ticks (10 minutes): **most hero kills wins**
3. Tie: team with more total base HP remaining wins

### Game Length

- First contact: tick 2-3 (60-90 seconds)
- First kills: tick 4-6 (2-3 minutes)
- Base under serious threat: tick 10-14 (5-7 minutes)
- Game over: tick 12-20 (6-10 minutes)
- Hard cap: 20 ticks (10 minutes). Lobby auto-cleans 30 minutes after creation.

---

## Tick Resolution Order

Every 30 seconds:

1. **Player commands resolve** — sorted by `submitted_at ASC`
2. **Fizzle check** per command against current board state:
   - Hero dead → fizzle
   - Move target occupied → fizzle
   - Ability target out of range → fizzle
   - Big skill on cooldown → fizzle
3. **Creep phase** — spawn new creeps, move existing, creep auto-attack
4. **Damage resolution** — apply all damage, check deaths, process respawns
5. **Economy** — distribute gold/XP from kills, passive gold
6. **Cooldowns** — decrement big skill CDs by 1
7. **State snapshot** — save board, queue events for clients
8. **Next tick** — `enqueue(tick_resolve, game_id, tick+1, _delay=30)`

---

## Architecture (-ler Stack)

### sqler Models

```
Player   — id (ULID), passphrase_hash, nickname, token, token_expires_at
Game     — id (ULID), status (waiting/ready/assigning/active/finished),
           tick, player_a, player_b, started_at, ends_at, winner,
           gold_a, gold_b, xp_a, xp_b
Hero     — id, game_id, player_id, name (blade/bolt/bastion), lane, tile,
           hp, max_hp, level, big_skill_cd, is_dead
Creep    — id, game_id, team (a/b), lane, tile, hp, atk, is_dead
Base     — game_id, team, lane, hp
Command  — id, game_id, player_id, hero_id, tick, action, args{},
           submitted_at, resolved, fizzled, fizzle_reason
GameTick — game_id, tick, events[], board_snapshot{}
```

### qler Jobs

| Job | Trigger | Notes |
|-----|---------|-------|
| `tick_resolve` | Self-chaining `_delay=30` | Core game loop per game |
| `end_game` | Enqueued at start `_delay=600` | 20-tick hard cap |
| `cleanup_game` | After game ends | Archive game data |

### FastAPI Endpoints

```
# Auth (no token required)
POST /auth/register          → {passphrase, nickname?} → token
POST /auth/login             → {passphrase} → token (re-auth)
GET  /auth/nicknames         → 3 random nickname suggestions

# Lobby (token required)
POST /lobby/create           → create game, return lobby ULID + join URL
GET  /lobby/{id}             → lobby state (players, phase, heroes assigned)
POST /lobby/{id}/join        → join as team B
POST /lobby/{id}/assign      → assign heroes to lanes {blade: "top", bolt: "mid", bastion: "bot"}
POST /lobby/{id}/start       → start game (host only, requires both assigned)

# Game (token required)
GET  /game/{id}/state        → full board state (no fog)
POST /game/{id}/command      → submit action (max 3 per tick, 429 if exceeded)
GET  /game/{id}/tick/{n}     → tick replay (events + board snapshot)
GET  /game/{id}/history      → full replay (all ticks)

# System
GET  /health                 → server health + active games + RSS
```

### Auth

No accounts, no email, no GDPR. Identity is a passphrase.

**Registration flow:**
1. Player picks (or generates) a secret passphrase — any length, the longer the better
2. Server hashes it (argon2), stores the hash, returns a bearer token (ULID)
3. Player picks a nickname (or chooses from 3 random ones: e.g. "IronCreep", "LaneDrifter", "BoltFiend")
4. Token is used for all subsequent requests (`Authorization: Bearer <token>`)
5. Token expires after 24h of inactivity — re-auth with passphrase to get a new one

**Rejoin:** Same passphrase → same identity → can rejoin active games after disconnect.

**No personal data stored.** Just: hash, nickname, token, token_expires_at. Nothing to GDPR.

### Lobby Flow

```
1. Player A: POST /lobby/create      → lobby ULID + join URL
2. Player A: shares URL with friend
3. Player B: POST /lobby/{id}/join   → joins as team B
4. Both:     POST /lobby/{id}/assign → assign heroes to lanes
5. Player A: POST /lobby/{id}/start  → game begins (requires both assigned)
```

Lobby states: `waiting` → `ready` (both joined) → `assigning` (hero placement) → `active` (game running) → `finished`

Lobby auto-expires 10 minutes after creation if game hasn't started. Active games hard-cap at 20 ticks + 30 min cleanup.

### Abuse Prevention

- Rate limit: 3 commands per player per tick (server-side, returns 429)
- IP throttle: max 60 requests/min per IP (FastAPI middleware)
- Max 3 concurrent games per token
- Max 10 tokens per IP per hour (prevents passphrase spam)
- AFK: 0 commands for 3 consecutive ticks → auto-forfeit

---

## Koyeb Deployment

- Single process: uvicorn + qler Worker in-process
- SQLite with constrained PRAGMAs (8 MB cache, no mmap)
- Per-game memory: ~5-10 KB (21 tiles, 6 heroes, ~30 creeps, commands)
- At 0.1 vCPU: tick resolution <100ms per game, supports 10-20 concurrent games
- Archival: finished games archived after 5 minutes

---

## Success Criteria

1. Server runs on Koyeb free tier (512 MB, 0.1 vCPU)
2. 2 concurrent games run to completion without missed ticks
3. Tick resolution < 500ms for worst case (tick 20, many creeps)
4. Every command recorded, every tick replayable
5. Shareable lobby URLs work
6. 8h bot soak with simulated players — stable RSS, no tick drift
