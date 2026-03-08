"""Authentication: argon2 passphrase hashing, token management, FastAPI dependency."""

from __future__ import annotations

import time
from collections import defaultdict

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from ulid import ULID

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from sqler import F

from .models import Player

# ---------------------------------------------------------------------------
# Argon2 hasher (32 MB memory_cost — Koyeb-safe)
# ---------------------------------------------------------------------------

_hasher = PasswordHasher(memory_cost=32768, time_cost=2, parallelism=1)


def hash_passphrase(passphrase: str) -> str:
    return _hasher.hash(passphrase)


def verify_passphrase(passphrase_hash: str, passphrase: str) -> bool:
    try:
        return _hasher.verify(passphrase_hash, passphrase)
    except VerifyMismatchError:
        return False


def generate_token() -> str:
    return str(ULID())


TOKEN_TTL = 24 * 3600  # 24 hours


def token_expiry() -> int:
    return int(time.time()) + TOKEN_TTL


# ---------------------------------------------------------------------------
# Rate limiting (in-memory, resets on restart — fine for single-process)
# ---------------------------------------------------------------------------

_request_counts: dict[str, list[float]] = defaultdict(list)
_token_counts: dict[str, list[float]] = defaultdict(list)

REQUEST_LIMIT = 60   # per minute per IP
TOKEN_LIMIT = 10     # per hour per IP


def _cleanup(entries: list[float], window: float) -> list[float]:
    cutoff = time.time() - window
    return [t for t in entries if t > cutoff]


def check_request_rate(ip: str) -> bool:
    """Returns True if under rate limit."""
    _request_counts[ip] = _cleanup(_request_counts[ip], 60)
    if len(_request_counts[ip]) >= REQUEST_LIMIT:
        return False
    _request_counts[ip].append(time.time())
    return True


def check_token_rate(ip: str) -> bool:
    """Returns True if under token creation limit."""
    _token_counts[ip] = _cleanup(_token_counts[ip], 3600)
    if len(_token_counts[ip]) >= TOKEN_LIMIT:
        return False
    _token_counts[ip].append(time.time())
    return True


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

_bearer = HTTPBearer(auto_error=False)


async def get_current_player(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> Player:
    if credentials is None:
        raise HTTPException(status_code=401, detail="Missing token")

    token = credentials.credentials
    player = await Player.query().filter(F("token") == token).first()

    if player is None:
        raise HTTPException(status_code=401, detail="Invalid token")

    if player.token_expires_at < int(time.time()):
        raise HTTPException(status_code=401, detail="Token expired")

    return player
