"""Random nickname generator for laneler players."""

from __future__ import annotations

import random

ADJECTIVES = [
    "Iron", "Shadow", "Swift", "Bold", "Crimson",
    "Storm", "Frost", "Dark", "Brave", "Silent",
    "Golden", "Fierce", "Keen", "Grim", "Jade",
    "Rapid", "Steel", "Wild", "Dusk", "Bright",
]

NOUNS = [
    "Blade", "Bolt", "Shield", "Creep", "Tower",
    "Lane", "Throne", "Drift", "Fang", "Spark",
    "Wraith", "Viper", "Hawk", "Wolf", "Bear",
    "Flame", "Tide", "Rune", "Shard", "Storm",
]


def random_nickname() -> str:
    return random.choice(ADJECTIVES) + random.choice(NOUNS)


def nickname_suggestions(n: int = 3) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    while len(result) < n:
        name = random_nickname()
        if name not in seen:
            seen.add(name)
            result.append(name)
    return result
