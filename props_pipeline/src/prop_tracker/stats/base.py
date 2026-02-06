"""Stats provider interfaces."""
from __future__ import annotations

from typing import Protocol


class StatsProvider(Protocol):
    name: str

    def fetch_stats(self, props_csv: str, out_csv: str) -> str:
        ...
