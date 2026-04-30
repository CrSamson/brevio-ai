"""
scrapers/base.py - common interface for every scraper.

Every source type (RSS blog, arXiv API, HF Daily Papers) implements the
same .fetch(hours) -> list. The Runner can then drive them uniformly
without knowing what kind of source it is.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseScraper(ABC):
    """ABC for any scraper driven from one entry of config/sources.json."""

    def __init__(self, source_config: dict) -> None:
        self.source_config = source_config
        self.source_id     = source_config.get("id", "<unknown>")

    @abstractmethod
    def fetch(self, hours: int) -> list[Any]:
        """
        Return items published within the last `hours` hours.

        Implementations must:
          - Never raise: catch all exceptions and return [] on failure.
          - Log per-entry failures and continue with the rest.
        """
        ...
