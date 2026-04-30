"""
scrapers/rss_blog_scraper.py - generic RSS/Atom scraper.

One class, driven entirely by a config dict (one entry of
config/sources.json[blogs[]]). Same code path for Anthropic / OpenAI /
AWS / Google / etc - only the URL changes.

Per multi-source plan, Phase 2:
  - HTTP GET with a 15s timeout and a stable User-Agent.
  - feedparser.parse on the response bytes.
  - Filter entries by published_parsed >= now - hours.
  - Optional Docling content fetch (per source_config.fetch_content).
  - Per-entry try/except so one bad row never drops the others.
  - Per-source try/except so one dead feed never aborts the runner.

Testability: the public .fetch() does HTTP, but the parsing logic lives
in ._parse_feed_bytes(raw, hours) so unit tests can drive it from a
fixture file without mocking requests.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import feedparser
import requests

from scrapers.base import BaseScraper
from scrapers.schemas import BlogArticle


log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 15
DEFAULT_UA      = "ai-news-aggregator/0.1"


class RssBlogScraper(BaseScraper):
    """Generic RSS/Atom scraper, driven by a sources.json blog entry."""

    def __init__(self, source_config: dict) -> None:
        super().__init__(source_config)
        if "feed_url" not in source_config:
            raise ValueError(f"source_config missing 'feed_url' for id={self.source_id}")
        self.feed_url      = source_config["feed_url"]
        self.fetch_content = bool(source_config.get("fetch_content", False))
        self.timeout       = int(source_config.get("timeout", DEFAULT_TIMEOUT))
        self.user_agent    = source_config.get("user_agent", DEFAULT_UA)
        # Docling has heavy startup; only build the converter on first need.
        self._converter = None

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def fetch(self, hours: int) -> list[BlogArticle]:
        """Return BlogArticle list for entries within `hours` hours.

        Never raises. On any unhandled error, logs and returns [].
        """
        try:
            r = requests.get(
                self.feed_url,
                timeout=self.timeout,
                headers={"User-Agent": self.user_agent},
                allow_redirects=True,
            )
            r.raise_for_status()
        except requests.RequestException as e:
            log.error("[%s] HTTP error fetching feed: %s", self.source_id, e)
            return []
        except Exception as e:  # noqa: BLE001
            log.exception("[%s] unexpected error fetching feed: %s", self.source_id, e)
            return []

        return self._parse_feed_bytes(r.content, hours)

    # ------------------------------------------------------------------
    # Internal (also the unit-test entry point)
    # ------------------------------------------------------------------

    def _parse_feed_bytes(self, raw: bytes, hours: int) -> list[BlogArticle]:
        """Parse feed bytes and return BlogArticle list. Pure function of input."""
        parsed = feedparser.parse(raw)

        if getattr(parsed, "bozo", False) and not parsed.entries:
            log.warning("[%s] feed parsed bozo=1 with 0 entries", self.source_id)
            return []

        # Round cutoff down to midnight UTC. Some feeds (notably Anthropic)
        # stamp entries with date-only timestamps (00:00:00); a strict
        # (now - hours) cutoff would drop today's article whenever this
        # runs after midnight. Matches the legacy AnthropicScraper window.
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        cutoff = cutoff.replace(hour=0, minute=0, second=0, microsecond=0)
        results: list[BlogArticle] = []

        for entry in parsed.entries:
            try:
                article = self._entry_to_article(entry, cutoff)
            except Exception as e:  # noqa: BLE001 - per-entry isolation
                log.warning("[%s] skipping bad entry: %s", self.source_id, e)
                continue
            if article is not None:
                results.append(article)

        # newest first; entries with no date sort to the end
        results.sort(
            key=lambda a: a.published_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        return results

    def _entry_to_article(self, entry: Any, cutoff: datetime) -> BlogArticle | None:
        url   = (entry.get("link")  or "").strip()
        title = (entry.get("title") or "").strip()
        if not url or not title:
            return None  # malformed entry, skip silently

        published = self._parse_date(entry)
        if published is not None and published < cutoff:
            return None

        content_md      : str | None = None
        content_fetched : bool       = False
        if self.fetch_content:
            content_md      = self._fetch_content(url)
            content_fetched = content_md is not None

        return BlogArticle(
            source          = self.source_id,
            url             = url,
            title           = title,
            author          = (entry.get("author") or "").strip() or None,
            published_at    = published,
            summary         = None,                    # LLM step fills this later
            content_md      = content_md,
            content_fetched = content_fetched,
            raw_metadata    = self._raw_meta(entry),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _fetch_content(self, url: str) -> str | None:
        """Docling extraction. Failures return None - never drop the article."""
        try:
            if self._converter is None:
                from docling.document_converter import DocumentConverter
                self._converter = DocumentConverter()
            result = self._converter.convert(url)
            return result.document.export_to_markdown()
        except Exception as e:  # noqa: BLE001
            log.warning("[%s] Docling failed for %s: %s", self.source_id, url, e)
            return None

    @staticmethod
    def _parse_date(entry: Any) -> datetime | None:
        for attr in ("published_parsed", "updated_parsed"):
            ts = getattr(entry, attr, None)
            if ts:
                return datetime(*ts[:6], tzinfo=timezone.utc)
        return None

    @staticmethod
    def _raw_meta(entry: Any) -> dict:
        """JSON-safe slice of the feedparser entry.

        feedparser returns FeedParserDict (subclass of dict) with parsed-date
        tuples and other non-JSON-safe items. Recursively coerce to plain
        dict/list/scalar; fall back to str() for anything weird so the JSONB
        adapter never trips.
        """
        def _safe(v: Any) -> Any:
            if v is None or isinstance(v, (str, int, float, bool)):
                return v
            if isinstance(v, dict):
                return {str(k): _safe(val) for k, val in v.items()}
            if isinstance(v, (list, tuple)):
                return [_safe(item) for item in v]
            return str(v)

        keys = ("id", "guid", "summary", "author", "tags", "published", "updated")
        out: dict = {}
        for k in keys:
            v = entry.get(k) if hasattr(entry, "get") else None
            if v is None:
                continue
            out[k] = _safe(v)
        return out
