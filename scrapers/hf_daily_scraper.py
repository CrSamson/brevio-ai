"""
scrapers/hf_daily_scraper.py - HuggingFace Daily Papers scraper (Phase 5).

Pulls the daily-curated HF papers list from a third-party RSS mirror.
Every retained entry MUST yield a parseable arXiv id - that id is the
cross-link key into the `papers` table populated by Phase 4 (ArxivScraper).

Driven by config/sources.json[papers[]] with type='hf_daily':

    {
      "id": "hf_daily_papers",
      "type": "hf_daily",
      "feed_url": "https://papers.takara.ai/api/feed",
      "fallback_feed_url": "https://raw.githubusercontent.com/huangboming/huggingface-daily-paper-feed/main/feed.xml"
    }

Feed shapes (two known, parser handles both):

  takara.ai (primary):
    - link:    https://tldr.takara.ai/p/{arxiv_id}
    - summary: abstract text directly (no Authors/Upvotes markers)

  GitHub mirror (fallback):
    - link:    https://arxiv.org/abs/{arxiv_id}
    - summary: HTML with <b>Authors:</b> X, Y, Z then <b>Upvotes:</b> N
               then <b>Summary:</b> abstract...

Discipline:
  - On primary failure (HTTP error, bozo+0 entries, or 0 valid Papers
    after parsing), automatically tries the fallback URL.
  - If both fail, returns []. Never raises.
  - Entries without a parseable arxiv_id are logged and skipped.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

import feedparser
import requests

from scrapers.base import BaseScraper
from scrapers.schemas import Paper


log = logging.getLogger(__name__)


DEFAULT_TIMEOUT = 15
DEFAULT_UA      = "ai-news-aggregator/0.1"

# Same modern arXiv id pattern used in the arxiv scraper.
_ARXIV_ID_RE = re.compile(r"\b(\d{4}\.\d{4,5})")

# These three regex match the GitHub-mirror HTML format. They harmlessly
# fall through (no match) on the takara.ai primary, where summary text is
# unstructured.
_AUTHORS_RE = re.compile(r"<b>\s*Authors?:?\s*</b>\s*([^<]+?)\s*</p>", re.IGNORECASE)
_UPVOTES_RE = re.compile(r"<b>\s*Upvotes?:?\s*</b>\s*(\d+)",            re.IGNORECASE)
_SUMMARY_RE = re.compile(r"<b>\s*Summary:?\s*</b>\s*(.+?)\s*</p>",      re.IGNORECASE | re.DOTALL)


class HfDailyScraper(BaseScraper):
    """HuggingFace Daily Papers scraper, with primary + fallback feeds."""

    def __init__(self, source_config: dict) -> None:
        super().__init__(source_config)
        if "feed_url" not in source_config:
            raise ValueError(
                f"hf_daily source {self.source_id!r} requires 'feed_url'"
            )
        self.feed_url          = source_config["feed_url"]
        self.fallback_feed_url = source_config.get("fallback_feed_url")
        self.timeout           = int(source_config.get("timeout", DEFAULT_TIMEOUT))
        self.user_agent        = source_config.get("user_agent", DEFAULT_UA)

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def fetch(self, hours: int = 24) -> list[Paper]:
        """
        Try primary, fall back to fallback. Return Paper list. Never raises.

        `hours` is accepted for BaseScraper compatibility but not enforced -
        HF Daily is curated daily; we keep whatever the feed gives us.
        """
        primary = self._try_fetch(self.feed_url, label="primary")
        if primary is not None:
            items = self._parse_feed_bytes(primary)
            if items:
                return items
            log.info("[%s] primary parsed 0 valid items; trying fallback",
                     self.source_id)

        if self.fallback_feed_url:
            fallback = self._try_fetch(self.fallback_feed_url, label="fallback")
            if fallback is not None:
                return self._parse_feed_bytes(fallback)

        log.error("[%s] both primary and fallback failed; returning []",
                  self.source_id)
        return []

    # ------------------------------------------------------------------
    # Internal (also unit-test entry points)
    # ------------------------------------------------------------------

    def _try_fetch(self, url: str, *, label: str) -> bytes | None:
        """HTTP GET with all errors logged. Returns bytes on success, None on any error."""
        try:
            r = requests.get(
                url,
                timeout=self.timeout,
                headers={"User-Agent": self.user_agent},
                allow_redirects=True,
            )
            r.raise_for_status()
        except requests.RequestException as e:
            log.warning("[%s] %s feed HTTP error: %s", self.source_id, label, e)
            return None
        except Exception as e:  # noqa: BLE001
            log.exception("[%s] %s feed unexpected error: %s",
                          self.source_id, label, e)
            return None
        return r.content

    def _parse_feed_bytes(self, raw: bytes) -> list[Paper]:
        parsed = feedparser.parse(raw)

        if getattr(parsed, "bozo", False) and not parsed.entries:
            log.warning("[%s] feed parsed bozo=1 with 0 entries", self.source_id)
            return []

        results: list[Paper] = []
        for entry in parsed.entries:
            try:
                paper = self._entry_to_paper(entry)
            except Exception as e:  # noqa: BLE001
                log.warning("[%s] skipping bad entry: %s", self.source_id, e)
                continue
            if paper is not None:
                results.append(paper)

        return results

    def _entry_to_paper(self, entry: Any) -> Paper | None:
        # arxiv_id is most reliably in the entry link (HF papers point to arxiv.org/abs/<id>
        # on the fallback; takara.ai uses tldr.takara.ai/p/<id>).
        link = (entry.get("link") or "").strip()
        m    = _ARXIV_ID_RE.search(link)
        if not m:
            for fallback_field in (entry.get("id"), entry.get("summary"),
                                   entry.get("description")):
                if fallback_field:
                    m = _ARXIV_ID_RE.search(str(fallback_field))
                    if m:
                        break
        if not m:
            log.warning("[%s] no arxiv_id extractable from entry link=%r; skipping",
                        self.source_id, link)
            return None
        arxiv_id = m.group(1)

        title    = (entry.get("title") or "").strip()
        raw_summary = (entry.get("summary") or entry.get("description") or "")
        authors, upvotes, abstract = self._extract_hf_metadata(raw_summary)

        return Paper(
            sources          = ["hf_daily"],
            arxiv_id         = arxiv_id,
            url              = link or f"https://arxiv.org/abs/{arxiv_id}",
            pdf_url          = None,
            title            = title or arxiv_id,
            authors          = authors,
            abstract         = abstract,
            categories       = [],
            published_at     = self._parse_date(entry),
            updated_at_arxiv = None,
            hf_upvotes       = upvotes,
            raw_metadata     = self._raw_meta(entry),
        )

    @staticmethod
    def _extract_hf_metadata(summary: str) -> tuple[list[str], int | None, str | None]:
        """
        Extract (authors, hf_upvotes, abstract) from an HF Daily summary.

        - Fallback (HTML) feed exposes all three via <b>Authors:</b> /
          <b>Upvotes:</b> / <b>Summary:</b> markers.
        - Primary (takara.ai) summary is plain abstract text - none of the
          markers match, so authors=[], upvotes=None, abstract=summary.
        """
        if not summary:
            return [], None, None

        m_authors = _AUTHORS_RE.search(summary)
        authors: list[str] = []
        if m_authors:
            authors = [a.strip() for a in m_authors.group(1).split(",") if a.strip()]

        m_upvotes = _UPVOTES_RE.search(summary)
        upvotes   = int(m_upvotes.group(1)) if m_upvotes else None

        m_abstract = _SUMMARY_RE.search(summary)
        if m_abstract:
            abstract = m_abstract.group(1).strip() or None
        else:
            # Primary-feed style: the whole summary is the abstract.
            abstract = summary.strip() or None

        return authors, upvotes, abstract

    @staticmethod
    def _parse_date(entry: Any) -> datetime | None:
        for attr in ("published_parsed", "updated_parsed"):
            ts = getattr(entry, attr, None)
            if ts:
                return datetime(*ts[:6], tzinfo=timezone.utc)
        return None

    @staticmethod
    def _raw_meta(entry: Any) -> dict:
        def _safe(v: Any) -> Any:
            if v is None or isinstance(v, (str, int, float, bool)):
                return v
            if isinstance(v, dict):
                return {str(k): _safe(val) for k, val in v.items()}
            if isinstance(v, (list, tuple)):
                return [_safe(item) for item in v]
            return str(v)
        keys = ("id", "guid", "summary", "description", "published",
                "updated", "tags", "link", "links")
        out: dict = {}
        for k in keys:
            v = entry.get(k) if hasattr(entry, "get") else None
            if v is not None:
                out[k] = _safe(v)
        return out
