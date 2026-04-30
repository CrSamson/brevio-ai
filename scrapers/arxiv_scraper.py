"""
scrapers/arxiv_scraper.py - arXiv Atom API scraper (Phase 4).

Uses export.arxiv.org/api/query (Atom, not the legacy RSS endpoint) for
control over max_results and sort order. Driven by one entry of
config/sources.json[papers[]] with type='arxiv':

    {
      "id": "arxiv_cs_lg_ai",
      "type": "arxiv",
      "categories": ["cs.LG", "cs.AI"],
      "max_results": 10,
      "keyword_filter": null
    }

Discipline notes:
  - We respect arXiv's >= 3-second-per-call rate limit (process-global guard).
  - We DO NOT download PDFs. pdf_url is stored as a string only.
  - Volume gate: if a single fetch returns more than 500 entries we log a
    warning and return [] (sanity check on max_results misconfiguration).
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from typing import Any

import feedparser
import requests

from scrapers.base import BaseScraper
from scrapers.schemas import Paper


log = logging.getLogger(__name__)


API_BASE        = "http://export.arxiv.org/api/query"
DEFAULT_TIMEOUT = 30
DEFAULT_UA      = "ai-news-aggregator/0.1"

# arXiv API guidance: stay under 1 request per 3 seconds.
MIN_INTERVAL_S  = 3.0

# A single fetch should never legitimately exceed this.
VOLUME_GATE     = 500

# arXiv ids (modern format): 4 digits, dot, 4 or 5 digits. We only anchor
# at the start (\b) - the trailing version suffix 'v1', 'v2' etc. is a
# word char run, so a closing \b would never match at that position.
_ARXIV_ID_RE    = re.compile(r"\b(\d{4}\.\d{4,5})")


# --- process-global rate limiter -------------------------------------------

_last_call_at: float = 0.0


def _wait_for_rate_limit() -> None:
    """Block until at least MIN_INTERVAL_S has passed since the last API call.

    First call (with default _last_call_at=0.0): elapsed is ~current monotonic,
    much larger than MIN_INTERVAL_S, so no sleep. Subsequent calls within
    the cooldown sleep for the remainder.
    """
    global _last_call_at
    elapsed = time.monotonic() - _last_call_at
    if elapsed < MIN_INTERVAL_S:
        time.sleep(MIN_INTERVAL_S - elapsed)
    _last_call_at = time.monotonic()


# --- scraper ---------------------------------------------------------------

class ArxivScraper(BaseScraper):
    """arXiv Atom API scraper, driven by a sources.json paper entry."""

    def __init__(self, source_config: dict) -> None:
        super().__init__(source_config)
        self.categories     = list(source_config.get("categories", []))
        self.max_results    = int(source_config.get("max_results", 10))
        self.keyword_filter = source_config.get("keyword_filter") or []
        self.timeout        = int(source_config.get("timeout", DEFAULT_TIMEOUT))
        self.user_agent     = source_config.get("user_agent", DEFAULT_UA)

        if not self.categories:
            raise ValueError(
                f"arxiv source {self.source_id!r} requires a non-empty 'categories' list"
            )

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def fetch(self, hours: int = 24) -> list[Paper]:
        """
        Return Paper objects for the most-recent `max_results` submissions.

        Note: `hours` is accepted only for BaseScraper interface compatibility -
        the arXiv API has no per-hour cutoff. Filtering by Paper.published_at
        is the caller's job.

        Never raises. On any unhandled error, logs and returns [].
        """
        try:
            url = self._build_url()
            _wait_for_rate_limit()
            r = requests.get(
                url,
                timeout=self.timeout,
                headers={"User-Agent": self.user_agent},
                allow_redirects=True,
            )
            r.raise_for_status()
        except requests.RequestException as e:
            log.error("[%s] HTTP error fetching arXiv API: %s", self.source_id, e)
            return []
        except Exception as e:  # noqa: BLE001
            log.exception("[%s] unexpected fetch error: %s", self.source_id, e)
            return []

        return self._parse_atom_bytes(r.content)

    # ------------------------------------------------------------------
    # Internal (also the unit-test entry point)
    # ------------------------------------------------------------------

    def _build_url(self) -> str:
        # arXiv expects the OR connector as the literal string '+OR+' inside
        # search_query, NOT a urlencoded form. Build manually.
        cat_query = "+OR+".join(f"cat:{c}" for c in self.categories)
        return (
            f"{API_BASE}"
            f"?search_query={cat_query}"
            f"&max_results={self.max_results}"
            "&sortBy=submittedDate&sortOrder=descending"
        )

    def _parse_atom_bytes(self, raw: bytes) -> list[Paper]:
        """Parse Atom bytes into Paper objects. Pure function of input."""
        parsed = feedparser.parse(raw)

        if getattr(parsed, "bozo", False) and not parsed.entries:
            log.warning("[%s] Atom parsed bozo=1 with 0 entries", self.source_id)
            return []

        if len(parsed.entries) > VOLUME_GATE:
            log.warning(
                "[%s] volume gate tripped: %d entries > %d. Likely misconfigured "
                "max_results. Returning [].",
                self.source_id, len(parsed.entries), VOLUME_GATE,
            )
            return []

        results: list[Paper] = []
        for entry in parsed.entries:
            try:
                paper = self._entry_to_paper(entry)
            except Exception as e:  # noqa: BLE001 - per-entry isolation
                log.warning("[%s] skipping bad entry: %s", self.source_id, e)
                continue
            if paper is None:
                continue
            if not self._matches_keyword(paper):
                continue
            results.append(paper)

        return results

    def _entry_to_paper(self, entry: Any) -> Paper | None:
        # arXiv puts the abs URL with version suffix in <id>.
        eid = (entry.get("id") or "").strip()
        m   = _ARXIV_ID_RE.search(eid)
        if not m:
            log.warning("[%s] no arxiv_id in entry id=%r; skipping",
                        self.source_id, eid)
            return None
        arxiv_id = m.group(1)

        # Strip version suffix from the URL too.
        url = re.sub(r"v\d+$", "", eid) if eid else f"https://arxiv.org/abs/{arxiv_id}"

        pdf_url = self._extract_pdf_url(entry, arxiv_id)

        title    = (entry.get("title")   or "").strip()
        abstract = (entry.get("summary") or "").strip()

        authors = [
            a.get("name", "").strip()
            for a in (entry.get("authors") or []) if a.get("name")
        ]
        categories = [
            t.get("term", "")
            for t in (entry.get("tags") or []) if t.get("term")
        ]

        published        = self._parse_date(entry, "published_parsed")
        updated_at_arxiv = self._parse_date(entry, "updated_parsed")

        return Paper(
            sources          = ["arxiv"],
            arxiv_id         = arxiv_id,
            url              = url,
            pdf_url          = pdf_url,
            title            = title,
            authors          = authors,
            abstract         = abstract,
            categories       = categories,
            published_at     = published,
            updated_at_arxiv = updated_at_arxiv,
            hf_upvotes       = None,
            raw_metadata     = {"arxiv_entry_id": eid},
        )

    def _matches_keyword(self, paper: Paper) -> bool:
        if not self.keyword_filter:
            return True
        haystack = (paper.title + " " + (paper.abstract or "")).lower()
        return any(k.lower() in haystack for k in self.keyword_filter)

    @staticmethod
    def _extract_pdf_url(entry: Any, arxiv_id: str) -> str | None:
        for link in (entry.get("links") or []):
            if link.get("type") == "application/pdf":
                href = link.get("href")
                if href:
                    return href
        # Fall back to the canonical PDF URL form.
        return f"https://arxiv.org/pdf/{arxiv_id}.pdf"

    @staticmethod
    def _parse_date(entry: Any, attr: str) -> datetime | None:
        ts = getattr(entry, attr, None)
        if ts:
            return datetime(*ts[:6], tzinfo=timezone.utc)
        return None
