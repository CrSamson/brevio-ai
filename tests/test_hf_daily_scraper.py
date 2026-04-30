r"""
tests/test_hf_daily_scraper.py - unit tests for HfDailyScraper.

Plain runnable script. Drives parsing via ._parse_feed_bytes() so most
tests need no HTTP mocking. Two tests stub requests.get to verify the
primary -> fallback fallthrough behavior.

Covers:
  1. Parses takara.ai (primary) fixture into Papers; arxiv_id extracted;
     hf_upvotes is None (primary doesn't expose upvotes).
  2. Parses GitHub (fallback) fixture; hf_upvotes IS NOT NULL on most
     entries; authors list non-empty.
  3. Every parsed arxiv_id matches ^\d{4}\.\d{4,5}$.
  4. fetch() with primary timing out + fallback succeeding -> uses fallback.
  5. fetch() with both endpoints failing -> returns []. No exception raised.

Run:
    python tests/test_hf_daily_scraper.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scrapers.hf_daily_scraper import HfDailyScraper
from scrapers.schemas import Paper

import scrapers.hf_daily_scraper as hf_mod  # for monkey-patching requests.get


FIXTURE_DIR    = Path(__file__).parent / "fixtures"
PRIMARY_XML    = FIXTURE_DIR / "hf_daily_primary.xml"
FALLBACK_XML   = FIXTURE_DIR / "hf_daily_fallback.xml"

PRIMARY_URL    = "https://papers.takara.ai/api/feed"
FALLBACK_URL   = "https://raw.githubusercontent.com/huangboming/huggingface-daily-paper-feed/main/feed.xml"

ARXIV_ID_STRICT = re.compile(r"^\d{4}\.\d{4,5}$")


def _make() -> HfDailyScraper:
    return HfDailyScraper({
        "id":                "hf_daily_test",
        "type":              "hf_daily",
        "feed_url":          PRIMARY_URL,
        "fallback_feed_url": FALLBACK_URL,
    })


# Lightweight stub for a `requests` Response object.
class _FakeResponse:
    def __init__(self, content: bytes, status_code: int = 200) -> None:
        self.content     = content
        self.status_code = status_code
    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"HTTP {self.status_code}")


# ---------------------------------------------------------------------------
# Tests - parsing
# ---------------------------------------------------------------------------

def test_primary_parses_correctly() -> None:
    raw   = PRIMARY_XML.read_bytes()
    items = _make()._parse_feed_bytes(raw)
    assert len(items) > 0, "primary fixture must parse to >=1 paper"
    p = items[0]
    assert isinstance(p, Paper)
    assert p.sources == ["hf_daily"]
    assert p.arxiv_id, "arxiv_id must be set"
    assert p.hf_upvotes is None, "primary feed doesn't expose upvotes"
    assert p.abstract, "primary summary IS the abstract"
    print(f"  ok - parsed {len(items)} primary papers; first arxiv_id={p.arxiv_id}, "
          f"abstract={p.abstract[:60]!r}...")


def test_fallback_parses_correctly_with_upvotes() -> None:
    raw   = FALLBACK_XML.read_bytes()
    items = _make()._parse_feed_bytes(raw)
    assert len(items) > 0
    with_upvotes = [p for p in items if p.hf_upvotes is not None]
    with_authors = [p for p in items if p.authors]
    # Fallback has <b>Upvotes:</b> N markers; expect most entries to surface them.
    assert len(with_upvotes) >= len(items) // 2, (
        f"expected >=50% with upvotes, got {len(with_upvotes)}/{len(items)}"
    )
    assert len(with_authors) >= len(items) // 2, (
        f"expected >=50% with authors, got {len(with_authors)}/{len(items)}"
    )
    print(f"  ok - {len(items)} fallback papers, {len(with_upvotes)} with upvotes, "
          f"{len(with_authors)} with authors")


def test_arxiv_id_regex_matches_all() -> None:
    for label, fx in (("primary", PRIMARY_XML), ("fallback", FALLBACK_XML)):
        items = _make()._parse_feed_bytes(fx.read_bytes())
        bad = [p.arxiv_id for p in items if not ARXIV_ID_STRICT.match(p.arxiv_id or "")]
        assert not bad, f"{label}: non-conforming arxiv_ids: {bad}"
    print("  ok - all arxiv_ids in both fixtures match ^\\d{4}\\.\\d{4,5}$")


# ---------------------------------------------------------------------------
# Tests - HTTP fallback behavior
# ---------------------------------------------------------------------------

def test_primary_fail_uses_fallback() -> None:
    """Primary raises Timeout; fallback succeeds with fixture bytes."""
    fallback_bytes = FALLBACK_XML.read_bytes()
    real_get = hf_mod.requests.get

    def stub_get(url, **kwargs):
        if url == PRIMARY_URL:
            raise hf_mod.requests.Timeout("mocked primary timeout")
        if url == FALLBACK_URL:
            return _FakeResponse(fallback_bytes)
        raise ValueError(f"unexpected url in test: {url}")

    hf_mod.requests.get = stub_get
    try:
        items = _make().fetch()
    finally:
        hf_mod.requests.get = real_get

    assert len(items) > 0, "expected fallback entries when primary times out"
    assert any(p.hf_upvotes is not None for p in items), (
        "fallback should surface upvotes for at least one paper"
    )
    print(f"  ok - primary timeout -> fallback used, {len(items)} papers returned")


def test_both_endpoints_fail_returns_empty() -> None:
    """Both primary and fallback raise; fetch() returns [] without raising."""
    real_get = hf_mod.requests.get

    def stub_get(url, **kwargs):
        raise hf_mod.requests.ConnectionError("mocked total network outage")

    hf_mod.requests.get = stub_get
    try:
        items = _make().fetch()
    finally:
        hf_mod.requests.get = real_get

    assert items == [], "both endpoints failing must return []"
    print("  ok - both endpoints failed -> returned [] gracefully")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

TESTS = [
    test_primary_parses_correctly,
    test_fallback_parses_correctly_with_upvotes,
    test_arxiv_id_regex_matches_all,
    test_primary_fail_uses_fallback,
    test_both_endpoints_fail_returns_empty,
]


def main() -> int:
    if not PRIMARY_XML.exists() or not FALLBACK_XML.exists():
        print(f"FAIL: fixture(s) missing - {PRIMARY_XML} / {FALLBACK_XML}", file=sys.stderr)
        return 1

    failed = 0
    for fn in TESTS:
        print(f"{fn.__name__} ...")
        try:
            fn()
        except AssertionError as e:
            failed += 1
            print(f"  FAIL: {e}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"  ERROR: {type(e).__name__}: {e}")

    print()
    if failed:
        print(f"{failed} test(s) failed.")
        return 1
    print(f"All {len(TESTS)} tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
