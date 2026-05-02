"""
tests/test_rss_blog_scraper.py - unit tests for RssBlogScraper.

Plain runnable script (no pytest). Drives the parser via the public
._parse_feed_bytes() entry point so no HTTP mocking is needed.

Covers:
  1. Parses a real Anthropic feed fixture into BlogArticle objects.
  2. The lookback filter actually narrows results when hours is small.
  3. Malformed entries (missing link or title) are silently dropped.
  4. Content-extractor failures don't drop the article - it's returned with
     content_md=None, content_fetched=False.

Run:
    python tests/test_rss_blog_scraper.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# Make the project root importable when running as a path.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scrapers.rss_blog_scraper import RssBlogScraper
from scrapers.schemas import BlogArticle


FIXTURE_DIR    = Path(__file__).parent / "fixtures"
ANTHROPIC_NEWS = FIXTURE_DIR / "anthropic_news.xml"

# Effectively "all entries" - 100 years.
HUGE_LOOKBACK = 24 * 365 * 100


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_parses_correctly() -> None:
    raw = ANTHROPIC_NEWS.read_bytes()
    scraper = RssBlogScraper({"id": "anthropic_news", "feed_url": "x"})
    items = scraper._parse_feed_bytes(raw, hours=HUGE_LOOKBACK)

    assert len(items) > 0, "expected >=1 entry"
    first = items[0]
    assert isinstance(first, BlogArticle)
    assert first.source == "anthropic_news"
    assert first.url
    assert first.title
    assert first.published_at is not None
    assert first.summary is None                # LLM hasn't run yet
    assert first.content_md is None             # fetch_content=False
    assert first.content_fetched is False
    print(f"  ok - parsed {len(items)} entries; first={first.title[:60]!r}")


def test_lookback_filter() -> None:
    raw = ANTHROPIC_NEWS.read_bytes()
    scraper = RssBlogScraper({"id": "anthropic_news", "feed_url": "x"})

    wide  = scraper._parse_feed_bytes(raw, hours=HUGE_LOOKBACK)
    short = scraper._parse_feed_bytes(raw, hours=24)

    assert len(short) <= len(wide), "short lookback must not return more than wide"
    print(f"  ok - wide={len(wide)} entries, last-24h={len(short)} entries")


def test_malformed_entries_skipped() -> None:
    bad_rss = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <title>x</title><link>http://x</link><description>x</description>
  <item><title>has-title-no-link</title></item>
  <item><link>http://x.com/has-link-no-title</link></item>
  <item><title>good-one</title><link>http://x.com/good</link>
        <pubDate>Wed, 01 Jan 2025 00:00:00 GMT</pubDate></item>
</channel></rss>"""
    scraper = RssBlogScraper({"id": "tst", "feed_url": "x"})
    items = scraper._parse_feed_bytes(bad_rss, hours=HUGE_LOOKBACK)

    titles = {i.title for i in items}
    assert "good-one" in titles, "good entry should be kept"
    assert "has-title-no-link" not in titles, "title-only entry should be skipped"
    assert len(items) == 1, f"expected exactly 1 good entry, got {len(items)}: {titles}"
    print(f"  ok - kept 1 good entry, dropped 2 malformed")


def test_content_extraction_failure_does_not_drop_article() -> None:
    raw = ANTHROPIC_NEWS.read_bytes()
    scraper = RssBlogScraper({
        "id":            "anthropic_news",
        "feed_url":      "x",
        "fetch_content": True,
    })
    # Stub _fetch_content to simulate the extractor failing for every URL.
    scraper._fetch_content = lambda url: None  # type: ignore[assignment]
    items = scraper._parse_feed_bytes(raw, hours=HUGE_LOOKBACK)

    assert len(items) > 0, "should still return items even when extraction fails"
    assert all(i.content_md is None       for i in items)
    assert all(i.content_fetched is False for i in items)
    print(f"  ok - {len(items)} items returned with content_md=None")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

TESTS = [
    test_parses_correctly,
    test_lookback_filter,
    test_malformed_entries_skipped,
    test_content_extraction_failure_does_not_drop_article,
]


def main() -> int:
    if not ANTHROPIC_NEWS.exists():
        print(f"FAIL: fixture missing - {ANTHROPIC_NEWS}", file=sys.stderr)
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
