"""
tools/phase2_idempotency.py - one-shot DB check for Phase 2 acceptance.

Scrapes one Anthropic feed twice into the new `articles` table.
First run: all rows should be inserts. Second run: all rows should be updates.

Run:
    python tools/phase2_idempotency.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.database.crud import upsert_articles
from app.database.db import get_db
from scrapers.rss_blog_scraper import RssBlogScraper


URL    = ("https://raw.githubusercontent.com/Olshansk/rss-feeds/main/"
          "feeds/feed_anthropic_news.xml")
ID     = "phase2_test_anthropic_news"   # distinct id so we don't pollute "anthropic_news"
HOURS  = 168


def run_once() -> dict:
    items = RssBlogScraper({"id": ID, "feed_url": URL}).fetch(hours=HOURS)
    with get_db() as db:
        report = upsert_articles(db, items)
    report["fetched"] = len(items)
    return report


def main() -> int:
    print(f"feed:  {URL}")
    print(f"window: last {HOURS}h, source_id='{ID}'\n")

    first  = run_once()
    second = run_once()

    print(f"first run:   {first}")
    print(f"second run:  {second}")
    print()

    ok = (
        first["inserted"]  == first["fetched"] and first["updated"] == 0 and
        second["inserted"] == 0                and second["updated"] == first["fetched"]
    )
    print(f"idempotency: {'OK' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
