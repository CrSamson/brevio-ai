"""
tools/phase5_check.py - Phase 5 acceptance check (live + DB cross-link).

  1. Fetches HF Daily papers via the live primary endpoint.
  2. Calls merge_hf_daily_papers - inserts new rows, merges sources for
     any rows already in `papers` from Phase 4 (arxiv).
  3. Reports pre/post counts of cross-linked rows: where 'arxiv' AND
     'hf_daily' both appear in sources.
  4. Prints up to 5 cross-linked rows so you can eyeball the merge.
  5. Notes whether hf_upvotes was populated (depends on which feed was used).

Run:
    python tools/phase5_check.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select, text

from app.database.crud import merge_hf_daily_papers
from app.database.db import get_db
from app.database.models import Paper
from scrapers.hf_daily_scraper import HfDailyScraper


SOURCE_CONFIG = {
    "id":                "hf_daily_papers",
    "type":              "hf_daily",
    "feed_url":          "https://papers.takara.ai/api/feed",
    "fallback_feed_url": "https://raw.githubusercontent.com/huangboming/huggingface-daily-paper-feed/main/feed.xml",
}


def cross_linked_count(db) -> int:
    return db.execute(text(
        "SELECT COUNT(*) FROM papers "
        "WHERE 'arxiv' = ANY(sources) AND 'hf_daily' = ANY(sources)"
    )).scalar()


def hf_only_count(db) -> int:
    return db.execute(text(
        "SELECT COUNT(*) FROM papers "
        "WHERE 'hf_daily' = ANY(sources) AND NOT ('arxiv' = ANY(sources))"
    )).scalar()


def arxiv_only_count(db) -> int:
    return db.execute(text(
        "SELECT COUNT(*) FROM papers "
        "WHERE 'arxiv' = ANY(sources) AND NOT ('hf_daily' = ANY(sources))"
    )).scalar()


def with_upvotes_count(db) -> int:
    return db.execute(text(
        "SELECT COUNT(*) FROM papers WHERE hf_upvotes IS NOT NULL"
    )).scalar()


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass

    print(f"source: {SOURCE_CONFIG['id']}")
    print(f"primary:  {SOURCE_CONFIG['feed_url']}")
    print(f"fallback: {SOURCE_CONFIG['fallback_feed_url']}\n")

    # Pre-state.
    with get_db() as db:
        before_total       = db.execute(text("SELECT COUNT(*) FROM papers")).scalar()
        before_cross       = cross_linked_count(db)
        before_hf_only     = hf_only_count(db)
        before_arxiv_only  = arxiv_only_count(db)
        before_with_upv    = with_upvotes_count(db)

    print("--- before ---")
    print(f"  total papers:      {before_total}")
    print(f"  cross-linked:      {before_cross}")
    print(f"  hf_daily only:     {before_hf_only}")
    print(f"  arxiv only:        {before_arxiv_only}")
    print(f"  hf_upvotes set:    {before_with_upv}\n")

    # Live fetch + merge.
    scraper = HfDailyScraper(SOURCE_CONFIG)
    items   = scraper.fetch()
    print(f"fetched {len(items)} items from HF Daily")
    if items:
        print(f"  sample arxiv_ids: {[p.arxiv_id for p in items[:5]]}")
        with_upv_in_fetch = [p for p in items if p.hf_upvotes is not None]
        print(f"  items with hf_upvotes: {len(with_upv_in_fetch)}/{len(items)}\n")

    with get_db() as db:
        report = merge_hf_daily_papers(db, items)
    print(f"merge report: {report}\n")

    # Post-state.
    with get_db() as db:
        after_total       = db.execute(text("SELECT COUNT(*) FROM papers")).scalar()
        after_cross       = cross_linked_count(db)
        after_hf_only     = hf_only_count(db)
        after_arxiv_only  = arxiv_only_count(db)
        after_with_upv    = with_upvotes_count(db)

    print("--- after ---")
    print(f"  total papers:      {after_total}  ({after_total - before_total:+d})")
    print(f"  cross-linked:      {after_cross}  ({after_cross - before_cross:+d})")
    print(f"  hf_daily only:     {after_hf_only}  ({after_hf_only - before_hf_only:+d})")
    print(f"  arxiv only:        {after_arxiv_only}  ({after_arxiv_only - before_arxiv_only:+d})")
    print(f"  hf_upvotes set:    {after_with_upv}  ({after_with_upv - before_with_upv:+d})\n")

    # Sample cross-linked rows.
    print("--- 5 sample cross-linked rows (arxiv + hf_daily) ---")
    with get_db() as db:
        rows = db.execute(
            select(Paper)
            .where(text("'arxiv' = ANY(sources) AND 'hf_daily' = ANY(sources)"))
            .limit(5)
        ).scalars().all()
        if not rows:
            print("  (no cross-linked rows yet - HF Daily picks may not overlap with the\n"
                  "   10 arxiv rows from Phase 4 today. Re-run on a day with overlap, or\n"
                  "   re-run main.py / tools/phase4_check.py to pick up more arxiv papers.)")
        else:
            for p in rows:
                print(f"  arxiv_id={p.arxiv_id}  sources={list(p.sources)}  "
                      f"hf_upvotes={p.hf_upvotes}")
                print(f"    title: {p.title[:70]!r}")

    # Acceptance.
    print("\n--- acceptance ---")
    fetched_ok      = len(items) > 0
    every_has_id    = all(p.arxiv_id for p in items)
    no_exceptions   = True   # by construction (fetch never raises)

    print(f"  fetched > 0:                          {'OK' if fetched_ok else 'FAIL'}  ({len(items)})")
    print(f"  every entry has arxiv_id:             {'OK' if every_has_id else 'FAIL'}")
    print(f"  fetch() never raised:                 OK by construction")
    print(f"  cross-link rows arxiv+hf_daily:       {after_cross}  "
          f"(0 is acceptable if no overlap with today's arxiv batch)")
    print(f"  hf_upvotes populated by primary feed: "
          f"{'NO (takara.ai does not surface upvotes; expected)' if (after_with_upv == before_with_upv) else 'YES'}")

    overall = fetched_ok and every_has_id
    print(f"\noverall: {'OK' if overall else 'FAIL'}")
    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())
