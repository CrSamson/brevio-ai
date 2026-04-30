"""
tools/phase4_check.py - Phase 4 acceptance check (live + DB).

  1. Fetches papers via the live arXiv API (max_results=10).
  2. Asserts the volume gate didn't fire and >=1 paper came back.
  3. Upserts twice. First run: all inserts. Second run: all updates.
  4. Prints 5 sample Paper rows so you can eyeball them.

Run:
    python tools/phase4_check.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.database.crud import upsert_papers
from app.database.db import get_db
from scrapers.arxiv_scraper import ArxivScraper


SOURCE_CONFIG = {
    "id":             "arxiv_cs_lg_ai",
    "type":           "arxiv",
    "categories":     ["cs.LG", "cs.AI"],
    "max_results":    10,
    "keyword_filter": None,
}


def run_once(scraper: ArxivScraper) -> tuple[list, dict]:
    items = scraper.fetch()
    with get_db() as db:
        report = upsert_papers(db, items)
    report["fetched"] = len(items)
    return items, report


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass

    print(f"source: {SOURCE_CONFIG['id']}")
    print(f"categories: {SOURCE_CONFIG['categories']}, max_results={SOURCE_CONFIG['max_results']}\n")

    scraper = ArxivScraper(SOURCE_CONFIG)

    print("--- run 1 ---")
    items_1, report_1 = run_once(scraper)
    print(f"  {report_1}")

    print("\n--- run 2 ---")
    _, report_2 = run_once(scraper)
    print(f"  {report_2}")

    print("\n--- 5 sample Paper rows from run 1 ---")
    for p in items_1[:5]:
        d = p.model_dump(mode="json")
        # Trim huge fields for readability.
        if d.get("abstract"):
            d["abstract"] = d["abstract"][:160] + ("..." if len(d["abstract"]) > 160 else "")
        if d.get("raw_metadata"):
            d["raw_metadata"] = {k: v for k, v in d["raw_metadata"].items()}
        print(json.dumps(d, indent=2, default=str))
        print()

    # Acceptance.
    fetched = report_1["fetched"]
    print("--- acceptance ---")
    ok_volume     = fetched > 0
    ok_first_run  = report_1["inserted"] == fetched and report_1["updated"] == 0
    ok_second_run = report_2["inserted"] == 0       and report_2["updated"] == fetched

    print(f"  fetched > 0:                          {'OK' if ok_volume else 'FAIL'}  (got {fetched})")
    print(f"  run 1 = all inserts:                  {'OK' if ok_first_run else 'FAIL'}  ({report_1})")
    print(f"  run 2 = 0 inserts, all updates:       {'OK' if ok_second_run else 'FAIL'}  ({report_2})")
    print(f"  no PDFs downloaded (only pdf_url stored): OK by construction (no requests.get on pdf_url anywhere)")

    overall = ok_volume and ok_first_run and ok_second_run
    print(f"\noverall: {'OK' if overall else 'FAIL'}")
    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())
