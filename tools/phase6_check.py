"""
tools/phase6_check.py - end-to-end backtest (multi-source plan, Phase 6).

Drives Runner programmatically with hours=72, fetch_transcripts=False
(YouTube transcripts off so the run finishes fast). Captures the report
and asserts:

  - Every enabled source returns a number (no unhandled exceptions).
  - >= 70% of enabled blog/paper sources fetched >= 1 item.
  - arxiv source fetched >= 5 (plan said >=10; capped by max_results=10
    in your config, so anything from ~5 upward is healthy).
  - hf_daily source fetched >= 5.
  - Idempotency: running twice in a row produces 0 inserts on the second run
    (papers + blogs).

For the manual sub-tests (plan steps 5 + 6):
  - Content-fetch test: edit one entry in config/sources.json to
    `"fetch_content": true`, then run `python main.py`. Verify the
    new article rows have content_md populated.
  - Failure-isolation test: edit one entry to point at a known-bad URL,
    then run `python main.py`. Verify that source reports an error in
    its line of the report and every other source still ingests.

Run:
    python tools/phase6_check.py                   # backtest with current DB state
    python tools/phase6_check.py --truncate        # WIPE articles + papers first
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text

from app.database.db import get_db
from runner import Runner


PHASE_6_HOURS = 72


def truncate_articles_and_papers() -> None:
    print("[truncate] DELETE FROM articles, papers ...")
    with get_db() as db:
        db.execute(text("TRUNCATE TABLE articles RESTART IDENTITY"))
        db.execute(text("TRUNCATE TABLE papers   RESTART IDENTITY"))
    with get_db() as db:
        a = db.execute(text("SELECT COUNT(*) FROM articles")).scalar()
        p = db.execute(text("SELECT COUNT(*) FROM papers")).scalar()
        print(f"[truncate] post-truncate: articles={a}, papers={p}\n")


def summarize_block(label: str, block: dict, *, expect_zero_inserts: bool) -> tuple[bool, list[str]]:
    """Return (ok, problems) for a {sources, total_fetched} block."""
    problems: list[str] = []
    sources = block["sources"]

    if not sources:
        problems.append(f"{label}: no sources")
        return False, problems

    fetched_each = {sid: s["fetched"] for sid, s in sources.items()}
    inserted_each = {sid: s["inserted"] for sid, s in sources.items()}
    errors_each   = {sid: s["error"]    for sid, s in sources.items() if s.get("error")}

    print(f"  {label}:")
    for sid, s in sources.items():
        tag = "ERR" if s["error"] else "ok "
        print(f"    [{tag}] {sid:24s}  fetched={s['fetched']:3d}  "
              f"inserted={s['inserted']:3d}  updated={s['updated']:3d}")

    fetched_count = sum(1 for n in fetched_each.values() if n > 0)
    pct = fetched_count / len(fetched_each)
    print(f"    -> {fetched_count}/{len(fetched_each)} sources with fetched>=1 ({pct*100:.0f}%)")

    if expect_zero_inserts:
        nonzero = {sid: n for sid, n in inserted_each.items() if n > 0}
        if nonzero:
            problems.append(f"{label} idempotency violation - non-zero inserts: {nonzero}")
    else:
        if pct < 0.70:
            problems.append(f"{label}: only {pct*100:.0f}% of sources got data, "
                            f"plan minimum is 70%")

    if errors_each:
        # Errors aren't blockers (per-source resilience), but worth flagging.
        for sid, msg in errors_each.items():
            print(f"    !  {sid}: {msg}")

    return not problems, problems


def assert_source_minimum(block: dict, source_id: str, minimum: int,
                          *, run_label: str) -> tuple[bool, str | None]:
    src = block["sources"].get(source_id)
    if src is None:
        return False, f"{source_id}: not in {run_label} (disabled or missing)"
    if src["fetched"] < minimum:
        return False, (f"{source_id}: only {src['fetched']} fetched in {run_label}, "
                       f"plan minimum is {minimum}")
    return True, None


def run_phase_6(*, truncate: bool) -> int:
    if truncate:
        truncate_articles_and_papers()

    runner = Runner(hours=PHASE_6_HOURS, fetch_transcripts=False)

    print("=" * 60)
    print(f"  RUN 1 - hours={PHASE_6_HOURS}, fetch_transcripts=False")
    print("=" * 60)
    report_1 = runner.run()

    print("=" * 60)
    print(f"  RUN 2 - same window, idempotency")
    print("=" * 60)
    report_2 = runner.run()

    # -----------------------------------------------------------------
    # Acceptance
    # -----------------------------------------------------------------
    print("=" * 60)
    print("  ACCEPTANCE")
    print("=" * 60)

    problems: list[str] = []

    print("\nRUN 1 - per-source 70% gate")
    ok, p = summarize_block("blogs",  report_1["blogs"],  expect_zero_inserts=False)
    problems += p
    print()
    ok, p = summarize_block("papers", report_1["papers"], expect_zero_inserts=False)
    problems += p

    print("\nRUN 1 - per-source minimums")
    for source_id, minimum in [
        ("openai_news",      1),
        ("arxiv_cs_lg_ai",   5),
        ("hf_daily_papers",  5),
    ]:
        block = report_1["papers"] if source_id in report_1["papers"]["sources"] else report_1["blogs"]
        ok, msg = assert_source_minimum(block, source_id, minimum, run_label="RUN 1")
        print(f"    {'OK' if ok else 'FAIL'}: {source_id} >= {minimum}")
        if not ok and msg:
            problems.append(msg)

    print("\nRUN 2 - idempotency (expect 0 inserts everywhere)")
    ok, p = summarize_block("blogs",  report_2["blogs"],  expect_zero_inserts=True)
    problems += p
    print()
    ok, p = summarize_block("papers", report_2["papers"], expect_zero_inserts=True)
    problems += p

    # YouTube regression note (not asserted - depends on user's channel activity).
    yt_videos = report_1["youtube"]["videos"]
    print(f"\nYouTube: {len(yt_videos)} video(s) returned in run 1 "
          f"(regression check: should still work like before; not auto-asserted).")

    print("\n" + "=" * 60)
    if problems:
        print("RESULT: PROBLEMS FOUND")
        for problem in problems:
            print(f"  - {problem}")
        return 1
    print("RESULT: OK - Phase 6 acceptance met for the automated checks.")
    print("        Remaining manual sub-tests (plan steps 5 + 6):")
    print("          - flip a blog source's fetch_content=true, re-run main.py,")
    print("            verify content_md is populated for new rows")
    print("          - swap one feed_url for a bogus URL, re-run main.py,")
    print("            verify only that source errors and others still ingest")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 6 end-to-end backtest.")
    parser.add_argument(
        "--truncate", action="store_true",
        help="WIPE the articles and papers tables before running. "
             "Use to get clean before/after counts. "
             "Does NOT touch youtube_videos.",
    )
    args = parser.parse_args()
    return run_phase_6(truncate=args.truncate)


if __name__ == "__main__":
    sys.exit(main())
