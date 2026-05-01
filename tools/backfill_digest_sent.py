"""
tools/backfill_digest_sent.py - one-shot, run ONCE after deploying the
digest_sent_at column.

Stamps `digest_sent_at = NOW()` on every row of articles / papers /
youtube_videos that already has a non-empty summary. The reasoning:
those rows were ingested + summarised before the column existed, so they
are already in past digest emails. Without this backfill, tomorrow's
first cron run would treat them as "never sent" and re-include them.

Idempotent: re-running just refreshes the timestamp on already-marked
rows; rows summarised AFTER this script runs stay NULL until a real
digest send marks them.

Run:
    python tools/backfill_digest_sent.py

Or:
    python tools/backfill_digest_sent.py --dry-run    # report only, no UPDATE
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import func, or_, select, update

from app.database.db import get_db
from app.database.models import Article, Paper, YoutubeVideo


def _eligible_filter(model):
    """Rows we should treat as 'already sent in some past digest'.

    A row is eligible if:
      - It has a non-empty summary (it was summarised, so it could have
        legitimately appeared in a digest), AND
      - Its digest_sent_at is currently NULL (we don't refresh already-
        marked rows; that would lose information).

    For YoutubeVideo.summary the column is NOT NULL DEFAULT '', so we
    only need a `!= ''` check. For Article/Paper.summary the column is
    nullable, so we exclude both NULL and ''.
    """
    summary = model.summary
    if model is YoutubeVideo:
        summary_filter = summary != ""
    else:
        summary_filter = summary.isnot(None) & (summary != "")
    return summary_filter & model.digest_sent_at.is_(None)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Count eligible rows but make no changes.")
    args = parser.parse_args()

    print(f"=== backfill digest_sent_at ({'DRY RUN' if args.dry_run else 'APPLY'}) ===\n")

    total_marked = 0
    with get_db() as db:
        for model in (Article, Paper, YoutubeVideo):
            tname = model.__tablename__

            # Count first.
            count_stmt = (
                select(func.count())
                .select_from(model)
                .where(_eligible_filter(model))
            )
            n_eligible = db.execute(count_stmt).scalar() or 0
            print(f"  {tname:<20} {n_eligible:>4} eligible row(s)")

            if args.dry_run or n_eligible == 0:
                continue

            stmt = (
                update(model)
                .where(_eligible_filter(model))
                .values(digest_sent_at=func.now())
            )
            result = db.execute(stmt)
            updated = result.rowcount or 0
            print(f"  {tname:<20} {updated:>4} marked sent")
            total_marked += updated

    print()
    if args.dry_run:
        print("Dry run complete - no changes made.")
    else:
        print(f"Done. {total_marked} row(s) backfilled.")
        print("Tomorrow's digest will only contain rows summarised AFTER this run.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
