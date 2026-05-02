"""
tools/backfill_topics.py — Phase 2 one-shot.

Stamps `topics` on every existing row of articles / papers / youtube_videos
based on its `source` (or `channel_handle` for videos), reading the topic
mapping from config/sources.json + config/channels.json. Idempotent: rows
that already have the right topic array stay unchanged; re-runs report 0
updates.

Without this backfill, every existing row has `topics = '{}'` (the default
from the additive ALTER), and the digest's per-topic queries would treat
them as untagged.

Run:
    python tools/backfill_topics.py            # apply
    python tools/backfill_topics.py --dry-run  # report counts only
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import func, select, update

from app.database.db import get_db
from app.database.models import Article, Paper, YoutubeVideo


PROJECT_ROOT  = Path(__file__).resolve().parents[1]
SOURCES_FILE  = PROJECT_ROOT / "config" / "sources.json"
CHANNELS_FILE = PROJECT_ROOT / "config" / "channels.json"


def _load_source_topics() -> dict[str, list[str]]:
    """Map source_id -> topics for both blog and paper sources."""
    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    out: dict[str, list[str]] = {}
    for src in cfg.get("blogs", []):
        out[src["id"]] = list(src.get("topics", []))
    for src in cfg.get("papers", []):
        out[src["id"]] = list(src.get("topics", []))
    return out


def _load_paper_type_topics() -> dict[str, list[str]]:
    """Map paper-source `type` -> unioned topics across all configured sources
    of that type.

    Papers store their discovery in `Paper.sources` as type strings (`"arxiv"`,
    `"hf_daily"`) - not source IDs - so the backfill needs this type-keyed
    mapping rather than the id-keyed one.
    """
    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    out: dict[str, list[str]] = {}
    for src in cfg.get("papers", []):
        ptype = src.get("type")
        if not ptype:
            continue
        for t in src.get("topics", []):
            if t not in out.setdefault(ptype, []):
                out[ptype].append(t)
    return out


def _load_channel_topics() -> dict[str, list[str]]:
    """Map channel_handle -> topics."""
    with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return {c["handle"]: list(c.get("topics", [])) for c in cfg.get("youtube_channels", [])}


def _backfill_articles(db, source_topics: dict[str, list[str]], dry_run: bool) -> tuple[int, int]:
    """Returns (eligible_pre, updated)."""
    eligible_pre = db.execute(
        select(func.count()).select_from(Article).where(Article.topics == [])
    ).scalar() or 0

    if dry_run or eligible_pre == 0:
        return eligible_pre, 0

    updated = 0
    for source_id, topics in source_topics.items():
        if not topics:
            continue
        result = db.execute(
            update(Article)
            .where(Article.source == source_id)
            .where(Article.topics == [])
            .values(topics=topics)
        )
        updated += result.rowcount or 0
    return eligible_pre, updated


def _backfill_papers(db, paper_type_topics: dict[str, list[str]], dry_run: bool) -> tuple[int, int]:
    """For papers, the `sources` array holds *type* strings ("arxiv",
    "hf_daily") not source IDs. Map each type to its topics and union per-row.
    """
    eligible_pre = db.execute(
        select(func.count()).select_from(Paper).where(Paper.topics == [])
    ).scalar() or 0

    if dry_run or eligible_pre == 0:
        return eligible_pre, 0

    rows = db.execute(
        select(Paper).where(Paper.topics == [])
    ).scalars().all()

    updated = 0
    for paper in rows:
        seen: set[str] = set()
        merged: list[str] = []
        for ptype in (paper.sources or []):
            for t in paper_type_topics.get(ptype, []):
                if t not in seen:
                    seen.add(t)
                    merged.append(t)
        if not merged:
            continue
        paper.topics = merged
        updated += 1
    db.flush()
    return eligible_pre, updated


def _backfill_youtube(db, channel_topics: dict[str, list[str]], dry_run: bool) -> tuple[int, int]:
    eligible_pre = db.execute(
        select(func.count()).select_from(YoutubeVideo).where(YoutubeVideo.topics == [])
    ).scalar() or 0

    if dry_run or eligible_pre == 0:
        return eligible_pre, 0

    updated = 0
    for handle, topics in channel_topics.items():
        if not topics:
            continue
        result = db.execute(
            update(YoutubeVideo)
            .where(YoutubeVideo.channel_handle == handle)
            .where(YoutubeVideo.topics == [])
            .values(topics=topics)
        )
        updated += result.rowcount or 0
    return eligible_pre, updated


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Count eligible rows but make no changes.")
    args = parser.parse_args()

    print(f"=== backfill topics ({'DRY RUN' if args.dry_run else 'APPLY'}) ===\n")

    source_topics      = _load_source_topics()
    paper_type_topics  = _load_paper_type_topics()
    channel_topics     = _load_channel_topics()
    print(f"source_topics:     {len(source_topics)} entries from sources.json (id-keyed, for blogs)")
    print(f"paper_type_topics: {paper_type_topics} (type-keyed, for papers)")
    print(f"channel_topics:    {len(channel_topics)} entries from channels.json\n")

    with get_db() as db:
        a_pre, a_upd = _backfill_articles(db, source_topics,     args.dry_run)
        p_pre, p_upd = _backfill_papers  (db, paper_type_topics, args.dry_run)
        v_pre, v_upd = _backfill_youtube (db, channel_topics,    args.dry_run)

    print(f"  {'table':<20} {'untagged_pre':>14} {'updated':>10}")
    print(f"  {'-'*20} {'-'*14} {'-'*10}")
    print(f"  {'articles':<20} {a_pre:>14} {a_upd:>10}")
    print(f"  {'papers':<20} {p_pre:>14} {p_upd:>10}")
    print(f"  {'youtube_videos':<20} {v_pre:>14} {v_upd:>10}")
    total_pre = a_pre + p_pre + v_pre
    total_upd = a_upd + p_upd + v_upd
    print(f"  {'TOTAL':<20} {total_pre:>14} {total_upd:>10}")
    print()

    if args.dry_run:
        print("Dry run - no changes made.")
    else:
        print(f"Done. {total_upd} row(s) backfilled.")

    # Post-state sanity: every row should now be tagged (assuming its source
    # has topics in config). Log how many remain untagged.
    with get_db() as db:
        a_post = db.execute(select(func.count()).select_from(Article)
                            .where(Article.topics == [])).scalar() or 0
        p_post = db.execute(select(func.count()).select_from(Paper)
                            .where(Paper.topics == [])).scalar() or 0
        v_post = db.execute(select(func.count()).select_from(YoutubeVideo)
                            .where(YoutubeVideo.topics == [])).scalar() or 0
    print(f"\npost-state untagged: articles={a_post}, papers={p_post}, youtube_videos={v_post}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
