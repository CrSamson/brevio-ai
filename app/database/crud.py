"""
app/database/crud.py — Single CRUD interface for all database operations.

Uses PostgreSQL ON CONFLICT … DO UPDATE (upsert) so scrapers can safely
re-insert the same article or video without duplicates.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, literal_column, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.database.models import Article, Paper, YoutubeVideo
from scrapers.schemas import BlogArticle, Paper as PaperItem


log = logging.getLogger(__name__)


def _digest_cutoff(hours: int) -> datetime:
    """
    Cutoff used by the digest queries.

    Rounded down to midnight UTC so a "today" article is never excluded
    by a few wall-clock hours - matches the cutoff rounding in
    RssBlogScraper._parse_feed_bytes.
    """
    return (
        datetime.now(timezone.utc) - timedelta(hours=hours)
    ).replace(hour=0, minute=0, second=0, microsecond=0)


# ===================================================================
# YouTube Videos
# ===================================================================

def upsert_youtube_video(db: Session, data: dict) -> YoutubeVideo:
    """
    Insert or update a single YouTube video (conflict key: video_id).

    `data` is the dict produced by runner._scrape_youtube() which contains
    scraper fields + "channel" (mapped to channel_handle) + "transcript".
    """
    values = {
        "title":          data["title"],
        "video_id":       data["video_id"],
        "url":            str(data["url"]),
        "published_at":   data["published_at"],
        "description":    data.get("description", ""),
        "channel_handle": data.get("channel", data.get("channel_handle", "")),
        "transcript":     data.get("transcript", ""),
    }

    stmt = (
        pg_insert(YoutubeVideo)
        .values(**values)
        .on_conflict_do_update(
            index_elements=["video_id"],
            set_={
                "title":          values["title"],
                "description":    values["description"],
                "channel_handle": values["channel_handle"],
                "transcript":     values["transcript"],
            },
        )
        .returning(YoutubeVideo)
    )

    row = db.execute(stmt).scalars().first()
    return row


def upsert_youtube_videos(db: Session, videos: list[dict]) -> list[YoutubeVideo]:
    """Upsert a batch of YouTube videos. Returns the list of ORM objects."""
    rows = []
    for video in videos:
        rows.append(upsert_youtube_video(db, video))
    db.flush()
    return rows


def get_all_youtube_videos(db: Session) -> list[YoutubeVideo]:
    """Return all YouTube videos, newest first."""
    stmt = select(YoutubeVideo).order_by(YoutubeVideo.published_at.desc())
    return list(db.execute(stmt).scalars().all())


def get_unsummarized_youtube_videos(db: Session, limit: Optional[int] = None) -> list[YoutubeVideo]:
    """Return YouTube videos whose `summary` is empty, newest first."""
    stmt = (
        select(YoutubeVideo)
        .where(YoutubeVideo.summary == "")
        .order_by(YoutubeVideo.published_at.desc())
    )
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(db.execute(stmt).scalars().all())


def set_youtube_summary(db: Session, video_id: int, summary: str) -> None:
    """Persist a generated summary for one YouTube video."""
    video = db.get(YoutubeVideo, video_id)
    if video is None:
        raise ValueError(f"YoutubeVideo id={video_id} not found")
    video.summary = summary


def get_recent_summarized_youtube_videos(db: Session, hours: int) -> list[YoutubeVideo]:
    """Return YouTube videos published in the last `hours` hours that have a
    non-empty summary AND have not already been included in a sent digest."""
    cutoff = _digest_cutoff(hours)
    stmt = (
        select(YoutubeVideo)
        .where(YoutubeVideo.summary != "")
        .where(YoutubeVideo.published_at >= cutoff)
        .where(YoutubeVideo.digest_sent_at.is_(None))
        .order_by(YoutubeVideo.published_at.desc())
    )
    return list(db.execute(stmt).scalars().all())


# ===================================================================
# Generalized Articles (multi-source plan, Phase 2)
# ===================================================================

def upsert_articles(db: Session, items: list[BlogArticle]) -> dict:
    """
    Upsert a batch of BlogArticle rows (conflict key: url).

    Returns {"inserted": N, "updated": N, "total": N}.

    Insert vs update is detected via Postgres' xmax trick:
        xmax = 0  -> row was just inserted
        xmax != 0 -> row already existed and ON CONFLICT fired

    `summary` is deliberately omitted from the SET clause so a re-scrape
    never overwrites an LLM-generated summary downstream.
    """
    inserted = 0
    updated  = 0

    for item in items:
        values = {
            "source"         : item.source,
            "url"            : item.url,
            "title"          : item.title,
            "author"         : item.author,
            "published_at"   : item.published_at,
            "summary"        : item.summary,           # always None at scrape time
            "content_md"     : item.content_md,
            "content_fetched": item.content_fetched,
            "raw_metadata"   : item.raw_metadata or {},
        }

        stmt = (
            pg_insert(Article)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["url"],
                set_={
                    "title"          : values["title"],
                    "author"         : values["author"],
                    "published_at"   : values["published_at"],
                    "content_md"     : values["content_md"],
                    "content_fetched": values["content_fetched"],
                    "raw_metadata"   : values["raw_metadata"],
                    # summary intentionally omitted - preserve LLM output
                    "updated_at"     : func.now(),
                },
            )
            .returning(literal_column("(xmax = 0)").label("was_inserted"))
        )
        was_inserted = db.execute(stmt).scalar()
        if was_inserted:
            inserted += 1
        else:
            updated += 1

    db.flush()
    return {"inserted": inserted, "updated": updated, "total": inserted + updated}


def get_all_articles(db: Session) -> list[Article]:
    """Return all Articles, newest first."""
    stmt = select(Article).order_by(Article.published_at.desc())
    return list(db.execute(stmt).scalars().all())


def get_unsummarized_articles(db: Session, limit: Optional[int] = None) -> list[Article]:
    """Return Articles whose summary is NULL or empty, newest first."""
    stmt = (
        select(Article)
        .where(or_(Article.summary.is_(None), Article.summary == ""))
        .order_by(Article.published_at.desc())
    )
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(db.execute(stmt).scalars().all())


def set_article_summary(db: Session, article_id: int, summary: str) -> None:
    """Persist a generated summary for one Article."""
    article = db.get(Article, article_id)
    if article is None:
        raise ValueError(f"Article id={article_id} not found")
    article.summary = summary


def get_recent_summarized_articles(db: Session, hours: int) -> list[Article]:
    """Return Articles published in the last `hours` hours that have a non-empty
    summary AND have not already been included in a sent digest."""
    cutoff = _digest_cutoff(hours)
    stmt = (
        select(Article)
        .where(Article.summary.isnot(None))
        .where(Article.summary != "")
        .where(Article.published_at >= cutoff)
        .where(Article.digest_sent_at.is_(None))
        .order_by(Article.published_at.desc())
    )
    return list(db.execute(stmt).scalars().all())


# ===================================================================
# Generalized Papers (multi-source plan, Phase 4)
# ===================================================================

# Postgres expression: dedup the union of two text arrays.
# Used in upsert_papers' ON CONFLICT SET clause to merge `sources`.
_SOURCES_UNION = text(
    "ARRAY(SELECT DISTINCT unnest(papers.sources || EXCLUDED.sources))"
)


def upsert_papers(db: Session, items: list[PaperItem]) -> dict:
    """
    Upsert a batch of Paper items (conflict key: arxiv_id).

    Returns {"inserted": N, "updated": N, "total": N, "skipped": N}.

    On conflict (matching arxiv_id), this:
      - Merges `sources` arrays via DISTINCT unnest (so a Phase-5 HF Daily
        ingestion appends 'hf_daily' to an existing arxiv-only row).
      - Keeps the existing `hf_upvotes` if the new row's value is NULL
        (so an arxiv-only re-scrape never blanks an HF count).
      - Refreshes title / authors / abstract / categories / pdf_url /
        published_at / updated_at_arxiv from the new payload.

    Items with `arxiv_id is None` are logged and skipped. The plan §4.1
    allows a URL-fallback conflict path; that's deferred until we
    actually have a non-arxiv paper source.

    Insert vs update is detected via Postgres' xmax trick:
        xmax = 0  -> row was just inserted
        xmax != 0 -> row already existed and ON CONFLICT fired
    """
    inserted = 0
    updated  = 0
    skipped  = 0

    for item in items:
        if item.arxiv_id is None:
            log.warning("[upsert_papers] item has no arxiv_id; skipping url=%s", item.url)
            skipped += 1
            continue

        values = {
            "sources"         : item.sources,
            "arxiv_id"        : item.arxiv_id,
            "url"             : item.url,
            "pdf_url"         : item.pdf_url,
            "title"           : item.title,
            "authors"         : item.authors,
            "abstract"        : item.abstract,
            "categories"      : item.categories,
            "published_at"    : item.published_at,
            "updated_at_arxiv": item.updated_at_arxiv,
            "hf_upvotes"      : item.hf_upvotes,
            "raw_metadata"    : item.raw_metadata or {},
        }

        stmt = pg_insert(Paper).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["arxiv_id"],
            index_where=text("arxiv_id IS NOT NULL"),  # match the partial index
            set_={
                "sources"         : _SOURCES_UNION,
                "title"           : stmt.excluded.title,
                "authors"         : stmt.excluded.authors,
                "abstract"        : stmt.excluded.abstract,
                "categories"      : stmt.excluded.categories,
                "pdf_url"         : stmt.excluded.pdf_url,
                "published_at"    : stmt.excluded.published_at,
                "updated_at_arxiv": stmt.excluded.updated_at_arxiv,
                # Keep existing hf_upvotes if the new row didn't provide one.
                "hf_upvotes"      : func.coalesce(
                    stmt.excluded.hf_upvotes, Paper.hf_upvotes
                ),
                "updated_at"      : func.now(),
            },
        ).returning(literal_column("(xmax = 0)").label("was_inserted"))

        was_inserted = db.execute(stmt).scalar()
        if was_inserted:
            inserted += 1
        else:
            updated += 1

    db.flush()
    return {
        "inserted": inserted,
        "updated":  updated,
        "total":    inserted + updated,
        "skipped":  skipped,
    }


def merge_hf_daily_papers(db: Session, items: list[PaperItem]) -> dict:
    """
    Merge HuggingFace Daily Papers entries into the papers table (Phase 5).

    Returns {"inserted": N, "updated": N, "total": N, "skipped": N}.

    Semantics — different from upsert_papers on purpose:

      INSERT (no existing row with this arxiv_id):
        Use HF's data as-is. arxiv-quality data may overwrite later when
        ArxivScraper covers the same paper.

      UPDATE (row already exists, e.g. ingested by ArxivScraper):
        - sources:    array union (so {'arxiv'} -> {'arxiv','hf_daily'})
        - hf_upvotes: COALESCE(new, existing)  -> set if new is non-null,
                       keep existing otherwise
        - updated_at: refresh
        title / authors / abstract / categories / pdf_url / published_at /
        updated_at_arxiv are LEFT ALONE so we don't clobber the arXiv-
        sourced versions with HF's potentially weaker text.

    Items with `arxiv_id is None` are logged and skipped.
    """
    inserted = 0
    updated  = 0
    skipped  = 0

    for item in items:
        if item.arxiv_id is None:
            log.warning(
                "[merge_hf_daily_papers] item has no arxiv_id; skipping url=%s",
                item.url,
            )
            skipped += 1
            continue

        values = {
            "sources"         : item.sources,
            "arxiv_id"        : item.arxiv_id,
            "url"             : item.url,
            "pdf_url"         : item.pdf_url,
            "title"           : item.title,
            "authors"         : item.authors,
            "abstract"        : item.abstract,
            "categories"      : item.categories,
            "published_at"    : item.published_at,
            "updated_at_arxiv": item.updated_at_arxiv,
            "hf_upvotes"      : item.hf_upvotes,
            "raw_metadata"    : item.raw_metadata or {},
        }

        stmt = pg_insert(Paper).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["arxiv_id"],
            index_where=text("arxiv_id IS NOT NULL"),
            set_={
                "sources"   : _SOURCES_UNION,
                "hf_upvotes": func.coalesce(
                    stmt.excluded.hf_upvotes, Paper.hf_upvotes
                ),
                "updated_at": func.now(),
            },
        ).returning(literal_column("(xmax = 0)").label("was_inserted"))

        was_inserted = db.execute(stmt).scalar()
        if was_inserted:
            inserted += 1
        else:
            updated += 1

    db.flush()
    return {
        "inserted": inserted,
        "updated":  updated,
        "total":    inserted + updated,
        "skipped":  skipped,
    }


def get_all_papers(db: Session) -> list[Paper]:
    """Return all Papers, newest first."""
    stmt = select(Paper).order_by(Paper.published_at.desc())
    return list(db.execute(stmt).scalars().all())


def get_unsummarized_papers(db: Session, limit: Optional[int] = None) -> list[Paper]:
    """Return Papers whose summary is NULL or empty, newest first."""
    stmt = (
        select(Paper)
        .where(or_(Paper.summary.is_(None), Paper.summary == ""))
        .order_by(Paper.published_at.desc())
    )
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(db.execute(stmt).scalars().all())


def set_paper_summary(db: Session, paper_id: int, summary: str) -> None:
    """Persist a generated summary for one Paper."""
    paper = db.get(Paper, paper_id)
    if paper is None:
        raise ValueError(f"Paper id={paper_id} not found")
    paper.summary = summary


def get_recent_summarized_papers(db: Session, hours: int) -> list[Paper]:
    """Return Papers published in the last `hours` hours that have a non-empty
    summary AND have not already been included in a sent digest."""
    cutoff = _digest_cutoff(hours)
    stmt = (
        select(Paper)
        .where(Paper.summary.isnot(None))
        .where(Paper.summary != "")
        .where(Paper.published_at >= cutoff)
        .where(Paper.digest_sent_at.is_(None))
        .order_by(Paper.published_at.desc())
    )
    return list(db.execute(stmt).scalars().all())


# ===================================================================
# Digest send-state (shared across all three content kinds)
# ===================================================================

def mark_digest_sent(db: Session, model, ids: list[int]) -> int:
    """
    Stamp `digest_sent_at = NOW()` on the rows of `model` whose `id` is in
    `ids`. Used after a digest email goes out successfully so the same row
    never ships twice.

    Returns the number of rows updated.

    Idempotent: re-applying to the same ids just refreshes the timestamp.
    `model` must have a `digest_sent_at` column - works for Article, Paper,
    and YoutubeVideo.
    """
    if not ids:
        return 0
    stmt = (
        update(model)
        .where(model.id.in_(ids))
        .values(digest_sent_at=func.now())
    )
    result = db.execute(stmt)
    db.flush()
    return result.rowcount or 0
