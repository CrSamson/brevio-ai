"""
app/database/crud.py — Single CRUD interface for all database operations.

Uses PostgreSQL ON CONFLICT … DO UPDATE (upsert) so scrapers can safely
re-insert the same article or video without duplicates.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.database.models import AnthropicArticle, YoutubeVideo


# ===================================================================
# Anthropic Articles
# ===================================================================

def upsert_anthropic_article(db: Session, data: dict) -> AnthropicArticle:
    """
    Insert or update a single Anthropic article (conflict key: url).

    `data` is expected to come from AnthropicArticle.model_dump(mode="json").
    """
    values = {
        "title":        data["title"],
        "description":  data.get("description", ""),
        "url":          str(data["url"]),
        "guid":         data.get("guid"),
        "published_at": data["published_at"],
        "category":     data.get("category"),
        "content":      data.get("content", ""),
    }

    stmt = (
        pg_insert(AnthropicArticle)
        .values(**values)
        .on_conflict_do_update(
            index_elements=["url"],
            set_={
                "title":       values["title"],
                "description": values["description"],
                "guid":        values["guid"],
                "category":    values["category"],
                "content":     values["content"],
            },
        )
        .returning(AnthropicArticle)
    )

    row = db.execute(stmt).scalars().first()
    return row


def upsert_anthropic_articles(db: Session, articles: list[dict]) -> list[AnthropicArticle]:
    """Upsert a batch of Anthropic articles. Returns the list of ORM objects."""
    rows = []
    for article in articles:
        rows.append(upsert_anthropic_article(db, article))
    db.flush()
    return rows


def get_all_anthropic_articles(db: Session) -> list[AnthropicArticle]:
    """Return all Anthropic articles, newest first."""
    stmt = select(AnthropicArticle).order_by(AnthropicArticle.published_at.desc())
    return list(db.execute(stmt).scalars().all())


def get_unsummarized_anthropic_articles(db: Session, limit: Optional[int] = None) -> list[AnthropicArticle]:
    """Return Anthropic articles whose `summary` is empty, newest first."""
    stmt = (
        select(AnthropicArticle)
        .where(AnthropicArticle.summary == "")
        .order_by(AnthropicArticle.published_at.desc())
    )
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(db.execute(stmt).scalars().all())


def set_anthropic_summary(db: Session, article_id: int, summary: str) -> None:
    """Persist a generated summary for one Anthropic article."""
    article = db.get(AnthropicArticle, article_id)
    if article is None:
        raise ValueError(f"AnthropicArticle id={article_id} not found")
    article.summary = summary


def _digest_cutoff(hours: int) -> datetime:
    """
    Cutoff used by the digest queries.

    Rounded down to midnight UTC so a "today" article is never excluded by
    a few wall-clock hours. Mirrors AnthropicScraper.fetch_articles().
    """
    return (
        datetime.now(timezone.utc) - timedelta(hours=hours)
    ).replace(hour=0, minute=0, second=0, microsecond=0)


def get_recent_summarized_anthropic_articles(db: Session, hours: int) -> list[AnthropicArticle]:
    """Return Anthropic articles published in the last `hours` hours that have a summary."""
    cutoff = _digest_cutoff(hours)
    stmt = (
        select(AnthropicArticle)
        .where(AnthropicArticle.summary != "")
        .where(AnthropicArticle.published_at >= cutoff)
        .order_by(AnthropicArticle.published_at.desc())
    )
    return list(db.execute(stmt).scalars().all())


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
    """Return YouTube videos published in the last `hours` hours that have a summary."""
    cutoff = _digest_cutoff(hours)
    stmt = (
        select(YoutubeVideo)
        .where(YoutubeVideo.summary != "")
        .where(YoutubeVideo.published_at >= cutoff)
        .order_by(YoutubeVideo.published_at.desc())
    )
    return list(db.execute(stmt).scalars().all())
