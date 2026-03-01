"""
app/database/crud.py — Single CRUD interface for all database operations.

Uses PostgreSQL ON CONFLICT … DO UPDATE (upsert) so scrapers can safely
re-insert the same article or video without duplicates.
"""

from datetime import datetime, timezone
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
