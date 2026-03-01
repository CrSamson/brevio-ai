"""
app/database/__init__.py — Makes app/database a package.
Re-exports the most commonly used symbols for convenience.
"""

from .db import engine, SessionLocal, get_db
from .models import Base, AnthropicArticle, YoutubeVideo
from .crud import (
    upsert_anthropic_article,
    upsert_anthropic_articles,
    get_all_anthropic_articles,
    upsert_youtube_video,
    upsert_youtube_videos,
    get_all_youtube_videos,
)

__all__ = [
    "engine",
    "SessionLocal",
    "get_db",
    "Base",
    "AnthropicArticle",
    "YoutubeVideo",
    "upsert_anthropic_article",
    "upsert_anthropic_articles",
    "get_all_anthropic_articles",
    "upsert_youtube_video",
    "upsert_youtube_videos",
    "get_all_youtube_videos",
]
