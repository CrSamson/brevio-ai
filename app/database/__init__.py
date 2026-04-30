"""
app/database/__init__.py — Makes app/database a package.
Re-exports the most commonly used symbols for convenience.
"""

from .db import engine, SessionLocal, get_db
from .models import Base, Article, Paper, YoutubeVideo
from .crud import (
    upsert_articles,
    get_all_articles,
    get_unsummarized_articles,
    set_article_summary,
    get_recent_summarized_articles,
    upsert_papers,
    merge_hf_daily_papers,
    get_all_papers,
    get_unsummarized_papers,
    set_paper_summary,
    get_recent_summarized_papers,
    upsert_youtube_video,
    upsert_youtube_videos,
    get_all_youtube_videos,
    get_unsummarized_youtube_videos,
    set_youtube_summary,
    get_recent_summarized_youtube_videos,
)

__all__ = [
    "engine",
    "SessionLocal",
    "get_db",
    "Base",
    "Article",
    "Paper",
    "YoutubeVideo",
    "upsert_articles",
    "get_all_articles",
    "get_unsummarized_articles",
    "set_article_summary",
    "get_recent_summarized_articles",
    "upsert_papers",
    "merge_hf_daily_papers",
    "get_all_papers",
    "get_unsummarized_papers",
    "set_paper_summary",
    "get_recent_summarized_papers",
    "upsert_youtube_video",
    "upsert_youtube_videos",
    "get_all_youtube_videos",
    "get_unsummarized_youtube_videos",
    "set_youtube_summary",
    "get_recent_summarized_youtube_videos",
]
