"""
app/database/models.py — SQLAlchemy ORM models.

Tables mirror the exact fields produced by the scrapers:
  • AnthropicArticle  ← scrapers/anthropic_scrapper.py  (AnthropicArticle.model_dump)
  • YoutubeVideo      ← scrapers/youtube_scraper.py      (VideoMetadata.model_dump)
                        + channel / transcript fields added by runner.py
"""

from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    String,
    Text,
    func,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


def _now() -> datetime:
    return datetime.now(timezone.utc)


class AnthropicArticle(Base):
    """
    Stores every Anthropic blog / research / engineering article.

    Unique key: url  (one row per canonical article URL)
    """

    __tablename__ = "anthropic_articles"

    id           = Column(BigInteger, primary_key=True, autoincrement=True)

    # --- scraper fields ---
    title        = Column(String(512),  nullable=False)
    description  = Column(Text,         nullable=False, default="")
    url          = Column(String(2048), nullable=False, unique=True)   # conflict key
    guid         = Column(String(2048), nullable=True)
    published_at = Column(DateTime(timezone=True), nullable=False)
    category     = Column(String(256),  nullable=True)
    content      = Column(Text,         nullable=False, default="")    # full article markdown

    # --- housekeeping ---
    created_at   = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    def __repr__(self) -> str:
        return f"<AnthropicArticle id={self.id} title={self.title!r}>"


class YoutubeVideo(Base):
    """
    Stores YouTube video metadata + transcript from every configured channel.

    Unique key: video_id  (YouTube's 11-char video identifier)
    """

    __tablename__ = "youtube_videos"

    id             = Column(BigInteger, primary_key=True, autoincrement=True)

    # --- scraper fields (VideoMetadata.model_dump) ---
    title          = Column(String(512),  nullable=False)
    video_id       = Column(String(32),   nullable=False, unique=True)  # conflict key
    url            = Column(String(2048), nullable=False)
    published_at   = Column(DateTime(timezone=True), nullable=False)
    description    = Column(Text,         nullable=False, default="")

    # --- fields added by runner.py ---
    channel_handle = Column(String(256),  nullable=False, default="")   # e.g. "@Fireship"
    transcript     = Column(Text,         nullable=False, default="")

    # --- housekeeping ---
    created_at     = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    def __repr__(self) -> str:
        return f"<YoutubeVideo id={self.id} video_id={self.video_id!r} title={self.title!r}>"
