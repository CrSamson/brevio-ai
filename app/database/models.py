"""
app/database/models.py — SQLAlchemy ORM models.

Tables:
  • Article       — any blog/news post from any source. Conflict key: url.
  • Paper         — arXiv / HF Daily Papers entries. Conflict key: arxiv_id
                    (partial unique index, with url unique as fallback).
  • YoutubeVideo  — YouTube video metadata + transcript.
                    Conflict key: video_id.
"""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()


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
    summary        = Column(Text,         nullable=False, default="")   # LLM-generated per-row summary

    # NULL = not yet included in a sent digest. Set to NOW() once an email
    # containing this row goes out successfully. Filtered by the digest queries
    # so the same row never ships in two emails.
    digest_sent_at = Column(DateTime(timezone=True), nullable=True)

    # Topic tags inherited from channels.json config (e.g. ["ai", "technology"]).
    # Populated at insert time; refreshed on conflict so config edits propagate.
    topics         = Column(ARRAY(String), nullable=False,
                            server_default=text("ARRAY[]::varchar[]"))

    # --- housekeeping ---
    created_at     = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    def __repr__(self) -> str:
        return f"<YoutubeVideo id={self.id} video_id={self.video_id!r} title={self.title!r}>"


class Article(Base):
    """
    Any blog / news post from any source.

    Conflict key: `url` (unique). One row per canonical URL.
    `source` identifies which entry of config/sources.json produced the row,
    e.g. 'anthropic_news', 'openai_news', 'aws_ml'.
    """

    __tablename__ = "articles"

    id              = Column(BigInteger, primary_key=True, autoincrement=True)

    source          = Column(String(64),  nullable=False, index=True)
    url             = Column(Text,        nullable=False, unique=True)   # conflict key
    title           = Column(Text,        nullable=False)
    author          = Column(Text,        nullable=True)
    published_at    = Column(DateTime(timezone=True), nullable=True, index=True)

    summary         = Column(Text,        nullable=True)                 # LLM-generated, set later
    content_md      = Column(Text,        nullable=True)                 # Docling output
    content_fetched = Column(Boolean,     nullable=False,
                             default=False, server_default=text("false"))

    # NULL = not yet included in a sent digest. See YoutubeVideo.digest_sent_at.
    digest_sent_at  = Column(DateTime(timezone=True), nullable=True)

    # Topic tags inherited from sources.json config (e.g. ["ai", "technology"]).
    # Populated at insert time; refreshed on conflict so config edits propagate.
    topics          = Column(ARRAY(String), nullable=False,
                             server_default=text("ARRAY[]::varchar[]"))

    # Original feedparser entry, kept verbatim so we don't lose anything.
    raw_metadata    = Column(JSONB,       nullable=False,
                             default=dict, server_default=text("'{}'::jsonb"))

    created_at      = Column(DateTime(timezone=True),
                             server_default=func.now(), nullable=False)
    updated_at      = Column(DateTime(timezone=True),
                             server_default=func.now(), onupdate=func.now(),
                             nullable=False)

    def __repr__(self) -> str:
        return f"<Article id={self.id} source={self.source!r} title={self.title!r}>"


class Paper(Base):
    """
    arXiv / HuggingFace Daily Papers entries.

    Conflict key: `arxiv_id` (unique partial — only enforced when not NULL).
    `url` is also unique as a fallback for non-arXiv entries.
    `sources` is an array so a single row can be tagged with multiple
    discoveries, e.g. {'arxiv'}, {'arxiv','hf_daily'}.
    """

    __tablename__ = "papers"

    id                = Column(BigInteger, primary_key=True, autoincrement=True)

    sources           = Column(ARRAY(String), nullable=False,
                               server_default=text("ARRAY[]::varchar[]"))
    arxiv_id          = Column(String(32),  nullable=True)               # e.g. "2401.12345"
    url               = Column(Text,        nullable=False, unique=True)
    pdf_url           = Column(Text,        nullable=True)

    title             = Column(Text,        nullable=False)
    authors           = Column(JSONB,       nullable=False,
                               default=list, server_default=text("'[]'::jsonb"))
    abstract          = Column(Text,        nullable=True)
    categories        = Column(JSONB,       nullable=False,
                               default=list, server_default=text("'[]'::jsonb"))

    published_at      = Column(DateTime(timezone=True), nullable=True, index=True)
    updated_at_arxiv  = Column(DateTime(timezone=True), nullable=True)
    hf_upvotes        = Column(Integer,     nullable=True)

    summary           = Column(Text,        nullable=True)              # LLM-generated, set later

    # NULL = not yet included in a sent digest. See YoutubeVideo.digest_sent_at.
    digest_sent_at    = Column(DateTime(timezone=True), nullable=True)

    # Topic tags inherited from sources.json config. For papers, the union of
    # topics across all source configs that discovered the row (mirrors the
    # `sources` array merge logic).
    topics            = Column(ARRAY(String), nullable=False,
                               server_default=text("ARRAY[]::varchar[]"))

    raw_metadata      = Column(JSONB,       nullable=False,
                               default=dict, server_default=text("'{}'::jsonb"))

    created_at        = Column(DateTime(timezone=True),
                               server_default=func.now(), nullable=False)
    updated_at        = Column(DateTime(timezone=True),
                               server_default=func.now(), onupdate=func.now(),
                               nullable=False)

    __table_args__ = (
        # Unique only where arxiv_id is set — non-arXiv rows can coexist with NULLs.
        Index(
            "ix_papers_arxiv_id_unique",
            "arxiv_id",
            unique=True,
            postgresql_where=text("arxiv_id IS NOT NULL"),
        ),
    )

    def __repr__(self) -> str:
        return f"<Paper id={self.id} arxiv_id={self.arxiv_id!r} title={self.title!r}>"
