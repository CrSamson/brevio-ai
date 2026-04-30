"""
scrapers/schemas.py - Pydantic v2 schemas for scraper output.

These are the validated payloads scrapers produce; the CRUD layer maps
them to Article / Paper rows.

Two intentional design notes:
  - `summary` is always None at scrape time. It's the LLM-output column
    in the DB; the scraper never writes it. The RSS feed's <description>
    goes into raw_metadata instead.
  - `frozen=True` mirrors the existing VideoMetadata convention in this repo.
"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class BlogArticle(BaseModel):
    """One blog/news entry from any RSS source."""

    source         : str                  # source.id from config/sources.json
    url            : str
    title          : str
    author         : str | None = None
    published_at   : datetime | None = None
    summary        : str | None = None    # LLM output, set later
    content_md     : str | None = None    # Docling output
    content_fetched: bool = False
    raw_metadata   : dict = {}

    model_config = {"frozen": True}


class Paper(BaseModel):
    """One paper from arXiv or HuggingFace Daily Papers (used in Phase 4-5)."""

    sources         : list[str]                    # e.g. ["arxiv"], ["arxiv","hf_daily"]
    arxiv_id        : str | None = None
    url             : str
    pdf_url         : str | None = None
    title           : str
    authors         : list[str] = []
    abstract        : str | None = None
    categories      : list[str] = []
    published_at    : datetime | None = None
    updated_at_arxiv: datetime | None = None
    hf_upvotes      : int | None = None
    raw_metadata    : dict = {}

    model_config = {"frozen": True}
