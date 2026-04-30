"""
agent/summarizer.py — Per-row LLM summarizer (OpenAI).

Reads rows whose `summary` column is empty from `anthropic_articles` and
`youtube_videos`, asks an OpenAI model to summarize each one, and writes
the result back.

Run directly to fill in summaries for everything currently unsummarized:

    python -m agent.summarizer
    python -m agent.summarizer --limit 5      # cap how many of each type
    python -m agent.summarizer --anthropic    # only Anthropic blog articles
    python -m agent.summarizer --youtube      # only YouTube videos

Note: "anthropic" in table/column names refers to the *content source*
(anthropic.com blog feeds), not the API provider.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI, OpenAIError

from app.database.crud import (
    get_all_anthropic_articles,
    get_all_youtube_videos,
    get_unsummarized_anthropic_articles,
    get_unsummarized_youtube_videos,
    set_anthropic_summary,
    set_youtube_summary,
)
from app.database.db import get_db
from app.database.models import AnthropicArticle, YoutubeVideo


# .env lives at the project root; mirror app/database/db.py
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_PROJECT_ROOT / ".env", override=True)


DEFAULT_MODEL = "gpt-4o"   
DEFAULT_MAX_TOKENS = 1024

# Hard cap on chars of source text we pass into the prompt. Anthropic articles
# in particular can run long once Docling has rendered them; trimming keeps
# cost predictable for a per-row summarizer. Tune as needed.
MAX_SOURCE_CHARS = 40_000

_SYSTEM_PROMPT = (
    "You write blurbs for an AI-industry daily digest email. The reader is a "
    "busy practitioner deciding in seconds whether to click through.\n\n"
    "Output a single paragraph of 2 to 4 sentences, plain text. No bullets, "
    "no markdown, no labels, no headings, no leading title. Just the "
    "paragraph itself.\n\n"
    "Lead with what is new and why it matters. Follow with the most concrete "
    "details that earn that lead: numbers, model names, benchmark scores, "
    "capabilities, who hired, what shipped, what changed. Prefer specifics "
    "over generalities. Skip throat-clearing ('In this article...', 'The "
    "speaker explains...', 'This post discusses...').\n\n"
    "Tone: direct and concrete. Lightly opinionated where the source "
    "supports it. Do not invent claims the source does not make.\n\n"
    "If the source is genuinely thin or routine (boilerplate hire, regional "
    "event, status note), say so honestly in one sentence. Do not pad."
)


class Summarizer:
    """Thin wrapper around the OpenAI Chat Completions API for per-row summaries."""

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        client: OpenAI | None = None,
    ) -> None:
        self.model = model
        self.max_tokens = max_tokens
        # Reads OPENAI_API_KEY from env (loaded from .env above).
        self.client = client or OpenAI()

    # ------------------------------------------------------------------
    # Source-specific entry points
    # ------------------------------------------------------------------

    def summarize_anthropic_article(self, article: AnthropicArticle) -> str:
        body = article.content or article.description or ""
        return self._summarize(
            kind="article",
            title=article.title,
            url=str(article.url),
            body=body,
        )

    def summarize_youtube_video(self, video: YoutubeVideo) -> str:
        body = video.transcript or video.description or ""
        return self._summarize(
            kind="video transcript",
            title=video.title,
            url=str(video.url),
            body=body,
        )

    # ------------------------------------------------------------------
    # Core call
    # ------------------------------------------------------------------

    def _summarize(self, *, kind: str, title: str, url: str, body: str) -> str:
        body = (body or "").strip()
        if not body:
            return f"No source text available to summarize ({kind})."

        if len(body) > MAX_SOURCE_CHARS:
            body = body[:MAX_SOURCE_CHARS] + "\n\n[... truncated for length]"

        user_prompt = (
            f"Summarize this {kind}.\n\n"
            f"Title: {title}\n"
            f"URL: {url}\n\n"
            f"--- SOURCE START ---\n{body}\n--- SOURCE END ---"
        )

        response = self.client.chat.completions.create(
            model=self.model,
            max_tokens=self.max_tokens,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",   "content": user_prompt},
            ],
        )

        return (response.choices[0].message.content or "").strip()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _run(limit: int | None, do_anthropic: bool, do_youtube: bool, force: bool) -> None:
    summarizer = Summarizer()

    with get_db() as db:
        if do_anthropic:
            articles = (
                get_all_anthropic_articles(db) if force
                else get_unsummarized_anthropic_articles(db)
            )
            if limit is not None:
                articles = articles[:limit]
            label = "re-summarize" if force else "summarize"
            print(f"[anthropic] {len(articles)} article(s) to {label}.")
            for i, article in enumerate(articles, start=1):
                print(f"  ({i}/{len(articles)}) {article.title[:80]}")
                try:
                    summary = summarizer.summarize_anthropic_article(article)
                    set_anthropic_summary(db, article.id, summary)
                except OpenAIError as e:
                    print(f"      ! failed: {e}")

        if do_youtube:
            videos = (
                get_all_youtube_videos(db) if force
                else get_unsummarized_youtube_videos(db)
            )
            if limit is not None:
                videos = videos[:limit]
            label = "re-summarize" if force else "summarize"
            print(f"[youtube]   {len(videos)} video(s) to {label}.")
            for i, video in enumerate(videos, start=1):
                print(f"  ({i}/{len(videos)}) {video.title[:80]}")
                try:
                    summary = summarizer.summarize_youtube_video(video)
                    set_youtube_summary(db, video.id, summary)
                except OpenAIError as e:
                    print(f"      ! failed: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Per-row LLM summarizer (OpenAI).")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap rows of each type (default: all unsummarized).")
    parser.add_argument("--anthropic", action="store_true",
                        help="Only Anthropic blog articles.")
    parser.add_argument("--youtube", action="store_true",
                        help="Only YouTube videos.")
    parser.add_argument("--force", action="store_true",
                        help="Re-summarize rows that already have a summary "
                             "(burns API credits — pair with --limit to test).")
    args = parser.parse_args()

    # If neither flag is passed, run both.
    do_anthropic = args.anthropic or not (args.anthropic or args.youtube)
    do_youtube   = args.youtube   or not (args.anthropic or args.youtube)

    if "OPENAI_API_KEY" not in os.environ:
        raise SystemExit(
            "OPENAI_API_KEY is not set. Add it to your .env file at the project root."
        )

    _run(limit=args.limit, do_anthropic=do_anthropic, do_youtube=do_youtube, force=args.force)


if __name__ == "__main__":
    main()
