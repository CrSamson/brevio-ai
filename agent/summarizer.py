"""
agent/summarizer.py — Per-row LLM summarizer (OpenAI).

Reads rows whose `summary` column is NULL/empty from `articles`,
`papers`, and `youtube_videos`, asks an OpenAI model to summarize each
one, and writes the result back.

The system prompt varies by kind:
  - articles / video transcripts -> a busy AI-practitioner blurb
  - papers                       -> a plain-English explainer for a
                                    general audience

Run directly to fill in summaries for everything currently unsummarized:

    python -m agent.summarizer
    python -m agent.summarizer --limit 5      # cap how many of each type
    python -m agent.summarizer --articles     # only blog/news articles
    python -m agent.summarizer --papers       # only research papers
    python -m agent.summarizer --youtube      # only YouTube videos
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI, OpenAIError

from app.database.crud import (
    get_all_articles,
    get_all_papers,
    get_all_youtube_videos,
    get_unsummarized_articles,
    get_unsummarized_papers,
    get_unsummarized_youtube_videos,
    set_article_summary,
    set_paper_summary,
    set_youtube_summary,
)
from app.database.db import get_db
from app.database.models import Article, Paper, YoutubeVideo


# .env lives at the project root; mirror app/database/db.py
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_PROJECT_ROOT / ".env", override=True)


DEFAULT_MODEL = "gpt-4o"   
DEFAULT_MAX_TOKENS = 1024

# Hard cap on chars of source text we pass into the prompt. Anthropic articles
# in particular can run long once Docling has rendered them; trimming keeps
# cost predictable for a per-row summarizer. Tune as needed.
MAX_SOURCE_CHARS = 40_000

_ARTICLE_PROMPT = (
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

_PAPER_PROMPT = (
    "You translate an academic AI/ML paper into plain English for a general "
    "audience in a daily digest email. The reader is intelligent and curious "
    "but not a researcher - they want to understand what the paper found, "
    "why it matters, and what is novel about it, without getting buried in "
    "jargon.\n\n"
    "Output a single paragraph of 3 to 5 sentences, plain text. No bullets, "
    "no markdown, no labels, no headings, no leading title.\n\n"
    "Lead with the central finding or contribution in plain English. Then "
    "briefly say what problem the work addresses and what is novel about the "
    "approach. Mention concrete details (numbers, comparisons, model names, "
    "datasets) when the abstract supports it. If the paper introduces a "
    "method or model, name it.\n\n"
    "Tone: clear, direct, lightly opinionated where the source supports it. "
    "Define a technical term inline only when it is load-bearing for the "
    "takeaway. Avoid throat-clearing ('In this paper...', 'The authors "
    "propose...', 'This work studies...') - go straight to what the paper "
    "says. Do not invent claims the abstract does not make.\n\n"
    "If the contribution is genuinely incremental or its claims are narrow, "
    "say so plainly in one sentence. Do not pad."
)

_PROMPTS: dict[str, str] = {
    "article":          _ARTICLE_PROMPT,
    "video transcript": _ARTICLE_PROMPT,
    "paper":            _PAPER_PROMPT,
}


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

    def summarize_article(self, article: Article) -> str:
        # Prefer Docling-extracted markdown when available (per-source
        # `fetch_content` toggle in config/sources.json). Fall back to the
        # RSS feed's <description> / <summary>, which is preserved verbatim
        # in raw_metadata by RssBlogScraper.
        body = article.content_md or ""
        if not body and article.raw_metadata:
            body = (
                article.raw_metadata.get("summary")
                or article.raw_metadata.get("description")
                or ""
            )
        return self._summarize(
            kind="article",
            title=article.title,
            url=article.url,
            body=body,
        )

    def summarize_paper(self, paper: Paper) -> str:
        # arXiv abstract is short (~150-300 words) and is the canonical
        # source. Prepend the categories so the model knows what flavor
        # of paper it's dealing with.
        body = paper.abstract or ""
        if paper.categories:
            body = f"[arXiv categories: {', '.join(paper.categories)}]\n\n{body}"
        return self._summarize(
            kind="paper",
            title=paper.title,
            url=paper.url,
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

        system_prompt = _PROMPTS.get(kind, _ARTICLE_PROMPT)

        response = self.client.chat.completions.create(
            model=self.model,
            max_tokens=self.max_tokens,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
        )

        return (response.choices[0].message.content or "").strip()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _run(limit: int | None, do_articles: bool, do_papers: bool,
         do_youtube: bool, force: bool) -> None:
    summarizer = Summarizer()
    label = "re-summarize" if force else "summarize"

    with get_db() as db:
        if do_articles:
            articles = (
                get_all_articles(db) if force
                else get_unsummarized_articles(db)
            )
            if limit is not None:
                articles = articles[:limit]
            print(f"[articles] {len(articles)} article(s) to {label}.")
            for i, article in enumerate(articles, start=1):
                print(f"  ({i}/{len(articles)}) [{article.source}] {article.title[:80]}")
                try:
                    summary = summarizer.summarize_article(article)
                    set_article_summary(db, article.id, summary)
                except OpenAIError as e:
                    print(f"      ! failed: {e}")

        if do_papers:
            papers = (
                get_all_papers(db) if force
                else get_unsummarized_papers(db)
            )
            if limit is not None:
                papers = papers[:limit]
            print(f"[papers]   {len(papers)} paper(s) to {label}.")
            for i, paper in enumerate(papers, start=1):
                cats = ",".join(paper.categories[:2]) if paper.categories else "-"
                print(f"  ({i}/{len(papers)}) [{cats}] {paper.title[:80]}")
                try:
                    summary = summarizer.summarize_paper(paper)
                    set_paper_summary(db, paper.id, summary)
                except OpenAIError as e:
                    print(f"      ! failed: {e}")

        if do_youtube:
            videos = (
                get_all_youtube_videos(db) if force
                else get_unsummarized_youtube_videos(db)
            )
            if limit is not None:
                videos = videos[:limit]
            print(f"[youtube]  {len(videos)} video(s) to {label}.")
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
    parser.add_argument("--articles", action="store_true",
                        help="Only blog/news articles.")
    parser.add_argument("--papers", action="store_true",
                        help="Only research papers.")
    parser.add_argument("--youtube", action="store_true",
                        help="Only YouTube videos.")
    parser.add_argument("--force", action="store_true",
                        help="Re-summarize rows that already have a summary "
                             "(burns API credits — pair with --limit to test).")
    args = parser.parse_args()

    # If no flag is passed, run all three.
    any_flag    = args.articles or args.papers or args.youtube
    do_articles = args.articles or not any_flag
    do_papers   = args.papers   or not any_flag
    do_youtube  = args.youtube  or not any_flag

    if "OPENAI_API_KEY" not in os.environ:
        raise SystemExit(
            "OPENAI_API_KEY is not set. Add it to your .env file at the project root."
        )

    _run(limit=args.limit, do_articles=do_articles, do_papers=do_papers,
         do_youtube=do_youtube, force=args.force)


if __name__ == "__main__":
    main()
