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
import json
import os
import sys
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


# Fixed topic taxonomy. The LLM is required to return values from this set.
# Anything else is dropped during validation; if validation produces an empty
# list, we fall back to the source-declared topics.
ALLOWED_TOPICS: frozenset[str] = frozenset({"ai", "technology", "business", "science", "general"})

# Cap on topics per item. The prompt asks for at most 2; we enforce in code.
MAX_TOPICS_PER_ITEM: int = 2


# .env lives at the project root; mirror app/database/db.py
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_PROJECT_ROOT / ".env", override=True)


DEFAULT_MODEL = "gpt-4o-mini"   
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

# Shared output-format block appended to every system prompt. Defines the
# JSON contract and the topic-classification rules. Kept separate so the
# article/paper voice prompts stay focused on tone + structure.
_TOPIC_TAXONOMY_BLOCK = (
    "\n\n--- OUTPUT FORMAT ---\n"
    "Return ONLY a single JSON object, no markdown fences, no prose:\n"
    '{"summary": "<the paragraph from the instructions above>", '
    '"topics": ["<topic1>", "<topic2>"]}\n\n'
    "TOPIC RULES:\n"
    "- Allowed values: \"ai\", \"technology\", \"business\", \"science\", \"general\". "
    "No others.\n"
    "- Pick 1 or 2 topics. Most items get exactly 1. Use 2 only when the item "
    "genuinely sits in two buckets (e.g. an AI-company funding round = "
    "[\"ai\", \"business\"]).\n"
    "- \"ai\": ML/LLMs/AI products/AI research/AI companies. Combine with "
    "\"business\" for AI-company funding/M&A/earnings.\n"
    "- \"technology\": non-AI tech — hardware, software, telecoms, gadgets, "
    "platforms, cybersecurity.\n"
    "- \"business\": finance, markets, earnings, deals, macroeconomics — when "
    "AI/tech is not the central subject.\n"
    "- \"science\": non-AI research — physics, biology, chemistry, space, "
    "medicine, climate.\n"
    "- \"general\": catch-all for politics, conflict, crime, sports, "
    "lifestyle, human interest. NEVER combine \"general\" with another topic. "
    "If \"general\" applies, return [\"general\"] alone.\n"
)


_PROMPTS: dict[str, str] = {
    "article":          _ARTICLE_PROMPT          + _TOPIC_TAXONOMY_BLOCK,
    "video transcript": _ARTICLE_PROMPT          + _TOPIC_TAXONOMY_BLOCK,
    "paper":            _PAPER_PROMPT            + _TOPIC_TAXONOMY_BLOCK,
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

    def summarize_article(self, article: Article) -> tuple[str, list[str]]:
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
            fallback_topics=list(article.topics or []),
        )

    def summarize_paper(self, paper: Paper) -> tuple[str, list[str]]:
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
            fallback_topics=list(paper.topics or []),
        )

    def summarize_youtube_video(self, video: YoutubeVideo) -> tuple[str, list[str]]:
        body = video.transcript or video.description or ""
        return self._summarize(
            kind="video transcript",
            title=video.title,
            url=str(video.url),
            body=body,
            fallback_topics=list(video.topics or []),
        )

    # ------------------------------------------------------------------
    # Core call
    # ------------------------------------------------------------------

    def _summarize(
        self,
        *,
        kind: str,
        title: str,
        url: str,
        body: str,
        fallback_topics: list[str],
    ) -> tuple[str, list[str]]:
        body = (body or "").strip()
        if not body:
            return (
                f"No source text available to summarize ({kind}).",
                _validate_topics(None, fallback=fallback_topics),
            )

        if len(body) > MAX_SOURCE_CHARS:
            body = body[:MAX_SOURCE_CHARS] + "\n\n[... truncated for length]"

        user_prompt = (
            f"Summarize this {kind}.\n\n"
            f"Title: {title}\n"
            f"URL: {url}\n\n"
            f"--- SOURCE START ---\n{body}\n--- SOURCE END ---"
        )

        system_prompt = _PROMPTS.get(kind, _ARTICLE_PROMPT + _TOPIC_TAXONOMY_BLOCK)

        response = self.client.chat.completions.create(
            model=self.model,
            max_tokens=self.max_tokens,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
        )

        raw = (response.choices[0].message.content or "").strip()
        summary, topics_raw = _parse_llm_json(raw)
        topics = _validate_topics(topics_raw, fallback=fallback_topics)
        return summary, topics


# ---------------------------------------------------------------------------
# Output parsing + topic validation
# ---------------------------------------------------------------------------

def _parse_llm_json(raw: str) -> tuple[str, object]:
    """Best-effort extraction of {summary, topics} from the LLM response.
    Returns (summary_str, topics_raw_for_validation). On any failure the
    summary falls back to the raw text and topics_raw is None so the
    validator uses fallback_topics."""
    if not raw:
        return "", None
    # Some models occasionally wrap JSON in ```json fences despite the
    # explicit instruction not to. Strip them defensively.
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # Treat the whole response as the summary; topics fall back.
        return raw.strip(), None
    if not isinstance(data, dict):
        return raw.strip(), None
    summary = (data.get("summary") or "").strip()
    return summary, data.get("topics")


def _validate_topics(raw: object, *, fallback: list[str]) -> list[str]:
    """Coerce the LLM's `topics` field into a clean list:
      - drop anything that isn't a string in ALLOWED_TOPICS
      - dedupe while preserving order
      - if "general" appears alongside other topics, drop "general"
        (mutual exclusivity rule)
      - cap at MAX_TOPICS_PER_ITEM
      - if the list is empty after cleanup, fall back to the source-declared
        topics (filtered to the allowed set; may itself be empty)."""
    if not isinstance(raw, list):
        cleaned: list[str] = []
    else:
        cleaned = []
        for t in raw:
            if not isinstance(t, str):
                continue
            norm = t.lower().strip()
            if norm in ALLOWED_TOPICS and norm not in cleaned:
                cleaned.append(norm)

    if "general" in cleaned and len(cleaned) > 1:
        cleaned = [t for t in cleaned if t != "general"]

    cleaned = cleaned[:MAX_TOPICS_PER_ITEM]

    if cleaned:
        return cleaned

    # Fallback path: source-declared topics, filtered to allowed values.
    fb = [t for t in (fallback or []) if t in ALLOWED_TOPICS]
    return fb[:MAX_TOPICS_PER_ITEM]


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
                    summary, topics = summarizer.summarize_article(article)
                    set_article_summary(db, article.id, summary, topics=topics)
                    db.commit()  # per-row commit — a later crash never wipes prior progress
                    print(f"      topics={topics}")
                except OpenAIError as e:
                    print(f"      ! failed: {e}")
                    db.rollback()

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
                    summary, topics = summarizer.summarize_paper(paper)
                    set_paper_summary(db, paper.id, summary, topics=topics)
                    db.commit()
                    print(f"      topics={topics}")
                except OpenAIError as e:
                    print(f"      ! failed: {e}")
                    db.rollback()

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
                    summary, topics = summarizer.summarize_youtube_video(video)
                    set_youtube_summary(db, video.id, summary, topics=topics)
                    db.commit()
                    print(f"      topics={topics}")
                except OpenAIError as e:
                    print(f"      ! failed: {e}")
                    db.rollback()


def main() -> None:
    # Windows consoles default to cp1252 and choke on '→', accented chars,
    # smart quotes that appear in some article titles (Le Monde, The
    # Independent etc.). A UnicodeEncodeError mid-batch was previously
    # taking down the entire `with get_db()` transaction and rolling back
    # every prior LLM-classified row. Force UTF-8 here as a belt; the
    # per-row commits in `_run` are the suspenders.
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        pass

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
