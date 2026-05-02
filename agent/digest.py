"""
agent/digest.py — Build and email the AI News digest.

Pulls rows summarized within the last N hours, renders an HTML + plain-text
email, and sends it via SMTP. Designed to run after `python -m agent.summarizer`
has populated the `summary` columns.

Run directly:

    python -m agent.digest                   # last 24h, send
    python -m agent.digest --hours 48        # different lookback
    python -m agent.digest --dry-run         # render to stdout, don't send
    python -m agent.digest --to a@b.com      # override recipient

Required env vars (in .env at the project root):

    SMTP_HOST       (default: smtp.gmail.com)
    SMTP_PORT       (default: 587)
    SMTP_USER       sender email (also used for STARTTLS auth)
    SMTP_PASSWORD   app password (Gmail: console.google.com -> Security -> App passwords)
    DIGEST_FROM     optional, defaults to SMTP_USER
    DIGEST_TO       recipient (comma-separated for multiple)
"""

from __future__ import annotations

import argparse
import html
import os
import smtplib
import ssl
import sys
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

from dotenv import load_dotenv

from app.database.crud import (
    get_recent_summarized_articles,
    get_recent_summarized_papers,
    get_recent_summarized_youtube_videos,
    mark_digest_sent,
)
from app.database.db import get_db
from app.database.models import Article, Paper, YoutubeVideo


# ---------------------------------------------------------------------------
# Anthropic title cleanup (was scrapers/anthropic_scrapper.clean_anthropic_title)
# ---------------------------------------------------------------------------
# The Olshansk RSS mirror prepends each Anthropic title with a date and
# category, no separator (e.g. "Apr 29, 2026ScienceEvaluating ..."). This
# strips both. Applied via _article_title() only to rows whose source
# starts with 'anthropic_'; other sources have clean titles already.
import re

_ANTHROPIC_DATE_PREFIX_RE = re.compile(
    r"^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*"
    r"\s+\d{1,2},\s*\d{4}"
)
_ANTHROPIC_CATEGORIES: tuple[str, ...] = (
    "Customer Stories",
    "Customer story",
    "Announcements",
    "Engineering",
    "Education",
    "Interpretability",
    "Product",
    "Policy",
    "Research",
    "Science",
    "Society",
    "News",
)


def clean_anthropic_title(raw: str) -> str:
    """Strip the leading date and category prepended by the Olshansk feed."""
    s = (raw or "").strip()
    m = _ANTHROPIC_DATE_PREFIX_RE.match(s)
    if m:
        s = s[m.end():].lstrip()
    for cat in _ANTHROPIC_CATEGORIES:
        if s.startswith(cat):
            s = s[len(cat):].lstrip()
            break
    return s


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_PROJECT_ROOT / ".env", override=True)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _summary_to_paragraph(summary: str) -> str:
    """
    Flatten the stored summary into one flowing paragraph.

    New summaries arrive as a paragraph already; older summaries (pre-prompt-
    rewrite) are bullet lines prefixed with '- '. Stripping the prefixes and
    joining with spaces makes both render identically.
    """
    parts: list[str] = []
    for raw in (summary or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        for prefix in ("- ", "* ", "• "):
            if line.startswith(prefix):
                line = line[len(prefix):]
                break
        parts.append(line)
    return " ".join(parts)


def _article_meta(article: Article) -> str:
    parts: list[str] = []
    if article.published_at:
        parts.append(article.published_at.strftime("%Y-%m-%d"))
    if article.source:
        parts.append(article.source)
    return " · ".join(parts)


def _article_title(article: Article) -> str:
    """Per-source title cleanup. Only Anthropic feeds need it (the
    Olshansk RSS mirror prepends a date+category run-on prefix to titles).
    Other sources return clean titles already."""
    if article.source.startswith("anthropic_"):
        return clean_anthropic_title(article.title)
    return article.title


def _youtube_meta(video: YoutubeVideo) -> str:
    parts = [video.published_at.strftime("%Y-%m-%d")]
    if video.channel_handle:
        parts.append(video.channel_handle)
    return " · ".join(parts)


def _paper_meta(paper: Paper) -> str:
    parts: list[str] = []
    if paper.published_at:
        parts.append(paper.published_at.strftime("%Y-%m-%d"))
    if paper.categories:
        parts.append(", ".join(paper.categories[:3]))
    if paper.hf_upvotes is not None and paper.hf_upvotes > 0:
        parts.append(f"↑ {paper.hf_upvotes}")
    return " · ".join(parts)


def _paper_authors(paper: Paper) -> str:
    """First 3 authors + 'et al.' if more. Empty string if no authors."""
    if not paper.authors:
        return ""
    if len(paper.authors) <= 3:
        return ", ".join(paper.authors)
    return ", ".join(paper.authors[:3]) + " et al."


def _youtube_thumbnail(video_id: str) -> str:
    """`hqdefault.jpg` always exists for any video and is 480x360 — safe default."""
    return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"


# Inline-styled chip background per kind. Subtle, neutral palette so the
# topic section heading remains the dominant visual cue.
_KIND_BADGE_STYLES: dict[str, tuple[str, str, str]] = {
    "article": ("Article", "#eef2ff", "#3730a3"),  # indigo
    "paper":   ("Paper",   "#fef3c7", "#92400e"),  # amber
    "video":   ("YouTube", "#fee2e2", "#991b1b"),  # red
}


def _kind_badge_html(kind: str) -> str:
    spec = _KIND_BADGE_STYLES.get(kind)
    if not spec:
        return ""
    label, bg, fg = spec
    return (
        f'<span style="display:inline-block;font-size:11px;font-weight:700;'
        f'text-transform:uppercase;letter-spacing:0.06em;'
        f'padding:2px 8px;border-radius:999px;'
        f'background:{bg};color:{fg};margin:0 8px 0 0;">{label}</span>'
    )


def _card_html(*, url: str, title: str, meta: str, summary: str,
               thumbnail: str | None, cta: str,
               authors: str | None = None,
               kind: str | None = None) -> str:
    """One article/paper/video card. No leading whitespace inside <a> tags —
    Gmail preserves it as a leading space before the link text."""
    safe_url   = html.escape(url)
    safe_title = html.escape(title)
    safe_meta  = html.escape(meta)
    safe_cta   = html.escape(cta)
    safe_body  = (
        html.escape(summary) if summary
        else '<em style="color:#888;">No summary available.</em>'
    )

    img = ""
    if thumbnail:
        img = (
            f'<a href="{safe_url}" style="display:block;margin:0 0 14px;">'
            f'<img src="{html.escape(thumbnail)}" alt="" '
            f'style="display:block;width:100%;max-width:560px;'
            f'border-radius:10px;border:0;outline:none;"></a>'
        )

    authors_block = ""
    if authors:
        authors_block = (
            f'<div style="font-size:13px;color:#475569;margin:0 0 6px;'
            f'font-style:italic;">{html.escape(authors)}</div>'
        )

    badge = _kind_badge_html(kind) if kind else ""

    return (
        f'<div style="margin:0 0 36px;padding:0 0 28px;border-bottom:1px solid #eee;">'
        f'{img}'
        f'<div style="margin:0 0 8px;">{badge}</div>'
        f'<h3 style="font-size:18px;font-weight:700;line-height:1.3;margin:0 0 6px;">'
        f'<a href="{safe_url}" style="color:#0f172a;text-decoration:none;">{safe_title}</a>'
        f'</h3>'
        f'{authors_block}'
        f'<div style="font-size:12px;color:#94a3b8;margin:0 0 10px;'
        f'text-transform:uppercase;letter-spacing:0.04em;">{safe_meta}</div>'
        f'<p style="font-size:15px;line-height:1.6;color:#334155;margin:0 0 12px;">'
        f'{safe_body}</p>'
        f'<a href="{safe_url}" style="font-size:13px;color:#cc785c;'
        f'text-decoration:none;font-weight:600;">{safe_cta}</a>'
        f'</div>'
    )


def _card_for(kind: str, item) -> str:
    """Render one (kind, item) tuple as an HTML card with a kind badge."""
    if kind == "article":
        return _card_html(
            url=str(item.url),
            title=_article_title(item),
            meta=_article_meta(item),
            summary=_summary_to_paragraph(item.summary),
            thumbnail=None,
            cta="Read more →",
            kind="article",
        )
    if kind == "paper":
        return _card_html(
            url=item.url,
            title=item.title,
            authors=_paper_authors(item) or None,
            meta=_paper_meta(item),
            summary=_summary_to_paragraph(item.summary),
            thumbnail=None,
            cta="Read on arXiv →",
            kind="paper",
        )
    # video
    return _card_html(
        url=str(item.url),
        title=item.title,
        meta=_youtube_meta(item),
        summary=_summary_to_paragraph(item.summary),
        thumbnail=_youtube_thumbnail(item.video_id),
        cta="Watch on YouTube →",
        kind="video",
    )


def render_html(
    *,
    hours: int,
    by_topic: dict[str, list[tuple[str, object]]],
) -> str:
    """Inline-styled HTML — no <style> blocks for max client compatibility.
    Renders one section per topic in DIGEST_TOPIC_ORDER, each card carrying
    a kind badge (Article / Paper / YouTube)."""

    def section_heading(title: str, count: int) -> str:
        return (
            f'<h2 style="font-size:13px;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:0.12em;color:#0f172a;margin:40px 0 22px;'
            f'padding:0 0 10px;border-bottom:2px solid #0f172a;">'
            f'{html.escape(title)} '
            f'<span style="color:#94a3b8;font-weight:500;">({count})</span>'
            f'</h2>'
        )

    def section_body(cards: list[str]) -> str:
        if not cards:
            return (
                '<p style="color:#94a3b8;font-size:14px;font-style:italic;'
                'margin:8px 0 0;">Nothing new in this window.</p>'
            )
        return "".join(cards)

    sections_html = ""
    total = 0
    for topic in DIGEST_TOPIC_ORDER:
        items = by_topic.get(topic, [])
        total += len(items)
        cards = [_card_for(kind, item) for kind, item in items]
        sections_html += section_heading(DIGEST_TOPIC_LABELS[topic], len(items))
        sections_html += section_body(cards)

    now = datetime.now(timezone.utc)
    subtitle = " · ".join(DIGEST_TOPIC_LABELS[t] for t in DIGEST_TOPIC_ORDER)

    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '</head>'
        '<body style="margin:0;padding:0;background:#f1f5f9;">'
        '<div style="height:6px;background:#cc785c;"></div>'
        '<div style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Roboto,'
        '\'Helvetica Neue\',Arial,sans-serif;max-width:680px;margin:0 auto;'
        'padding:36px 28px 28px;background:#ffffff;color:#0f172a;">'
        f'<h1 style="font-size:26px;font-weight:700;letter-spacing:-0.02em;'
        f'margin:0 0 6px;">Brevio Daily</h1>'
        f'<p style="color:#475569;font-size:14px;margin:0 0 4px;font-weight:600;">'
        f'{html.escape(subtitle)}'
        f'</p>'
        f'<p style="color:#64748b;font-size:13px;margin:0 0 4px;'
        f'text-transform:uppercase;letter-spacing:0.1em;font-weight:600;">'
        f'{now.strftime("%A, %B %d, %Y")}'
        f'</p>'
        f'<p style="color:#94a3b8;font-size:13px;margin:0;">'
        f'{total} item(s) from the last {hours}h'
        f'</p>'
        f'{sections_html}'
        f'<p style="color:#cbd5e1;font-size:11px;margin-top:40px;'
        f'border-top:1px solid #e2e8f0;padding-top:16px;text-align:center;">'
        f'Generated {now.strftime("%Y-%m-%d %H:%M UTC")}'
        f'</p>'
        '</div>'
        '<div style="height:24px;background:#f1f5f9;"></div>'
        '</body></html>'
    )


def _text_lines_for(kind: str, item) -> list[str]:
    """Render one (kind, item) tuple as a list of plain-text lines, with a
    [KIND] tag prefix on the title line."""
    tag_map = {"article": "[ARTICLE]", "paper": "[PAPER]", "video": "[YOUTUBE]"}
    tag = tag_map.get(kind, "")

    if kind == "article":
        title     = _article_title(item)
        url       = str(item.url)
        meta      = _article_meta(item)
        paragraph = _summary_to_paragraph(item.summary)
        return [
            f"{tag} {title}".strip(),
            meta,
            paragraph or "(no summary)",
            f"-> {url}",
            "",
        ]
    if kind == "paper":
        title     = item.title
        url       = item.url
        authors   = _paper_authors(item)
        meta      = _paper_meta(item)
        paragraph = _summary_to_paragraph(item.summary)
        out = [f"{tag} {title}".strip()]
        if authors:
            out.append(authors)
        out.append(meta)
        out.append(paragraph or "(no summary)")
        out.append(f"-> {url}")
        out.append("")
        return out
    # video
    return [
        f"{tag} {item.title}".strip(),
        _youtube_meta(item),
        _summary_to_paragraph(item.summary) or "(no summary)",
        f"-> {str(item.url)}",
        "",
    ]


def render_text(
    *,
    hours: int,
    by_topic: dict[str, list[tuple[str, object]]],
) -> str:
    """Plain-text fallback for clients that don't render HTML. Sections are
    organised by topic (matching render_html); kind shown as a [TAG] prefix
    on each item's title line."""
    now = datetime.now(timezone.utc)
    subtitle = " · ".join(DIGEST_TOPIC_LABELS[t] for t in DIGEST_TOPIC_ORDER)
    total = sum(len(items) for items in by_topic.values())

    lines: list[str] = [
        "BREVIO DAILY",
        subtitle,
        f"Last {hours}h · {now.strftime('%Y-%m-%d')} · {total} item(s)",
        "",
    ]

    for topic in DIGEST_TOPIC_ORDER:
        items = by_topic.get(topic, [])
        lines.append(f"== {DIGEST_TOPIC_LABELS[topic].upper()} ({len(items)}) ==")
        if not items:
            lines.extend(["  (nothing new)", ""])
            continue
        for kind, item in items:
            lines.extend(_text_lines_for(kind, item))

    lines.append(f"-- generated {now.strftime('%Y-%m-%d %H:%M UTC')} --")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build_digest(
    hours: int,
) -> tuple[list[Article], list[Paper], list[YoutubeVideo]]:
    with get_db() as db:
        articles = get_recent_summarized_articles(db, hours=hours)
        papers   = get_recent_summarized_papers(db, hours=hours)
        videos   = get_recent_summarized_youtube_videos(db, hours=hours)
        # Detach from session so callers can read attributes after the context exits.
        for obj in (*articles, *papers, *videos):
            db.expunge(obj)
    return articles, papers, videos


# Order of topic sections in the email. The first topic with a remaining
# slot also gets any rounding-remainder when max_items doesn't divide evenly
# by the topic count. At max_items=15 across 5 topics: 3/3/3/3/3.
#
# "general" is intentionally last so genuine domain items keep their priority
# placement; only items the LLM could not assign to a domain bucket end up
# here. Multi-topic items are placed in the first matching topic in this
# order — e.g. ["ai", "business"] -> AI section.
DIGEST_TOPIC_ORDER: list[str] = ["ai", "technology", "business", "science", "general"]

# Display labels for each topic in section headings + subject line.
DIGEST_TOPIC_LABELS: dict[str, str] = {
    "ai":         "AI",
    "technology": "Technology",
    "business":   "Business",
    "science":    "Science",
    "general":    "General News",
}

# Within a single topic section, no one source/channel may contribute more
# than this many items. Diversity guard against a single publisher dominating.
DIGEST_MAX_PER_SOURCE: int = 2


def _diversity_key(kind: str, item) -> str | None:
    """Return a per-section diversity key, or None if the kind doesn't enforce one.

    Articles dedupe by source. Videos dedupe by channel. Papers don't enforce
    a cap because every paper is unique by arxiv_id - one publisher (arxiv,
    hf_daily) can legitimately surface multiple distinct papers.
    """
    if kind == "article":
        return f"a:{getattr(item, 'source', None)}"
    if kind == "video":
        return f"v:{getattr(item, 'channel_handle', None)}"
    return None  # papers


def cap_by_topic(
    articles: list[Article],
    papers: list[Paper],
    videos: list[YoutubeVideo],
    max_items: int,
) -> dict[str, list[tuple[str, object]]]:
    """
    Distribute items across topic sections with per-source diversity inside
    each section. Returns a dict keyed by topic (in DIGEST_TOPIC_ORDER order),
    each value a list of (kind, item) tuples sorted by published_at desc.

    Quota: max_items split as evenly as possible across DIGEST_TOPIC_ORDER.
    For max_items=15 across 4 topics: 4/4/4/3 (first three topics get the +1).

    Multi-topic items: a row tagged ["ai", "technology"] is rendered in the
    first matching topic in DIGEST_TOPIC_ORDER and never duplicated. The
    `placed` set tracks (kind, id) pairs across topics.

    Per-source diversity: within a single topic, no source/channel exceeds
    DIGEST_MAX_PER_SOURCE items. Papers don't enforce diversity.
    """
    EARLIEST = datetime.min.replace(tzinfo=timezone.utc)

    # Unified stream sorted by recency desc - one pass, used for every topic.
    stream: list[tuple[str, object, datetime]] = []
    stream += [("article", a, a.published_at or EARLIEST) for a in articles]
    stream += [("paper",   p, p.published_at or EARLIEST) for p in papers]
    stream += [("video",   v, v.published_at or EARLIEST) for v in videos]
    stream.sort(key=lambda t: t[2], reverse=True)

    n_topics = len(DIGEST_TOPIC_ORDER)
    base, rem = divmod(max(0, max_items), n_topics)
    quotas = {t: base + (1 if i < rem else 0) for i, t in enumerate(DIGEST_TOPIC_ORDER)}

    placed: set[tuple[str, int]] = set()
    by_topic: dict[str, list[tuple[str, object]]] = {t: [] for t in DIGEST_TOPIC_ORDER}

    for topic in DIGEST_TOPIC_ORDER:
        quota = quotas[topic]
        if quota <= 0:
            continue
        source_counts: dict[str, int] = {}
        for kind, item, _date in stream:
            if len(by_topic[topic]) >= quota:
                break
            key = (kind, item.id)
            if key in placed:
                continue
            if topic not in (getattr(item, "topics", None) or []):
                continue
            div_key = _diversity_key(kind, item)
            if div_key is not None:
                if source_counts.get(div_key, 0) >= DIGEST_MAX_PER_SOURCE:
                    continue
                source_counts[div_key] = source_counts.get(div_key, 0) + 1
            by_topic[topic].append((kind, item))
            placed.add(key)

    return by_topic


def flatten_by_topic(
    by_topic: dict[str, list[tuple[str, object]]],
) -> tuple[list[Article], list[Paper], list[YoutubeVideo]]:
    """Split a by_topic dict back into three per-kind lists. Used by the
    post-send mark step which still operates per-table."""
    articles: list[Article]      = []
    papers:   list[Paper]        = []
    videos:   list[YoutubeVideo] = []
    for items in by_topic.values():
        for kind, item in items:
            if kind == "article":
                articles.append(item)   # type: ignore[arg-type]
            elif kind == "paper":
                papers.append(item)     # type: ignore[arg-type]
            elif kind == "video":
                videos.append(item)     # type: ignore[arg-type]
    return articles, papers, videos


# ---------------------------------------------------------------------------
# Send
# ---------------------------------------------------------------------------

def send_email(
    *,
    subject: str,
    text_body: str,
    html_body: str,
    recipients: list[str],
) -> None:
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ["SMTP_USER"]
    smtp_password = os.environ["SMTP_PASSWORD"]
    sender = os.environ.get("DIGEST_FROM", smtp_user)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = ", ".join(recipients)
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_host, smtp_port) as smtp:
        smtp.starttls(context=context)
        smtp.login(smtp_user, smtp_password)
        smtp.send_message(msg)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    # Windows consoles default to cp1252 and choke on '→', '·' etc. Force UTF-8
    # so --dry-run output is printable. No effect on the email body itself.
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        pass

    parser = argparse.ArgumentParser(description="Build and email the Brevio Daily digest.")
    parser.add_argument("--hours", type=int, default=24,
                        help="Lookback window in hours (default: 24).")
    parser.add_argument("--to", type=str, default=None,
                        help="Override DIGEST_TO recipient (comma-separated for multiple).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Render to stdout instead of sending.")
    parser.add_argument("--max-items", type=int, default=15,
                        help="Cap total items in the digest. Quotas split evenly "
                             "across DIGEST_TOPIC_ORDER (default 4 topics → 4/4/4/3 "
                             "at max=15). Within each topic no one source may "
                             "contribute more than DIGEST_MAX_PER_SOURCE items "
                             "(default 2). Multi-topic items render in the first "
                             "matching topic only. Default: 15.")
    args = parser.parse_args()

    articles, papers, videos = build_digest(hours=args.hours)
    pre_total = len(articles) + len(papers) + len(videos)
    print(f"[digest] {len(articles)} article(s), {len(papers)} paper(s), "
          f"{len(videos)} video(s) in last {args.hours}h.")

    by_topic = cap_by_topic(articles, papers, videos, max_items=args.max_items)
    total = sum(len(items) for items in by_topic.values())
    if total < pre_total:
        print(f"[digest] capped from {pre_total} to {total} items (--max-items={args.max_items}).")

    # Per-topic placement breakdown (mirrors the verification report format).
    for topic in DIGEST_TOPIC_ORDER:
        items = by_topic.get(topic, [])
        kinds = {"article": 0, "paper": 0, "video": 0}
        for k, _ in items:
            kinds[k] = kinds.get(k, 0) + 1
        print(f"  [{DIGEST_TOPIC_LABELS[topic]}] {len(items)} item(s) "
              f"(articles={kinds['article']}, papers={kinds['paper']}, videos={kinds['video']})")

    if total == 0:
        print("[digest] Nothing to send. Exiting.")
        return

    html_body = render_html(hours=args.hours, by_topic=by_topic)
    text_body = render_text(hours=args.hours, by_topic=by_topic)
    subject   = (
        f"Brevio Daily — {' · '.join(DIGEST_TOPIC_LABELS[t] for t in DIGEST_TOPIC_ORDER)} "
        f"({total} item{'s' if total != 1 else ''})"
    )

    if args.dry_run:
        print("\n----- SUBJECT -----")
        print(subject)
        print("\n----- TEXT -----")
        print(text_body)
        print("\n----- HTML -----")
        print(html_body)
        return

    recipients_raw = args.to or os.environ.get("DIGEST_TO")
    if not recipients_raw:
        raise SystemExit(
            "No recipient. Set DIGEST_TO in .env or pass --to."
        )
    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]

    for var in ("SMTP_USER", "SMTP_PASSWORD"):
        if var not in os.environ:
            raise SystemExit(f"{var} is not set. Add it to your .env file.")

    send_email(
        subject=subject,
        text_body=text_body,
        html_body=html_body,
        recipients=recipients,
    )
    print(f"[digest] Sent to {', '.join(recipients)}.")

    # Mark every row that was actually included so the same content never
    # appears in a future digest. We mark AFTER the send so a SMTP failure
    # leaves the rows unsent and they get a second chance on the next run.
    sent_articles, sent_papers, sent_videos = flatten_by_topic(by_topic)
    _mark_sent(sent_articles, sent_papers, sent_videos)


def _mark_sent(
    articles: list[Article],
    papers:   list[Paper],
    videos:   list[YoutubeVideo],
) -> None:
    """Stamp digest_sent_at = NOW() on every row that just shipped."""
    try:
        with get_db() as db:
            n_a = mark_digest_sent(db, Article,      [a.id for a in articles])
            n_p = mark_digest_sent(db, Paper,        [p.id for p in papers])
            n_v = mark_digest_sent(db, YoutubeVideo, [v.id for v in videos])
        print(f"[digest] Marked {n_a} article(s), {n_p} paper(s), {n_v} video(s) as sent.")
    except Exception as e:  # noqa: BLE001 - mark failure is recoverable; double-email better than silent loss
        print(f"[digest] WARNING: send succeeded but mark_digest_sent failed: {e}")


if __name__ == "__main__":
    main()
