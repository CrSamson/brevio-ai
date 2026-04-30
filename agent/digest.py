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
    get_recent_summarized_anthropic_articles,
    get_recent_summarized_youtube_videos,
)
from app.database.db import get_db
from app.database.models import AnthropicArticle, YoutubeVideo
from scrapers.anthropic_scrapper import clean_anthropic_title


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


def _anthropic_meta(article: AnthropicArticle) -> str:
    parts = [article.published_at.strftime("%Y-%m-%d")]
    if article.category:
        parts.append(article.category)
    return " · ".join(parts)


def _youtube_meta(video: YoutubeVideo) -> str:
    parts = [video.published_at.strftime("%Y-%m-%d")]
    if video.channel_handle:
        parts.append(video.channel_handle)
    return " · ".join(parts)


def _youtube_thumbnail(video_id: str) -> str:
    """`hqdefault.jpg` always exists for any video and is 480x360 — safe default."""
    return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"


def _card_html(*, url: str, title: str, meta: str, summary: str,
               thumbnail: str | None, cta: str) -> str:
    """One article/video card. No leading whitespace inside <a> tags — Gmail
    preserves it as a leading space before the link text."""
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

    return (
        f'<div style="margin:0 0 36px;padding:0 0 28px;border-bottom:1px solid #eee;">'
        f'{img}'
        f'<h3 style="font-size:18px;font-weight:700;line-height:1.3;margin:0 0 6px;">'
        f'<a href="{safe_url}" style="color:#0f172a;text-decoration:none;">{safe_title}</a>'
        f'</h3>'
        f'<div style="font-size:12px;color:#94a3b8;margin:0 0 10px;'
        f'text-transform:uppercase;letter-spacing:0.04em;">{safe_meta}</div>'
        f'<p style="font-size:15px;line-height:1.6;color:#334155;margin:0 0 12px;">'
        f'{safe_body}</p>'
        f'<a href="{safe_url}" style="font-size:13px;color:#cc785c;'
        f'text-decoration:none;font-weight:600;">{safe_cta}</a>'
        f'</div>'
    )


def render_html(
    *,
    hours: int,
    articles: list[AnthropicArticle],
    videos: list[YoutubeVideo],
) -> str:
    """Inline-styled HTML — no <style> blocks for max client compatibility."""

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

    article_cards = [
        _card_html(
            url=str(a.url),
            title=clean_anthropic_title(a.title),
            meta=_anthropic_meta(a),
            summary=_summary_to_paragraph(a.summary),
            thumbnail=None,
            cta="Read more →",
        )
        for a in articles
    ]
    video_cards = [
        _card_html(
            url=str(v.url),
            title=v.title,
            meta=_youtube_meta(v),
            summary=_summary_to_paragraph(v.summary),
            thumbnail=_youtube_thumbnail(v.video_id),
            cta="Watch on YouTube →",
        )
        for v in videos
    ]

    now = datetime.now(timezone.utc)

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
        f'margin:0 0 6px;">AI News Digest</h1>'
        f'<p style="color:#64748b;font-size:13px;margin:0 0 4px;'
        f'text-transform:uppercase;letter-spacing:0.1em;font-weight:600;">'
        f'{now.strftime("%A, %B %d, %Y")}'
        f'</p>'
        f'<p style="color:#94a3b8;font-size:13px;margin:0;">'
        f'{len(articles) + len(videos)} item(s) from the last {hours}h'
        f'</p>'
        f'{section_heading("Anthropic", len(articles))}'
        f'{section_body(article_cards)}'
        f'{section_heading("YouTube", len(videos))}'
        f'{section_body(video_cards)}'
        f'<p style="color:#cbd5e1;font-size:11px;margin-top:40px;'
        f'border-top:1px solid #e2e8f0;padding-top:16px;text-align:center;">'
        f'Generated {now.strftime("%Y-%m-%d %H:%M UTC")}'
        f'</p>'
        '</div>'
        '<div style="height:24px;background:#f1f5f9;"></div>'
        '</body></html>'
    )


def render_text(
    *,
    hours: int,
    articles: list[AnthropicArticle],
    videos: list[YoutubeVideo],
) -> str:
    """Plain-text fallback for clients that don't render HTML."""
    now = datetime.now(timezone.utc)
    lines: list[str] = [
        "AI NEWS DIGEST",
        f"Last {hours}h · {now.strftime('%Y-%m-%d')}",
        "",
    ]

    def section(title: str, rows: list) -> None:
        lines.append(f"== {title.upper()} ({len(rows)}) ==")
        if not rows:
            lines.extend(["  (nothing new)", ""])
            return
        for headline, url, meta, paragraph in rows:
            lines.append(headline)
            lines.append(meta)
            lines.append(paragraph or "(no summary)")
            lines.append(f"→ {url}")
            lines.append("")

    section("Anthropic", [
        (clean_anthropic_title(a.title), str(a.url), _anthropic_meta(a), _summary_to_paragraph(a.summary))
        for a in articles
    ])
    section("YouTube", [
        (v.title, str(v.url), _youtube_meta(v), _summary_to_paragraph(v.summary))
        for v in videos
    ])

    lines.append(f"-- generated {now.strftime('%Y-%m-%d %H:%M UTC')} --")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build_digest(hours: int) -> tuple[list[AnthropicArticle], list[YoutubeVideo]]:
    with get_db() as db:
        articles = get_recent_summarized_anthropic_articles(db, hours=hours)
        videos   = get_recent_summarized_youtube_videos(db, hours=hours)
        # Detach from session so callers can read attributes after the context exits.
        for obj in (*articles, *videos):
            db.expunge(obj)
    return articles, videos


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

    parser = argparse.ArgumentParser(description="Build and email the AI News digest.")
    parser.add_argument("--hours", type=int, default=24,
                        help="Lookback window in hours (default: 24).")
    parser.add_argument("--to", type=str, default=None,
                        help="Override DIGEST_TO recipient (comma-separated for multiple).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Render to stdout instead of sending.")
    args = parser.parse_args()

    articles, videos = build_digest(hours=args.hours)
    total = len(articles) + len(videos)
    print(f"[digest] {len(articles)} article(s), {len(videos)} video(s) in last {args.hours}h.")

    if total == 0:
        print("[digest] Nothing to send. Exiting.")
        return

    html_body = render_html(hours=args.hours, articles=articles, videos=videos)
    text_body = render_text(hours=args.hours, articles=articles, videos=videos)
    subject   = f"AI News Digest — {datetime.now(timezone.utc).strftime('%Y-%m-%d')} ({total} item{'s' if total != 1 else ''})"

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


if __name__ == "__main__":
    main()
