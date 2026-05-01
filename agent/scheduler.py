"""
agent/scheduler.py — Daily pipeline driver.

Runs the full pipeline once per day at a configurable local time:

    1. Scrape RSS sources (Anthropic blogs + YouTube channels)
    2. Summarize any rows still missing a summary
    3. Build and email the digest

Run modes:

    python -m agent.scheduler                # block forever, fire daily
    python -m agent.scheduler --run-now      # run pipeline once, then start scheduler
    python -m agent.scheduler --once         # run pipeline once, exit (cron-style)
    python -m agent.scheduler --skip-email   # run scrape + summarize, skip digest

Configuration (in .env):

    SCHEDULE_HOUR     hour of day to fire, local time   (default: 7)
    SCHEDULE_MINUTE   minute of the hour                (default: 0)
    SCHEDULE_HOURS_LOOKBACK  scrape + digest window     (default: 24)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

from agent.digest import build_digest, cap_balanced, render_html, render_text, send_email
from agent.summarizer import Summarizer
from app.database.crud import (
    get_unsummarized_articles,
    get_unsummarized_papers,
    get_unsummarized_youtube_videos,
    mark_digest_sent,
    set_article_summary,
    set_paper_summary,
    set_youtube_summary,
)
from app.database.db import get_db
from app.database.models import Article, Paper, YoutubeVideo
from runner import Runner


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_PROJECT_ROOT / ".env", override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("scheduler")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def _scrape(hours: int) -> None:
    log.info("step 1/3 — scraping (lookback=%dh)", hours)
    # Per-source content fetching is now configured in config/sources.json,
    # not via the Runner constructor.
    Runner(hours=hours, fetch_transcripts=True).run()


def _summarize() -> None:
    log.info("step 2/3 — summarizing unsummarized rows")
    summarizer = Summarizer()
    with get_db() as db:
        articles = get_unsummarized_articles(db)
        papers   = get_unsummarized_papers(db)
        videos   = get_unsummarized_youtube_videos(db)
        log.info("  %d article(s), %d paper(s), %d video(s) to summarize",
                 len(articles), len(papers), len(videos))

        for a in articles:
            try:
                set_article_summary(db, a.id, summarizer.summarize_article(a))
            except Exception as e:  # noqa: BLE001 — keep batch going
                log.warning("    article id=%s failed: %s", a.id, e)

        for p in papers:
            try:
                set_paper_summary(db, p.id, summarizer.summarize_paper(p))
            except Exception as e:  # noqa: BLE001
                log.warning("    paper id=%s failed: %s", p.id, e)

        for v in videos:
            try:
                set_youtube_summary(db, v.id, summarizer.summarize_youtube_video(v))
            except Exception as e:  # noqa: BLE001
                log.warning("    video id=%s failed: %s", v.id, e)


def _email_digest(hours: int, max_items: int | None = None) -> None:
    log.info("step 3/3 — building + sending digest (window=%dh, max_items=%s)",
             hours, max_items)
    articles, papers, videos = build_digest(hours=hours)
    pre_total = len(articles) + len(papers) + len(videos)
    if max_items:
        articles, papers, videos = cap_balanced(
            articles, papers, videos, max_items=max_items,
        )
    total = len(articles) + len(papers) + len(videos)
    if max_items and total < pre_total:
        log.info("  capped from %d to %d items (max_items=%d)",
                 pre_total, total, max_items)
    if total == 0:
        log.info("  nothing to send for the last %dh", hours)
        return

    recipients_raw = os.environ.get("DIGEST_TO")
    if not recipients_raw:
        log.warning("  DIGEST_TO not set — skipping send (add it to .env)")
        return
    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]

    for var in ("SMTP_USER", "SMTP_PASSWORD"):
        if var not in os.environ:
            log.warning("  %s not set — skipping send", var)
            return

    subject = (
        f"AI News Digest — {datetime.now(timezone.utc).strftime('%Y-%m-%d')} "
        f"({total} item{'s' if total != 1 else ''})"
    )
    send_email(
        subject=subject,
        text_body=render_text(hours=hours, articles=articles, papers=papers, videos=videos),
        html_body=render_html(hours=hours, articles=articles, papers=papers, videos=videos),
        recipients=recipients,
    )
    log.info("  sent to %s", ", ".join(recipients))

    # Mark every row that was actually emailed so it never ships twice. We
    # mark AFTER the send so an SMTP failure leaves rows unsent for retry.
    try:
        with get_db() as db:
            n_a = mark_digest_sent(db, Article,      [a.id for a in articles])
            n_p = mark_digest_sent(db, Paper,        [p.id for p in papers])
            n_v = mark_digest_sent(db, YoutubeVideo, [v.id for v in videos])
        log.info("  marked sent: %d article(s), %d paper(s), %d video(s)", n_a, n_p, n_v)
    except Exception as e:  # noqa: BLE001 - mark failure is recoverable
        log.warning("  send succeeded but mark_digest_sent failed: %s", e)


def run_pipeline(
    *,
    hours: int,
    send_email_step: bool = True,
    max_items: int | None = None,
) -> None:
    """Full daily pipeline. Each step is wrapped so a failure doesn't abort the rest."""
    log.info("=" * 60)
    log.info("pipeline start")
    started = datetime.now(timezone.utc)

    for label, fn in (
        ("scrape",    lambda: _scrape(hours)),
        ("summarize", _summarize),
        ("digest",    (
            (lambda: _email_digest(hours, max_items=max_items))
            if send_email_step
            else (lambda: log.info("step 3/3 — skipped (--skip-email)"))
        )),
    ):
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            log.exception("step '%s' raised: %s", label, e)

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    log.info("pipeline done in %.1fs", elapsed)
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

def _start_scheduler(
    *,
    hours: int,
    send_email_step: bool,
    max_items: int | None,
) -> None:
    sched_hour   = int(os.environ.get("SCHEDULE_HOUR", "7"))
    sched_minute = int(os.environ.get("SCHEDULE_MINUTE", "0"))

    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_pipeline,
        trigger=CronTrigger(hour=sched_hour, minute=sched_minute),
        kwargs={
            "hours":           hours,
            "send_email_step": send_email_step,
            "max_items":       max_items,
        },
        id="daily_digest",
        max_instances=1,
        coalesce=True,         # if missed (e.g. machine asleep), run once on resume — not N times
        misfire_grace_time=60 * 60,  # tolerate up to 1h late
    )

    log.info(
        "scheduler armed: daily at %02d:%02d (local time, lookback=%dh, email=%s, max_items=%s)",
        sched_hour, sched_minute, hours, "on" if send_email_step else "off", max_items,
    )
    log.info("Ctrl+C to stop.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("scheduler stopped.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Daily pipeline driver.")
    parser.add_argument("--once", action="store_true",
                        help="Run the pipeline once and exit (no scheduler).")
    parser.add_argument("--run-now", action="store_true",
                        help="Run the pipeline once immediately, then start the daily scheduler.")
    parser.add_argument("--skip-email", action="store_true",
                        help="Run scrape + summarize but do not send the digest.")
    parser.add_argument("--hours", type=int, default=None,
                        help="Override SCHEDULE_HOURS_LOOKBACK (default: env or 24).")
    parser.add_argument("--max-items", type=int, default=None,
                        help="Cap total items in the digest with a balanced "
                             "per-section quota (40%% articles / 40%% papers / "
                             "20%% videos) and per-source diversity (max 2 "
                             "items per source/channel within a section). "
                             "Default: env DIGEST_MAX_ITEMS, or unlimited.")
    args = parser.parse_args()

    hours = args.hours or int(os.environ.get("SCHEDULE_HOURS_LOOKBACK", "24"))
    send_email_step = not args.skip_email
    max_items = args.max_items
    if max_items is None and os.environ.get("DIGEST_MAX_ITEMS"):
        max_items = int(os.environ["DIGEST_MAX_ITEMS"])

    if args.once:
        run_pipeline(hours=hours, send_email_step=send_email_step, max_items=max_items)
        return

    if args.run_now:
        run_pipeline(hours=hours, send_email_step=send_email_step, max_items=max_items)

    _start_scheduler(hours=hours, send_email_step=send_email_step, max_items=max_items)


if __name__ == "__main__":
    sys.exit(main())
