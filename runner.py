"""
runner.py — Orchestrates all scrapers and aggregates results from the past N hours.

Usage:
    from runner import Runner
    runner = Runner(hours=24)
    report = runner.run()
"""

import json
from pathlib import Path
from datetime import datetime, timezone

from scrapers import AnthropicScraper, YouTubeScraper
from app.database.db import get_db
from app.database.crud import upsert_anthropic_articles, upsert_youtube_videos


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

CONFIG_DIR   = Path(__file__).parent / "config"
CHANNELS_FILE = CONFIG_DIR / "channels.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_channels(path: Path = CHANNELS_FILE) -> list[str]:
    """Load the list of YouTube channel handles from the JSON config."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("youtube_channels", [])


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class Runner:
    """
    Runs every configured scraper for a given time window and returns
    a unified report dict.

    Report structure:
    {
        "generated_at": "...",
        "hours": 24,
        "anthropic": { "count": N, "articles": [...] },
        "youtube":   { "count": N, "videos":   [...] },
    }
    """

    def __init__(self, hours: int = 24, fetch_content: bool = False,
                 fetch_transcripts: bool = True) -> None:
        self.hours             = hours
        self.fetch_content     = fetch_content
        self.fetch_transcripts = fetch_transcripts

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def run(self) -> dict:
        """Execute all scrapers and return the aggregated report."""
        print(f"\n{'='*60}")
        print(f"  AI News Aggregator — last {self.hours}h")
        print(f"{'='*60}\n")

        anthropic_data = self._scrape_anthropic()
        youtube_data   = self._scrape_youtube()

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "hours":        self.hours,
            "anthropic":    anthropic_data,
            "youtube":      youtube_data,
        }

        self._save_to_db(report)
        self._print_summary(report)
        return report

    # ------------------------------------------------------------------
    # Anthropic
    # ------------------------------------------------------------------

    def _scrape_anthropic(self) -> dict:
        print("[1/2] Scraping Anthropic blogs …")
        scraper  = AnthropicScraper(hours=self.hours)
        articles = scraper.fetch_articles(with_content=self.fetch_content)
        print(f"      Found {len(articles)} article(s).\n")
        return {"count": len(articles), "articles": articles}

    # ------------------------------------------------------------------
    # YouTube
    # ------------------------------------------------------------------

    def _scrape_youtube(self) -> dict:
        print("[2/2] Scraping YouTube channels …")
        channels = load_channels()
        scraper  = YouTubeScraper()

        all_videos: list[dict] = []

        for handle in channels:
            print(f"      → Resolving {handle} …", end=" ")
            channel_id = scraper.get_channel_id(handle)

            if channel_id is None:
                print("SKIP (could not resolve)")
                continue

            videos = scraper.get_latest_videos(channel_id, hours=self.hours)
            print(f"{len(videos)} video(s)")

            for video in videos:
                video["channel"] = handle        # tag with source channel

                if self.fetch_transcripts:
                    transcript = scraper.get_transcript(video["video_id"])
                    video["transcript"] = transcript or ""
                else:
                    video["transcript"] = ""

            all_videos.extend(videos)

        # Sort all videos by publish date (newest first)
        all_videos.sort(key=lambda v: v["published_at"], reverse=True)
        print(f"      Total: {len(all_videos)} video(s).\n")
        return {"count": len(all_videos), "videos": all_videos}

    # ------------------------------------------------------------------
    # Database persistence
    # ------------------------------------------------------------------

    @staticmethod
    def _save_to_db(report: dict) -> None:
        """Upsert all scraped data into PostgreSQL."""
        articles = report["anthropic"]["articles"]
        videos   = report["youtube"]["videos"]

        with get_db() as db:
            if articles:
                saved_articles = upsert_anthropic_articles(db, articles)
                print(f"[DB] Upserted {len(saved_articles)} Anthropic article(s).")
            else:
                print("[DB] No Anthropic articles to save.")

            if videos:
                saved_videos = upsert_youtube_videos(db, videos)
                print(f"[DB] Upserted {len(saved_videos)} YouTube video(s).")
            else:
                print("[DB] No YouTube videos to save.")

        print()

    # ------------------------------------------------------------------
    # Pretty-print
    # ------------------------------------------------------------------

    @staticmethod
    def _print_summary(report: dict) -> None:
        print(f"{'='*60}")
        print("  SUMMARY")
        print(f"{'='*60}\n")

        # Anthropic
        articles = report["anthropic"]["articles"]
        print(f"  Anthropic articles: {len(articles)}")
        for a in articles:
            print(f"    • {a['published_at']}  {a['title']}")
            print(f"      {a['url']}")

        # YouTube
        videos = report["youtube"]["videos"]
        print(f"\n  YouTube videos: {len(videos)}")
        for v in videos:
            has_transcript = "✓" if v.get("transcript") else "✗"
            print(f"    • [{has_transcript}] {v['published_at']}  {v['title']}")
            print(f"          {v['url']}  ({v.get('channel', '?')})")

        print(f"\n{'='*60}\n")
