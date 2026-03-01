import re
import json
import requests
import feedparser
from datetime import datetime, timedelta, timezone
from typing import Iterable

from pydantic import BaseModel, AnyHttpUrl, field_validator
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api import (
    NoTranscriptFound,
    TranscriptsDisabled,
    VideoUnavailable,
    RequestBlocked,
    CouldNotRetrieveTranscript,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

YT_BASE_URL   = "https://www.youtube.com"
YT_RSS_URL    = f"{YT_BASE_URL}/feeds/videos.xml?channel_id={{channel_id}}"
CHANNEL_ID_RE = re.compile(r"UC[a-zA-Z0-9_-]{22}")

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

DEFAULT_LANGUAGES: tuple[str, ...] = ("en", "en-US", "en-GB")


# ---------------------------------------------------------------------------
# Pydantic model
# ---------------------------------------------------------------------------

class VideoMetadata(BaseModel):
    """Validated output schema for a single YouTube video."""

    title       : str
    video_id    : str
    url         : AnyHttpUrl
    published_at: datetime
    description : str

    @field_validator("video_id")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("video_id must not be empty")
        return v

    model_config = {"frozen": True}   # instances are immutable once created


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class YouTubeScraper:
    """
    Resolves channel handles to channel IDs, fetches recent video metadata
    via RSS, and retrieves transcripts — all without a YouTube API key.

    Thread-safety note: YouTubeTranscriptApi uses a requests.Session internally.
    Create one instance per thread when parallelising.
    """

    def __init__(self, timeout: int = 10,
                 languages: Iterable[str] = DEFAULT_LANGUAGES):
        self.timeout         = timeout
        self.languages       = tuple(languages)
        self._transcript_api = YouTubeTranscriptApi()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get_channel_id(self, channel_name: str) -> str | None:
        """Resolve any channel handle / URL to a UC… channel ID."""
        channel_name = channel_name.strip()

        if CHANNEL_ID_RE.fullmatch(channel_name):
            return channel_name

        url = self._build_channel_url(channel_name)

        try:
            r = requests.get(url, headers=REQUEST_HEADERS,
                             timeout=self.timeout, allow_redirects=True)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"[get_channel_id] Network error: {e}")
            return None

        return self._extract_channel_id(r.text, url)

    def get_latest_videos(self, channel_id: str,
                          hours: int = 24) -> list[dict]:
        """
        Return validated video metadata for videos published within `hours` hours.
        YouTube Shorts are automatically filtered out.

        Returns:
            List of dicts with keys: title, video_id, url, published_at, description
        """
        feed   = feedparser.parse(YT_RSS_URL.format(channel_id=channel_id))
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        videos = []

        for entry in feed.entries:
            published = self._parse_date(entry)
            if published is None or published < cutoff:
                continue

            video_id = entry.get("yt_videoid", "")

            # Skip YouTube Shorts
            if self._is_short(video_id):
                print(f"        [SKIP] Short detected: {entry.get('title', video_id)}")
                continue

            metadata = VideoMetadata(
                title        = entry.get("title", ""),
                video_id     = video_id,
                url          = f"{YT_BASE_URL}/watch?v={video_id}",
                published_at = published,
                description  = self._parse_description(entry),
            )

            # model_dump(mode="json") serializes datetime → ISO string, url → string
            videos.append(metadata.model_dump(mode="json"))

        videos.sort(key=lambda v: v["published_at"], reverse=True)
        return videos

    def get_transcript(self, video_id: str) -> str | None:
        """
        Fetch and return the full transcript text for a video.

        Strategy:
          1. List all available transcripts for the video.
          2. Prefer manually created over auto-generated for `self.languages`.
          3. Fall back to any available transcript if none match the language list.
          4. Join all snippet texts into a single string.
        """
        try:
            transcript_list = self._transcript_api.list(video_id)

            try:
                transcript = transcript_list.find_manually_created_transcript(self.languages)
            except NoTranscriptFound:
                try:
                    transcript = transcript_list.find_generated_transcript(self.languages)
                except NoTranscriptFound:
                    transcript = transcript_list.find_transcript(
                        [t.language_code for t in transcript_list]
                    )

            fetched = transcript.fetch()
            return " ".join(snippet.text for snippet in fetched)

        except TranscriptsDisabled:
            print(f"[get_transcript] Transcripts disabled for '{video_id}'.")
        except VideoUnavailable:
            print(f"[get_transcript] Video '{video_id}' is unavailable.")
        except RequestBlocked:
            print(f"[get_transcript] Request blocked by YouTube for '{video_id}'. "
                  "Consider using a ProxyConfig.")
        except CouldNotRetrieveTranscript as e:
            print(f"[get_transcript] Could not retrieve transcript for '{video_id}': {e}")

        return None

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _is_short(self, video_id: str) -> bool:
        """
        Detect whether a video is a YouTube Short.

        Makes a HEAD request to youtube.com/shorts/{video_id}.
        If YouTube returns 200 (no redirect away), it's a Short.
        If it redirects to /watch?v=…, it's a regular video.
        """
        try:
            r = requests.head(
                f"{YT_BASE_URL}/shorts/{video_id}",
                headers=REQUEST_HEADERS,
                timeout=self.timeout,
                allow_redirects=True,
            )
            # After redirects, if the final URL still contains /shorts/, it's a Short
            return "/shorts/" in r.url
        except requests.exceptions.RequestException:
            return False  # on error, assume it's not a Short

    @staticmethod
    def _build_channel_url(name: str) -> str:
        if name.startswith("http"):
            return name
        if name.startswith(("@", "c/", "user/", "channel/")):
            return f"{YT_BASE_URL}/{name}"
        return f"{YT_BASE_URL}/@{name}"

    @staticmethod
    def _extract_channel_id(html: str, url: str) -> str | None:
        strategies = [
            r'<meta\s+itemprop="identifier"\s+content="(UC[a-zA-Z0-9_-]{22})"',
            r'"externalId"\s*:\s*"(UC[a-zA-Z0-9_-]{22})"',
            r'<link\s+rel="canonical"\s+href="[^"]+/channel/(UC[a-zA-Z0-9_-]{22})"',
            r'feeds/videos\.xml\?channel_id=(UC[a-zA-Z0-9_-]{22})',
        ]
        for pattern in strategies:
            m = re.search(pattern, html)
            if m:
                return m.group(1)

        header = re.search(r'"header"\s*:\s*\{(.{0,2000})', html, re.DOTALL)
        if header:
            m = re.search(r'"channelId"\s*:\s*"(UC[a-zA-Z0-9_-]{22})"', header.group(1))
            if m:
                return m.group(1)

        print(f"[get_channel_id] Could not extract channel ID from {url}")
        return None

    @staticmethod
    def _parse_date(entry) -> datetime | None:
        for attr in ("published_parsed", "updated_parsed"):
            ts = getattr(entry, attr, None)
            if ts:
                return datetime(*ts[:6], tzinfo=timezone.utc)
        return None

    @staticmethod
    def _parse_description(entry) -> str:
        for attr in ("media_description", "summary"):
            value = getattr(entry, attr, None)
            if value:
                return value
        return ""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    scraper    = YouTubeScraper()
    channel_id = scraper.get_channel_id("@Fireship")
    videos     = scraper.get_latest_videos(channel_id, hours=168)

    print(json.dumps(videos, indent=2))

    if videos:
        first_id   = videos[0]["video_id"]
        transcript = scraper.get_transcript(first_id)
        if transcript:
            print(f"\nTranscript preview ({len(transcript)} chars):")
            print(transcript[:300] + "...")