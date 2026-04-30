"""
tools/verify_feeds.py - Phase 0 pre-flight check for the multi-source plan.

For each candidate feed URL, fetches the resource via requests, parses it
with feedparser, and reports:
    HTTP status, content-type, bozo flag, entry count, newest entry
    publication date, and a sample title.

Output is markdown to stdout, intended for the Phase 0 checkpoint.

Run:
    python tools/verify_feeds.py
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import feedparser
import requests


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "application/rss+xml, application/atom+xml, application/xml, "
        "text/xml, */*"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


# Section 4.1 - already-verified set in the plan.
VERIFIED: list[tuple[str, str]] = [
    ("anthropic_news",
     "https://raw.githubusercontent.com/Olshansk/rss-feeds/main/feeds/feed_anthropic_news.xml"),
    ("anthropic_research",
     "https://raw.githubusercontent.com/Olshansk/rss-feeds/main/feeds/feed_anthropic_research.xml"),
    ("anthropic_engineering",
     "https://raw.githubusercontent.com/Olshansk/rss-feeds/main/feeds/feed_anthropic_engineering.xml"),
    ("openai_news",
     "https://openai.com/news/rss.xml"),
    ("arxiv_cs_lg_ai",
     "http://export.arxiv.org/api/query?"
     "search_query=cat:cs.LG+OR+cat:cs.AI"
     "&max_results=20&sortBy=submittedDate&sortOrder=descending"),
    ("hf_daily_papers",
     "https://papers.takara.ai/api/feed"),
]

# Section 4.2 - candidates to verify before adding.
CANDIDATES: list[tuple[str, str]] = [
    ("google_research",
     "https://research.google/blog/rss/"),
    ("meta_ai",
     "https://ai.meta.com/blog/rss/"),
    ("aws_ml",
     "https://aws.amazon.com/blogs/machine-learning/feed/"),
    ("nvidia_developer",
     "https://developer.nvidia.com/blog/feed/"),
    ("bair",
     "https://bair.berkeley.edu/blog/feed.xml"),
    ("cmu_ml",
     "https://blog.ml.cmu.edu/feed/"),
    ("techcrunch_ai",
     "https://techcrunch.com/category/artificial-intelligence/feed/"),
    ("venturebeat_ai",
     "https://venturebeat.com/category/ai/feed/"),
    ("mit_news_ai",
     "https://news.mit.edu/topic/artificial-intelligence2-rss.xml"),
    ("hf_daily_fallback",
     "https://raw.githubusercontent.com/huangboming/huggingface-daily-paper-feed/main/feed.xml"),
]


@dataclass
class FeedReport:
    source_id: str
    url: str
    status: Optional[int] = None
    content_type: str = ""
    bozo: bool = False
    bozo_msg: str = ""
    entries: int = 0
    newest_published: Optional[datetime] = None
    sample_title: str = ""
    error: str = ""

    @property
    def passes_phase_0(self) -> bool:
        """Phase 0 acceptance: HTTP 200, no bozo, >=1 entry within 60 days."""
        if self.status != 200:
            return False
        if self.bozo:
            return False
        if self.entries < 1:
            return False
        if self.newest_published is None:
            return False
        age_days = (datetime.now(timezone.utc) - self.newest_published).days
        if age_days > 60:
            return False
        return True

    def fail_reason(self) -> str:
        if self.error:
            return self.error
        if self.status != 200:
            return f"HTTP {self.status}"
        if self.bozo:
            return f"bozo=1 ({self.bozo_msg or 'parser warning'})"
        if self.entries < 1:
            return "0 entries"
        if self.newest_published is None:
            return "no parseable dates"
        age_days = (datetime.now(timezone.utc) - self.newest_published).days
        if age_days > 60:
            return f"stale (newest {age_days}d old)"
        return "ok"


def _newest(parsed) -> tuple[Optional[datetime], str]:
    newest: Optional[datetime] = None
    sample = ""
    for entry in parsed.entries:
        title = (entry.get("title", "") or "").strip()
        for attr in ("published_parsed", "updated_parsed"):
            ts = getattr(entry, attr, None)
            if ts:
                dt = datetime(*ts[:6], tzinfo=timezone.utc)
                if newest is None or dt > newest:
                    newest = dt
                    sample = title
                break
        else:
            if not sample:
                sample = title
    if not sample and parsed.entries:
        sample = (parsed.entries[0].get("title", "") or "").strip()
    return newest, sample


def verify(source_id: str, url: str, timeout: int = 15) -> FeedReport:
    report = FeedReport(source_id=source_id, url=url)
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
    except requests.RequestException as e:
        report.error = f"{type(e).__name__}: {e}"
        return report

    report.status = r.status_code
    report.content_type = r.headers.get("Content-Type", "").split(";")[0].strip()

    if r.status_code != 200:
        return report

    parsed = feedparser.parse(r.content)
    report.bozo = bool(getattr(parsed, "bozo", False))
    bozo_exc = getattr(parsed, "bozo_exception", None)
    report.bozo_msg = str(bozo_exc) if bozo_exc else ""
    report.entries = len(parsed.entries)
    report.newest_published, report.sample_title = _newest(parsed)
    return report


def _truncate(s: str, n: int) -> str:
    s = (s or "").replace("|", "\\|").replace("\n", " ").strip()
    return s if len(s) <= n else s[: n - 1] + "..."


def render_table(label: str, reports: list[FeedReport]) -> str:
    out: list[str] = [f"## {label}", ""]
    out.append("| ID | Status | Content-Type | Bozo | Entries | Newest | Sample title |")
    out.append("|---|---|---|---|---|---|---|")
    for r in reports:
        status = str(r.status) if r.status is not None else "ERR"
        ctype = r.content_type or "-"
        bozo = "1" if r.bozo else "0"
        if r.newest_published:
            newest = r.newest_published.strftime("%Y-%m-%d")
        else:
            newest = "-"
        title = _truncate(r.sample_title or r.error, 70)
        out.append(
            f"| `{r.source_id}` | {status} | {ctype} | {bozo} | "
            f"{r.entries} | {newest} | {title} |"
        )
    return "\n".join(out)


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass

    print("# Phase 0 - feed verification report")
    print(f"_Generated: {datetime.now(timezone.utc).isoformat()}_\n")

    print("Probing 4.1 verified set...", file=sys.stderr)
    verified = [verify(sid, url) for sid, url in VERIFIED]
    print("Probing 4.2 candidates...", file=sys.stderr)
    candidates = [verify(sid, url) for sid, url in CANDIDATES]

    print(render_table("4.1 - already-verified set", verified))
    print()
    print(render_table("4.2 - candidates", candidates))
    print()

    cand_pass = [r for r in candidates if r.passes_phase_0]
    cand_fail = [r for r in candidates if not r.passes_phase_0]
    ver_fail  = [r for r in verified if not r.passes_phase_0]

    print("## Acceptance gate\n")
    print(f"- 4.2 candidates passing Phase 0: **{len(cand_pass)}/{len(candidates)}** "
          f"(plan requires >= 7).\n")

    if cand_fail:
        print("**4.2 candidates failing Phase 0 (move to skip list):**\n")
        for r in cand_fail:
            print(f"- `{r.source_id}` ({r.url}) - {r.fail_reason()}")
        print()

    if ver_fail:
        print("**4.1 verified set entries that failed (review manually):**\n")
        for r in ver_fail:
            print(f"- `{r.source_id}` ({r.url}) - {r.fail_reason()}")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
