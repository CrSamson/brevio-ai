"""
tools/verify_topic_sources.py - Phase 1 of the multi-topic plan.

Probes the candidate RSS feeds for the four-topic expansion (Technology,
Business, Science, plus general-news for cross-source breadth). For each URL:
HTTP status, content-type, bozo flag, entry count, newest entry date, sample
title. Output is grouped by topic and ends with a per-topic + overall
pass-rate summary.

Acceptance bar (same as the original Phase 0 verifier):
    HTTP 200, bozo=0, >=1 entry, newest entry within 60 days.

Run:
    python tools/verify_topic_sources.py
"""
from __future__ import annotations

import sys
from collections import defaultdict
from dataclasses import dataclass
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


# Candidates: (source_id, url, topics).
# `topics` is the list of topic tags this source will carry once it lands in
# config/sources.json - the verifier doesn't enforce it, just records.
CANDIDATES: list[tuple[str, str, list[str]]] = [
    # --- technology ---
    ("wired",            "https://www.wired.com/feed/rss",                          ["technology"]),
    ("the_verge",        "https://www.theverge.com/rss/index.xml",                  ["technology"]),
    ("ars_technica",     "http://feeds.arstechnica.com/arstechnica/index",          ["technology"]),

    # --- business ---
    ("yahoo_finance",    "https://finance.yahoo.com/news/rssindex",                 ["business"]),
    ("cnbc",             "https://www.cnbc.com/id/100003114/device/rss/rss.html",   ["business"]),
    ("benzinga",         "https://www.benzinga.com/feed",                           ["business"]),
    ("insider_monkey",   "https://www.insidermonkey.com/blog/feed/",                ["business"]),
    ("forbes_business",  "https://www.forbes.com/business/feed/",                   ["business"]),

    # --- science ---
    ("phys_org",         "https://phys.org/rss-feed/",                              ["science"]),
    ("sciencedaily",     "https://www.sciencedaily.com/rss/all.xml",                ["science"]),
    ("quanta",           "https://api.quantamagazine.org/feed/",                    ["science"]),
    ("nature",           "https://www.nature.com/nature.rss",                       ["science"]),
    ("mit_news",         "https://news.mit.edu/rss/feed",                           ["science"]),
    ("air_space_forces", "https://www.airandspaceforces.com/feed/",                 ["science", "technology"]),

    # --- general (Canadian / European, multi-topic) ---
    ("bbc_news",         "http://feeds.bbci.co.uk/news/rss.xml",                    ["technology", "business", "science"]),
    ("the_independent",  "https://www.independent.co.uk/news/rss",                  ["technology", "business", "science"]),
    ("cbc_news",         "https://rss.cbc.ca/lineup/topstories.xml",                ["technology", "business", "science"]),
    ("cna",              "https://www.channelnewsasia.com/rssfeeds/8395986",        ["technology", "business", "science"]),
    ("le_monde",         "https://www.lemonde.fr/rss/une.xml",                      ["technology", "business", "science"]),
]


@dataclass
class FeedReport:
    source_id: str
    url: str
    topics: list[str]
    status: Optional[int] = None
    content_type: str = ""
    bozo: bool = False
    bozo_msg: str = ""
    entries: int = 0
    newest_published: Optional[datetime] = None
    sample_title: str = ""
    error: str = ""

    @property
    def passes(self) -> bool:
        """HTTP 200, no bozo, >=1 entry, newest entry within 60 days."""
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


def verify(source_id: str, url: str, topics: list[str], timeout: int = 15) -> FeedReport:
    report = FeedReport(source_id=source_id, url=url, topics=topics)
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


def _topic_label(report: FeedReport) -> str:
    """Primary topic (for grouping) - first topic in the list."""
    return report.topics[0] if report.topics else "uncategorized"


def render_grouped_tables(reports: list[FeedReport]) -> str:
    """Group reports by primary topic and render one table per topic."""
    by_topic: dict[str, list[FeedReport]] = defaultdict(list)
    for r in reports:
        by_topic[_topic_label(r)].append(r)

    out: list[str] = []
    for topic in ("technology", "business", "science", "general", "uncategorized"):
        if topic not in by_topic:
            continue
        topic_reports = by_topic[topic]
        passing = sum(1 for r in topic_reports if r.passes)
        total   = len(topic_reports)
        out.append(f"### {topic} ({passing}/{total} pass)")
        out.append("")
        out.append("| Source ID | Status | Content-Type | Bozo | Entries | Newest | Topics | Sample |")
        out.append("|---|---|---|---|---|---|---|---|")
        for r in topic_reports:
            status   = str(r.status) if r.status is not None else "ERR"
            ctype    = r.content_type or "-"
            bozo     = "1" if r.bozo else "0"
            newest   = r.newest_published.strftime("%Y-%m-%d") if r.newest_published else "-"
            topics_s = ",".join(r.topics)
            sample   = _truncate(r.sample_title or r.error, 50)
            out.append(
                f"| `{r.source_id}` | {status} | {ctype} | {bozo} | "
                f"{r.entries} | {newest} | {topics_s} | {sample} |"
            )
        out.append("")
    return "\n".join(out)


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass

    print("# Phase 1 - candidate RSS feed verification")
    print(f"_Generated: {datetime.now(timezone.utc).isoformat()}_\n")
    print(f"Probing {len(CANDIDATES)} candidates...\n", file=sys.stderr)

    reports = [verify(sid, url, topics) for sid, url, topics in CANDIDATES]

    # Group "general" sources together for display (those with 3+ topics).
    # Their primary topic is the first listed but we want to group them as
    # general-news rather than under tech.
    for r in reports:
        if len(r.topics) >= 3:
            r.topics = ["general"] + [t for t in r.topics if t != "general"]

    print(render_grouped_tables(reports))

    # Summary
    passing = [r for r in reports if r.passes]
    failing = [r for r in reports if not r.passes]
    pass_rate = len(passing) / len(reports) if reports else 0.0

    print("## Acceptance gate\n")
    print(f"- Candidates passing: **{len(passing)}/{len(reports)}** ({pass_rate*100:.0f}%)")
    print(f"- Plan requires **>=70%** -> {'PASS' if pass_rate >= 0.70 else 'STOP'}\n")

    if failing:
        print("**Failing candidates (do not add to sources.json):**\n")
        for r in failing:
            print(f"- `{r.source_id}` ({r.url}) - {r.fail_reason()}")
        print()

    return 0 if pass_rate >= 0.70 else 1


if __name__ == "__main__":
    sys.exit(main())
