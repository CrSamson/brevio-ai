# AI News Aggregator

> Pulls fresh AI news from **10 RSS blog sources**, **arXiv (cs.LG + cs.AI)**, **HuggingFace Daily Papers**, and **4 YouTube channels**. Normalises everything into Postgres, summarises each item with an LLM, and emails a daily HTML digest with three sections: Articles, Papers, YouTube.

## 🎯 Objective

Keep up with AI without ad-hoc tab-checking. Every run, the aggregator fetches what was published in the last *N* hours from a configurable list of trusted sources, normalises it into Postgres, summarises each row with GPT-4o, and ships a single HTML email so the reader can decide in seconds what to click.

The summariser uses **two system prompts** depending on source kind:
- **Articles + video transcripts** → busy-practitioner blurb, 2–4 sentences, specifics over generalities.
- **Papers** → plain-English explainer for a general audience, 3–5 sentences, jargon defined inline only when load-bearing.

The pipeline is end-to-end. It is intentionally **single-user**: one recipient, one channel/source list, one machine running the daily cron.

Sources are managed by config files, not code:
- Blog and paper sources: [config/sources.json](config/sources.json)
- YouTube channels: [config/channels.json](config/channels.json)

Adding a new RSS blog source is a JSON edit, no scraper to write.

## 🏗️ Architecture

*Four scrapers run for every window. Blog and paper sources are driven entirely by `sources.json`. YouTube reads from a flat channel list. All four feed Postgres via idempotent upserts. A summariser fills empty `summary` columns with GPT-4o output (kind-specific prompt). The digest builder turns recently-summarised rows into a three-section HTML email. APScheduler chains everything once a day.*

```mermaid
flowchart LR
    subgraph Config["config/"]
        SJ[sources.json<br/>blogs + papers]
        CJ[channels.json<br/>YouTube handles]
    end

    SJ --> R1[RssBlogScraper<br/>10 RSS feeds]
    SJ --> R2[ArxivScraper<br/>Atom API, no PDFs]
    SJ --> R3[HfDailyScraper<br/>primary + fallback]
    CJ --> R4[YouTubeScraper<br/>RSS + transcripts]

    R1 --> U1[upsert_articles]
    R2 --> U2[upsert_papers]
    R3 --> U3[merge_hf_daily_papers<br/>array union on sources]
    R4 --> U4[upsert_youtube_videos]

    U1 --> DB[(PostgreSQL<br/>articles · papers · youtube_videos)]
    U2 --> DB
    U3 --> DB
    U4 --> DB

    DB --> SUM[Summarizer<br/>OpenAI gpt-4o<br/>article + paper prompts]
    SUM --> DB
    DB --> DIG[Digest builder<br/>3-section HTML + plain text]
    DIG --> EM[SMTP / STARTTLS]

    SCHED[APScheduler<br/>daily cron] -.drives.-> R1
    SCHED -.drives.-> SUM
    SCHED -.drives.-> DIG
```

## 🛠️ Tech Stack

- **Language**: Python 3.10+
- **RSS / Atom parsing**: `feedparser`
- **HTTP**: `requests`
- **Article content extraction (optional, per-source)**: [Docling](https://github.com/docling-project/docling) `DocumentConverter` — URL → markdown
- **YouTube transcripts**: [`youtube-transcript-api`](https://github.com/jdepoix/youtube-transcript-api) — no Google API key required
- **Validation**: Pydantic v2 (`BlogArticle`, `Paper`, `VideoMetadata`)
- **Database**: PostgreSQL 16 + SQLAlchemy 2.x ORM
  - Idempotent upserts via `INSERT … ON CONFLICT DO UPDATE`
  - Insert-vs-update distinguished via Postgres' `xmax = 0` trick
  - `papers.arxiv_id` is a **partial unique index** (`WHERE arxiv_id IS NOT NULL`)
  - `papers.sources` is a `TEXT[]` array; cross-linking uses `ARRAY(SELECT DISTINCT unnest(...))` for deduped union
  - `summary` columns are deliberately omitted from upsert SET clauses so re-scrapes never overwrite LLM output
- **arXiv**: Atom API (`export.arxiv.org/api/query`), with a process-global ≥ 3 s rate limiter; **PDFs are never downloaded**, only `pdf_url` strings
- **LLM summarisation**: OpenAI `gpt-4o`, kind-specific system prompts
- **Email delivery**: stdlib `smtplib` + `EmailMessage` (multipart text + inline-styled HTML)
- **Scheduling**: APScheduler `BlockingScheduler` with a daily `CronTrigger`

## 📊 What it does today

### Sources

10 enabled blog sources in [config/sources.json](config/sources.json):
`anthropic_news`, `anthropic_research`, `anthropic_engineering`, `openai_news`, `google_research`, `aws_ml`, `nvidia_developer`, `bair`, `cmu_ml`, `techcrunch_ai`.

2 paper sources:
`arxiv_cs_lg_ai` (Atom API, `cs.LG ∪ cs.AI`, `max_results=10`), `hf_daily_papers` (GitHub mirror primary so `hf_upvotes` populates routinely; takara.ai as fallback).

4 YouTube channels in [config/channels.json](config/channels.json).

### Tables

Three tables, all in PostgreSQL:

- **`articles`** — every blog/news post. Conflict key: `url`. Per-row: `source` (e.g. `anthropic_news`, `openai_news`), `title`, `published_at`, `summary` (LLM, busy-practitioner tone), `content_md` (Docling, optional), `raw_metadata` (JSONB).
- **`papers`** — arXiv + HF Daily entries, cross-linked. Conflict key: `arxiv_id` (partial unique). Per-row: `sources TEXT[]` (e.g. `{arxiv,hf_daily}`), `title`, `authors` (JSONB), `abstract`, `categories` (JSONB), `pdf_url`, `hf_upvotes`, `summary` (LLM, plain-English explainer for a general audience).
- **`youtube_videos`** — YouTube video metadata + transcript + LLM `summary`. Conflict key: `video_id`.

### Resilience

- Per-entry try/except — one bad RSS row doesn't drop the others.
- Per-source try/except — one dead feed doesn't abort the run.
- HF Daily auto-falls-back to the alternate feed when the primary fails.
- arXiv volume gate: a single fetch returning >500 entries (likely misconfiguration) is logged and dropped.
- Idempotency: running twice in a row yields 0 net new inserts on the second run.

## 📁 Repository Structure

```
brevio-ai/
├── main.py                          # Entry point - one-shot scrape across all sources
├── runner.py                        # Orchestrates 3 scrape steps + per-source reports
├── scrapers/
│   ├── base.py                      # BaseScraper ABC
│   ├── schemas.py                   # Pydantic v2: BlogArticle, Paper
│   ├── rss_blog_scraper.py          # Generic RSS scraper, drives off sources.json
│   ├── arxiv_scraper.py             # Atom API, rate-limited, no PDFs
│   ├── hf_daily_scraper.py          # Primary + fallback, dual-shape parser
│   └── youtube_scraper.py           # RSS + transcript + Shorts detection
├── agent/
│   ├── summarizer.py                # Dual-prompt OpenAI summariser
│   ├── digest.py                    # 3-section HTML + plain-text email, SMTP send
│   └── scheduler.py                 # Daily APScheduler driver
├── app/database/
│   ├── db.py                        # Engine + session factory
│   ├── models.py                    # SQLAlchemy: Article, Paper, YoutubeVideo
│   ├── crud.py                      # upsert_articles, upsert_papers, merge_hf_daily_papers, ...
│   └── create_tables.py             # Idempotent schema init + additive ALTERs
├── config/
│   ├── sources.json                 # blogs + papers config (drives the runner)
│   └── channels.json                # YouTube channel handles
├── tests/
│   ├── fixtures/                    # saved RSS / Atom snapshots
│   ├── test_schema.py               # DB schema smoke test
│   ├── test_rss_blog_scraper.py     # 4 tests
│   ├── test_arxiv_scraper.py        # 5 tests
│   └── test_hf_daily_scraper.py     # 5 tests, includes mocked HTTP fallback
├── tools/
│   ├── verify_feeds.py              # pre-flight feed verifier
│   ├── phase2_idempotency.py        # one-shot DB idempotency check (RSS)
│   ├── phase4_check.py              # arXiv live + idempotency
│   ├── phase5_check.py              # HF Daily live + cross-link
│   └── phase6_check.py              # E2E backtest with --truncate flag
├── Docker/docker-compose.yml        # Postgres 16 (app runs on host)
└── requirements.txt
```

## 🚀 How to Run

### 1. Start Postgres

```bash
cd Docker
docker compose up -d
```

### 2. Configure environment

Create `.env` at the project root:

```dotenv
# Database
DATABASE_URL=postgresql+psycopg2://USER:PASS@localhost:5433/DBNAME
POSTGRES_USER=...
POSTGRES_PASSWORD=...
POSTGRES_DB=...

# LLM summarisation
OPENAI_API_KEY=sk-...

# Email delivery (Gmail example - use an App Password)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=you@gmail.com
SMTP_PASSWORD=your-16-char-app-password
DIGEST_FROM=you@gmail.com           # optional
DIGEST_TO=you@gmail.com             # comma-separated for multiple

# Scheduler (local time)
SCHEDULE_HOUR=7
SCHEDULE_MINUTE=0
SCHEDULE_HOURS_LOOKBACK=24
```

### 3. Install + initialise schema

```bash
pip install -r requirements.txt
python -m app.database.create_tables
```

### 4. Run

```bash
# (a) Scrape every blog + paper + YouTube source once.
python main.py

# (b) Summarise rows still missing a summary. With no flag, all three run.
python -m agent.summarizer
python -m agent.summarizer --limit 5              # cap rows of each type (good for cheap tests)
python -m agent.summarizer --articles             # only blog/news articles
python -m agent.summarizer --papers               # only research papers (plain-English prompt)
python -m agent.summarizer --youtube              # only YouTube videos
python -m agent.summarizer --force --limit 3      # re-summarise existing rows (test prompt changes)

# (c) Build + send the digest.
python -m agent.digest
python -m agent.digest --hours 48
python -m agent.digest --dry-run
python -m agent.digest --to a@b.com

# (d) The full pipeline.
python -m agent.scheduler --once                  # scrape + summarise + email, exit
python -m agent.scheduler --run-now               # run immediately, then arm cron
python -m agent.scheduler                         # arm cron, block forever
python -m agent.scheduler --skip-email            # scrape + summarise only
```

### 5. Add or edit a source

To add a new RSS blog source, append to [config/sources.json](config/sources.json) under `blogs[]`:

```json
{
  "id":            "new_source_id",
  "name":          "Friendly name",
  "type":          "rss",
  "feed_url":      "https://example.com/feed.xml",
  "fetch_content": false,
  "enabled":       true,
  "fragile":       false
}
```

Set `fetch_content: true` if you want full-article markdown via Docling for that source. Set `enabled: false` to skip without removing.

## 🧪 Tests

Three offline test suites + a schema smoke test, all plain-runnable scripts (no pytest dependency):

```bash
python tests/test_schema.py                 # DB tables + indices
python tests/test_rss_blog_scraper.py       # 4 tests, fixture-driven
python tests/test_arxiv_scraper.py          # 5 tests
python tests/test_hf_daily_scraper.py       # 5 tests, includes mocked HTTP fallback
```

The fixtures under [tests/fixtures/](tests/fixtures/) are real saved RSS / Atom responses; tests work fully offline.

For runtime/DB checks, `tools/` has one-shot scripts — most usefully [tools/phase6_check.py](tools/phase6_check.py) which runs the full Runner with `hours=72`, asserts per-source minimums, and verifies idempotency on a second run.

## 📝 Limitations

What this **isn't**, by design and by current state:

**Single-tenant.** One global `DIGEST_TO`, one global source list. No user table, no auth, no per-user preferences (delivery time, sources, topic filters), no web UI. To change channels: edit [config/channels.json](config/channels.json) or [config/sources.json](config/sources.json).

**App is not containerised.** [Docker/docker-compose.yml](Docker/docker-compose.yml) runs Postgres only; the Python process runs on the host. `BlockingScheduler` requires a long-lived process — production deployment needs a wrapper (systemd unit, container with restart policy, or a hosted cron calling `--once`).

**No retry/backoff on OpenAI rate limits.** A failed summary row is logged and stays unsummarised; the next pipeline run picks it up. No exponential backoff inside a single run.

**Long-source truncation is silent.** The summariser trims source text to 40k chars before the API call (see `MAX_SOURCE_CHARS` in [agent/summarizer.py](agent/summarizer.py)). Long arXiv abstracts or full-content blog articles can lose their tail.

**No relevance ranking or cross-feed dedup.** A single AI announcement can ship as one card from `anthropic_news`, another from `techcrunch_ai`, and again from `openai_news` if everyone covers it. arXiv and HF Daily are deduplicated against each other via `arxiv_id` (rows merge their `sources` arrays), but they aren't deduplicated against blog coverage of the same paper.

**`hf_daily` runs against the GitHub mirror as primary** so `hf_upvotes` (and `authors`) populate routinely. Trade-off: 22 entries/day vs takara.ai's 50. The takara.ai feed is wired as the fallback in case the GitHub mirror goes stale or 404s — it'll keep ingestion alive but `hf_upvotes` will go NULL on those days.

**`cmu_ml` and `bair` regularly publish less than once per week.** Don't treat zero-fetched runs from those sources as failures — the runner reports them with a clean `[ ok]` tag because no error occurred; they just didn't have new content.

Other gotchas worth knowing:

- **YouTube transcript scraping is fragile.** `youtube-transcript-api` can be rate-limited or blocked; `RequestBlocked` is caught and logged but the video is stored with an empty transcript.
- **Channel-handle resolution depends on YouTube HTML.** Four fallback regex strategies cover the common cases, but a layout change upstream would break it.
- **Anthropic feed source.** Feeds come from [Olshansk/rss-feeds](https://github.com/Olshansk/rss-feeds) on GitHub, not from Anthropic directly — freshness depends on that repo being maintained.
- **TechCrunch may Cloudflare-block** with a 403 from the runtime UA. Per-source try/except catches it; re-running usually succeeds.
- **Per-source title cleanup.** Anthropic feed titles arrive prefixed with date + category by the Olshansk mirror; `agent/digest.py:_article_title` strips that prefix only for sources whose id starts with `anthropic_`. A new upstream category leaks through unstripped.
