# AI News Aggregator

> Pulls fresh AI news from **10 RSS blog sources**, **arXiv (cs.LG + cs.AI)**, **HuggingFace Daily Papers**, and **4 YouTube channels**. Stores everything in **Neon Postgres**, summarises each item with an LLM (kind-specific prompts), and emails a daily HTML digest with three sections: Articles, Papers, YouTube. Runs in a **Docker container** triggered by **Windows Task Scheduler** at 08:00 every morning.

## 🎯 Objective

Keep up with AI without ad-hoc tab-checking. Every morning at 08:00 Montreal time, a Docker container fires up, scrapes a configurable list of trusted sources for the last *N* hours, summarises new rows with GPT-4o, and ships a single HTML email so the reader can decide in seconds what to click.

The summariser uses **two system prompts** depending on source kind:
- **Articles + video transcripts** → busy-practitioner blurb, 2–4 sentences, specifics over generalities.
- **Papers** → plain-English explainer for a general audience, 3–5 sentences, jargon defined inline only when load-bearing.

The pipeline is end-to-end and intentionally **single-user**: one recipient, one source list. Sources are config-driven:
- Blog and paper sources: [config/sources.json](config/sources.json)
- YouTube channels: [config/channels.json](config/channels.json)

Adding a new RSS blog source is a JSON edit, no scraper to write.

## 🏗️ Architecture

*Once a day, Windows Task Scheduler launches a `brevio-ai` Docker container that runs three steps in sequence: scrape → summarize → email. All state lives in Neon. The container is one-shot — it exits when the pipeline is done; the OS scheduler fires it again the next morning.*

```mermaid
flowchart LR
    Cron[Windows Task Scheduler<br/>08:00 daily] --> App[brevio-ai container<br/>Docker, runs once]
    Cfg[sources.json<br/>channels.json] -.config.-> App

    App --> Scrape[Step 1: Scrape<br/>10 blogs · arXiv · HF Daily · YouTube]
    Scrape --> DB[(Neon Postgres<br/>articles · papers · youtube_videos)]
    DB --> Sum[Step 2: Summarize<br/>GPT-4o, dual prompts]
    Sum --> DB
    DB --> Dig[Step 3: Digest<br/>4/4/2 balanced + per-source diversity]
    Dig --> Mail[SMTP → inbox]
```

## 🛠️ Tech Stack

- **Language**: Python 3.10+ (pinned to 3.12 in the Docker image)
- **RSS / Atom parsing**: `feedparser`
- **HTTP**: `requests`
- **Article content extraction (optional, per-source)**: [Docling](https://github.com/docling-project/docling) — URL → markdown
- **YouTube transcripts**: [`youtube-transcript-api`](https://github.com/jdepoix/youtube-transcript-api) — no Google API key required
- **Validation**: Pydantic v2 (`BlogArticle`, `Paper`, `VideoMetadata`)
- **Database**: **Neon Postgres** (managed, free-tier — 500 MB / 191.9 compute-hours per month) accessed via SQLAlchemy 2.x ORM
  - Idempotent upserts via `INSERT … ON CONFLICT DO UPDATE`
  - Insert-vs-update distinguished via Postgres' `xmax = 0` trick
  - `papers.arxiv_id` is a **partial unique index** (`WHERE arxiv_id IS NOT NULL`)
  - `papers.sources` is a `TEXT[]` array; cross-linking uses `ARRAY(SELECT DISTINCT unnest(...))` for deduped union
  - `summary` columns deliberately omitted from upsert SET clauses so re-scrapes never overwrite LLM output
- **arXiv**: Atom API (`export.arxiv.org/api/query`), with a process-global ≥ 3 s rate limiter; **PDFs are never downloaded**, only `pdf_url` strings
- **LLM summarisation**: OpenAI `gpt-4o`, kind-specific system prompts
- **Email delivery**: stdlib `smtplib` + `EmailMessage` (multipart text + inline-styled HTML)
- **Containerization**: multi-stage `Dockerfile` (`python:3.12-slim`), runs as non-root user
- **Daily trigger**: **Windows Task Scheduler** in production. APScheduler ships in [agent/scheduler.py](agent/scheduler.py) for in-process scheduling if you'd rather have a long-running Python process

## 📊 What it does today

### Sources

10 enabled blog sources in [config/sources.json](config/sources.json):
`anthropic_news`, `anthropic_research`, `anthropic_engineering`, `openai_news`, `google_research`, `aws_ml`, `nvidia_developer`, `bair`, `cmu_ml`, `techcrunch_ai`.

2 paper sources:
`arxiv_cs_lg_ai` (Atom API, `cs.LG ∪ cs.AI`, `max_results=10`), `hf_daily_papers` (GitHub mirror primary so `hf_upvotes` populates routinely; takara.ai as fallback).

4 YouTube channels in [config/channels.json](config/channels.json).

### Tables (on Neon)

- **`articles`** — every blog/news post. Conflict key: `url`. Per-row: `source` (e.g. `anthropic_news`, `openai_news`), `title`, `published_at`, `summary` (LLM, busy-practitioner tone), `content_md` (Docling, optional), `raw_metadata` (JSONB).
- **`papers`** — arXiv + HF Daily entries, cross-linked. Conflict key: `arxiv_id` (partial unique). Per-row: `sources TEXT[]` (e.g. `{arxiv,hf_daily}`), `title`, `authors` (JSONB), `abstract`, `categories` (JSONB), `pdf_url`, `hf_upvotes`, `summary` (LLM, plain-English explainer for a general audience).
- **`youtube_videos`** — YouTube video metadata + transcript + LLM `summary`. Conflict key: `video_id`.

### Digest selection

`cap_balanced` (in [agent/digest.py](agent/digest.py)) takes the most-recent summarised rows in the lookback window and trims them to a hard cap (default `--max-items 10`):

- **Quotas**: `DIGEST_QUOTAS = (0.4, 0.4, 0.2)` → 4 articles + 4 papers + 2 videos at cap=10.
- **Per-source diversity**: `DIGEST_MAX_PER_SOURCE = 2` → no single publisher (e.g. TechCrunch) can dominate the articles section.
- **Overflow refill**: empty quota slots refill from the other sections by recency, still respecting per-source caps. So a quiet YouTube day shifts those 2 slots to articles or papers, never wasted.

### Resilience

- Per-entry try/except — one bad RSS row doesn't drop the others.
- Per-source try/except — one dead feed doesn't abort the run.
- HF Daily auto-falls-back to its alternate feed when the primary fails.
- arXiv volume gate: a single fetch returning >500 entries (likely misconfiguration) is logged and dropped.
- **Idempotent re-runs**: the daily cron skips already-summarised rows. Running the pipeline twice in a day costs zero extra OpenAI tokens.

## 📁 Repository Structure

```
brevio-ai/
├── main.py                          # Manual one-shot scrape across all sources
├── runner.py                        # Orchestrates 3 scrape steps + per-source reports
├── scrapers/
│   ├── base.py                      # BaseScraper ABC
│   ├── schemas.py                   # Pydantic v2: BlogArticle, Paper
│   ├── rss_blog_scraper.py          # Generic RSS scraper, drives off sources.json
│   ├── arxiv_scraper.py             # Atom API, rate-limited, no PDFs
│   ├── hf_daily_scraper.py          # Primary + fallback, dual-shape parser
│   └── youtube_scraper.py           # RSS + transcript + Shorts detection
├── agent/
│   ├── summarizer.py                # OpenAI gpt-4o, dual prompts (article / paper)
│   ├── digest.py                    # 3-section HTML + plain-text email + cap_balanced
│   └── scheduler.py                 # APScheduler driver (alt to Task Scheduler)
├── app/database/
│   ├── db.py                        # Engine + session factory (reads DATABASE_URL)
│   ├── models.py                    # SQLAlchemy: Article, Paper, YoutubeVideo
│   ├── crud.py                      # upsert_articles / upsert_papers / merge_hf_daily_papers / ...
│   └── create_tables.py             # Idempotent schema init + additive ALTERs
├── config/
│   ├── sources.json                 # blogs + papers config (drives the runner)
│   └── channels.json                # YouTube channel handles
├── tests/
│   ├── fixtures/                    # saved RSS / Atom snapshots
│   ├── test_schema.py
│   ├── test_rss_blog_scraper.py     # 4 tests
│   ├── test_arxiv_scraper.py        # 5 tests
│   └── test_hf_daily_scraper.py     # 5 tests, mocks HTTP fallback
├── tools/
│   ├── verify_feeds.py              # pre-flight feed verifier
│   ├── migrate_to_neon.py           # one-shot local-Postgres → Neon migration
│   ├── phase4_check.py              # arXiv live + idempotency
│   ├── phase5_check.py              # HF Daily live + cross-link
│   └── phase6_check.py              # E2E backtest with --truncate flag
├── Docker/
│   ├── Dockerfile                   # multi-stage, slim runtime, non-root user
│   ├── docker-compose.yml           # `app` (pipeline) + `postgres` (local-dev) profiles
│   └── run_pipeline.ps1             # Task Scheduler entry point (Windows)
├── .dockerignore
└── requirements.txt
```

## 🚀 How to Run

### Configure environment

Create `.env` at the project root:

```dotenv
# Database (Neon Postgres in production)
DATABASE_URL=postgresql+psycopg2://user:pass@ep-xxx.aws.neon.tech/db?sslmode=require&channel_binding=require
# Optional rollback handle:
DATABASE_URL_LOCAL_BACKUP=postgresql+psycopg2://user:pass@localhost:5433/db

# LLM
OPENAI_API_KEY=sk-...

# Email (Gmail App Password example)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=you@gmail.com
SMTP_PASSWORD=your-16-char-app-password
DIGEST_FROM=you@gmail.com
DIGEST_TO=you@gmail.com
```

If you ever spin up the local-dev Postgres (`docker compose --profile local-dev up -d postgres`), `.env` also needs `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` for compose substitution.

### Initialise schema

```bash
pip install -r requirements.txt
python -m app.database.create_tables
```

Idempotent. Safe to re-run.

### One-shot from your venv

```powershell
# Smoke test - scrape, summarise, email
python -m agent.scheduler --once --max-items 10 --hours 48

# No email - render the digest to stdout
python -m agent.digest --hours 48 --max-items 10 --dry-run

# Cheaper iteration loops while tweaking the paper prompt
python -m agent.summarizer --papers --limit 3 --force
```

### One-shot via Docker (matches production)

```powershell
# Build the image once
docker compose -f Docker/docker-compose.yml --env-file .env --profile pipeline build app

# Run the daily pipeline once
pwsh -File Docker/run_pipeline.ps1
```

`Docker/run_pipeline.ps1` logs to `logs/pipeline_YYYY-MM-DD.log`.

### Add or edit a source

Append to [config/sources.json](config/sources.json) under `blogs[]`:

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

Set `fetch_content: true` to pull full article markdown via Docling for that source. Set `enabled: false` to skip without removing.

## 🧪 Tests

Three offline test suites + a schema smoke test. Plain-runnable scripts (no pytest dependency):

```bash
python tests/test_schema.py                 # DB tables + indices
python tests/test_rss_blog_scraper.py       # 4 tests, fixture-driven
python tests/test_arxiv_scraper.py          # 5 tests
python tests/test_hf_daily_scraper.py       # 5 tests, mocked HTTP fallback
```

The fixtures under [tests/fixtures/](tests/fixtures/) are real saved RSS / Atom responses; tests work fully offline.

For runtime/DB checks, [tools/phase6_check.py](tools/phase6_check.py) runs the full Runner with `hours=72`, asserts per-source minimums, and verifies idempotency on a second run.

## 🐳 Deployment

### Containerization

[Docker/Dockerfile](Docker/Dockerfile) is multi-stage:
- **Builder** stage: installs all Python deps with `build-essential` + `libpq-dev` so wheels compile cleanly.
- **Runtime** stage: `python:3.12-slim` + `libpq5 libgl1 libglib2.0-0` (Docling needs them via opencv). Non-root user `app`.

Image size: ~3.2 GB compressed. Most of that is Docling + transitives (opencv, pypdfium2, accelerate). Stays installed for portability even when `fetch_content=false`.

### Windows Task Scheduler — daily at 08:00

The cron lives in Windows Task Scheduler, not in a long-running Python process. [Docker/run_pipeline.ps1](Docker/run_pipeline.ps1) is the entry point.

Setup:

1. Docker Desktop → Settings → General → check **"Start Docker Desktop when you sign in"**.
2. Task Scheduler → **Create Task**:
   - General: name `Brevio AI Daily Digest`, "Run only when user is logged on".
   - Triggers → Daily → 08:00 local time.
   - Actions → "Start a program": `pwsh.exe`, args `-NoProfile -ExecutionPolicy Bypass -File "<project>\Docker\run_pipeline.ps1"`, start in `<project root>`.
   - Settings → "Stop the task if it runs longer than: 1 hour".
3. Right-click the task → Run → confirm `logs/pipeline_<today>.log` populates and email arrives.

### Migrating local Postgres → Neon

[tools/migrate_to_neon.py](tools/migrate_to_neon.py) is a one-shot SQLAlchemy-based migration. Reads `DATABASE_URL` (local) and `DATABASE_URL_NEON` (target) from `.env`, creates schema on Neon, copies every row of `articles`/`papers`/`youtube_videos`, and prints a side-by-side row-count audit. Idempotent — skips tables that already have data on Neon.

### Free-tier capacity (Neon)

- **Storage**: 500 MB free. Current usage ~1 MB. Daily growth ~250 KB → ~5 years before the cap.
- **Compute**: 191.9 hours/month free. Daily 5-minute run uses ~2.5 hrs/month — 1.3% of the limit.
- If you flip `fetch_content: true` for blog sources, daily growth jumps to ~1 MB and you'd hit the cap in ~12–18 months. That's the main lever.

## 📝 Limitations

What this **isn't**, by design and by current state:

**Single-tenant.** One global `DIGEST_TO`, one global source list. No user table, no auth, no per-user preferences. To change channels: edit [config/channels.json](config/channels.json) or [config/sources.json](config/sources.json).

**Daily run requires the laptop to be on at 08:00.** Windows Task Scheduler fires only when the machine is awake (and Docker Desktop is running). If you travel, the digest pauses until you're back. Migrating to **GitHub Actions** is a one-shot follow-up — same Dockerfile drops in unchanged, only `DATABASE_URL` becomes a GH secret.

**No retry/backoff on OpenAI rate limits.** A failed summary row is logged and stays unsummarised; the next pipeline run picks it up. No exponential backoff inside a single run.

**Long-source truncation is silent.** The summariser trims source text to 40k chars before the API call (see `MAX_SOURCE_CHARS` in [agent/summarizer.py](agent/summarizer.py)). Long arXiv abstracts or full-content blog articles can lose their tail.

**No relevance ranking or cross-feed dedup.** A single AI announcement can ship as a card from `anthropic_news`, another from `techcrunch_ai`, and again from `openai_news` if everyone covers it. arXiv and HF Daily *are* deduplicated against each other via `arxiv_id` (rows merge their `sources` arrays).

**`hf_daily` runs against the GitHub mirror as primary** so `hf_upvotes` (and `authors`) populate routinely. Trade-off: 22 entries/day vs takara.ai's 50. The takara.ai feed is wired as the fallback.

**`cmu_ml` and `bair` regularly publish less than once per week.** Don't treat zero-fetched runs from those sources as failures.

Other gotchas:
- **YouTube transcript scraping is fragile.** `youtube-transcript-api` can be rate-limited or blocked; `RequestBlocked` is caught and logged but the video is stored with an empty transcript.
- **Channel-handle resolution depends on YouTube HTML.** Four fallback regex strategies cover the common cases, but a layout change upstream would break it.
- **Anthropic feed source.** Feeds come from [Olshansk/rss-feeds](https://github.com/Olshansk/rss-feeds) on GitHub, not from Anthropic directly — freshness depends on that repo being maintained.
- **TechCrunch may Cloudflare-block** with a 403 from the runtime UA. Per-source try/except catches it; re-running usually succeeds.
- **Per-source title cleanup.** Anthropic feed titles arrive prefixed with date + category by the Olshansk mirror; `agent/digest.py:_article_title` strips that prefix only for sources whose id starts with `anthropic_`. A new upstream category leaks through unstripped.
