# AI News Aggregator

> Pulls fresh AI news from RSS feeds (Anthropic blogs + a curated set of YouTube channels), enriches it with article content and video transcripts, summarizes each item with an LLM, and emails a daily HTML digest.

## 🎯 Objective

Keep up with AI without ad-hoc tab-checking. Every run, the aggregator fetches what was published in the last *N* hours from a fixed set of trusted sources, normalizes it into Postgres, asks GPT-4o for a one-paragraph blurb per row, and ships a single HTML email so the reader can decide in seconds what to click.

The pipeline is end-to-end. It is intentionally **single-user**: one recipient, one channel list, one machine running the daily cron.

## 🏗️ Architecture

*Two RSS-based scrapers feed a Runner that upserts to Postgres. A Summarizer fills in any rows missing a summary. The Digest builder pulls summarized rows from the last N hours and emails them. A blocking APScheduler chains all three once a day.*

```mermaid
flowchart LR
    A[Anthropic RSS<br/>3 feeds] --> C[AnthropicScraper<br/>+ optional Docling content]
    B[YouTube RSS<br/>configured channels] --> D[YouTubeScraper<br/>+ transcript API]
    C --> E[Runner<br/>aggregate + window]
    D --> E
    E --> F[(PostgreSQL<br/>upsert ON CONFLICT)]
    F --> G[Summarizer<br/>OpenAI gpt-4o<br/>fills empty summary]
    G --> F
    F --> H[Digest builder<br/>HTML + plain-text]
    H --> I[SMTP<br/>STARTTLS]
    J[APScheduler<br/>daily cron] -.drives.-> E
    J -.drives.-> G
    J -.drives.-> H
```

## 🛠️ Tech Stack

- **Language**: Python 3.10+
- **RSS parsing**: `feedparser`
- **Article content extraction**: [Docling](https://github.com/docling-project/docling) `DocumentConverter` (URL → markdown)
- **YouTube transcripts**: [`youtube-transcript-api`](https://github.com/jdepoix/youtube-transcript-api) — no Google API key required
- **Validation**: Pydantic v2 (`AnthropicArticle`, `VideoMetadata`)
- **Database**: PostgreSQL 16 (Docker) + SQLAlchemy 2.x ORM
- **Upsert strategy**: PostgreSQL `INSERT … ON CONFLICT DO UPDATE` (summary column deliberately omitted from the SET clause so re-scrapes don't clobber LLM work)
- **LLM summarization**: OpenAI `gpt-4o` via the official `openai` SDK
- **Email delivery**: stdlib `smtplib` + `EmailMessage` (multipart text + inline-styled HTML)
- **Scheduling**: APScheduler `BlockingScheduler` with a daily `CronTrigger`

## 📊 What it does today

- Pulls from **3 Anthropic RSS feeds** (news, research, engineering) and **4 YouTube channels** (configured in [config/channels.json](config/channels.json)).
- Resolves YouTube `@handle` → `UC…` channel ID by scraping the channel page (4 fallback regex strategies).
- Detects and skips YouTube Shorts via a HEAD request to `/shorts/{video_id}`.
- Prefers **manually-created** transcripts over auto-generated, falls back gracefully across language codes (`en`, `en-US`, `en-GB`).
- Two tables, both with idempotent upserts:
  - `anthropic_articles` — conflict key: `url`
  - `youtube_videos` — conflict key: `video_id`
- Summarizes any row whose `summary` column is empty using `gpt-4o`. Per-row try/except so a single API failure doesn't abort the batch.
- Renders an inline-styled HTML email (no `<style>` blocks — Gmail strips them) with article cards, YouTube thumbnails, and a plain-text fallback.
- Sends via SMTP/STARTTLS (Gmail-friendly defaults).
- Runs the full **scrape → summarize → email** pipeline once a day on a configurable local time.

## 📁 Repository Structure

```
brevio-ai/
├── main.py                          # Entry point — one-shot scrape only
├── runner.py                        # Orchestrates scrapers + DB upsert
├── scrapers/
│   ├── anthropic_scrapper.py        # 3 RSS feeds → AnthropicArticle (Pydantic)
│   └── youtube_scraper.py           # RSS + transcript + Shorts detection
├── agent/
│   ├── summarizer.py                # OpenAI gpt-4o per-row summaries
│   ├── digest.py                    # Build HTML+text email, send via SMTP
│   └── scheduler.py                 # Daily APScheduler driver (scrape → summarize → email)
├── app/database/
│   ├── db.py                        # Engine + session factory
│   ├── models.py                    # SQLAlchemy: AnthropicArticle, YoutubeVideo
│   ├── crud.py                      # Upsert + summary read/write helpers
│   └── create_tables.py             # Idempotent schema init (incl. additive ALTERs)
├── config/channels.json             # YouTube channel handles
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

# LLM summarization
OPENAI_API_KEY=sk-...

# Email delivery (Gmail example — use an App Password, not your account password)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=you@gmail.com
SMTP_PASSWORD=your-16-char-app-password
DIGEST_FROM=you@gmail.com           # optional, defaults to SMTP_USER
DIGEST_TO=you@gmail.com             # comma-separated for multiple recipients

# Scheduler (local time)
SCHEDULE_HOUR=7
SCHEDULE_MINUTE=0
SCHEDULE_HOURS_LOOKBACK=24
```

### 3. Install + initialize schema

```bash
pip install -r requirements.txt
python -m app.database.create_tables
```

### 4. Run

```bash
# (a) Scrape only — populate the DB once.
python main.py

# (b) Summarize any rows still missing a summary.
python -m agent.summarizer
python -m agent.summarizer --limit 5         # cap rows of each type
python -m agent.summarizer --anthropic       # only blog articles
python -m agent.summarizer --youtube         # only videos

# (c) Build + send the digest for the last 24h (no scheduler).
python -m agent.digest
python -m agent.digest --hours 48
python -m agent.digest --dry-run             # render to stdout, don't send
python -m agent.digest --to a@b.com          # override DIGEST_TO

# (d) The full pipeline.
python -m agent.scheduler --once             # scrape + summarize + email, then exit
python -m agent.scheduler --run-now          # run immediately, then arm daily cron
python -m agent.scheduler                    # arm daily cron, block forever
python -m agent.scheduler --skip-email       # scrape + summarize, no email
```

Tweak the lookback window, content/transcript fetching in [main.py](main.py):

```python
runner = Runner(
    hours=24,
    fetch_content=True,        # Docling-extract Anthropic article markdown
    fetch_transcripts=True,    # pull YouTube transcripts
)
```

## 📝 Limitations

What this **isn't**, by design and by current state:

**Single-tenant.** One global recipient (`DIGEST_TO`), one global channel list ([config/channels.json](config/channels.json)). No user table, no auth, no per-user preferences (delivery time, sources, topic filters), no web UI. To change channels: edit JSON.

**App is not containerized.** [Docker/docker-compose.yml](Docker/docker-compose.yml) runs Postgres only; the Python process runs on the host. `BlockingScheduler` requires a long-lived process, so production deployment needs a wrapper (systemd unit, container with a restart policy, or a hosted cron that calls `--once`).

**No tests.** Zero test files in the repo.

**No retry/backoff on OpenAI rate limits.** A failed row is logged and stays unsummarized; the next pipeline run will pick it up, but there is no exponential-backoff retry within a single run.

**No relevance ranking or cross-feed dedup.** Five mediocre videos from one channel all land in the digest. An Anthropic announcement and a YouTuber's reaction to the same announcement both ship as separate cards.

**Source list is small and Anthropic-centric.** No other AI labs (OpenAI/DeepMind/Meta/Mistral), no arXiv, no Twitter/X.

**Long-source truncation is silent.** The summarizer trims source text to 40k chars before sending it to the model (see `MAX_SOURCE_CHARS` in [agent/summarizer.py](agent/summarizer.py)). Long Anthropic research articles can lose their tail without the email signaling it.

Other gotchas worth knowing:

- **YouTube transcript scraping is fragile.** `youtube-transcript-api` can be rate-limited or blocked; `RequestBlocked` is caught and logged but the video is stored with an empty transcript.
- **Channel-handle resolution depends on YouTube HTML.** Four fallback regex strategies cover the common cases, but a layout change upstream would break it.
- **Anthropic feed source.** Feeds come from [Olshansk/rss-feeds](https://github.com/Olshansk/rss-feeds) on GitHub, not from Anthropic directly — so freshness depends on that repo being maintained.
- **Title cleanup is brittle.** Anthropic feed titles arrive prefixed with date + category and are scrubbed via a hardcoded category list in [scrapers/anthropic_scrapper.py](scrapers/anthropic_scrapper.py); a new upstream category would leak through unstripped.
