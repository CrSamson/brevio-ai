# AI News Aggregator

> Pulls fresh AI news from RSS feeds (Anthropic blogs + a curated set of YouTube channels), enriches it with article content and video transcripts, and persists everything to Postgres via idempotent upserts.

## 🎯 Objective

Keep up with AI without ad-hoc tab-checking. Every run, the aggregator fetches what was published in the last *N* hours from a fixed set of trusted sources, normalizes it, and stores it in a database that downstream consumers (digesters, dashboards, agents) can query later.

The current build is the **ingestion layer** — collection and storage. LLM summarization and email delivery are planned but not yet implemented; see *Limitations*.

## 🏗️ Architecture

*Two RSS-based scrapers feed a single Runner that upserts results into Postgres on `url` / `video_id` conflict keys.*

```mermaid
flowchart LR
    A[Anthropic RSS<br/>3 feeds] --> C[AnthropicScraper<br/>+ optional Docling content]
    B[YouTube RSS<br/>configured channels] --> D[YouTubeScraper<br/>+ transcript API]
    C --> E[Runner<br/>aggregate + window]
    D --> E
    E --> F[(PostgreSQL<br/>upsert ON CONFLICT)]
```

## 🛠️ Tech Stack

- **Language**: Python 3.10+
- **RSS parsing**: `feedparser`
- **Article content extraction**: [Docling](https://github.com/docling-project/docling) `DocumentConverter` (URL → markdown)
- **YouTube transcripts**: [`youtube-transcript-api`](https://github.com/jdepoix/youtube-transcript-api) — no Google API key required
- **Validation**: Pydantic v2 (`AnthropicArticle`, `VideoMetadata`)
- **Database**: PostgreSQL 16 (Docker) + SQLAlchemy ORM
- **Upsert strategy**: PostgreSQL `INSERT … ON CONFLICT DO UPDATE`

## 📊 What it does today

- Pulls from **3 Anthropic RSS feeds** (news, research, engineering) and **4 YouTube channels** (configured in [config/channels.json](config/channels.json)).
- Resolves YouTube `@handle` → `UC…` channel ID by scraping the channel page (4 fallback regex strategies).
- Detects and skips YouTube Shorts via a HEAD request to `/shorts/{video_id}`.
- Prefers **manually-created** transcripts over auto-generated, falls back gracefully across language codes (`en`, `en-US`, `en-GB`).
- Two tables, both with idempotent upserts:
  - `anthropic_articles` — conflict key: `url`
  - `youtube_videos` — conflict key: `video_id`
- One-shot run from `main.py` returns a single report dict (not yet wired to a scheduler).

## 📁 Repository Structure

```
my-ai-news-aggregator/
├── main.py                          # Entry point — one-shot run
├── runner.py                        # Orchestrates scrapers + DB upsert
├── scrapers/
│   ├── anthropic_scrapper.py        # 3 RSS feeds → AnthropicArticle (Pydantic)
│   └── youtube_scraper.py           # RSS + transcript + Shorts detection
├── app/database/
│   ├── db.py                        # Engine + session factory
│   ├── models.py                    # SQLAlchemy: AnthropicArticle, YoutubeVideo
│   ├── crud.py                      # Upsert via pg_insert.on_conflict_do_update
│   └── create_tables.py             # Idempotent schema init
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

```
DATABASE_URL=postgresql+psycopg2://USER:PASS@localhost:5433/DBNAME
POSTGRES_USER=...
POSTGRES_PASSWORD=...
POSTGRES_DB=...
```

### 3. Install + initialize schema

```bash
pip install -r requirements.txt
python -m app.database.create_tables
```

### 4. Run a collection

```bash
python main.py
```

Tweak the lookback window, content/transcript fetching in [main.py](main.py:14):

```python
runner = Runner(
    hours=24,
    fetch_content=True,        # Docling-extract Anthropic article markdown
    fetch_transcripts=True,    # pull YouTube transcripts
)
```

## 📝 Limitations

The previous README described an end-to-end system (LLM summarization → digest → Gmail delivery → user profiles → scheduler). **None of that is committed yet.** This rewrite reflects what's actually in the repo.

Concretely, what's **not** built yet:

- **No LLM summarization.** No OpenAI/Anthropic API calls anywhere. Articles and transcripts are stored raw.
- **No email delivery.** No SMTP, no digest formatting.
- **No scheduler.** `main.py` runs once and exits. No APScheduler, no cron wiring.
- **No user profiles or personalization.** Channel list is global (`config/channels.json`), not per-user.
- **App is not containerized.** `docker-compose.yml` runs Postgres only; the Python app runs on the host.

Other gotchas worth knowing:

- **YouTube transcript scraping is fragile.** `youtube-transcript-api` can be rate-limited or blocked; `RequestBlocked` is caught and logged but the video is stored with an empty transcript.
- **Channel-handle resolution depends on YouTube HTML.** Four fallback regex strategies cover the common cases, but a layout change upstream would break it.
- **Anthropic feed source.** Feeds come from [Olshansk/rss-feeds](https://github.com/Olshansk/rss-feeds) on GitHub, not from Anthropic directly — so freshness depends on that repo being maintained.
