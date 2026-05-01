# AI News Aggregator

> Pulls fresh AI news from **10 RSS blog sources**, **arXiv (cs.LG + cs.AI)**, **HuggingFace Daily Papers**, and **4 YouTube channels**. Stores everything in **Neon Postgres**, summarises each item with an LLM (kind-specific prompts), and emails a daily HTML digest with three sections: Articles, Papers, YouTube. Runs on **GitHub Actions** at 08:00 Montreal time, every day.

## 🎯 Objective

Keep up with AI without ad-hoc tab-checking. Every morning a GitHub Actions runner fires up, scrapes a configurable list of trusted sources for the last *N* hours, summarises new rows with GPT-4o, and ships a single HTML email so the reader can decide in seconds what to click.

The summariser uses **two system prompts** depending on source kind:
- **Articles + video transcripts** → busy-practitioner blurb, 2–4 sentences, specifics over generalities.
- **Papers** → plain-English explainer for a general audience, 3–5 sentences, jargon defined inline only when load-bearing.

The pipeline is end-to-end and intentionally **single-user**: one recipient, one source list. Sources are config-driven:
- Blog and paper sources: [config/sources.json](config/sources.json)
- YouTube channels: [config/channels.json](config/channels.json)

Adding a new RSS blog source is a JSON edit, no scraper to write.

## 🏗️ Architecture

*Once a day, a GitHub Actions cron job spins up a runner that runs three steps in sequence: scrape → summarize → email. All state lives in Neon. The runner exits when the pipeline is done; nothing stays running between fires.*

```mermaid
flowchart LR
    Cron[GitHub Actions cron<br/>0 12 * * * UTC] --> Scrape[Step 1: Scrape<br/>10 blogs · arXiv · HF Daily · YouTube]
    Cfg[sources.json<br/>channels.json] -.config.-> Scrape

    Scrape --> DB[(Neon Postgres<br/>articles · papers · youtube_videos)]
    DB --> Sum[Step 2: Summarize<br/>GPT-4o, dual prompts]
    Sum --> DB
    DB --> Dig[Step 3: Digest<br/>4/4/2 balanced + per-source diversity]
    Dig --> Mail[SMTP → inbox]
```

## 🛠️ Tech Stack

- **Language**: Python 3.12
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
- **Daily trigger**: **GitHub Actions cron** (`.github/workflows/digest.yml`). Secrets in repo Actions secrets. APScheduler also ships in [agent/scheduler.py](agent/scheduler.py) for in-process scheduling if you'd rather have a long-running Python process

## 📊 What it does today

### Sources

10 enabled blog sources in [config/sources.json](config/sources.json):
`anthropic_news`, `anthropic_research`, `anthropic_engineering`, `openai_news`, `google_research`, `aws_ml`, `nvidia_developer`, `bair`, `cmu_ml`, `techcrunch_ai`.

2 paper sources:
`arxiv_cs_lg_ai` (Atom API, `cs.LG ∪ cs.AI`, `max_results=10`), `hf_daily_papers` (GitHub mirror primary so `hf_upvotes` populates routinely; takara.ai as fallback).

4 YouTube channels in [config/channels.json](config/channels.json).

### Tables (on Neon)

All three tables carry `digest_sent_at TIMESTAMPTZ NULL` — the timestamp at which the row was first included in a sent digest, or `NULL` if it has never shipped. Used by the digest's no-duplicate filter (see *Digest selection* below).

- **`articles`** — every blog/news post. Conflict key: `url`. Per-row: `source` (e.g. `anthropic_news`, `openai_news`), `title`, `published_at`, `summary` (LLM, busy-practitioner tone), `content_md` (Docling, optional), `raw_metadata` (JSONB), `digest_sent_at`.
- **`papers`** — arXiv + HF Daily entries, cross-linked. Conflict key: `arxiv_id` (partial unique). Per-row: `sources TEXT[]` (e.g. `{arxiv,hf_daily}`), `title`, `authors` (JSONB), `abstract`, `categories` (JSONB), `pdf_url`, `hf_upvotes`, `summary` (LLM, plain-English explainer for a general audience), `digest_sent_at`.
- **`youtube_videos`** — YouTube video metadata + transcript + LLM `summary`. Conflict key: `video_id`. Plus `digest_sent_at`.

### Digest selection

`cap_balanced` (in [agent/digest.py](agent/digest.py)) takes the most-recent summarised rows in the lookback window and trims them to a hard cap (default `--max-items 10`):

- **Quotas**: `DIGEST_QUOTAS = (0.4, 0.4, 0.2)` → 4 articles + 4 papers + 2 videos at cap=10.
- **Per-source diversity**: `DIGEST_MAX_PER_SOURCE = 2` → no single publisher (e.g. TechCrunch) can dominate the articles section.
- **Overflow refill**: empty quota slots refill from the other sections by recency, still respecting per-source caps. So a quiet YouTube day shifts those 2 slots to articles or papers, never wasted.
- **No duplicate sends**: `get_recent_summarized_*` filters `WHERE digest_sent_at IS NULL`, so a row that has shipped in a previous email is never picked again. After `send_email()` returns, every included row is stamped with `digest_sent_at = NOW()` (see `mark_digest_sent` in [app/database/crud.py](app/database/crud.py)). The mark step runs **after** the SMTP call — an SMTP failure leaves the rows unsent and they get retried on the next cron. `--dry-run` skips the mark entirely. Re-scrapes never reset send state because all upserts omit `digest_sent_at` from their `SET` clauses.

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
│   └── scheduler.py                 # Pipeline driver (--once for cron, BlockingScheduler for in-process)
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
│   ├── backfill_digest_sent.py      # one-shot: stamp existing rows as already-sent
│   ├── phase4_check.py              # arXiv live + idempotency
│   ├── phase5_check.py              # HF Daily live + cross-link
│   └── phase6_check.py              # E2E backtest
├── .github/
│   └── workflows/
│       └── digest.yml               # daily cron + manual trigger
└── requirements.txt
```

## 🚀 How to Run

### Configure environment (local development)

Create `.env` at the project root:

```dotenv
# Database (Neon)
DATABASE_URL=postgresql+psycopg2://user:pass@ep-xxx.aws.neon.tech/db?sslmode=require&channel_binding=require

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

For production, the same values live in **GitHub Actions Secrets** (see Deployment).

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

## 🚀 Deployment (GitHub Actions)

The daily cron lives in [.github/workflows/digest.yml](.github/workflows/digest.yml). The runner installs Python 3.12, the deps from `requirements.txt`, runs `create_tables` (idempotent), and then `python -m agent.scheduler --once --hours 48 --max-items 10`.

### Schedule

```yaml
on:
  schedule:
    - cron: '0 12 * * *'   # 12:00 UTC daily
  workflow_dispatch:
    inputs: { hours, max_items }
```

12:00 UTC lands at **08:00 EDT** (March → November, Montreal in daylight time) and **07:00 EST** (November → March). GitHub Actions cron is fixed to UTC and has no DST awareness, so the 1-hour winter drift is the lesser evil compared to running twice and double-emailing.

### Secrets

Repo → **Settings → Secrets and variables → Actions** → New repository secret. **Five mandatory:**

| Name | Value |
|---|---|
| `DATABASE_URL` | Your Neon URL — `postgresql+psycopg2://...neon.tech/...?sslmode=require&channel_binding=require` |
| `OPENAI_API_KEY` | Your `sk-proj-...` key |
| `SMTP_USER` | Sender Gmail address |
| `SMTP_PASSWORD` | Gmail App Password (16-char, no spaces in the secret) |
| `DIGEST_TO` | Recipient |

Three optional with defaults baked into the workflow (`smtp.gmail.com`, `587`, fallback to `SMTP_USER`):

| Name | Default if absent |
|---|---|
| `SMTP_HOST` | `smtp.gmail.com` |
| `SMTP_PORT` | `587` |
| `DIGEST_FROM` | `${{ secrets.SMTP_USER }}` |

### Manual run / smoke test

GitHub repo → **Actions** tab → **Daily Digest** workflow → **Run workflow** button → optional `hours` and `max_items` form fields → **Run**. ~5–7 minutes later, the email arrives. Each step's stdout is preserved in the run log for 90 days.

### Free-tier capacity

| Resource | Free quota | Current usage | Headroom |
|---|---|---|---|
| **Neon storage** | 500 MB | ~1 MB | ~5 years at current ingestion rate |
| **Neon compute hours** | 191.9 hr/month | ~2.5 hr/month | 1.3% of cap |
| **GitHub Actions runner-min** (private repos) | 2,000 min/month | ~150 min/month | 7.5% of cap |
| **OpenAI tokens** | n/a (pay-as-you-go) | ~$0.15–0.25/day | ~$5–8/month |

If you flip `fetch_content: true` for blog sources, daily growth jumps to ~1 MB and you'd hit Neon's storage cap in ~12–18 months. That's the main lever.

## 📝 Limitations

What this **isn't**, by design and by current state:

**Single-tenant.** One global `DIGEST_TO`, one global source list. No user table, no auth, no per-user preferences. To change channels: edit [config/channels.json](config/channels.json) or [config/sources.json](config/sources.json).

**No retry/backoff on OpenAI rate limits.** A failed summary row is logged and stays unsummarised; the next pipeline run picks it up. No exponential backoff inside a single run.

**Long-source truncation is silent.** The summariser trims source text to 40k chars before the API call (see `MAX_SOURCE_CHARS` in [agent/summarizer.py](agent/summarizer.py)). Long arXiv abstracts or full-content blog articles can lose their tail.

**No relevance ranking or cross-feed dedup.** A single AI announcement can ship as a card from `anthropic_news`, another from `techcrunch_ai`, and again from `openai_news` if everyone covers it. arXiv and HF Daily *are* deduplicated against each other via `arxiv_id` (rows merge their `sources` arrays).

**`hf_daily` runs against the GitHub mirror as primary** so `hf_upvotes` (and `authors`) populate routinely. Trade-off: 22 entries/day vs takara.ai's 50. The takara.ai feed is wired as the fallback.

**`cmu_ml` and `bair` regularly publish less than once per week.** Don't treat zero-fetched runs from those sources as failures.

**DST drift.** GitHub Actions cron is UTC-only. The daily fire is 08:00 in summer, 07:00 in winter. Acceptable for a personal digest; not acceptable if you ever need precise Montreal-local timing.

Other gotchas:
- **YouTube transcript scraping is fragile.** `youtube-transcript-api` can be rate-limited or blocked; `RequestBlocked` is caught and logged but the video is stored with an empty transcript.
- **Channel-handle resolution depends on YouTube HTML.** Four fallback regex strategies cover the common cases, but a layout change upstream would break it.
- **Anthropic feed source.** Feeds come from [Olshansk/rss-feeds](https://github.com/Olshansk/rss-feeds) on GitHub, not from Anthropic directly — freshness depends on that repo being maintained.
- **TechCrunch may Cloudflare-block** with a 403 from the runtime UA. Per-source try/except catches it; re-running usually succeeds.
- **Per-source title cleanup.** Anthropic feed titles arrive prefixed with date + category by the Olshansk mirror; `agent/digest.py:_article_title` strips that prefix only for sources whose id starts with `anthropic_`. A new upstream category leaks through unstripped.
