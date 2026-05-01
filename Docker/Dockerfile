# syntax=docker/dockerfile:1
# ---------------------------------------------------------------------------
# Multi-stage build for the AI News Aggregator pipeline.
#
# - Stage 1 (builder): installs all Python deps with build toolchain available
#   so wheels that need to compile (psycopg2-binary, native bits of Docling
#   transitives) succeed.
# - Stage 2 (runtime): slim image with only the runtime libs Docling +
#   psycopg2 need at execution time. Non-root user. Python deps copied from
#   the builder via pip --user.
#
# Default ENTRYPOINT runs ONE pass of the daily pipeline, then exits.
# Override CMD or ENTRYPOINT at run time for one-off summarizer runs etc.
# ---------------------------------------------------------------------------

# ============================================================================
# Builder
# ============================================================================
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy only requirements first - lets Docker cache the heavy pip-install
# layer unless requirements.txt actually changes.
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt


# ============================================================================
# Runtime
# ============================================================================
FROM python:3.12-slim AS runtime

# Runtime libs:
#   libpq5             - psycopg2 needs libpq.so at execution time
#   libgl1, libglib2.0-0 - opencv (Docling transitive) needs these
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        libgl1 \
        libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Non-root execution. uid 1000 lines up with most desktop hosts so bind-mounts
# stay sane if you ever mount a volume for logs.
RUN useradd --create-home --uid 1000 --shell /bin/bash app

USER app
WORKDIR /app

# Bring Python deps from the builder; same Python minor version means the
# user-site layout is portable.
COPY --from=builder --chown=app:app /root/.local /home/app/.local
ENV PATH="/home/app/.local/bin:${PATH}"

# Application code. .dockerignore excludes .venv/, tests/, tools/, .git/,
# .env, etc. so the image stays lean.
COPY --chown=app:app . .

# Default entrypoint = run the pipeline once and exit.
ENTRYPOINT ["python", "-m", "agent.scheduler"]
CMD ["--once", "--max-items", "10", "--hours", "48"]
