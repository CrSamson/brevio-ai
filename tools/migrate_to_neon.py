"""
tools/migrate_to_neon.py - One-shot data migration: local Postgres -> Neon.

Uses pure SQLAlchemy (already in your venv). No psql, no pg_dump, no Docker
exec - so no shell-encoding or pipe-through-Windows concerns.

Reads both connection strings from .env:
    DATABASE_URL        -> source (your local Docker postgres at localhost:5433)
    DATABASE_URL_NEON   -> target (your Neon project)

Both URLs must use the `postgresql+psycopg2://...` SQLAlchemy form.

Steps:
    1. Probe both databases. Bail out if either is unreachable.
    2. Create the schema on Neon (Base.metadata.create_all + additive ALTERs).
       Idempotent - safe to re-run.
    3. Per table (articles, papers, youtube_videos):
         - If Neon already has rows in that table, skip (treat the migration
           as already done; deliberate guard against accidental dup-inserts).
         - Otherwise, read every row from local and INSERT into Neon, letting
           Neon assign fresh `id`s. The conflict keys (url / arxiv_id /
           video_id) carry through and stay unique on Neon.
    4. Print a side-by-side row-count table on both ends.

Run:
    python tools/migrate_to_neon.py

Re-running is safe: each table is skipped if Neon already has data in it.
To force a re-import, TRUNCATE the table on Neon first.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker

from app.database.models import Article, Base, Paper, YoutubeVideo


# Run the same additive ALTERs that app/database/create_tables.py applies, so
# Neon's schema matches our current production columns even if Base.metadata
# was older when the table was first created.
ADDITIVE_COLUMNS: list[tuple[str, str, str]] = [
    ("youtube_videos", "summary", "TEXT NOT NULL DEFAULT ''"),
    ("papers",         "summary", "TEXT"),
]


def _safe_url_repr(url: str) -> str:
    """Return host/db part of a postgres URL without the password."""
    try:
        tail = url.split("@", 1)[1]
        return f"...@{tail.split('?')[0]}"
    except Exception:  # noqa: BLE001
        return "<unparseable url>"


def _probe(engine, label: str) -> bool:
    try:
        with engine.connect() as conn:
            v = conn.execute(text("SELECT version()")).scalar()
        print(f"  [ok] {label}: connected (server: {v.split(',')[0] if v else '?'})")
        return True
    except Exception as e:  # noqa: BLE001
        print(f"  [FAIL] {label}: {type(e).__name__}: {e}")
        return False


def _create_schema_on_neon(neon_engine) -> None:
    print("\n=== creating schema on Neon (idempotent) ===")
    Base.metadata.create_all(neon_engine)
    with neon_engine.begin() as conn:
        for table, col, ddl in ADDITIVE_COLUMNS:
            conn.execute(text(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {ddl}"
            ))
    print("  schema ready")


def _migrate_table(local_engine, neon_engine, model) -> tuple[int, int, str]:
    tname = model.__tablename__
    LocalSession = sessionmaker(bind=local_engine)
    NeonSession  = sessionmaker(bind=neon_engine)

    # Guard: skip if Neon already has rows.
    with neon_engine.connect() as conn:
        existing = conn.execute(select(func.count()).select_from(model)).scalar() or 0
    if existing > 0:
        print(f"  {tname}: Neon already has {existing} rows -> SKIP "
              "(TRUNCATE on Neon first if you want to re-import)")
        return existing, 0, "skipped"

    with LocalSession() as ls:
        rows = list(ls.execute(select(model)).scalars().all())
    if not rows:
        print(f"  {tname}: local is empty, nothing to copy")
        return 0, 0, "empty"

    print(f"  {tname}: copying {len(rows)} rows...")
    cols = [c.name for c in model.__table__.columns if c.name != "id"]
    with NeonSession() as ns:
        for row in rows:
            values = {c: getattr(row, c) for c in cols}
            ns.add(model(**values))
        ns.commit()
    print(f"  {tname}: inserted {len(rows)} rows on Neon")
    return existing, len(rows), "ok"


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env", override=True)

    local_url = os.environ.get("DATABASE_URL", "").strip()
    neon_url  = os.environ.get("DATABASE_URL_NEON", "").strip()
    if not local_url:
        print("ERROR: DATABASE_URL not set in .env")
        return 1
    if not neon_url:
        print("ERROR: DATABASE_URL_NEON not set in .env")
        return 1

    print("=== source/target ===")
    print(f"  local: {_safe_url_repr(local_url)}")
    print(f"  neon : {_safe_url_repr(neon_url)}")

    local_engine = create_engine(local_url, pool_pre_ping=True)
    neon_engine  = create_engine(neon_url,  pool_pre_ping=True)

    print("\n=== probing both databases ===")
    ok_local = _probe(local_engine, "local")
    ok_neon  = _probe(neon_engine,  "neon ")
    if not (ok_local and ok_neon):
        print("\nABORT: at least one endpoint is unreachable")
        return 1

    _create_schema_on_neon(neon_engine)

    print("\n=== copying rows ===")
    for model in (Article, Paper, YoutubeVideo):
        _migrate_table(local_engine, neon_engine, model)

    print("\n=== final row counts ===")
    print(f"  {'table':<20} {'local':>8} {'neon':>8}  status")
    print(f"  {'-'*20} {'-'*8} {'-'*8}  ------")
    overall_ok = True
    for model in (Article, Paper, YoutubeVideo):
        with local_engine.connect() as lc, neon_engine.connect() as nc:
            lcount = lc.execute(select(func.count()).select_from(model)).scalar() or 0
            ncount = nc.execute(select(func.count()).select_from(model)).scalar() or 0
        status = "OK" if lcount == ncount else "MISMATCH"
        if lcount != ncount:
            overall_ok = False
        print(f"  {model.__tablename__:<20} {lcount:>8} {ncount:>8}  {status}")

    print()
    if overall_ok:
        print("DONE - row counts match. Phase 3 complete; ready for Phase 4 (config swap).")
        return 0
    print("DONE with COUNT MISMATCH - investigate before flipping DATABASE_URL.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
