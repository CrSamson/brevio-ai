"""
app/database/create_tables.py — Idempotent table initialisation script.

Run once (or any number of times) to create all tables in the database.
Tables that already exist are left untouched.

Usage:
    python app/database/create_tables.py
"""

import sys
from pathlib import Path

# Allow running from the project root without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import inspect, text

from app.database.db import engine
from app.database.models import (  # noqa: F401
    Base,
    Article,
    Paper,
    YoutubeVideo,
)


# (table, column, ddl-fragment) — kept here so re-running this script is enough
# to bring an existing DB in line with the current models.
_ADDITIVE_COLUMNS: list[tuple[str, str, str]] = [
    ("youtube_videos", "summary",        "TEXT NOT NULL DEFAULT ''"),
    ("papers",         "summary",        "TEXT"),  # nullable, matches Article.summary
    # Digest send-state. NULL = not yet emailed; set to NOW() once a digest
    # containing the row is successfully sent. See agent/digest.py.
    ("articles",       "digest_sent_at", "TIMESTAMPTZ"),
    ("papers",         "digest_sent_at", "TIMESTAMPTZ"),
    ("youtube_videos", "digest_sent_at", "TIMESTAMPTZ"),
]


def main() -> None:
    print(f"Connecting to: {engine.url}\n")

    inspector   = inspect(engine)
    before      = set(inspector.get_table_names())
    all_tables  = set(Base.metadata.tables.keys())

    Base.metadata.create_all(engine)

    inspector   = inspect(engine)          # refresh after create
    after       = set(inspector.get_table_names())
    created     = after - before
    preexisting = all_tables - created

    for table in sorted(created):
        print(f"  [CREATED]    {table}")
    for table in sorted(preexisting):
        print(f"  [EXISTS]     {table}")

    # ADD COLUMN IF NOT EXISTS keeps existing rows + lets this script stay idempotent
    with engine.begin() as conn:
        for table, column, ddl in _ADDITIVE_COLUMNS:
            if table not in after:
                continue
            conn.execute(text(
                f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {ddl}'
            ))
            print(f"  [COLUMN OK]  {table}.{column}")

    print(f"\nDone. {len(created)} table(s) created, {len(preexisting)} already existed.")


if __name__ == "__main__":
    main()
