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

from sqlalchemy import inspect

from app.database.db import engine
from app.database.models import Base, AnthropicArticle, YoutubeVideo  # noqa: F401


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

    print(f"\nDone. {len(created)} table(s) created, {len(preexisting)} already existed.")


if __name__ == "__main__":
    main()
