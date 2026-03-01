"""
app/database/db.py — Database engine and session factory.

Reads DATABASE_URL from the .env file (or the environment) and creates:
  - engine       : SQLAlchemy Engine (reuse across the app)
  - SessionLocal : factory for creating DB sessions
  - get_db()     : context-manager that yields a session and ensures cleanup
"""

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

# Load .env from the project root (two levels up from this file: app/database/db.py)
# override=True ensures the .env file wins over any pre-existing env vars
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / ".env", override=True)

DATABASE_URL: str = os.environ["DATABASE_URL"]

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,   # detect stale connections automatically
    echo=False,           # set True to log all SQL statements
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


@contextmanager
def get_db() -> Generator[Session, None, None]:
    """
    Yield a SQLAlchemy session, committing on success and rolling back on error.

    Usage:
        with get_db() as db:
            db.add(some_object)
    """
    db: Session = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
