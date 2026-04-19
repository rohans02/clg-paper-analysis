from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from publication_manager.models import Base


def get_engine(db_path: str = "publication_manager.db"):
    sqlite_path = Path(db_path).resolve()
    return create_engine(f"sqlite:///{sqlite_path}", future=True)


def init_db(db_path: str = "publication_manager.db") -> None:
    engine = get_engine(db_path)
    Base.metadata.create_all(engine)
    _ensure_schema(engine)


def _ensure_schema(engine) -> None:
    with engine.begin() as conn:
        cols = conn.execute(text("PRAGMA table_info(publications)")).fetchall()
        col_names = {row[1] for row in cols}
        if "publication_name" not in col_names:
            conn.execute(text("ALTER TABLE publications ADD COLUMN publication_name TEXT"))
        if "conference_date" not in col_names:
            conn.execute(text("ALTER TABLE publications ADD COLUMN conference_date TEXT"))


def get_session_factory(db_path: str = "publication_manager.db"):
    engine = get_engine(db_path)
    return sessionmaker(bind=engine, expire_on_commit=False, class_=Session)


@contextmanager
def session_scope(db_path: str = "publication_manager.db"):
    session_factory = get_session_factory(db_path)
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
