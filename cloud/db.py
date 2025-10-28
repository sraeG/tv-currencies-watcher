from __future__ import annotations
import time
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    BigInteger,
    String,
    Text,
    DateTime,
    create_engine,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from sqlalchemy.dialects.postgresql import insert as pg_insert


class Base(DeclarativeBase):
    pass


class Chart(Base):
    __tablename__ = "charts"

    uuid: Mapped[str] = mapped_column(String, primary_key=True)
    username: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    symbol: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)  # epoch seconds
    interval: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    direction: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    data: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    scraped_at: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    source_page: Mapped[str] = mapped_column(
        Text, nullable=False, server_default="currencies_recent"
    )


def epoch_now() -> int:
    return int(time.time())


def make_engine(db_url: str):
    if not db_url:
        raise RuntimeError(
            "DATABASE_URL not provided. Set it as an environment variable."
        )
    # Ensure SSL for Supabase/Neon
    if "sslmode=" not in db_url and db_url.startswith("postgres"):
        joiner = "&" if "?" in db_url else "?"
        db_url = f"{db_url}{joiner}sslmode=require"
    engine = create_engine(db_url, pool_pre_ping=True, future=True)
    return engine


def create_tables(engine) -> None:
    Base.metadata.create_all(engine)


def has_uuid(session: Session, uuid: str) -> bool:
    return session.get(Chart, uuid) is not None


def insert_first_seen(session: Session, uuid: str, source_page: str) -> None:
    stmt = pg_insert(Chart).values(
        uuid=uuid,
        first_seen_at=datetime.now(timezone.utc),
        source_page=source_page,
    ).on_conflict_do_nothing(index_elements=[Chart.__table__.c.uuid])
    session.execute(stmt)


def upsert_full_record(
    session: Session,
    *,
    uuid: str,
    username: Optional[str],
    symbol: Optional[str],
    created_at: Optional[int],
    interval: Optional[str],
    direction: Optional[str],
    data: Optional[dict],
) -> None:
    now_epoch = epoch_now()
    stmt = pg_insert(Chart).values(
        uuid=uuid,
        username=username,
        symbol=symbol,
        created_at=created_at,
        interval=interval,
        direction=direction,
        data=data,
        scraped_at=now_epoch,
    ).on_conflict_do_update(
        index_elements=[Chart.__table__.c.uuid],
        set_={
            "username": username,
            "symbol": symbol,
            "created_at": created_at,
            "interval": interval,
            "direction": direction,
            "data": data,
            "scraped_at": now_epoch,
        },
    )
    session.execute(stmt)
