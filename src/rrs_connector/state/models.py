"""SQLAlchemy ORM records for the connector state database."""

from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class DbBase(DeclarativeBase):
    """Base class for all SQLAlchemy ORM records."""

    pass


class SenderRecord(DbBase):
    """Configured Home Assistant report sender persisted in local state."""

    __tablename__ = "senders"

    id: Mapped[int] = mapped_column(primary_key=True)
    client_id: Mapped[str | None] = mapped_column(String, nullable=True)
    robonomics_address: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # MVP: The cursor is stored in senders.
    # Future: Move to a separate sender_poll_state.
    # The cursor is the timestamp: datalog slots are reused once the ring
    # buffer wraps, so the index alone is only informational.
    last_scanned_datalog_timestamp: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_scanned_datalog_index: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    last_scanned_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    datalog_entries: Mapped[list["DatalogEntryRecord"]] = relationship(
        back_populates="sender"
    )


class DatalogStatus(StrEnum):
    """Processing state for one datalog event."""

    IGNORED = "ignored"
    NEW = "new"
    FETCHING = "fetching"
    FETCHED = "fetched"
    DECRYPTING = "decrypting"
    PROCESSED = "processed"
    FAILED = "failed"


class DatalogEntryRecord(DbBase):
    """One datalog event observed for a sender."""

    __tablename__ = "datalog_entries"
    __table_args__ = (
        # The index is a reusable ring buffer slot, so the timestamp is part of
        # the event identity.
        UniqueConstraint("sender_id", "datalog_index", "datalog_timestamp"),
        Index("ix_datalog_entries_sender_id_cid", "sender_id", "cid"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    sender_id: Mapped[int] = mapped_column(ForeignKey("senders.id"), nullable=False)
    datalog_index: Mapped[int] = mapped_column(Integer, nullable=False)
    datalog_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    raw_payload: Mapped[str | None] = mapped_column(Text, nullable=True)
    cid: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[DatalogStatus] = mapped_column(
        Enum(
            DatalogStatus,
            values_callable=lambda statuses: [status.value for status in statuses],
            native_enum=False,
            validate_strings=True,
            name="datalog_status",
        ),
        nullable=False,
        index=True,
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    sender: Mapped["SenderRecord"] = relationship(back_populates="datalog_entries")
    report_artifact: Mapped["ReportArtifactRecord | None"] = relationship(
        back_populates="datalog_entry",
    )


class ReportArtifactRecord(DbBase):
    """Local filesystem artifacts stored for one datalog entry."""

    __tablename__ = "report_artifacts"

    id: Mapped[int] = mapped_column(primary_key=True)
    datalog_entry_id: Mapped[int] = mapped_column(
        ForeignKey("datalog_entries.id"), nullable=False, unique=True
    )
    archive_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_dir: Mapped[str | None] = mapped_column(Text, nullable=True)
    decrypted_dir: Mapped[str | None] = mapped_column(Text, nullable=True)
    meta_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    datalog_entry: Mapped["DatalogEntryRecord"] = relationship(
        back_populates="report_artifact"
    )
