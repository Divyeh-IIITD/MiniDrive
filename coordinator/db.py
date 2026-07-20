from __future__ import annotations

import os
from datetime import datetime

from sqlalchemy import Boolean, CheckConstraint, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, create_engine, func, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker


DATABASE_URL = os.getenv(
    "MINIDRIVE_DATABASE_URL",
    "postgresql+psycopg://postgres:postgres@localhost/minidrive",
)


class Base(DeclarativeBase):
    pass


class FileRecord(Base):
    __tablename__ = "files"
    __table_args__ = (
        CheckConstraint(
            "status IN ('uploading', 'committed', 'failed')",
            name="ck_files_status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    filename: Mapped[str] = mapped_column(String(512), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        server_default=text("'uploading'"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    chunks: Mapped[list[ChunkRecord]] = relationship(
        back_populates="file",
        cascade="all, delete-orphan",
    )


class ChunkRecord(Base):
    __tablename__ = "chunks"
    __table_args__ = (
        CheckConstraint("chunk_index >= 0", name="ck_chunks_nonnegative_index"),
        CheckConstraint("size_bytes >= 0", name="ck_chunks_nonnegative_size"),
        UniqueConstraint("file_id", "chunk_index", name="uq_chunks_file_chunk_index"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    file_id: Mapped[int] = mapped_column(
        ForeignKey("files.id", ondelete="CASCADE"),
        nullable=False,
    )
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
    hash: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)

    file: Mapped[FileRecord] = relationship(back_populates="chunks")
    locations: Mapped[list[ChunkLocationRecord]] = relationship(
        back_populates="chunk",
        cascade="all, delete-orphan",
    )


class ChunkLocationRecord(Base):
    __tablename__ = "chunk_locations"
    __table_args__ = (
        CheckConstraint("node_url <> ''", name="ck_chunk_locations_node_url_nonempty"),
        UniqueConstraint("chunk_id", "node_url", name="uq_chunk_locations_chunk_node"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chunk_id: Mapped[int] = mapped_column(
        ForeignKey("chunks.id", ondelete="CASCADE"),
        nullable=False,
    )
    node_url: Mapped[str] = mapped_column(Text, nullable=False)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    written_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    chunk: Mapped[ChunkRecord] = relationship(back_populates="locations")


ENGINE = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, autocommit=False, expire_on_commit=False)