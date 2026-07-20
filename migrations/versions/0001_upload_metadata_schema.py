from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0001_upload_metadata_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "files",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("filename", sa.String(length=512), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'uploading'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.CheckConstraint("status IN ('uploading', 'committed', 'failed')", name="ck_files_status"),
    )

    op.create_table(
        "chunks",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("file_id", sa.Integer(), sa.ForeignKey("files.id", ondelete="CASCADE"), nullable=False),
        sa.Column("chunk_index", sa.Integer(), nullable=False),
        sa.Column("hash", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.CheckConstraint("chunk_index >= 0", name="ck_chunks_nonnegative_index"),
        sa.CheckConstraint("size_bytes >= 0", name="ck_chunks_nonnegative_size"),
        sa.UniqueConstraint("file_id", "chunk_index", name="uq_chunks_file_chunk_index"),
    )
    op.create_index("ix_chunks_file_id", "chunks", ["file_id"])

    op.create_table(
        "chunk_locations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("chunk_id", sa.Integer(), sa.ForeignKey("chunks.id", ondelete="CASCADE"), nullable=False),
        sa.Column("node_url", sa.Text(), nullable=False),
        sa.Column("is_primary", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("written_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.CheckConstraint("node_url <> ''", name="ck_chunk_locations_node_url_nonempty"),
        sa.UniqueConstraint("chunk_id", "node_url", name="uq_chunk_locations_chunk_node"),
    )
    op.create_index("ix_chunk_locations_chunk_id", "chunk_locations", ["chunk_id"])


def downgrade() -> None:
    op.drop_index("ix_chunk_locations_chunk_id", table_name="chunk_locations")
    op.drop_table("chunk_locations")
    op.drop_index("ix_chunks_file_id", table_name="chunks")
    op.drop_table("chunks")
    op.drop_table("files")