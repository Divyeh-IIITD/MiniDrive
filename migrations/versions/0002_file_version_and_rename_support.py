from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0002_file_version_and_rename_support"
down_revision = "0001_upload_metadata_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "files",
        sa.Column("version", sa.Integer(), nullable=False, server_default=sa.text("0")),
    )


def downgrade() -> None:
    op.drop_column("files", "version")