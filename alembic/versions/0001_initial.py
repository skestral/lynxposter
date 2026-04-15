"""Initial schema for the canonical crossposter service."""

from __future__ import annotations

from alembic import op

from app import models  # noqa: F401
from app.database import Base


revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)


def downgrade() -> None:
    bind = op.get_bind()
    Base.metadata.drop_all(bind=bind)
