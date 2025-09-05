"""add composite index on JobRecord filters"""

from alembic import op
import sqlalchemy as sa

# IDs must match your filenames / prior migration chain
revision = "db47237b45a9"      # <-- this file's hash
down_revision = "7a26bb5d3959" # <-- previous file's hash
branch_labels = None
depends_on = None


def upgrade():
    # If your JobRecord has a custom __tablename__, replace "job_record" below
    op.create_index(
        "ix_jobrecord_filters",
        "job_record",
        ["sector", "job_role", "county", "imported_year", "imported_month"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_jobrecord_filters", table_name="job_record")
