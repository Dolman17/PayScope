"""Convert JobPosting string fields to Text

Revision ID: 1bf55d1d5e50
Revises: 1d357c432682
Create Date: 2025-12-11 15:56:15.474924

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1bf55d1d5e50'
down_revision = '1d357c432682'
branch_labels = None
depends_on = None


def upgrade():
    # Convert JobPosting fields from VARCHAR → TEXT
    op.alter_column("job_postings", "title",
        existing_type=sa.String(length=255),
        type_=sa.Text(),
        existing_nullable=False,
    )

    op.alter_column("job_postings", "company_name",
        existing_type=sa.String(length=255),
        type_=sa.Text(),
        existing_nullable=True,
    )

    op.alter_column("job_postings", "location_text",
        existing_type=sa.String(length=255),
        type_=sa.Text(),
        existing_nullable=True,
    )

    op.alter_column("job_postings", "sector",
        existing_type=sa.String(length=100),
        type_=sa.Text(),
        existing_nullable=True,
    )

    op.alter_column("job_postings", "search_role",
        existing_type=sa.String(length=255),
        type_=sa.Text(),
        existing_nullable=True,
    )

    op.alter_column("job_postings", "search_location",
        existing_type=sa.String(length=255),
        type_=sa.Text(),
        existing_nullable=True,
    )

    op.alter_column("job_postings", "source_site",
        existing_type=sa.String(length=100),
        type_=sa.Text(),
        existing_nullable=False,
    )


def downgrade():
    # Revert TEXT → VARCHAR
    op.alter_column("job_postings", "source_site",
        existing_type=sa.Text(),
        type_=sa.String(length=100),
        existing_nullable=False,
    )

    op.alter_column("job_postings", "search_location",
        existing_type=sa.Text(),
        type_=sa.String(length=255),
        existing_nullable=True,
    )

    op.alter_column("job_postings", "search_role",
        existing_type=sa.Text(),
        type_=sa.String(length=255),
        existing_nullable=True,
    )

    op.alter_column("job_postings", "sector",
        existing_type=sa.Text(),
        type_=sa.String(length=100),
        existing_nullable=True,
    )

    op.alter_column("job_postings", "location_text",
        existing_type=sa.Text(),
        type_=sa.String(length=255),
        existing_nullable=True,
    )

    op.alter_column("job_postings", "company_name",
        existing_type=sa.Text(),
        type_=sa.String(length=255),
        existing_nullable=True,
    )

    op.alter_column("job_postings", "title",
        existing_type=sa.Text(),
        type_=sa.String(length=255),
        existing_nullable=False,
    )

