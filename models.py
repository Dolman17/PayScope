# models.py
from __future__ import annotations

from datetime import datetime, date, UTC

from flask_login import UserMixin
from sqlalchemy import CheckConstraint, Index, UniqueConstraint
from sqlalchemy.exc import IntegrityError

from extensions import db


def utc_now() -> datetime:
    return datetime.now(UTC)


# -------------------------------------------------------------------
# Core job/pay data
# -------------------------------------------------------------------
class JobRecord(db.Model):
    __tablename__ = "job_record"

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(50), index=True)
    company_name = db.Column(db.String(100))
    sector = db.Column(db.String(50), index=True)
    job_role = db.Column(db.String(100), index=True)
    # canonical / grouped job role (e.g. "Care & Support Worker")
    job_role_group = db.Column(db.String(120), index=True)
    postcode = db.Column(db.String(20))
    county = db.Column(db.String(50), index=True)
    pay_rate = db.Column(db.Float)

    imported_month = db.Column(db.String(20), index=True)
    imported_year = db.Column(db.String(10), index=True)

    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)

    created_at = db.Column(db.DateTime, default=utc_now)

    imported_from_posting_id = db.Column(db.Integer)
    imported_at = db.Column(db.DateTime)

    external_url = db.Column(db.Text)
    logo_filename = db.Column(db.String(200))


class JobSummaryDaily(db.Model):
    __tablename__ = "job_summary_daily"

    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, index=True)

    # group dimensions
    county = db.Column(db.String(50), index=True)
    sector = db.Column(db.String(50), index=True)
    job_role_group = db.Column(db.String(120), index=True)

    adverts_count = db.Column(db.Integer)

    median_pay_rate = db.Column(db.Float)
    p25_pay_rate = db.Column(db.Float)
    p75_pay_rate = db.Column(db.Float)

    min_pay_rate = db.Column(db.Float)
    max_pay_rate = db.Column(db.Float)


class PayRate(db.Model):
    __tablename__ = "pay_rates"

    id = db.Column(db.Integer, primary_key=True)

    employer_name = db.Column(db.String(200), nullable=False)
    organisation = db.Column(
        db.String(40), nullable=False
    )  # Competitor | Blue Ribbon | Forevermore
    role = db.Column(
        db.String(120), nullable=False
    )  # e.g. Care Assistant, Senior Carer, RN
    contract_type = db.Column(
        db.String(20), nullable=False
    )  # employed | bank | agency

    base_rate = db.Column(db.Numeric(10, 2), nullable=False)  # £/hr
    weekend_rate = db.Column(db.Numeric(10, 2), nullable=True)
    night_rate = db.Column(db.Numeric(10, 2), nullable=True)
    bh_rate = db.Column(db.Numeric(10, 2), nullable=True)
    enhancement_notes = db.Column(db.String(400), nullable=True)
    mileage_pence = db.Column(db.Integer, nullable=True)

    effective_from = db.Column(db.Date, nullable=False)
    source_url = db.Column(db.String(400), nullable=True)
    notes = db.Column(db.String(400), nullable=True)

    region = db.Column(db.String(80), nullable=False)
    county = db.Column(db.String(80), nullable=True)
    town_city = db.Column(db.String(120), nullable=True)
    postcode = db.Column(db.String(12), nullable=False)

    lat = db.Column(db.Float, nullable=True)
    lon = db.Column(db.Float, nullable=True)

    # linkage & soft delete
    upload_batch_id = db.Column(
        db.Integer, db.ForeignKey("upload_batches.id"), nullable=True
    )
    is_deleted = db.Column(db.Boolean, default=False, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "employer_name",
            "role",
            "contract_type",
            "postcode",
            "effective_from",
            name="uq_payrate_employer_role_contract_postcode_date",
        ),
        CheckConstraint("base_rate >= 0", name="chk_payrate_base_rate_nonneg"),
        Index("ix_payrate_org", "organisation"),
        Index("ix_payrate_role", "role"),
        Index("ix_payrate_region", "region"),
        Index("ix_payrate_postcode", "postcode"),
        Index("ix_payrate_latlon", "lat", "lon"),
    )


class JobPosting(db.Model):
    __tablename__ = "job_postings"

    id = db.Column(db.Integer, primary_key=True)

    # External/API text fields → Text to avoid varchar overflow
    title = db.Column(db.Text, nullable=False)
    company_name = db.Column(db.Text, nullable=True)
    location_text = db.Column(db.Text, nullable=True)
    postcode = db.Column(db.String(20), nullable=True)

    # high-level sector classification, e.g. "Social Care", "Nursing", "HR"
    sector = db.Column(db.Text, nullable=True)

    min_rate = db.Column(db.Numeric(10, 2), nullable=True)
    max_rate = db.Column(db.Numeric(10, 2), nullable=True)
    rate_type = db.Column(db.String(50), nullable=True)  # hourly, annual, etc.
    contract_type = db.Column(db.String(50), nullable=True)

    source_site = db.Column(db.Text, nullable=False)
    external_id = db.Column(db.String(255), nullable=True)
    url = db.Column(db.Text, nullable=True)

    posted_date = db.Column(db.Date, nullable=True)
    scraped_at = db.Column(db.DateTime, nullable=False, default=utc_now)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    imported = db.Column(db.Boolean, default=False)

    raw_json = db.Column(db.Text, nullable=True)

    search_role = db.Column(db.Text, nullable=True)
    search_location = db.Column(db.Text, nullable=True)

    __table_args__ = (
        db.Index("ix_job_postings_source_ext", "source_site", "external_id"),
        db.Index("ix_job_postings_sector", "sector"),
    )


class WaitlistSignup(db.Model):
    __tablename__ = "waitlist_signups"

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), nullable=False, index=True, unique=True)
    source = db.Column(db.String(50), nullable=True)  # e.g. "landing"
    notes = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=utc_now)


class AccessRequest(db.Model):
    __tablename__ = "access_requests"

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), nullable=True, index=True)
    notes = db.Column(db.Text, nullable=True)
    source = db.Column(db.String(50), nullable=True)  # e.g. "landing"
    created_at = db.Column(db.DateTime, nullable=False, default=utc_now)
    status = db.Column(
        db.String(30), nullable=False, default="new"
    )  # new/triaged/approved/rejected


class Company(db.Model):
    __tablename__ = "companies"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), unique=True, index=True)
    canonical_name = db.Column(
        db.String(255), index=True
    )  # normalized form for fuzzy match
    sector = db.Column(db.String(50), nullable=True)
    created_at = db.Column(db.DateTime, default=utc_now)


class JobRoleMapping(db.Model):
    """
    Canonical mapping from a messy raw job title to a clean canonical role.

    Used by:
      - Job Role Cleaner
      - AI-assisted suggestions (ai_* fields)
      - Hygiene analytics in the dashboard/report views
    """
    __tablename__ = "job_role_mappings"

    id = db.Column(db.Integer, primary_key=True)
    raw_value = db.Column(db.Text, unique=True, nullable=False)
    canonical_role = db.Column(db.String(255), nullable=False)
    source = db.Column(db.String(50))  # optional: e.g. "adzuna", "indeed"
    created_at = db.Column(
        db.DateTime, default=utc_now, nullable=False
    )
    updated_at = db.Column(
        db.DateTime,
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )

    # Optional AI suggestion cache (to avoid repeat calls)
    ai_canonical_role = db.Column(db.String(255), nullable=True)
    ai_score = db.Column(
        db.Integer, nullable=True
    )  # 0–100 confidence from AI/local engine
    ai_model = db.Column(
        db.String(80), nullable=True
    )  # e.g. "gpt-4o-mini" or "local-rules-fuzzy"
    ai_reason = db.Column(
        db.Text, nullable=True
    )  # short explanation from AI / rules engine


class SectorMapping(db.Model):
    __tablename__ = "sector_mappings"

    id = db.Column(db.Integer, primary_key=True)

    # The messy input we see in the wild (case-insensitive match)
    raw_value = db.Column(
        db.String(120), unique=True, nullable=False, index=True
    )

    # The clean sector we want everywhere
    canonical_sector = db.Column(
        db.String(80), nullable=False, index=True
    )

    created_at = db.Column(
        db.DateTime, default=utc_now, nullable=False
    )
    updated_at = db.Column(
        db.DateTime,
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )

    def __repr__(self) -> str:
        return f"<SectorMapping {self.raw_value!r} -> {self.canonical_sector!r}>"


class CronRunLog(db.Model):
    __tablename__ = "cron_run_logs"

    id = db.Column(db.Integer, primary_key=True)
    job_name = db.Column(db.String(100), nullable=False)
    started_at = db.Column(db.DateTime, nullable=False, default=utc_now)
    finished_at = db.Column(db.DateTime, nullable=True)
    status = db.Column(
        db.String(20), nullable=False, default="running"
    )  # running/success/partial/error
    message = db.Column(db.Text, nullable=True)
    rows_scraped = db.Column(db.Integer, nullable=True)
    records_created = db.Column(db.Integer, nullable=True)
    triggered_by = db.Column(db.String(150), nullable=True)
    trigger = db.Column(
        db.String(50), nullable=True
    )  # manual/cron/etc
    day_label = db.Column(db.String(20))
    run_stats = db.Column(db.Text)


class OnsEarnings(db.Model):
    __tablename__ = "ons_earnings"

    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, nullable=False)
    geography_code = db.Column(
        db.String(32), nullable=False, index=True
    )
    geography_name = db.Column(db.String(255), nullable=False)
    measure_code = db.Column(
        db.String(16), nullable=False
    )  # e.g. 20100, 20701
    value = db.Column(db.Float, nullable=True)

    created_at = db.Column(
        db.DateTime, nullable=False, default=utc_now
    )

    __table_args__ = (
        db.UniqueConstraint(
            "year",
            "geography_code",
            "measure_code",
            name="uq_ons_year_geo_measure",
        ),
    )


# -------------------------------------------------------------------
# Auth / multi-tenant
# -------------------------------------------------------------------
class Organisation(db.Model):
    __tablename__ = "organisations"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False, unique=True)
    slug = db.Column(db.String(255), nullable=False, unique=True)
    is_active = db.Column(
        db.Boolean, default=True, nullable=False
    )
    created_at = db.Column(
        db.DateTime, default=utc_now, nullable=False
    )

    def __repr__(self):
        return f"<Organisation {self.id} {self.slug}>"


class User(UserMixin, db.Model):
    __tablename__ = "user"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(
        db.String(150), unique=True, nullable=False
    )
    password = db.Column(db.String(200), nullable=False)

    # Track when a user last logged in (for monitoring / auditing)
    last_login_at = db.Column(db.DateTime, nullable=True, index=True)

    # 0 = normal user, 1 = superuser, 2 = admin
    # (matches usage in code: admin_level == 1 => superuser)
    admin_level = db.Column(
        db.Integer, default=0, nullable=False
    )

    # Organisation / multi-tenant fields
    org_role = db.Column(
        db.String(20), default="member", nullable=False
    )  # member | admin | owner

    organisation_id = db.Column(
        db.Integer, db.ForeignKey("organisations.id"), nullable=True
    )
    organisation = db.relationship(
        "Organisation", backref=db.backref("users", lazy="dynamic")
    )

    def is_superuser(self) -> bool:
        return self.admin_level == 1

    def is_admin(self) -> bool:
        return self.admin_level in (1, 2)

    def is_member(self) -> bool:
        return self.admin_level == 0


class AIAnalysisLog(db.Model):
    __tablename__ = "ai_analysis_log"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(
        db.Integer, db.ForeignKey("user.id"), nullable=True
    )
    filters = db.Column(
        db.Text
    )  # JSON string of filters applied
    record_count = db.Column(db.Integer)
    output_html = db.Column(
        db.Text
    )  # store AI output so you can review it later
    created_at = db.Column(
        db.DateTime, default=utc_now
    )

    user = db.relationship("User", backref="ai_logs")


class UploadBatch(db.Model):
    __tablename__ = "upload_batches"

    id = db.Column(db.Integer, primary_key=True)
    created_at = db.Column(
        db.DateTime, default=utc_now, nullable=False
    )
    created_by = db.Column(
        db.String(120), nullable=True
    )  # username/email if available
    source_filename = db.Column(db.String(255), nullable=True)
    total_rows = db.Column(
        db.Integer, default=0, nullable=False
    )
    inserted_rows = db.Column(
        db.Integer, default=0, nullable=False
    )
    updated_rows = db.Column(
        db.Integer, default=0, nullable=False
    )
    skipped_rows = db.Column(
        db.Integer, default=0, nullable=False
    )
    errors_json = db.Column(
        db.Text, nullable=True
    )  # aggregated errors per row


class WeeklyMarketChange(db.Model):
    __tablename__ = "weekly_market_changes"

    id = db.Column(db.Integer, primary_key=True)

    week_start = db.Column(db.Date, nullable=False, index=True)
    week_end = db.Column(db.Date, nullable=False, index=True)

    # 'pay', 'vacancy', 'volume', 'coverage'
    metric_type = db.Column(
        db.String(30), nullable=False, index=True
    )

    job_role = db.Column(
        db.String(120), nullable=True, index=True
    )
    sector = db.Column(
        db.String(120), nullable=True, index=True
    )
    location = db.Column(
        db.String(120), nullable=True, index=True
    )

    value_previous = db.Column(db.Numeric(10, 2), nullable=True)
    value_current = db.Column(db.Numeric(10, 2), nullable=True)
    delta_value = db.Column(db.Numeric(10, 2), nullable=True)

    # IMPORTANT:
    # Was Numeric(6,2) which only supports up to 9999.99 and was overflowing on extreme % changes.
    delta_percent = db.Column(db.Numeric(10, 2), nullable=True)

    # 'up', 'down', 'flat'
    direction = db.Column(
        db.String(10), nullable=True, index=True
    )

    headline = db.Column(db.Text, nullable=False)
    interpretation = db.Column(db.Text, nullable=True)

    # 1–5
    confidence_level = db.Column(db.SmallInteger, nullable=True)

    is_featured = db.Column(
        db.Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )
    is_published = db.Column(
        db.Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )

    # -----------------------------
    # AI narrative fields (NEW)
    # -----------------------------
    ai_narrative = db.Column(
        db.Text, nullable=True
    )  # The per-item narrative paragraph(s)
    ai_driver_tags = db.Column(
        db.String(255), nullable=True
    )  # Comma-separated tags
    ai_model = db.Column(
        db.String(80), nullable=True
    )  # e.g. "gpt-4o-mini"
    ai_updated_at = db.Column(
        db.DateTime, nullable=True
    )  # When the AI fields were generated

    created_at = db.Column(
        db.DateTime, nullable=False, default=utc_now
    )

    __table_args__ = (
        db.Index("ix_wmc_week_metric", "week_start", "metric_type"),
    )


class WeeklyInsight(db.Model):
    __tablename__ = "weekly_insights"

    id = db.Column(db.Integer, primary_key=True)
    week_start = db.Column(
        db.Date, nullable=False, unique=True, index=True
    )
    week_end = db.Column(db.Date, nullable=False)

    headline = db.Column(db.String(200), nullable=True)
    overview = db.Column(
        db.Text, nullable=True
    )  # the top-of-page brief (paragraph/bullets)

    ai_generated_at = db.Column(db.DateTime, nullable=True)
    ai_model = db.Column(db.String(64), nullable=True)

    created_at = db.Column(
        db.DateTime, default=utc_now, nullable=False
    )
    updated_at = db.Column(
        db.DateTime,
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )


class JobRoleSectorOverride(db.Model):
    """
    Manual mapping from a canonical role (job_role_group) to a canonical sector.
    Used to fix cases where role ends up in "Other" sector.
    """
    __tablename__ = "job_role_sector_overrides"

    id = db.Column(db.Integer, primary_key=True)

    # The canonical role label you use in analytics (typically JobRecord.job_role_group)
    canonical_role = db.Column(
        db.String(255), unique=True, nullable=False, index=True
    )

    # The sector you want this role to belong to
    canonical_sector = db.Column(
        db.String(80), nullable=False, index=True
    )

    created_at = db.Column(
        db.DateTime, default=utc_now, nullable=False
    )
    updated_at = db.Column(
        db.DateTime,
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )

    def __repr__(self) -> str:
        return f"<JobRoleSectorOverride {self.canonical_role!r} -> {self.canonical_sector!r}>"


def resolve_sector_for_canonical_role(
    canonical_role: str | None, fallback_sector: str | None
) -> str:
    """
    Resolve sector using:
      1) JobRoleSectorOverride(canonical_role) if present
      2) fallback_sector if present/non-empty
      3) "Other"
    """
    role = (canonical_role or "").strip()
    if role:
        try:
            ov = JobRoleSectorOverride.query.filter_by(
                canonical_role=role
            ).first()
            if ov and (ov.canonical_sector or "").strip():
                return ov.canonical_sector.strip()
        except Exception:
            # fail open: fall back to stored sector
            pass

    fb = (fallback_sector or "").strip()
    return fb if fb else "Other"


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def ensure_default_organisation() -> Organisation:
    """Idempotent helper: ensure there's a 'Default Organisation' row and return it."""
    org = Organisation.query.filter_by(slug="default").first()
    if not org:
        org = Organisation(
            name="Default Organisation",
            slug="default",
            is_active=True,
            created_at=utc_now(),
        )
        db.session.add(org)
        db.session.commit()
    return org


def get_or_create_role_mapping(
    raw_value: str | None,
    canonical_role: str | None,
    source: str | None = None,
):
    """
    Safely get or create a JobRoleMapping without triggering duplicate-key errors.
    Returns JobRoleMapping instance, or None if raw_value is empty.

    Also supports job hygiene tooling by:
      - preserving the original raw_value exactly as seen
      - letting canonical_role be updated on re-use
    """
    if not raw_value or not raw_value.strip():
        return None

    raw_value = raw_value.strip()

    mapping = JobRoleMapping.query.filter_by(raw_value=raw_value).first()
    if mapping:
        if canonical_role and mapping.canonical_role != canonical_role:
            mapping.canonical_role = canonical_role
            if source:
                mapping.source = source
            db.session.add(mapping)
        return mapping

    mapping = JobRoleMapping(
        raw_value=raw_value,
        canonical_role=canonical_role or raw_value,
        source=source,
    )
    db.session.add(mapping)

    try:
        db.session.flush()
        return mapping
    except IntegrityError:
        db.session.rollback()
        return JobRoleMapping.query.filter_by(raw_value=raw_value).first()