# models.py
from datetime import datetime
from flask_login import UserMixin
from extensions import db  # shared db from extensions.py
from datetime import datetime
from app import db  # adjust import if needed


class JobRecord(db.Model):
    __tablename__ = "job_record"

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(50), index=True)
    company_name = db.Column(db.String(100))
    sector = db.Column(db.String(50), index=True)
    job_role = db.Column(db.String(100), index=True)
    # NEW: canonical / grouped job role (e.g. "Care & Support Worker")
    job_role_group = db.Column(db.String(120), index=True)  # <--- NEW
    postcode = db.Column(db.String(20))
    county = db.Column(db.String(50), index=True)
    pay_rate = db.Column(db.Float)

    imported_month = db.Column(db.String(20), index=True)
    imported_year = db.Column(db.String(10), index=True)

    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

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

    # you can add job_role if you want more granularity
    # job_role = db.Column(db.String(100), index=True)

    adverts_count = db.Column(db.Integer)

    median_pay_rate = db.Column(db.Float)
    p25_pay_rate = db.Column(db.Float)
    p75_pay_rate = db.Column(db.Float)

    min_pay_rate = db.Column(db.Float)
    max_pay_rate = db.Column(db.Float)


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(150), unique=True, nullable=False)
    password = db.Column(db.String(200), nullable=False)
    admin_level = db.Column(db.Integer, default=0)  # 0=user, 1=admin, 2=superuser
    org_role = db.Column(db.String(20), default="member", nullable=False)

    organisation_id = db.Column(
        db.Integer,
        db.ForeignKey("organisations.id"),
        nullable=True  # temporarily nullable for migration
    )
    organisation = db.relationship(
        "Organisation",
        backref=db.backref("users", lazy="dynamic")
    )

    


    def is_admin(self) -> bool:
        return self.admin_level in (1, 2)

    def is_superuser(self) -> bool:
        return self.admin_level == 1


class AIAnalysisLog(db.Model):
    __tablename__ = "ai_analysis_log"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    filters = db.Column(db.Text)      # JSON string of filters applied
    record_count = db.Column(db.Integer)
    output_html = db.Column(db.Text)  # store AI output so you can review it later
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    user = db.relationship("User", backref="ai_logs")


# models_payrate.py (or wherever you keep models)
from datetime import datetime
from sqlalchemy import Index, CheckConstraint, UniqueConstraint
from extensions import db


class UploadBatch(db.Model):
    __tablename__ = "upload_batches"
    id = db.Column(db.Integer, primary_key=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    created_by = db.Column(db.String(120), nullable=True)  # username/email if available
    source_filename = db.Column(db.String(255), nullable=True)
    total_rows = db.Column(db.Integer, default=0, nullable=False)
    inserted_rows = db.Column(db.Integer, default=0, nullable=False)
    updated_rows = db.Column(db.Integer, default=0, nullable=False)
    skipped_rows = db.Column(db.Integer, default=0, nullable=False)
    errors_json = db.Column(db.Text, nullable=True)  # store aggregated errors per row


class PayRate(db.Model):
    __tablename__ = "pay_rates"
    id = db.Column(db.Integer, primary_key=True)

    employer_name = db.Column(db.String(200), nullable=False)
    organisation = db.Column(db.String(40), nullable=False)          # Competitor | Blue Ribbon | Forevermore
    role = db.Column(db.String(120), nullable=False)                  # e.g. Care Assistant, Senior Carer, RN
    contract_type = db.Column(db.String(20), nullable=False)          # employed | bank | agency

    base_rate = db.Column(db.Numeric(10, 2), nullable=False)          # £/hr
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
    upload_batch_id = db.Column(db.Integer, db.ForeignKey("upload_batches.id"), nullable=True)
    is_deleted = db.Column(db.Boolean, default=False, nullable=False)

    __table_args__ = (
        # Natural key to prevent duplicates per employer/role/place/date
        UniqueConstraint(
            "employer_name", "role", "contract_type", "postcode", "effective_from",
            name="uq_payrate_employer_role_contract_postcode_date"
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
    title = db.Column(db.String(255), nullable=False)
    company_name = db.Column(db.String(255), nullable=True)
    location_text = db.Column(db.String(255), nullable=True)
    postcode = db.Column(db.String(20), nullable=True)

    # NEW: high-level sector classification, e.g. "Social Care", "Nursing", "HR"
    sector = db.Column(db.String(100), nullable=True, index=True)

    min_rate = db.Column(db.Numeric(10, 2), nullable=True)
    max_rate = db.Column(db.Numeric(10, 2), nullable=True)
    rate_type = db.Column(db.String(50), nullable=True)      # hourly, annual, etc.
    contract_type = db.Column(db.String(50), nullable=True)

    source_site = db.Column(db.String(100), nullable=False)
    external_id = db.Column(db.String(255), nullable=True)
    url = db.Column(db.Text, nullable=True)

    posted_date = db.Column(db.Date, nullable=True)
    scraped_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    imported = db.Column(db.Boolean, default=False)

    raw_json = db.Column(db.Text, nullable=True)

    search_role = db.Column(db.String(255), nullable=True)
    search_location = db.Column(db.String(255), nullable=True)

    __table_args__ = (
        db.Index("ix_job_postings_source_ext", "source_site", "external_id"),
        db.Index("ix_job_postings_sector", "sector"),
    )


class Company(db.Model):
    __tablename__ = "companies"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), unique=True, index=True)
    canonical_name = db.Column(db.String(255), index=True)  # normalized form for fuzzy match
    sector = db.Column(db.String(50), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class JobRoleMapping(db.Model):
    __tablename__ = "job_role_mappings"

    id = db.Column(db.Integer, primary_key=True)
    raw_value = db.Column(db.Text, unique=True, nullable=False)
    canonical_role = db.Column(db.String(255), nullable=False)
    source = db.Column(db.String(50))  # optional: e.g. "adzuna", "indeed"
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(
        db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )


class CronRunLog(db.Model):
    __tablename__ = "cron_run_logs"

    id = db.Column(db.Integer, primary_key=True)
    job_name = db.Column(db.String(100), nullable=False)
    started_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    finished_at = db.Column(db.DateTime, nullable=True)
    status = db.Column(db.String(20), nullable=False, default="running")  # running/success/error
    message = db.Column(db.Text, nullable=True)
    rows_scraped = db.Column(db.Integer, nullable=True)
    records_created = db.Column(db.Integer, nullable=True)
    triggered_by = db.Column(db.String(150), nullable=True)
    trigger = db.Column(db.String(50), nullable=True)  # <-- ADD THIS
    day_label = db.Column(db.String(20))


class OnsEarnings(db.Model):
    __tablename__ = "ons_earnings"

    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, nullable=False)
    geography_code = db.Column(db.String(32), nullable=False, index=True)
    geography_name = db.Column(db.String(255), nullable=False)
    measure_code = db.Column(db.String(16), nullable=False)   # e.g. 20100, 20701
    value = db.Column(db.Float, nullable=True)

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint(
            "year", "geography_code", "measure_code",
            name="uq_ons_year_geo_measure",
        ),
    )



class Organisation(db.Model):
    __tablename__ = "organisations"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False, unique=True)
    slug = db.Column(db.String(255), nullable=False, unique=True)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    # optional: default plan placeholder (Epic 2)
    # default_plan_id = db.Column(db.Integer, nullable=True)

    def __repr__(self):
        return f"<Organisation {self.id} {self.slug}>"


