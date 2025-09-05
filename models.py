# models.py
from datetime import datetime
from flask_login import UserMixin
from extensions import db  # shared db from extensions.py


class JobRecord(db.Model):
    __tablename__ = "job_record"

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(50), index=True)
    company_name = db.Column(db.String(100))
    sector = db.Column(db.String(50), index=True)
    job_role = db.Column(db.String(100), index=True)
    postcode = db.Column(db.String(20))
    county = db.Column(db.String(50), index=True)
    pay_rate = db.Column(db.Float)
    imported_month = db.Column(db.String(20), index=True)
    imported_year = db.Column(db.String(10), index=True)  # keep as string to match filters
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Optional legacy field; safe to keep if you were storing filenames
    logo_filename = db.Column(db.String(200))


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(150), unique=True, nullable=False)
    password = db.Column(db.String(200), nullable=False)
    admin_level = db.Column(db.Integer, default=0)  # 0=user, 1=admin, 2=superuser

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
