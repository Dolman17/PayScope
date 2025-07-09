from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

class JobRecord(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(100), nullable=False)
    sector = db.Column(db.String(100), nullable=False)
    postcode = db.Column(db.String(20), nullable=False)
    latitude = db.Column(db.Float, nullable=True)
    longitude = db.Column(db.Float, nullable=True)
    county = db.Column(db.String(100), nullable=True)
    job_role = db.Column(db.String(100), nullable=False)
    pay_rate = db.Column(db.Float, nullable=False)
    logo_filename = db.Column(db.String(200), nullable=True)
    imported_month = db.Column(db.String(20), nullable=True)
    imported_year = db.Column(db.Integer, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
