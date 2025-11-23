import os

# ---------------------------------------------------------
# SECRET KEY
# ---------------------------------------------------------
SECRET_KEY = os.getenv(
    "SECRET_KEY",
    "b9e3f4d2a8c74f019d5b0c6e2f38a1c44e7d9f5a30b812e9c4d53afa9120e7b3"
)

# ---------------------------------------------------------
# DATABASE CONFIG
# ---------------------------------------------------------
raw_db_url = os.getenv("DATABASE_URL")

if raw_db_url:
    db_url = raw_db_url

    # Railway sometimes uses postgres:// which SQLAlchemy rejects
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    # Use pg8000 to avoid needing psycopg2 system packages
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+pg8000://", 1)

    SQLALCHEMY_DATABASE_URI = db_url
else:
    # Local fallback only if user *intentionally* has no env var
    print("⚠ WARNING: DATABASE_URL not set — using SQLite fallback app.db")
    SQLALCHEMY_DATABASE_URI = "sqlite:///app.db"

SQLALCHEMY_TRACK_MODIFICATIONS = False
