import os

# ---- Secret key for sessions / login ----
# In production, set SECRET_KEY as an environment variable on Railway.
SECRET_KEY = os.getenv("SECRET_KEY", "b9e3f4d2a8c74f019d5b0c6e2f38a1c44e7d9f5a30b812e9c4d53afa9120e7b3")  # <- change in prod

# Railway / cloud database URL (Postgres)
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    # Railway often provides `postgres://` which SQLAlchemy dislikes
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    # Use pg8000 in production (no libpq / psycopg2 system deps)
    if DATABASE_URL.startswith("postgresql://"):
        DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+pg8000://", 1)

    SQLALCHEMY_DATABASE_URI = DATABASE_URL
else:
    # Local fallback – keep using your existing SQLite path
    SQLALCHEMY_DATABASE_URI = "sqlite:///app.db"

SQLALCHEMY_TRACK_MODIFICATIONS = False
