import os

# Railway / cloud database URL (Postgres)
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    # Normalise old-style postgres://
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    # Use pg8000 driver so we don't depend on system libpq
    if DATABASE_URL.startswith("postgresql://"):
        SQLALCHEMY_DATABASE_URI = DATABASE_URL.replace(
            "postgresql://", "postgresql+pg8000://", 1
        )
    else:
        # Fallback if it's something unexpected
        SQLALCHEMY_DATABASE_URI = DATABASE_URL
else:
    # Local fallback – keep using your existing SQLite path
    SQLALCHEMY_DATABASE_URI = "sqlite:///app.db"

SQLALCHEMY_TRACK_MODIFICATIONS = False
