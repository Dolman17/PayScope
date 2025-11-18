# app/scrapers/__init__.py
from __future__ import annotations

"""
Scrapers package.

Deliberately kept minimal to avoid circular imports.
Import concrete scrapers explicitly, e.g.:

    from app.scrapers.adzuna import AdzunaScraper
    from app.scrapers.run import run_all

rather than importing via app.scrapers directly.
"""
