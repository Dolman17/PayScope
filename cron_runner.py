# cron_runner.py
from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from app import create_app
from extensions import db
from models import CronRunLog, JobPosting, JobRoleMapping

from app.scrapers.adzuna import AdzunaScraper
from app.importers.job_importer import import_posting_to_record

from job_summaries import build_daily_job_summaries

# Optional ONS import (safe to skip if you don't want it in cron yet)
try:
    from ons_importer import import_ons_earnings_to_db
except Exception:
    import_ons_earnings_to_db = None  # type: ignore


# Optional OpenAI client for job-role canonicalisation (safe if missing)
try:
    from openai import OpenAI  # type: ignore

    _openai_client = OpenAI()
except Exception:
    _openai_client = None


# -----------------------------
# Config
# -----------------------------
DEFAULT_RESULTS_PER_PAGE = int(os.getenv("ADZUNA_RESULTS_PER_PAGE", "50"))
DEFAULT_MAX_PAGES = int(os.getenv("ADZUNA_MAX_PAGES", "2"))

SLEEP_BETWEEN_QUERIES_SEC = float(os.getenv("SCRAPE_SLEEP_SEC", "0.25"))

# 0 = Monday ... 6 = Sunday
DAY_CONFIG: Dict[int, Dict[str, Any]] = {
    0: {"roles": ["Support Worker", "Care Assistant"], "where": "United Kingdom", "label": "Mon"},
    1: {"roles": ["Nurse", "RMN", "RGN"], "where": "United Kingdom", "label": "Tue"},
    2: {"roles": ["HR Advisor", "Recruiter"], "where": "United Kingdom", "label": "Wed"},
    3: {"roles": ["Finance Analyst", "Accountant"], "where": "United Kingdom", "label": "Thu"},
    4: {"roles": ["Developer", "Data Analyst"], "where": "United Kingdom", "label": "Fri"},
    5: {"roles": ["Customer Service Advisor"], "where": "United Kingdom", "label": "Sat"},
    6: {"roles": ["Operations Manager", "Warehouse Operative"], "where": "United Kingdom", "label": "Sun"},
}


def _utcnow() -> datetime:
    return datetime.utcnow()


def _start_log(job_name: str, trigger: str, day_label: Optional[str] = None) -> CronRunLog:
    log = CronRunLog(
        job_name=job_name,
        started_at=_utcnow(),
        status="running",
        message=None,
        rows_scraped=0,
        records_created=0,
        triggered_by=os.getenv("TRIGGERED_BY") or None,
        trigger=trigger,
        day_label=day_label,
    )
    db.session.add(log)
    db.session.commit()
    return log


def _finish_log(log: CronRunLog, status: str, message: str, rows_scraped: int, records_created: int) -> None:
    log.finished_at = _utcnow()
    log.status = status
    log.message = message
    log.rows_scraped = rows_scraped
    log.records_created = records_created
    db.session.add(log)
    db.session.commit()


def _get_existing_posting(source_site: str, external_id: str | None) -> Optional[JobPosting]:
    if not external_id:
        return None
    return (
        JobPosting.query
        .filter(JobPosting.source_site == source_site, JobPosting.external_id == external_id)
        .first()
    )


def _upsert_posting_from_scraper_record(
    source_site: str,
    rec: Any,
    search_role: str | None = None,
    search_location: str | None = None,
) -> Tuple[JobPosting, bool]:
    """
    Convert a scraper JobRecord (temporary object from app/scrapers/base.py)
    into a DB JobPosting, upserting on (source_site, external_id).

    Returns (posting, created_new)
    """
    external_id = getattr(rec, "external_id", None)
    external_id = str(external_id) if external_id else None

    existing = _get_existing_posting(source_site, external_id)

    # Pull fields defensively
    title = getattr(rec, "title", None)
    company_name = getattr(rec, "company_name", None)
    location_text = getattr(rec, "location_text", None)
    postcode = getattr(rec, "postcode", None)
    sector = getattr(rec, "sector", None)  # usually None from AdzunaScraper; OK
    min_rate = getattr(rec, "min_rate", None)
    max_rate = getattr(rec, "max_rate", None)
    rate_type = getattr(rec, "rate_type", None)
    contract_type = getattr(rec, "contract_type", None)
    url = getattr(rec, "url", None)
    posted_date = getattr(rec, "posted_date", None)

    raw_json = getattr(rec, "raw_json", None)
    # Your DB column is Text, so str(dict) is fine; JSON is better but not required.
    # If raw_json is already a dict, store json.dumps.
    try:
        import json
        raw_json_text = json.dumps(raw_json, ensure_ascii=False) if isinstance(raw_json, dict) else (raw_json or None)
    except Exception:
        raw_json_text = None

    if existing:
        existing.title = title or existing.title
        existing.company_name = company_name or existing.company_name
        existing.location_text = location_text or existing.location_text
        existing.postcode = postcode or existing.postcode
        existing.sector = sector or existing.sector

        existing.min_rate = min_rate if min_rate is not None else existing.min_rate
        existing.max_rate = max_rate if max_rate is not None else existing.max_rate
        existing.rate_type = rate_type or existing.rate_type
        existing.contract_type = contract_type or existing.contract_type

        existing.url = url or existing.url
        existing.posted_date = posted_date or existing.posted_date

        existing.scraped_at = _utcnow()
        existing.is_active = True
        existing.raw_json = raw_json_text or existing.raw_json

        if search_role:
            existing.search_role = search_role
        if search_location:
            existing.search_location = search_location

        db.session.add(existing)
        db.session.commit()
        return existing, False

    posting = JobPosting(
        title=title or "",
        company_name=company_name,
        location_text=location_text,
        postcode=postcode,
        sector=sector,
        min_rate=min_rate,
        max_rate=max_rate,
        rate_type=rate_type,
        contract_type=contract_type,
        source_site=source_site,
        external_id=external_id,
        url=url,
        posted_date=posted_date,
        scraped_at=_utcnow(),
        is_active=True,
        imported=False,
        raw_json=raw_json_text,
        search_role=search_role,
        search_location=search_location,
    )
    db.session.add(posting)

    try:
        db.session.commit()
        return posting, True
    except IntegrityError:
        db.session.rollback()
        again = _get_existing_posting(source_site, external_id)
        if again:
            return again, False
        raise


def _scrape_adzuna_for_roles(roles: List[str], where: str) -> List[Any]:
    """
    For each role, instantiate AdzunaScraper(what=role, where=where) and call .scrape().
    Returns list of scraper JobRecord objects (temporary).
    """
    out: List[Any] = []
    for role in roles:
        scraper = AdzunaScraper(
            what=role,
            where=where,
            results_per_page=DEFAULT_RESULTS_PER_PAGE,
            max_pages=DEFAULT_MAX_PAGES,
        )
        batch = scraper.scrape() or []
        # annotate in-memory (optional)
        for r in batch:
            try:
                r.search_role = role
                r.search_location = where
            except Exception:
                pass
        out.extend(batch)
    return out


def run_scrape_import_and_summaries(trigger: str = "manual") -> Dict[str, Any]:
    """
    Main pipeline:
      - optional ONS import
      - scrape Adzuna
      - upsert JobPosting
      - import JobPosting -> JobRecord (sector/role normalisation happens in job_importer)
      - build daily summaries for yesterday
    """
    app = create_app()
    with app.app_context():
        weekday = date.today().weekday()
        cfg = DAY_CONFIG.get(weekday) or DAY_CONFIG[0]
        day_label = cfg.get("label")

        log = _start_log("scrape_import_summaries", trigger=trigger, day_label=day_label)

        rows_scraped = 0
        records_created = 0
        created_postings = 0

        try:
            # 1) ONS import (optional, non-fatal)
            if import_ons_earnings_to_db is not None:
                try:
                    import_ons_earnings_to_db()
                except Exception as e:
                    print(f"[CRON] ONS import skipped/failed: {e}")

            # 2) Scrape
            roles = list(cfg.get("roles") or [])
            where = str(cfg.get("where") or "United Kingdom")
            scraped = _scrape_adzuna_for_roles(roles, where=where)
            rows_scraped = len(scraped)

            # 3) Upsert JobPosting
            for rec in scraped:
                role = getattr(rec, "search_role", None) or None
                loc = getattr(rec, "search_location", None) or None
                _, created = _upsert_posting_from_scraper_record("adzuna", rec, search_role=role, search_location=loc)
                if created:
                    created_postings += 1

            # 4) Import unimported postings to JobRecord
            postings = (
                JobPosting.query
                .filter(JobPosting.imported.is_(False))
                .order_by(JobPosting.id.asc())
                .all()
            )
            for p in postings:
                import_posting_to_record(p)
                records_created += 1

            db.session.commit()

            # 5) Build daily summaries (cron stays light)
            target = date.today() - timedelta(days=1)
            created_summaries = build_daily_job_summaries(target_date=target, delete_existing=True)

            msg = (
                f"OK. Scraped={rows_scraped}, NewPostings={created_postings}, "
                f"Imported={records_created}, Summaries({target})={created_summaries}"
            )
            _finish_log(log, "success", msg, rows_scraped, records_created)
            return {"ok": True, "message": msg}

        except Exception as e:
            db.session.rollback()
            msg = f"ERROR: {e}"
            _finish_log(log, "error", msg, rows_scraped, records_created)
            return {"ok": False, "message": msg}


def run_job_role_canonicaliser(trigger: str = "manual", limit: int = 500) -> Dict[str, Any]:
    """
    Exists mainly so admin.py can import it safely:
      from cron_runner import run_job_role_canonicaliser

    If OpenAI isn't configured, it falls back to deterministic title-casing.
    """
    app = create_app()
    with app.app_context():
        log = _start_log("job_role_canonicaliser", trigger=trigger, day_label=None)

        updated = 0
        scanned = 0

        try:
            q = (
                JobRoleMapping.query
                .filter(
                    (JobRoleMapping.canonical_role.is_(None)) |
                    (func.trim(JobRoleMapping.canonical_role) == "") |
                    (JobRoleMapping.canonical_role == JobRoleMapping.raw_value)
                )
                .order_by(JobRoleMapping.id.asc())
                .limit(limit)
            )

            rows = q.all()
            scanned = len(rows)

            for m in rows:
                raw = (m.raw_value or "").strip()
                if not raw:
                    continue

                canonical = raw.title()

                if _openai_client is not None:
                    try:
                        prompt = (
                            "Normalise this job title into a short canonical role group.\n"
                            "Return ONLY the canonical role name.\n"
                            f"Raw job title: {raw}"
                        )
                        resp = _openai_client.responses.create(
                            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
                            input=prompt,
                        )
                        text = (resp.output_text or "").strip()
                        if text:
                            canonical = text[:255]
                    except Exception:
                        pass

                if m.canonical_role != canonical:
                    m.canonical_role = canonical
                    m.updated_at = _utcnow()
                    db.session.add(m)
                    updated += 1

            db.session.commit()

            msg = f"OK. Scanned={scanned}, Updated={updated}"
            _finish_log(log, "success", msg, rows_scraped=0, records_created=updated)
            return {"ok": True, "message": msg}

        except Exception as e:
            db.session.rollback()
            msg = f"ERROR: {e}"
            _finish_log(log, "error", msg, rows_scraped=0, records_created=updated)
            return {"ok": False, "message": msg}


if __name__ == "__main__":
    print(run_scrape_import_and_summaries(trigger="manual"))
