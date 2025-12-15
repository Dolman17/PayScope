from __future__ import annotations

import os
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from collections import Counter
from types import SimpleNamespace

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from app import create_app
from extensions import db
from models import (
    CronRunLog,
    JobPosting,
    JobRoleMapping,
    JobSummaryDaily,
)

from app.scrapers.adzuna import AdzunaScraper
from app.importers.job_importer import import_posting_to_record

from job_summaries import build_daily_job_summaries

# Optional ONS import
try:
    from ons_importer import import_ons_earnings_to_db
except Exception:
    import_ons_earnings_to_db = None  # type: ignore

# Optional OpenAI client
try:
    from openai import OpenAI  # type: ignore
    _openai_client = OpenAI()
except Exception:
    _openai_client = None


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
DEFAULT_RESULTS_PER_PAGE = int(os.getenv("ADZUNA_RESULTS_PER_PAGE", "50"))
DEFAULT_MAX_PAGES = int(os.getenv("ADZUNA_MAX_PAGES", "2"))
SLEEP_BETWEEN_QUERIES_SEC = float(os.getenv("SCRAPE_SLEEP_SEC", "0.25"))

ONS_IMPORT_ENABLED = os.getenv("ONS_IMPORT_ENABLED", "0").lower() in ("1", "true", "yes")
ONS_IMPORT_YEAR = int(os.getenv("ONS_IMPORT_YEAR", str(date.today().year - 1)))

# ---------------------------------------------------------------------
# Reed (optional second source)
# ---------------------------------------------------------------------
REED_ENABLED = os.getenv("REED_ENABLED", "0").lower() in ("1", "true", "yes")
REED_API_KEY = os.getenv("REED_API_KEY", "").strip()

REED_RESULTS_PER_PAGE = int(os.getenv("REED_RESULTS_PER_PAGE", "100"))  # Reed cap is 100
REED_MAX_PAGES = int(os.getenv("REED_MAX_PAGES", "1"))                  # conservative
REED_DISTANCE = int(os.getenv("REED_DISTANCE", "10"))
REED_THROTTLE_SECONDS = float(os.getenv("REED_THROTTLE_SECONDS", "1.0"))


# ---------------------------------------------------------------------
# Weekly coverage config (sectors + rotating locations)
# ---------------------------------------------------------------------
# 0 = Monday ... 6 = Sunday
DAY_CONFIG: Dict[int, Dict[str, Any]] = {
    0: {
        "label": "Mon",
        "roles": [
            "Support Worker", "Care Assistant", "Senior Support Worker",
            "Registered Manager", "Service Manager", "Operations Manager",
            "Cleaner", "Housekeeper",
            "Retail Assistant", "Store Manager",
        ],
        "where": ["London", "Croydon", "Brighton", "Reading", "Milton Keynes"],
    },
    1: {
        "label": "Tue",
        "roles": [
            "Nurse", "RGN", "RMN",
            "Housing Officer", "Support Officer", "Tenancy Officer",
            "Administrator", "Office Administrator", "Office Manager",
        ],
        "where": ["Birmingham", "Coventry", "Leicester", "Nottingham", "Stoke-on-Trent"],
    },
    2: {
        "label": "Wed",
        "roles": [
            "Software Developer", "Web Developer", "Data Analyst", "BI Analyst",
            "Research Analyst", "Insight Analyst",
        ],
        "where": ["Manchester", "Liverpool", "Preston", "Bolton", "Chester"],
    },
    3: {
        "label": "Thu",
        "roles": [
            "Accountant", "Finance Analyst", "Management Accountant",
            "HR Advisor", "Recruiter", "HR Administrator",
            "Paralegal", "Legal Assistant",
        ],
        "where": ["Leeds", "Sheffield", "Bradford", "Hull", "York"],
    },
    4: {
        "label": "Fri",
        "roles": [
            "Administrator", "Executive Assistant", "Receptionist",
            "Customer Service Advisor", "Call Centre Agent",
            "Marketing Executive", "Digital Marketing Executive", "Sales Executive",
        ],
        "where": ["Newcastle", "Sunderland", "Middlesbrough", "Durham", "Gateshead"],
    },
    5: {
        "label": "Sat",
        "roles": [
            "Trainer", "Learning and Development Trainer", "Assessor",
            "Teaching Assistant", "Tutor",
            "Housing Officer", "Support Worker (Housing)",
        ],
        "where": ["Bristol", "Bath", "Plymouth", "Exeter", "Swindon"],
    },
    6: {
        "label": "Sun",
        "roles": [
            "Electrician", "Plumber", "Site Manager", "Quantity Surveyor",
            "Warehouse Operative", "Operations Manager",
        ],
        "where": ["Glasgow", "Edinburgh", "Aberdeen", "Cardiff", "Belfast"],
    },
}


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json_dump_safe(data) -> str:
    try:
        return json.dumps(data, default=str)
    except Exception:
        return "{}"


def _start_log(job_name: str, trigger: str, day_label: Optional[str]) -> CronRunLog:
    log = CronRunLog(
        job_name=job_name,
        started_at=_utcnow(),
        status="running",
        trigger=trigger,
        day_label=day_label,
    )
    db.session.add(log)
    db.session.commit()
    return log


def _finish_log(
    log: CronRunLog,
    status: str,
    message: str,
    rows_scraped: int,
    records_created: int,
    run_stats: Optional[Dict[str, Any]],
) -> None:
    log.finished_at = _utcnow()
    log.status = status
    log.message = message
    log.rows_scraped = rows_scraped
    log.records_created = records_created
    if run_stats is not None:
        log.run_stats = _json_dump_safe(run_stats)
    db.session.commit()


def _coverage_warnings(days: int = 7) -> dict:
    start = date.today() - timedelta(days=days)

    sector_days = (
        db.session.query(
            JobSummaryDaily.sector,
            func.count(func.distinct(JobSummaryDaily.date)).label("days_seen"),
        )
        .filter(JobSummaryDaily.date >= start)
        .group_by(JobSummaryDaily.sector)
        .all()
    )

    weak_sectors = [row.sector for row in sector_days if (row.days_seen or 0) < 2]

    return {
        "window_days": days,
        "weak_sectors_count": len(weak_sectors),
        "weak_sectors": weak_sectors[:10],
    }


def _get_existing_posting(source_site: str, external_id: str | None) -> Optional[JobPosting]:
    if not external_id:
        return None
    return JobPosting.query.filter_by(
        source_site=source_site,
        external_id=external_id,
    ).first()


def _upsert_posting_from_scraper_record(
    source_site: str,
    rec: Any,
    search_role: Optional[str],
    search_location: Optional[str],
) -> Tuple[JobPosting, bool]:
    external_id = str(getattr(rec, "external_id", None) or "") or None
    existing = _get_existing_posting(source_site, external_id)

    raw_json = getattr(rec, "raw_json", None)
    try:
        raw_json_text = json.dumps(raw_json) if isinstance(raw_json, dict) else raw_json
    except Exception:
        raw_json_text = None

    if existing:
        existing.title = rec.title or existing.title
        existing.company_name = rec.company_name or existing.company_name
        existing.location_text = rec.location_text or existing.location_text
        existing.postcode = rec.postcode or existing.postcode
        existing.min_rate = rec.min_rate if rec.min_rate is not None else existing.min_rate
        existing.max_rate = rec.max_rate if rec.max_rate is not None else existing.max_rate
        existing.rate_type = rec.rate_type or existing.rate_type
        existing.contract_type = rec.contract_type or existing.contract_type
        existing.url = rec.url or existing.url
        existing.posted_date = rec.posted_date or existing.posted_date
        existing.scraped_at = _utcnow()
        existing.is_active = True
        existing.raw_json = raw_json_text or existing.raw_json
        existing.search_role = search_role
        existing.search_location = search_location
        db.session.commit()
        return existing, False

    posting = JobPosting(
        title=rec.title or "",
        company_name=rec.company_name,
        location_text=rec.location_text,
        postcode=rec.postcode,
        min_rate=rec.min_rate,
        max_rate=rec.max_rate,
        rate_type=rec.rate_type,
        contract_type=rec.contract_type,
        source_site=source_site,
        external_id=external_id,
        url=rec.url,
        posted_date=rec.posted_date,
        scraped_at=_utcnow(),
        is_active=True,
        imported=False,
        raw_json=raw_json_text,
        search_role=search_role,
        search_location=search_location,
    )
    db.session.add(posting)
    db.session.commit()
    return posting, True


def _scrape_adzuna_for_roles(roles: List[str], where: str) -> List[Any]:
    out: List[Any] = []
    for role in roles:
        scraper = AdzunaScraper(
            what=role,
            where=where,
            results_per_page=DEFAULT_RESULTS_PER_PAGE,
            max_pages=DEFAULT_MAX_PAGES,
        )
        batch = scraper.scrape() or []
        for r in batch:
            r.search_role = role
            r.search_location = where
        out.extend(batch)
    return out


def _payload_to_rec_obj(payload: Dict[str, Any]) -> Any:
    """
    Reed returns dict payloads (Adzuna-shaped). Convert to a record-like object so the
    existing upsert logic remains unchanged and low-risk.
    """
    posted = payload.get("posted_date")
    if isinstance(posted, datetime):
        posted = posted.date()

    return SimpleNamespace(
        title=(payload.get("title") or ""),
        company_name=payload.get("company_name"),
        location_text=payload.get("location_text"),
        postcode=payload.get("postcode"),
        min_rate=payload.get("min_rate"),
        max_rate=payload.get("max_rate"),
        rate_type=payload.get("rate_type"),
        contract_type=payload.get("contract_type"),
        source_site=(payload.get("source_site") or "reed"),
        external_id=str(payload.get("external_id") or "") or None,
        url=payload.get("url"),
        posted_date=posted if isinstance(posted, date) else None,
        raw_json=payload.get("raw_json"),
        search_role=payload.get("search_role"),
        search_location=payload.get("search_location"),
    )


def _scrape_reed_for_roles(roles: List[str], where: str, run_stats: Dict[str, Any]) -> List[Any]:
    """
    Conservative Reed scraping: per role+location, 1 page by default, throttled.

    ReedScraper contract (per your baseline):
    returns posting-shaped dicts with:
      title, company_name, location_text, postcode, min_rate, max_rate,
      rate_type, contract_type, source_site="reed", external_id, url,
      posted_date, raw_json
    """
    out: List[Any] = []

    if not REED_ENABLED:
        return out

    if not REED_API_KEY:
        run_stats.setdefault("errors", [])
        run_stats["errors"].append("REED_ENABLED=1 but REED_API_KEY is missing.")
        return out

    try:
        from app.scrapers.reed import ReedScraper  # type: ignore
    except Exception as e:
        run_stats.setdefault("errors", [])
        run_stats["errors"].append(f"Failed to import ReedScraper: {e}")
        return out

    for role in roles:
        scraper = ReedScraper(
            api_key=REED_API_KEY,
            keywords=role,
            location_name=where,
            distance_from_location=REED_DISTANCE,
            results_per_page=min(max(REED_RESULTS_PER_PAGE, 1), 100),
            max_pages=max(REED_MAX_PAGES, 1),
            throttle_seconds=max(REED_THROTTLE_SECONDS, 0.5),
        )

        payloads = scraper.scrape() or []
        for p in payloads:
            if isinstance(p, dict):
                p.setdefault("source_site", "reed")
                p["search_role"] = role
                p["search_location"] = where
                out.append(_payload_to_rec_obj(p))
            else:
                # If reed ever returns objects later, still support them.
                try:
                    setattr(p, "search_role", role)
                    setattr(p, "search_location", where)
                except Exception:
                    pass
                out.append(p)

    return out


# ---------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------
def run_scrape_import_and_summaries(trigger: str = "manual") -> Dict[str, Any]:
    app = create_app()
    with app.app_context():

        override_roles = os.getenv("CRON_WHAT")
        override_where = os.getenv("CRON_WHERE")

        if override_roles:
            roles = [r.strip() for r in override_roles.split(",") if r.strip()]
            wheres = [override_where] if override_where else ["United Kingdom"]
            day_label = "override"
        else:
            cfg = DAY_CONFIG.get(date.today().weekday(), DAY_CONFIG[0])
            roles = cfg["roles"]
            wheres = cfg["where"] if isinstance(cfg["where"], list) else [cfg["where"]]
            day_label = cfg["label"]

        log = _start_log("scrape_import_summaries", trigger, day_label)

        run_stats: Dict[str, Any] = {
            "roles": roles,
            "where": wheres,
            "day_label": day_label,
            "trigger": trigger,
            "rows_scraped": 0,
            "postings_created": 0,
            "postings_updated": 0,
            "postings_imported_success": 0,
            "postings_import_failed": 0,
            "summaries_created": 0,
            # Additive stats (does not affect behaviour)
            "sources": {
                "adzuna": {"rows_scraped": 0, "created": 0, "updated": 0},
                "reed": {"rows_scraped": 0, "created": 0, "updated": 0},
            },
            "errors": [],
        }

        rows_scraped = 0
        records_created = 0

        try:
            if ONS_IMPORT_ENABLED and import_ons_earnings_to_db:
                import_ons_earnings_to_db(ONS_IMPORT_YEAR)

            for where in wheres:
                # ---- Adzuna (existing source) ----
                scraped_adzuna = _scrape_adzuna_for_roles(roles, where)
                rows_scraped += len(scraped_adzuna)
                run_stats["sources"]["adzuna"]["rows_scraped"] += len(scraped_adzuna)

                for rec in scraped_adzuna:
                    _, created = _upsert_posting_from_scraper_record(
                        "adzuna",
                        rec,
                        rec.search_role,
                        rec.search_location,
                    )
                    if created:
                        run_stats["postings_created"] += 1
                        run_stats["sources"]["adzuna"]["created"] += 1
                    else:
                        run_stats["postings_updated"] += 1
                        run_stats["sources"]["adzuna"]["updated"] += 1

                # ---- Reed (optional second source) ----
                if REED_ENABLED:
                    scraped_reed = _scrape_reed_for_roles(roles, where, run_stats)
                    rows_scraped += len(scraped_reed)
                    run_stats["sources"]["reed"]["rows_scraped"] += len(scraped_reed)

                    for rec in scraped_reed:
                        _, created = _upsert_posting_from_scraper_record(
                            "reed",
                            rec,
                            getattr(rec, "search_role", None),
                            getattr(rec, "search_location", None),
                        )
                        if created:
                            run_stats["postings_created"] += 1
                            run_stats["sources"]["reed"]["created"] += 1
                        else:
                            run_stats["postings_updated"] += 1
                            run_stats["sources"]["reed"]["updated"] += 1

            # Import any postings not yet imported (all sources)
            postings = JobPosting.query.filter(JobPosting.imported.is_(False)).all()
            for p in postings:
                try:
                    import_posting_to_record(p)
                    p.imported = True
                    records_created += 1
                    run_stats["postings_imported_success"] += 1
                except Exception:
                    run_stats["postings_import_failed"] += 1

            db.session.commit()

            target = date.today() - timedelta(days=1)
            run_stats["summaries_created"] = build_daily_job_summaries(
                target_date=target,
                delete_existing=True,
            )

            run_stats["coverage"] = _coverage_warnings(days=7)

            # keep existing keys updated
            run_stats["rows_scraped"] = rows_scraped

            msg = f"OK. Scraped={rows_scraped}, Imported={records_created}"
            _finish_log(log, "success", msg, rows_scraped, records_created, run_stats)
            return {"ok": True, "message": msg}

        except Exception as e:
            db.session.rollback()
            run_stats["coverage"] = _coverage_warnings(days=7)
            run_stats["rows_scraped"] = rows_scraped
            _finish_log(log, "error", str(e), rows_scraped, records_created, run_stats)
            return {"ok": False, "message": str(e)}


# ---------------------------------------------------------------------
# Job role canonicaliser
# ---------------------------------------------------------------------
def run_job_role_canonicaliser(trigger: str = "manual", limit: int = 500) -> Dict[str, Any]:
    app = create_app()
    with app.app_context():
        log = _start_log("job_role_canonicaliser", trigger, None)

        updated = 0
        scanned = 0

        try:
            rows = (
                JobRoleMapping.query
                .filter(
                    (JobRoleMapping.canonical_role.is_(None)) |
                    (func.trim(JobRoleMapping.canonical_role) == "") |
                    (JobRoleMapping.canonical_role == JobRoleMapping.raw_value)
                )
                .limit(limit)
                .all()
            )

            scanned = len(rows)

            for m in rows:
                raw = (m.raw_value or "").strip()
                if not raw:
                    continue

                canonical = raw.title()
                if _openai_client:
                    try:
                        resp = _openai_client.responses.create(
                            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
                            input=f"Return a canonical job role for: {raw}",
                        )
                        canonical = (resp.output_text or canonical)[:255]
                    except Exception:
                        pass

                if m.canonical_role != canonical:
                    m.canonical_role = canonical
                    m.updated_at = _utcnow()
                    updated += 1

            db.session.commit()
            msg = f"OK. Scanned={scanned}, Updated={updated}"
            _finish_log(log, "success", msg, 0, updated, None)
            return {"ok": True, "message": msg}

        except Exception as e:
            db.session.rollback()
            _finish_log(log, "error", str(e), 0, updated, None)
            return {"ok": False, "message": str(e)}


if __name__ == "__main__":
    print(run_scrape_import_and_summaries(trigger="manual"))
