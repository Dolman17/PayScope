from __future__ import annotations

import os
import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from types import SimpleNamespace

from sqlalchemy import func

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
# Coverage-aware boosting (Reed-first)
# ---------------------------------------------------------------------
COVERAGE_BOOST_ENABLED = os.getenv("COVERAGE_BOOST_ENABLED", "1").lower() in ("1", "true", "yes")
COVERAGE_BOOST_SOURCE = (os.getenv("COVERAGE_BOOST_SOURCE", "reed") or "reed").strip().lower()
COVERAGE_BOOST_EXTRA_ROLES = int(os.getenv("COVERAGE_BOOST_EXTRA_ROLES", "6"))
COVERAGE_BOOST_WINDOW_DAYS = int(os.getenv("COVERAGE_BOOST_WINDOW_DAYS", "7"))
COVERAGE_BOOST_MIN_DAYS_SEEN = int(os.getenv("COVERAGE_BOOST_MIN_DAYS_SEEN", "2"))
CRON_MAX_TOTAL_ROLES = int(os.getenv("CRON_MAX_TOTAL_ROLES", "18"))

# Optional: override where list for boost only (comma-separated). If blank, uses today's wheres.
COVERAGE_BOOST_WHERE = os.getenv("COVERAGE_BOOST_WHERE", "").strip()

# ---------------------------------------------------------------------
# Geo backfill config
# ---------------------------------------------------------------------
GEO_BACKFILL_LIMIT = int(os.getenv("GEO_BACKFILL_LIMIT", "2000"))
GEO_BACKFILL_REVERSE_ENABLED = os.getenv("GEO_BACKFILL_REVERSE_ENABLED", "1").lower() in (
    "1",
    "true",
    "yes",
)
GEO_BACKFILL_REVERSE_DELAY = float(os.getenv("GEO_BACKFILL_REVERSE_DELAY", "1.0"))

# Map sector labels (from JobSummaryDaily.sector) to role keywords to search.
# Keep this small + editable. It’s steering, not taxonomy perfection.
SECTOR_ROLE_MAP: Dict[str, List[str]] = {
    # Care / health-ish
    "Social Care": [
        "Support Worker",
        "Care Assistant",
        "Senior Support Worker",
        "Registered Manager",
        "Service Manager",
    ],
    "Healthcare": ["Nurse", "RGN", "RMN", "Healthcare Assistant"],
    # Office / corp
    "Admin & Office": [
        "Administrator",
        "Office Administrator",
        "Office Manager",
        "Receptionist",
        "Executive Assistant",
    ],
    "HR": ["HR Advisor", "Recruiter", "HR Administrator"],
    "Finance": ["Accountant", "Finance Analyst", "Management Accountant"],
    # Tech
    "IT & Technology": ["Software Developer", "Web Developer", "Data Analyst", "BI Analyst"],
    # Ops / logistics / retail
    "Leadership & Management": ["Operations Manager", "Service Manager", "Store Manager"],
    "Logistics": ["Warehouse Operative", "Driver", "FLT Driver"],
    "Retail": ["Retail Assistant", "Store Manager"],
    # Trades / construction
    "Construction": ["Electrician", "Plumber", "Quantity Surveyor", "Site Manager"],
    # Hospitality (often patchy – keep modest)
    "Hospitality": ["Chef", "Kitchen Porter", "Bar Staff", "Waiting Staff"],
}


# ---------------------------------------------------------------------
# Weekly coverage config (roles + rotating locations)
# ---------------------------------------------------------------------
# 0 = Monday ... 6 = Sunday
DAY_CONFIG: Dict[int, Dict[str, Any]] = {
    0: {
        "label": "Mon",
        "roles": [
            "Support Worker",
            "Care Assistant",
            "Senior Support Worker",
            "Registered Manager",
            "Service Manager",
            "Operations Manager",
            "Cleaner",
            "Housekeeper",
            "Retail Assistant",
            "Store Manager",
        ],
        "where": ["London", "Croydon", "Brighton", "Reading", "Milton Keynes"],
    },
    1: {
        "label": "Tue",
        "roles": [
            "Nurse",
            "RGN",
            "RMN",
            "Housing Officer",
            "Support Officer",
            "Tenancy Officer",
            "Administrator",
            "Office Administrator",
            "Office Manager",
        ],
        "where": ["Birmingham", "Coventry", "Leicester", "Nottingham", "Stoke-on-Trent"],
    },
    2: {
        "label": "Wed",
        "roles": [
            "Software Developer",
            "Web Developer",
            "Data Analyst",
            "BI Analyst",
            "Research Analyst",
            "Insight Analyst",
        ],
        "where": ["Manchester", "Liverpool", "Preston", "Bolton", "Chester"],
    },
    3: {
        "label": "Thu",
        "roles": [
            "Accountant",
            "Finance Analyst",
            "Management Accountant",
            "HR Advisor",
            "Recruiter",
            "HR Administrator",
            "Paralegal",
            "Legal Assistant",
        ],
        "where": ["Leeds", "Sheffield", "Bradford", "Hull", "York"],
    },
    4: {
        "label": "Fri",
        "roles": [
            "Administrator",
            "Executive Assistant",
            "Receptionist",
            "Customer Service Advisor",
            "Call Centre Agent",
            "Marketing Executive",
            "Digital Marketing Executive",
            "Sales Executive",
        ],
        "where": ["Newcastle", "Sunderland", "Middlesbrough", "Durham", "Gateshead"],
    },
    5: {
        "label": "Sat",
        "roles": [
            "Trainer",
            "Learning and Development Trainer",
            "Assessor",
            "Teaching Assistant",
            "Tutor",
            "Housing Officer",
            "Support Worker (Housing)",
        ],
        "where": ["Bristol", "Bath", "Plymouth", "Exeter", "Swindon"],
    },
    6: {
        "label": "Sun",
        "roles": [
            "Electrician",
            "Plumber",
            "Site Manager",
            "Quantity Surveyor",
            "Warehouse Operative",
            "Operations Manager",
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
    """
    Backwards-compatible coverage block:
    - weak_sectors_count
    - weak_sectors (top 10 names)
    PLUS:
    - weak_sectors_detail: list[{sector, days_seen}]
    """
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

    weak_detail = []
    for row in sector_days:
        d = int(getattr(row, "days_seen", 0) or 0)
        if d < COVERAGE_BOOST_MIN_DAYS_SEEN:
            weak_detail.append({"sector": row.sector, "days_seen": d})

    # sort weakest first (0 days seen before 1 day seen)
    weak_detail.sort(key=lambda x: x.get("days_seen", 0))

    weak_sectors = [x["sector"] for x in weak_detail]

    return {
        "window_days": days,
        "weak_sectors_count": len(weak_sectors),
        "weak_sectors": weak_sectors[:10],
        "weak_sectors_detail": weak_detail,
    }


def _pick_coverage_boost_roles(coverage: dict, base_roles: List[str]) -> List[str]:
    """
    Take weak sectors and pick a small set of extra role queries to run.
    Deterministic + capped (safe for ops).
    """
    if not COVERAGE_BOOST_ENABLED:
        return []

    weak_detail = coverage.get("weak_sectors_detail") or []
    if not isinstance(weak_detail, list) or not weak_detail:
        return []

    # Build a weighted pool: 0 days => stronger push than 1 day
    weighted_roles: List[str] = []
    for row in weak_detail:
        sector = (row or {}).get("sector")
        days_seen = int((row or {}).get("days_seen") or 0)
        roles = SECTOR_ROLE_MAP.get(str(sector), [])
        if not roles:
            continue

        weight = 3 if days_seen <= 0 else 1
        for r in roles:
            weighted_roles.extend([r] * weight)

    if not weighted_roles:
        return []

    picked: List[str] = []
    seen = set(base_roles)

    for r in weighted_roles:
        if r in seen:
            continue
        picked.append(r)
        seen.add(r)
        if len(picked) >= max(COVERAGE_BOOST_EXTRA_ROLES, 0):
            break

    return picked


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


def _scrape_adzuna_for_roles(
    roles: List[str],
    where: str,
    run_stats: Dict[str, Any],
) -> List[Any]:
    out: List[Any] = []

    # Upper-bound estimate: 1 API hit per page
    estimated_hits = len(roles) * DEFAULT_MAX_PAGES
    run_stats.setdefault("sources", {}).setdefault("adzuna", {})
    run_stats["sources"]["adzuna"].setdefault("api_hits_estimated", 0)
    run_stats["sources"]["adzuna"]["api_hits_estimated"] += estimated_hits

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

        # Gentle throttle to stay well under per-minute limits
        if SLEEP_BETWEEN_QUERIES_SEC > 0:
            time.sleep(SLEEP_BETWEEN_QUERIES_SEC)

    return out


def _payload_to_rec_obj(payload: Dict[str, Any]) -> Any:
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
                try:
                    setattr(p, "search_role", role)
                    setattr(p, "search_location", where)
                except Exception:
                    pass
                out.append(p)

    return out


def _boost_where_list(today_wheres: List[str]) -> List[str]:
    if COVERAGE_BOOST_WHERE:
        return [w.strip() for w in COVERAGE_BOOST_WHERE.split(",") if w.strip()]
    return today_wheres


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
            "sources": {
                "adzuna": {"rows_scraped": 0, "created": 0, "updated": 0, "api_hits_estimated": 0},
                "reed": {"rows_scraped": 0, "created": 0, "updated": 0},
            },
            "coverage_boost": {
                "enabled": COVERAGE_BOOST_ENABLED,
                "source": COVERAGE_BOOST_SOURCE,
                "window_days": COVERAGE_BOOST_WINDOW_DAYS,
                "min_days_seen": COVERAGE_BOOST_MIN_DAYS_SEEN,
                "extra_roles_target": COVERAGE_BOOST_EXTRA_ROLES,
                "roles_picked": [],
                "wheres_used": [],
                "rows_scraped": 0,
            },
            "errors": [],
        }

        rows_scraped = 0
        records_created = 0

        try:
            # ONS import (optional)
            if ONS_IMPORT_ENABLED and import_ons_earnings_to_db:
                import_ons_earnings_to_db(ONS_IMPORT_YEAR)

            # Compute coverage early so we can steer tonight’s run
            coverage = _coverage_warnings(days=COVERAGE_BOOST_WINDOW_DAYS)
            run_stats["coverage"] = coverage

            # Pick coverage boost roles (kept separate from baseline roles)
            boost_roles = _pick_coverage_boost_roles(coverage, base_roles=roles)

            # Cap total roles (baseline + boost) so we never explode API usage
            merged_roles = list(roles)
            for r in boost_roles:
                if r not in merged_roles:
                    merged_roles.append(r)
                if len(merged_roles) >= CRON_MAX_TOTAL_ROLES:
                    break

            # Log steering decisions
            run_stats["coverage_boost"]["roles_picked"] = boost_roles[:]
            run_stats["coverage_boost"]["wheres_used"] = _boost_where_list(wheres)

            # -------------------------------
            # Baseline scrape (as before)
            # -------------------------------
            for where in wheres:
                # ---- Adzuna ----
                scraped_adzuna = _scrape_adzuna_for_roles(merged_roles, where, run_stats)
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

                # ---- Reed ----
                if REED_ENABLED:
                    scraped_reed = _scrape_reed_for_roles(merged_roles, where, run_stats)
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

            # -------------------------------
            # Coverage boost pass (Reed-first)
            # -------------------------------
            # We *only* do this when boost_roles exist and Reed is enabled.
            # By default we DO NOT add extra Adzuna calls.
            if boost_roles and COVERAGE_BOOST_ENABLED:
                boost_wheres = _boost_where_list(wheres)

                if COVERAGE_BOOST_SOURCE in ("reed", "both") and REED_ENABLED:
                    for where in boost_wheres:
                        boosted = _scrape_reed_for_roles(boost_roles, where, run_stats)
                        run_stats["coverage_boost"]["rows_scraped"] += len(boosted)

                        rows_scraped += len(boosted)
                        run_stats["sources"]["reed"]["rows_scraped"] += len(boosted)

                        for rec in boosted:
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

                # Optional: allow adzuna boosting later if you explicitly enable it
                if COVERAGE_BOOST_SOURCE in ("adzuna", "both"):
                    run_stats["errors"].append(
                        "Coverage boost for Adzuna is disabled by design unless you explicitly choose it. "
                        "Set COVERAGE_BOOST_SOURCE=both or adzuna to enable."
                    )

            # -------------------------------
            # Import any postings not yet imported (all sources)
            # -------------------------------
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

            # Summaries (yesterday)
            target = date.today() - timedelta(days=1)
            run_stats["summaries_created"] = build_daily_job_summaries(
                target_date=target,
                delete_existing=True,
            )

            # refresh coverage post-run
            run_stats["coverage_post_run"] = _coverage_warnings(days=COVERAGE_BOOST_WINDOW_DAYS)

            # keep existing keys updated
            run_stats["rows_scraped"] = rows_scraped

            msg = f"OK. Scraped={rows_scraped}, Imported={records_created}, BoostRoles={len(boost_roles)}"
            _finish_log(log, "success", msg, rows_scraped, records_created, run_stats)
            return {"ok": True, "message": msg}

        except Exception as e:
            db.session.rollback()
            run_stats["coverage_post_run"] = _coverage_warnings(days=COVERAGE_BOOST_WINDOW_DAYS)
            run_stats["rows_scraped"] = rows_scraped
            _finish_log(log, "error", str(e), rows_scraped, records_created, run_stats)
            return {"ok": False, "message": str(e)}


# ---------------------------------------------------------------------
# Job role canonicaliser
# ---------------------------------------------------------------------
def _clean_canonical_role(raw: str, text: str) -> str:
    """
    Defensive cleanup so we always store a plain label like:
    'Support Worker', not a sentence.
    """
    if not text:
        return raw.title()

    t = str(text).strip()

    # Strip surrounding quotes / code fences
    if t.startswith("```"):
        t = t.strip("`").strip()
    t = t.strip().strip('"').strip("'")

    # If the model returns a sentence, try to extract the label part
    lower = t.lower()
    for sep in (":", " is ", " - "):
        if sep in lower:
            parts = t.split(sep, 1)
            if len(parts) == 2:
                candidate = parts[1].strip().strip('"').strip("'")
                if candidate:
                    t = candidate
                    lower = t.lower()

    # Remove trailing punctuation
    t = t.strip().strip(".").strip()

    # Hard stop: if it's still long / sentencey, fall back
    words = t.split()
    if len(words) > 8:
        return raw.title()

    # Title-case unless it looks like an acronym-heavy role
    if any(w.isupper() and len(w) <= 5 for w in words):
        return " ".join(words)
    return " ".join(w[:1].upper() + w[1:] for w in words if w)


def run_job_role_canonicaliser(trigger: str = "manual", limit: int = 500) -> Dict[str, Any]:
    app = create_app()
    with app.app_context():
        log = _start_log("job_role_canonicaliser", trigger, None)

        updated = 0
        scanned = 0
        ai_ok = 0
        ai_fail = 0

        try:
            print(f"[CANON] Starting trigger={trigger} limit={limit}", flush=True)

            rows = (
                JobRoleMapping.query.filter(
                    (JobRoleMapping.canonical_role.is_(None))
                    | (func.trim(JobRoleMapping.canonical_role) == "")
                    | (JobRoleMapping.canonical_role == JobRoleMapping.raw_value)
                    | (func.lower(JobRoleMapping.canonical_role).like("the canonical%"))
                    | (func.lower(JobRoleMapping.canonical_role).like("a canonical%"))
                )
                .limit(limit)
                .all()
            )

            scanned = len(rows)
            print(f"[CANON] Rows fetched: {scanned}", flush=True)

            for idx, m in enumerate(rows, start=1):
                raw = (m.raw_value or "").strip()
                if not raw:
                    continue

                canonical = raw.title()

                if _openai_client:
                    try:
                        resp = _openai_client.responses.create(
                            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
                            input=(
                                "You are normalising job titles into a canonical UK job role label.\n"
                                "Return ONLY the canonical role label as plain text.\n"
                                "Rules:\n"
                                "- Output must be 2–6 words.\n"
                                "- No quotes, no punctuation, no prefixes like 'The canonical role is'.\n"
                                "- Title Case.\n"
                                "- If uncertain, return the cleaned title-case version of the input.\n"
                                f"Input: {raw}\n"
                                "Output:"
                            ),
                        )
                        canonical = _clean_canonical_role(
                            raw,
                            (resp.output_text or canonical)[:255],
                        )
                        ai_ok += 1
                    except Exception:
                        ai_fail += 1
                        canonical = raw.title()

                canonical = _clean_canonical_role(raw, canonical)

                if m.canonical_role != canonical:
                    m.canonical_role = canonical
                    m.updated_at = _utcnow()
                    updated += 1

                if idx % 25 == 0:
                    print(
                        f"[CANON] Progress {idx}/{scanned} updated={updated} ai_ok={ai_ok} ai_fail={ai_fail}",
                        flush=True,
                    )

            db.session.commit()
            msg = f"OK. Scanned={scanned}, Updated={updated}, AI_OK={ai_ok}, AI_FAIL={ai_fail}"
            print(f"[CANON] Done. {msg}", flush=True)
            _finish_log(log, "success", msg, 0, updated, None)
            return {"ok": True, "message": msg}

        except Exception as e:
            db.session.rollback()
            print(f"[CANON] ERROR: {e}", flush=True)
            _finish_log(log, "error", str(e), 0, updated, None)
            return {"ok": False, "message": str(e)}


# ---------------------------------------------------------------------
# Rebuild JobSummaryDaily (override-aware)
# ---------------------------------------------------------------------
def run_rebuild_job_summaries(
    trigger: str = "manual",
    days_back: int = 90,
) -> dict[str, Any]:
    from job_summaries import build_daily_job_summaries_range

    app = create_app()
    with app.app_context():
        log = CronRunLog(
            job_name="rebuild_job_summaries",
            started_at=datetime.utcnow(),
            status="running",
            trigger=trigger,
        )
        db.session.add(log)
        db.session.commit()

        try:
            end = date.today()
            start = end - timedelta(days=days_back)

            created = build_daily_job_summaries_range(
                start_date=start,
                end_date=end,
                delete_existing=True,
            )

            log.finished_at = datetime.utcnow()
            log.status = "success"
            log.message = f"Rebuilt summaries from {start} to {end}"
            log.records_created = created
            db.session.commit()

            return {"ok": True, "rows": created}

        except Exception as e:
            db.session.rollback()
            log.finished_at = datetime.utcnow()
            log.status = "error"
            log.message = str(e)
            db.session.commit()
            return {"ok": False, "error": str(e)}


# ---------------------------------------------------------------------
# Geo backfill: postcode, lat/lon, county
# ---------------------------------------------------------------------
def run_geo_backfill(trigger: str = "manual", limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Nightly geo backfill:
    - normalises postcodes
    - fills missing latitude/longitude from postcode
    - snaps coord-only rows to nearest postcode
    - reverse-geocodes county for rows missing JobRecord.county
    """
    app = create_app()
    with app.app_context():
        job_limit = int(limit or GEO_BACKFILL_LIMIT)
        log = _start_log("geo_backfill", trigger, None)

        stats: Dict[str, Any] = {
            "limit": job_limit,
            "processed": 0,
            "geocoded_from_postcode": 0,
            "snapped_from_latlon": 0,
            "county_updated": 0,
            "skipped_no_location": 0,
            "errors": [],
        }

        try:
            # Import here to avoid circular imports at module load time
            from app.blueprints.utils import (
                normalize_uk_postcode,
                geocode_postcode_cached,
                snap_to_nearest_postcode,
            )

            # Only rows that actually need work:
            # - missing lat/lon OR missing/blank county
            # - and we have *either* postcode or lat+lon to work with
            needing = (
                JobRecord.query.filter(
                    (
                        (JobRecord.latitude.is_(None))
                        | (JobRecord.longitude.is_(None))
                        | (JobRecord.county.is_(None))
                        | (func.trim(JobRecord.county) == "")
                    )
                )
                .filter(
                    (JobRecord.postcode.isnot(None))
                    | ((JobRecord.latitude.isnot(None)) & (JobRecord.longitude.isnot(None)))
                )
                .order_by(JobRecord.id.asc())
                .limit(job_limit)
                .all()
            )

            reverse = None
            if GEO_BACKFILL_REVERSE_ENABLED:
                try:
                    from geopy.geocoders import Nominatim  # type: ignore
                    from geopy.extra.rate_limiter import RateLimiter  # type: ignore

                    geolocator = Nominatim(user_agent="payscope-geo-backfill")
                    reverse = RateLimiter(
                        geolocator.reverse,
                        min_delay_seconds=GEO_BACKFILL_REVERSE_DELAY,
                    )
                except Exception as e:  # pragma: no cover - optional dependency
                    stats["errors"].append(f"reverse_geocoder_init: {e!r}")
                    reverse = None

            for job in needing:
                stats["processed"] += 1

                postcode = (job.postcode or "").strip()
                lat = job.latitude
                lon = job.longitude

                if not postcode and lat is None and lon is None:
                    stats["skipped_no_location"] += 1
                    continue

                # 1) Normalise + geocode from postcode if coords missing
                norm_pc: Optional[str] = None
                if postcode:
                    try:
                        norm_pc = normalize_uk_postcode(postcode) or postcode
                    except Exception as e:
                        stats["errors"].append(f"normalize_pc id={job.id}: {e!r}")
                        norm_pc = postcode

                if norm_pc and (lat is None or lon is None):
                    try:
                        new_lat, new_lon = geocode_postcode_cached(norm_pc)
                        if new_lat is not None and new_lon is not None:
                            job.latitude = new_lat
                            job.longitude = new_lon
                            job.postcode = norm_pc
                            lat, lon, postcode = new_lat, new_lon, norm_pc
                            stats["geocoded_from_postcode"] += 1
                    except Exception as e:
                        stats["errors"].append(f"geocode_pc id={job.id}: {e!r}")

                # 2) If we have coords but no postcode, snap to nearest postcode
                if (not postcode) and job.latitude is not None and job.longitude is not None:
                    try:
                        snapped_pc, snapped_lat, snapped_lon = snap_to_nearest_postcode(
                            job.latitude,
                            job.longitude,
                        )
                        if snapped_pc:
                            job.postcode = snapped_pc
                            postcode = snapped_pc
                        if snapped_lat is not None and snapped_lon is not None:
                            job.latitude = snapped_lat
                            job.longitude = snapped_lon
                        stats["snapped_from_latlon"] += 1
                    except Exception as e:
                        stats["errors"].append(f"snap_pc id={job.id}: {e!r}")

                # 3) Reverse-geocode county if still blank and we have coords
                existing_county = (job.county or "").strip()
                if reverse and not existing_county and job.latitude is not None and job.longitude is not None:
                    try:
                        loc = reverse((job.latitude, job.longitude), exactly_one=True)
                        if loc:
                            addr = loc.raw.get("address", {})
                            county_name = (
                                addr.get("county")
                                or addr.get("state_district")
                                or addr.get("state")
                            )
                            if county_name:
                                job.county = county_name
                                stats["county_updated"] += 1
                    except Exception as e:
                        stats["errors"].append(f"reverse_county id={job.id}: {e!r}")

            db.session.commit()

            msg = (
                f"Geo backfill processed={stats['processed']}, "
                f"geocoded={stats['geocoded_from_postcode']}, "
                f"snapped={stats['snapped_from_latlon']}, "
                f"county_updated={stats['county_updated']}"
            )
            _finish_log(log, "success", msg, 0, stats["processed"], stats)
            return {"ok": True, "message": msg, "stats": stats}

        except Exception as e:
            db.session.rollback()
            stats["errors"].append(repr(e))
            _finish_log(log, "error", str(e), 0, 0, stats)
            return {"ok": False, "error": str(e), "stats": stats}


# ---------------------------------------------------------------------
# Unified scheduled entrypoint (for Railway cron / admin button)
# ---------------------------------------------------------------------
def run_scheduled_jobs(trigger: str = "cron") -> Dict[str, Any]:
    """
    Run the standard nightly stack:
    - scrape/import + summaries
    - job role canonicaliser (optional via env)
    - geo backfill (optional via env)
    """
    results: Dict[str, Any] = {}

    results["scrape_import_and_summaries"] = run_scrape_import_and_summaries(trigger=trigger)

    if os.getenv("CRON_RUN_CANONICALISER", "1").lower() in ("1", "true", "yes"):
        canon_limit = int(os.getenv("CRON_CANON_LIMIT", "500"))
        results["job_role_canonicaliser"] = run_job_role_canonicaliser(
            trigger=f"{trigger}:canonicaliser",
            limit=canon_limit,
        )

    if os.getenv("CRON_RUN_GEO_BACKFILL", "1").lower() in ("1", "true", "yes"):
        gb_limit = int(os.getenv("GEO_BACKFILL_LIMIT", str(GEO_BACKFILL_LIMIT)))
        results["geo_backfill"] = run_geo_backfill(
            trigger=f"{trigger}:geo",
            limit=gb_limit,
        )

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="PayScope Cron Runner")
    parser.add_argument(
        "job",
        nargs="?",
        default="scrape-import",
        choices=["scrape-import", "job-role-canonicaliser", "rebuild-summaries", "geo-backfill", "scheduled"],
        help="Which job to run",
    )
    parser.add_argument("--trigger", default="manual", help="Trigger label for CronRunLog")
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Limit for canonicaliser rows or geo backfill batch size",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=90,
        help="Days back for rebuild-summaries job",
    )
    args = parser.parse_args()

    if args.job == "job-role-canonicaliser":
        print(run_job_role_canonicaliser(trigger=args.trigger, limit=args.limit))
    elif args.job == "rebuild-summaries":
        print(run_rebuild_job_summaries(trigger=args.trigger, days_back=args.days_back))
    elif args.job == "geo-backfill":
        print(run_geo_backfill(trigger=args.trigger, limit=args.limit))
    elif args.job == "scheduled":
        print(run_scheduled_jobs(trigger=args.trigger))
    else:
        print(run_scrape_import_and_summaries(trigger=args.trigger))
