# cron_runner.py
from __future__ import annotations

import json
import os
import time
from datetime import datetime, date

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from app.scrapers.adzuna import AdzunaScraper
from app.importers.job_importer import import_posting_to_record, classify_sector
from extensions import db
from models import JobPosting, CronRunLog, JobRecord, JobRoleMapping
from job_summaries import build_daily_job_summaries  # Daily summaries
from ons_importer import import_ons_earnings_to_db   # ONS → DB upsert


# ------------------------------
# Safety / runtime guardrails
# ------------------------------
CRON_TIME_BUDGET_SECONDS = int(os.getenv("CRON_TIME_BUDGET_SECONDS", "85"))  # aim < 90s
ADZUNA_PAIR_SLEEP_SECONDS = float(os.getenv("ADZUNA_PAIR_SLEEP_SECONDS", "0.15"))

MAX_TOTAL_PAIRS_PER_RUN = int(os.getenv("MAX_TOTAL_PAIRS_PER_RUN", "30"))

ADZUNA_MAX_PAGES = int(os.getenv("ADZUNA_MAX_PAGES", "2"))
ADZUNA_RESULTS_PER_PAGE = int(os.getenv("ADZUNA_RESULTS_PER_PAGE", "40"))

CANON_DAILY_ENABLED = os.getenv("CANON_DAILY_ENABLED", "1").strip() == "1"
CANON_DAILY_MAX_ROLES = int(os.getenv("CANON_DAILY_MAX_ROLES", "100"))
CANON_DAILY_CHUNK_SIZE = int(os.getenv("CANON_DAILY_CHUNK_SIZE", "25"))


# Optional OpenAI client for AI canonicalisation
try:
    from openai import OpenAI
    _openai_client = OpenAI()
except Exception:
    _openai_client = None


def _truncate(value, max_len: int | None):
    """Safely truncate strings for fixed-length VARCHAR columns."""
    if value is None or max_len is None:
        return value
    s = str(value)
    if len(s) <= max_len:
        return s
    return s[:max_len]


# -------------------------------------------------------------------
# Day-of-week scrape configuration
# -------------------------------------------------------------------
DAY_CONFIG = {
    0: {  # Monday – Social Care & Nursing
        "label": "Social Care & Nursing",
        "roles": [
            "support worker",
            "care assistant",
            "senior care assistant",
            "healthcare assistant",
            "nurse",
            "registered nurse",
            "team leader",
            "deputy manager",
        ],
        "locations": [
            "United Kingdom",
            "London",
            "Birmingham",
            "Manchester",
            "Leeds",
            "Glasgow",
        ],
    },
    1: {  # Tuesday – IT & Tech
        "label": "IT & Technology",
        "roles": [
            "software developer",
            "software engineer",
            "it support",
            "data analyst",
            "business analyst",
            "devops engineer",
        ],
        "locations": [
            "London",
            "Manchester",
            "Birmingham",
            "Leeds",
            "Bristol",
        ],
    },
    2: {  # Wednesday – Finance & Accounting
        "label": "Finance & Accounting",
        "roles": [
            "accountant",
            "finance manager",
            "financial analyst",
            "bookkeeper",
            "payroll clerk",
        ],
        "locations": [
            "London",
            "Manchester",
            "Leeds",
            "Edinburgh",
        ],
    },
    3: {  # Thursday – HR, Admin & Operations
        "label": "HR, Admin & Operations",
        "roles": [
            "hr advisor",
            "hr manager",
            "recruitment consultant",
            "office manager",
            "administrator",
            "operations manager",
        ],
        "locations": [
            "United Kingdom",
            "London",
            "Birmingham",
            "Manchester",
        ],
    },
    4: {  # Friday – Mixed Support Roles
        "label": "Support & Customer",
        "roles": [
            "customer service advisor",
            "call centre advisor",
            "receptionist",
            "support officer",
        ],
        "locations": [
            "United Kingdom",
            "London",
            "Birmingham",
            "Leeds",
        ],
    },
    5: {  # Saturday – Light Social Care refresh
        "label": "Weekend Social Care",
        "roles": [
            "support worker",
            "care assistant",
            "senior care assistant",
        ],
        "locations": [
            "United Kingdom",
            "London",
            "Manchester",
        ],
    },
    6: {  # Sunday – Light Nursing / Care
        "label": "Weekend Nursing & Care",
        "roles": [
            "nurse",
            "registered nurse",
            "support worker",
        ],
        "locations": [
            "United Kingdom",
            "London",
            "Birmingham",
        ],
    },
}


# -------------------------------------------------------------------
# Gap-filling config: target coverage + mappings
# -------------------------------------------------------------------
SECTOR_TARGET_MIN = 1500
COUNTY_TARGET_MIN = 800

MAX_GAP_SECTOR_JOBS = 6
MAX_GAP_COUNTY_JOBS = 6

SECTOR_KEYWORDS = {
    "Education & Training": ["teacher", "lecturer", "trainer", "tutor"],
    "Legal": ["solicitor", "paralegal", "legal assistant"],
    "Sales & Marketing": ["sales executive", "sales manager", "marketing manager", "business development"],
    "Customer Service": ["customer service advisor", "call centre advisor", "contact centre"],
    "HR & Recruitment": ["hr advisor", "hr officer", "recruitment consultant"],
    "Finance & Accounting": ["accountant", "finance analyst", "bookkeeper"],
    "IT & Digital": ["web developer", "frontend developer", "backend developer", "python developer"],
    "Retail": ["retail assistant", "store manager", "sales assistant", "merchandiser", "supervisor"],
}

COUNTY_LOCATIONS = {
    "Staffordshire": "Staffordshire",
    "Cheshire": "Cheshire",
    "Norfolk": "Norfolk",
    "Lancashire": "Lancashire",
    "Herefordshire": "Herefordshire",
    "Shropshire": "Shropshire",
    "Worcestershire": "Worcestershire",
    "West Midlands": "West Midlands",
    "Greater Manchester": "Greater Manchester",
    "Bristol": "Bristol",
    "Glasgow": "Glasgow",
    "Edinburgh": "Edinburgh",
    "London": "London",
    "Leeds": "Leeds",
    "Birmingham": "Birmingham",
}


def get_underrepresented_sectors(limit: int = 20) -> list[str]:
    rows = (
        db.session.query(JobRecord.sector, func.count(JobRecord.id))
        .filter(JobRecord.sector.isnot(None))
        .group_by(JobRecord.sector)
        .order_by(func.count(JobRecord.id).asc())
        .limit(limit)
        .all()
    )

    result: list[str] = []
    for sector, count in rows:
        if not sector:
            continue
        if sector in SECTOR_KEYWORDS and (count or 0) < SECTOR_TARGET_MIN:
            result.append(sector)
    return result


def get_underrepresented_counties(limit: int = 50) -> list[str]:
    rows = (
        db.session.query(JobRecord.county, func.count(JobRecord.id))
        .filter(JobRecord.county.isnot(None))
        .group_by(JobRecord.county)
        .order_by(func.count(JobRecord.id).asc())
        .limit(limit)
        .all()
    )

    result: list[str] = []
    for county, count in rows:
        if not county:
            continue
        if county in COUNTY_LOCATIONS and (count or 0) < COUNTY_TARGET_MIN:
            result.append(county)
    return result


def build_gap_fill_pairs() -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for sector in get_underrepresented_sectors():
        keywords = SECTOR_KEYWORDS.get(sector) or []
        for kw in keywords:
            pair = (kw, "United Kingdom")
            if pair in seen:
                continue
            pairs.append(pair)
            seen.add(pair)
            if len([p for p in pairs if p[0]]) >= MAX_GAP_SECTOR_JOBS:
                break
        if len([p for p in pairs if p[0]]) >= MAX_GAP_SECTOR_JOBS:
            break

    for county in get_underrepresented_counties():
        where = COUNTY_LOCATIONS[county]
        pair = ("", where)
        if pair in seen:
            continue
        pairs.append(pair)
        seen.add(pair)
        if len([p for p in pairs if p[0] == ""]) >= MAX_GAP_COUNTY_JOBS:
            break

    return pairs


def _find_existing_posting(source_site: str, external_id: str | None, url: str | None):
    q = None
    if external_id:
        q = JobPosting.query.filter_by(source_site=source_site, external_id=external_id)
    elif url:
        q = JobPosting.query.filter_by(source_site=source_site, url=url)
    return q.first() if q is not None else None


def _build_pairs(roles: list[str], locations: list[str], extra_pairs: list[tuple[str, str]] | None):
    pairs_set: set[tuple[str, str]] = set()
    for role in roles:
        for loc in locations:
            pairs_set.add((role, loc))
    if extra_pairs:
        for r, l in extra_pairs:
            pairs_set.add((r, l))

    pairs = sorted(pairs_set)
    if len(pairs) > MAX_TOTAL_PAIRS_PER_RUN:
        pairs = pairs[:MAX_TOTAL_PAIRS_PER_RUN]
    return pairs


def _run_for_config(
    label: str,
    roles: list[str],
    locations: list[str],
    extra_pairs: list[tuple[str, str]] | None = None,
    started_monotonic: float | None = None,
    time_budget_seconds: int | None = None,
) -> dict:
    rows_scraped = 0
    records_created = 0
    postings_created = 0
    postings_updated = 0
    errors: list[str] = []
    stopped_early = False

    pairs = _build_pairs(roles, locations, extra_pairs)

    for role, loc in pairs:
        if started_monotonic is not None and time_budget_seconds is not None:
            if (time.monotonic() - started_monotonic) > time_budget_seconds:
                stopped_early = True
                msg = f"{label}: stopping early due to time budget ({time_budget_seconds}s)."
                print("⏱️", msg)
                errors.append(msg)
                break

        if ADZUNA_PAIR_SLEEP_SECONDS > 0:
            time.sleep(ADZUNA_PAIR_SLEEP_SECONDS)

        try:
            scraper = AdzunaScraper(
                what=role,
                where=loc,
                max_pages=ADZUNA_MAX_PAGES,
                results_per_page=ADZUNA_RESULTS_PER_PAGE,
            )
            results = scraper.scrape()

            for rec in results:
                rows_scraped += 1

                existing = _find_existing_posting(
                    source_site=rec.source_site,
                    external_id=rec.external_id,
                    url=rec.url,
                )

                now = datetime.utcnow()
                sector_value = classify_sector(rec.title, role)

                title = _truncate(rec.title, 255)
                company_name = _truncate(rec.company_name, 255)
                location_text = _truncate(rec.location_text, 255)
                postcode = _truncate(rec.postcode, 20)
                sector_val = _truncate(sector_value, 100)
                rate_type = _truncate(rec.rate_type, 50)
                contract_type = _truncate(rec.contract_type, 50)
                source_site = _truncate(rec.source_site, 100)
                external_id = _truncate(rec.external_id, 255)
                search_role_val = _truncate(role, 255)
                search_location_val = _truncate(loc, 255)

                if existing:
                    posting = existing
                    posting.title = title
                    posting.company_name = company_name
                    posting.location_text = location_text
                    posting.postcode = postcode
                    posting.sector = sector_val
                    posting.min_rate = rec.min_rate
                    posting.max_rate = rec.max_rate
                    posting.rate_type = rate_type
                    posting.contract_type = contract_type
                    posting.url = rec.url
                    posting.posted_date = rec.posted_date
                    posting.raw_json = json.dumps(rec.raw_json or {})
                    posting.search_role = search_role_val
                    posting.search_location = search_location_val
                    posting.scraped_at = now
                    posting.is_active = True
                    postings_updated += 1
                else:
                    posting = JobPosting(
                        title=title,
                        company_name=company_name,
                        location_text=location_text,
                        postcode=postcode,
                        sector=sector_val,
                        min_rate=rec.min_rate,
                        max_rate=rec.max_rate,
                        rate_type=rate_type,
                        contract_type=contract_type,
                        source_site=source_site,
                        external_id=external_id,
                        url=rec.url,
                        posted_date=rec.posted_date,
                        raw_json=json.dumps(rec.raw_json or {}),
                        search_role=search_role_val,
                        search_location=search_location_val,
                    )
                    db.session.add(posting)
                    postings_created += 1

                job_record = import_posting_to_record(posting)
                db.session.add(job_record)
                records_created += 1

            db.session.commit()

        except Exception as e:  # noqa: BLE001
            db.session.rollback()
            msg = f"{label}: error scraping '{role}' @ '{loc}': {e!r}"
            print("⚠", msg)
            errors.append(msg)

    return {
        "rows_scraped": rows_scraped,
        "records_created": records_created,
        "postings_created": postings_created,
        "postings_updated": postings_updated,
        "errors": errors,
        "stopped_early": stopped_early,
        "pairs_attempted": len(pairs),
    }


def _run_daily_summaries_for_date(target_date: date, trigger: str = "scheduled", triggered_by: str | None = None) -> dict:
    now = datetime.utcnow()
    safe_label = target_date.isoformat()[:20]

    log = CronRunLog(
        job_name="job_summary_daily_builder",
        started_at=now,
        status="running",
        trigger=trigger,
        triggered_by=triggered_by,
        day_label=safe_label,
    )
    db.session.add(log)
    db.session.commit()

    result: dict = {"log_id": log.id, "rows_created": 0, "error": None, "date": target_date.isoformat()}

    try:
        rows_created = build_daily_job_summaries(target_date)
        log.finished_at = datetime.utcnow()
        log.status = "success"
        log.records_created = rows_created
        log.message = f"Built {rows_created} JobSummaryDaily rows for {target_date.isoformat()}."
        db.session.commit()
        result["rows_created"] = rows_created
        return result
    except Exception as e:  # noqa: BLE001
        db.session.rollback()
        log.finished_at = datetime.utcnow()
        log.status = "error"
        log.message = f"{e!r}"
        db.session.commit()
        result["error"] = repr(e)
        return result


def _canonicalise_job_roles_with_ai(
    trigger: str = "scheduled",
    triggered_by: str | None = None,
    day_label: str | None = None,
    max_roles: int = 500,
    chunk_size: int = 25,
) -> dict:
    now = datetime.utcnow()
    safe_day_label = (day_label or "Job roles")[:20]

    log = CronRunLog(
        job_name="job_role_canonicaliser",
        started_at=now,
        status="running",
        trigger=trigger,
        triggered_by=triggered_by,
        day_label=safe_day_label,
    )
    db.session.add(log)
    db.session.commit()

    summary: dict = {"log_id": log.id, "updated": 0, "examined": 0, "skipped": 0, "error": None}

    try:
        if _openai_client is None:
            msg = "OpenAI client not available; skipping canonicaliser."
            log.status = "error"
            log.message = msg
            db.session.commit()
            summary["error"] = msg
            return summary

        roles_query = (
            db.session.query(
                JobRecord.job_role.label("raw_value"),
                func.count(JobRecord.id).label("count"),
            )
            .outerjoin(JobRoleMapping, JobRoleMapping.raw_value == JobRecord.job_role)
            .filter(JobRecord.job_role.isnot(None))
            .filter(JobRoleMapping.id.is_(None))
            .group_by(JobRecord.job_role)
            .order_by(func.count(JobRecord.id).desc())
            .limit(max_roles)
        )

        rows = roles_query.all()
        summary["examined"] = len(rows)

        if not rows:
            log.status = "success"
            log.message = "No job roles needing canonicalisation."
            db.session.commit()
            return summary

        def chunked(seq, n):
            for i in range(0, len(seq), n):
                yield seq[i : i + n]

        system_msg = (
            "You are normalising job titles for an analytics tool.\n"
            "You will be given a list of raw job titles from multiple sectors. "
            "Assign a short canonical group name for each.\n\n"
            "Rules:\n"
            "- Similar titles must share the SAME canonical group label.\n"
            "- Keep labels concise (2–4 words).\n"
            "- Do NOT include contract details.\n"
            "- Do NOT include seniority unless it changes job level.\n"
            "- Respond ONLY with valid JSON:\n"
            "{ \"mappings\": { \"raw\": \"Canonical\" } }\n"
        )

        total_updated = 0
        total_skipped = 0

        for chunk_index, chunk_rows in enumerate(chunked(rows, chunk_size), start=1):
            role_items: list[str] = []
            for raw_value, _count in chunk_rows:
                raw_title = (raw_value or "").strip()
                if not raw_title:
                    total_skipped += 1
                    continue
                role_items.append(raw_title)

            if not role_items:
                continue

            list_block = "\n".join(f"- {title}" for title in role_items)
            user_msg = (
                "Normalise these raw job titles:\n\n"
                f"{list_block}\n\n"
                "Return ONLY JSON with mappings for ALL titles."
            )

            try:
                resp = _openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": user_msg},
                    ],
                    temperature=0.1,
                    timeout=20,
                    max_tokens=1500,
                )
                content = resp.choices[0].message.content.strip()
            except Exception as e:  # noqa: BLE001
                msg = f"OpenAI call failed (chunk {chunk_index}): {e!r}"
                log.status = "error"
                log.message = msg
                db.session.commit()
                summary["error"] = msg
                summary["updated"] = total_updated
                summary["skipped"] = total_skipped
                return summary

            cleaned = content.strip()
            if cleaned.startswith("```"):
                first_newline = cleaned.find("\n")
                if first_newline != -1:
                    cleaned = cleaned[first_newline + 1 :]
                if "```" in cleaned:
                    cleaned = cleaned.rsplit("```", 1)[0].strip()

            try:
                data = json.loads(cleaned)
                mapping = data.get("mappings") if isinstance(data, dict) else {}
                if not isinstance(mapping, dict):
                    mapping = {}
            except Exception as e:  # noqa: BLE001
                msg = f"Failed to parse AI JSON (chunk {chunk_index}): {e!r} | content={cleaned[:500]}"
                log.status = "error"
                log.message = msg
                db.session.commit()
                summary["error"] = msg
                summary["updated"] = total_updated
                summary["skipped"] = total_skipped
                return summary

            for raw_value, _count in chunk_rows:
                raw_title = (raw_value or "").strip()
                if not raw_title:
                    continue

                canonical = (mapping.get(raw_title) or "").strip()
                if not canonical:
                    total_skipped += 1
                    continue

                try:
                    m = JobRoleMapping.query.filter_by(raw_value=raw_title).first()
                    if m is None:
                        m = JobRoleMapping(raw_value=raw_title, canonical_role=canonical)
                        db.session.add(m)
                        db.session.flush()
                    else:
                        m.canonical_role = canonical
                except IntegrityError:
                    db.session.rollback()
                    m = JobRoleMapping.query.filter_by(raw_value=raw_title).first()
                    if not m:
                        raise

                q = JobRecord.query.filter(JobRecord.job_role == raw_title)
                count_updated = q.update(
                    {"job_role": canonical, "job_role_group": canonical},
                    synchronize_session=False,
                )
                total_updated += count_updated

        db.session.commit()
        log.finished_at = datetime.utcnow()
        log.status = "success"
        log.message = f"Canonicalised roles. updated={total_updated}, examined={summary['examined']}, skipped={total_skipped}"
        db.session.commit()

        summary["updated"] = total_updated
        summary["skipped"] = total_skipped
        return summary

    except Exception as e:  # noqa: BLE001
        db.session.rollback()
        log.finished_at = datetime.utcnow()
        log.status = "error"
        log.message = f"{e!r}"
        db.session.commit()
        summary["error"] = repr(e)
        return summary


def run_scheduled_jobs(trigger: str = "scheduled", triggered_by: str | None = None) -> dict:
    # IMPORTANT: Lazy import to avoid circular imports with admin blueprint
    from app import create_app

    app = create_app()

    with app.app_context():
        started_monotonic = time.monotonic()
        now = datetime.utcnow()
        weekday = date.today().weekday()
        day_cfg = DAY_CONFIG.get(weekday) or DAY_CONFIG[0]
        label = day_cfg["label"]
        roles = day_cfg["roles"]
        locations = day_cfg["locations"]

        print(f"🔔 Cron: starting scheduled Adzuna scrape for '{label}' ({date.today().isoformat()})")

        safe_label = str(label)[:20] if label is not None else None

        log = CronRunLog(
            job_name="adzuna_daily_scrape",
            started_at=now,
            status="running",
            trigger=trigger,
            triggered_by=triggered_by,
            day_label=safe_label,
        )
        db.session.add(log)
        db.session.commit()

        try:
            gap_pairs = build_gap_fill_pairs()
            if gap_pairs:
                print(f"🎯 Gap-fill enabled: {len(gap_pairs)} extra (role, location) pairs this run.")

            result = _run_for_config(
                label,
                roles,
                locations,
                extra_pairs=gap_pairs,
                started_monotonic=started_monotonic,
                time_budget_seconds=CRON_TIME_BUDGET_SECONDS,
            )

            log.finished_at = datetime.utcnow()
            log.rows_scraped = result["rows_scraped"]
            log.records_created = result["records_created"]

            if result["errors"]:
                log.status = "partial"
                log.message = "\n".join(result["errors"])[:4000]
            else:
                log.status = "success"
                log.message = None

            db.session.commit()

            result_with_log = dict(result)
            result_with_log["log_id"] = log.id
            result_with_log["day_label"] = safe_label

            if CANON_DAILY_ENABLED:
                try:
                    if (time.monotonic() - started_monotonic) < (CRON_TIME_BUDGET_SECONDS * 0.85):
                        canon_result = _canonicalise_job_roles_with_ai(
                            trigger=trigger,
                            triggered_by=triggered_by,
                            day_label=safe_label,
                            max_roles=CANON_DAILY_MAX_ROLES,
                            chunk_size=CANON_DAILY_CHUNK_SIZE,
                        )
                        result_with_log["canonicaliser_log_id"] = canon_result.get("log_id")
                        result_with_log["canonicaliser_updated"] = canon_result.get("updated", 0)
                    else:
                        print("⏱️ Skipping canonicaliser (time budget nearly exhausted).")
                except Exception as e:  # noqa: BLE001
                    print("⚠ job_role_canonicaliser failed:", e)

            try:
                if (time.monotonic() - started_monotonic) < (CRON_TIME_BUDGET_SECONDS * 0.90):
                    target_date = date.today()
                    summary_result = _run_daily_summaries_for_date(
                        target_date=target_date,
                        trigger=trigger,
                        triggered_by=triggered_by,
                    )
                    result_with_log["summary_log_id"] = summary_result.get("log_id")
                    result_with_log["summary_rows_created"] = summary_result.get("rows_created", 0)
                else:
                    print("⏱️ Skipping daily summaries (time budget nearly exhausted).")
            except Exception as e:  # noqa: BLE001
                print("⚠ job_summary_daily_builder failed:", e)

            try:
                if (time.monotonic() - started_monotonic) < (CRON_TIME_BUDGET_SECONDS * 0.95):
                    ashe_year = date.today().year - 1
                    ons_result = import_ons_earnings_to_db(
                        ashe_year,
                        trigger=trigger,
                        triggered_by=triggered_by,
                        use_app_context=True,
                    )
                    result_with_log["ons_log_id"] = ons_result.get("log_id")
                    result_with_log["ons_fetched"] = ons_result.get("fetched", 0)
                    result_with_log["ons_created"] = ons_result.get("created", 0)
                    result_with_log["ons_updated"] = ons_result.get("updated", 0)
                else:
                    print("⏱️ Skipping ONS import (time budget nearly exhausted).")
            except Exception as e:  # noqa: BLE001
                print("⚠ ONS ASHE import failed:", e)

            return result_with_log

        except Exception as e:  # noqa: BLE001
            log.finished_at = datetime.utcnow()
            log.status = "error"
            log.message = f"{e!r}"[:4000]
            db.session.commit()
            print("💥 Cron failed:", e)
            raise


def run_job_role_canonicaliser(trigger: str = "manual", triggered_by: str | None = None, max_roles: int = 5000) -> dict:
    # IMPORTANT: Lazy import to avoid circular imports with admin blueprint
    from app import create_app

    app = create_app()
    with app.app_context():
        print("🧹 One-off job-role canonicaliser: starting…")
        result = _canonicalise_job_roles_with_ai(
            trigger=trigger,
            triggered_by=triggered_by,
            day_label="Manual run",
            max_roles=max_roles,
        )
        print(
            "🧹 Canonicaliser complete: "
            f"updated={result.get('updated')}, "
            f"examined={result.get('examined')}, "
            f"error={result.get('error')}"
        )
        return result


if __name__ == "__main__":
    run_scheduled_jobs(trigger="railway")
