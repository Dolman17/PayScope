# scripts/backfill_adzuna_hourly_rates.py
from __future__ import annotations

import json
import os
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple, List

from extensions import db
from models import JobPosting, JobRecord


# -----------------------------
# Guardrails / defaults
# -----------------------------
def _safe_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(str(raw).strip())
    except Exception:
        return default


def _clamp(v: float, lo: float, hi: float, default: float) -> float:
    try:
        if v < lo or v > hi:
            return default
        return v
    except Exception:
        return default


_DEFAULT_HOURS_PER_WEEK = 37.5
_DEFAULT_WEEKS_PER_YEAR = 52.0
_DEFAULT_DAYS_PER_WEEK = 5.0

HOURS_PER_WEEK = _clamp(
    _safe_float_env("JOB_HOURS_PER_WEEK", _DEFAULT_HOURS_PER_WEEK),
    30.0,
    45.0,
    _DEFAULT_HOURS_PER_WEEK,
)
WEEKS_PER_YEAR = _clamp(
    _safe_float_env("JOB_WEEKS_PER_YEAR", _DEFAULT_WEEKS_PER_YEAR),
    48.0,
    53.0,
    _DEFAULT_WEEKS_PER_YEAR,
)
DAYS_PER_WEEK = _clamp(
    _safe_float_env("JOB_DAYS_PER_WEEK", _DEFAULT_DAYS_PER_WEEK),
    4.0,
    6.0,
    _DEFAULT_DAYS_PER_WEEK,
)


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, Decimal):
        return float(v)
    try:
        s = str(v).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _normalise_interval(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = str(raw).strip().lower()
    if not s:
        return None
    if s in ("year", "annum", "annual", "per year", "pa", "p.a."):
        return "year"
    if s in ("month", "per month", "pm", "p.m."):
        return "month"
    if s in ("week", "per week", "pw", "p.w."):
        return "week"
    if s in ("day", "per day", "pd", "p.d."):
        return "day"
    if s in ("hour", "per hour", "ph", "p.h."):
        return "hour"
    return s  # unknown token for visibility


def _maybe_fix_scaled_annual(
    annual_min: Optional[float],
    annual_max: Optional[float],
    interval_norm: Optional[str],
) -> Tuple[Optional[float], Optional[float], Dict[str, Any]]:
    """
    Heuristic fix for Adzuna salary fields that appear to be scaled up (commonly x10).

    We ONLY apply this when:
      - interval is 'year' OR missing (None)
    And the value looks like:
      - very high (>= 200,000)
      - divisible by 10
      - dividing by 10 lands in a plausible UK salary band (8k–150k)
    """
    dbg: Dict[str, Any] = {"_backfill_scale_fix_applied": False, "_backfill_scale_factor": None}

    if interval_norm not in (None, "year"):
        return annual_min, annual_max, dbg

    def _fix(v: Optional[float]) -> Tuple[Optional[float], bool]:
        if v is None:
            return None, False
        if v < 200000:
            return v, False
        if int(v) % 10 != 0:
            return v, False
        candidate = v / 10.0
        if 8000.0 <= candidate <= 150000.0:
            return candidate, True
        return v, False

    new_min, fixed_min = _fix(annual_min)
    new_max, fixed_max = _fix(annual_max)

    if fixed_min or fixed_max:
        dbg["_backfill_scale_fix_applied"] = True
        dbg["_backfill_scale_factor"] = 10
        dbg["_backfill_annual_min_before_scale"] = annual_min
        dbg["_backfill_annual_max_before_scale"] = annual_max
        dbg["_backfill_annual_min_after_scale"] = new_min
        dbg["_backfill_annual_max_after_scale"] = new_max

    return new_min, new_max, dbg


def _salary_to_hourly(
    salary_min: Optional[float],
    salary_max: Optional[float],
    salary_interval: Optional[str],
) -> Tuple[Optional[float], Optional[float], Dict[str, Any]]:
    """
    Convert Adzuna salary_min/max + salary_interval to hourly.
    Returns (hourly_min, hourly_max, debug_fields)
    """
    interval = _normalise_interval(salary_interval)

    debug: Dict[str, Any] = {
        "_backfill_salary_interval": interval,
        "_backfill_hours_per_week": HOURS_PER_WEEK,
        "_backfill_weeks_per_year": WEEKS_PER_YEAR,
        "_backfill_days_per_week": DAYS_PER_WEEK,
    }

    # already hourly
    if interval == "hour":
        debug["_backfill_method"] = "interval=hour"
        return salary_min, salary_max, debug

    # Convert to annual first
    annual_min = None
    annual_max = None

    if interval == "year" or interval is None:
        annual_min = salary_min
        annual_max = salary_max
        debug["_backfill_method"] = "annual_assumed" if interval is None else "annual"
    elif interval == "month":
        annual_min = salary_min * 12.0 if salary_min is not None else None
        annual_max = salary_max * 12.0 if salary_max is not None else None
        debug["_backfill_method"] = "month_to_annual"
    elif interval == "week":
        annual_min = salary_min * WEEKS_PER_YEAR if salary_min is not None else None
        annual_max = salary_max * WEEKS_PER_YEAR if salary_max is not None else None
        debug["_backfill_method"] = "week_to_annual"
    elif interval == "day":
        annual_min = salary_min * DAYS_PER_WEEK * WEEKS_PER_YEAR if salary_min is not None else None
        annual_max = salary_max * DAYS_PER_WEEK * WEEKS_PER_YEAR if salary_max is not None else None
        debug["_backfill_method"] = "day_to_annual"
    else:
        debug["_backfill_method"] = f"unknown_interval:{interval}"
        return None, None, debug

    # Apply scale fix ONLY for annual/unknown interval
    annual_min, annual_max, scale_dbg = _maybe_fix_scaled_annual(annual_min, annual_max, interval)
    debug.update(scale_dbg)

    debug["_backfill_annual_min"] = annual_min
    debug["_backfill_annual_max"] = annual_max

    divisor = HOURS_PER_WEEK * WEEKS_PER_YEAR
    if not divisor or divisor <= 0:
        divisor = _DEFAULT_HOURS_PER_WEEK * _DEFAULT_WEEKS_PER_YEAR
    debug["_backfill_divisor"] = divisor

    hourly_min = annual_min / divisor if annual_min is not None else None
    hourly_max = annual_max / divisor if annual_max is not None else None

    # sanity: < £1/hr or > £200/hr is almost certainly wrong conversion/interval data
    def _sane(x: Optional[float]) -> bool:
        if x is None:
            return True
        return 1.0 <= x <= 200.0

    sane = _sane(hourly_min) and _sane(hourly_max)
    debug["_backfill_is_sane"] = bool(sane)

    if not sane:
        return None, None, debug

    return hourly_min, hourly_max, debug


def _parse_raw_json(raw_text: Optional[str]) -> Dict[str, Any]:
    if not raw_text:
        return {}
    if isinstance(raw_text, dict):
        return raw_text
    try:
        data = json.loads(raw_text)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _get_salary_fields(raw: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    salary_min = _to_float(raw.get("salary_min"))
    salary_max = _to_float(raw.get("salary_max"))
    interval = raw.get("salary_interval")

    if salary_min is None:
        salary_min = _to_float(raw.get("_salary_min_raw"))
    if salary_max is None:
        salary_max = _to_float(raw.get("_salary_max_raw"))
    if not interval:
        interval = raw.get("_salary_interval_raw")

    return salary_min, salary_max, interval if interval is None else str(interval)


def run_backfill(
    *,
    dry_run: bool = True,
    commit_every: int = 500,
    only_if_suspicious: bool = True,
    suspicious_over_hourly: float = 30.0,
    id_min: Optional[int] = None,
    id_max: Optional[int] = None,
    max_rows: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Backfill hourly rates for Adzuna postings and their imported JobRecords.

    only_if_suspicious:
      - True: only update rows where current hourly looks suspiciously high OR missing
              OR where the scale-fix heuristic has triggered
      - False: recompute and update all Adzuna rows where we can compute a sane hourly

    suspicious_over_hourly:
      - threshold above which we consider a stored hourly "suspicious"

    id_min / id_max:
      - optional ID range filter to process only a slice of postings

    max_rows:
      - optional hard cap on rows processed in this run

    commit_every:
      - also used as the chunk size (IDs per load+commit) to avoid server-side cursor issues.
    """
    base_q = JobPosting.query.filter(JobPosting.source_site == "adzuna")
    if id_min is not None:
        base_q = base_q.filter(JobPosting.id >= id_min)
    if id_max is not None:
        base_q = base_q.filter(JobPosting.id <= id_max)

    id_q = base_q.with_entities(JobPosting.id).order_by(JobPosting.id.asc())
    if max_rows is not None:
        id_q = id_q.limit(max_rows)

    id_rows: List[Tuple[int]] = id_q.all()
    id_list: List[int] = [row[0] for row in id_rows]
    total = len(id_list)

    print(f"[backfill] Adzuna postings in scope: {total}")
    print(
        f"[backfill] dry_run={dry_run} "
        f"only_if_suspicious={only_if_suspicious} "
        f"threshold={suspicious_over_hourly} "
        f"id_min={id_min} id_max={id_max} max_rows={max_rows}"
    )
    print(f"[backfill] using hours/week={HOURS_PER_WEEK} weeks/year={WEEKS_PER_YEAR} days/week={DAYS_PER_WEEK}")

    if total == 0:
        summary = {
            "total_adzuna_postings_in_scope": 0,
            "scanned": 0,
            "can_recompute": 0,
            "would_update": 0,
            "updated_postings": 0,
            "updated_job_records": 0,
            "skipped_no_salary": 0,
            "skipped_not_sane": 0,
            "scaled_fix_triggered_count": 0,
            "dry_run": dry_run,
            "only_if_suspicious": only_if_suspicious,
            "suspicious_over_hourly": suspicious_over_hourly,
            "hours_per_week": HOURS_PER_WEEK,
            "weeks_per_year": WEEKS_PER_YEAR,
            "days_per_week": DAYS_PER_WEEK,
            "id_min": id_min,
            "id_max": id_max,
            "max_rows": max_rows,
        }
        print("[backfill] done (nothing in scope):", summary)
        return summary

    chunk_size = commit_every if commit_every and commit_every > 0 else total

    scanned = 0
    can_recompute = 0
    would_update = 0
    updated_postings = 0
    updated_records = 0
    skipped_not_sane = 0
    skipped_no_salary = 0
    scaled_fixed = 0

    # Process in ID chunks to avoid server-side cursor issues
    for offset in range(0, total, chunk_size):
        chunk_ids = id_list[offset : offset + chunk_size]

        postings = (
            JobPosting.query
            .filter(JobPosting.id.in_(chunk_ids))
            .order_by(JobPosting.id.asc())
            .all()
        )

        for posting in postings:
            scanned += 1

            raw = _parse_raw_json(posting.raw_json)
            salary_min, salary_max, interval = _get_salary_fields(raw)

            if salary_min is None and salary_max is None:
                skipped_no_salary += 1
                continue

            hourly_min, hourly_max, dbg = _salary_to_hourly(salary_min, salary_max, interval)

            if dbg.get("_backfill_scale_fix_applied") is True:
                scaled_fixed += 1

            if hourly_min is None and hourly_max is None:
                skipped_not_sane += 1
                continue

            can_recompute += 1

            new_min = round(hourly_min, 2) if hourly_min is not None else None
            new_max = round(hourly_max, 2) if hourly_max is not None else None
            new_pay = new_min if new_min is not None else new_max

            # current values
            cur_min = _to_float(posting.min_rate)
            cur_max = _to_float(posting.max_rate)
            cur_pay_like = cur_min if cur_min is not None else cur_max

            suspicious = (cur_pay_like is None) or (cur_pay_like >= suspicious_over_hourly)
            scale_fix = (dbg.get("_backfill_scale_fix_applied") is True)

            # In "suspicious only" mode, we still update whenever the scale-fix triggers
            if only_if_suspicious and not (suspicious or scale_fix):
                continue

            would_update += 1

            if dry_run:
                if would_update <= 25:
                    title = (posting.title or "").strip().replace("\n", " ")
                    company = (posting.company_name or "").strip().replace("\n", " ")
                    search_role = (posting.search_role or "").strip()
                    search_loc = (posting.search_location or "").strip()

                    if len(title) > 80:
                        title = title[:77] + "…"
                    if len(company) > 50:
                        company = company[:47] + "…"

                    print(
                        "[dry_run sample] "
                        f"posting_id={posting.id} "
                        f"title={title!r} "
                        f"company={company!r} "
                        f"search_role={search_role!r} "
                        f"search_location={search_loc!r} "
                        f"interval={interval!r} "
                        f"salary_min={salary_min} salary_max={salary_max} "
                        f"cur_min={cur_min} cur_max={cur_max} -> new_min={new_min} new_max={new_max} "
                        f"method={dbg.get('_backfill_method')} "
                        f"scale_fix={dbg.get('_backfill_scale_fix_applied')}"
                    )
                continue

            # --- apply updates ---
            changed_posting = False
            if new_min is not None and (cur_min is None or abs(cur_min - new_min) > 0.005):
                posting.min_rate = Decimal(f"{new_min:.2f}")
                changed_posting = True
            if new_max is not None and (cur_max is None or abs(cur_max - new_max) > 0.005):
                posting.max_rate = Decimal(f"{new_max:.2f}")
                changed_posting = True

            if changed_posting:
                updated_postings += 1
                db.session.add(posting)

            # Update corresponding JobRecord if it was imported from this posting
            if new_pay is not None:
                rec = JobRecord.query.filter_by(imported_from_posting_id=posting.id).first()
                if rec:
                    cur_rec_pay = _to_float(rec.pay_rate)
                    if cur_rec_pay is None or abs(cur_rec_pay - float(new_pay)) > 0.005:
                        rec.pay_rate = float(new_pay)
                        db.session.add(rec)
                        updated_records += 1

        if not dry_run:
            db.session.commit()
            print(
                f"[backfill] committed chunk "
                f"offset={offset} size={len(chunk_ids)} scanned={scanned}/{total}"
            )

    summary = {
        "total_adzuna_postings_in_scope": total,
        "scanned": scanned,
        "can_recompute": can_recompute,
        "would_update": would_update,
        "updated_postings": updated_postings,
        "updated_job_records": updated_records,
        "skipped_no_salary": skipped_no_salary,
        "skipped_not_sane": skipped_not_sane,
        "scaled_fix_triggered_count": scaled_fixed,
        "dry_run": dry_run,
        "only_if_suspicious": only_if_suspicious,
        "suspicious_over_hourly": suspicious_over_hourly,
        "hours_per_week": HOURS_PER_WEEK,
        "weeks_per_year": WEEKS_PER_YEAR,
        "days_per_week": DAYS_PER_WEEK,
        "id_min": id_min,
        "id_max": id_max,
        "max_rows": max_rows,
    }

    print("[backfill] done:", summary)
    return summary
