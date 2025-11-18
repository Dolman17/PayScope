# app/blueprints/utils.py
from __future__ import annotations

import os
import time
import re
from functools import lru_cache
from typing import Any, Mapping, Tuple, Callable, List, Optional

import requests
from flask import url_for, current_app
from sqlalchemy import or_
from extensions import db
from models import JobRecord


# ---------- Small TTL cache (local, no external imports) ----------
def _ttl_cache(seconds: int = 120):
    """
    Simple, process-local TTL cache decorator for zero-arg or any-arg functions.
    Cache key is (args, kwargs) so different calls don't collide.
    """
    def deco(fn):
        store = {}  # dict[(args_tuple, frozenset(kwargs.items()))] = (timestamp, value)

        def wrapper(*a, **k):
            key = (a, frozenset(k.items()))
            now = time.time()
            if key not in store or (now - store[key][0]) > seconds:
                store[key] = (now, fn(*a, **k))
            return store[key][1]

        return wrapper

    return deco


# ---------- Filter options (cached with optional force refresh) ----------
def _compute_filter_options():
    def col_distinct(col):
        return [
            v[0]
            for v in db.session.query(col)
            .filter(col.isnot(None))
            .distinct()
            .order_by(col)
            .all()
        ]

    return {
        "sectors": col_distinct(JobRecord.sector),
        "roles": col_distinct(JobRecord.job_role),
        "counties": col_distinct(JobRecord.county),
        "months": col_distinct(JobRecord.imported_month),
        "years": col_distinct(JobRecord.imported_year),
    }


@_ttl_cache(seconds=120)
def _cached_filter_options():
    return _compute_filter_options()


def get_filter_options(force: bool = False):
    """
    Return distinct values for filter dropdowns.
    - Cached for 120s by default.
    - Pass force=True to bypass cache (e.g., right after uploads).
    """
    return _compute_filter_options() if force else _cached_filter_options()


# ---------- Filter builder ----------
def build_filters_from_request(mapping: Mapping[str, Any]) -> tuple[list, Optional[Callable]]:
    """
    Build SQLAlchemy filters and (optionally) a closure that applies extra search logic.

    mapping keys supported:
      - 'sector', 'job_role', 'county', 'month', 'year'  (exact matches)
      - 'rate_min', 'rate_max' (floats)
      - 'q' free-text search across company_name, job_role, sector, county, postcode
    Returns: (filters_list, extra_search_fn | None)
    """
    filters: List = []

    # Exact filters
    if mapping.get("sector"):
        filters.append(JobRecord.sector == mapping["sector"])
    if mapping.get("job_role"):
        filters.append(JobRecord.job_role == mapping["job_role"])
    if mapping.get("county"):
        filters.append(JobRecord.county == mapping["county"])
    if mapping.get("month"):
        filters.append(JobRecord.imported_month == mapping["month"])
    if mapping.get("year"):
        filters.append(JobRecord.imported_year == mapping["year"])

    # Pay range
    rate_min = mapping.get("rate_min")
    rate_max = mapping.get("rate_max")
    if rate_min not in (None, "", "None"):
        try:
            filters.append(JobRecord.pay_rate >= float(rate_min))
        except (TypeError, ValueError):
            pass
    if rate_max not in (None, "", "None"):
        try:
            filters.append(JobRecord.pay_rate <= float(rate_max))
        except (TypeError, ValueError):
            pass

    # Free text search -> return a callable to apply later (keeps OR logic separate)
    q = (mapping.get("q") or "").strip()

    def _extra(qtext: str) -> Callable:
        like = f"%{qtext}%"
        return lambda qry: qry.filter(
            or_(
                JobRecord.company_name.ilike(like),
                JobRecord.job_role.ilike(like),
                JobRecord.sector.ilike(like),
                JobRecord.county.ilike(like),
                JobRecord.postcode.ilike(like),
            )
        )

    extra: Optional[Callable] = _extra(q) if q else None

    return filters, extra


# ---------- DB commit helper ----------
def commit_or_rollback():
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        raise e


# ---------- Logo helper ----------
def logo_url_for(company_id: str) -> str:
    fs_path = os.path.join(current_app.root_path, "static", "logos", f"{company_id}.png")
    if os.path.exists(fs_path):
        return url_for("static", filename=f"logos/{company_id}.png")
    return url_for("static", filename="logos/placeholder.png")


# ---------- UK geocoding ----------
POSTCODES_IO_BULK_URL = "https://api.postcodes.io/postcodes"
POSTCODES_IO_SINGLE_URL = "https://api.postcodes.io/postcodes/{pc}"

# Hard UK bounding box to prevent overseas mis-geocoding
UK_BBOX = {
    "min_lon": -10.5,
    "max_lon": 1.9,
    "min_lat": 49.8,
    "max_lat": 59.0,
}


def inside_uk(lat: float, lon: float) -> bool:
    return (
        UK_BBOX["min_lat"] <= lat <= UK_BBOX["max_lat"]
        and UK_BBOX["min_lon"] <= lon <= UK_BBOX["max_lon"]
    )


def normalize_uk_postcode(pc: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]", "", (pc or "")).upper()
    if len(s) < 5:
        return s
    return s[:-3] + " " + s[-3:]


def bulk_geocode_postcodes(postcodes: list[str]) -> dict[str, tuple[float | None, float | None]]:
    results: dict[str, tuple[float | None, float | None]] = {}
    cleaned = [normalize_uk_postcode(p) for p in postcodes if p]
    unique = sorted(set(cleaned))
    for i in range(0, len(unique), 100):
        chunk = unique[i : i + 100]
        try:
            resp = requests.post(POSTCODES_IO_BULK_URL, json={"postcodes": chunk}, timeout=20)
            resp.raise_for_status()
            data = resp.json() or {}
            for item in data.get("result", []):
                query = item.get("query")
                res = item.get("result")
                if res:
                    results[query] = (res.get("latitude"), res.get("longitude"))
                else:
                    results[query] = (None, None)
        except Exception as e:
            print(f"Bulk geocode error for chunk {i}-{i+len(chunk)}: {e}")
            for q in chunk:
                results.setdefault(q, (None, None))
    return results

@lru_cache(maxsize=5000)
def geocode_postcode_cached(postcode: str) -> Tuple[float | None, float | None]:
    return geocode_postcode(postcode)


def geocode_postcode(postcode: str) -> Tuple[float | None, float | None]:
    """
    Geocode a UK postcode using postcodes.io only.

    - Normalises the postcode
    - Calls postcodes.io
    - Returns (lat, lon) only if the result lies within the UK bounding box
    - Otherwise returns (None, None)
    """
    pc = normalize_uk_postcode(postcode)
    if not pc:
        return (None, None)

    try:
        r = requests.get(POSTCODES_IO_SINGLE_URL.format(pc=pc), timeout=10)
        if r.status_code == 200:
            d = (r.json() or {}).get("result")
            if d:
                lat = float(d["latitude"])
                lon = float(d["longitude"])
                if inside_uk(lat, lon):
                    return (lat, lon)
                else:
                    # Out-of-UK result (should not happen, but be safe)
                    print(f"postcodes.io returned out-of-UK coords for {pc}: {lat}, {lon}")
        else:
            print(f"postcodes.io non-200 ({r.status_code}) for {pc}")
    except Exception as e:
        print(f"postcodes.io error for {pc}: {e}")

    return (None, None)
