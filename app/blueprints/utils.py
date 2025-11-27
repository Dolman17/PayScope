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
from models import JobRecord, Company  # Company now used for canonical mapping


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


# ============================================================
# Job role helpers for maps / filters (sector-aware)
# ============================================================
def build_role_groups_for_sector(sector: str | None) -> list[str]:
    """
    Return a sorted list of group labels for job roles in a sector.

    Logic:
      - Use job_role_group if set
      - Fall back to job_role when group is NULL / empty
      - Limit to the given sector (if provided)
    """
    q = db.session.query(
        JobRecord.job_role_group,
        JobRecord.job_role,
    ).filter(JobRecord.job_role.isnot(None))

    if sector:
        q = q.filter(JobRecord.sector == sector)

    groups: set[str] = set()

    for grp, raw in q.distinct().all():
        # grp = canonical (e.g. "Care & Support Worker")
        # raw = original job_role (e.g. "Care Assistant (Nights)")
        label = (grp or raw or "").strip()
        if label:
            groups.add(label)

    return sorted(groups)


def get_raw_roles_for_group(group_label: str, sector: str | None = None) -> list[str]:
    """
    Given a group label (what user selected in dropdown), return the list
    of raw job_role values that belong to that group for filtering.

    Matching rules:
      - Records where job_role_group == group_label
      - PLUS records where job_role == group_label (backwards compat)
      - Optionally limited to sector
    """
    label = (group_label or "").strip()
    if not label:
        return []

    q = db.session.query(JobRecord.job_role).filter(JobRecord.job_role.isnot(None))

    if sector:
        q = q.filter(JobRecord.sector == sector)

    q = q.filter(
        or_(
            JobRecord.job_role_group == label,
            JobRecord.job_role == label,
        )
    )

    roles = {
        (row[0] or "").strip()
        for row in q.distinct().all()
        if (row[0] or "").strip()
    }

    return sorted(roles)


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


def company_has_logo(company_id: str | None) -> bool:
    """
    Return True if there is a custom logo PNG for this company_id
    under static/logos/<company_id>.png, False otherwise.
    """
    if not company_id:
        return False
    fs_path = os.path.join(
        current_app.root_path,
        "static",
        "logos",
        f"{company_id}.png",
    )
    return os.path.exists(fs_path)


# ---------- Company ID / canonical name helpers ----------
COMPANY_STOPWORDS = [
    r"\blimited\b",
    r"\bltd\b",
    r"\bgroup\b",
    r"\bcare\b",
    r"\bhealthcare\b",
    r"\bservices\b",
    r"\bservice\b",
    r"\bplc\b",
    r"\bholdings\b",
]


def _clean_company_name(name: str) -> str:
    """
    Normalise a company name into a canonical form for grouping.

    Examples:
      "Blue Ribbon Healthcare Ltd"  -> "blue ribbon"
      "BLUE RIBBON HEALTH CARE"     -> "blue ribbon"
      "Blue Ribbon Group Services"  -> "blue ribbon"
    """
    if not name:
        return ""

    s = name.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)

    for pattern in COMPANY_STOPWORDS:
        s = re.sub(pattern, " ", s, flags=re.IGNORECASE)

    s = re.sub(r"\s+", " ", s).strip()
    return s


def _slugify(text: str, max_len: int = 50) -> str:
    """
    Simple slug generator for company_id based on canonical name.
    """
    if not text:
        return "unknown"

    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    if not slug:
        slug = "unknown"
    if len(slug) > max_len:
        slug = slug[:max_len]
    return slug


def get_or_create_company_id(raw_name: str | None) -> str:
    """
    Given a raw company name from scraping, return a stable company_id string
    that groups similar names together.

    - Uses a canonical cleaned version of the name
    - Reuses or creates a row in the Company table with that canonical_name
    - Returns a slug (string) that you store in JobRecord.company_id
    """
    raw = (raw_name or "").strip()
    if not raw:
        return "unknown"

    canonical = _clean_company_name(raw)
    if not canonical:
        canonical = raw.lower().strip()

    # Try to reuse an existing Company row with the same canonical_name
    company = Company.query.filter_by(canonical_name=canonical).first()

    if not company:
        company = Company(name=raw, canonical_name=canonical)
        db.session.add(company)
        try:
            db.session.commit()
        except Exception as e:
            # Don't break import just because company insert failed
            db.session.rollback()
            print(f"⚠️ Failed to insert Company for '{raw}': {e}")

    # Derive a stable slug from canonical_name
    return _slugify(canonical)


# ---------- UK geocoding ----------
POSTCODES_IO_BULK_URL = "https://api.postcodes.io/postcodes"
POSTCODES_IO_SINGLE_URL = "https://api.postcodes.io/postcodes/{pc}"
POSTCODES_IO_REVERSE_URL = "https://api.postcodes.io/postcodes"  # lat/lon params


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


# ---------- Nearest-postcode helpers from coordinates ----------
def lookup_nearest_postcode(
    lat: float,
    lon: float,
) -> tuple[str | None, float | None, float | None]:
    """
    Given a lat/lon (typically from Nominatim or other geocoder),
    look up the nearest UK postcode using postcodes.io reverse geocoding.

    Returns:
        (postcode | None, postcode_lat | None, postcode_lon | None)
    """
    if lat is None or lon is None:
        return (None, None, None)

    try:
        resp = requests.get(
            POSTCODES_IO_REVERSE_URL,
            params={
                "lat": lat,
                "lon": lon,
                "limit": 1,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            print(f"postcodes.io reverse non-200 ({resp.status_code}) for {lat}, {lon}")
            return (None, None, None)

        data = resp.json() or {}
        results = data.get("result") or []
        if not results:
            return (None, None, None)

        best = results[0]
        raw_pc = (best.get("postcode") or "").strip()
        pc = normalize_uk_postcode(raw_pc) if raw_pc else None
        pc_lat = best.get("latitude")
        pc_lon = best.get("longitude")

        # Safety: make sure the postcode coordinates are in the UK box
        if pc_lat is not None and pc_lon is not None and not inside_uk(pc_lat, pc_lon):
            print(f"postcodes.io reverse returned out-of-UK coords for {pc}: {pc_lat}, {pc_lon}")
            return (None, None, None)

        return (pc, pc_lat, pc_lon)
    except Exception as e:
        print(f"postcodes.io reverse error for {lat}, {lon}: {e}")
        return (None, None, None)


def snap_to_nearest_postcode(
    lat: float,
    lon: float,
) -> tuple[str | None, float | None, float | None]:
    """
    Convenience wrapper:

    - Look up nearest postcode for the given lat/lon
    - If found (and valid), return (postcode, snapped_lat, snapped_lon)
    - If not, keep the original lat/lon and return (None, lat, lon)
    """
    pc, pc_lat, pc_lon = lookup_nearest_postcode(lat, lon)
    if pc and pc_lat is not None and pc_lon is not None:
        return (pc, pc_lat, pc_lon)

    # Fallback: no postcode, but keep original coordinates
    return (None, lat, lon)
