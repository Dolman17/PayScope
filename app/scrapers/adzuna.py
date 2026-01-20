# app/scrapers/adzuna.py
from __future__ import annotations

import os
import re
import time
import random
from datetime import datetime, date
from typing import List, Optional, Tuple

import requests

from .base import BaseScraper, JobRecord, normalise_whitespace

ADZUNA_BASE_URL = "https://api.adzuna.com/v1/api/jobs"

# -------------------------------------------------------------------
# Salary conversion guardrails
# -------------------------------------------------------------------
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


# Sane UK defaults
_DEFAULT_HOURS_PER_WEEK = 37.5
_DEFAULT_WEEKS_PER_YEAR = 52.0
_DEFAULT_DAYS_PER_WEEK = 5.0

# Read env, then clamp to sane ranges (prevents accidental inflation)
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

# Very broad UK postcode regex
UK_POSTCODE_REGEX = re.compile(
    r"\b([A-Z]{1,2}[0-9][0-9A-Z]?\s*[0-9][A-Z]{2})\b",
    re.IGNORECASE,
)


def _to_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _normalise_interval(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = str(raw).strip().lower()
    if not s:
        return None
    # Adzuna commonly uses: "year", "month", "week", "day", "hour"
    # Some feeds may return "annum" etc – map those.
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
    return s  # keep unknown token for debugging


def _salary_to_hourly(
    salary_min: Optional[float],
    salary_max: Optional[float],
    salary_interval: Optional[str],
) -> Tuple[Optional[float], Optional[float], dict]:
    """
    Convert Adzuna salary_min/max + salary_interval to hourly.

    Returns: (hourly_min, hourly_max, debug_dict)
    """
    interval = _normalise_interval(salary_interval)
    debug = {
        "_salary_interval": interval,
        "_hours_per_week": HOURS_PER_WEEK,
        "_weeks_per_year": WEEKS_PER_YEAR,
        "_days_per_week": DAYS_PER_WEEK,
    }

    # If interval is hourly already, no conversion.
    if interval == "hour":
        hourly_min = salary_min
        hourly_max = salary_max
        debug["_hourly_method"] = "interval=hour"
        debug["_hourly_divisor"] = None
        return hourly_min, hourly_max, debug

    # Convert to annual first
    annual_min = None
    annual_max = None

    if interval == "year" or interval is None:
        # If interval is missing, Adzuna salary fields are often annual.
        annual_min = salary_min
        annual_max = salary_max
        debug["_hourly_method"] = "annual_assumed" if interval is None else "annual"
    elif interval == "month":
        annual_min = salary_min * 12.0 if salary_min is not None else None
        annual_max = salary_max * 12.0 if salary_max is not None else None
        debug["_hourly_method"] = "month_to_annual"
    elif interval == "week":
        annual_min = salary_min * WEEKS_PER_YEAR if salary_min is not None else None
        annual_max = salary_max * WEEKS_PER_YEAR if salary_max is not None else None
        debug["_hourly_method"] = "week_to_annual"
    elif interval == "day":
        # Approximate: daily * days/week * weeks/year
        annual_min = salary_min * DAYS_PER_WEEK * WEEKS_PER_YEAR if salary_min is not None else None
        annual_max = salary_max * DAYS_PER_WEEK * WEEKS_PER_YEAR if salary_max is not None else None
        debug["_hourly_method"] = "day_to_annual"
    else:
        # Unknown interval: do not guess. Keep None and surface debug.
        debug["_hourly_method"] = f"unknown_interval:{interval}"
        return None, None, debug

    debug["_annual_min"] = annual_min
    debug["_annual_max"] = annual_max

    divisor = HOURS_PER_WEEK * WEEKS_PER_YEAR
    if not divisor or divisor <= 0:
        divisor = _DEFAULT_HOURS_PER_WEEK * _DEFAULT_WEEKS_PER_YEAR

    debug["_hourly_divisor"] = divisor

    hourly_min = annual_min / divisor if annual_min is not None else None
    hourly_max = annual_max / divisor if annual_max is not None else None

    # Sanity guard: if computed hourly is implausible, don’t publish it.
    # (Still store debug fields so you can inspect.)
    def _sane(v: Optional[float]) -> bool:
        if v is None:
            return True
        # pay < £1/hr or > £200/hr is almost certainly broken conversion/interval data
        return 1.0 <= v <= 200.0

    is_sane = _sane(hourly_min) and _sane(hourly_max)
    debug["_hourly_is_sane"] = bool(is_sane)

    if not is_sane:
        return None, None, debug

    return hourly_min, hourly_max, debug


class AdzunaScraper(BaseScraper):
    """
    Scraper for the Adzuna Jobs API.
    Produces JobRecord objects that will later be saved into JobPosting.

    NOTE (PayScope constraints):
    - Scrapers remain "dumb": they fetch + lightly parse.
    - Sector/role normalisation is done ONLY at import time.
    """

    source_site = "adzuna"

    def __init__(
        self,
        country: str = "gb",
        what: str = "support worker",
        where: str = "United Kingdom",
        results_per_page: int = 50,
        max_pages: int = 2,
    ) -> None:
        self.country = country
        self.what = what
        self.where = where
        self.results_per_page = max(1, min(results_per_page, 50))
        self.max_pages = max_pages

        self.app_id = os.getenv("ADZUNA_APP_ID")
        self.app_key = os.getenv("ADZUNA_APP_KEY")

        # Debug toggle for API visibility (does NOT change behaviour)
        self.debug = os.getenv("ADZUNA_DEBUG", "0").strip() in ("1", "true", "True", "yes", "YES")

        # Sleep between page requests (tiny throttle inside the scraper)
        # This is independent of cron_runner's role-level throttle.
        self.sleep_seconds = float(os.getenv("ADZUNA_SLEEP_SEC", "0.25"))

        if not self.app_id or not self.app_key:
            raise RuntimeError(
                "AdzunaScraper: ADZUNA_APP_ID and ADZUNA_APP_KEY must be set."
            )

    def _is_nationwide_where(self, where: Optional[str]) -> bool:
        """
        Adzuna already scopes by country via the URL (/gb/).
        For a nationwide search, omit 'where' entirely.
        """
        if where is None:
            return True
        w = (where or "").strip().lower()
        if not w:
            return True
        return w in {
            "united kingdom",
            "uk",
            "great britain",
            "gb",
            "britain",
            "england",
            "scotland",
            "wales",
            "northern ireland",
        }

    # -------------------------------------------------------------------------
    # HTTP helpers with backoff
    # -------------------------------------------------------------------------
    def _fetch_page(self, page: int) -> dict:
        """
        Fetch a page from Adzuna with simple exponential backoff.

        Retries on:
          - HTTP 429 (rate limit)
          - HTTP 5xx
          - network errors
        """
        url = f"{ADZUNA_BASE_URL}/{self.country}/search/{page}"

        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": self.results_per_page,
            "what": self.what,
            # Adzuna doesn't require this, but leaving it doesn't hurt
            "content-type": "application/json",
        }

        # IMPORTANT: Nationwide searches should omit 'where'
        if not self._is_nationwide_where(self.where):
            params["where"] = self.where

        max_retries = 5
        attempt = 0

        while True:
            try:
                resp = requests.get(url, params=params, timeout=20)
            except requests.RequestException as exc:
                attempt += 1
                if attempt > max_retries:
                    raise RuntimeError(
                        f"AdzunaScraper: request failed after {max_retries} retries: {exc}"
                    )
                sleep_for = (2 ** (attempt - 1)) + random.random()
                print(f"[Adzuna] network error, retry {attempt}/{max_retries} in {sleep_for:.1f}s")
                time.sleep(sleep_for)
                continue

            if self.debug:
                print(f"[Adzuna][DEBUG] status={resp.status_code} url={resp.url}")

            # Rate limit or temporary server errors
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                attempt += 1
                if attempt > max_retries:
                    raise RuntimeError(
                        f"AdzunaScraper: HTTP {resp.status_code} after {max_retries} retries "
                        f"body={resp.text[:300]!r}"
                    )
                sleep_for = (2 ** (attempt - 1)) + random.random()
                print(f"[Adzuna] HTTP {resp.status_code}, retry {attempt}/{max_retries} in {sleep_for:.1f}s")
                time.sleep(sleep_for)
                continue

            try:
                resp.raise_for_status()
            except Exception:
                if self.debug:
                    print(f"[Adzuna][DEBUG] non-OK body (first 300 chars): {resp.text[:300]}")
                raise

            try:
                data = resp.json()
            except Exception:
                if self.debug:
                    print(f"[Adzuna][DEBUG] non-json response (first 300 chars): {resp.text[:300]}")
                return {}

            if self.debug:
                if isinstance(data, dict):
                    top_keys = list(data.keys())
                    print(f"[Adzuna][DEBUG] keys={top_keys}")
                    if "error" in data:
                        print(f"[Adzuna][DEBUG] error={data.get('error')!r}")
                    if "count" in data:
                        print(f"[Adzuna][DEBUG] count={data.get('count')!r}")
                    if "results" in data and isinstance(data.get("results"), list):
                        print(f"[Adzuna][DEBUG] results_len={len(data.get('results'))}")
                else:
                    print(f"[Adzuna][DEBUG] unexpected json type={type(data)}")

            # Tiny sleep to avoid hammering
            if self.sleep_seconds and self.sleep_seconds > 0:
                time.sleep(self.sleep_seconds)

            return data

    # -------------------------------------------------------------------------
    # Parsing helpers
    # -------------------------------------------------------------------------
    def _parse_posted_date(self, created_str: Optional[str]) -> Optional[date]:
        if not created_str:
            return None

        formats = [
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(created_str, fmt)
                return dt.date()
            except ValueError:
                pass

        try:
            return datetime.strptime(created_str[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    def _extract_postcode(
        self,
        location_display_name: Optional[str],
        description: Optional[str],
    ) -> Optional[str]:
        for text in (location_display_name, description):
            if not text:
                continue
            match = UK_POSTCODE_REGEX.search(text.upper())
            if match:
                pc = match.group(1).upper().replace(" ", "")
                return f"{pc[:-3]} {pc[-3:]}"
        return None

    # -------------------------------------------------------------------------
    # Core mapper: Adzuna JSON → JobRecord (temporary object)
    # -------------------------------------------------------------------------
    def _map_adzuna_result_to_record(self, item: dict) -> JobRecord:
        title = normalise_whitespace(item.get("title") or "")
        company_obj = item.get("company") or {}
        company_name = normalise_whitespace(company_obj.get("display_name") or "") or None

        location_obj = item.get("location") or {}
        location_text = normalise_whitespace(location_obj.get("display_name") or "") or None

        area = location_obj.get("area") or []
        country = area[0] if len(area) >= 1 else None
        region = area[1] if len(area) >= 2 else None
        city = area[2] if len(area) >= 3 else None

        latitude = item.get("latitude")
        longitude = item.get("longitude")

        # Raw salary fields from Adzuna
        salary_min_raw = item.get("salary_min")
        salary_max_raw = item.get("salary_max")
        salary_interval_raw = item.get("salary_interval")  # hour/day/week/month/year (often)
        salary_currency = item.get("salary_currency")
        salary_is_predicted = item.get("salary_is_predicted")

        salary_min = _to_float(salary_min_raw)
        salary_max = _to_float(salary_max_raw)

        hourly_min, hourly_max, salary_debug = _salary_to_hourly(
            salary_min=salary_min,
            salary_max=salary_max,
            salary_interval=salary_interval_raw,
        )

        rate_type = "hourly"

        created_raw = item.get("created")
        posted_date: Optional[date] = None

        if isinstance(created_raw, (int, float)):
            try:
                ts = created_raw / 1000.0 if created_raw > 10**12 else float(created_raw)
                posted_date = datetime.utcfromtimestamp(ts).date()
            except Exception:
                posted_date = None
        elif isinstance(created_raw, str):
            posted_date = self._parse_posted_date(created_raw)
        else:
            posted_date = None

        if posted_date is None:
            posted_date = datetime.utcnow().date()

        external_id = str(item.get("id") or "") or None
        url = item.get("redirect_url") or None

        description = item.get("description") or ""
        postcode = self._extract_postcode(location_text, description)

        raw = dict(item)
        raw["_country"] = country
        raw["_region"] = region
        raw["_city"] = city
        raw["_latitude"] = latitude
        raw["_longitude"] = longitude
        raw["_postcode_extracted"] = postcode

        # Preserve raw salary fields + conversion debug (additive)
        raw["_salary_min_raw"] = salary_min_raw
        raw["_salary_max_raw"] = salary_max_raw
        raw["_salary_currency"] = salary_currency
        raw["_salary_interval_raw"] = salary_interval_raw
        raw["_salary_is_predicted"] = salary_is_predicted

        raw.update(salary_debug)
        raw["_hourly_min"] = hourly_min
        raw["_hourly_max"] = hourly_max

        raw["_search_what"] = self.what
        raw["_search_where"] = self.where

        return JobRecord(
            title=title,
            company_name=company_name,
            location_text=location_text,
            postcode=postcode,
            min_rate=hourly_min,
            max_rate=hourly_max,
            rate_type=rate_type,
            contract_type=item.get("contract_time") or item.get("contract_type"),
            source_site=self.source_site,
            external_id=external_id,
            url=url,
            posted_date=posted_date,
            raw_json=raw,
        )

    # -------------------------------------------------------------------------
    # Public scrape() API (adaptive paging)
    # -------------------------------------------------------------------------
    def scrape(self) -> List[JobRecord]:
        """
        Adaptive paging:
        - Always fetch page 1 (unless error/empty)
        - Only fetch page 2+ if the previous page was "full"
          (i.e., results_count >= results_per_page)
        This cuts pointless extra page calls for sparse searches.
        """
        all_records: List[JobRecord] = []

        page = 1
        while page <= self.max_pages:
            data = self._fetch_page(page) or {}

            error_msg = data.get("error") if isinstance(data, dict) else None
            results = data.get("results") if isinstance(data, dict) else None
            results = results or []

            print(f"[Adzuna] page={page}, error={error_msg!r}, results={len(results)}")

            if error_msg:
                break
            if not results:
                break

            mapped_this_page = 0

            for item in results:
                try:
                    record = self._map_adzuna_result_to_record(item)
                    if not record.title or not record.url:
                        continue
                    all_records.append(record)
                    mapped_this_page += 1
                except Exception as exc:
                    print(f"AdzunaScraper: error mapping result: {exc}")
                    continue

            # ✅ Adaptive paging rule:
            # If we didn't get a full page back, don't request the next page.
            # (Adzuna may return fewer than requested when results are sparse.)
            if len(results) < self.results_per_page:
                break

            page += 1

        print(f"[Adzuna] total records mapped: {len(all_records)}")
        return all_records
