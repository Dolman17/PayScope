# app/scrapers/adzuna.py
from __future__ import annotations

import os
import re
from datetime import datetime, date
from typing import List, Optional

import requests

from .base import BaseScraper, JobRecord, normalise_whitespace

ADZUNA_BASE_URL = "https://api.adzuna.com/v1/api/jobs"

# Hours + weeks → annual → hourly conversion
HOURS_PER_WEEK = float(os.getenv("JOB_HOURS_PER_WEEK", 37.5))
WEEKS_PER_YEAR = float(os.getenv("JOB_WEEKS_PER_YEAR", 52))

# Very broad UK postcode regex
UK_POSTCODE_REGEX = re.compile(
    r"\b([A-Z]{1,2}[0-9][0-9A-Z]?\s*[0-9][A-Z]{2})\b",
    re.IGNORECASE,
)


class AdzunaScraper(BaseScraper):
    """
    Scraper for the Adzuna Jobs API.
    Produces JobRecord objects that will later be saved into JobPosting.
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

        if not self.app_id or not self.app_key:
            raise RuntimeError(
                "AdzunaScraper: ADZUNA_APP_ID and ADZUNA_APP_KEY must be set."
            )

    # -------------------------------------------------------------------------
    # HTTP helpers
    # -------------------------------------------------------------------------
    def _fetch_page(self, page: int) -> dict:
        url = f"{ADZUNA_BASE_URL}/{self.country}/search/{page}"
        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": self.results_per_page,
            "what": self.what,
            "where": self.where,
            "content-type": "application/json",
        }

        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json()

    # -------------------------------------------------------------------------
    # Parsing helpers
    # -------------------------------------------------------------------------
    def _parse_posted_date(self, created_str: Optional[str]) -> Optional[date]:
        """
        Parse Adzuna 'created' string to a date, if possible.
        Returns None on failure.
        """
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

        # Fallback: first 10 chars as YYYY-MM-DD if that works
        try:
            return datetime.strptime(created_str[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    def _extract_postcode(
        self,
        location_display_name: Optional[str],
        description: Optional[str],
    ) -> Optional[str]:
        """
        Best-effort UK postcode extraction from location display or description.
        """
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
        """
        Convert a single Adzuna item to a JobRecord (NOT the DB model).
        This is later upserted into JobPosting.
        """

        # -------- Title & company ----------
        title = normalise_whitespace(item.get("title") or "")
        company_obj = item.get("company") or {}
        company_name = normalise_whitespace(company_obj.get("display_name") or "") or None

        # -------- Location ----------
        location_obj = item.get("location") or {}
        location_text = normalise_whitespace(location_obj.get("display_name") or "") or None

        # region breakdown: ["UK", "Region", "City", ...]
        area = location_obj.get("area") or []
        country = area[0] if len(area) >= 1 else None
        region = area[1] if len(area) >= 2 else None
        city = area[2] if len(area) >= 3 else None

        # lat/lon directly from Adzuna item
        latitude = item.get("latitude")
        longitude = item.get("longitude")

        # -------- Salary converting --------
        salary_min = item.get("salary_min")
        salary_max = item.get("salary_max")

        try:
            annual_min = float(salary_min) if salary_min is not None else None
        except Exception:
            annual_min = None
        try:
            annual_max = float(salary_max) if salary_max is not None else None
        except Exception:
            annual_max = None

        divisor = HOURS_PER_WEEK * WEEKS_PER_YEAR
        hourly_min = annual_min / divisor if annual_min else None
        hourly_max = annual_max / divisor if annual_max else None

        rate_type = "hourly"

        # -------- Meta: posted date ----------
        created_raw = item.get("created")
        posted_date: Optional[date] = None

        # Adzuna sometimes returns ISO strings, sometimes epoch-like numbers
        if isinstance(created_raw, (int, float)):
            try:
                # Heuristic: if huge, assume ms; otherwise seconds
                ts = created_raw / 1000.0 if created_raw > 10**12 else float(created_raw)
                posted_date = datetime.utcfromtimestamp(ts).date()
            except Exception:
                posted_date = None
        elif isinstance(created_raw, str):
            posted_date = self._parse_posted_date(created_raw)
        else:
            posted_date = None

        # Hard guarantee: downstream never sees None for posted_date
        if posted_date is None:
            posted_date = datetime.utcnow().date()

        external_id = str(item.get("id") or "") or None
        url = item.get("redirect_url") or None

        description = item.get("description") or ""
        postcode = self._extract_postcode(location_text, description)

        # -------- RAW JSON (enriched) for importer ----------
        raw = dict(item)
        raw["_country"] = country
        raw["_region"] = region
        raw["_city"] = city
        raw["_latitude"] = latitude
        raw["_longitude"] = longitude
        raw["_postcode_extracted"] = postcode
        raw["_hourly_min"] = hourly_min
        raw["_hourly_max"] = hourly_max

        # -------- Build JobRecord (temporary) ----------
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
    # Public scrape() API
    # -------------------------------------------------------------------------
    def scrape(self) -> List[JobRecord]:
        all_records: List[JobRecord] = []

        for page in range(1, self.max_pages + 1):
            data = self._fetch_page(page)

            error_msg = data.get("error")
            results = data.get("results") or []
            print(f"[Adzuna] page={page}, error={error_msg!r}, results={len(results)}")

            if error_msg:
                break
            if not results:
                break

            for item in results:
                try:
                    record = self._map_adzuna_result_to_record(item)
                    if not record.title or not record.url:
                        continue
                    all_records.append(record)
                except Exception as exc:
                    print(f"AdzunaScraper: error mapping result: {exc}")
                    continue

        print(f"[Adzuna] total records mapped: {len(all_records)}")
        return all_records
