# app/scrapers/adzuna.py
from __future__ import annotations

import os
import re
from datetime import datetime
from typing import List, Optional

import requests

from .base import BaseScraper, JobRecord, normalise_whitespace

ADZUNA_BASE_URL = "https://api.adzuna.com/v1/api/jobs"

# Assumptions for converting annual salary -> hourly
HOURS_PER_WEEK = float(os.getenv("JOB_HOURS_PER_WEEK", 37.5))
WEEKS_PER_YEAR = float(os.getenv("JOB_WEEKS_PER_YEAR", 52))

# Very rough UK postcode regex (covers most standard formats)
UK_POSTCODE_REGEX = re.compile(
    r"\b([A-Z]{1,2}[0-9][0-9A-Z]?\s*[0-9][A-Z]{2})\b",
    re.IGNORECASE,
)


class AdzunaScraper(BaseScraper):
    """
    Scraper for the Adzuna Jobs API.

    Docs: https://developer.adzuna.com/overview

    It queries the search endpoint and converts each result
    into a JobRecord compatible with your JobPosting model.

    Configure via environment variables:

        ADZUNA_APP_ID="your_app_id"
        ADZUNA_APP_KEY="your_app_key"

    You can also override query/where/country/max_pages via
    constructor parameters.
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
                "AdzunaScraper: ADZUNA_APP_ID and ADZUNA_APP_KEY must be set in environment variables."
            )

    # -------------------------------------------------------------------------
    # HTTP + parsing helpers
    # -------------------------------------------------------------------------

    def _fetch_page(self, page: int) -> dict:
        """
        Call Adzuna search endpoint, returning parsed JSON.
        Page is 1-based.
        """
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

    def _parse_posted_date(self, created_str: Optional[str]) -> Optional[datetime.date]:
        """
        Parse Adzuna's ISO-ish created timestamp into a date.
        Example: "2025-01-22T10:35:42Z"
        """
        if not created_str:
            return None

        for fmt in (
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                dt = datetime.strptime(created_str, fmt)
                return dt.date()
            except ValueError:
                continue

        # Fallback: just take the date part (first 10 chars) if it looks like YYYY-MM-DD
        try:
            return datetime.strptime(created_str[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    def _extract_postcode(self, location_display_name: Optional[str], description: Optional[str]) -> Optional[str]:
        """
        Best-effort UK postcode extraction from location display name or description.
        """
        text_candidates = []

        if location_display_name:
            text_candidates.append(location_display_name)

        if description:
            text_candidates.append(description)

        for text in text_candidates:
            match = UK_POSTCODE_REGEX.search(text.upper())
            if match:
                # Normalise spacing (ensure single space before last 3 chars)
                pc = match.group(1).upper().replace(" ", "")
                return f"{pc[:-3]} {pc[-3:]}"

        return None

    # -------------------------------------------------------------------------
    # Mapping Adzuna result -> JobRecord
    # -------------------------------------------------------------------------

    def _map_adzuna_result_to_record(self, item: dict) -> JobRecord:
        """
        Convert a single Adzuna job item into a JobRecord.
        Pulls detailed location info, salary, and converts to hourly.
        """
        title = normalise_whitespace(item.get("title") or "")

        company_obj = item.get("company") or {}
        company_name = normalise_whitespace(company_obj.get("display_name") or "") or None

        # Location object
        location_obj = item.get("location") or {}
        location_text = normalise_whitespace(location_obj.get("display_name") or "") or None

        # More detailed location breakdown from area[]
        area = location_obj.get("area") or []
        # Example: ["UK", "West Midlands", "Lichfield"]
        country = area[0] if len(area) >= 1 else None
        region = area[1] if len(area) >= 2 else None
        city = area[2] if len(area) >= 3 else None

        # Job-level lat/lon if provided
        latitude = item.get("latitude")
        longitude = item.get("longitude")

        # Description used as an extra source for postcode sniffing
        description = item.get("description") or ""

        # Annual salary from Adzuna
        salary_min = item.get("salary_min")
        salary_max = item.get("salary_max")
        try:
            annual_min = float(salary_min) if salary_min is not None else None
        except (TypeError, ValueError):
            annual_min = None
        try:
            annual_max = float(salary_max) if salary_max is not None else None
        except (TypeError, ValueError):
            annual_max = None

        # Convert annual -> hourly equivalent (if we have a value)
        divisor = HOURS_PER_WEEK * WEEKS_PER_YEAR
        hourly_min = annual_min / divisor if annual_min and divisor else None
        hourly_max = annual_max / divisor if annual_max and divisor else None

        # Adzuna uses contract_time ('full_time', 'part_time') and contract_type ('permanent', 'contract')
        contract_time = item.get("contract_time") or None
        contract_type = item.get("contract_type") or None

        # Store hourly in JobPosting.min_rate/max_rate
        rate_type = "hourly"

        created_str = item.get("created")
        posted_date = self._parse_posted_date(created_str)

        external_id = str(item.get("id") or "") or None
        url = item.get("redirect_url") or None

        # Best-effort UK postcode extraction
        postcode = self._extract_postcode(location_text, description)

        # Build enriched raw_json with all the extra fields
        raw = dict(item)
        raw["_annual_min"] = annual_min
        raw["_annual_max"] = annual_max
        raw["_country"] = country
        raw["_region"] = region
        raw["_city"] = city
        raw["_latitude"] = latitude
        raw["_longitude"] = longitude
        raw["_postcode_extracted"] = postcode

        return JobRecord(
            title=title,
            company_name=company_name,
            location_text=location_text,
            postcode=postcode,
            min_rate=hourly_min,
            max_rate=hourly_max,
            rate_type=rate_type,
            contract_type=contract_time or contract_type,
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
        """
        Fetch up to max_pages of results and return a list of JobRecord objects.
        """
        all_records: List[JobRecord] = []

        for page in range(1, self.max_pages + 1):
            data = self._fetch_page(page)

            error_msg = data.get("error")
            results = data.get("results") or []
            print(f"[Adzuna] page={page}, error={error_msg!r}, results={len(results)}")

            if error_msg:
                # If Adzuna is unhappy (bad credentials, quota, etc.), stop early
                break

            if not results:
                break

            for item in results:
                try:
                    record = self._map_adzuna_result_to_record(item)
                    # Skip if no title or no URL
                    if not record.title or not record.url:
                        continue
                    all_records.append(record)
                except Exception as exc:  # noqa: BLE001
                    print(f"AdzunaScraper: error mapping result: {exc}")
                    continue

        print(f"[Adzuna] total records mapped: {len(all_records)}")
        return all_records
