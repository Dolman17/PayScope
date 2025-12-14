# app/scrapers/adzuna.py
from __future__ import annotations

import os
import re
import time
import random
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

            time.sleep(0.25)
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
    # Public scrape() API
    # -------------------------------------------------------------------------
    def scrape(self) -> List[JobRecord]:
        all_records: List[JobRecord] = []

        for page in range(1, self.max_pages + 1):
            data = self._fetch_page(page) or {}

            error_msg = data.get("error") if isinstance(data, dict) else None
            results = data.get("results") if isinstance(data, dict) else None
            results = results or []

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
