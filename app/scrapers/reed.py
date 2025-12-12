# app/scrapers/reed.py
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from types import SimpleNamespace

import requests


@dataclass
class ReedScraper:
    """
    Reed Jobseeker API scraper.

    Docs:
      - Search:   https://www.reed.co.uk/api/1.0/search?... (resultsToTake <= 100)
      - Details:  https://www.reed.co.uk/api/1.0/jobs/{jobId}
      - Auth: Basic auth header with API key as username, empty password

    This returns objects shaped like your Adzuna scraper records so cron_runner.py
    can ingest them without changes.
    """

    api_key: str
    keywords: str
    location_name: Optional[str] = None
    distance_from_location: int = 10
    results_per_page: int = 100
    max_pages: int = 2
    throttle_seconds: float = 0.25
    fetch_details: bool = False  # keep False for speed; True gives richer fields

    BASE = "https://www.reed.co.uk/api/1.0"

    def _auth(self):
        # Basic Auth: username=api_key, password empty
        return (self.api_key, "")

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.BASE}{path}"
        r = requests.get(
            url,
            params=params or {},
            auth=self._auth(),
            headers={"Accept": "application/json"},
            timeout=25,
        )
        r.raise_for_status()
        return r.json() or {}

    def _parse_posted_date(self, raw: Any) -> Optional[datetime]:
        # Reed search often returns "date" as string like "28/08/2020" (dd/mm/yyyy).
        if not raw:
            return None
        if isinstance(raw, datetime):
            return raw
        s = str(raw).strip()
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                pass
        return None

    def _to_record(self, item: Dict[str, Any], details: Optional[Dict[str, Any]] = None):
        """
        Convert Reed API item into the fields your pipeline expects.
        """
        job_id = item.get("jobId")
        title = (item.get("jobTitle") or "").strip() or None
        company_name = (item.get("employerName") or "").strip() or None

        # Reed search gives locationName (not a postcode). Keep postcode None.
        location_text = (item.get("locationName") or "").strip() or None
        postcode = None

        min_rate = item.get("minimumSalary")
        max_rate = item.get("maximumSalary")

        # Best-effort rate_type / contract_type from details (optional)
        rate_type = None
        contract_type = None
        url = item.get("jobUrl") or None

        if details:
            # salaryType e.g. "per annum", contractType e.g. "permanent"
            rate_type = details.get("salaryType") or details.get("SalaryType")
            contract_type = details.get("contractType") or details.get("ContractType")
            # prefer canonical URL field if present
            url = details.get("url") or details.get("jobUrl") or details.get("externalUrl") or url

        posted_date = self._parse_posted_date(item.get("date") or (details or {}).get("date"))

        raw_json = {"search_item": item}
        if details:
            raw_json["details"] = details

        # Use SimpleNamespace so cron_runner can do attribute access
        return SimpleNamespace(
            title=title,
            company_name=company_name,
            location_text=location_text,
            postcode=postcode,
            min_rate=min_rate,
            max_rate=max_rate,
            rate_type=rate_type,
            contract_type=contract_type,
            source_site="reed",
            external_id=str(job_id) if job_id is not None else None,
            url=url,
            posted_date=posted_date,
            raw_json=raw_json,
        )

    def scrape(self) -> List[Any]:
        """
        Returns a list of record-like objects for ingestion.
        """
        results: List[Any] = []

        # Reed limits resultsToTake to max 100 :contentReference[oaicite:2]{index=2}
        take = max(1, min(int(self.results_per_page), 100))
        skip = 0

        for _page in range(1, int(self.max_pages) + 1):
            params: Dict[str, Any] = {
                "keywords": self.keywords,
                "resultsToTake": take,
                "resultsToSkip": skip,
            }
            if self.location_name:
                params["locationName"] = self.location_name
                params["distanceFromLocation"] = int(self.distance_from_location)

            data = self._get("/search", params=params)
            items = data.get("results") or []
            if not items:
                break

            for item in items:
                details = None
                if self.fetch_details and item.get("jobId"):
                    try:
                        details = self._get(f"/jobs/{item['jobId']}")
                    except Exception:
                        # Don’t fail the whole scrape if one details call dies
                        details = None

                results.append(self._to_record(item, details=details))

            # paging
            skip += take
            time.sleep(self.throttle_seconds)

        return results
