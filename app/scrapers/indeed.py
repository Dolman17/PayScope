# app/scrapers/indeed.py
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

from .base import BaseScraper, JobRecord, normalise_whitespace, parse_salary_range

logger = logging.getLogger(__name__)

INDEED_BASE_URL = "https://uk.indeed.com/jobs"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PayRateMapScraper/1.0; +https://example.com)"
}


class IndeedScraper(BaseScraper):
    """
    Basic scraper for Indeed UK.
    This is a best-effort HTML scraper and may need tweaks if Indeed updates markup.

    It currently fetches the first page of results for a given query/location.
    """

    source_site = "indeed"

    def __init__(self, query: str = "support worker", location: str = "United Kingdom", radius_km: int = 50):
        self.query = query
        self.location = location
        # Indeed radius is miles; rough conversion:
        self.radius_miles = min(100, max(5, int(radius_km * 0.621371)))

    def _fetch_page(self, start: int = 0) -> str:
        params = {
            "q": self.query,
            "l": self.location,
            "radius": str(self.radius_miles),
            "start": str(start),
        }
        resp = requests.get(INDEED_BASE_URL, params=params, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        return resp.text

    def _parse_posted_date(self, text: Optional[str]) -> Optional[date]:
        """
        Parse Indeed's relative 'posted' text: "Just posted", "2 days ago", "30+ days ago".
        Returns a date, or None if unknown.
        """
        if not text:
            return None

        text = text.strip().lower()
        today = date.today()

        if "today" in text or "just posted" in text:
            return today

        if "30+" in text:
            # treat as 30 days ago as a rough approximation
            return today - timedelta(days=30)

        # "2 days ago", "1 day ago"
        try:
            parts = text.split()
            num = int(parts[0])
            if "day" in text:
                return today - timedelta(days=num)
        except Exception:
            return None

        return None

    def _build_job_url(self, job_key: Optional[str], href: Optional[str]) -> Optional[str]:
        """
        Ensure we return a full URL. We prefer building from job key if available.
        """
        if job_key:
            return f"https://uk.indeed.com/viewjob?jk={job_key}"

        if not href:
            return None

        if href.startswith("http://") or href.startswith("https://"):
            return href

        if href.startswith("/"):
            return f"https://uk.indeed.com{href}"

        return f"https://uk.indeed.com/{href}"

    def scrape(self) -> List[JobRecord]:
        html = self._fetch_page(start=0)
        soup = BeautifulSoup(html, "html.parser")

        # Indeed often wraps each result in a div with class "job_seen_beacon"
        cards = soup.select("div.job_seen_beacon")
        logger.info("IndeedScraper: found %d job cards", len(cards))

        records: List[JobRecord] = []

        for card in cards:
            try:
                job_key = card.get("data-jk") or card.get("data-jobs-key")

                title_el = card.select_one("h2.jobTitle")
                if not title_el:
                    # sometimes nested span
                    title_el = card.select_one("h2 a span")

                if not title_el:
                    continue

                title = normalise_whitespace(title_el.get_text(strip=True))

                company_el = card.select_one(".companyName")
                company_name = normalise_whitespace(company_el.get_text(strip=True)) if company_el else None

                location_el = card.select_one(".companyLocation")
                location_text = normalise_whitespace(location_el.get_text(strip=True)) if location_el else None

                salary_el = card.select_one(".salary-snippet-container")
                salary_text = normalise_whitespace(salary_el.get_text(strip=True)) if salary_el else None
                min_rate, max_rate, rate_type = parse_salary_range(salary_text)

                # posted date text
                date_el = card.select_one("span.date, span.dateClass")
                posted_date = self._parse_posted_date(date_el.get_text(strip=True)) if date_el else None

                # job URL
                link_el = card.select_one("a[data-jk]") or card.select_one("a")
                href = link_el.get("href") if link_el else None
                job_url = self._build_job_url(job_key, href)

                record = JobRecord(
                    title=title,
                    company_name=company_name,
                    location_text=location_text,
                    postcode=None,  # can be post-processed/geocoded later
                    min_rate=min_rate,
                    max_rate=max_rate,
                    rate_type=rate_type,
                    contract_type=None,  # can be inferred later if needed
                    source_site=self.source_site,
                    external_id=job_key or job_url,
                    url=job_url,
                    posted_date=posted_date,
                    raw_json=None,
                )
                records.append(record)
            except Exception as exc:  # noqa: BLE001
                logger.warning("IndeedScraper: error parsing card: %s", exc, exc_info=True)
                continue

        return records
