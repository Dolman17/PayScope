# app/scrapers/base.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional, Tuple


@dataclass
class JobRecord:
    """
    Normalised representation of a job advert before it is written to the DB.
    """
    title: str
    company_name: Optional[str]
    location_text: Optional[str]
    postcode: Optional[str]
    min_rate: Optional[float]
    max_rate: Optional[float]
    rate_type: Optional[str]      # 'hourly', 'annual', 'daily', etc.
    contract_type: Optional[str]  # 'full-time', 'part-time', etc.
    source_site: str              # e.g. 'indeed'
    external_id: Optional[str]    # unique ID from the source site if available
    url: Optional[str]
    posted_date: Optional[date]
    raw_json: Optional[dict] = None


class BaseScraper:
    """
    Base class for all job scrapers.
    Each scraper must implement .scrape() and return a list[JobRecord].
    """

    source_site: str = "base"

    def scrape(self) -> list[JobRecord]:
        raise NotImplementedError


def normalise_whitespace(text: str) -> str:
    """
    Collapse multiple spaces/newlines into a single space.
    """
    return " ".join(text.split()) if text else text


def parse_salary_range(text: Optional[str]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Very simple UK salary parser.
    Handles things like:
        "£12.50 - £13.75 an hour"
        "From £12.00 an hour"
        "Up to £28,000 a year"
        "£23,000 - £25,000 per year"

    Returns: (min_rate, max_rate, rate_type)
    where rate_type is 'hourly', 'annual', 'daily' or None.
    """
    if not text:
        return None, None, None

    original = text
    text = text.strip().lower().replace(",", "")
    rate_type: Optional[str] = None

    if "hour" in text:
        rate_type = "hourly"
    elif "year" in text or "annum" in text:
        rate_type = "annual"
    elif "day" in text:
        rate_type = "daily"

    # remove common words
    for token in ["per hour", "an hour", "hourly", "a year", "per year", "per annum", "annum", "a day", "per day"]:
        text = text.replace(token, "")

    for token in ["from", "up to"]:
        text = text.replace(token, "")

    # strip currency symbols
    text = text.replace("£", "").strip()

    # split on range separators
    sep = "-"
    if "to" in text:
        sep = "to"

    parts = [p.strip() for p in text.split(sep) if p.strip()]

    def to_float(val: str) -> Optional[float]:
        try:
            return float(val)
        except ValueError:
            return None

    if len(parts) == 1:
        value = to_float(parts[0])
        return value, value, rate_type

    if len(parts) >= 2:
        low = to_float(parts[0])
        high = to_float(parts[1])
        return low, high, rate_type

    # fallback: give up gracefully
    return None, None, rate_type
