# ons_loader.py
from __future__ import annotations

"""
Helpers for pulling ONS / Nomis earnings data (ASHE) via the REST API.

This module is deliberately lightweight:

- Reads your Nomis Unique ID from the NOMIS_UID env var.
- Fetches Annual Survey of Hours and Earnings (ASHE) data for a single year.
- Returns a list of parsed rows ready to be stored / joined in the app.

At the moment this DOES NOT write to the database. The function
`import_ons_earnings_for_year()` just downloads + parses and returns
a summary dict so we can iterate on the schema safely.
"""

import csv
import io
import os
from dataclasses import dataclass
from datetime import date
from typing import List, Dict, Any, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

BASE_URL = "https://www.nomisweb.co.uk/api/v01"

# Your Nomis Unique ID (from the page you screenshotted)
# e.g. NOMIS_UID=0x2ebb06d0df0f3107c4cc4ea8211320cc306e94b
NOMIS_UID = os.getenv("NOMIS_UID")

# ------------------- DATASET & PARAMS --------------------
# We HARD-CODE the ASHE dataset id here to avoid legacy NM_99_1 issues.
# If Nomis changes naming again later, we can revisit, but for now this
# prevents env vars pointing to old datasets from breaking imports.

ASHE_DATASET_ID = "ASHE"  # do NOT override via env; NM_99_1 is deprecated for 2024+

# Geography dimension:
#   TYPE450 = Local authorities: County / Unitary (for England/Wales)
ASHE_GEOGRAPHY = os.getenv("NOMIS_ASHE_GEOGRAPHY")  # e.g. "TYPE450" or explicit geo codes

# Measures:
#   20101 = Median gross hourly pay (excluding overtime)
ASHE_MEASURES = os.getenv("NOMIS_ASHE_MEASURES")  # e.g. "20101"

# Sex / full-time / occupation etc can also be constrained.
# Copy these straight from the Nomis URL.
ASHE_EXTRA_PARAMS = os.getenv("NOMIS_ASHE_EXTRA_PARAMS", "").strip()
# Example:
#   NOMIS_ASHE_EXTRA_PARAMS="sex=0&item=1&pay=5&freq=A"
# These get appended to the query string as-is.

# ASHE API typically uses "date" for the year.
ASHE_DATE_PARAM = os.getenv("NOMIS_ASHE_DATE_PARAM", "date")  # usually "date"


# -------------------------------------------------------------------
# Types
# -------------------------------------------------------------------

@dataclass
class OnsEarningsRow:
    year: int
    geography_code: str
    geography_name: str
    measure_code: str
    value: Optional[float]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "year": self.year,
            "geography_code": self.geography_code,
            "geography_name": self.geography_name,
            "measure_code": self.measure_code,
            "value": self.value,
        }


# -------------------------------------------------------------------
# Low-level HTTP helper
# -------------------------------------------------------------------

def _nomis_get_csv(dataset_id: str, params: Dict[str, Any]) -> str:
    """
    Fetch raw CSV from a Nomis dataset.

    `dataset_id` is something like "ASHE".
    `params` is the querystring dict (geography, time/date, measures, etc).
    """
    if not dataset_id:
        raise ValueError(
            "ASHE_DATASET_ID is not set. This should be hard-coded to 'ASHE'."
        )

    url = f"{BASE_URL}/dataset/{dataset_id}.data.csv"

    # Inject UID if available
    if NOMIS_UID:
        params = dict(params)
        params["uid"] = NOMIS_UID

    resp = requests.get(url, params=params, timeout=30)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        status = resp.status_code
        if status == 406:
            raise RuntimeError(
                f"Nomis request failed: 406 Not Acceptable for dataset '{dataset_id}' "
                f"with params {params!r}. This often means the parameter combination "
                f"is not valid for that year. | URL={resp.url} | status={status}"
            ) from e
        raise RuntimeError(
            f"Nomis request failed: {e!r} | URL={resp.url} | status={status}"
        ) from e

    return resp.text


# -------------------------------------------------------------------
# High-level ASHE earnings importer
# -------------------------------------------------------------------

def _ensure_config():
    """Simple guard to make sure you've wired the environment vars."""
    missing = []
    # Dataset id is hard-coded now, so no check here.
    if not ASHE_GEOGRAPHY:
        missing.append("NOMIS_ASHE_GEOGRAPHY (e.g. TYPE450 or explicit geography codes)")
    if not ASHE_MEASURES:
        missing.append("NOMIS_ASHE_MEASURES (e.g. 20101 for median hourly)")

    if missing:
        raise ValueError(
            "ONS / Nomis ASHE config is incomplete. Please set:\n- "
            + "\n- ".join(missing)
            + "\n\nUse the Nomis UI to build your ASHE query, "
              "copy the RESTful link, and copy the geography + measures "
              "into these environment variables."
        )


def import_ons_earnings_for_year(year: int) -> Dict[str, Any]:
    """
    Fetch ASHE earnings for a single year from Nomis.

    Returns a summary dict:

      {
        "year": 2024,
        "dataset_id": "...",
        "measure_code": "...",
        "row_count": 152,
        "rows": [OnsEarningsRow.as_dict(), ...],
      }
    """
    _ensure_config()

    # Build core params for the CSV call.
    params: Dict[str, Any] = {
        ASHE_DATE_PARAM: str(year),        # e.g. 'date': '2024'
        "geography": ASHE_GEOGRAPHY,
        "measures": ASHE_MEASURES,
        # Only pull what we need — names + codes + value.
        "select": "geography_code,geography_name,measures,obs_value",
        # One row per geography x measure.
        "rows": "geography",
        "cols": "measures",
    }

    # Allow you to tack on extra params straight from your Nomis URL,
    # e.g. "sex=0&item=1&pay=5&freq=A".
    if ASHE_EXTRA_PARAMS:
        for part in ASHE_EXTRA_PARAMS.split("&"):
            part = part.strip()
            if not part or "=" not in part:
                continue
            k, v = part.split("=", 1)
            if k and k not in params:
                params[k] = v

    csv_text = _nomis_get_csv(ASHE_DATASET_ID, params)

    # Parse CSV
    f = io.StringIO(csv_text)
    reader = csv.DictReader(f)

    rows: List[OnsEarningsRow] = []
    for raw in reader:
        # Nomis column names are typically upper-case: GEOGRAPHY_CODE, OBS_VALUE, etc.
        geo_code = (raw.get("GEOGRAPHY_CODE") or "").strip()
        geo_name = (raw.get("GEOGRAPHY_NAME") or "").strip()
        measure = (raw.get("MEASURES") or raw.get("MEASURES_NAME") or "").strip()
        val_raw = (raw.get("OBS_VALUE") or "").strip()

        if not geo_code and not geo_name:
            # Header / blank line / rubbish
            continue

        try:
            value = float(val_raw) if val_raw not in ("", ".", ":", None) else None
        except ValueError:
            value = None

        rows.append(
            OnsEarningsRow(
                year=year,
                geography_code=geo_code,
                geography_name=geo_name,
                measure_code=measure,
                value=value,
            )
        )

    summary = {
        "year": year,
        "dataset_id": ASHE_DATASET_ID,
        "measure_code": ASHE_MEASURES,
        "row_count": len(rows),
        "rows": [r.as_dict() for r in rows],
        "fetched_at": date.today().isoformat(),
    }

    print(
        f"[ONS] Imported ASHE earnings for {year}: "
        f"{summary['row_count']} rows from dataset {ASHE_DATASET_ID}"
    )

    return summary

