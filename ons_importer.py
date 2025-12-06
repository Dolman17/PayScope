# ons_importer.py
from __future__ import annotations

"""
Upsert ONS / Nomis ASHE earnings data into the database.

This module glues together:
- ons_loader.import_ons_earnings_for_year (fetches + parses CSV from Nomis)
- OnsEarnings model (stores earnings by year + geography + measure)
- CronRunLog model (records what happened)

Usage patterns:

1) From a Python REPL (no app context needed):

    from ons_importer import import_ons_earnings_to_db
    result = import_ons_earnings_to_db(2023)
    print(result)

2) From cron_runner.py (already inside app.app_context):

    from ons_importer import import_ons_earnings_to_db
    ons_result = import_ons_earnings_to_db(
        year,
        trigger=trigger,
        triggered_by=triggered_by,
        use_app_context=True,
    )

3) Import a range of years (e.g. 2020–2023):

    from app import create_app
    from ons_importer import import_ons_earnings_range_to_db

    app = create_app()
    with app.app_context():
        summaries = import_ons_earnings_range_to_db(2020, 2023, use_app_context=True)
        print(summaries)
"""

from datetime import datetime
from typing import Any, Dict, List

from app import create_app
from extensions import db
from models import OnsEarnings, CronRunLog
from ons_loader import import_ons_earnings_for_year


# -------------------------------------------------------------------
# Core single-year implementation
# -------------------------------------------------------------------


def _import_ons_earnings_to_db_impl(
    year: int,
    trigger: str = "manual",
    triggered_by: str | None = None,
) -> Dict[str, Any]:
    """
    Core implementation. Assumes we are already inside app.app_context().

    - Calls ons_loader.import_ons_earnings_for_year(year)
    - Upserts into OnsEarnings (year + geography_code + measure_code)
    - Logs to CronRunLog as job_name="ons_ashe_import"
    """
    now = datetime.utcnow()
    day_label = str(year)[:20]

    # Create CronRunLog row
    log = CronRunLog(
        job_name="ons_ashe_import",
        started_at=now,
        finished_at=None,
        status="running",
        trigger=trigger,
        triggered_by=triggered_by,
        day_label=day_label,
        rows_scraped=None,
        records_created=None,
        message=None,
    )
    db.session.add(log)
    db.session.commit()

    summary: Dict[str, Any] = {
        "year": year,
        "log_id": log.id,
        "fetched": 0,
        "created": 0,
        "updated": 0,
        "error": None,
    }

    try:
        # Fetch from Nomis via ons_loader
        fetched = import_ons_earnings_for_year(year)
        rows = fetched.get("rows", [])
        fetched_count = int(fetched.get("row_count", len(rows)) or 0)
        measure_code = str(fetched.get("measure_code") or "").strip()

        summary["fetched"] = fetched_count

        created = 0
        updated = 0

        # Simple, robust upsert: one SELECT per geo_code + measure_code.
        # This is fine for ~25k rows on a local dev machine.
        for r in rows:
            geo_code = (r.get("geography_code") or "").strip()
            geo_name = (r.get("geography_name") or "").strip()
            measure = (r.get("measure_code") or "").strip() or measure_code
            value = r.get("value", None)

            if not geo_code:
                continue

            existing = OnsEarnings.query.filter_by(
                year=year,
                geography_code=geo_code,
                measure_code=measure,
            ).first()

            if existing:
                existing.geography_name = geo_name
                existing.value = value
                updated += 1
            else:
                db.session.add(
                    OnsEarnings(
                        year=year,
                        geography_code=geo_code,
                        geography_name=geo_name,
                        measure_code=measure,
                        value=value,
                    )
                )
                created += 1

        db.session.commit()

        log.finished_at = datetime.utcnow()
        log.status = "success"
        log.rows_scraped = fetched_count
        log.records_created = created
        log.message = (
            f"Upserted ONS ASHE for {year}: fetched={fetched_count}, "
            f"created={created}, updated={updated}"
        )
        db.session.commit()

        print(
            f"[ONS] Upserted earnings for {year}: "
            f"fetched={fetched_count}, created={created}, updated={updated}"
        )

        summary["created"] = created
        summary["updated"] = updated
        return summary

    except Exception as e:  # noqa: BLE001
        db.session.rollback()
        log.finished_at = datetime.utcnow()
        log.status = "error"
        log.message = f"{e!r}"
        db.session.commit()

        summary["error"] = repr(e)
        print(f"[ONS] Import failed for {year}: {e!r}")
        return summary


# -------------------------------------------------------------------
# Public single-year entry point
# -------------------------------------------------------------------


def import_ons_earnings_to_db(
    year: int,
    trigger: str = "manual",
    triggered_by: str | None = None,
    use_app_context: bool = False,
) -> Dict[str, Any]:
    """
    Public entry point for a single year.

    - If use_app_context=False (default):
        Creates an app and app context internally. Safe for REPL / CLI use.

    - If use_app_context=True:
        Assumes the caller has already created an app and pushed app.app_context().
        This is what cron_runner.py should use.
    """
    if use_app_context:
        # Caller is responsible for having an active app context.
        return _import_ons_earnings_to_db_impl(
            year,
            trigger=trigger,
            triggered_by=triggered_by,
        )

    app = create_app()
    with app.app_context():
        return _import_ons_earnings_to_db_impl(
            year,
            trigger=trigger,
            triggered_by=triggered_by,
        )


# -------------------------------------------------------------------
# Range helper (multi-year import)
# -------------------------------------------------------------------


def import_ons_earnings_range_to_db(
    start_year: int,
    end_year: int,
    trigger: str = "manual",
    triggered_by: str | None = None,
    use_app_context: bool = False,
) -> List[Dict[str, Any]]:
    """
    Convenience helper to import a whole range of ASHE years.

    Returns a list of per-year summary dicts in ascending year order.

    Example:

        with app.app_context():
            summaries = import_ons_earnings_range_to_db(2020, 2023, use_app_context=True)
            for s in summaries:
                print(s["year"], s["created"], s["updated"], s["error"])
    """
    if start_year > end_year:
        start_year, end_year = end_year, start_year

    summaries: List[Dict[str, Any]] = []

    if use_app_context:
        # Assume caller already created an app and pushed context.
        for year in range(start_year, end_year + 1):
            res = _import_ons_earnings_to_db_impl(
                year,
                trigger=trigger,
                triggered_by=triggered_by,
            )
            summaries.append(res)
        return summaries

    # Manage app context ourselves for CLI / quick scripts
    app = create_app()
    with app.app_context():
        for year in range(start_year, end_year + 1):
            res = _import_ons_earnings_to_db_impl(
                year,
                trigger=trigger,
                triggered_by=triggered_by,
            )
            summaries.append(res)
        return summaries

from datetime import datetime as _dt


def import_latest_ons_earnings_for_cron(
    trigger: str = "cron",
    triggered_by: str | None = None,
    use_app_context: bool = True,
) -> dict:
    """
    Convenience wrapper for cron/admin:

    - Picks a 'latest' ASHE year (typically last full year)
    - Calls import_ons_earnings_to_db with trigger + triggered_by
    - Assumes app context by default (cron_runner/admin already have it)
    """
    # ASHE for year N usually lands well into year N+1,
    # so "latest full year" is often current_year - 1.
    current_year = _dt.utcnow().year
    target_year = current_year - 1

    return import_ons_earnings_to_db(
        year=target_year,
        trigger=trigger,
        triggered_by=triggered_by,
        use_app_context=use_app_context,
    )


# -------------------------------------------------------------------
# CLI helper
# -------------------------------------------------------------------


if __name__ == "__main__":
    # Usage:
    #
    #   py ons_importer.py 2023
    #   py ons_importer.py 2020 2023
    #
    # First form imports a single year.
    # Second form imports a whole range (inclusive).
    import sys

    if len(sys.argv) == 2:
        year = int(sys.argv[1])
        result = import_ons_earnings_to_db(year)
        print(result)
    elif len(sys.argv) == 3:
        start = int(sys.argv[1])
        end = int(sys.argv[2])
        results = import_ons_earnings_range_to_db(start, end)
        print(results)
    else:
        print("Usage:")
        print("  py ons_importer.py <year>")
        print("  py ons_importer.py <start_year> <end_year>")
        raise SystemExit(1)
