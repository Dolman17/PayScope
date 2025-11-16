# app/cli.py
from __future__ import annotations

import os
import click
import pandas as pd
from datetime import datetime, timezone

from extensions import db
from models import JobRecord
from .blueprints.utils import (
    normalize_uk_postcode,
    bulk_geocode_postcodes,
    geocode_postcode_cached,
)
from .blueprints.utils import commit_or_rollback

def _read_dataframe_from_path(path: str):
    ext = os.path.splitext(path.lower())[1]
    if ext == ".csv":
        df = pd.read_csv(path)
    elif ext in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    else:
        raise ValueError("Unsupported file type; use .csv, .xlsx, or .xls")
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df, ext

def _ingest_df(df, *, month: int | None, year: int | None, skip_geocode: bool = False) -> int:
    required = {"company_id", "company_name", "sector", "postcode", "job_role", "pay_rate"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {', '.join(missing)}")

    now_utc = datetime.now(timezone.utc)
    imonth = str(month or now_utc.month)
    iyear = str(year or now_utc.year)

    pc_to_latlon: dict[str, tuple[float | None, float | None]] = {}
    if not skip_geocode and "postcode" in df.columns:
        postcodes = df["postcode"].astype(str).fillna("").map(normalize_uk_postcode).tolist()
        pc_to_latlon = bulk_geocode_postcodes(postcodes)

    added = 0
    for _, row in df.iterrows():
        rowd = row.to_dict()
        postcode_raw = str(rowd.get("postcode", "") or "")
        postcode = normalize_uk_postcode(postcode_raw)
        if skip_geocode or not postcode:
            lat, lon = (None, None)
        else:
            lat, lon = pc_to_latlon.get(postcode, (None, None))
            if lat is None or lon is None:
                lat, lon = geocode_postcode_cached(postcode)

        rec = JobRecord(
            company_id=str(rowd.get("company_id", "") or ""),
            company_name=str(rowd.get("company_name", "") or ""),
            sector=str(rowd.get("sector", "") or ""),
            postcode=postcode,
            job_role=str(rowd.get("job_role", "") or ""),
            pay_rate=float(rowd.get("pay_rate") or 0.0),
            county=str(rowd.get("county", "") or ""),
            latitude=lat,
            longitude=lon,
            imported_month=imonth,
            imported_year=iyear,
        )
        db.session.add(rec)
        added += 1

    commit_or_rollback()
    return added

@click.command("purge-records")
def purge_records():
    """Delete ALL JobRecord rows (keeps users)."""
    count = db.session.query(JobRecord).delete(synchronize_session=False)
    db.session.commit()
    click.echo(f"Purged {count} JobRecord rows.")

@click.command("import-data")
@click.argument("path", type=click.Path(exists=True, dir_okay=False))
@click.option("--purge", is_flag=True, help="Delete all existing JobRecord rows first.")
@click.option("--skip-geocode", is_flag=True, help="Do not geocode postcodes (faster).")
@click.option("--month", type=int, default=None, help="Override imported month (1-12).")
@click.option("--year", type=int, default=None, help="Override imported year, e.g. 2025.")
def import_data(path, purge, skip_geocode, month, year):
    """Import Excel/CSV file into JobRecord."""
    if purge:
        from flask.cli import with_appcontext
        # Call purge directly (we're inside app context when registered)
        count = db.session.query(JobRecord).delete(synchronize_session=False)
        db.session.commit()
        click.echo(f"Purged {count} JobRecord rows.")
    df, _ = _read_dataframe_from_path(path)
    added = _ingest_df(df, month=month, year=year, skip_geocode=skip_geocode)
    click.echo(f"Imported {added} records from {os.path.basename(path)} (skip_geocode={skip_geocode}).")

@click.command("geocode-missing")
@click.option("--limit", type=int, default=None, help="Max rows to process this run.")
def geocode_missing(limit):
    """Bulk-geocode JobRecords missing lat/lon using postcodes.io."""
    rows_q = JobRecord.query.filter(
        (JobRecord.latitude == None) | (JobRecord.longitude == None)  # noqa: E711
    )
    if limit:
        rows_q = rows_q.limit(limit)
    rows = rows_q.all()
    pcs = [normalize_uk_postcode(r.postcode or "") for r in rows if r.postcode]
    mapping = bulk_geocode_postcodes(pcs)
    updated = 0
    for r in rows:
        pc = normalize_uk_postcode(r.postcode or "")
        latlon = mapping.get(pc)
        if latlon and latlon[0] is not None:
            r.latitude, r.longitude = latlon
            updated += 1
    commit_or_rollback()
    click.echo(f"Geocoded {updated} of {len(rows)} records.")
