from datetime import date, timedelta
from collections import defaultdict
from sqlalchemy import func

from extensions import db
from models import JobSummaryDaily


def get_weekly_coverage_snapshot(days: int = 7) -> dict:
    start = date.today() - timedelta(days=days)

    rows = (
        db.session.query(
            JobSummaryDaily.sector,
            func.count(func.distinct(JobSummaryDaily.date)).label("days_seen"),
        )
        .filter(JobSummaryDaily.date >= start)
        .group_by(JobSummaryDaily.sector)
        .all()
    )

    coverage = []
    weak_sectors = []

    for r in rows:
        entry = {
            "sector": r.sector,
            "days_seen": int(r.days_seen or 0),
            "ok": (r.days_seen or 0) >= 2,
        }
        coverage.append(entry)

        if not entry["ok"]:
            weak_sectors.append(entry["sector"])

    return {
        "window_days": days,
        "sectors": sorted(coverage, key=lambda x: x["sector"]),
        "weak_sectors": weak_sectors,
        "weak_sectors_count": len(weak_sectors),
        "status": (
            "green" if len(weak_sectors) == 0
            else "amber" if len(weak_sectors) <= 3
            else "red"
        ),
    }
