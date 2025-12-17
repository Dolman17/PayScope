# summary_runner.py
from __future__ import annotations

import os
import json
from datetime import date, timedelta

from app import create_app
from extensions import db
from models import CronRunLog
from job_summaries import build_daily_job_summaries

DEFAULT_DAYS_BACK = int(os.getenv("SUMMARY_DAYS_BACK", "14"))
JOB_NAME = os.getenv("SUMMARY_JOB_NAME", "job_summary_daily_rebuild")


def _daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d = d + timedelta(days=1)


def main() -> int:
    app = create_app()

    with app.app_context():
        run = CronRunLog(
            job_name=JOB_NAME,
            status="running",
            message=f"Rebuilding JobSummaryDaily for last {DEFAULT_DAYS_BACK} day(s).",
            rows_scraped=None,
            records_created=None,
            triggered_by="summary_runner",
            trigger=os.getenv("TRIGGER", "cron"),
            day_label=date.today().isoformat(),
            run_stats=None,
        )
        db.session.add(run)
        db.session.commit()

        try:
            end = date.today()
            start = end - timedelta(days=DEFAULT_DAYS_BACK - 1)

            created_total = 0
            per_day = []

            # Important: do it day-by-day so each day commits and you don’t blow memory/time.
            for d in _daterange(start, end):
                created = build_daily_job_summaries(d, delete_existing=True)
                created_total += int(created or 0)
                per_day.append({"date": d.isoformat(), "rows_created": int(created or 0)})

            run.status = "success"
            run.message = f"JobSummaryDaily rebuilt for {start.isoformat()} → {end.isoformat()}."
            run.records_created = created_total
            run.run_stats = json.dumps(
                {"days_back": DEFAULT_DAYS_BACK, "total_rows_created": created_total, "per_day": per_day},
                ensure_ascii=False,
            )
            db.session.add(run)
            db.session.commit()
            return 0

        except Exception as e:
            db.session.rollback()
            run.status = "error"
            run.message = f"Summary rebuild failed: {e}"
            db.session.add(run)
            db.session.commit()
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
