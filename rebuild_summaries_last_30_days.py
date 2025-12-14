# rebuild_summaries_last_30_days.py
from __future__ import annotations

from datetime import date, timedelta

from app import create_app
from job_summaries import build_daily_job_summaries


def run(days: int = 30):
    app = create_app()
    with app.app_context():
        today = date.today()
        start = today - timedelta(days=days - 1)

        print(f"Rebuilding JobSummaryDaily for {days} days: {start} -> {today}")

        total_created = 0
        for i in range(days):
            d = start + timedelta(days=i)
            created = build_daily_job_summaries(d, delete_existing=True)
            total_created += created
            print(f"{d}: created {created}")

        print(f"✅ Done. Total summary rows created: {total_created}")


if __name__ == "__main__":
    run(60)
