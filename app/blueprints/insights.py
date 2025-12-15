# app/blueprints/insights.py
from __future__ import annotations

from datetime import date, timedelta

from flask import Blueprint, render_template, abort
from sqlalchemy import desc

from extensions import db
from models import WeeklyMarketChange, WeeklyInsight

bp = Blueprint("insights", __name__)


def _monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())


@bp.route("/insights/week/<string:week_start_iso>")
def weekly_insight(week_start_iso: str):
    """
    Public weekly insight page.
    Shows:
      - Top weekly brief (WeeklyInsight) if present
      - Published + featured WeeklyMarketChange items
    """
    try:
        raw = date.fromisoformat(week_start_iso)
    except Exception:
        abort(404)

    # Be forgiving: if someone passes a date that isn't Monday, snap to Monday.
    week_start = _monday_of(raw)
    week_end = week_start + timedelta(days=6)

    items = (
        db.session.query(WeeklyMarketChange)
        .filter(WeeklyMarketChange.week_start == week_start)
        .filter(WeeklyMarketChange.is_published.is_(True))
        .filter(WeeklyMarketChange.is_featured.is_(True))
        .order_by(desc(WeeklyMarketChange.created_at))
        .all()
    )

    if not items:
        abort(404)

    weekly_brief = (
        db.session.query(WeeklyInsight)
        .filter(WeeklyInsight.week_start == week_start)
        .first()
    )

    return render_template(
        "insights/weekly.html",
        week_start=week_start,
        week_end=week_end,
        items=items,
        weekly_brief=weekly_brief,
    )
