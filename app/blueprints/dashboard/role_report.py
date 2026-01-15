from __future__ import annotations

import csv
import io
from typing import Dict, List

from flask import render_template, redirect, url_for, flash, Response
from flask_login import login_required, current_user

from . import bp
from .helpers import (
    _job_roles_report_data,
    _unmapped_role_hotspots,
    _sector_override_mismatches,
)


@bp.route("/admin/job-roles/report")
@login_required
def admin_job_roles_report():
    """
    Report: for each canonical role, show which raw job_role values map to it,
    plus counts of JobRecord rows per raw value.

    This is read-only and safe for export.
    """
    # Keep consistent with dashboard access: superusers only.
    if not getattr(current_user, "is_superuser", None) or not current_user.is_superuser():
        flash("You do not have access to the Job Role Mapping report.", "error")
        return redirect(url_for("auth.home"))

    summary, grouped_roles = _job_roles_report_data()
    unmapped_hotspots = _unmapped_role_hotspots(limit=50)
    sector_mismatches = _sector_override_mismatches()

    return render_template(
        "admin_job_roles_report.html",
        summary=summary,
        grouped_roles=grouped_roles,
        unmapped_hotspots=unmapped_hotspots,
        sector_mismatches=sector_mismatches,
    )


@bp.route("/admin/job-roles/report/export")
@login_required
def admin_job_roles_report_export():
    """
    CSV export for the Job Role Mapping report.

    One row per (canonical_role, raw_value) with JobRecord count:
      canonical_role, raw_value, jobrecord_count
    """
    # Same access control as the HTML report
    if not getattr(current_user, "is_superuser", None) or not current_user.is_superuser():
        flash("You do not have access to the Job Role Mapping export.", "error")
        return redirect(url_for("auth.home"))

    _summary, grouped_roles = _job_roles_report_data()

    # Flatten into rows for CSV
    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    writer.writerow(["canonical_role", "raw_value", "jobrecord_count"])

    for canonical_role, raw_list in grouped_roles.items():
        cr = canonical_role or "—"
        for item in raw_list:
            writer.writerow(
                [
                    cr,
                    item.get("raw_value") or "—",
                    item.get("count") or 0,
                ]
            )

    csv_data = output.getvalue()
    output.close()

    resp = Response(csv_data, mimetype="text/csv")
    resp.headers["Content-Disposition"] = 'attachment; filename="job_role_mapping_report.csv"'
    return resp
