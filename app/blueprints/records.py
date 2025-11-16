# app/blueprints/records.py
from __future__ import annotations

from io import BytesIO
from datetime import datetime, timezone
import pandas as pd

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file
from flask_login import login_required
from extensions import db
from models import JobRecord
from .utils import (
    build_filters_from_request,
    get_filter_options,
    commit_or_rollback,
    logo_url_for,
)

bp = Blueprint("records", __name__)

@bp.route("/records")
@login_required
def records():
    page = request.args.get("page", 1, type=int)
    edit_id = request.args.get("edit_id", type=int)

    # NEW: free text + rate bounds included
    filters_map = {
        "q": request.args.get("q"),
        "sector": request.args.get("sector"),
        "job_role": request.args.get("job_role"),
        "county": request.args.get("county"),
        "month": request.args.get("month"),
        "year": request.args.get("year"),
        "rate_min": request.args.get("rate_min"),
        "rate_max": request.args.get("rate_max"),
    }

    filters, extra_search = build_filters_from_request(filters_map)
    base_q = JobRecord.query.filter(*filters)
    if extra_search is not None:
        base_q = extra_search(base_q)

    pagination = base_q.order_by(
        JobRecord.imported_year.desc(),
        JobRecord.imported_month.desc(),
        JobRecord.company_name.asc()
    ).paginate(page=page, per_page=25, error_out=False)

    all_records = pagination.items
    options = get_filter_options()
    selected_record = db.session.get(JobRecord, edit_id) if edit_id else None

    return render_template(
        "records.html",
        records=all_records,
        pagination=pagination,
        filters=filters_map,
        options=options,
        filter_query=request.query_string.decode(),
        selected_record=selected_record,
    )

@bp.route("/edit/<int:record_id>", methods=["GET", "POST"])
@login_required
def edit_record(record_id: int):
    record = JobRecord.query.get_or_404(record_id)

    if request.method == "POST":
        record.company_id = request.form.get("company_id", record.company_id)
        record.company_name = request.form.get("company_name", record.company_name)
        record.sector = request.form.get("sector", record.sector)
        record.job_role = request.form.get("job_role", record.job_role)
        record.postcode = request.form.get("postcode", record.postcode)
        record.county = request.form.get("county", record.county)
        pay_rate = request.form.get("pay_rate", None)
        if pay_rate is not None and pay_rate != "":
            try:
                record.pay_rate = float(pay_rate)
            except ValueError:
                flash("Invalid pay rate.", "error")
                return redirect(request.referrer or url_for("records.records"))
        try:
            commit_or_rollback()
            flash(f"Record {record_id} updated.", "success")
        except Exception:
            flash("Failed to update record.", "error")
        return redirect(request.referrer or url_for("records.records"))

    return jsonify(
        {
            "id": record.id,
            "company_id": record.company_id,
            "company_name": record.company_name,
            "sector": record.sector,
            "job_role": record.job_role,
            "postcode": record.postcode,
            "county": record.county,
            "pay_rate": record.pay_rate,
        }
    )

@bp.route("/delete/<int:record_id>", methods=["POST"])
@login_required
def delete_record(record_id: int):
    record = JobRecord.query.get_or_404(record_id)
    db.session.delete(record)
    try:
        commit_or_rollback()
        flash(f"Record {record_id} deleted.", "success")
    except Exception:
        flash("Failed to delete record.", "error")
    return redirect(request.referrer or url_for("records.records"))

@bp.route("/export")
@login_required
def export_records():
    """Export current filtered records to Excel (default) or CSV via ?format=csv."""
    export_format = (request.args.get("format") or "xlsx").lower()

    # Respect new filters as well
    filters_map = {
        "q": request.args.get("q"),
        "sector": request.args.get("sector"),
        "job_role": request.args.get("job_role"),
        "county": request.args.get("county"),
        "month": request.args.get("month"),
        "year": request.args.get("year"),
        "rate_min": request.args.get("rate_min"),
        "rate_max": request.args.get("rate_max"),
    }
    filters, extra_search = build_filters_from_request(filters_map)

    q = db.session.query(JobRecord).filter(*filters)
    if extra_search is not None:
        q = extra_search(q)

    rows = q.order_by(JobRecord.imported_year.desc(), JobRecord.imported_month.desc()).all()

    data = [
        {
            "company_id": r.company_id,
            "company_name": r.company_name,
            "sector": r.sector,
            "job_role": r.job_role,
            "postcode": r.postcode,
            "county": r.county,
            "pay_rate": r.pay_rate,
            "imported_month": r.imported_month,
            "imported_year": r.imported_year,
            "latitude": r.latitude,
            "longitude": r.longitude,
        }
        for r in rows
    ]

    df = pd.DataFrame(data)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    if export_format == "csv":
        buf = BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(
            buf,
            as_attachment=True,
            download_name=f"pay-rate-export-{stamp}.csv",
            mimetype="text/csv",
        )

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="records")
    buf.seek(0)
    return send_file(
        buf,
        as_attachment=True,
        download_name=f"pay-rate-export-{stamp}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

@bp.route("/company/<company_id>")
@login_required
def company_profile(company_id: str):
    jobs = JobRecord.query.filter_by(company_id=company_id).all()
    if not jobs:
        flash("No records found for this company.", "warning")
        return redirect(url_for("records.records"))

    company_name = jobs[0].company_name
    sector = jobs[0].sector
    logo_url = logo_url_for(company_id)

    pays = [j.pay_rate for j in jobs if j.pay_rate is not None]
    average_pay = round(sum(pays) / len(pays), 2) if pays else 0

    from collections import defaultdict

    county_pay = defaultdict(list)
    for j in jobs:
        if j.county and j.pay_rate is not None:
            county_pay[j.county].append(j.pay_rate)

    county_avg = {k: round(sum(v) / len(v), 2) for k, v in county_pay.items()}
    counties = list(county_avg.keys())

    peer_companies = []
    if counties:
        peer_jobs = JobRecord.query.filter(
            JobRecord.company_id != company_id,
            JobRecord.county.in_(counties),
            JobRecord.sector == sector,
        ).all()

        peer_data = {}
        for j in peer_jobs:
            if j.company_id not in peer_data:
                peer_data[j.company_id] = {
                    "company_name": j.company_name,
                    "jobs": [],
                    "logo": logo_url_for(j.company_id),
                }
            if j.pay_rate is not None:
                peer_data[j.company_id]["jobs"].append(j.pay_rate)

        for cid, data in peer_data.items():
            if data["jobs"]:
                avg = round(sum(data["jobs"]) / len(data["jobs"]), 2)
                peer_companies.append(
                    {
                        "company_id": cid,
                        "company_name": data["company_name"],
                        "logo_url": data["logo"],
                        "average_pay": avg,
                    }
                )
    else:
        flash("This company has no valid county data. Showing job list only.", "warning")

    return render_template(
        "company_profile.html",
        company_name=company_name,
        logo_url=logo_url,
        jobs=jobs,
        average_pay=average_pay,
        county_avg=county_avg,
        peer_companies=peer_companies,
    )
