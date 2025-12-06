# app/blueprints/api.py

from flask import Blueprint, request, jsonify

from .pay_compare import get_pay_explorer_data

bp = Blueprint("api", __name__)


@bp.route("/pay-compare", methods=["GET"])
def pay_compare():
    """
    Backing endpoint for the Pay Explorer page.

    Query params (all optional):
      - sector
      - job_role_group
      - group_by: county | sector | sector_county (default: county)
      - start_date: YYYY-MM-DD
      - end_date: YYYY-MM-DD
    """
    sector = (request.args.get("sector") or "").strip() or None
    job_role_group = (request.args.get("job_role_group") or "").strip() or None
    group_by = (request.args.get("group_by") or "county").strip() or "county"
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    data = get_pay_explorer_data(
        start_date_str=start_date,
        end_date_str=end_date,
        sector=sector,
        job_role_group=job_role_group,
        group_by=group_by,
    )
    return jsonify(data)
