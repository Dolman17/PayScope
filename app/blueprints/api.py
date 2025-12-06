# app/api.py

from flask import Blueprint, request, jsonify
from app.pay_compare import get_pay_comparison  # or from .pay_compare import get_pay_comparison

api_bp = Blueprint("api", __name__)


@api_bp.route("/pay-compare", methods=["GET"])
def pay_compare():
    """
    GET /api/pay-compare

    Query parameters (all optional):
    - county
    - sector
    - job_role_group
    - days (int, default 30)
    """
    county = request.args.get("county") or None
    sector = request.args.get("sector") or None
    job_role_group = request.args.get("job_role_group") or None
    days = request.args.get("days", type=int) or 30

    data = get_pay_comparison(
        county=county,
        sector=sector,
        job_role_group=job_role_group,
        days=days,
    )
    return jsonify(data)
