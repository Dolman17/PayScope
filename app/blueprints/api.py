# app/blueprints/api.py

# app/blueprints/api.py

from flask import Blueprint, request, jsonify
from .pay_compare import get_pay_comparison   # ⬅️ LOCAL IMPORT (same folder)

bp = Blueprint("api", __name__)


@bp.route("/pay-compare", methods=["GET"])
def pay_compare():
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

