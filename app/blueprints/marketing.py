from flask import Blueprint, render_template

marketing_bp = Blueprint(
    "marketing",
    __name__,
    template_folder="templates"
)

# --- Marketing / Top-Level Site Pages ---

@marketing_bp.route("/solutions")
def solutions():
    return render_template("solutions.html")

@marketing_bp.route("/data")
def data_page():
    return render_template("data.html")

@marketing_bp.route("/customer-success")
def customer_success():
    return render_template("customer_success.html")

@marketing_bp.route("/pricing")
def pricing():
    return render_template("pricing.html")

@marketing_bp.route("/resources")
def resources_page():
    return render_template("resources.html")

@marketing_bp.route("/company")
def company():
    return render_template("company.html")
