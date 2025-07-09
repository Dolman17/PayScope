from flask import Flask, render_template, request, redirect, url_for, flash
from flask_login import LoginManager, login_required, login_user, logout_user
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from werkzeug.security import check_password_hash
from werkzeug.utils import secure_filename
from datetime import datetime
import pandas as pd
import os
import requests

app = Flask(__name__)
app.config.from_pyfile("config.py")
db = SQLAlchemy(app)
migrate = Migrate(app, db)

login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.init_app(app)

# ---------------------- MODELS ----------------------

class JobRecord(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(50))
    company_name = db.Column(db.String(100))
    sector = db.Column(db.String(50))
    job_role = db.Column(db.String(100))
    postcode = db.Column(db.String(20))
    county = db.Column(db.String(50))
    pay_rate = db.Column(db.Float)
    imported_month = db.Column(db.String(20))
    imported_year = db.Column(db.String(10))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)

from flask_login import UserMixin

class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password = db.Column(db.String(120), nullable=False)
    is_admin = db.Column(db.Boolean, default=False)

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

# ---------------------- ROUTES ----------------------

@app.route("/")
def home():
    return redirect(url_for("records"))

@app.route("/records")
def records():
    page = request.args.get("page", 1, type=int)
    edit_id = request.args.get("edit_id", type=int)
    filters = {
        "sector": request.args.get("sector"),
        "job_role": request.args.get("job_role"),
        "county": request.args.get("county"),
        "month": request.args.get("month"),
        "year": request.args.get("year"),
    }

    query = JobRecord.query

    for key, value in filters.items():
        if value:
            if key == "month":
                query = query.filter_by(imported_month=value)
            elif key == "year":
                query = query.filter_by(imported_year=value)
            else:
                query = query.filter(getattr(JobRecord, key) == value)

    pagination = query.paginate(page=page, per_page=25)
    all_records = pagination.items

    options = {
        "sectors": sorted({r.sector for r in JobRecord.query.all()}),
        "roles": sorted({r.job_role for r in JobRecord.query.all()}),
        "counties": sorted({r.county for r in JobRecord.query.all() if r.county}),
        "months": sorted({r.imported_month for r in JobRecord.query.all()}),
        "years": sorted({r.imported_year for r in JobRecord.query.all()}),
    }

    selected_record = JobRecord.query.get(edit_id) if edit_id else None

    return render_template("records.html", records=all_records, pagination=pagination,
                           filters=filters, options=options, filter_query=request.query_string.decode(),
                           selected_record=selected_record)

@app.route("/edit/<int:record_id>", methods=["GET", "POST"])
@login_required
def edit_record(record_id):
    record = JobRecord.query.get_or_404(record_id)

    if request.method == 'POST':
        record.company_id = request.form["company_id"]
        record.company_name = request.form["company_name"]
        record.sector = request.form["sector"]
        record.job_role = request.form["job_role"]
        record.postcode = request.form["postcode"]
        record.county = request.form["county"]
        record.pay_rate = float(request.form["pay_rate"])
        db.session.commit()
        flash(f"Record {record_id} updated.", "success")
        return redirect(url_for("records"))

    return {
        "id": record.id,
        "company_id": record.company_id,
        "company_name": record.company_name,
        "sector": record.sector,
        "job_role": record.job_role,
        "postcode": record.postcode,
        "county": record.county,
        "pay_rate": record.pay_rate,
    }

@app.route("/delete/<int:record_id>", methods=["POST"])
def delete_record(record_id):
    record = JobRecord.query.get_or_404(record_id)
    db.session.delete(record)
    db.session.commit()
    flash(f"Record {record_id} deleted.", "success")
    return redirect(request.referrer or url_for("records"))

@app.route("/export")
def export_records():
    return "🚧 Export feature coming soon!"

@app.route("/map")
@login_required
def map_sector_select():
    sectors = db.session.query(JobRecord.sector).distinct().all()
    sectors = sorted([s[0] for s in sectors if s[0]])
    return render_template("map_select.html", sectors=sectors)

@app.route("/map/<sector>")
@login_required
def sector_map(sector):
    job_role = request.args.get("job_role")
    min_pay = request.args.get("min_pay", type=float)
    max_pay = request.args.get("max_pay", type=float)

    query = JobRecord.query.filter_by(sector=sector)
    if job_role:
        query = query.filter_by(job_role=job_role)
    if min_pay is not None:
        query = query.filter(JobRecord.pay_rate >= min_pay)
    if max_pay is not None:
        query = query.filter(JobRecord.pay_rate <= max_pay)

    records = query.all()

    # Pre-check logo file existence
    for record in records:
        logo_path = f"static/logos/{record.company_id}.png"
        if os.path.exists(logo_path):
            record.logo_url = url_for("static", filename=f"logos/{record.company_id}.png")
        else:
            record.logo_url = url_for("static", filename="logos/placeholder.png")

    job_roles = [r[0] for r in db.session.query(JobRecord.job_role).distinct().all()]

    return render_template(
        "map.html",
        sector=sector,
        records=records,
        job_roles=job_roles,
        filters={
            "job_role": job_role or "",
            "min_pay": min_pay or "",
            "max_pay": max_pay or ""
        }
    )


@app.route("/admin/backfill-counties")
@login_required
def backfill_counties():
    from geopy.geocoders import Nominatim
    from geopy.extra.rate_limiter import RateLimiter

    geolocator = Nominatim(user_agent="pay-rate-map")
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=2)

    updated = 0
    skipped = 0

    for record in JobRecord.query.filter((JobRecord.county == None) | (JobRecord.county == '')).all():
        if record.latitude and record.longitude:
            try:
                location = reverse((record.latitude, record.longitude), exactly_one=True)
                if location and 'county' in location.raw['address']:
                    record.county = location.raw['address']['county']
                    updated += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"❌ Error reverse geocoding ID {record.id}: {e}")
                skipped += 1

    db.session.commit()
    flash(f"✅ County backfill complete. Updated: {updated}, Skipped: {skipped}", "success")
    return redirect(url_for('records'))


# ---------------------- UPLOAD + GEOCODE ----------------------

def geocode_postcode(postcode):
    try:
        response = requests.get(
            f"https://nominatim.openstreetmap.org/search?format=json&q={postcode}",
            headers={"User-Agent": "PayRateMapUploader"}
        )
        data = response.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"Geocoding error for {postcode}: {e}")
    return None, None

@app.route("/upload", methods=['GET', 'POST'])
@login_required
def upload():
    if request.method == 'POST':
        file = request.files.get('file')
        if not file:
            flash('No file selected.', 'error')
            return redirect(request.url)

        filename = secure_filename(file.filename)
        if not filename.endswith(('.xlsx', '.xls')):
            flash('Only Excel files are supported.', 'error')
            return redirect(request.url)

        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        try:
            df = pd.read_excel(filepath)
            added = 0
            for _, row in df.iterrows():
                row_dict = {k.lower(): v for k, v in row.items()}
                try:
                    lat, lon = geocode_postcode(row_dict.get('postcode', ''))

                    record = JobRecord(
                        company_id=row_dict.get('company_id', ''),
                        company_name=row_dict.get('company_name', ''),
                        sector=row_dict.get('sector', ''),
                        postcode=row_dict.get('postcode', ''),
                        job_role=row_dict.get('job_role', ''),
                        pay_rate=row_dict.get('pay_rate', 0.0),
                        county=row_dict.get('county', ''),
                        latitude=lat,
                        longitude=lon,
                        imported_month=datetime.now().month,
                        imported_year=datetime.now().year
                    )
                    db.session.add(record)
                    added += 1
                except Exception as e:
                    print(f"⚠️ Error with row: {row_dict} ➡ {e}")

            db.session.commit()
            flash(f"{added} records successfully uploaded.", "success")
        except Exception as e:
            flash('Error processing file.', 'error')
            print(f"🚫 Upload failed ➡ {e}")

        return redirect(url_for('upload'))

    return render_template('upload.html')

# ---------------------- LOGIN ----------------------

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = User.query.filter_by(username=username).first()
        if user and check_password_hash(user.password, password):
            login_user(user)
            flash('Logged in successfully.', 'success')
            return redirect(request.args.get('next') or url_for('upload'))
        flash('Invalid username or password.', 'error')
    return render_template('login.html')

@app.route('/logout')
def logout():
    logout_user()
    flash('Logged out.', 'info')
    return redirect(url_for('login'))

if __name__ == "__main__":
    app.run(debug=True)
