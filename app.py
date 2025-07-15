from flask import Flask, render_template, request, redirect, url_for, flash, session
from flask_login import LoginManager, login_required, login_user, logout_user, current_user
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from werkzeug.security import check_password_hash
from werkzeug.utils import secure_filename
from datetime import datetime
from sqlalchemy import func
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
    username = db.Column(db.String(100), unique=True, nullable=False)
    password = db.Column(db.String(200), nullable=False)
    admin_level = db.Column(db.Integer, default=0)

    def is_admin(self):
        return self.admin_level in [1, 2]

    def is_superuser(self):
        return self.admin_level == 1

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))



@app.route("/admin/users", methods=["GET", "POST"])
@login_required
def manage_users():
    if not current_user.is_authenticated or not current_user.is_superuser():
        flash("Access denied – superuser only.", "error")
        return redirect(url_for("home"))

    if request.method == "POST":
        action = request.form.get("action")
        
        if action == "add":
            username = request.form.get("username")
            password = request.form.get("password")
            admin_level = int(request.form.get("admin_level", 0))

            if User.query.filter_by(username=username).first():
                flash("Username already exists.", "error")
            else:
                from werkzeug.security import generate_password_hash
                new_user = User(
                    username=username,
                    password=generate_password_hash(password),
                    admin_level=admin_level
                )
                db.session.add(new_user)
                db.session.commit()
                flash(f"User '{username}' added.", "success")

        elif action == "delete":
            user_id = request.form.get("user_id")
            if int(user_id) == current_user.id:
                flash("You cannot delete your own account.", "error")
            else:
                user = User.query.get(user_id)
                if user:
                    db.session.delete(user)
                    db.session.commit()
                    flash("User deleted.", "info")

        elif action == "update":
            user_id = request.form.get("user_id")
            admin_level = int(request.form.get("admin_level"))
            user = User.query.get(user_id)
            if user:
                user.admin_level = admin_level
                db.session.commit()
                flash("User updated.", "success")

    users = User.query.all()
    return render_template("manage_users.html", users=users)


# ---------------------- ROUTES ----------------------

@app.route("/")
@login_required
def home():
    return render_template("index.html", now=datetime.now)


@app.route("/records")
@login_required
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
        return redirect(url_for("map_sector_select"))

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
    return redirect(url_for('upload'))

@app.context_processor
def inject_now():
    return {'current_year': datetime.now().year}


@app.route('/dashboard')
@login_required
def dashboard():
    # Get filter values from query string
    selected_sector = request.args.get('sector')
    selected_county = request.args.get('county')
    selected_role = request.args.get('role')

    # Base query
    query = JobRecord.query

    # Apply filters
    if selected_sector:
        query = query.filter(JobRecord.sector == selected_sector)
    if selected_county:
        query = query.filter(JobRecord.county == selected_county)
    if selected_role:
        query = query.filter(JobRecord.job_role == selected_role)

    filtered_records = query.all()

    # Totals and averages from filtered records
    total_records = len(filtered_records)
    total_companies = len(set(r.company_id for r in filtered_records))
    avg_pay = round(sum(r.pay_rate for r in filtered_records) / total_records, 2) if total_records > 0 else 0

    # Group by sector
    by_sector = db.session.query(
        JobRecord.sector,
        func.count(),
        func.avg(JobRecord.pay_rate)
    ).filter(query.whereclause).group_by(JobRecord.sector).all()

    # Group by county
    by_county = db.session.query(
        JobRecord.county,
        func.count()
    ).filter(query.whereclause).group_by(JobRecord.county).all()

    # Available options for filters
    all_records = JobRecord.query.all()
    available_sectors = sorted(set(r.sector for r in all_records if r.sector))
    available_counties = sorted(set(r.county for r in all_records if r.county))
    available_roles = sorted(set(r.job_role for r in all_records if r.job_role))

    return render_template(
        "dashboard.html",
        total_records=total_records,
        total_companies=total_companies,
        avg_pay=avg_pay,
        by_sector=by_sector,
        by_county=by_county,
        available_sectors=available_sectors,
        available_counties=available_counties,
        available_roles=available_roles,
    )


@app.route("/insights")
@login_required
def insights():
    sector = request.args.get("sector")
    job_role = request.args.get("job_role")
    county = request.args.get("county")
    month = request.args.get("month")
    year = request.args.get("year")

    # Base query
    query = JobRecord.query

    # Apply filters if present
    if sector:
        query = query.filter(JobRecord.sector == sector)
    if job_role:
        query = query.filter(JobRecord.job_role == job_role)
    if county:
        query = query.filter(JobRecord.county == county)
    if month:
        query = query.filter(JobRecord.imported_month == month)
    if year:
        query = query.filter(JobRecord.imported_year == year)

    records = query.all()

    # Convert records to serializable format for JS
    serialized_records = [
        {
            "company_id": r.company_id,
            "company_name": r.company_name,
            "sector": r.sector,
            "job_role": r.job_role,
            "postcode": r.postcode,
            "county": r.county,
            "pay_rate": r.pay_rate,
            "month": r.imported_month,
            "year": r.imported_year
        }
        for r in records
    ]

    # Filter options
    options = {
        "sectors": sorted({r.sector for r in JobRecord.query.all() if r.sector}),
        "roles": sorted({r.job_role for r in JobRecord.query.all() if r.job_role}),
        "counties": sorted({r.county for r in JobRecord.query.all() if r.county}),
        "months": sorted({r.imported_month for r in JobRecord.query.all() if r.imported_month}),
        "years": sorted({r.imported_year for r in JobRecord.query.all() if r.imported_year}),
    }

    return render_template(
        "insights.html",
        records=serialized_records,
        filters={
            "sector": sector or "",
            "job_role": job_role or "",
            "county": county or "",
            "month": month or "",
            "year": year or "",
        },
        options=options
    )
@app.route('/company/<company_id>')
@login_required
def company_profile(company_id):
    jobs = JobRecord.query.filter_by(company_id=company_id).all()
    if not jobs:
        flash("No records found for this company.", "warning")
        return redirect(url_for('records'))

    company_name = jobs[0].company_name
    sector = jobs[0].sector
    logo_url = url_for('static', filename=f'logos/{company_id}.png')
    average_pay = round(
        sum(j.pay_rate for j in jobs if j.pay_rate is not None) / len(jobs), 2
    )

    from collections import defaultdict
    county_pay = defaultdict(list)

    for j in jobs:
        if j.county:
            county_pay[j.county].append(j.pay_rate)

    county_avg = {k: round(sum(v) / len(v), 2) for k, v in county_pay.items()}
    counties = list(county_avg.keys())
    if not counties:
        flash("This company has no valid county data. Showing job list only.", "warning")
        peer_companies = []
    # No `else:` — just let the code below run if `counties` exists

    # Peer companies in same county + same sector
    peer_jobs = JobRecord.query.filter(
        JobRecord.company_id != company_id,
        JobRecord.county.in_(counties),
        JobRecord.sector == sector
    ).all()


    # Group by peer company
    peer_data = {}
    for j in peer_jobs:
        if j.company_id not in peer_data:
            peer_data[j.company_id] = {
                'company_name': j.company_name,
                'jobs': [],
                'logo': f"/static/logos/{j.company_id}.png"
            }
        peer_data[j.company_id]['jobs'].append(j.pay_rate)

    peer_companies = []
    for cid, data in peer_data.items():
        avg = round(sum(data['jobs']) / len(data['jobs']), 2)
        peer_companies.append({
            'company_id': cid,
            'company_name': data['company_name'],
            'logo_url': data['logo'],
            'average_pay': avg
        })

    return render_template(
        'company_profile.html',
        company_name=company_name,
        logo_url=logo_url,
        jobs=jobs,
        average_pay=average_pay,
        county_avg=county_avg,
        peer_companies=peer_companies
    )






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
        remember = 'remember' in request.form
        user = User.query.filter_by(username=username).first()
        if user and check_password_hash(user.password, password):
            login_user(user, remember=remember)
            flash('Logged in successfully.', 'success')
            next_page = request.args.get('next')
            return redirect(next_page) if next_page else redirect(url_for('home'))
        flash('Invalid username or password.', 'error')
    return render_template('login.html')

@app.route('/logout')
def logout():
    logout_user()
    flash('Logged out.', 'info')
    return redirect(url_for('login'))

# ---------------------- MAIN ----------------------

if __name__ == '__main__':
    import os
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
