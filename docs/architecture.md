# PayScope – Architecture & Developer Overview

_Last updated: 23 January 2026_

This document is a “how the thing actually hangs together” map. It does **not** define product rules (see `brief.md` / PayScope Memory Spec for that); it explains where those rules are implemented in code.

---

## 1. High-level overview

**What PayScope is**

- UK-focused pay intelligence platform.
- Aggregates scraped job adverts, normalises them, and exposes:
  - **Maps** of pay by area and sector.
  - **Pay Explorer** comparisons (market vs ONS).
  - **Recruiter Radar** – one-page market snapshot for a role+area with forecast and AI commentary.
  - **Dashboards & insights** for monitoring.
  - **Admin tools** for ingestion, hygiene, coverage, and diagnostics.
  - **AI Insights** – narrative summaries of filtered views in the Insights screen.
  - **Data hygiene tools** – job-role & sector mapping, canonical label cleaning, review/export/import workflow.

**Runtime surfaces**

- **Flask web app**
  - Primary UI for users, admin tools, marketing site, and Recruiter Radar.
- **Cron / batch runner**
  - `cron_runner.py` orchestrates scraping, importing, summaries, and coverage.
- **One-off scripts**
  - Backfills, seeds, and utilities (e.g. `backfill_*`, `seed_sector_mappings.py`, `backfill_adzuna_hourly_rates.py`, `summary_runner.py`, etc.).

---

## 2. Entry points & app lifecycle

### 2.1 Web app

- **`run.py`**
  - Loads environment variables via `dotenv`.
  - Calls `app = create_app()` from `app/__init__.py`.
  - Starts the Flask development server when run directly.

- **`app/__init__.py`**
  - Creates the Flask app.
  - Loads config from `config.py`.
  - Initialises extensions:
    - SQLAlchemy (`extensions.db`)
    - Migrations (`extensions.migrate`)
    - Login manager (`login_manager` in this file).
  - Imports and **registers blueprints**:
    - `auth`, `admin`, `upload`, `records`, `maps`, `dashboard`, `api`,
      `marketing`, `public_landing`, `insights`, `company`, `recruiter_radar`
      (and any others defined in `app/blueprints`).
  - Defines an app-level `/` route (login-required home) that renders `index.html`.

### 2.2 Cron / batch

- **`cron_runner.py`**
  - Entry point for scheduled jobs (e.g. Railway cron).
  - Creates an app instance (via `create_app()`), sets up DB session.
  - Coordinates:
    - Scrapers (Adzuna etc.).
    - Importing `JobPosting` → `JobRecord`.
    - Rebuilding summaries (`JobSummaryDaily`).
    - Coverage stats and boosting.
    - Role canonicalisation passes.
  - Writes to `CronRunLog` for observability.

### 2.3 CLI helpers & scripts

Key files at the repo root:

- **Migration / DB**
  - `run_migrations.py` – wrapper for running Alembic/Flask-Migrate.
  - `seed_users.py`, `seed_sector_mappings.py` – initial data seeds.

- **Backfills / hygiene**
  - `backfill_counties.py`, `backfill_coordinates.py`, `backfill_sectors.py`,
    `backfill_other_sectors_from_roles.py`, `restore_sectors_from_postings.py`, etc.
  - `backfill_adzuna_hourly_rates.py` – audits Adzuna-sourced `JobPosting` rows and recomputes `min_rate` / `max_rate` hourly values from annual salaries using a fixed hours-per-week/weeks-per-year assumption.  
    - Flags “suspicious” postings whose current hourly values exceed a configurable threshold (e.g. `suspicious_over_hourly=30.0`) and, when `only_if_suspicious=True`, only touches those rows.
    - Applies a simple **scale fix** when the existing hourly looks like a clean 10× multiple of the recomputed rate (e.g. £162.56 → £16.26), to undo mis-scaled salaries.
    - Supports:
      - `dry_run=True` / `False` – logging-only vs real updates.
      - `commit_every` – batching for large backfills to avoid giant transactions.
      - `id_min` / `id_max` / `max_rows` – processing JobPosting IDs in slices.
    - Optionally updates linked `JobRecord` rows where `JobRecord.imported_from_posting_id` matches the corrected `JobPosting.id`, so map/records/Radar views see the corrected hourly pay immediately.

- **Summaries & ONS**
  - `summary_runner.py` – drives **day-by-day** rebuilding of `JobSummaryDaily` over a configurable window:
    - Uses `SUMMARY_DAYS_BACK` (default 30) to decide how many days to rebuild ending at “today”.
    - Wraps the run in a `CronRunLog` row (`job_name` from `SUMMARY_JOB_NAME` or a sensible default), capturing per-day row counts and totals in `run.run_stats`.
    - Calls `build_daily_job_summaries(target_date, delete_existing=True)` for each date in the window, committing each day in turn to avoid huge transactions.
  - `ons_loader.py` – loads ONS earnings into `OnsEarnings`.

- **Ops**
  - `adzuna_cron.py`, `reed_checks.py`, `backup_to_dropbox.py`,
    `payscope_backup_now.dump` (example backup).

> These scripts are intended to be run manually or via platform jobs, not as part of the web request path.

---

## 3. Project layout

At a glance:

- `/app`
  - `__init__.py` – app factory, blueprint registration, root route.
  - `blueprints/` – route handlers grouped by feature.
    - `dashboard/` is now a package with:
      - `core.py` – main dashboard, core charts.
      - `insights.py` – Insights view + AI analyse endpoint.
      - `quick_search.py` – quick search UI/API.
      - `role_admin.py` – role & sector hygiene tools (job-role cleaner, sector overrides, canonical label cleaner).
      - `role_report.py` – role mapping reports and CSV review-export/import.
      - `helpers.py` – shared helpers (canonical vocab, fuzzy matching, hygiene scoring).
  - `importers/` – logic to convert scraped postings into canonical records.
  - `scrapers/` – Adzuna/Reed/etc. scraping clients.
  - `templates/` – Jinja templates (app, admin, marketing, legal).
  - `static/` – CSS, JS, images, charts helpers.
  - `geo_backfill.py` – helpers for geocoding and location backfills.
- `/models.py` – all SQLAlchemy models.
- `/extensions.py` – shared extension instances (db, migrate, etc.).
- `/config.py` – application configuration from env vars.
- Root scripts: `run.py`, `cron_runner.py`, backfills, seeds, etc.
- `/uploads` – upload directory (e.g. for CSVs; may be git-ignored).

---

## 4. Data model (high-level groups)

_All models live in `models.py`._

### 4.1 Auth & tenancy

- **`User`**
  - Login + permissions (`admin_level` etc.).
  - Links to `Organisation` where relevant.

- **`Organisation`**
  - Represents a client organisation / tenant.

### 4.2 Ingested job data

- **`JobPosting`**
  - Raw scraped advert.
  - Includes:
    - Source (`source_site`), job title, raw text, pay fields, location text.
    - `raw_json` blob (API payload).
  - Feeds into `JobRecord`.
  - For Adzuna rows, `min_rate` / `max_rate` and `rate_type` are normalised
    from the API payload and can later be audited/recomputed by
    `backfill_adzuna_hourly_rates.py` if pay logic changes.

- **`JobRecord`**
  - Normalised record used for maps, dashboards, Pay Explorer and Recruiter Radar.
  - Contains:
    - Employer, role, **canonical `job_role_group`**, sector, contract type.
    - Pay values (hourly, annual, etc.), with `pay_rate` being the canonical hourly used in analytics.
    - Location (postcode, county, lat/lon).
    - Metadata: import batch, imported date/year, source, etc.
  - Some JobRecords carry a link back to `JobPosting` via
    `imported_from_posting_id` – this is used by the Adzuna backfill script to
    cascade corrected hourly pay into existing records where safe.

### 4.3 Canonicalisation & mapping

- **`JobRoleMapping`**
  - Maps “raw” job role strings → canonical job role group.
  - Also used as:
    - A cache for AI suggestions.
    - The target of review-export/review-import workflows in role hygiene.

- **`SectorMapping`**
  - Maps raw sectors → canonical sectors.

- **`JobRoleSectorOverride`**
  - Manual overrides: canonical job role → sector.
  - Takes precedence over other sector derivations when present.

### 4.4 Aggregates & insights

- **`JobSummaryDaily`**
  - Daily aggregates of `JobRecord` by date, sector, **canonical `job_role_group`**, geography (county, region) and other dimensions.
  - Used by:
    - Dashboard / Insights.
    - Pay Explorer backend (via `pay_compare.py`).
    - Coverage reporting.
    - Recruiter Radar (trend & forecast for a role+area).

- **`WeeklyMarketChange`**
  - Weekly summary of market changes (counts, medians).

- **`WeeklyInsight`**
  - Narrative insights and commentary for weekly reports.

- **`OnsEarnings`**
  - Imported ONS pay data (measure codes, geography codes, etc.).
  - Used for Pay Explorer comparisons.

### 4.5 Ingestion meta & cron

- **`CronRunLog`**
  - One row per cron execution.
  - Stores:
    - What ran.
    - When.
    - Success/failure, counts, and optional error info.
  - Also used by:
    - `summary_runner.py` to log summary rebuilds.
    - Any manual/cron executions of larger backfills or hygiene jobs (via their own wrappers) if you choose to log them.

### 4.6 Funnel & marketing

- **`WaitlistSignup`**
  - Public waitlist registrations.

- **`AccessRequest`**
  - Requests for access/trials, tracked via admin leads tools.

### 4.7 Other

- **`Company`**
  - Employer/company entity.
  - Linked from `JobRecord` and surfaced on the company detail page.

There may be additional models; this list is focused on core data and behaviour.

---

## 5. Blueprints & responsibilities

All blueprints live in `app/blueprints`.

### 5.1 `auth` – authentication & home

- File: `auth.py`
- Core routes:
  - `/login` (GET/POST) – login form.
  - `/logout` (POST) – logs the user out.
  - `/home` – logged-in home / tiles page.
  - `/init-admin` – bootstrap route to create the first admin user.
- Uses: `User`, Flask-Login.

### 5.2 `public_landing` – public site: landing, waitlist, legal

- File: `public_landing.py`
- Routes:
  - `/` – public landing (marketing index) when logged out.
  - `/waitlist` – waitlist capture.
  - `/access` – access request form.
  - `/legal/privacy`, `/legal/terms` – legal static pages.
- Uses: `WaitlistSignup`, `AccessRequest`.

### 5.3 `marketing` – marketing pages

- File: `marketing.py`
- Routes:
  - `/solutions`, `/data`, `/customer-success`, `/pricing`, `/resources`, `/company`.
- Purely content/marketing; no DB writes.

### 5.4 `main` – legacy/home routing glue

- File: `main.py`
- Routes:
  - `/` and `/home` variants depending on logged-in/out state.
- Coexists with app-level `/` route in `app/__init__.py` and `auth.home`.

> Important: multiple `/` and `/home` routes exist. Behaviour depends on blueprint registration order in `create_app()`. Treat these as **sensitive**: don’t rename or remove without a deliberate change plan.

### 5.5 `records` – table view of JobRecords

- File: `records.py`
- Routes:
  - `/records` – main table view of `JobRecord`.
  - `/edit/<int:record_id>` – edit single record.
  - `/delete/<int:record_id>` – delete record.
  - `/export` – export records (CSV).
  - `/company/<company_id>` – records view for a specific company.
- Uses: `JobRecord`, `Company`.

### 5.6 `company` – company detail page

- File: `company.py`
- Routes:
  - `/company/<company_id>` – richer detail for an employer/company.
- Uses:
  - `JobRecord`, `Company`.
  - Helpers from `app.blueprints.utils` (slugification, logo URLs, etc.).
- Computes stats like counts, typical pay, and role mix.

### 5.7 `upload` – manual CSV upload

- File: `upload.py`
- Routes:
  - `/upload` – form to upload files (e.g. CSV with pay data).
  - `/upload/preview` – preview parsed rows before commit.
  - `/upload/confirm` – commit previewed data into `JobRecord`.
- Uses:
  - Upload parsing logic.
  - Writes to `JobRecord` (and related models) on confirm.

### 5.8 `maps` – map view & Pay Explorer UI/API

- File: `maps.py`
- Routes:
  - `/map` – sector selection screen.
  - `/map/<sector>` – **immersive sector map view** (renders `map.html`):
    - Uses a **2-column layout**:
      - Left: sticky filters sidebar (job role, min/max pay, reset/apply buttons + “current view” pills).
      - Right: large Leaflet map with a **“Map Insights”** panel beside it.
    - Each teal `£` marker on the map represents **one `JobRecord`** (no clustering).
    - The map and insights both respond to:
      - Current filters (job_role, min_pay, max_pay).
      - Current viewport (pan/zoom). Only **visible markers** are included in the insights.
    - Includes a **“Visible jobs”** card in the insights panel:
      - Clicking it opens a modal listing all jobs currently in view (sortable table).
  - `/api/points` – JSON API returning map markers built from `JobRecord`:
    - Accepts query params including `sector`, `bbox`, and filter fields (e.g. `job_role`, `min_pay`, `max_pay`).
    - Returns GeoJSON where each feature corresponds to a single `JobRecord` with properties:
      - `name`, `role`, `rate`, `postcode`, `county`, `imported_month`, `imported_year`, etc.
    - Used exclusively by `map.html` to draw markers and populate the insights and “visible jobs” list.
  - `/pay-explorer` – Pay Explorer UI (renders `pay_explorer.html`).
    - Populates:
      - A sector dropdown from canonical `JobSummaryDaily.sector` values.
      - A job role group dropdown from `JobSummaryDaily.job_role_group`.
  - `/api/pay-compare` – thin JSON/CSV API endpoint for Pay Explorer.
    - Delegates all business logic to `get_pay_explorer_data` in `pay_compare.py`.
    - Supports query params:
      - `sector`, `job_role_group`, `group_by`, `start_date`, `end_date`.
      - `format=json|csv` (CSV export uses the same shape as the “Detail by area” table).

- Uses:
  - `JobRecord` for map markers.
  - `JobSummaryDaily`, `OnsEarnings` indirectly via `pay_compare.get_pay_explorer_data`.
  - Helpers in `app.blueprints.utils` (`logo_url_for`, `company_has_logo`, role grouping).

### 5.9 `pay_compare` – Pay Explorer business logic

- File: `pay_compare.py` (no Blueprint; imported and used by `maps.py` and `api.py`).
- Responsibilities:
  - **Sector normalisation**
    - `SECTOR_ALIASES` maps mixed sector labels to canonical forms.
    - `normalise_sector_name()` ensures UI filters work even with legacy DB values.
  - **ONS index & fuzzy geography mapping**
    - Builds an in-memory index (`ONS_INDEX_YEAR`, `ONS_GEOG_LIST`, `ONS_VALUES`) from `OnsEarnings` for the chosen median measure.
    - Uses RapidFuzz when available (fallback to `difflib`) plus `FUZZY_HINTS` to map counties / area labels → ONS geography names.
    - Helper functions:
      - `_ensure_ons_index()`, `_match_to_ons_geography()`, `_debug_match_to_ons_geography()`.
  - **Core Pay Explorer data**
    - `get_pay_explorer_data(start_date_str, end_date_str, sector, job_role_group, group_by)`:
      - Queries `JobSummaryDaily` for a date range.
      - Supports grouping modes:
        - `"county"` – aggregate by county.
        - `"sector"` – aggregate by sector only.
        - `"sector_county"` – aggregate by sector + county.
      - Filters by:
        - Canonical sector (normalised via `SECTOR_ALIASES`).
        - `job_role_group` (canonical job role group).
      - Computes:
        - `adverts_count`, `median_pay_rate`, `p25_pay_rate`, `p75_pay_rate`,
          `min_pay_rate`, `max_pay_rate`.
        - ONS comparison where geography is present:
          - `ons_median_hourly`, `pay_vs_ons`, `pay_vs_ons_pct`.
      - Returns:
        - `results` (list of rows).
        - `ons_available`, `ons_year`.
        - `params` echo for debugging.
        - `summary` with `row_count` and `total_adverts`.
  - **Debug helpers**
    - `build_pay_explorer_debug_snapshot(days)` to inspect how counties map to ONS geographies for admin diagnostics.

> This is the main place where “compare this job/sector in this area vs the market/ONS” lives. The JS in `pay_explorer.html` relies on the JSON shape returned by `get_pay_explorer_data` via `/api/pay-compare`.

### 5.10 `dashboard` package – dashboards, Insights & AI, quick search, hygiene tools

- Package: `app/blueprints/dashboard/`
  - `__init__.py` – defines the `dashboard` blueprint (`url_prefix` as configured).
  - `core.py`
    - Routes:
      - `/dashboard` – main dashboard (key stats, charts).
  - `insights.py`
    - Routes:
      - `/insights` – interactive insights view over `JobSummaryDaily` (filters, charts, summary cards).
      - `/insights/ai-analyze` – **POST-only endpoint** used by the “Use AI to analyse this view” button in `insights.html`.
        - Expects JSON: `{ filters: {...}, records: [ {sector, county, job_role, company_name, pay_rate, imported_month, imported_year}, ... ] }`.
        - Extracts numeric hourly pay from the `pay_rate` field (already normalised by the frontend).
        - Computes simple stats (count, median, quartiles, range) from the payload.
        - Calls OpenAI to generate a narrative summary of the current filter slice.
        - Returns JSON with `{"html": "<p>...</p>", "text": "plain text version"}`.
  - `quick_search.py`
    - Routes:
      - `/quick-search` – lightweight search over `JobRecord` (role+location+free-text) with shortcuts into Records, Maps, Pay Explorer and Recruiter Radar.
  - `role_admin.py`
    - Routes:
      - `/admin/job-roles` – Job Role Cleaner UI for raw `job_role` → canonical mapping.
      - `/admin/job-roles/map` – create/update a single mapping; optional backfill into `JobRecord.job_role_group`.
      - `/admin/job-roles/bulk-map` – bulk assign canonical role to multiple raw values.
      - `/admin/job-roles/auto-clean` – run rules+fuzzy auto-clean over selected raw roles with confidence threshold; optional backfill.
      - `/admin/job-roles/ai-suggest` – AJAX AI helper for per-row canonical suggestions (uses JobRoleMapping as cache + heuristics, only calls OpenAI as last resort).
      - `/admin/role-sectors` – sector override cleaner for canonical roles currently sitting in “Other”/missing sectors.
      - `/admin/role-sectors/map` – map one canonical role → canonical sector.
      - `/admin/role-sectors/bulk-map` – bulk sector overrides for many roles.
      - `/admin/job-roles/clean-canonical` – one-off (but safe to re-run) canonical label cleaner to strip AI-paragraph-style labels down to proper job-title-style labels using `_clean_canonical_label`.
  - `role_report.py`
    - Routes:
      - `/admin/job-roles/report` – HTML report over current role mappings, hygiene flags and counts.
      - `/admin/job-roles/report.csv` (or similar) – CSV export of the mapping report.
      - `/admin/job-roles/review-export` – exports a **review CSV** containing raw roles, counts, existing canonical roles, suggested roles and hygiene flags for offline review.
      - `/admin/job-roles/review-import` – accepts an updated review CSV; for each row decides whether to:
        - apply the suggested canonical role, or
        - apply a manually-edited canonical role from the CSV,
        and then optionally backfills `JobRecord.job_role_group` where configured.
        This is the “review list export + re-upload” workflow.
  - `helpers.py`
    - Functions such as:
      - `_build_canonical_vocab()` – builds canonical role vocabulary from existing mappings and data.
      - `_suggest_canonical_for_raw()` – rules + fuzzy matching suggestion engine.
      - `_clean_canonical_label()` – canonical label cleaner (used by the route above and other tools).
      - `_role_hygiene_flags()` / `_role_hygiene_score()` – helpers to score how “clean” a mapping is for reporting.

- Uses:
  - `JobRecord`, `JobSummaryDaily`, `JobRoleMapping`, `JobRoleSectorOverride`, `SectorMapping`.
  - Helpers in `app.blueprints.utils` (filter building, caching, canonical role/sector filters).

The **Insights feature** depends heavily on:

- Canonical `job_role_group`.
- Canonical sectors.
- Aggregations in `JobSummaryDaily` that mirror those used in Pay Explorer, but with a more “dashboard-style” UX (multiple charts and cards).

The **AI analyse endpoint** is intentionally decoupled:

- It does **not** hit the DB again; it trusts the compact `records` payload from the browser.
- Rate parsing is defensive and accepts both numeric and string pay values.
- Falls back to a “no numeric hourly rates in this view” message if nothing sane is found.

### 5.11 `insights` – weekly insights

- File: `insights.py` (separate from the dashboard package; this one is for **weekly** insights)
- Routes:
  - `/insights/week/<string:week_start_iso>`
- Uses:
  - `WeeklyInsight`, `WeeklyMarketChange`.
- Renders weekly view of changes + narrative insights.

### 5.12 `api` – additional JSON API

- File: `api.py`
- Routes:
  - `/api/pay-compare` (with blueprint prefix) – alternative Pay Explorer API endpoint.
- Uses:
  - `JobSummaryDaily`, `OnsEarnings`, `pay_compare` helpers.

> Note: There are **two** `/api/pay-compare` endpoints – one in `maps` and one in `api` blueprints. Registration order determines which one handles requests; treat this as sensitive.

### 5.13 `admin` – ingestion, coverage, and diagnostic tools

- File: `admin.py`, blueprint registered with `url_prefix="/admin"`.
- Responsibilities:
  - Scraping & importing:
    - `/admin/jobs/scrape` – run scrapes.
    - `/admin/jobs`, `/admin/jobs/<id>/import`, `/admin/jobs/import-all` – manage `JobPosting` and imports.
  - User admin:
    - `/admin/users` – manage `User` rows.
  - Data hygiene & backfills:
    - `/admin/backfill-counties`, `/admin/regeocode-jobs`,
      `/admin/companies/regenerate-ids`, etc.
    - These routes are for smaller, targeted jobs. Larger or more experimental
      runs (like `backfill_adzuna_hourly_rates.py` and `summary_runner.py`)
      are typically run via CLI / cron rather than interactive admin buttons.
  - ONS & Pay Explorer diagnostics:
    - `/admin/ons/import`, `/admin/inspect/ons`.
    - `/admin/debug/pay-explorer-json`, `/admin/debug/pay-explorer-mapping`.
  - Companies/admin views:
    - `/admin/companies`.
  - Summaries & cron:
    - `/admin/admin/rebuild-summaries`, `/admin/weekly`,
      `/admin/weekly-market-changes`.
    - `/admin/cron-runs`, `/admin/cron-runs/run-now`.
    - `/admin/cron/job-role-canonicaliser/run-now`.
    - `/admin/status.json`.
  - Coverage:
    - `/admin/coverage`, `/admin/coverage/export`, `/admin/coverage/heatmap`.
  - Tools:
    - `/admin/tools`, `/admin/db-health`, `ai_logs` pages.

### 5.14 `coverage` – coverage computations (helpers, not a blueprint)

- File: `coverage.py`
- Contains:
  - Helper functions for computing coverage statistics across:
    - Sectors.
    - Locations.
    - Source sites.
  - Used by admin coverage routes and cron.

### 5.15 `utils` – cross-blueprint helpers

- File: `utils.py`
- Examples:
  - Logo helpers (`logo_url_for`, `company_has_logo`).
  - Slugification and cleaning helpers.
  - Filter builders for dashboards/maps.
  - Small caching utilities.
  - Cached filter options for sectors / job_role_group used by dashboard and insights.
  - UK geocoding helpers:
    - `normalize_uk_postcode()`
    - `geocode_postcode()` / `geocode_postcode_cached()`
    - `lookup_nearest_postcode()` / `snap_to_nearest_postcode()`
  - These are used by:
    - Import/backfill scripts.
    - Map features.
    - Recruiter Radar radius search.

### 5.16 `recruiter_radar` – Recruiter Radar (role+area snapshot)

- File: `recruiter_radar.py`
- Routes:
  - `/recruiter/radar` (GET)
    - Renders `recruiter_radar.html`.
    - UI:
      - Free-text **Role** input with auto-complete/autosuggest based on canonical roles in the DB (job_role_group / canonical roles).
      - **Location** free-text input (typically town, postcode district or full postcode).
      - **Radius** selectors (5, 15, 25 miles).
      - **Lookback window** dropdown (e.g. 30, 60, 90, 180 days – configurable).
      - Role + location + radius + lookback together define the **Radar slice**.
    - On submit, JS calls the API endpoint below.
  - `/api/recruiter/radar` (GET/JSON)
    - Query params:
      - `role` – canonical role label (or raw job_role; backend normalises).
      - `location` – free-typed location/postcode.
      - `radius_miles` – numeric radius (5/15/25).
      - `lookback_days` – how many days back to consider.
    - Behaviour:
      - Uses `geocode_postcode()` + `normalize_uk_postcode()` from `utils.py` to resolve `location` into a UK postcode and lat/lon.
      - Builds a radius filter over `JobRecord` using the geocoded point and `radius_miles`.
      - Filters to the selected role (via canonical job_role_group where available) and lookback window.
      - Uses:
        - `JobRecord` for **most recent adverts**, recent employers, raw demand and competition metrics.
        - `JobSummaryDaily` for **trend and forecast**:
          - Pulls last N months of daily medians for the role+area.
          - Fits a simple trend line (e.g. linear) for median hourly pay.
          - Projects a forecast a few months ahead.
    - Returns JSON:
      - `input_summary` – echo of role, location, radius, lookback, and any canonicalisation applied.
      - `headline` – current and forecasted typical pay:
        - `median_now`, `median_3m_ahead`, `median_6m_ahead` (depending on forecast horizon).
      - `recommendation` – suggested hourly pay to offer:
        - Contains `recommended_rate` and a band (e.g. `recommended_low`, `recommended_high`),
          derived from current medians + recent trend and competition.
      - `demand` – demand metrics in the radius:
        - `total_adverts`, `adverts_last_30_days`, simple trend flag (rising/flat/falling).
      - `competition` – competition metrics:
        - `distinct_employers`, `top_employers` list with counts.
      - `recent_roles` – list of the most recent adverts matching the role+area.
      - `forecast_series` – time series used to plot the trend/forecast (dates, median pay).
      - `ai_commentary` – optional pre-rendered HTML/text if the API chooses to call AI, or `null` if deferring to frontend AI.

- AI behaviour:
  - Either the API calls OpenAI directly per request, or (preferred) returns a compact `context` object which the frontend sends to a dedicated AI endpoint if/when the user asks for commentary.
  - The AI commentary summarises:
    - Current level vs recent history.
    - Whether the market looks tight or loose.
    - How aggressive the recommended rate is vs local typical pay.
    - Any obvious changes in demand/competition.

- Uses:
  - `JobRecord`, `JobSummaryDaily`.
  - Geocoding helpers from `utils.py`.
  - Shared canonicalisation logic for roles and sectors from dashboard helpers.

---

## 6. Data flow: end-to-end

### 6.1 Scrape → JobPosting

1. `cron_runner.py` (or admin `/admin/jobs/scrape`) calls scrapers:
   - `app/scrapers/adzuna.py` (Adzuna API).
   - Others as configured (e.g. Reed).
2. Scrapers:
   - Call external APIs (Adzuna, etc.) using API keys from env (e.g. `ADZUNA_APP_ID`, `ADZUNA_APP_KEY`).
   - Normalise payloads and insert/update rows in `JobPosting`.
   - For Adzuna:
     - Pay fields are parsed into `min_rate`, `max_rate` and `rate_type`.
     - Initial hourly equivalents are computed in the scraper/importer using a standard hours/week & weeks/year assumption.

3. When pay-normalisation rules are tuned or Adzuna behaviour changes in the wild, you can run **`backfill_adzuna_hourly_rates.py`** over the existing JobPostings to:
   - Recompute hourly equivalents from the stored annual salaries.
   - Detect and correct clearly mis-scaled hourly values (10× errors) while leaving plausible rates alone.
   - Restrict changes to suspicious rows only (above a threshold) to avoid churning the whole dataset unnecessarily.

### 6.2 JobPosting → JobRecord

1. `app/importers/job_importer.py` contains the core mapping logic.
2. Entry points:
   - Cron (`cron_runner.py`).
   - Admin tools (`/admin/jobs/import-all`, `/admin/jobs/<id>/import`).
3. During import:
   - Normalises role titles (and may update `JobRoleMapping`).
   - Derives sector (with `SectorMapping` and overrides).
   - Cleans pay fields:
     - Uses hourly rates derived from `JobPosting.min_rate` / `max_rate` + `rate_type`.
     - If `backfill_adzuna_hourly_rates.py` has been run, the importer benefits from the corrected hourly equivalents for Adzuna postings.
   - Resolves or creates `Company` as needed.
   - Writes final records into `JobRecord`:
     - `pay_rate` stores the canonical hourly figure used by Maps, Insights, Pay Explorer and Recruiter Radar.
     - `imported_from_posting_id` may store the originating `JobPosting.id`.

4. When the Adzuna backfill script is run with `update_job_records=True` (or equivalent flag), it:
   - Locates JobRecords whose `imported_from_posting_id` matches a corrected JobPosting.
   - Updates `pay_rate` (and any derived hourly fields) to the corrected values.
   - Leaves records from other sources untouched.

### 6.3 JobRecord → JobSummaryDaily

1. Summaries are built via:
   - `summary_runner.py` (CLI/cron).
   - Admin routes (`/admin/admin/rebuild-summaries`).
2. Aggregation:
   - Groups `JobRecord` by date, sector, **job_role_group**, geography, etc.
   - Stores counts, medians, and other summary stats in `JobSummaryDaily`.

3. **After any large pay backfill affecting historical JobRecords** (e.g. Adzuna hourly corrections), you should:
   - Run `summary_runner.py` for an appropriate window (e.g. last 30–90 days) so that:
     - Map, Insights, Pay Explorer and Recruiter Radar views based on `JobSummaryDaily` align with the corrected hourly rates.
   - This is necessary because `JobSummaryDaily` stores **snapshots** of pay stats; it does not automatically recalc when individual JobRecords change.

### 6.4 Summaries → UI & analytics

- **Dashboard & Insights**
  - `/dashboard`, `/insights`, `/insights/week/...` read from:
    - `JobSummaryDaily`, `WeeklyMarketChange`, `WeeklyInsight`, plus role/sector mappings.
- **Maps**
  - `/map`, `/api/points` read from:
    - `JobRecord` directly.
- **Pay Explorer**
  - `/pay-explorer`, `/api/pay-compare` use:
    - `JobSummaryDaily` + `OnsEarnings`.
    - Logic in `pay_compare.py`.
- **Recruiter Radar**
  - `/recruiter/radar`, `/api/recruiter/radar` use:
    - `JobRecord` for most recent adverts/employers.
    - `JobSummaryDaily` for trend + forecast of typical pay in the radius.
- **AI Insights**
  - `insights.html` builds a **compact JSON payload** from the current view:
    - Filters (`sector`, `county`, `job_role`, `year`, pay band constraints).
    - A sampled list of records with numeric `pay_rate`.
  - Posts to `/insights/ai-analyze`.
  - Renders the returned HTML/text into the “Narrative insights” panel.
- **Role hygiene review**
  - `role_report.py` exports:
    - A CSV view of raw roles, canonical roles, suggestions, counts, and hygiene flags.
  - Superusers can edit this offline and re-import via `/admin/job-roles/review-import`,
    which updates `JobRoleMapping` and optionally backfills `JobRecord.job_role_group`.

---

## 7. Configuration & environment

### 7.1 Core config (`config.py`)

- `SECRET_KEY`
- `SQLALCHEMY_DATABASE_URI`
  - Built from `DATABASE_URL`, with special handling:
    - Converts `postgres://` → `postgresql+pg8000://` to use `pg8000`.
  - Falls back to `sqlite:///app.db` if `DATABASE_URL` is missing (local only).
- `SQLALCHEMY_TRACK_MODIFICATIONS = False`
- Other flags may be defined here; see file for details.

### 7.2 Important environment variables (non-exhaustive)

- **DB & Flask**
  - `DATABASE_URL`, `FLASK_ENV`, `FLASK_DEBUG`.
- **Scrapers**
  - `ADZUNA_APP_ID`, `ADZUNA_APP_KEY`, `ADZUNA_DEBUG`, `ADZUNA_SLEEP_SEC`.
- **Cron / coverage (from `cron_runner.py`)**
  - `COVERAGE_BOOST_ENABLED`
  - `COVERAGE_BOOST_SOURCE`
  - `COVERAGE_BOOST_EXTRA_ROLES`
  - `COVERAGE_BOOST_WINDOW_DAYS`
  - `COVERAGE_BOOST_MIN_DAYS_SEEN`
  - `CRON_MAX_TOTAL_ROLES`
  - `COVERAGE_BOOST_WHERE`
- **Summaries**
  - `SUMMARY_DAYS_BACK` – number of days for `summary_runner.py` to rebuild, counting backwards from today (default 30).
  - `SUMMARY_JOB_NAME` – optional job name string for `CronRunLog.job_name` when `summary_runner.py` runs.
- **AI Insights & Recruiter Radar**
  - OpenAI key and model config (e.g. `OPENAI_API_KEY`, `OPENAI_MODEL`), used by:
    - `/insights/ai-analyze`
    - Recruiter Radar commentary (if server-side AI is enabled).
- Other feature flags and tuning knobs are defined inline in their respective modules.

---

## 8. Frontend structure

### 8.1 Base layout

- **`app/templates/base.html`**
  - Shared layout: nav, footer, flash messages, scripts.
  - Handles:
    - Logged-out marketing nav vs logged-in app nav.
    - Dark-theme styles (as per PayScope design).
  - App nav includes:
    - Home, Upload, Map View, Pay Explorer, Records, Recruiter Radar, Admin Tools (for superusers).
  - On immersive map pages (e.g. `map.html`), the app nav supports an **auto-hide** behaviour:
    - The header collapses out of view to free vertical space for the map.
    - Moving the pointer towards the top edge of the viewport reveals the nav again.
    - This is wired via small JS/CSS hooks in `base.html` and is opt-in per page.

  - Exposes a global `window.fetchWithCsrf` helper used by:
    - AI Insights (`/insights/ai-analyze`).
    - Role hygiene AI buttons.
    - Recruiter Radar (if using POST/JSON).
    - Any other POST/JSON ajax calls that need CSRF tokens.

### 8.2 Key templates

- **App / workspace**
  - `home.html` – logged-in tiles/landing.
  - `dashboard.html` – dashboard charts over `JobSummaryDaily`.
  - `map.html`, `map_select.html` – sector map view:
    - `map_select.html` – list of canonical sectors with entry points into the map.
    - `map.html` – **full-width, two-column layout**:
      - Left: filters card (job role, min/max pay, “current view” pills).
      - Right: Leaflet map plus a “Map Insights” card.
      - The map uses **one teal `£` marker per `JobRecord`** (no clustering).
      - “Map Insights” computes live stats from the **currently visible markers**:
        - Count, average, median, min/max, simple banded distribution.
        - Estimated Real Living Wage (RLW) compliance (UK vs London thresholds).
      - A “Visible jobs” card opens a modal listing the jobs currently in view.
  - `pay_explorer.html` – Pay Explorer UI:
    - Filters:
      - Sector dropdown (canonical sectors).
      - Job role group dropdown (canonical job_role_groups, filtered client-side by sector in the UI).
      - Group-by selector (`county`, `sector`, `sector_county`).
      - Date range controls.
    - Visuals:
      - Chart.js bar+line chart comparing advertised median vs ONS median.
      - Summary card with averaged advertised pay, ONS pay, and gap.
      - “Detail by area” table with:
        - ONS comparison columns.
        - Sort controls (gap, advertised, adverts, A→Z).
        - Scrollable body within a fixed-height card so the page itself doesn’t scroll excessively.
  - `insights.html` – interactive Insights UI:
    - Filter strip for sector / county / role / year / pay band.
    - Snapshot cards:
      - Records in view.
      - Key takeaways (median, P25–P75, range, most common slice, confidence note).
    - Multiple Chart.js charts driven by `JobSummaryDaily`-backed stats.
    - “Use AI to analyse this view” button:
      - Opens a confirmation modal.
      - Compacts the current dataset (`records`) into `{sector, county, job_role, company_name, pay_rate, imported_month, imported_year}`.
      - POSTs to `/insights/ai-analyze` and renders the HTML/text into the AI card.
  - `recruiter_radar.html` – Recruiter Radar UI:
    - Inputs:
      - Role auto-complete (searches canonical roles as the user types).
      - Location box with helper text (postcode or town/city).
      - Radius buttons (5 / 15 / 25 miles).
      - Lookback window selector.
    - Outputs (single-page snapshot):
      - Headline cards:
        - Typical pay now (median).
        - Forecasted pay (e.g. 3 months ahead).
        - Recommended pay to offer (single value + band).
      - Demand & competition:
        - Recent adverts count, trend over last N days.
        - Distinct employers and top employer list.
      - Recent roles:
        - Table/list of most recent adverts for this role+area.
      - Trend chart:
        - Simple chart showing median pay over time plus forecast line.
      - AI commentary:
        - Card with narrative summary of the above (optional, via AI call).
  - `records.html`, `edit_record.html` – records table and editor.
  - `upload.html`, `upload_preview.html` – upload flow.

- **Admin**
  - `admin/jobs*.html` – scraping/import UI.
  - `admin/coverage*.html` – coverage dashboard + heatmap.
  - `admin/weekly*.html` – weekly changes & insights.
  - `admin/cron_runs.html`, `admin/admin_tools.html`, `admin/db_health.html`, etc.
  - `admin_job_roles.html` – Job Role Cleaner:
    - Shows raw `job_role` values with counts, mapping inputs, suggestions, AI buttons.
    - Includes:
      - Bulk map and auto-clean forms (multi-select + threshold).
      - A button to open the mapping report and review-export.
    - Uses JS helpers:
      - “Select all” and “collect selected” logic for bulk forms.
      - Per-row “Use suggestion” and “Ask AI” actions.
  - `admin_job_roles_report.html` – role mapping report view, with CSV/Review export links.

- **Marketing & legal**
  - `index.html` – marketing landing (plus reused as logged-in landing).
  - `solutions.html`, `pricing.html`, `data.html`, `resources.html`,
    `customer_success.html`, `company.html`.
  - `legal/privacy.html`, `legal/terms.html`.

### 8.3 JavaScript

- Charting and UI helpers live under `app/static/` (for example, charts JS for dashboards/insights).
- Pay Explorer:
  - Uses inline JS in `pay_explorer.html` plus Chart.js (CDN) for the main comparison chart.
  - Calls `/api/pay-compare` with AJAX, then:
    - Renders the chart.
    - Sorts and renders the “Detail by area” table.
    - Updates summary metrics and ONS badge.
- Insights:
  - Uses `static/insights_charts.js` plus inline JS in `insights.html`.
  - Builds charts from `stats` (pre-aggregated on the server) and `records`.
  - Provides client-side calculation of medians/IQR, with a fallback that reconstructs these from histogram-style `stats.distribution` when raw records are thin.
  - Calls `/insights/ai-analyze` for AI narratives.
- Map view uses Leaflet (and associated JS/CSS) for marker layers:
  - `map.html` initialises a Leaflet map, calls `/api/points` with the current `bbox` + filters, and draws one divIcon `£` marker per job.
  - The same JS layer:
    - Derives a **“visible providers”** array from markers currently inside the viewport.
    - Computes stats (count, mean, median, min/max, simple distribution, RLW compliance) and renders them into the “Map Insights” card.
    - Drives the “Visible jobs” modal table.
  - All analysis is done client-side from the markers payload; `/api/points` remains a simple GeoJSON provider.
- Recruiter Radar:
  - Inline JS in `recruiter_radar.html`:
    - Handles role auto-complete by hitting a small suggestion endpoint or reusing cached canonical role list from the dashboard helpers.
    - Debounces calls to `/api/recruiter/radar`.
    - Populates cards and charts from the JSON response.
    - Optionally triggers an AI commentary request and renders the returned HTML/text.
  - Uses the same `window.fetchWithCsrf` helper for any POST/JSON calls.

---

## 9. Development workflows (summary)

### 9.1 Local app run

- Ensure `.env` has at least:
  - `DATABASE_URL` (or accept SQLite fallback).
  - Any required scraper keys if you plan to scrape.
  - `OPENAI_API_KEY` if you want AI Insights and Recruiter Radar commentary to work.
- Typical commands:
  - `python run.py`
  - or `flask run` if configured appropriately.

### 9.2 Database migrations

- Use Flask-Migrate / Alembic via helper script:
  - `python run_migrations.py` or direct `flask db upgrade` depending on setup.
- New models/columns require:
  - `flask db migrate -m "message"`
  - `flask db upgrade`

### 9.3 Cron / ingestion

- For production deployment:
  - Point the platform scheduler at `cron_runner.py` with appropriate env.
- For manual runs / debugging:
  - `python cron_runner.py` (parameters/config as defined in the file).

### 9.4 Adzuna hourly backfill & summary rebuild

Operational pattern when you need to correct Adzuna hourly rates and keep analytics consistent:

1. **Inspect what would change (dry run)**  
   In a Flask shell or standalone script:

   ```python
   from scripts.backfill_adzuna_hourly_rates import run_backfill

   run_backfill(
       dry_run=True,
       only_if_suspicious=True,
       suspicious_over_hourly=30.0,
       commit_every=200,
       id_min=None,
       id_max=None,
   )

   Confirms:

How many Adzuna postings are in scope.

How many would be updated.

A sample of postings with current vs recomputed hourly, plus job titles and companies.

Apply corrections in batches
Once you’re happy:

run_backfill(
    dry_run=False,
    only_if_suspicious=True,
    suspicious_over_hourly=30.0,
    commit_every=200,
    id_min=1,
    id_max=5000,
)


Repeat for subsequent ID ranges as needed (e.g. 5001–10000, etc.).

commit_every keeps transactions small and avoids long-lived cursors.

The script:

Fixes obviously mis-scaled hourly values.

Leaves plausible hourly rates alone.

Optionally cascades corrections into JobRecord rows via imported_from_posting_id.

Rebuild JobSummaryDaily for the affected window

After the backfill (and any re-imports if you choose to re-run importers for older postings):

Run summary_runner.py to rebuild daily summaries:

SUMMARY_DAYS_BACK=60 SUMMARY_JOB_NAME=job_summary_daily_rebuild python summary_runner.py


Or trigger the equivalent admin route (/admin/admin/rebuild-summaries) if available.

This keeps:

Pay Explorer.

Insights.

Recruiter Radar trend/forecast.
aligned with the corrected hourly pay from Adzuna.

Check CronRunLog / admin screens

Verify CronRunLog entries for:

The backfill run(s) (if you wire them to CronRunLog).

The summary rebuild (SUMMARY_JOB_NAME).

Spot-check:

Pay Explorer outputs for Adzuna-heavy roles/areas.

Recruiter Radar slices where you previously saw absurd hourly rates.

10. Invariants & “handle with care” zones

These are architectural hotspots where behaviour is coupled across modules:

Route names & paths

Multiple definitions exist for / and /home.

/api/pay-compare is implemented in both maps and api blueprints.

admin routes are used throughout the UI; names and paths should be treated as stable.

/insights/ai-analyze is called directly from insights.html JavaScript.

/recruiter/radar and /api/recruiter/radar are called from the Recruiter Radar UI.

Changing paths, methods or JSON shapes for these endpoints requires a coordinated frontend update.

Canonicalisation

JobRoleMapping, SectorMapping, and JobRoleSectorOverride drive:

Dashboard filters.

Coverage.

Pay Explorer.

Recruiter Radar (role canonicalisation).

Admin mapping tools.

Changes here have cross-cutting effects.

Pay Explorer

pay_compare.py + maps.py + api.py + pay_explorer.html.

Input/output formats from the API are consumed directly by JS charts and table UI.

Do not change the JSON shape from get_pay_explorer_data without updating the UI in lockstep.

Coverage

coverage.py helpers are used by:

cron_runner.py for coverage stats.

Admin coverage and heatmap routes.

Insights

/dashboard and /insights both depend on:

Canonical job_role_group and sectors.

Correct aggregation in JobSummaryDaily.

Changes to the way summaries are computed or filtered will affect both dashboards and Pay Explorer, so be deliberate and keep behaviour consistent.

AI Insights relies on:

Frontend numeric pay_rate extraction.

Backend trusting that payload rather than re-querying the DB.

Role hygiene review

The review CSV export/import routes assume a specific column layout and semantics.

Changing column names/order in role_report.py must be mirrored in:

admin_job_roles.html export links.

Any documentation/training material for users editing the CSV offline.

Adzuna hourly backfill

backfill_adzuna_hourly_rates.py assumes:

Stable JobPosting IDs.

Consistent rate_type semantics (annual vs hourly).

Known hours-per-week/weeks-per-year conversion values.

Running it repeatedly with the same parameters is intended to be idempotent:

Suspicious postings converge to the recomputed hourly values.

Non-suspicious postings are left untouched.

Always treat backfill scripts as “handle with care”:

Dry-run first.

Run in batches.

Rebuild summaries where appropriate.
