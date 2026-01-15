# PayScope Memory Spec (Product Rules & Behaviours)
_Version 2.0 – 14 January 2026_

This document describes **what PayScope should do** and how features are expected to behave from a product point of view.

It is **not** an architecture guide – see `architecture.md` for routes, blueprints and technical wiring. Treat this file as the product “source of truth” for all future work.

---

## 1. Product positioning & core idea

**What PayScope is**

- A **UK-focused pay intelligence platform** for HR, TA and reward people.
- Built on scraped job adverts and client data, cleaned and normalised into a canonical model.
- Surfaces:
  - Pay maps by area and sector.
  - A Pay Explorer view that compares advertised pay vs ONS earnings.
  - Dashboards/Insights for monitoring pay levels, spread and change over time.
  - Admin tools for ingestion, role/sector hygiene, coverage and diagnostics.
  - AI helpers that explain patterns in plain language (AI Insights).

**Primary user personas**

1. **HR / Reward lead**
   - Wants to benchmark pay, spot under/over-payment and justify changes.
2. **Talent Acquisition / Resourcing lead**
   - Wants to understand market pressure, hotspots, and where to advertise.
3. **Internal “data steward” / superuser**
   - Maintains mappings, monitors coverage and ingestion, runs backfills.

The product should be opinionated but explainable: higher-level analytics and AI where useful, always backed by transparent charts, counts and tables.

---

## 2. Core concepts & terminology

These terms should be used consistently in the UI and docs.

- **JobPosting** – raw scraped advert (API payload).
- **JobRecord** – cleaned, canonicalised record (one row per advert).
- **JobSummaryDaily** – aggregated metrics per day/sector/role/geography.
- **Canonical job role group** (`job_role_group`) – normalised role name for analysis.
- **Canonical sector** – normalised sector bucket (e.g. “Social Care”, “Admin & Office”).
- **Market slice** – the subset of the dataset defined by current filters (sector, county, role, year, pay band).
- **Coverage** – how well the dataset represents the market by sector and geography over time.
- **Outlier** – a pay value that is unusually high or low relative to the bulk of the distribution.
- **Typical pay** – the **median hourly** pay in the current market slice (not the average).
- **Pay spread** – the interquartile range (P25 → P75) of hourly pay.
- **Range** – minimum → maximum hourly pay in the current slice.
- **Records in view** – number of **JobSummaryDaily-backed pay points** contributing to the current Insights view (or raw JobRecords where relevant).

---

## 3. Data rules & normalisation

### 3.1 Pay values

- PayScope aims to work in **hourly equivalent** where possible.
- When importing, the importer:
  - Converts annual salaries to an hourly equivalent using configurable assumptions (e.g. 37.5–40 hours/week).
  - Normalises mixed formats (£14–£16 per hour, “£30k–£35k per annum”, etc.) into numeric fields on `JobRecord`.
- Whenever pay is displayed in analytics:
  - **Prefer the canonical hourly field** (e.g. `canonical_hourly_rate` or equivalent).
  - Ignore non-sensical values (≤0, missing, or clearly broken).
  - Ignore values clearly marked as annual-only where conversion was not possible.

### 3.2 Outliers

Outliers are **not silently removed** from the dataset; they are:

- Included in the **min/max range** and any explicit “outlier” views.
- De-emphasised in “typical” metrics:
  - Medians and P25–P75 spread are used for all “typical pay” messaging.
  - UI copy encourages users to focus on P25–P75 and medians for decision-making.
- Highlighted in:
  - “Range” cards (“check outliers if very wide”).
  - Outlier-specific charts in Insights (e.g. “Pay outliers over time”).

Implementation detail: the exact definition of “outlier” (e.g. > P75 + k·IQR, < P25 − k·IQR) is a **tuning parameter** in code, not fixed here. The product rule is: call them out clearly and keep “typical pay” robust.

### 3.3 Canonical roles & sectors

- Every `JobRecord` should try to map to:
  - A canonical **job_role_group**.
  - A canonical **sector**.
- When mapping is missing:
  - Records still count towards totals and pay stats.
  - “Role hygiene” messaging shows the number of records without a canonical group.
- Role mappings and sector mappings are:
  - Editable via admin tools (job roles and sector override screens).
  - Used consistently by:
    - Pay Explorer filters.
    - Dashboard & Insights filters.
    - Coverage computations.

---

## 4. Main user-facing surfaces

### 4.1 Workspace home (`/home`)

Purpose: tile-based “launchpad” for main tools.

- Shows tiles for:
  - **Upload data**
  - **Map View**
  - **Pay Explorer**
  - **Insights & Analytics**
  - **Records**
  - **Admin / Tools** (for superusers)
- Tiles should:
  - Briefly explain what each section does in user language.
  - Make it clear that analytics are built on imported/scraped data.

### 4.2 Records view (`/records`)

Purpose: low-level table of individual job adverts (`JobRecord`).

Behaviour:

- Default view shows recent records with pagination.
- Filters:
  - Company, sector, role group, county, source site, date range.
- Users can:
  - Search, sort, and export results (CSV).
  - Edit a record in detail (e.g. fix pay, sector, role mapping).
- Edits:
  - Should not silently break canonicalisation – if a user changes a role name, mappings need to be respected or re-applied.
- This view is **not optimised for analytics** – it’s the “spreadsheet behind the charts”.

### 4.3 Map View (`/map`)

Purpose: make **geography + pay** instantly understandable, with the **map as the main feature of the page**.

#### Layout & interaction

- User first chooses a **sector** from a canonical list (sector selection screen).
- The sector map view (`/map/<sector>`) is a **full-width, two-column layout**:
  - **Left column – Filters sidebar**
    - Job role (canonical job role group within the chosen sector).
    - Min / Max hourly pay.
    - “Apply” and “Reset” buttons.
    - “Current view” pills summarising the active filters.
    - Copy explaining that:
      - Filters change the underlying dataset.
      - Map + Insights update when you pan/zoom.
  - **Right column – Map + Insights**
    - Large Leaflet map (no vertical scrolling required to see it).
    - “Map Insights” panel to the right of the map on wide screens (below it on smaller screens).

#### Data behaviour

- Each teal `£` marker on the map is a **single `JobRecord`**:
  - No clustering by default; density is represented by many overlapping/nearby markers.
  - Each marker tooltip shows:
    - Job role (canonical label if available).
    - Employer.
    - Hourly rate (or “—” if missing).
    - County and postcode.
    - Imported month/year (approximate “age” of the record).
- The map calls `/api/points` with:
  - Current sector.
  - Current filters (job role, min/max pay, etc.).
  - Current map **bounding box** (`bbox`) so only relevant records are returned.

#### Map Insights panel

The “Map Insights” panel always reflects **only the markers currently visible on screen** (after filters + pan/zoom):

- Shows:
  - **Visible jobs** – count of markers within the viewport.
  - **Average hourly rate** and **median hourly rate** for visible jobs.
  - **Range** – min → max hourly rate.
  - **Estimated Real Living Wage (RLW) compliance**:
    - Uses UK vs London RLW thresholds.
    - Expressed as a % of visible jobs meeting or exceeding the relevant RLW.
  - Top counties by count (among visible jobs).
  - A simple banded **rate distribution** (e.g. `<£11`, `£11–£12`, …, `≥£14`).
- The “Visible jobs” card is clickable:
  - Opens a modal listing the jobs currently in view (sortable by role, rate, county, etc.).
  - Makes it easy to move from the map to a **concrete list** of adverts.

All of these stats are computed **client-side** from the markers payload; the backend only needs to provide clean JobRecords to the map API.

#### UX rules

- The map should feel **central and immersive**:
  - No need to scroll down to see the whole map on typical laptop screens.
  - The app header/nav can auto-hide on map pages to free vertical space, reappearing when the user moves the cursor to the top of the screen.
- Copy should reinforce:
  - “Each teal £ marker is one job record.”
  - “Map + Insights update when you pan/zoom.”
  - “Use ‘Visible jobs’ for a quick list of what’s behind the map.”
- Sample-size sensitivity:
  - If no markers are visible, the Insights panel should clearly say so and nudge the user to pan/zoom or relax filters.
  - RLW and other metrics must show dashes / “n/a” rather than misleading numbers when there is no numeric pay.


### 4.4 Pay Explorer (`/pay-explorer`)

Purpose: “Compare my chosen market slice vs ONS earnings.”

Inputs:

- **Sector** – canonical sector (normalised).
- **Job role group** – canonical role group.
- **Date range** – typically last 12 months by default.
- **Group by** – one of:
  - `county`
  - `sector`
  - `sector_county` (sector × county matrix)

Outputs:

1. **Headline summary**
   - Advertised median hourly pay (for the chosen slice).
   - ONS median hourly pay (where available) for the same or nearest geography.
   - Gap in £ and %.
   - A clear label: “Advertised vs ONS” with direction (above / below).

2. **Chart**
   - Bar/line chart showing:
     - Advertised medians by area.
     - ONS medians alongside where available.
   - Hover tooltips must show:
     - Area name.
     - Advertised median (+ P25 / P75 if available).
     - ONS median.
     - Gap in £ and %.

3. **Detail table**
   - One row per group (e.g. per county).
   - Columns:
     - Area (county / sector / sector+county).
     - Number of adverts.
     - Advertised median, P25, P75, min, max.
     - ONS median (if mapped).
     - Gap (£) and %.
   - Sort options:
     - By gap (descending/ascending).
     - By advertised median.
     - By number of adverts.
     - A→Z by area.
   - Table is scrollable within its card; the page itself should not become excessively tall.

Interpretation rules:

- ONS is a **benchmark**, not “truth”; UI should avoid implying legal or statutory rates.
- If ONS is **missing** for an area:
  - Show “n/a” and a tooltip explaining that no ONS match is available.
- If sample size for an area is very small:
  - Use an icon or text hint (“low volume – treat cautiously”).

### 4.5 Insights & Analytics (`/insights`)

Purpose: multi-chart analytics, plus a “Key takeaways” and AI narrative for the current market slice.

#### 4.5.1 Filters

Filters define the **market slice**:

- Sector (multi-select).
- County (multi-select).
- Job role (multi-select) – using canonical groups.
- Year (single select).
- Pay band constraint:
  - Min pay.
  - Max pay.

Behaviour:

- Changing filters updates:
  - Records in view count.
  - Key takeaways card.
  - All charts.
  - AI narrative (only when user actively clicks the AI button).

#### 4.5.2 “Records in view” card

- Shows:
  - `Records in view` (total count after filters).
  - Copy: “Use volume as a sanity check — tiny samples can mislead.”
- If role hygiene data is available:
  - Show a pill: “Role hygiene: X record(s) have no canonical role group.”
- This card is deliberately simple – it’s a volume sanity-check.

#### 4.5.3 “Key takeaways” card

Data sources:

- Prefer **real numeric hourly pay values** from the records in the current slice.
- If there are no usable numeric values:
  - Fall back to histogram / distribution stats (`stats.distribution`).
- If there is still no numeric signal:
  - Show a “no numeric pay rates found” message.

Displayed metrics:

1. **Typical pay (median)**
   - Main number: median hourly pay (fallback to average if necessary).
   - Sub-note: “Average: £X.XX • Based on N pay rates”.

2. **Spread (P25 → P75)**
   - Main number: “£P25 → £P75”.
   - Sub-note: “Middle 50% spread: £W (less spread = more consistent pay).”

3. **Range (min → max)**
   - Main number: “£min → £max”.
   - Sub-note: “Range width: £(max − min) (check outliers if very wide).”

4. **Most common slice**
   - Text that favours:
     - Sector with highest count in the slice (if clear).
     - Role with highest count in the slice.
   - Example: “Sector: Admin & Office • Role: Administration Officer”.
   - Sub-note: “Most common role appears in X record(s).”

5. **Interpretation tip**
   - Confidence message based on sample size:
     - **N < 50** – “Small sample … treat conclusions as directional.”
     - **50 ≤ N < 250** – “Moderate sample … good for benchmarking; still watch thin counties/sectors.”
     - **N ≥ 250** – “Strong sample … focus on spread and trends.”
   - If a pay band (min/max) is applied:
     - Add a sentence: “You’ve constrained the pay band — that can narrow the spread and shift the ‘typical’ rate.”

If **no numeric pay** is available:

- The card should show dashes and the interpretation tip:

> “No numeric pay rates found for this filter set.”

and encourage broadening filters.

#### 4.5.4 Charts in Insights

Key charts (non-exhaustive, but current set):

- Average pay by sector.
- Average pay by role.
- Records by county.
- Records by sector.
- Pay distribution (bands / histogram).
- Pay range by sector (min/avg/max).
- Average pay over time (trend).
- Volatility by sector (standard deviation).
- Sector × county pay heatmap.
- Top 10 companies by average pay.
- Pay outliers over time (scatter).
- Role mix by sector.
- Top counties – pay trend.
- Role × sector pay matrix.

Product rules:

- Every chart must:
  - Be clearly titled in user language.
  - Have helper text (info icon → modal) that explains how to read it.
  - Support PNG download.
- Charts are driven from:
  - `stats` pre-computed on the server.
  - `records` where finer-grained data is needed (e.g. outliers).
- Charts should visually emphasise **relative patterns** more than absolute numbers.

#### 4.5.5 AI Insights (“Use AI to analyse this view”)

Purpose: generate a human-readable narrative for the current Insights filter set.

Trigger:

- Button: **“Use AI to analyse this view”** opens a confirmation modal.
- After confirmation:
  - Client builds a compact payload from `records`:
    - Sector, county, job_role, company_name.
    - Numeric hourly `pay_rate` (already parsed on the frontend).
    - Imported month/year.
  - Sends `{filters, records}` via POST to `/insights/ai-analyze`.

Backend behaviour:

- Validate payload; if no usable `pay_rate` values:
  - Return text equivalent to:
    - “I couldn’t see any numeric hourly rates in this view, so there’s nothing to summarise yet. Try widening the filters or removing any tight pay band constraints.”
- Otherwise:
  - Compute basic stats (count, medians, quartiles, trends, sector/role mix).
  - Call OpenAI with:
    - A system prompt that enforces:
      - Neutral, factual tone.
      - Explanation of typical pay, spread, hotspots, and caveats.
      - Emphasis on sample size and volatility where relevant.
    - A user message including:
      - A summary of filters.
      - A compact tabular representation of the records (aggregated where needed).
  - Return:
    - `html` – preferred, with paragraphs and bullet points.
    - `text` – plain-text version as fallback.

Frontend behaviour:

- AI section is **hidden by default**.
- When a request starts:
  - Show loading text (“Analysing the filtered dataset…”).
- On success:
  - Render `html` into the AI card.
  - Show the footer note:

> “Generated from the current filter set. Always sanity-check against source data before making decisions.”

- On failure:
  - Show a brief, readable error message in the card (e.g. “AI analysis failed: …”).
- Copy button:
  - Copies the visible narrative text to clipboard.
  - Temporarily changes to “Copied!” then back to “Copy text”.

AI constraints:

- AI does **not** change any data or save anything back to the DB.
- AI should only summarise and interpret **what is in the payload**; no hallucinated external numbers.
- Wording should avoid making strong legal/compliance claims.

---

## 5. Admin, coverage & quality

### 5.1 Role and sector mapping tools

Product goals:

- Give superusers the tools to:
  - Map messy role titles into canonical groups.
  - Override sectors for specific roles.
  - See the impact of mappings on coverage and analytics.

Expected behaviours:

- Mappings are:
  - Searchable and filterable.
  - Editable in bulk (bulk-map screens).
  - Annotated with counts (how many JobRecords they affect).
- Changes should:
  - Be idempotent and reversible via UI (editing existing mapping).
  - Take effect in future imports and, where reasonable, in recomputed summaries.

### 5.2 Coverage & health

Purpose: monitor “how much of the market we can see” and guide scraping/coverage strategy.

Concepts:

- **Coverage by sector** – how many roles and adverts we have per sector over a time window.
- **Coverage by geography** – equivalent for counties/regions.
- **Weak coverage** – combinations of sector+location where counts fall below thresholds.

Admin UI:

- Coverage dashboard shows:
  - A list/table of sector/location combos with coverage scores.
  - A heatmap or similar visual representation.
  - Export options (CSV).
- Coverage health tile:
  - Rolls up coverage statistics into **green / amber / red**.
  - Uses thresholds defined in `coverage.py`:
    - Green – overall coverage good, few weak spots.
    - Amber – some notable gaps.
    - Red – many weak spots; scraping strategy needs attention.

Cron behaviour:

- `cron_runner.py` periodically:
  - Recomputes coverage stats.
  - Logs coverage summary into `CronRunLog` (for trend & debugging).
- Coverage logic is centralised in `coverage.py` so the admin UI and cron share the same rules.

### 5.3 Cron & ingestion monitoring

Product expectations:

- Admin UI exposes a **Cron Runs** screen:
  - Shows history of `CronRunLog` entries.
  - Includes:
    - Trigger (scheduled/manual).
    - Status (success/fail).
    - Basic stats (scraped, imported, summaries rebuilt, coverage updated).
    - Any error text.
  - Provides a **“Run scheduled jobs now”** action for superusers.
- Status JSON endpoint (e.g. `/admin/status.json`) provides:
  - Health summary (DB reachable, last cron, coverage status).
  - Used for external monitoring or internal dashboards.

---

## 6. Safety rails & invariants (product-level)

Even though the implementation details live in `architecture.md` and code, these rules matter from a product perspective:

1. **Never silently change meaning of core metrics.**
   - “Typical pay” = median hourly, not average.
   - “Spread” = P25–P75; “range” = min–max.

2. **Routes and JSON shapes are contracts.**
   - URLs like `/pay-explorer`, `/api/pay-compare`, `/insights`, `/insights/ai-analyze` are assumed stable by the UI and cron tools.
   - Any change here is a breaking product change and must be deliberate.

3. **Sample size must always be visible.**
   - Every major chart or number should be interpretable in the context of how many records sit behind it.
   - The UI should nudge users away from over-reading tiny samples.

4. **AI features are advisory, not authoritative.**
   - They summarise, they don’t decide.
   - Messaging must encourage verification against charts/tables.

5. **Canonicalisation is central.**
   - Role and sector mappings power almost everything.
   - Admin tools for managing these mappings are first-class, not “hidden dev knobs”.

---

## 7. Roadmap hooks (for future spec extensions)

These are **not** all implemented yet, but the spec assumes they’re on the roadmap and should be added without contradicting current behaviour:

- Deeper AI support:
  - AI-assisted role/sector mapping suggestions with confidence scores.
  - AI narrative for Pay Explorer comparisons (per role/sector).
- More coverage-aware UX:
  - Badges/alerts in Pay Explorer/Insights when a view crosses weak-coverage thresholds.
- Client-specific overlays:
  - Ability to overlay a client’s own pay bands on top of market data.

When adding new features, they should:

- Reuse existing concepts (market slice, canonical roles, coverage).
- Prefer additive behaviour over changing existing semantics.
- Be documented by **extending this Memory Spec**, not overwriting it.

---


