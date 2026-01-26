
---

## `payscopememory.md` – PayScope Memory Spec (updated)

:contentReference[oaicite:1]{index=1}  

```markdown
# PayScope Memory Spec (Product Rules & Behaviours)

_Version 2.2 – 23 January 2026_

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
  - A Recruiter Radar view that gives a one-page snapshot for a specific role+area (current/forecast pay, demand, competition, and recommended pay to offer).
  - Dashboards/Insights for monitoring pay levels, spread and change over time.
  - Admin tools for ingestion, role/sector hygiene, coverage and diagnostics.
  - AI helpers that explain patterns in plain language (AI Insights, Radar commentary).
  - Role & sector hygiene workflows including AI-assisted mapping and CSV review-export/import.

**Primary user personas**

1. **HR / Reward lead**
   - Wants to benchmark pay, spot under/over-payment and justify changes.
2. **Talent Acquisition / Resourcing lead**
   - Wants to understand market pressure, hotspots, and where to advertise.
   - Uses **Recruiter Radar** to answer “what should we pay for this role in this area?” quickly and defensibly.
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
- **Radar slice** – the filtered mini-market used by Recruiter Radar:
  - A canonical role (or small cluster of roles).
  - A central location + radius.
  - A lookback window (e.g. last 90 days).

---

## 3. Data rules & normalisation

### 3.1 Pay values

- PayScope aims to work in **hourly equivalent** where possible.

- When importing, the importer:
  - Converts annual salaries to an hourly equivalent using configurable assumptions (e.g. 37.5–40 hours/week).
  - Normalises mixed formats (“£14–£16 per hour”, “£30k–£35k per annum”, etc.) into numeric fields on `JobRecord`.
  - Interprets source-specific quirks (e.g. Adzuna’s API fields) into a consistent `min_rate`, `max_rate` and `rate_type` on `JobPosting` that can then be converted to hourly.

- Whenever pay is displayed in analytics:
  - **Prefer the canonical hourly field** (e.g. `JobRecord.pay_rate` or equivalent).
  - Ignore non-sensical values (≤0, missing, or clearly broken).
  - Ignore values clearly marked as annual-only where conversion was not possible.

- For **Adzuna-sourced data**, there is an additional safety rail in the form of a dedicated backfill:

  - A script (`backfill_adzuna_hourly_rates.py`) can be run to **audit and correct** hourly rates derived from Adzuna’s payload when either:
    - Adzuna changes how it encodes pay, or
    - New conversion rules are introduced (e.g. hours/week assumptions are updated).

  - The backfill does the following (implementation detail, but important for product behaviour):

    - Recomputes hourly equivalents from the stored annual salaries using fixed assumptions:
      - e.g. 37.5 hours/week, 52 weeks/year (plus days/week where relevant).
    - Flags “suspicious” postings where the currently stored hourly is **too high to be plausible** for the sector (e.g. above a threshold like £30/hour, configurable).
    - Applies a simple **scale fix** where the existing hourly is almost exactly 10× the recomputed hourly:
      - e.g. a support worker job at £162.56/hour with a `salary_min` of £317,000/year gets corrected to £16.26/hour.
    - Leaves non-suspicious postings alone, so genuine high-paying roles are not automatically flattened.
    - Optionally propagates the corrected hourly into any linked JobRecords via `imported_from_posting_id`, ensuring that:
      - Maps.
      - Insights.
      - Pay Explorer.
      - Recruiter Radar.
      all see the corrected hourly value.

  - From a **product point of view**, the promise is:

    - Adzuna-sourced pay numbers should sit in **plausible bands** for the role/sector, rather than silently showing absurd 10× salaries that distort charts.
    - If and when pay logic is tuned, **historical Adzuna adverts can be brought into line** via a controlled backfill process, rather than being stuck with legacy conversion mistakes.
    - After a significant backfill, daily summaries (`JobSummaryDaily`) are rebuilt for the affected time window, so users always see analytics that match the corrected underlying adverts.

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
    - Recruiter Radar role selection and canonicalisation.

---

## 4. Main user-facing surfaces

### 4.1 Workspace home (`/home`)

Purpose: tile-based “launchpad” for main tools.

- Shows tiles for:
  - **Upload data**
  - **Map View**
  - **Pay Explorer**
  - **Recruiter Radar**
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

### 4.6 Recruiter Radar (`/recruiter/radar`)

Purpose: give a recruiter a **one-page, decision-ready view** for a specific role in a specific area:

> “If I advertise for this role here, what should I pay, and how hot is the market?”

#### 4.6.1 Inputs

- **Role**:
  - Single text box with typeahead/auto-complete.
  - As the user types, suggest canonical role groups (e.g. “Support Worker”, “Registered Nurse”) backed by `job_role_group` / `JobRoleMapping`.
  - The selected value should always map to a canonical role if possible.
- **Location**:
  - Single free-text input:
    - Accepts full postcodes (e.g. “WS13 6QX”).
    - Accepts postcode districts (e.g. “WS13”).
    - Accepts town/city names where possible.
  - UI hint: “Postcode or town/city (WS13, Lichfield, etc.).”
- **Radius**:
  - Three quick buttons:
    - 5 miles, 15 miles, 25 miles.
  - Exactly one must be selected; default can be 15 miles.
- **Lookback window**:
  - Configurable options, typically:
    - 30 days, 60 days, 90 days, 180 days.
  - Default: 90 days.

Behind the scenes:

- The system geocodes the location into a **UK postcode + lat/lon** and applies a radius filter.
- The **Radar slice** is defined as:
  - `role` (canonical group).
  - adverts within `radius` miles of location.
  - adverts in the `lookback window`.

If geocoding fails or falls outside the UK bounds:

- Show a clear error:
  - “We couldn’t find that as a UK location. Try a full postcode or nearby town/city.”

#### 4.6.2 Outputs: layout & content

The Recruiter Radar page should follow a consistent layout:

1. **Header & filter summary**
   - Role, location and radius clearly summarised.
   - Lookback window shown in plain English (“Last 90 days of adverts”).

2. **Headline pay cards**
   - **Typical pay now**
     - Main number: median hourly pay in the Radar slice.
     - Sub-note: average pay and sample size (adverts in slice).
   - **Forecasted pay**
     - Main number: forecast median hourly pay 3 months ahead (or similar).
     - Sub-note: trend direction:
       - “Trend: rising”, “Trend: flat”, “Trend: falling”.
       - Include a short phrase: “based on last 6 months of adverts”.
   - **Recommended pay to offer**
     - Main number: recommended hourly rate if recruiting **now**.
     - Sub-band: “Recommended band: £X.XX – £Y.YY”.
     - Principle:
       - Base on current median + trend + competition:
         - Tight market: recommend above median.
         - Loose market: close to median or slightly below, but not absurdly low.
       - Always display it as **guidance**, not a guarantee.

3. **Demand & competition panel**

- **Demand**
  - Advert counts:
    - Total adverts in the lookback window.
    - Adverts in last 30 days (or shorter sub-window).
  - Trend:
    - Rising / flat / falling – based on simple counts by week or month.
  - Copy:
    - “Demand has increased/decreased X% vs the previous period.”
- **Competition**
  - Distinct employers:
    - Number of unique employers posting for that role in the radius.
  - Top employers:
    - List top 3–5 employers by advert count, with badges showing count.
  - Optional note:
    - “Most active employer: <name> (N adverts in this window).”

4. **Recent adverts list**

- A small table (or card list) of the most recent adverts in the Radar slice:
  - Columns:
    - Employer.
    - Job title (raw).
    - Hourly pay (or – if missing).
    - Postcode / town.
    - Posting date / imported month.
  - Typically limited to the last 10–20 adverts, with a link to “View these in Records” if the user wants the full table.

5. **Trend & forecast chart**

- Simple line (or area) chart:
  - X-axis: time (e.g. weeks or months).
  - Y-axis: median hourly pay.
- Shows:
  - Historical medians (solid line).
  - Forecast line (dashed) for the next 2–3 points (e.g. next 3 months).
- Visual cues:
  - Clear distinction between historical and forecast.
  - Tooltip explaining that forecasts are simple trend projections, not guarantees.

6. **AI commentary card**

- Goal: a short, well-structured narrative that ties everything together.
- Should answer:
  - Where is typical pay sitting right now?
  - Is the market heating up or cooling down?
  - Are there a lot of other employers fishing in the same pond?
  - How “safe” vs “aggressive” is the recommended pay?
- Tone:
  - Calm, factual, non-alarmist.
  - Avoid jargon and legalistic wording.
- The card should include:
  - 2–4 paragraphs or bullet blocks.
  - A final “sanity check” note along the lines of:

> “Use this as a starting point and sense-check against your own bands and internal equity.”

AI behaviour:

- Inputs to the AI should be:
  - Role, location, radius, lookback window.
  - The key numeric outputs (medians, forecast, recommended band).
  - Demand and competition metrics.
  - Any caveats (small sample size, patchy coverage).
- Constraints:
  - AI should not invent external market numbers.
  - AI should never promise that offering the recommended rate will “guarantee applicants”; instead talk in likelihood / attractiveness terms.
  - If sample size is low, AI must say so explicitly.

#### 4.6.3 Sample size & coverage in Radar

- If the Radar slice has **very few adverts** (e.g. <20 ads in lookback window):
  - A visible warning should appear near the header:
    - “Small sample – treat these figures as directional only.”
  - Recommended pay should still be calculated, but with softer language (e.g. “rough starting point”).
- If local coverage is weak (based on coverage logic):
  - Add a note in the commentary or a badge:
    - “Coverage for this role/area is thin; actual market may be noisier than shown.”

---

## 5. Admin, coverage & quality

### 5.1 Role and sector mapping tools

Product goals:

- Give superusers the tools to:
  - Map messy role titles into canonical groups.
  - Override sectors for specific roles.
  - See the impact of mappings on coverage and analytics.
  - Review and approve large batches of mappings safely.

Expected behaviours:

- Mappings are:
  - Searchable and filterable.
  - Editable in bulk (bulk-map screens).
  - Annotated with counts (how many JobRecords they affect).
- Changes should:
  - Be idempotent and reversible via UI (editing existing mapping).
  - Take effect in future imports and, where reasonable, in recomputed summaries.

#### 5.1.1 Job Role Cleaner (interactive)

- Screen shows:
  - Raw `job_role` strings with counts.
  - Current canonical mapping (if any).
  - Suggestions:
    - Rule-based cleaning.
    - Fuzzy matches against existing canonical vocabulary.
    - Optional AI suggestions (“Ask AI”) when heuristics are weak.
- Actions:
  - Map a single raw value → canonical role.
  - Bulk-map multiple selected raw values → canonical role.
  - Auto-clean subset:
    - Uses rules + fuzzy matching.
    - Only accepts suggestions above a confidence threshold (configurable per run).
- Optional **Apply to existing records** flags:
  - If ticked, updates `job_role_group` on matching `JobRecord` rows to the chosen canonical role.
  - Defaults can be tuned; generally safe to enable on high-confidence mappings.

#### 5.1.2 Canonical label cleaner

- One-off (but safe to re-run) action that:
  - Scans `JobRoleMapping.canonical_role` labels.
  - Detects labels that look like long AI paragraphs (too many words/sentences).
  - Replaces them with cleaner, title-like labels using a deterministic helper.
- Product rule:
  - Do not silently change meaning; aim to compress long text into a sane job title.
  - Provide a clear success message with counts of updated vs unchanged mappings.

#### 5.1.3 Sector override cleaner

- Focuses on roles whose sector is:
  - Missing, empty, or “Other”.
- Shows:
  - Canonical roles (prefer `job_role_group`, fallback to `job_role`).
  - Current sector.
  - Basic pay stats (min, max, average).
- Actions:
  - Set canonical sector per role.
  - Bulk-assign canonical sector to multiple roles.
- These overrides:
  - Take precedence over other sector-derivation logic.
  - Feed through to dashboards, Insights, Pay Explorer and Recruiter Radar.

#### 5.1.4 CSV review-export & re-import workflow

Goal: allow a superuser to **export a review list**, work on it in Excel, then re-import their decisions.

- From the Job Role Cleaner UI:
  - A button like “Download review CSV” should export a file containing:
    - `raw_role` – raw `job_role` text.
    - `count` – how many records share this raw role.
    - `current_canonical` – current canonical role (if any).
    - `suggested_canonical` – best suggestion from rules/fuzzy/AI (non-binding).
    - `hygiene_score` / flags – optional, to help prioritise.
  - The CSV should be reasonably self-explanatory with a header row.

- Offline workflow (user expectation):
  - Open CSV in Excel/Sheets.
  - For each row, the user can:
    - Accept the suggested canonical role.
    - Override with a better canonical role.
    - Leave blank if they want to defer a decision.
  - The spec does not prescribe exact column names for decisions, but a typical pattern is:
    - `final_canonical` – field the system actually uses on re-import.
    - Optionally, a `mode` column (“use_suggested” vs “manual”) if needed.

- Re-import behaviour:
  - Admin uploads the modified CSV via a dedicated screen.
  - The system:
    - Validates file shape (columns present, no obviously broken rows).
    - For each row where a final canonical role is present:
      - Creates or updates `JobRoleMapping` for that `raw_role`.
    - Optional toggle:
      - “Also backfill existing records”: if enabled, updates `job_role_group` on matching JobRecords.
  - After a successful import:
    - The UI should confirm:
      - How many mappings were created vs updated.
      - Whether backfill ran and how many records were affected (approximate is fine).
  - Failure modes:
    - If columns are missing or misnamed, show a clear error and example.

- Product rules:
  - This flow is intended for **trusted superusers** only.
  - It should be easier and safer than hacking mappings directly in the DB.
  - It must not silently delete or overwrite mappings where the CSV row is blank.

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

Coverage logic is centralised in `coverage.py` so the admin UI and cron share the same rules.

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

In addition:

- Larger **backfill / hygiene operations** that materially affect pay or summaries are expected to run as **explicit maintenance tasks**, not as part of the regular cron:

  - Example: running `backfill_adzuna_hourly_rates.py` with `only_if_suspicious=True` and a high `suspicious_over_hourly` threshold to clean up obviously mis-scaled Adzuna rates.
  - Example: running `summary_runner.py` afterwards to rebuild `JobSummaryDaily` for the last N days so that:
    - Pay Explorer.
    - Insights.
    - Recruiter Radar.
    all reflect the corrected pay.

- From a product perspective, the important behaviours are:

  - Users **do not need to know** that a backfill happened behind the scenes; they simply see more realistic hourly rates and analytics that no longer spike because of encoding errors.
  - Superusers can see, from `CronRunLog` and admin tools, when such maintenance was done and at what scale.
  - Backfills are **deliberate events** with:
    - Dry-run capability.
    - Batching where needed.
    - Clear start/end logging.

---

## 6. Safety rails & invariants (product-level)

Even though the implementation details live in `architecture.md` and code, these rules matter from a product perspective:

1. **Never silently change meaning of core metrics.**
   - “Typical pay” = median hourly, not average.
   - “Spread” = P25–P75; “range” = min–max.

2. **Routes and JSON shapes are contracts.**
   - URLs like `/pay-explorer`, `/api/pay-compare`, `/insights`, `/insights/ai-analyze`, `/recruiter/radar`, `/api/recruiter/radar` are assumed stable by the UI and cron/tools.
   - Any change here is a breaking product change and must be deliberate.

3. **Sample size must always be visible.**
   - Every major chart or number should be interpretable in the context of how many records sit behind it.
   - The UI should nudge users away from over-reading tiny samples.

4. **AI features are advisory, not authoritative.**
   - They summarise, they don’t decide.
   - Messaging must encourage verification against charts/tables.
   - Recruiter Radar commentary should explicitly position recommendations as guidance, not guarantees or legal advice.

5. **Canonicalisation is central.**
   - Role and sector mappings power almost everything:
     - Maps, Pay Explorer, Insights, Recruiter Radar, coverage.
   - Admin tools for managing these mappings are first-class, not “hidden dev knobs”.
   - The CSV review-export/import workflow is the preferred mechanism for large-scale cleanups.

6. **Location handling must be UK-sensitive.**
   - Geocoding and radius searches should respect UK bounds; obviously non-UK results should be rejected.
   - If a user enters something ambiguous, the system should fail clearly rather than silently mis-locating the Radar slice.

7. **Backfills should be transparent but non-disruptive.**
   - When pay backfills (like the Adzuna hourly correction) are run:
     - They should be logged and inspectable by superusers.
     - They should be **idempotent and narrow** by default (target suspicious values, not everything).
     - Downstream analytics (`JobSummaryDaily`) should be refreshed so user-facing surfaces are coherent.
   - Business copy in the UI should not need to change; the backfill is there to make the product keep its existing promises (“typical pay” is believable, outliers are real, not formatting bugs).

---

## 7. Roadmap hooks (for future spec extensions)

These are **not** all implemented yet, but the spec assumes they’re on the roadmap and should be added without contradicting current behaviour:

- Deeper AI support:
  - AI-assisted role/sector mapping suggestions with confidence scores (already partially present through “Ask AI”; can be extended).
  - AI narrative for Pay Explorer comparisons (per role/sector).
  - AI commentary variants tuned for different personas (HR vs TA vs Reward).
- More coverage-aware UX:
  - Badges/alerts in Pay Explorer/Insights/Radar when a view crosses weak-coverage thresholds.
- Client-specific overlays:
  - Ability to overlay a client’s own pay bands on top of market data (Pay Explorer, Insights, Radar).
- Recruiter Radar enhancements:
  - Allow saving “Radar snapshots” as sharable reports.
  - Add a “What if we pay £X?” slider to show how aggressive/competitive that rate would be vs local distribution.
- Pay-quality diagnostics:
  - Admin-facing view that surfaces:
    - Sectors/roles with lots of suspiciously high hourly values.
    - Where backfills (like Adzuna hourly correction) have had the biggest impact.
  - This can help direct future ingestion rule tweaks.

When adding new features, they should:

- Reuse existing concepts (market slice, Radar slice, canonical roles, coverage).
- Prefer additive behaviour over changing existing semantics.
- Be documented by **extending this Memory Spec**, not overwriting it.

---
