PayScope – Memory Spec (Living Project Brief)

Version: v1.1
Last updated: 14 Dec 2025
Status: Canonical baseline

1. Purpose

PayScope is a UK-focused pay intelligence platform designed to provide real-world, market-led pay benchmarking across all sectors, starting with high-volume, hard-to-hire roles.

It aggregates live job advert data, normalises it into a clean canonical structure, overlays official datasets (ONS), and produces decision-grade insights for employers, operators, and advisors.

2. Core Design Principles (LOCKED)

These are non-negotiable unless explicitly overridden.

Normalise once, at import time

No recurring historic cleanups

No “fix later” pipelines

Raw in → Canonical out

Scrapers stay dumb

Importers do the intelligence

Derived data is disposable

Summaries can be rebuilt

Source records are sacred

Ambiguity is allowed

“Other” is acceptable where roles genuinely don’t classify

Not everything must be forced

3. Canonical Data Model (Key Tables)
JobPosting

Raw scraped data.

One row per advert

Source-specific fields allowed

Sector may be noisy here

JobRecord

Canonical, analytics-ready layer.

sector → canonical sector only

job_role_group → canonical role group

Used by all analytics, charts, summaries

JobSummaryDaily

Derived aggregation.

Grouped by:

date

county

sector

job_role_group

Fully rebuildable

SectorMapping

Authoritative sector dictionary.

raw_value → canonical_sector

Case-insensitive matching

Seeded and extendable

JobRoleMapping

Authoritative role dictionary.

raw_value → canonical_role

Populated dynamically during import

Human-reviewable over time

4. Sector Normalisation (LOCKED)
How it works

All sector normalisation happens during import

Importer:

Uppercases + trims raw sector

Looks up SectorMapping

Falls back to "Other"

Rules

No scraper sets canonical sector

No background job re-normalises sectors

Historic cleanup scripts exist only for emergency recovery

Current Canonical Sectors (examples)

Social Care

Nursing

IT & Technology

Finance & Accounting

HR / People

Admin & Office

Leadership & Management

Operations & Logistics

Customer Service

Education & Training

Legal

Domestic

Other

5. Job Role Normalisation (LOCKED)
How it works

Importer extracts:

raw title

job_role_group

JobRoleMapping:

created on first sight

reused thereafter

Canonical role drives all analytics

Intentional behaviour

Similar roles collapse together

“HR Advisor”, “Human Resources Advisor” → HR Advisor

Marketing titles, events, speculative listings may remain uncategorised

6. Scrapers (Current State)
Adzuna ✅ (Production-ready)

Robust retry + backoff

Salary normalisation to hourly

Best-effort postcode extraction

Produces clean JobRecord objects for importer

Reed (In progress)

Will follow same contract as Adzuna

No sector intelligence in scraper

Planned

NHS Jobs

GOV / Civil Service Jobs

Indeed (if viable within ToS)

Sector-specific sources later

7. Historic Cleanup (COMPLETED – DO NOT REPEAT)
What happened

Sector mappings seeded (83 rows)

Historic JobRecord.sector normalised

“Other” reduced from ~3,400 → ~1,050

Remaining “Other” roles reviewed and accepted

Outcome

Dataset is now clean enough

Remaining noise is real-world ambiguity

No further historic processing planned

8. Analytics & Pay Explorer
Inputs

JobRecord

JobSummaryDaily

OnsEarnings

Guarantees

Uses canonical sector + job_role_group only

ONS overlay via fuzzy geography matching (RapidFuzz)

Safe to rebuild summaries without data loss

9. Cron & Automation (LOCKED)
cron_runner.py

Single entry point for:

scraping

importing

canonicalisation

summaries

Explicit job functions only

No hidden side effects

Summary rebuilds

Standard window: last 60 days

Full rebuild only when explicitly triggered

10. Explicit Non-Goals

These are intentionally not part of PayScope (for now):

Continuous historic reprocessing

Auto-AI sector guessing on old data

Schema churn without instruction

Scraper-specific business logic

Perfect classification of edge-case roles

11. Current State (Snapshot)

Canonical pipeline stable

Sector & role normalisation locked

Adzuna fully integrated

Summaries rebuilt post-cleanup

System ready for:

New scrapers

UI/insight expansion

Commercialisation

12. How to Resume in a New Chat (IMPORTANT)

When starting a new chat, paste this prompt 👇

🔁 New Chat Starter Prompt

We are continuing development of PayScope.

The canonical project state is defined in PayScope Memory Spec v1.1.

Key constraints:

Sector and job role normalisation happens only at import time

No historic cleanups unless explicitly requested

Scrapers are dumb; importers are authoritative

JobRecord and JobSummaryDaily are the analytics sources

Current status:

Adzuna scraper live

Sector mappings seeded and locked

Historic data cleaned once

Next focus: continue development from this baseline without revisiting past decisions.