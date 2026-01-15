# app/blueprints/dashboard/__init__.py
from __future__ import annotations

from flask import Blueprint

# Blueprint name STAYS "dashboard" so endpoint names don't change.
bp = Blueprint("dashboard", __name__)

# Import modules that attach routes to `bp`.
# These imports must come AFTER bp is created.
from . import core  # noqa: F401
from . import insights  # noqa: F401
from . import quick_search  # noqa: F401
from . import role_admin  # noqa: F401
from . import role_report  # noqa: F401

# Optional: re-export helper functions for backwards compatibility
# (in case any other module imports them from app.blueprints.dashboard).
from .helpers import (  # noqa: F401
    _fresh_filter_options,
    _clean_raw_job_title,
    _rule_based_canonical,
    _build_canonical_vocab,
    _canonical_role_filter_options,
    _fuzzy_best_match,
    _suggest_canonical_for_raw,
    _role_hygiene_flags,
    _role_hygiene_score,
    _job_roles_report_data,
    _unmapped_role_hotspots,
    _sector_override_mismatches,
    _clean_canonical_label,
)
