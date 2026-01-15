from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import difflib
import re
from collections import Counter
import statistics as stats

from sqlalchemy import func, cast, Integer, case

from extensions import db
from models import JobRecord, JobRoleMapping, JobRoleSectorOverride

# Optional fuzzy matcher (RapidFuzz preferred, fallback to difflib)
try:
    from rapidfuzz import fuzz  # type: ignore
except Exception:  # pragma: no cover
    fuzz = None  # type: ignore


def _fresh_filter_options():
    """
    Avoid TTL cache; query distincts directly so selects always populate
    on the dashboard filters.
    """

    def col_distinct(col):
        return [
            v[0]
            for v in db.session.query(col)
            .filter(col.isnot(None))
            .distinct()
            .order_by(col)
            .all()
        ]

    # Prefer canonical roles if present; fallback to raw job_role
    role_col = JobRecord.job_role
    if hasattr(JobRecord, "job_role_group"):
        role_col = JobRecord.job_role_group

    return {
        "sectors": col_distinct(JobRecord.sector),
        "roles": col_distinct(role_col),
        "counties": col_distinct(JobRecord.county),
        "months": col_distinct(JobRecord.imported_month),
        "years": col_distinct(JobRecord.imported_year),
    }


# ----------------------------------------------------------------------
# Job role hygiene helpers (rules + suggestions)
# ----------------------------------------------------------------------

# A small, opinionated ruleset for turning messy raw titles into canonical roles.
# This is intentionally conservative: we only auto-map when we're confident.
_ROLE_RULES: List[Tuple[re.Pattern[str], str]] = [
    # Registered Nurse variations
    (re.compile(r"\b(rn|rgn|registered\s*nurse|staff\s*nurse)\b", re.I), "Registered Nurse"),
    (re.compile(r"\b(nurse\s*associate)\b", re.I), "Nurse Associate"),
    (re.compile(r"\b(community\s*nurse)\b", re.I), "Registered Nurse"),
    # Care / support
    (re.compile(r"\b(care\s*assistant|carer|care\s*worker|health\s*care\s*assistant|hca)\b", re.I), "Care Assistant"),
    (re.compile(r"\b(senior\s*(care\s*assistant|carer|care\s*worker|hca))\b", re.I), "Senior Care Assistant"),
    (re.compile(r"\b(support\s*worker)\b", re.I), "Support Worker"),
    (re.compile(r"\b(senior\s*support\s*worker)\b", re.I), "Senior Support Worker"),
    (re.compile(r"\b(learning\s*disabilities?\s*support)\b", re.I), "Support Worker"),
    # Leadership / management
    (re.compile(r"\b(team\s*leader)\b", re.I), "Team Leader"),
    (re.compile(r"\b(deputy\s*manager)\b", re.I), "Deputy Manager"),
    (re.compile(r"\b(registered\s*manager|service\s*manager|home\s*manager)\b", re.I), "Registered Manager"),
    # Domestic / housekeeping
    (re.compile(r"\b(house\s*keeper|housekeeper|domestic\s*assistant|domestic)\b", re.I), "Domestic Assistant"),
    (re.compile(r"\b(cleaner|cleaning)\b", re.I), "Cleaner"),
    (re.compile(r"\b(cook|chef|kitchen\s*assistant)\b", re.I), "Kitchen Assistant"),
    # Maintenance
    (re.compile(r"\b(maintenance\s*(assistant|person|operative)|handyman)\b", re.I), "Maintenance"),
    (re.compile(r"\b(electrician)\b", re.I), "Electrician"),
    # Admin
    (re.compile(r"\b(administrator|admin\s*assistant|office\s*administrator)\b", re.I), "Administrator"),
]


def _clean_raw_job_title(raw: str) -> str:
    """Normalize a raw job title into a comparable 'cleaned' string."""
    s = (raw or "").strip()
    if not s:
        return ""

    # Remove bracketed noise: (Nights), [Temp], etc.
    s = re.sub(r"\([^\)]*\)", " ", s)
    s = re.sub(r"\[[^\]]*\]", " ", s)

    # Remove obvious pay fragments: £12.34, 12.34/hr, per hour
    s = re.sub(r"£\s*\d+(?:\.\d+)?", " ", s, flags=re.I)
    s = re.sub(r"\b\d+(?:\.\d+)?\s*(?:ph|p\/h|per\s*hour|\/hr|hr)\b", " ", s, flags=re.I)

    # Remove contract/time qualifiers (keep conservative)
    s = re.sub(
        r"\b(full\s*time|part\s*time|temp(?:orary)?|permanent|contract|bank|agency)\b",
        " ",
        s,
        flags=re.I,
    )
    s = re.sub(r"\b(days?|nights?|weekends?)\b", " ", s, flags=re.I)

    # Strip location-like suffixes after separators (common in scraped titles)
    s = re.split(r"\s[-–|•]\s", s, maxsplit=1)[0]

    # Lower, keep letters/numbers/spaces, collapse whitespace
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s\+\/]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    return s


def _rule_based_canonical(raw: str) -> Optional[str]:
    """Return a canonical role if a rule matches, else None."""
    if not raw:
        return None
    for pat, canonical in _ROLE_RULES:
        if pat.search(raw):
            return canonical
    return None


def _build_canonical_vocab() -> List[str]:
    """Build a stable list of canonical roles we can suggest against."""
    vocab: List[str] = []

    # 1) Existing canonical roles in mappings
    try:
        rows = db.session.query(JobRoleMapping.canonical_role).distinct().all()
        vocab.extend([r[0] for r in rows if (r and (r[0] or "").strip())])
    except Exception:
        pass

    # 2) Existing canonical roles already applied on JobRecord (job_role_group)
    try:
        if hasattr(JobRecord, "job_role_group"):
            rows = (
                db.session.query(JobRecord.job_role_group)
                .filter(
                    JobRecord.job_role_group.isnot(None),
                    func.trim(JobRecord.job_role_group) != "",
                )
                .distinct()
                .all()
            )
            vocab.extend([r[0] for r in rows if (r and (r[0] or "").strip())])
    except Exception:
        pass

    # 3) Built-in role taxonomy seeds (kept short on purpose)
    seed = [
        "Care Assistant",
        "Senior Care Assistant",
        "Support Worker",
        "Senior Support Worker",
        "Registered Nurse",
        "Nurse Associate",
        "Team Leader",
        "Deputy Manager",
        "Registered Manager",
        "Domestic Assistant",
        "Cleaner",
        "Kitchen Assistant",
        "Maintenance",
        "Electrician",
        "Administrator",
    ]
    vocab.extend(seed)

    # De-dupe, preserve order-ish
    seen = set()
    out: List[str] = []
    for v in vocab:
        vv = (v or "").strip()
        if not vv:
            continue
        key = vv.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(vv)

    return out


def _canonical_role_filter_options() -> List[str]:
    """
    Build a sorted list of canonical roles for the Insights Job Role filter.

    Uses:
      - JobRoleMapping.canonical_role
      - JobRecord.job_role_group (if present)

    Falls back to raw JobRecord.job_role only if nothing canonical is available,
    so the filter never appears empty.
    """
    labels: set[str] = set()

    # Canonical roles from mappings
    try:
        rows = (
            db.session.query(JobRoleMapping.canonical_role)
            .filter(
                JobRoleMapping.canonical_role.isnot(None),
                func.trim(JobRoleMapping.canonical_role) != "",
            )
            .distinct()
            .order_by(JobRoleMapping.canonical_role)
            .all()
        )
        for (val,) in rows:
            s = (val or "").strip()
            if s:
                labels.add(s)
    except Exception:
        pass

    # Canonical roles already written into job_role_group
    try:
        if hasattr(JobRecord, "job_role_group"):
            rows = (
                db.session.query(JobRecord.job_role_group)
                .filter(
                    JobRecord.job_role_group.isnot(None),
                    func.trim(JobRecord.job_role_group) != "",
                )
                .distinct()
                .order_by(JobRecord.job_role_group)
                .all()
            )
            for (val,) in rows:
                s = (val or "").strip()
                if s:
                    labels.add(s)
    except Exception:
        pass

    # If we genuinely have no canonical labels yet, fall back to raw job_role
    if not labels:
        try:
            rows = (
                db.session.query(JobRecord.job_role)
                .filter(
                    JobRecord.job_role.isnot(None),
                    func.trim(JobRecord.job_role) != "",
                )
                .distinct()
                .order_by(JobRecord.job_role)
                .limit(1000)
                .all()
            )
            for (val,) in rows:
                s = (val or "").strip()
                if s:
                    labels.add(s)
        except Exception:
            pass

    return sorted(labels, key=lambda x: x.lower())


def _fuzzy_best_match(query: str, choices: List[str]) -> Tuple[Optional[str], int]:
    """Return (best_choice, score 0-100)."""
    q = (query or "").strip()
    if not q or not choices:
        return (None, 0)

    if fuzz is not None:
        best = None
        best_score = 0
        for c in choices:
            sc = int(fuzz.token_set_ratio(q, c))
            if sc > best_score:
                best_score = sc
                best = c
        return (best, best_score)

    # difflib fallback
    best = None
    best_score = 0
    for c in choices:
        sc = int(100 * difflib.SequenceMatcher(None, q.lower(), (c or "").lower()).ratio())
        if sc > best_score:
            best_score = sc
            best = c
    return (best, best_score)


def _suggest_canonical_for_raw(raw: str, vocab: List[str]) -> Dict[str, object]:
    """Compute cleaned form + best suggestion + score + source."""
    cleaned = _clean_raw_job_title(raw)
    rule_hit = _rule_based_canonical(raw or "")

    if rule_hit:
        return {"cleaned": cleaned, "suggested": rule_hit, "score": 100, "source": "rule"}

    best, score = _fuzzy_best_match(cleaned, vocab)
    return {"cleaned": cleaned, "suggested": best, "score": int(score), "source": "fuzzy"}


# ----------------------------------------------------------------------
# Canonical label cleanup helper
# ----------------------------------------------------------------------

ROLE_LABEL_MAX_LEN = 80  # keep canonical labels short and tidy


def _clean_canonical_label(raw: str) -> str:
    """
    Best-effort normalisation for JobRoleMapping.canonical_role based on
    patterns seen in the export (markdown blobs, 'Canonical Job Role:', etc).

    Returns a cleaned label or an empty string if we can't confidently improve it.
    """
    if not raw:
        return ""

    s = str(raw)
    # Normalise newlines but keep them so we can reason about "first line"
    s = s.replace("\r\n", "\n").replace("\r", "\n").strip()

    # Quick bail-out: looks like an already clean, short, single-line label
    if (
        "\n" not in s
        and len(s) <= ROLE_LABEL_MAX_LEN
        and "**" not in s
        and not re.search(r"(canonical\s+job\s+role|job\s+role|job\s+title)\s*[:\-]", s, re.I)
    ):
        clean = re.sub(r"\s+", " ", s).strip()
        clean = clean.strip("*").strip()
        clean = re.sub(r"^[#\-\*\s]+", "", clean).strip()
        return clean

    original = s

    # 1) Try to extract after "Canonical Job Role:", "Job Role:", or "Job Title:"
    label_re = re.compile(
        r"(canonical\s+job\s+role|job\s+role|job\s+title)\s*[:\-]\s*(.+)",
        re.IGNORECASE,
    )
    m = label_re.search(s)
    if m:
        candidate = m.group(2).strip()
        # Strip surrounding markdown ** if present
        candidate = candidate.strip("*").strip()
        # Only use up to first line / markdown break
        candidate = candidate.split("\n", 1)[0].strip()
        if "**" in candidate:
            candidate = candidate.split("**", 1)[0].strip()

        # Final clean-up
        candidate = re.sub(r"\s+", " ", candidate).strip()
        if candidate and len(candidate) <= ROLE_LABEL_MAX_LEN:
            return candidate

    # 2) If the string starts with a bold block, take the first **…** as the label
    if s.startswith("**"):
        inner = s[2:]
        if "**" in inner:
            candidate = inner.split("**", 1)[0].strip()
            candidate = re.sub(r"\s+", " ", candidate).strip()
            if candidate and len(candidate) <= ROLE_LABEL_MAX_LEN:
                return candidate

    # 3) Fallback: use the first line, stripped of markdown headers / bullets
    first_line = original.split("\n", 1)[0]
    first_line = re.sub(r"^[#\-\*\s]+", "", first_line).strip()  # strip bullets / '#' etc
    first_line = first_line.strip("*").strip()
    first_line = re.sub(r"\s+", " ", first_line).strip()

    # Don’t keep obviously over-long lines as canonical labels
    if len(first_line) > ROLE_LABEL_MAX_LEN:
        return ""

    # Require at least one letter
    if not re.search(r"[A-Za-z]", first_line):
        return ""

    return first_line


# ----------------------------------------------------------------------
# Role hygiene scoring helpers (for report + unmapped hotspots)
# ----------------------------------------------------------------------

def _role_hygiene_flags(raw: str) -> Dict[str, bool]:
    """
    Lightweight heuristics to flag 'noisy' job titles.

    These are intentionally simple and cheap – just enough to surface
    the worst offenders in the admin report.
    """
    s = (raw or "").strip()
    has_letters = bool(re.search(r"[A-Za-z]", s))

    is_all_caps = has_letters and s.upper() == s

    has_pay_terms = bool(
        re.search(
            r"(£\s*\d|\b\d+(?:\.\d+)?\s*(ph|p\/h|per\s*hour|hourly|rate|salary))",
            s,
            re.IGNORECASE,
        )
    )

    has_location_terms = bool(
        re.search(
            r"\b(london|manchester|birmingham|birmingham|leeds|liverpool|sheffield|nottingham|bristol|"
            r"cardiff|glasgow|scotland|wales|england|uk|united\s+kingdom|remote|hybrid)\b",
            s,
            re.IGNORECASE,
        )
    )

    has_agency_noise = bool(
        re.search(
            r"\b(agency|recruitment|recruiting|staffing|solutions|limited|ltd|plc)\b",
            s,
            re.IGNORECASE,
        )
    )

    has_brackets_or_codes = any(ch in s for ch in "[]()") or bool(
        re.search(r"\b(ref|reference)\s*[:#]\s*\w+", s, re.IGNORECASE)
    )

    has_shift_words = bool(
        re.search(
            r"\b(nights?|days?|weekends?|shifts?|rota|rotational)\b",
            s,
            re.IGNORECASE,
        )
    )

    return {
        "is_all_caps": is_all_caps,
        "has_pay_terms": has_pay_terms,
        "has_location_terms": has_location_terms,
        "has_agency_noise": has_agency_noise,
        "has_brackets_or_codes": has_brackets_or_codes,
        "has_shift_words": has_shift_words,
    }


def _role_hygiene_score(flags: Dict[str, bool]) -> int:
    """
    Convert hygiene flags into a 0–100 'cleanliness' score.

    100 = looks like a clean, reusable job title
      0 = very messy (location/pay/agency/code noise everywhere)
    """
    score = 100

    if flags.get("has_pay_terms"):
        score -= 25
    if flags.get("has_location_terms"):
        score -= 20
    if flags.get("has_agency_noise"):
        score -= 15
    if flags.get("has_brackets_or_codes"):
        score -= 10
    if flags.get("has_shift_words"):
        score -= 5
    if flags.get("is_all_caps"):
        score -= 5

    if score < 0:
        score = 0
    if score > 100:
        score = 100
    return score


# ----------------------------------------------------------------------
# Job Role Mapping Report helpers
# ----------------------------------------------------------------------

def _job_roles_report_data() -> Tuple[List[Dict[str, object]], Dict[str, List[Dict[str, object]]]]:
    """
    Shared query for job role mapping report.

    Returns:
      summary: list of {
          canonical_role,
          raw_variants,
          total_count,
          share_total_pct,
          worst_hygiene_score,
          noisy_variants
      }
      grouped_roles: {
          canonical_role: [
              {
                  raw_value,
                  count,
                  share_total_pct,
                  hygiene_flags,
                  hygiene_score,
              },
              ...
          ]
      }
    """
    # Global total for % of whole dataset
    total_records = db.session.query(func.count(JobRecord.id)).scalar() or 0

    # Join JobRoleMapping -> JobRecord to get counts per raw_value
    q = (
        db.session.query(
            JobRoleMapping.canonical_role.label("canonical_role"),
            JobRoleMapping.raw_value.label("raw_value"),
            func.count(JobRecord.id).label("count"),
        )
        .outerjoin(JobRecord, JobRecord.job_role == JobRoleMapping.raw_value)
        .group_by(JobRoleMapping.canonical_role, JobRoleMapping.raw_value)
        .order_by(JobRoleMapping.canonical_role.asc(), func.count(JobRecord.id).desc())
    )

    rows = q.all()

    grouped_roles: Dict[str, List[Dict[str, object]]] = {}
    for canonical_role, raw_value, count in rows:
        cr = canonical_role or "—"
        rv = raw_value or "—"
        c = int(count or 0)

        flags = _role_hygiene_flags(rv)
        hygiene_score = _role_hygiene_score(flags)

        if total_records > 0:
            share_total_pct = round((c / total_records) * 100.0, 2)
        else:
            share_total_pct = 0.0

        grouped_roles.setdefault(cr, []).append(
            {
                "raw_value": rv,
                "count": c,
                "share_total_pct": share_total_pct,
                "hygiene_flags": flags,
                "hygiene_score": hygiene_score,
            }
        )

    # Summary table: one row per canonical_role
    summary: List[Dict[str, object]] = []
    for canonical_role, raw_list in grouped_roles.items():
        total_count = sum(r["count"] for r in raw_list)
        raw_variants = len(raw_list)
        worst_hygiene_score = 100
        noisy_variants = 0

        for r in raw_list:
            score = int(r.get("hygiene_score") or 0)
            if score < worst_hygiene_score:
                worst_hygiene_score = score
            if score < 80:
                noisy_variants += 1

        if total_records > 0:
            share_total_pct = round((total_count / total_records) * 100.0, 2)
        else:
            share_total_pct = 0.0

        summary.append(
            {
                "canonical_role": canonical_role,
                "raw_variants": raw_variants,
                "total_count": total_count,
                "share_total_pct": share_total_pct,
                "worst_hygiene_score": worst_hygiene_score,
                "noisy_variants": noisy_variants,
            }
        )

    # Sort summary by total_count desc so biggest roles float to the top
    summary.sort(key=lambda r: r["total_count"], reverse=True)

    return summary, grouped_roles


def _unmapped_role_hotspots(limit: int = 50) -> List[Dict[str, object]]:
    """
    Top unmapped raw job_role values by volume, with hygiene metrics.

    Intended for a 'Unmapped hotspots' widget on the report.
    """
    total_records = db.session.query(func.count(JobRecord.id)).scalar() or 0

    q = (
        db.session.query(
            JobRecord.job_role.label("raw_value"),
            func.count(JobRecord.id).label("count"),
        )
        .filter(JobRecord.job_role.isnot(None), func.trim(JobRecord.job_role) != "")
        .outerjoin(
            JobRoleMapping,
            JobRecord.job_role == JobRoleMapping.raw_value,
        )
        .filter(JobRoleMapping.id.is_(None))
        .group_by(JobRecord.job_role)
        .order_by(func.count(JobRecord.id).desc())
        .limit(limit)
    )

    rows = q.all()
    hotspots: List[Dict[str, object]] = []

    for raw_value, count in rows:
        rv = raw_value or "—"
        c = int(count or 0)
        flags = _role_hygiene_flags(rv)
        hygiene_score = _role_hygiene_score(flags)

        if total_records > 0:
            share_total_pct = round((c / total_records) * 100.0, 2)
        else:
            share_total_pct = 0.0

        hotspots.append(
            {
                "raw_value": rv,
                "count": c,
                "share_total_pct": share_total_pct,
                "hygiene_flags": flags,
                "hygiene_score": hygiene_score,
            }
        )

    return hotspots


def _sector_override_mismatches(
    min_rows: int = 20,
    dominance_threshold: float = 0.5,
) -> List[Dict[str, object]]:
    """
    For each JobRoleSectorOverride, compare the override sector with the
    dominant observed sector in JobRecord for that canonical role.

    Returns a list of likely mismatches to surface as gentle warnings.
    """
    # Use canonical role expression consistent with admin_role_sectors
    role_expr = func.coalesce(JobRecord.job_role_group, JobRecord.job_role)

    q = (
        db.session.query(
            JobRoleSectorOverride.canonical_role.label("canonical_role"),
            JobRoleSectorOverride.canonical_sector.label("override_sector"),
            JobRecord.sector.label("observed_sector"),
            func.count(JobRecord.id).label("count"),
        )
        .outerjoin(JobRecord, role_expr == JobRoleSectorOverride.canonical_role)
        .group_by(
            JobRoleSectorOverride.canonical_role,
            JobRoleSectorOverride.canonical_sector,
            JobRecord.sector,
        )
    )

    rows = q.all()
    by_role: Dict[str, Dict[str, object]] = {}

    for canonical_role, override_sector, observed_sector, count in rows:
        cr = canonical_role or "—"
        ov_sector = (override_sector or "Unknown") or "Unknown"
        obs_sector = (observed_sector or "Unknown") or "Unknown"
        c = int(count or 0)

        if cr not in by_role:
            by_role[cr] = {
                "override_sector": ov_sector,
                "sector_counts": {},
                "total_rows": 0,
            }

        role_entry = by_role[cr]
        sector_counts = role_entry["sector_counts"]  # type: ignore[assignment]
        sector_counts[obs_sector] = sector_counts.get(obs_sector, 0) + c  # type: ignore[index]
        role_entry["total_rows"] = int(role_entry["total_rows"]) + c  # type: ignore[index]

    mismatches: List[Dict[str, object]] = []

    for canonical_role, data in by_role.items():
        override_sector = data["override_sector"]  # type: ignore[assignment]
        sector_counts: Dict[str, int] = data["sector_counts"]  # type: ignore[assignment]
        total_rows = int(data["total_rows"])  # type: ignore[assignment]

        if total_rows < min_rows:
            continue

        # Find dominant observed sector
        if not sector_counts:
            continue
        dominant_sector, dominant_count = max(sector_counts.items(), key=lambda kv: kv[1])

        if total_rows > 0:
            dominant_share = dominant_count / float(total_rows)
        else:
            dominant_share = 0.0

        # Only flag when the dominant observed sector strongly disagrees
        if (
            dominant_sector
            and override_sector
            and dominant_sector != override_sector
            and dominant_share >= dominance_threshold
        ):
            mismatches.append(
                {
                    "canonical_role": canonical_role,
                    "override_sector": override_sector,
                    "dominant_sector": dominant_sector,
                    "dominant_share": round(dominant_share * 100.0, 1),
                    "total_rows": total_rows,
                }
            )

    # Sort by importance: biggest total_rows first
    mismatches.sort(key=lambda r: r["total_rows"], reverse=True)
    return mismatches
