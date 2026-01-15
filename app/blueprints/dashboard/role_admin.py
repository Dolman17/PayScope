from __future__ import annotations

from typing import Dict, List, Tuple

from flask import render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from sqlalchemy import func
from sqlalchemy.orm import aliased

from extensions import db
from models import JobRecord, JobRoleMapping, JobRoleSectorOverride
from . import bp
from .helpers import (
    _build_canonical_vocab,
    _suggest_canonical_for_raw,
    _clean_canonical_label,
    _role_hygiene_flags,
    _role_hygiene_score,
)


@bp.route("/admin/job-roles")
@login_required
def admin_job_roles():
    """
    Admin view to see distinct job_role values and map them to canonical roles.
    Self-healing: ensures job_role_mappings table exists before querying.
    Supports:
      - q: search over raw roles (JobRecord.job_role)
      - canonical_search: search over canonical roles (JobRoleMapping.canonical_role)
      - status: all / with / without canonical mapping
    """
    # Make sure the mapping table exists (safe if already created)
    try:
        JobRoleMapping.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        # If this somehow fails, we still try to render with empty mappings below
        pass

    search = (request.args.get("q") or "").strip()
    canonical_search = (request.args.get("canonical_search") or "").strip()
    status = (request.args.get("status") or "all").strip().lower()
    if status not in ("all", "with", "without"):
        status = "all"

    # Base query: distinct job_role values with counts
    JRM = aliased(JobRoleMapping)

    q = db.session.query(
        JobRecord.job_role.label("raw_value"),
        func.count(JobRecord.id).label("count"),
    ).filter(JobRecord.job_role.isnot(None))

    if search:
        pattern = f"%{search}%"
        q = q.filter(JobRecord.job_role.ilike(pattern))

    # Join mode depends on status + canonical_search
    if status == "with" or canonical_search:
        # Need a join so we can filter on canonical_role
        q = q.join(JRM, JobRecord.job_role == JRM.raw_value)
    elif status == "without":
        q = q.outerjoin(JRM, JobRecord.job_role == JRM.raw_value).filter(JRM.id.is_(None))
    # status == "all" and no canonical_search: no join needed, we want everything

    if canonical_search:
        pattern_c = f"%{canonical_search}%"
        q = q.filter(JRM.canonical_role.ilike(pattern_c))

    rows = (
        q.group_by(JobRecord.job_role)
        .order_by(func.count(JobRecord.id).desc())
        .limit(500)
        .all()
    )

    # Existing mappings keyed by raw_value; if table is still missing for some reason,
    # fall back to an empty dict rather than crashing.
    try:
        mapping_rows = JobRoleMapping.query.order_by(JobRoleMapping.raw_value).all()
        mappings = {m.raw_value: m for m in mapping_rows}
    except Exception:
        mappings = {}

    # Keep consistent with the rest of the dashboard: count None OR empty
    # (This page is about raw roles, but the hygiene count should reflect canonical if available)
    if hasattr(JobRecord, "job_role_group"):
        uncategorised_roles_count = (
            db.session.query(func.count(JobRecord.id))
            .filter(
                (JobRecord.job_role_group.is_(None))
                | (func.trim(JobRecord.job_role_group) == "")
            )
            .scalar()
            or 0
        )
    else:
        uncategorised_roles_count = (
            db.session.query(func.count(JobRecord.id))
            .filter(
                (JobRecord.job_role.is_(None))
                | (func.trim(JobRecord.job_role) == "")
            )
            .scalar()
            or 0
        )

    # Suggestions (rules + fuzzy) for this page of raw roles
    vocab = _build_canonical_vocab()
    suggestions: Dict[str, Dict[str, object]] = {}
    for r in rows:
        rv = getattr(r, "raw_value", None)
        suggestions[rv] = _suggest_canonical_for_raw(rv or "", vocab)

    return render_template(
        "admin_job_roles.html",
        rows=rows,
        mappings=mappings,
        search=search,
        status=status,
        canonical_search=canonical_search,
        uncategorised_roles_count=uncategorised_roles_count,
        suggestions=suggestions,
    )


@bp.route("/admin/job-roles/map", methods=["POST"])
@login_required
def admin_job_roles_map():
    """
    Create/update a mapping for a raw job_role value to a canonical role.
    Optionally applies the change immediately to existing JobRecord rows.
    Redirects back to the current Job Role Cleaner filters (q, status, canonical_search).
    """
    raw_value = (request.form.get("raw_value") or "").strip()
    canonical_role = (request.form.get("canonical_role") or "").strip()
    apply_now = request.form.get("apply_now") == "1"

    # Preserve filters on redirect
    q_param = (request.form.get("q") or "").strip()
    status_param = (request.form.get("status") or "all").strip().lower()
    canonical_param = (request.form.get("canonical_search") or "").strip()

    if not raw_value or not canonical_role:
        flash("Raw value and canonical role are required.", "error")
        return redirect(
            url_for(
                "dashboard.admin_job_roles",
                q=q_param,
                status=status_param,
                canonical_search=canonical_param,
            )
        )

    # Ensure table exists here as well, in case this endpoint is hit first.
    try:
        JobRoleMapping.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    mapping = JobRoleMapping.query.filter_by(raw_value=raw_value).first()
    if mapping is None:
        mapping = JobRoleMapping(raw_value=raw_value, canonical_role=canonical_role)
    else:
        mapping.canonical_role = canonical_role

    db.session.add(mapping)

    if apply_now:
        # Prefer writing canonical into job_role_group (preserves raw job_role for audit),
        # but fall back to overwriting job_role if the canonical column doesn't exist.
        if hasattr(JobRecord, "job_role_group"):
            db.session.query(JobRecord).filter(JobRecord.job_role == raw_value).update(
                {JobRecord.job_role_group: canonical_role},
                synchronize_session=False,
            )
        else:
            db.session.query(JobRecord).filter(JobRecord.job_role == raw_value).update(
                {JobRecord.job_role: canonical_role},
                synchronize_session=False,
            )

    db.session.commit()
    flash(f"Mapping saved for role '{raw_value}' → '{canonical_role}'.", "success")
    return redirect(
        url_for(
            "dashboard.admin_job_roles",
            q=q_param,
            status=status_param,
            canonical_search=canonical_param,
        )
    )


@bp.route("/admin/job-roles/bulk-map", methods=["POST"])
@login_required
def admin_job_roles_bulk_map():
    """
    Bulk-create/update mappings for multiple raw job_role values to a single canonical role.
    Optionally applies the change immediately to existing JobRecord rows.

    Expects:
      - raw_values: repeated form fields (one per selected checkbox)
      - canonical_role: the target canonical role
      - apply_now: "1" if JobRecord rows should be updated too
      - q, status, canonical_search: current filter state on the Job Role Cleaner page
    """
    raw_values = request.form.getlist("raw_values") or []
    # De-duplicate, strip empty
    raw_values = sorted({(rv or "").strip() for rv in raw_values if (rv or "").strip()})

    canonical_role = (request.form.get("canonical_role") or "").strip()
    apply_now = request.form.get("apply_now") == "1"

    q_param = (request.form.get("q") or "").strip()
    status_param = (request.form.get("status") or "all").strip().lower()
    canonical_param = (request.form.get("canonical_search") or "").strip()

    if not raw_values:
        flash("Select at least one job title before using bulk assign.", "error")
        return redirect(
            url_for(
                "dashboard.admin_job_roles",
                q=q_param,
                status=status_param,
                canonical_search=canonical_param,
            )
        )

    if not canonical_role:
        flash("Canonical role is required for bulk assignment.", "error")
        return redirect(
            url_for(
                "dashboard.admin_job_roles",
                q=q_param,
                status=status_param,
                canonical_search=canonical_param,
            )
        )

    # Ensure table exists
    try:
        JobRoleMapping.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    updated_mappings = 0

    for raw_value in raw_values:
        mapping = JobRoleMapping.query.filter_by(raw_value=raw_value).first()
        if mapping is None:
            mapping = JobRoleMapping(raw_value=raw_value, canonical_role=canonical_role)
            db.session.add(mapping)
        else:
            mapping.canonical_role = canonical_role
        updated_mappings += 1

    if apply_now:
        # Prefer writing canonical into job_role_group (preserves raw job_role for audit),
        # but fall back to overwriting job_role if the canonical column doesn't exist.
        if hasattr(JobRecord, "job_role_group"):
            db.session.query(JobRecord).filter(JobRecord.job_role.in_(raw_values)).update(
                {JobRecord.job_role_group: canonical_role},
                synchronize_session=False,
            )
        else:
            db.session.query(JobRecord).filter(JobRecord.job_role.in_(raw_values)).update(
                {JobRecord.job_role: canonical_role},
                synchronize_session=False,
            )

    db.session.commit()

    flash(
        f"Bulk mapping applied: {updated_mappings} raw role(s) → '{canonical_role}'.",
        "success",
    )
    return redirect(
        url_for(
            "dashboard.admin_job_roles",
            q=q_param,
            status=status_param,
            canonical_search=canonical_param,
        )
    )


@bp.route("/admin/job-roles/auto-clean", methods=["POST"])
@login_required
def admin_job_roles_auto_clean():
    """Auto-clean + auto-map selected raw roles using rules + fuzzy suggestions.

    Expects:
      - raw_values: repeated fields
      - threshold: integer (0-100), default 88
      - apply_now: "1" to backfill existing JobRecord rows (writes to job_role_group if present)
      - q, status, canonical_search: preserved filter params
    """
    raw_values = request.form.getlist("raw_values") or []
    raw_values = sorted({(rv or "").strip() for rv in raw_values if (rv or "").strip()})

    q_param = (request.form.get("q") or "").strip()
    status_param = (request.form.get("status") or "all").strip().lower()
    canonical_param = (request.form.get("canonical_search") or "").strip()

    try:
        threshold = int((request.form.get("threshold") or "88").strip())
    except Exception:
        threshold = 88
    threshold = max(0, min(100, threshold))

    apply_now = request.form.get("apply_now") == "1"

    if not raw_values:
        flash("Select at least one job title before running auto-clean.", "error")
        return redirect(
            url_for(
                "dashboard.admin_job_roles",
                q=q_param,
                status=status_param,
                canonical_search=canonical_param,
            )
        )

    # Ensure mapping table exists
    try:
        JobRoleMapping.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    vocab = _build_canonical_vocab()

    mapped = 0
    skipped = 0

    # We'll also keep a list of (raw_value, canonical) for an efficient backfill update.
    backfill_pairs: List[Tuple[str, str]] = []

    for raw in raw_values:
        suggestion = _suggest_canonical_for_raw(raw, vocab)
        canonical = suggestion.get("suggested")  # type: ignore
        score = int(suggestion.get("score") or 0)  # type: ignore

        if not canonical or score < threshold:
            skipped += 1
            continue

        canonical_role = str(canonical).strip()
        if not canonical_role:
            skipped += 1
            continue

        mapping = JobRoleMapping.query.filter_by(raw_value=raw).first()
        if mapping is None:
            mapping = JobRoleMapping(raw_value=raw, canonical_role=canonical_role)
            db.session.add(mapping)
        else:
            mapping.canonical_role = canonical_role

        mapped += 1
        if apply_now:
            backfill_pairs.append((raw, canonical_role))

    if apply_now and backfill_pairs:
        # Backfill existing JobRecord rows. Prefer job_role_group if available.
        if hasattr(JobRecord, "job_role_group"):
            for raw, canonical_role in backfill_pairs:
                db.session.query(JobRecord).filter(JobRecord.job_role == raw).update(
                    {JobRecord.job_role_group: canonical_role},
                    synchronize_session=False,
                )
        else:
            for raw, canonical_role in backfill_pairs:
                db.session.query(JobRecord).filter(JobRecord.job_role == raw).update(
                    {JobRecord.job_role: canonical_role},
                    synchronize_session=False,
                )

    db.session.commit()

    if mapped and skipped:
        flash(
            f"Auto-clean mapped {mapped} role(s). Skipped {skipped} below the {threshold}% confidence threshold.",
            "success",
        )
    elif mapped:
        flash(f"Auto-clean mapped {mapped} role(s) (threshold {threshold}%).", "success")
    else:
        flash(
            f"No roles were auto-mapped. Try lowering the threshold (currently {threshold}%).",
            "info",
        )

    return redirect(
        url_for(
            "dashboard.admin_job_roles",
            q=q_param,
            status=status_param,
            canonical_search=canonical_param,
        )
    )


@bp.route("/admin/job-roles/ai-suggest", methods=["POST"])
@login_required
def admin_job_roles_ai_suggest():
    """
    Lightweight AI helper:
      - Reuses existing JobRoleMapping as a cache (no cost).
      - Falls back to our rules + fuzzy logic (no cost).
      - Only calls OpenAI if heuristics are low-confidence.
    Returns JSON:
      { ok, canonical_role, score, source, model, reason }
    """
    data = request.get_json(silent=True) or {}
    raw_value = (data.get("raw_value") or "").strip()

    if not raw_value:
        return jsonify({"ok": False, "error": "No raw job title provided."}), 400

    # 1) If we already have a mapping, treat it as cached and avoid AI entirely
    mapping = JobRoleMapping.query.filter_by(raw_value=raw_value).first()
    if mapping and (mapping.canonical_role or "").strip():
        return jsonify(
            {
                "ok": True,
                "canonical_role": mapping.canonical_role.strip(),
                "score": 100,
                "source": "cache",
                "model": None,
                "reason": "Existing mapping from job_role_mappings used as cache.",
            }
        )

    # 2) Use our deterministic rules + fuzzy matching first (cheap)
    vocab = _build_canonical_vocab()
    suggestion = _suggest_canonical_for_raw(raw_value, vocab)
    heuristic_canonical = (suggestion.get("suggested") or "").strip()  # type: ignore
    heuristic_score = int(suggestion.get("score") or 0)  # type: ignore
    heuristic_source = suggestion.get("source") or "heuristic"  # type: ignore

    # If the heuristic is strong enough, just use that and skip AI
    HEURISTIC_THRESHOLD = 90
    if heuristic_canonical and heuristic_score >= HEURISTIC_THRESHOLD:
        return jsonify(
            {
                "ok": True,
                "canonical_role": heuristic_canonical,
                "score": heuristic_score,
                "source": heuristic_source,
                "model": None,
                "reason": "High-confidence heuristic (rules/fuzzy) – no AI call needed.",
            }
        )

    # 3) Call OpenAI as a last resort
    try:
        from openai import OpenAI  # type: ignore

        client = OpenAI()
    except Exception:
        # OpenAI not installed or not configured
        # Still return the heuristic if we have *something*
        if heuristic_canonical:
            return jsonify(
                {
                    "ok": True,
                    "canonical_role": heuristic_canonical,
                    "score": heuristic_score,
                    "source": heuristic_source,
                    "model": None,
                    "reason": "OpenAI client not available; returned best heuristic match instead.",
                }
            )
        return jsonify(
            {
                "ok": False,
                "error": "AI client not configured on server and no high-confidence heuristic match was found.",
            }
        ), 500

    # Keep candidate list reasonably small for cost
    candidate_roles = _build_canonical_vocab()[:60]
    bullets = "\n".join(f"- {r}" for r in candidate_roles)

    system_prompt = (
        "You are a data cleaning assistant for UK social care job adverts.\n"
        "Your job is to map messy raw job titles into a clean, standardised canonical job role.\n"
        "Only choose from the provided canonical roles list. If nothing fits, return an empty string.\n"
        "Be conservative and aim for accuracy over recall."
    )

    user_prompt = (
        f'Raw job title: "{raw_value}"\n\n'
        "Candidate canonical roles:\n"
        f"{bullets}\n\n"
        "Return a SINGLE JSON object with keys:\n"
        '  - "canonical_role": either one of the candidate roles above, or "" if none is suitable\n'
        '  - "confidence": integer 0–100 reflecting how confident you are in the mapping\n'
        '  - "reason": a short explanation (max 2 sentences)\n'
        "Do not include any extra text, only valid JSON."
    )

    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=200,
            temperature=0.1,
        )
        text = completion.choices[0].message.content or ""
    except Exception:
        # If AI call fails, fall back to heuristic if we have anything
        if heuristic_canonical:
            return jsonify(
                {
                    "ok": True,
                    "canonical_role": heuristic_canonical,
                    "score": heuristic_score,
                    "source": heuristic_source,
                    "model": None,
                    "reason": "AI backend error; returned best heuristic match instead.",
                }
            )
        return jsonify(
            {
                "ok": False,
                "error": "AI backend error and no high-confidence heuristic match was found.",
            }
        ), 500

    # Try to extract JSON from the AI response
    try:
        # Handle possible code fences
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1:
            json_str = text[start : end + 1]
        else:
            json_str = text

        payload = __import__("json").loads(json_str)
    except Exception:
        # If parsing fails, again fall back to heuristic if possible
        if heuristic_canonical:
            return jsonify(
                {
                    "ok": True,
                    "canonical_role": heuristic_canonical,
                    "score": heuristic_score,
                    "source": heuristic_source,
                    "model": "gpt-4o-mini",
                    "reason": "AI response was not valid JSON; returned best heuristic match instead.",
                }
            )
        return jsonify(
            {
                "ok": False,
                "error": "AI response was not valid JSON and no high-confidence heuristic match was found.",
            }
        ), 500

    canonical_role = (payload.get("canonical_role") or "").strip()
    confidence = int(payload.get("confidence") or 0)
    reason = (payload.get("reason") or "").strip()

    # If AI says "none suitable", surface that gently
    if not canonical_role or canonical_role not in candidate_roles:
        # Still return ok=True so UI can show the explanation
        return jsonify(
            {
                "ok": True,
                "canonical_role": "",
                "score": confidence,
                "source": "ai",
                "model": "gpt-4o-mini",
                "reason": reason
                or "AI could not confidently map this title to any canonical role.",
            }
        )

    return jsonify(
        {
            "ok": True,
            "canonical_role": canonical_role,
            "score": confidence,
            "source": "ai",
            "model": "gpt-4o-mini",
            "reason": reason,
        }
    )


# ----------------------------------------------------------------------
# Admin: Sector Override Cleaner
# ----------------------------------------------------------------------

@bp.route("/admin/role-sectors")
@login_required
def admin_role_sectors():
    """
    Admin view to map canonical roles (job_role_group) to canonical sectors.
    Focus is: roles currently sitting in sector == "Other" (or missing).
    """
    # Ensure the overrides table exists
    try:
        JobRoleSectorOverride.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    search = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "all").strip().lower()
    if status not in ("all", "with", "without"):
        status = "all"

    only_other = (request.args.get("only_other") or "1").strip()
    only_other = only_other in ("1", "true", "yes", "on")

    # Canonical role: prefer job_role_group, fallback to job_role
    role_expr = func.coalesce(JobRecord.job_role_group, JobRecord.job_role).label("canonical_role")

    q = db.session.query(
        role_expr,
        func.count(JobRecord.id).label("count"),
        func.avg(JobRecord.pay_rate).label("avg_pay"),
        func.min(JobRecord.pay_rate).label("min_pay"),
        func.max(JobRecord.pay_rate).label("max_pay"),
    ).filter(role_expr.isnot(None))

    if only_other:
        q = q.filter(
            (JobRecord.sector.is_(None))
            | (func.trim(JobRecord.sector) == "")
            | (func.lower(func.trim(JobRecord.sector)) == "other")
        )

    if search:
        pattern = f"%{search}%"
        q = q.filter(role_expr.ilike(pattern))

    if status == "with":
        q = q.join(JobRoleSectorOverride, JobRoleSectorOverride.canonical_role == role_expr)
    elif status == "without":
        q = q.outerjoin(
            JobRoleSectorOverride,
            JobRoleSectorOverride.canonical_role == role_expr,
        ).filter(JobRoleSectorOverride.id.is_(None))

    rows = (
        q.group_by(role_expr)
        .order_by(func.count(JobRecord.id).desc())
        .limit(500)
        .all()
    )

    try:
        override_rows = JobRoleSectorOverride.query.order_by(JobRoleSectorOverride.canonical_role).all()
        overrides = {o.canonical_role: o for o in override_rows}
    except Exception:
        overrides = {}

    # Sector dropdown options (existing sectors + "Other")
    sector_opts = [
        v[0]
        for v in db.session.query(JobRecord.sector)
        .filter(JobRecord.sector.isnot(None))
        .distinct()
        .order_by(JobRecord.sector)
        .all()
    ]
    sector_opts = [s for s in sector_opts if (s or "").strip()]
    if "Other" not in sector_opts:
        sector_opts.append("Other")

    return render_template(
        "admin_role_sectors.html",
        rows=rows,
        overrides=overrides,
        sector_options=sector_opts,
        search=search,
        status=status,
        only_other=only_other,
    )


@bp.route("/admin/role-sectors/map", methods=["POST"])
@login_required
def admin_role_sectors_map():
    canonical_role = (request.form.get("canonical_role") or "").strip()
    canonical_sector = (request.form.get("canonical_sector") or "").strip()

    q_param = (request.form.get("q") or "").strip()
    status_param = (request.form.get("status") or "all").strip().lower()
    only_other_param = (request.form.get("only_other") or "1").strip()

    if not canonical_role or not canonical_sector:
        flash("Canonical role and canonical sector are required.", "error")
        return redirect(
            url_for("dashboard.admin_role_sectors", q=q_param, status=status_param, only_other=only_other_param)
        )

    try:
        JobRoleSectorOverride.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    ov = JobRoleSectorOverride.query.filter_by(canonical_role=canonical_role).first()
    if ov is None:
        ov = JobRoleSectorOverride(canonical_role=canonical_role, canonical_sector=canonical_sector)
    else:
        ov.canonical_sector = canonical_sector

    db.session.add(ov)
    db.session.commit()

    flash(f"Sector override saved: '{canonical_role}' → '{canonical_sector}'.", "success")
    return redirect(
        url_for("dashboard.admin_role_sectors", q=q_param, status=status_param, only_other=only_other_param)
    )


@bp.route("/admin/role-sectors/bulk-map", methods=["POST"])
@login_required
def admin_role_sectors_bulk_map():
    canonical_roles = request.form.getlist("canonical_roles") or []
    canonical_roles = sorted({(r or "").strip() for r in canonical_roles if (r or "").strip()})

    canonical_sector = (request.form.get("canonical_sector") or "").strip()

    q_param = (request.form.get("q") or "").strip()
    status_param = (request.form.get("status") or "all").strip().lower()
    only_other_param = (request.form.get("only_other") or "1").strip()

    if not canonical_roles:
        flash("Select at least one role before using bulk assign.", "error")
        return redirect(
            url_for("dashboard.admin_role_sectors", q=q_param, status=status_param, only_other=only_other_param)
        )

    if not canonical_sector:
        flash("Canonical sector is required for bulk assignment.", "error")
        return redirect(
            url_for("dashboard.admin_role_sectors", q=q_param, status=status_param, only_other=only_other_param)
        )

    try:
        JobRoleSectorOverride.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    updated = 0
    for role in canonical_roles:
        ov = JobRoleSectorOverride.query.filter_by(canonical_role=role).first()
        if ov is None:
            ov = JobRoleSectorOverride(canonical_role=role, canonical_sector=canonical_sector)
        else:
            ov.canonical_sector = canonical_sector
        db.session.add(ov)
        updated += 1

    db.session.commit()

    flash(f"Bulk sector override applied: {updated} role(s) → '{canonical_sector}'.", "success")
    return redirect(
        url_for("dashboard.admin_role_sectors", q=q_param, status=status_param, only_other=only_other_param)
    )


# ----------------------------------------------------------------------
# Admin: One-off Canonical Label Cleaner
# ----------------------------------------------------------------------

@bp.route("/admin/job-roles/clean-canonical", methods=["POST"])
@login_required
def admin_job_roles_clean_canonical():
    """
    One-off (but safe to re-run) canonical label cleaner.

    It:
      - scans all JobRoleMapping rows
      - identifies labels that look like long AI paragraphs / summaries
      - replaces them with a shorter, job-title-style label via _clean_canonical_label
      - leaves already-clean labels unchanged
    """
    # Same access rules as other admin hygiene tools
    if not getattr(current_user, "is_superuser", None) or not current_user.is_superuser():
        flash("You do not have access to the canonical role cleaner.", "error")
        return redirect(url_for("auth.home"))

    try:
        JobRoleMapping.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    mappings = JobRoleMapping.query.all()
    updated = 0
    skipped = 0

    for m in mappings:
        old = (m.canonical_role or "").strip()
        if not old:
            skipped += 1
            continue

        new = _clean_canonical_label(old)

        # Only write if the helper actually changed the label
        if new and new != old:
            m.canonical_role = new
            updated += 1
        else:
            skipped += 1

    if updated:
        db.session.commit()

    if updated:
        flash(
            f"Canonical label cleaner updated {updated} mapping(s). "
            f"{skipped} left unchanged.",
            "success",
        )
    else:
        flash(
            "Canonical label cleaner did not change any mappings. "
            "Existing labels already look clean.",
            "info",
        )

    return redirect(url_for("dashboard.admin_job_roles_report"))
