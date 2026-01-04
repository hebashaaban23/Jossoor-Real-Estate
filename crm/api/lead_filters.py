# apps/crm/crm/api/lead_filters.py
from __future__ import annotations
import frappe
from frappe import _

# ---------- Helpers ----------
def _has_field(doctype: str, fieldname: str) -> bool:
    try:
        meta = frappe.get_meta(doctype)
        return any(df.fieldname == fieldname for df in meta.fields)
    except Exception:
        return False

def _first_existing_field(doctype: str, candidates: list[str]) -> str | None:
    for f in candidates:
        if f and _has_field(doctype, f):
            return f
    return None

def _first_existing_doctype(candidates: list[str]) -> str | None:
    """
    Robust & permission-safe: try loading meta for each candidate.
    Returns the first doctype whose meta can be loaded.
    """
    for dt in candidates:
        try:
            frappe.get_meta(dt)
            return dt
        except Exception:
            continue
    return None

def _link_opts(doctype_candidates: list[str], title_field_candidates: list[str] | None = None, limit: int = 500):
    dt = _first_existing_doctype(doctype_candidates)
    if not dt:
        return []
    if title_field_candidates:
        title_field = _first_existing_field(dt, title_field_candidates) or "name"
    else:
        title_field = "name"

    rows = frappe.get_all(dt, fields=["name", title_field], limit_page_length=limit, order_by="modified desc")
    out = []
    for r in rows:
        name = r.get("name")
        label = r.get(title_field) or name
        if name:
            out.append({"value": name, "label": label})
    return out

def _distinct_from_lead(fieldname: str, limit: int = 500):
    if not fieldname:
        return []
    rows = frappe.get_all(
        "CRM Lead",
        fields=[f"distinct {fieldname} as name"],
        filters=[[fieldname, "!=", ""]],
        limit_page_length=limit,
        order_by=f"{fieldname} asc",
    )
    out = []
    for r in rows:
        v = (r.get("name") or "").strip()
        if v:
            out.append({"value": v, "label": v})
    return out

# ---------- Main API ----------
@frappe.whitelist()
def lead_filter_options():
    """
    Returns:
    {
      status, project, territory, lead_source, lead_origin, lead_type: [{value,label}],
      last_contact_field, location_field, has_budget, has_space
    }
    Smartly detects field names and doctypes across forks.
    """
    lead_dt = "CRM Lead"

    # Field synonyms across forks
    status_field     = _first_existing_field(lead_dt, ["status", "lead_status", "crm_lead_status"])
    project_field    = _first_existing_field(lead_dt, ["project", "real_estate_project", "crm_project"])
    location_field   = _first_existing_field(lead_dt, ["territory", "location", "crm_territory"])
    last_contact_fld = _first_existing_field(lead_dt, ["last_contacted_on", "last_contacted", "last_contact_date", "last_contact"])
    budget_field     = _first_existing_field(lead_dt, ["budget", "expected_budget", "estimated_budget"])
    space_field      = _first_existing_field(lead_dt, ["space", "area", "unit_area", "sqm"])

    source_field     = _first_existing_field(lead_dt, ["lead_source", "source"])
    origin_field     = _first_existing_field(lead_dt, ["lead_origin"])
    type_field       = _first_existing_field(lead_dt, ["lead_type", "type"])

    # Option sources (DocType synonyms)
    status_opts    = _link_opts(["CRM Lead Status", "Lead Status"])
    project_opts   = _link_opts(["Real Estate Project", "Project"])
    territory_opts = _link_opts(["CRM Territory", "Territory"])
    source_opts    = _link_opts(["CRM Lead Source", "Lead Source"])
    origin_opts    = _link_opts(["CRM Lead Origin", "Lead Origin"])
    type_opts      = _link_opts(["CRM Lead Type", "Lead Type"])

    # Fallback to distinct values from Lead if link doctypes don't exist
    if not status_opts and status_field:
        status_opts = _distinct_from_lead(status_field)
    if not project_opts and project_field:
        project_opts = _distinct_from_lead(project_field)
    if not territory_opts and location_field:
        territory_opts = _distinct_from_lead(location_field)
    if not source_opts and source_field:
        source_opts = _distinct_from_lead(source_field)
    if not origin_opts and origin_field:
        origin_opts = _distinct_from_lead(origin_field)
    if not type_opts and type_field:
        type_opts = _distinct_from_lead(type_field)

    return {
        "status": status_opts,
        "project": project_opts,
        "territory": territory_opts,
        "lead_source": source_opts,
        "lead_origin": origin_opts,
        "lead_type": type_opts,
        "last_contact_field": last_contact_fld or "last_contacted",
        "location_field": location_field,
        "has_budget": bool(budget_field),
        "has_space": bool(space_field),
    }


@frappe.whitelist()
def drawer_options():
    """
    Simplified version for drawer UI - returns arrays of string values.
    Returns:
    {
      projects: [str],
      locations: [str],
      source: [str],
      lead_origin: [str],
      lead_type: [str]
    }
    """
    opts = lead_filter_options()
    
    return {
        "projects": [item["value"] for item in opts.get("project", [])],
        "locations": [item["value"] for item in opts.get("territory", [])],
        "source": [item["value"] for item in opts.get("lead_source", [])],
        "lead_origin": [item["value"] for item in opts.get("lead_origin", [])],
        "lead_type": [item["value"] for item in opts.get("lead_type", [])],
    }
