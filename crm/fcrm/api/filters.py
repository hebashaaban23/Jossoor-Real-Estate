import json
from typing import List, Dict, Any

import frappe
from pypika import Order
from frappe.query_builder import functions as fn  # <-- for Count, etc.

# ---------- helpers ----------

def _coerce(val, fieldtype):
    return frappe.utils.getdate(val) if fieldtype == "Date" and isinstance(val, str) and val else val

def _has_field(doctype: str, fieldname: str) -> bool:
    try:
        return any(df.fieldname == fieldname for df in frappe.get_meta(doctype).fields)
    except Exception:
        return False

def _existing_fields(doctype: str, candidates: List[str]) -> List[str]:
    return [f for f in candidates if _has_field(doctype, f)]

def _apply_dynamic_filters(q, doctype: str, items: List[Dict[str, Any]]):
    """
    items = [{fieldname, op, value, fieldtype?}, ...]
    """
    Doc = frappe.qb.DocType(doctype)
    conds = []
    for it in items or []:
        field = it.get("fieldname")
        op = (it.get("op") or "=").lower()
        val = it.get("value")
        ftype = it.get("fieldtype")

        if not field or not _has_field(doctype, field):
            continue
        if val in (None, "", []):
            continue

        col = getattr(Doc, field)

        if op in ("between", "date_between") and isinstance(val, dict):
            start = _coerce(val.get("from"), ftype)
            end = _coerce(val.get("to"), ftype)
            if start and end:
                conds.append(col.between(start, end))
        elif op in ("in", "anyof") and isinstance(val, (list, tuple)):
            conds.append(col.isin(val))
        elif op in ("like", "ilike"):
            conds.append(col.like(f"%{val}%"))
        elif op in (">", "<", ">=", "<=", "!=","="):
            conds.append(getattr(col, {"=":"eq","!=":"ne",
                                       ">":"gt","<":"lt",
                                       ">=":"ge","<=":"le"}[op])(val))
    return conds

# ---------- public API ----------

@frappe.whitelist()
def get_filters_config(doctype: str):
    """
    Build config based on fields that actually exist on your site.
    Adjust the candidate lists as your schema evolves.
    """
    if doctype == "Lead":
        # Your Lead meta shows these exist: lead_name, status, source, lead_owner, email_id,
        # mobile_no, phone, company_name, territory, city, campaign_name
        quick = []
        if _has_field("Lead", "status"):
            quick.append({"label":"Status","fieldname":"status","type":"Select",
                          "options":["New","Qualified","Disqualified","Hot","Follow Up","No Answer","Junk"]})
        if _has_field("Lead", "lead_owner"):
            quick.append({"label":"Lead Owner","fieldname":"lead_owner","type":"Link","target":"User"})

        all_cfg = []
        if _has_field("Lead", "status"):
            all_cfg.append({"label":"Status","fieldname":"status","type":"MultiSelect",
                            "options":["New","Qualified","Disqualified","Hot","Follow Up","No Answer","Junk"]})
        if _has_field("Lead", "city"):
            all_cfg.append({"label":"City","fieldname":"city","type":"Data"})
        if _has_field("Lead", "source"):
            # In your meta, source is a Link; we can still expose as Select or Link. Keep Select with common sources.
            all_cfg.append({"label":"Source","fieldname":"source","type":"Select",
                            "options":["Website","Facebook","Instagram","Walk-in","Referral","Broker"]})

        sort = [{"label":"Creation","fieldname":"creation"},
                {"label":"Modified","fieldname":"modified"}]

        return {"quick": quick, "all": all_cfg, "sort": sort}

    # default
    return {"quick": [], "all": [], "sort": [{"label":"Modified","fieldname":"modified"},
                                             {"label":"Creation","fieldname":"creation"}]}

@frappe.whitelist()
def list_saved_filters(doctype: str):
    user = frappe.session.user
    rows = frappe.get_all(
        "Saved Filter",
        filters={"reference_doctype": doctype},
        or_filters={"owner_user": user, "is_public": 1},
        fields=[
            "name","title","is_public","is_favorite",
            "filters_json","quick_state_json","sort_by","sort_order","limit"
        ],
        order_by="is_favorite desc, modified desc",
    )
    return rows

@frappe.whitelist()
def save_filter(payload: str):
    data = json.loads(payload)
    name = data.get("name")

    if name and frappe.db.exists("Saved Filter", name):
        doc = frappe.get_doc("Saved Filter", name)
        if doc.owner_user and doc.owner_user != frappe.session.user and not doc.is_public:
            frappe.throw("You can edit only your own saved filters unless they are public.")
    else:
        doc = frappe.new_doc("Saved Filter")

    doc.update({
        "title": data["title"],
        "reference_doctype": data["reference_doctype"],
        "filters_json": json.dumps(data.get("filters") or []),
        "quick_state_json": json.dumps(data.get("quick_state") or {}),
        "is_public": int(bool(data.get("is_public"))),
        "is_favorite": int(bool(data.get("is_favorite"))),
        "sort_by": data.get("sort_by"),
        "sort_order": (data.get("sort_order") or "desc").lower(),
        "limit": data.get("limit"),
        "owner_user": frappe.session.user,
    })
    doc.save()
    return {"name": doc.name}

@frappe.whitelist()
def delete_filter(name: str):
    doc = frappe.get_doc("Saved Filter", name)
    if doc.owner_user and doc.owner_user != frappe.session.user and not doc.is_public:
        frappe.throw("You can delete only your own saved filters unless they are public.")
    frappe.delete_doc("Saved Filter", name)

@frappe.whitelist()
def get_leads(filters_json: str = "[]", page: int = 1, page_size: int = 20,
              sort_by: str = "modified", sort_order: str = "desc"):
    items = json.loads(filters_json or "[]")

    Doc = frappe.qb.DocType("Lead")

    # Build SELECT only with fields that exist
    base_fields = ["name", "lead_name", "status", "lead_owner", "email_id", "mobile_no", "phone", "company_name", "territory", "city", "campaign_name", "owner"]
    select_fields = _existing_fields("Lead", base_fields)
    if not select_fields:
        select_fields = ["name"]  # absolute minimum

    q = frappe.qb.from_(Doc).select(*[getattr(Doc, f) for f in select_fields])

    # WHERE
    conds = _apply_dynamic_filters(q, "Lead", items)
    for c in conds:
        q = q.where(c)

    # ORDER BY
    sby = sort_by if (sort_by and (_has_field("Lead", sort_by) or sort_by in ("creation","modified","name"))) else "modified"
    scol = getattr(Doc, sby, Doc.modified)
    sord = Order.desc if (str(sort_order).lower() == "desc") else Order.asc
    q = q.orderby(scol, order=sord)

    # pagination
    page = max(int(page), 1)
    page_size = min(max(int(page_size), 1), 200)
    q = q.limit(page_size).offset((page - 1) * page_size)

    data = q.run(as_dict=True)

    # total (accurate) using QB functions
    cq = frappe.qb.from_(Doc)
    for c in conds:
        cq = cq.where(c)
    total = cq.select(fn.Count("*")).run()[0][0]

    return {"data": data, "page": page, "page_size": page_size, "total": total}
