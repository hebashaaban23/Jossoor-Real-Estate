# jossoor/crm/api/leads.py
import frappe

ALLOWED_FIELDS = [
    "name", "lead_name", "status", "source", "campaign", "project",
    "single_unit", "mobile_no", "email", "lead_owner", "owner",
    "last_contacted", "creation", "modified", "fb_campaign_name",
    "fb_adset_id", "fb_ad_id"
]

def _safe_fields(fields):
    if not fields:
        return ALLOWED_FIELDS
    # خُد التقاطع بس علشان الأمان
    ok = [f for f in fields if f in ALLOWED_FIELDS]
    return ok or ALLOWED_FIELDS

@frappe.whitelist()
def search_leads(
    query: str | None = None,
    status: str | None = None,
    source: str | None = None,
    campaign: str | None = None,
    project: str | None = None,
    owner: str | None = None,
    limit: int = 10,
    fields: list[str] | None = None
):
    """
    بحث مرن عن الـ Leads:
    - query: يُطابق أيًا من (lead_name / mobile_no / email / fb_campaign_name / fb_adset_id / fb_ad_id)
    - فلاتر اختيارية: status, source, campaign, project, owner
    """
    fields = _safe_fields(fields)

    filters = []
    if status:   filters.append(["CRM Lead", "status", "=", status])
    if source:   filters.append(["CRM Lead", "source", "=", source])
    if campaign: filters.append(["CRM Lead", "campaign", "=", campaign])
    if project:  filters.append(["CRM Lead", "project", "=", project])
    if owner:    filters.append(["CRM Lead", "owner", "=", owner])

    or_filters = []
    if query:
        like = f"%{query}%"
        or_filters = [
            ["CRM Lead", "lead_name", "like", like],
            ["CRM Lead", "mobile_no", "like", like],
            ["CRM Lead", "email", "like", like],
            ["CRM Lead", "fb_campaign_name", "like", like],
            ["CRM Lead", "fb_adset_id", "like", like],
            ["CRM Lead", "fb_ad_id", "like", like],
        ]

    data = frappe.get_list(
        "CRM Lead",
        fields=fields,
        filters=filters,
        or_filters=or_filters,
        order_by="creation desc",
        limit=limit,
        as_list=False
    )
    return {"count": len(data), "data": data}
