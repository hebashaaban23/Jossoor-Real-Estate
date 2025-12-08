import frappe
from frappe.utils import getdate
from crm.fcrm.report.new_leads_summary.new_leads_summary import get_lead_counts

@frappe.whitelist()
def total_new_leads(from_date=None, to_date=None, owner=None):
    """
    Returns just the total count of new CRM Leads in the given range,
    optionally filtered by owner.
    This can be called from JS, dashboard widget, etc.
    """
    if from_date:
        from_date = getdate(from_date)
    if to_date:
        to_date = getdate(to_date)

    rows = get_lead_counts(from_date, to_date, owner)

    total = sum(r["lead_count"] for r in rows)
    return {
        "from_date": str(from_date) if from_date else None,
        "to_date": str(to_date) if to_date else None,
        "owner": owner,
        "total_new_leads": total,
        "daily_breakdown": rows,
    }
