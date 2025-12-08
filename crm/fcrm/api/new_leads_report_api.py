import frappe
from frappe import _

from crm.fcrm.report.new_leads_summary.new_leads_summary import execute

@frappe.whitelist()
def get_new_leads_report(from_date=None, to_date=None, owner=None):
    """
    Returns the same data as the 'New Leads Summary' report,
    filtered by from_date / to_date / owner.
    """

    # call the same execute() you already tested
    cols, rows = execute({
        "from_date": from_date,
        "to_date": to_date,
        "owner": owner,
    })

    # make it easy for frontend / Excel export
    return {
        "columns": cols,
        "rows": rows,
        "filters_used": {
            "from_date": from_date,
            "to_date": to_date,
            "owner": owner,
        },
    }

