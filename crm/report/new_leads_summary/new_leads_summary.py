import frappe
from frappe.utils import getdate

def execute(filters=None):
    """
    This is what the Report UI would call.
    We'll also call it manually from bench console.
    """
    if not filters:
        filters = {}

    from_date = filters.get("from_date")
    to_date = filters.get("to_date")
    owner = filters.get("owner")

    # normalize dates
    if from_date:
        from_date = getdate(from_date)
    if to_date:
        to_date = getdate(to_date)

    data_rows = get_lead_counts(from_date, to_date, owner)

    columns = [
        {
            "label": "Date",
            "fieldname": "creation_date",
            "fieldtype": "Date",
            "width": 120,
        },
        {
            "label": "New Leads",
            "fieldname": "lead_count",
            "fieldtype": "Int",
            "width": 120,
        },
    ]

    total_leads = sum(r["lead_count"] for r in data_rows) if data_rows else 0
    data_rows.append({
        "creation_date": "TOTAL",
        "lead_count": total_leads,
    })

    return columns, data_rows


def get_lead_counts(from_date=None, to_date=None, owner=None):
    """
    Return rows like:
    [
      {"creation_date": 2025-10-01, "lead_count": 1203},
      {"creation_date": 2025-10-04, "lead_count": 54},
      ...
    ]
    filtered by date range and optionally by owner (Uploader).
    """
    conditions = []
    params = {}

    if from_date:
        conditions.append("date(creation) >= %(from_date)s")
        params["from_date"] = from_date

    if to_date:
        conditions.append("date(creation) <= %(to_date)s")
        params["to_date"] = to_date

    if owner:
        conditions.append("owner = %(owner)s")
        params["owner"] = owner

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    query = f"""
        SELECT
            date(creation) AS creation_date,
            COUNT(name) AS lead_count
        FROM `tabCRM Lead`
        {where_clause}
        GROUP BY date(creation)
        ORDER BY date(creation) ASC
    """

    return frappe.db.sql(query, params, as_dict=True)
