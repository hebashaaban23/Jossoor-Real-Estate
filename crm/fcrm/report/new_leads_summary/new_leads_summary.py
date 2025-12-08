import frappe
from frappe.utils import getdate

def execute(filters=None):
    filters = filters or {}
    from_date = getdate(filters.get("from_date")) if filters.get("from_date") else None
    to_date = getdate(filters.get("to_date")) if filters.get("to_date") else None

    conditions = ""
    if from_date:
        conditions += f" AND creation >= '{from_date}'"
    if to_date:
        conditions += f" AND creation <= '{to_date}'"

    data = frappe.db.sql(
        f"""
        SELECT DATE(creation) AS creation_date, COUNT(name) AS lead_count
        FROM `tabCRM Lead`
        WHERE 1=1 {conditions}
        GROUP BY DATE(creation)
        ORDER BY creation_date
        """,
        as_dict=True,
    )

    total = sum([d["lead_count"] for d in data])
    data.append({"creation_date": "TOTAL", "lead_count": total})

    columns = [
        {"label": "Date", "fieldname": "creation_date", "fieldtype": "Date", "width": 120},
        {"label": "New Leads", "fieldname": "lead_count", "fieldtype": "Int", "width": 120},
    ]

    return columns, data

