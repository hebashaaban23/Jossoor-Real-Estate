# apps/crm/crm/api/payment_plans.py
import frappe

def _existing_fields(doctype: str, requested: list[str]) -> list[str]:
    """Return only fields that exist on the doctype."""
    meta = frappe.get_meta(doctype)
    existing = {df.fieldname for df in meta.fields}
    return [f for f in requested if f in existing]

@frappe.whitelist()
def get_payment_plans_for_lead(lead: str):
    """Return minimal info about plans linked to this lead (robust to schema differences)."""
    if not lead:
        return []

    if not frappe.has_permission("Payment Plan", ptype="read"):
        frappe.throw("Not permitted to read Payment Plan", frappe.PermissionError)

    # Always-safe meta fields
    base_fields = ["name", "modified", "owner"]

    # Optional custom fields — include only if they exist on your site
    optional_fields = _existing_fields(
        "Payment Plan",
        [
            "plan_name",
            "unit_name",          # only if this field exists
            "project_name",       # only if this field exists
            "total_price",
            "years",
            "frequency",
            "installment_type",
        ],
    )

    fields = base_fields + optional_fields

    return frappe.get_all(
        "Payment Plan",
        filters={"lead": lead},
        fields=fields,
        order_by="modified desc",
        limit_page_length=200,
    )
