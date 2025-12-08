import frappe

@frappe.whitelist(allow_guest=False)
def is_sales_user():
    """
    Returns True if the current logged-in portal user is a Sales user,
    but treat System Managers and Administrator as NOT Sales (so they see Activity).
    """
    sales_role_names = {"Sales User", "Sales", "Salesman", "Salesperson"}

    try:
        user = frappe.session.user
        roles = set(frappe.get_roles(user) or [])

        # Exempt full admins from being considered "sales"
        if "System Manager" in roles or user in ("Administrator", "admin", "Admin"):
            return False

        return bool(roles & sales_role_names)
    except Exception:
        # On error, do NOT treat as sales (safer for admin visibility)
        return False
