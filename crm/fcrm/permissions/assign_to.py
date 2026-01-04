# apps/crm/crm/permissions/assign.py
from __future__ import annotations

from typing import List, Set
import frappe
from frappe import _


# ---- reuse the same heuristics you used in lead.py ----
def _member_user_col() -> tuple[str, str]:
    """Return (child_doctype_name, user_link_fieldname). Defaults to ('Member','user')."""
    try:
        meta = frappe.get_meta("Member", cached=True)
        for f in meta.get("fields", []):
            if getattr(f, "fieldtype", None) == "Link" and getattr(f, "options", None) == "User":
                return "Member", f.fieldname
        for alt in ("user", "member", "user_id", "user_email", "allocated_to"):
            if meta.get_field(alt):
                return "Member", alt
    except Exception:
        pass
    return "Member", "user"


# -----------------------------
# v15 compatible role check
# -----------------------------
def _has_role(role: str, user: str | None = None) -> bool:
    user = user or frappe.session.user
    try:
        return bool(frappe.has_role(role, user=user))
    except TypeError:
        # fallback for older signatures
        return role in (frappe.get_roles(user) or [])


def _is_privileged(user: str | None = None) -> bool:
    user = user or frappe.session.user
    # kept for compatibility/logging, but OPEN policy doesn't rely on it
    return user in ("Administrator",) or _has_role("System Manager", user=user)


def _is_sales_master_manager(user: str | None = None) -> bool:
    """Check if user has Sales Master Manager role."""
    user = user or frappe.session.user
    return _has_role("Sales Master Manager", user=user)


def _is_sales_manager(user: str | None = None) -> bool:
    user = user or frappe.session.user
    return _has_role("Sales Manager", user=user)


def _is_sales_user(user: str | None = None) -> bool:
    user = user or frappe.session.user
    return _has_role("Sales User", user=user)


def _is_team_leader(user: str | None = None) -> bool:
    """Check if user is a Team Leader (has a Team where they are team_leader)."""
    user = user or frappe.session.user
    return bool(frappe.db.exists("Team", {"team_leader": user}))


def _is_team_member(user: str | None = None) -> bool:
    """Check if user is a Team Member (exists in Member child table but not as team_leader)."""
    user = user or frappe.session.user
    if _is_team_leader(user):
        return False  # Team Leader is not considered a Team Member for this purpose
    member_dt, member_col = _member_user_col()
    return bool(frappe.db.exists(member_dt, {member_col: user, "parenttype": "Team"}))


def _team_members_of(team_leader: str) -> Set[str]:
    """Return set of Users that belong to the Team led by team_leader."""
    team = frappe.db.get_value("Team", {"team_leader": team_leader}, "name")
    if not team:
        return set()
    member_dt, member_col = _member_user_col()
    rows = frappe.get_all(member_dt, filters={"parent": team, "parenttype": "Team"}, pluck=member_col)
    return set(filter(None, rows or []))


@frappe.whitelist()
def get_assignable_users(doctype: str = "", name: str = "") -> list[dict]:
    """
    OPEN MODE:
    Any user can assign to any enabled user.
    Returns all enabled users (no role/team restrictions).

    Args:
        doctype: Document type (optional)
        name: Document name (optional)
    """
    me = frappe.session.user
    frappe.logger().info(f"[OPEN] get_assignable_users called for user: {me}, doctype: {doctype}, name: {name}")

    return frappe.get_all(
        "User",
        filters={"enabled": 1},
        fields=["name", "full_name", "user_image"],
        order_by="full_name asc",
    )


@frappe.whitelist()
def assign_lead(
    doctype: str,
    name: str,
    users: List[str] | None = None,
    description: str | None = None
):
    """
    OPEN MODE:
    Any user can assign any document to any users.

    NOTE:
    We keep minimal safety: actor must have READ access to the document.
    If you want to remove even that, tell me.
    """
    users = [u for u in (users or []) if u]
    actor = frappe.session.user

    if not doctype or not name:
        frappe.throw(_("doctype and name are required"), frappe.ValidationError)

    # Frappe v15 uses doc= for docname (name= isn't accepted in some versions)
    if not frappe.has_permission(doctype, "read", doc=name):
        frappe.throw(_("Not permitted to access this document"), frappe.PermissionError)

    if not users:
        frappe.throw(_("users is required"), frappe.ValidationError)

    from frappe.desk.form.assign_to import add as add_assignment

    for u in users:
        add_assignment({
            "doctype": doctype,
            "name": name,
            "assign_to": [u],
            "description": description or "",
            "notify": 1,
        })

    frappe.logger().info(f"[OPEN] {actor} assigned {doctype} {name} to {users}")
    return {"ok": True, "assigned_to": users}


def validate_todo_assignment(doc, method=None):
    """
    OPEN MODE:
    Do not block any ToDo assignment.
    """
    return






