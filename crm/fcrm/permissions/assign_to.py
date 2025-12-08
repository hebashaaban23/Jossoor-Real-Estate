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

def _is_privileged(user: str | None = None) -> bool:
    user = user or frappe.session.user
    return user in ("Administrator",) or frappe.permissions.has_role("System Manager", user=user)

def _is_sales_manager(user: str | None = None) -> bool:
    user = user or frappe.session.user
    return frappe.permissions.has_role("Sales Manager", user=user)

def _is_sales_user(user: str | None = None) -> bool:
    user = user or frappe.session.user
    return frappe.permissions.has_role("Sales User", user=user)

def _team_members_of(team_leader: str) -> Set[str]:
    """Return set of Users that belong to the Team led by team_leader."""
    team = frappe.db.get_value("Team", {"team_leader": team_leader}, "name")
    if not team:
        return set()
    member_dt, member_col = _member_user_col()
    rows = frappe.get_all(member_dt, filters={"parent": team, "parenttype": "Team"}, pluck=member_col)
    return set(filter(None, rows or []))

@frappe.whitelist()
def get_assignable_users(doctype: str, name: str) -> list[dict]:
    """
    - System Manager/Administrator: all enabled users
    - Sales Manager: only members of their Team
    - Sales User (or others): empty
    """
    me = frappe.session.user
    if _is_privileged(me):
        return frappe.get_all("User", filters={"enabled": 1}, fields=["name", "full_name", "user_image"])

    if _is_sales_manager(me):
        allowed = _team_members_of(me)
        if not allowed:
            return []
        return frappe.get_all("User",
                              filters={"enabled": 1, "name": ("in", list(allowed))},
                              fields=["name", "full_name", "user_image"])
    # Sales User and others -> none
    return []

@frappe.whitelist()
def assign_lead(doctype: str, name: str, users: List[str] | None = None, description: str | None = None):
    """
    The ONLY endpoint your UI should call to assign.
    - Sales User: forbidden
    - Sales Manager: only to their team members
    - System Manager / Administrator: allowed
    """
    users = [u for u in (users or []) if u]
    actor = frappe.session.user

    # must at least read the target doc
    if not frappe.has_permission(doctype, "read", name=name):
        frappe.throw(_("Not permitted to access this document"), frappe.PermissionError)

    if _is_privileged(actor):
        pass
    elif _is_sales_manager(actor):
        allowed = _team_members_of(actor)
        illegal = [u for u in users if u not in allowed]
        if illegal:
            frappe.throw(_("You can only assign to your team members: {0}").format(", ".join(illegal)),
                         frappe.PermissionError)
    else:
        # Sales User (and any other roles) -> block
        frappe.throw(_("You are not allowed to assign"), frappe.PermissionError)

    from frappe.desk.form.assign_to import add as add_assignment
    for u in users:
        add_assignment({
            "doctype": doctype,
            "name": name,
            "assign_to": [u],
            "description": description or "",
            "notify": 1,
        })
    return {"ok": True, "assigned_to": users}

def validate_todo_assignment(doc, method=None):
    """
    Guard-rail: prevents creating ToDo assignments that break our policy,
    even if someone calls the low-level API directly.
    """
    # Only police assignments that link to a doc
    if not doc.reference_type or not doc.reference_name or not doc.owner:
        return

    actor = getattr(doc, "assigned_by", None) or frappe.session.user

    if _is_privileged(actor):
        return

    if _is_sales_user(actor):
        frappe.throw(_("Sales Users cannot assign"), frappe.PermissionError)

    if _is_sales_manager(actor):
        allowed = _team_members_of(actor)
        if doc.owner not in allowed:
            frappe.throw(_("Managers can only assign to their team members"), frappe.PermissionError)
