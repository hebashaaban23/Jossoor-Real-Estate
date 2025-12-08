
from __future__ import annotations
from typing import Optional, Tuple, List
import json
import frappe

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def _member_user_col() -> Tuple[str, str]:
  
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


# --------------------------------------------------------------------
# Query Conditions (List View / get_list)
# --------------------------------------------------------------------
def get_permission_query_conditions(user: str) -> Optional[str]:
    """
    - المدراء (System Manager / Sales Manager): وصول كامل → None
    - غير ذلك:
        * يرى الـLeads المعيَّن عليها هو شخصيًا (ToDo مفتوح).
        * لو Team Leader: يرى كذلك المعيَّن على أي عضو من فريقه (ToDo مفتوح).
    """
    if not user or user == "Guest":
        return "1=0"

    roles = set(frappe.get_roles(user))
    if "System Manager" in roles:
        return None  # Full access

    escaped_user = frappe.db.escape(user)

    assigned_self = f"""EXISTS (
        SELECT 1
        FROM `tabToDo` td
        WHERE td.`reference_type` = 'CRM Lead'
          AND td.`reference_name` = `tabCRM Lead`.`name`
          AND td.`allocated_to` = {escaped_user}
          AND td.`status` = 'Open'
    )"""

    member_dt, member_col = _member_user_col()
    assigned_team = f"""EXISTS (
        SELECT 1
        FROM `tab{member_dt}` m
        JOIN `tabTeam` t ON m.`parent` = t.`name`
        JOIN `tabToDo` td
             ON td.`reference_type` = 'CRM Lead'
            AND td.`reference_name` = `tabCRM Lead`.`name`
            AND td.`status` = 'Open'
        WHERE t.`team_leader` = {escaped_user}
          AND td.`allocated_to` = m.`{member_col}`
    )"""

    return f"({assigned_self} OR {assigned_team})"


# --------------------------------------------------------------------
# Document-Level Permission (Form Open / Read / Write)
# --------------------------------------------------------------------
def has_permission(doc, ptype: str, user: str) -> bool:
    """
    - المدراء: True
    - المستخدِم نفسه في _assign أو عنده ToDo مفتوح: True
    - Team Leader وأي عضو من فريقه في _assign أو عنده ToDo مفتوح: True
    - غير ذلك: False
    """
    if not user or user == "Guest":
        return False

    roles = set(frappe.get_roles(user))
    if "System Manager" in roles:
        return True

    # 1) مُعيَّن مباشرةً (_assign أو ToDo مفتوح)
    try:
        assigned_list: List[str] = json.loads(doc._assign or "[]")
    except Exception:
        assigned_list = []

    if user in assigned_list:
        return True

    if frappe.db.exists(
        "ToDo",
        {
            "reference_type": doc.doctype,
            "reference_name": doc.name,
            "allocated_to": user,
            "status": "Open",
        },
    ):
        return True

    # 2) Team Leader → أعضاء فريقه
    member_dt, member_col = _member_user_col()
    members = frappe.db.sql_list(
        f"""
        SELECT m.`{member_col}`
        FROM `tab{member_dt}` m
        JOIN `tabTeam` t ON m.`parent` = t.`name`
        WHERE t.`team_leader` = %s
        """,
        (user,),
    ) or []

    if any(mem and mem in assigned_list for mem in members):
        return True

    if members:
        in_tuple = tuple(x for x in members if x)
        if in_tuple and frappe.db.sql(
            """
            SELECT 1
            FROM `tabToDo`
            WHERE `reference_type` = %(rt)s
              AND `reference_name` = %(rn)s
              AND `status` = 'Open'
              AND `allocated_to` IN %(members)s
            LIMIT 1
            """,
            {"rt": doc.doctype, "rn": doc.name, "members": in_tuple},
            as_dict=True,
        ):
            return True

    return False








