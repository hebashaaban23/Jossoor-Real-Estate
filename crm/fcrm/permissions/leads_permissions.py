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
    - System Manager: وصول كامل (يرى كل شيء) -> None
    - Sales Manager / Others:
        * يرى الـLeads الخاصة به (المالك Owner).
        * يرى الـLeads المعينة عليه (Assigned To).
        * يرى الـLeads المعينة على فريقه (إذا كان هو Team Leader).
    """
    if not user or user == "Guest":
        return "1=0"

    roles = set(frappe.get_roles(user))
    
    # تحذير: لا تضف Sales Manager هنا لتجنب كشف كل البيانات
    if "System Manager" in roles:
        return None  

    escaped_user = frappe.db.escape(user)

    # 1. المالك (Owner)
    is_owner = f"`tabCRM Lead`.owner = {escaped_user}"

    # 2. المسند إليه (Assigned To Self)
    assigned_self = f"""EXISTS (
        SELECT 1
        FROM `tabToDo` td
        WHERE td.`reference_type` = 'CRM Lead'
          AND td.`reference_name` = `tabCRM Lead`.`name`
          AND td.`allocated_to` = {escaped_user}
          AND td.`status` = 'Open'
    )"""

    # 3. الفريق (Team Leader logic)
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

    # الجمع بين الشروط: المالك OR مسند لي OR مسند لفريقي
    return f"({is_owner} OR {assigned_self} OR {assigned_team})"


# --------------------------------------------------------------------
# Document-Level Permission (Form Open / Read / Write)
# --------------------------------------------------------------------
def has_permission(doc, ptype: str, user: str) -> bool:
    """
    - System Manager: True
    - المالك (Owner): True (هام جداً لتفادي الخطأ عند الإسناد)
    - مسند للمستخدم: True
    - مسند لأحد أعضاء فريق المستخدم: True
    """
    if not user or user == "Guest":
        return False

    roles = set(frappe.get_roles(user))
    
    # 1. System Manager فقط له صلاحية مطلقة
    if "System Manager" in roles:
        return True
    
    # 2. المالك (Owner) - يحل مشكلة اختفاء الصلاحية عند التعديل
    if doc.owner == user:
        return True

    # تحضير قائمة المسند إليهم
    try:
        assigned_list: List[str] = json.loads(doc._assign or "[]")
    except Exception:
        assigned_list = []

    # 3. هل المستخدم مسند إليه مباشرة؟
    if user in assigned_list:
        return True

    # فحص الـ ToDo للتأكد (Fail-safe)
    if frappe.db.exists("ToDo", {
        "reference_type": doc.doctype,
        "reference_name": doc.name,
        "allocated_to": user,
        "status": "Open",
    }):
        return True

    # 4. منطق Team Leader: هل Lead مسند لأي شخص في فريقي؟
    member_dt, member_col = _member_user_col()
    
    # جلب جميع الأعضاء الذين يرأسهم هذا المستخدم
    members = frappe.db.sql_list(
        f"""
        SELECT m.`{member_col}`
        FROM `tab{member_dt}` m
        JOIN `tabTeam` t ON m.`parent` = t.`name`
        WHERE t.`team_leader` = %s
        """,
        (user,),
    ) or []

    # هل أي عضو من فريقي موجود في قائمة الإسناد الحالية؟
    if any(mem and mem in assigned_list for mem in members):
        return True

    # فحص أعمق في جدول ToDo للأعضاء (للحالات المعقدة)
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