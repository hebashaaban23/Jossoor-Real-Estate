import frappe
from frappe.query_builder import Order
from typing import Dict, Any, List, Optional

# ----------------------------- Helpers -----------------------------

CRM_DTYPES = {"CRM Lead": "lead", "CRM Deal": "deal"}
CRM_ROUTE = {"CRM Lead": "Lead", "CRM Deal": "Deal"}


def _has(col: str) -> bool:
    """هل عمود موجود في Notification Log؟"""
    try:
        return frappe.db.has_column("Notification Log", col)
    except Exception:
        return False


def _seen_column_name() -> Optional[str]:
    """اسم عمود حالة القراءة في Notification Log (seen أو read)"""
    if _has("seen"):
        return "seen"
    if _has("read"):
        return "read"
    return None


def _bool_seen(row: Dict[str, Any], seen_col: Optional[str]) -> bool:
    if not seen_col:
        return False
    return bool(row.get(seen_col, 0))


def _text_from_nlog(row: Dict[str, Any]) -> str:
    txt = (row.get("subject") or "").strip()
    if not txt:
        txt = (row.get("email_content") or "").strip()
    return frappe.utils.strip_html(txt) or "Notification"


def _looks_like_reminder(row: Dict[str, Any]) -> bool:
    """تعريف مرن للـ Reminder: لو النوع Reminder/Alert أو العنوان فيه remind"""
    t = (row.get("type") or "").lower()
    subj = (row.get("subject") or "").lower()
    return t in {"reminder", "alert"} or ("remind" in subj)


def _map_ref_doctype(dt: Optional[str]) -> Optional[str]:
    return CRM_DTYPES.get(dt) if dt else None


def _map_route(dt: Optional[str]) -> Optional[str]:
    return CRM_ROUTE.get(dt) if dt else None



def _nlog_to_portal_dict(row: Dict[str, Any], seen_col: Optional[str]) -> Dict[str, Any]:
    ref_dt = row.get("document_type")
    ref_name = row.get("document_name")

    reference_doctype = _map_ref_doctype(ref_dt)
    route_name = _map_route(ref_dt)

    to_user = row.get("for_user") or row.get("owner")

    is_read = _bool_seen(row, seen_col)

    return {
        "id": row.get("name"),  # id عام
        "name": row.get("name"),
        "creation": row.get("creation"),
        "from_user": {
            "name": row.get("owner"),
            "full_name": frappe.get_value("User", row.get("owner"), "full_name"),
        },
        "type": "reminder" if _looks_like_reminder(row) else (row.get("type") or "system"),
        

        "to_user": to_user,
        "read": is_read,
        "unread": not is_read,
        "hash": "#reminder" if _looks_like_reminder(row) else "",
        "notification_text": _text_from_nlog(row),
        "notification_type_doctype": ref_dt,
        "notification_type_doc": ref_name,
        "reference_doctype": reference_doctype,  # 'lead' | 'deal' | None
        "reference_name": ref_name,
        "route_name": route_name,                # 'Lead' | 'Deal' | None
        "source": "Notification Log",
    }





def get_hash(n):
    _hash = ""
    if n.type == "Mention" and n.notification_type_doc:
        _hash = "#" + n.notification_type_doc
    if n.type == "WhatsApp":
        _hash = "#whatsapp"
    if n.type == "Assignment" and n.notification_type_doctype == "CRM Task":
        _hash = "#tasks"
        if "has been removed by" in getattr(n, "message", ""):
            _hash = ""
    return _hash


# ----------------------- Unseen Count -----------------------

def _get_unseen_count_for(user: str) -> int:
    """يحسب إجمالي غير المقروء للمستخدم من Notification Log + CRM Notification (لو موجودة)."""
    total = 0

    # Notification Log
    seen_col = _seen_column_name()
    fields = ["name"]
    if seen_col:
        fields.append(seen_col)

    rows = frappe.get_all(
        "Notification Log",
        filters={"for_user": user},
        fields=fields,
        order_by="creation desc",
        limit_page_length=500,
        ignore_permissions=True,
        as_list=False,
    )
    for r in rows:
        if not _bool_seen(r, seen_col):
            total += 1

    # CRM Notification (legacy)
    if frappe.db.table_exists("CRM Notification"):
        total += frappe.db.count("CRM Notification", {"to_user": user, "read": 0})

    return total


@frappe.whitelist()
def get_unseen_count() -> int:
    return _get_unseen_count_for(frappe.session.user)


@frappe.whitelist()
def get_unread_count() -> int:
    """Alias للتوافق الخلفي."""
    return get_unseen_count()


# ----------------------- Realtime helpers -----------------------

def _broadcast_count(user: str):
    """يذيع العدد الحالي للمستخدم."""
    frappe.publish_realtime(
        event="crm_portal_notification",
        message={"type": "count", "unseen": _get_unseen_count_for(user)},
        user=user,
        after_commit=True,
    )


# ----------------------- New Portal Endpoints -----------------------

@frappe.whitelist()
def list_portal_notifications(
    limit: int = 20,
    include_legacy: int = 1,
    unread_only: int = 0,
    before: Optional[str] = None,
):
    """
    يرجّع إشعارات المستخدم من Notification Log (+ اختياري CRM Notification).
    - unread_only = 1 → يرجع فقط الغير مقروءة
    - before = ISO datetime string → يرجع الإشعارات الأقدم من التاريخ ده (لـ pagination)
    """
    user = frappe.session.user
    seen_col = _seen_column_name()

    fields = [
        "name",
        "subject",
        "email_content",
        "creation",
        "type",
        "document_type",
        "document_name",
        "for_user",
        "owner",
    ]
    if _has("from_user"):
        fields.append("from_user")
    if seen_col:
        fields.append(seen_col)

    filters = {}
    if before:
        filters["creation"] = ["<", before]

    if unread_only and seen_col:
        filters[seen_col] = 0

    filters["for_user"] = user

    base_rows = frappe.get_all(
        "Notification Log",
        filters=filters,
        fields=fields,
        order_by="creation desc",
        limit_page_length=max(limit, 200),
        ignore_permissions=True,
        as_list=False,
    )

    out: List[Dict[str, Any]] = [_nlog_to_portal_dict(r, seen_col) for r in base_rows]

    if include_legacy:
        legacy = _list_crm_notifications(limit=max(limit, 200))
        out.extend(legacy)

    out.sort(key=lambda x: x.get("creation") or frappe.utils.now_datetime(), reverse=True)
    return out[:limit]


@frappe.whitelist()
def mark_portal_seen(name: str, source: str = "Notification Log"):
    """تعليم إشعار كمقروء من Notification Log أو CRM Notification + بثّ realtime لتحديث العدّاد."""
    if not name:
        frappe.throw("Notification name is required")

    user = frappe.session.user

    if source == "Notification Log":
        seen_col = _seen_column_name()
        if seen_col:
            frappe.db.set_value("Notification Log", name, seen_col, 1)
        else:
            doc = frappe.get_doc("Notification Log", name)
            setattr(doc, "seen", 1)
            doc.save(ignore_permissions=True)
        _broadcast_count(user)
        return {"ok": True}

    if source == "CRM Notification":
        d = frappe.get_doc("CRM Notification", name)
        d.read = True
        d.save(ignore_permissions=True)
        _broadcast_count(user)
        return {"ok": True}

    frappe.throw(f"Unknown source: {source}")



@frappe.whitelist()
def mark_all_portal_seen(source: str = "Notification Log"):
    """
    تعليم كل إشعارات المستخدم كمقروءة (Notification Log / CRM Notification).
    مفيدة للـ mobile لما يعمل 'Mark all as read'.
    """
    user = frappe.session.user
    updated = 0

    if source == "Notification Log":
        seen_col = _seen_column_name()
        if not seen_col:
            frappe.throw("No 'seen' or 'read' column defined on Notification Log")

        names = frappe.get_all(
            "Notification Log",
            filters={"for_user": user, seen_col: 0},
            pluck="name",
            limit_page_length=1000,
        )
        for name in names:
            frappe.db.set_value("Notification Log", name, seen_col, 1)
        updated += len(names)

    elif source == "CRM Notification" and frappe.db.table_exists("CRM Notification"):
        names = frappe.get_all(
            "CRM Notification",
            filters={"to_user": user, "read": 0},
            pluck="name",
            limit_page_length=1000,
        )
        for name in names:
            frappe.db.set_value("CRM Notification", name, "read", 1)
        updated += len(names)

    _broadcast_count(user)
    return {"ok": True, "updated": updated}


# -------------------- Backward-compatible APIs ---------------------

@frappe.whitelist()
def list_logs(limit: int = 30):
    """
    نسخة خام من Notification Log مع توحيد seen.
    ترجع فقط السجلات المرتبطة بالمستخدم الحالي (for_user/owner/from_user).
    """
    user = frappe.session.user
    seen_col = _seen_column_name()

    fields = [
        "name",
        "subject",
        "email_content",
        "creation",
        "type",
        "document_type",
        "document_name",
        "for_user",
        "owner",
    ]
    if _has("from_user"):
        fields.append("from_user")
    if seen_col:
        fields.append(seen_col)

    rows = frappe.get_all(
        "Notification Log",
        filters={"for_user": user},
        fields=fields,
        order_by="creation desc",
        limit_page_length=limit,
        ignore_permissions=True,
        as_list=False,
    )

    out = []
    for r in rows:
        d = dict(r)
        d["seen"] = _bool_seen(r, seen_col)
        if seen_col in d:
            d.pop(seen_col, None)
        out.append(d)
    return out


@frappe.whitelist()
def mark_seen(name: str):
    """تعليم إشعار Notification Log كمقروء (توافق مع seen/read) + بثّ realtime."""
    if not name:
        frappe.throw("Notification name is required")

    seen_col = _seen_column_name()
    if seen_col:
        frappe.db.set_value("Notification Log", name, seen_col, 1)
    else:
        doc = frappe.get_doc("Notification Log", name)
        setattr(doc, "seen", 1)
        doc.save(ignore_permissions=True)

    _broadcast_count(frappe.session.user)
    return {"ok": True}




def _list_crm_notifications(limit: int = 50) -> List[Dict[str, Any]]:
    if not frappe.db.table_exists("CRM Notification"):
        return []

    Notification = frappe.qb.DocType("CRM Notification")
    query = (
        frappe.qb.from_(Notification)
        .select("*")
        .where(Notification.to_user == frappe.session.user)
        .orderby("creation", order=Order.desc)
    )
    notifications = query.run(as_dict=True)

    out = []
    for n in notifications[:limit]:
        is_read = bool(n.read)
        out.append(
            {
                "id": n.name,
                "name": n.name,
                "creation": n.creation,
                "from_user": {
                    "name": n.from_user,
                    "full_name": frappe.get_value("User", n.from_user, "full_name"),
                },
                "type": n.type,
                "to_user": n.to_user,
                "read": is_read,
                "unread": not is_read,
                "hash": get_hash(n),
                "notification_text": n.notification_text,
                "notification_type_doctype": n.notification_type_doctype,
                "notification_type_doc": n.notification_type_doc,
                "reference_doctype": (
                    "deal" if n.reference_doctype == "CRM Deal" else "lead"
                ),
                "reference_name": n.reference_name,
                "route_name": (
                    "Deal" if n.reference_doctype == "CRM Deal" else "Lead"
                ),
                "source": "CRM Notification",
            }
        )
    return out


@frappe.whitelist()
def get_notifications():
    """الإصدار القديم (يقرأ فقط CRM Notification)."""
    return _list_crm_notifications()


@frappe.whitelist()
def mark_as_read(user=None, doc=None):
    """تعليم إشعارات CRM Notification كمقروء (تاريخيًا) + بثّ realtime."""
    user = user or frappe.session.user
    filters = {"to_user": user, "read": False}
    or_filters = []
    if doc:
        or_filters = [{"comment": doc}, {"notification_type_doc": doc}]
    for k in frappe.get_all("CRM Notification", filters=filters, or_filters=or_filters):
        d = frappe.get_doc("CRM Notification", k.name)
        d.read = True
        d.save(ignore_permissions=True)
    _broadcast_count(user)
    return True


# ----------------------- Broadcast on insert -----------------------

def broadcast_log_realtime(doc, method=None):
    """
    يُستدعى من doc_events بعد إدراج Notification Log:
    يبُثّ حدث realtime لتحديث عدّاد البورتال.
    """
    target_user = getattr(doc, "for_user", None) or getattr(doc, "owner", None)
    if not target_user:
        return
    _broadcast_count(target_user)



@frappe.whitelist()
def notifications_overview(
    limit: int = 20,
    include_legacy: int = 1,
    unread_only: int = 0,
    before: Optional[str] = None,
):
    """
    يرجّع:
    - unseen_count
    - items (نفس فورمات list_portal_notifications)
    """
    user = frappe.session.user
    items = list_portal_notifications(
        limit=limit,
        include_legacy=include_legacy,
        unread_only=unread_only,
        before=before,
    )
    return {
        "user": user,
        "unseen_count": _get_unseen_count_for(user),
        "items": items,
    }


# ----------------------- Assignment API (for Mobile) -----------------------

@frappe.whitelist(methods=["POST"])
def assign_doc(doctype: str, name: str, assign_to: str, description: str = ""):
    """
    Create an official Frappe assignment (ToDo) and generate Notification Log.
    This is the correct way to make "assignment notifications" fire.
    """
    if not doctype or not name or not assign_to:
        frappe.throw("doctype, name, and assign_to are required")

    # assign_to may come as comma-separated string
    users = [u.strip() for u in (assign_to or "").split(",") if u.strip()]
    if not users:
        frappe.throw("assign_to is empty")

    from frappe.desk.form.assign_to import add as assign_add

    assign_add({
        "assign_to": users,
        "doctype": doctype,
        "name": name,
        "description": description or "New assignment",
        "notify": 1,  # IMPORTANT: creates Notification Log / in-app notification
    })

    return {"ok": True, "assigned_to": users, "doctype": doctype, "name": name}
