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

    return {
        "creation": row.get("creation"),
        "from_user": {
            "name": row.get("owner"),
            "full_name": frappe.get_value("User", row.get("owner"), "full_name"),
        },
        "type": "reminder" if _looks_like_reminder(row) else (row.get("type") or "system"),
        "to_user": to_user,
        "read": _bool_seen(row, seen_col),
        "hash": "#reminder" if _looks_like_reminder(row) else "",
        "notification_text": _text_from_nlog(row),
        "notification_type_doctype": ref_dt,
        "notification_type_doc": ref_name,
        "reference_doctype": reference_doctype,  # 'lead' | 'deal' | None
        "reference_name": ref_name,
        "route_name": route_name,                # 'Lead' | 'Deal' | None
        "source": "Notification Log",
        "name": row.get("name"),
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

    or_filters = [{"for_user": user}, {"owner": user}]
    if _has("from_user"):
        or_filters.append({"from_user": user})

    rows = frappe.get_all(
        "Notification Log",
        filters={},
        or_filters=or_filters,
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
def list_portal_notifications(limit: int = 50, include_legacy: int = 1):
    """
    يرجّع إشعارات المستخدم من Notification Log (+ اختياري CRM Notification).
    - يلتقط السجلات للمستخدم سواء كان for_user أو owner أو from_user.
    - يضيف Route فقط لو document_type من Doctypes الـCRM.
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

    or_filters = [{"for_user": user}, {"owner": user}]
    if _has("from_user"):
        or_filters.append({"from_user": user})

    base_rows = frappe.get_all(
        "Notification Log",
        filters={},
        or_filters=or_filters,
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

    or_filters = [{"for_user": user}, {"owner": user}]
    if _has("from_user"):
        or_filters.append({"from_user": user})

    rows = frappe.get_all(
        "Notification Log",
        filters={},
        or_filters=or_filters,
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


# ------- (اختياري) التوافق الخلفي مع CRM Notification القديمة -------

def _list_crm_notifications(limit: int = 50) -> List[Dict[str, Any]]:
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
        out.append(
            {
                "creation": n.creation,
                "from_user": {
                    "name": n.from_user,
                    "full_name": frappe.get_value("User", n.from_user, "full_name"),
                },
                "type": n.type,
                "to_user": n.to_user,
                "read": n.read,
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
                "name": n.name,
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
