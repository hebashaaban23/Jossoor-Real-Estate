import frappe
from typing import List, Optional

import firebase_admin
from firebase_admin import credentials, messaging


# -----------------------------
# Firebase init helpers
# -----------------------------

def _get_firebase_app():
    """
    Initialize Firebase app once per site using service account JSON:
    sites/<site>/private/firebase_service_account.json
    """
    try:
        return firebase_admin.get_app()
    except ValueError:
        service_account_path = frappe.get_site_path("private", "firebase_service_account.json")
        cred = credentials.Certificate(service_account_path)
        return firebase_admin.initialize_app(cred)


# -----------------------------
# Token helpers
# -----------------------------

def _get_user_tokens(user: str) -> List[str]:
    """Get all active FCM tokens stored for this user."""
    filters = {"user": user}
    # لو عندك حقل active استخدمه
    if "active" in [d.fieldname for d in frappe.get_meta("User Device Token").fields]:
        filters["active"] = 1

    return frappe.get_all(
        "User Device Token",
        filters=filters,
        pluck="fcm_token"
    )


def _send_push_to_tokens(tokens: List[str], title: str, body: str, data: Optional[dict] = None):
    """
    Send push notification via Firebase Admin SDK (FCM v1) – one token at a time.
    Handles invalid tokens by deactivating them.
    """
    if not tokens:
        return {"sent": 0, "failed": 0, "errors": []}

    _get_firebase_app()  # ensure Firebase is initialized

    data = data or {}
    sent = 0
    failed = 0
    errors = []

    for token in tokens:
        message = messaging.Message(
            notification=messaging.Notification(
                title=title,
                body=body,
            ),
            data=data,
            token=token,
        )
        try:
            messaging.send(message)
            sent += 1
        except Exception as e:
            failed += 1
            errors.append({"token": token, "error": str(e)})

            # لو التوكن باظ نشيله أو نعمله inactive
            if (
                "registration token is not a valid FCM" in str(e)
                or "UNREGISTERED" in str(e)
                or "Requested entity was not found" in str(e)
                or "registration-token-not-registered" in str(e)
            ):
                _deactivate_token(token)

    return {
        "sent": sent,
        "failed": failed,
        "errors": errors,
    }


def _deactivate_token(token: str):
    """Mark token inactive or delete row when Firebase says it's invalid."""
    meta = frappe.get_meta("User Device Token")
    has_active = any(df.fieldname == "active" for df in meta.fields)

    rows = frappe.get_all(
        "User Device Token",
        filters={"fcm_token": token},
        pluck="name"
    )
    for name in rows:
        if has_active:
            doc = frappe.get_doc("User Device Token", name)
            doc.active = 0
            doc.save(ignore_permissions=True)
        else:
            frappe.delete_doc("User Device Token", name, ignore_permissions=True)


# -----------------------------
# API: Save token from Flutter
# -----------------------------

@frappe.whitelist(allow_guest=False, methods=["POST"])
def save_fcm_token(
    fcm_token: Optional[str] = None,
    device_info: Optional[str] = None,
    platform: Optional[str] = None,
    app_version: Optional[str] = None,
):
    """
    Called by Flutter after login to register / refresh device token.

    Accepts JSON body:
    {
      "fcm_token": "...",
      "device_info": "iPhone 15",
      "platform": "ios",
      "app_version": "1.0.0"
    }
    """
    data = frappe.form_dict or {}
    fcm_token = fcm_token or data.get("fcm_token")
    device_info = device_info or data.get("device_info")
    platform = platform or data.get("platform")
    app_version = app_version or data.get("app_version")

    if not fcm_token:
        frappe.throw("FCM Token is required")

    user = frappe.session.user

    existing = frappe.get_all(
        "User Device Token",
        filters={"user": user, "fcm_token": fcm_token},
        pluck="name",
        limit=1,
    )

    if existing:
        doc = frappe.get_doc("User Device Token", existing[0])
    else:
        doc = frappe.new_doc("User Device Token")
        doc.user = user
        doc.fcm_token = fcm_token

    if device_info:
        doc.device_info = device_info
    if platform and hasattr(doc, "platform"):
        doc.platform = platform
    if app_version and hasattr(doc, "app_version"):
        doc.app_version = app_version
    if hasattr(doc, "active"):
        doc.active = 1
    if hasattr(doc, "last_seen"):
        doc.last_seen = frappe.utils.now_datetime()

    doc.save(ignore_permissions=True)

    return {"status": "success", "message": "Token saved"}


@frappe.whitelist(allow_guest=False, methods=["POST"])
def unregister_fcm_token(fcm_token: Optional[str] = None):
    """
    Optional: called by app on logout/uninstall to deactivate a specific token.
    """
    data = frappe.form_dict or {}
    fcm_token = fcm_token or data.get("fcm_token")
    if not fcm_token:
        frappe.throw("fcm_token is required")

    _deactivate_token(fcm_token)
    return {"status": "success", "message": "Token deactivated"}


# -----------------------------
# API: Test push (for Postman)
# -----------------------------

@frappe.whitelist(methods=["POST", "GET"])
def send_test_push(
    user: Optional[str] = None,
    title: str = "Test notification",
    body: str = "Hello from Frappe + Firebase v1"
):
    """
    Send test push notification to given user (email) or current session user.
    """
    user = user or frappe.session.user
    if not user or user == "Guest":
        frappe.throw("User is required (login or send ?user=<email>)")

    tokens = _get_user_tokens(user)
    if not tokens:
        frappe.throw("No device tokens found for this user")

    result = _send_push_to_tokens(tokens, title, body, data={"type": "test"})
    return {
        "status": "sent",
        "user": user,
        "tokens": tokens,
        "result": result,
    }


# -----------------------------
# Hook: Notification Log -> Push
# -----------------------------

def send_push_for_notification_log(doc, method=None):
    """
    Doc event for Notification Log (after_insert).
    Sends a push to the target user when a new Notification Log is created.
    """
    target_user = getattr(doc, "for_user", None) or getattr(doc, "owner", None)
    if not target_user:
        return

    tokens = _get_user_tokens(target_user)
    if not tokens:
        return

    # Title + body for push
    title = "New notification"
    body = (doc.subject or doc.email_content or "You have a new notification").strip()
    body = frappe.utils.strip_html(body)[:200]

    data = {
        "notification_log_name": doc.name,
        "doctype": doc.document_type or "",
        "docname": doc.document_name or "",
        "type": doc.type or "",
    }

    try:
        _send_push_to_tokens(tokens, title, body, data=data)
    except Exception:
        frappe.log_error(frappe.get_traceback(), "Error sending FCM push from Notification Log")
