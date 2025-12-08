# apps/crm/crm/reminder_runner.py
import frappe
def run_reminders_locked():
    """يشغّل send_reminders() مع قفل Redis بسيط لتجنّب التوازي."""
    # لو نِسخة فربّي قديمة مافيهاش redis_lock، استخدم النسخة البديلة
    lock = getattr(frappe.utils, "redis_lock", None)
    if lock:
        _lock = lock("reminder_send_lock", timeout=120)
        if not _lock.acquire(blocking=False):
            return
        try:
            _run_core_send()
        finally:
            try:
                _lock.release()
            except Exception:
                pass
    else:
        # fallback: شغّل مباشرة بدون قفل
        _run_core_send()

def _run_core_send():
    from frappe.automation.doctype.reminder.reminder import send_reminders
    from crm.api.reminders import flag_overdue_comments_for_leads

    send_reminders()
    try:
        flag_overdue_comments_for_leads()
    except Exception:
        frappe.log_error(frappe.get_traceback(), "flag_overdue_comments_for_leads failed")
