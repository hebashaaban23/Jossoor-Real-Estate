# your_app/dup_leads.py
import frappe
from frappe.utils import now

# ---- config (عدّل لو اسم الحقل/التابل مختلف) ----
CHILD_TABLE_FIELDNAME = "duplicate_leads"   # child table field in CRM Lead
CHILD_LINK_FIELDNAME  = "lead"              # Link field inside child table -> CRM Lead

# ---------- 1) Utilities ----------
def normalize_egyptian_phone(number: str) -> str:
    """Normalize many Egyptian formats to +20XXXXXXXXXXX. Non-matching => cleaned."""
    if not number:
        return ""
    number = number.translate(str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789"))
    cleaned = []
    for i, ch in enumerate(number.strip()):
        if ch.isdigit() or (ch == "+" and i == 0):
            cleaned.append(ch)
    number = "".join(cleaned)
    if number.startswith("+20"):
        return "+20" + number[3:]
    if number.startswith("0020"):
        return "+20" + number[4:]
    if number.startswith("20"):
        return "+20" + number[2:]
    if number.startswith("0"):
        return "+20" + number[1:]
    if len(number) == 10 and number.startswith("1"):
        return "+20" + number
    if len(number) == 11 and number.startswith("01"):
        return "+20" + number[1:]
    return number

def _safe_notify(msg: str):
    # Avoid UI noise during Data Import / background jobs
    if getattr(frappe.local, "request", None):
        frappe.msgprint(msg=msg, indicator="orange", alert=True)

def _collect_normalized_numbers(doc) -> list[str]:
    """Return unique normalized numbers from doc.phone / doc.mobile_no and write them back."""
    p = normalize_egyptian_phone(getattr(doc, "phone", "") or "")
    m = normalize_egyptian_phone(getattr(doc, "mobile_no", "") or "")
    if hasattr(doc, "phone"):
        doc.phone = p
    if hasattr(doc, "mobile_no"):
        doc.mobile_no = m
    return [n for n in {p, m} if n]

def _find_canonical_original(numbers: list[str], exclude_name: str | None = None) -> str | None:
    """Pick a stable canonical:
       1) original_lead=1 first, then 2) non-duplicate (is_duplicate=0), then 3) oldest creation.
    """
    if not numbers:
        return None
    or_filters = (
        [["CRM Lead", "phone",     "=", n] for n in numbers] +
        [["CRM Lead", "mobile_no", "=", n] for n in numbers]
    )
    rows = frappe.get_all(
        "CRM Lead",
        filters={"name": ["!=", exclude_name]} if exclude_name else {},
        or_filters=or_filters,
        fields=["name", "creation", "original_lead", "is_duplicate"],
        order_by="original_lead desc, is_duplicate asc, creation asc",
        limit=1,
    )
    return rows[0]["name"] if rows else None

def _ensure_child_row_once(original, duplicate_name: str, timestamp: str):
    """Idempotent add; prevents self-link & duplicates in child table."""
    original.reload()

    # 1) never self-link
    if duplicate_name == original.name:
        return

    # 2) avoid duplicates if it already exists
    child_rows = original.get(CHILD_TABLE_FIELDNAME) or []
    if any(getattr(r, CHILD_LINK_FIELDNAME, None) == duplicate_name for r in child_rows):
        return

    original.append(
        CHILD_TABLE_FIELDNAME,
        {
            CHILD_LINK_FIELDNAME: duplicate_name,
            "created_on": timestamp,
            "note": "Write Note",
        },
    )

# ---------- 2) Hooks ----------
def check_duplicates(doc, method):
    """before_insert: mark the doc as duplicate & remember the 'duplicated_from'."""
    # run only for brand-new docs
    if not getattr(doc, "is_new", lambda: False)():
        return

    # avoid re-entrancy
    if getattr(doc.flags, "ignore_duplicate_check", False):
        return

    numbers = _collect_normalized_numbers(doc)
    if not numbers:
        doc.is_duplicate = 0
        doc.duplicated_from = None
        return

    original_name = _find_canonical_original(numbers, exclude_name=getattr(doc, "name", None))

    if original_name:
        doc.is_duplicate = 1
        doc.duplicated_from = original_name
        _safe_notify(f"Already exists lead {original_name}. Marked as Duplicate.")
        doc.flags.ignore_duplicate_check = True
    else:
        doc.is_duplicate = 0
        doc.duplicated_from = None

def append_to_original_lead(doc, method):
    """after_insert / on_submit: append duplicate lead into original's child table."""
    if not doc or not getattr(doc, "is_duplicate", 0):
        return

    numbers = _collect_normalized_numbers(doc)
    if not numbers:
        return

    true_original = _find_canonical_original(numbers, exclude_name=doc.name)

    # لو المستند دا هو نفسه الأصل أو مفيش أصل واضح
    if not true_original or true_original == doc.name:
        return

    # تأكيد أعلام التكرار على نفس المستند
    if doc.duplicated_from != true_original or not getattr(doc, "is_duplicate", 0):
        frappe.db.set_value(
            "CRM Lead",
            doc.name,
            {"duplicated_from": true_original, "is_duplicate": 1},
            update_modified=False,
        )

    timestamp = now()
    try:
        original = frappe.get_doc("CRM Lead", true_original)
        original.flags.ignore_duplicate_check = True

        _ensure_child_row_once(original, doc.name, timestamp)
        original.save(ignore_permissions=True)

        if not original.get("original_lead"):
            original.db_set("original_lead", 1, update_modified=False)
            original.original_lead = 1

    except Exception:
        frappe.log_error(title="Lead Duplicate Append Error", message=frappe.get_traceback())
