import frappe
from frappe.model.document import Document

FREQ = {"Monthly": 12, "Quarterly": 4, "Biannual": 2, "Annual": 1}


def _pick_first(*vals):
    """Return first non-empty/stripped value."""
    for v in vals:
        if isinstance(v, str):
            v = v.strip()
            if v:
                return v
        elif v:
            return v
    return None


class Reservation(Document):
    def validate(self):
        self._pull_lead()
        self._pull_plan()          # ← add this
        self._pull_unit_meta()
        self._compute_plan()

    def _pull_plan(self):
        """If a Payment Plan is selected, capture a human title and pull key fields."""
        if not getattr(self, "payment_plan", None):
            return

        try:
            pp = frappe.get_doc("Payment Plan", self.payment_plan)
        except Exception:
            return

        # Human-friendly plan title
        plan_title = _pick_first(
            getattr(pp, "plan_name", None),
            getattr(pp, "title", None),
            pp.name,
        )

        # Save into whichever field exists on Reservation (create one via Customize:
        #   Data field "plan_name" or "plan_label" — either works).
        for fld in ("plan_name", "plan_label"):
            if frappe.get_meta("Reservation").has_field(fld):
                setattr(self, fld, plan_title)
                break

        # If project / unit are empty, try to pull from plan
        plan_project = _pick_first(getattr(pp, "project", None), getattr(pp, "project_name", None))
        plan_unit    = _pick_first(getattr(pp, "project_unit", None), getattr(pp, "unit", None),
                                   getattr(pp, "unit_name", None), getattr(pp, "unit_id", None))
        if plan_project and not getattr(self, "project", None):
            self.project = plan_project
        if plan_unit and not getattr(self, "unit", None):
            self.unit = plan_unit

        # Prefer server-side total cost = Area × Price if Reservation total is empty
        area = _pick_first(
            getattr(pp, "area", None),
            getattr(pp, "unit_area", None),
            getattr(pp, "built_up_area", None),
            getattr(pp, "bua", None),
            getattr(pp, "sqm", None),
            getattr(pp, "sqft", None),
            getattr(pp, "total_area", None),
            getattr(pp, "net_area", None),
        )
        price = _pick_first(
            getattr(pp, "price", None),
            getattr(pp, "rate", None),
            getattr(pp, "price_per_sqm", None),
            getattr(pp, "price_per_sqft", None),
            getattr(pp, "basic_rate", None),
            getattr(pp, "selling_price", None),
            getattr(pp, "list_price", None),
            getattr(pp, "base_price", None),
        )

        if not getattr(self, "total_cost", None):
            try:
                if area and price:
                    self.total_cost = float(area) * float(price)
                elif getattr(pp, "total_cost", None):
                    self.total_cost = float(pp.total_cost)
            except Exception:
                pass

        # Frequency / Years fallback from plan
        if not getattr(self, "frequency", None):
            self.frequency = _pick_first(getattr(pp, "frequency", None))
        if not getattr(self, "years", None):
            self.years = _pick_first(
                getattr(pp, "years", None),
                getattr(pp, "tenure", None),
                getattr(pp, "tenor", None),
                getattr(pp, "duration_years", None),
            )

# ---------- AJAX helpers (whitelisted) ----------
@frappe.whitelist()
def get_unit_meta(name: str, project: str | None = None):
    """
    Normalize Unit / Project Unit data for the Reservation form.
    Returns keys that actually exist on your doctypes.
    """
    if not name:
        return {}

    target_dt = None
    if frappe.db.exists("Project Unit", name):
        target_dt = "Project Unit"
    elif frappe.db.exists("Unit", name):
        target_dt = "Unit"
    else:
        return {}

    doc = frappe.get_doc(target_dt, name)

    def pick(*fields):
        for f in fields:
            if hasattr(doc, f):
                v = getattr(doc, f)
                if isinstance(v, str):
                    v = v.strip()
                if v not in (None, ""):
                    return v
        return None

    # Normalize fields by availability on your Unit doctype dump
    area_val = _pick_first(
        getattr(doc, "area_sqm", None),
        getattr(doc, "builtup_area_m²", None),
    )

    # Availability field can be "availability" or "status"
    availability_val = _pick_first(
        getattr(doc, "availability", None),
        getattr(doc, "status", None),
    )

    return {
        "doctype": target_dt,
        "name": doc.name,
        "unit_name": pick("unit_name") or doc.name,
        "project": pick("project") or (project or None),

        # Property meta we’ll map into Reservation
        "property_type": pick("type"),  # your Unit.type → Reservation.property_type
        "area": area_val,
        "price": pick("price"),
        "price_per_meter": pick("price_per_meter"),
        "maintenance_fees": pick("maintenance_fees"),
        "availability": availability_val,
        "floor": pick("floor"),
        "view": pick("view"),
        "orientation": pick("orientation"),
        "furnished": pick("furnished"),
        "finishing": pick("finishing"),
        "parking": pick("parking"),
        "bedrooms": pick("bedrooms"),
        "bathrooms": pick("bathrooms"),
        "master_bed_rooms": pick("master_bed_rooms"),
        "categories": pick("categories"),

        # Media normalization
        "property_image": pick("cover_image", "property_image", "image", "unit_image", "image_url", "thumbnail"),
        "floor_plan": pick("floor_plan"),
        "brochure": pick("brochure"),
        "video_url": pick("video_url"),
    }


@frappe.whitelist()
def search_units_by_title(doctype=None, txt: str = "", searchfield: str = "name",
                          start: int = 0, page_len: int = 20, filters=None):
    """
    Custom search used by Reservation.unit. Returns [[value, label], ...].
    Label = human title (no numeric prefixes).
    """
    if isinstance(filters, str):
        try:
            filters = frappe.parse_json(filters) or {}
        except Exception:
            filters = {}
    filters = filters or {}

    target_dt = (filters.get("target_dt") or "Unit").strip()
    q = (txt or "").strip().lower()

    meta = frappe.get_meta(target_dt)
    candidates = ["unit_name", "unit_title", "title", "name"]
    display_field = next((f for f in candidates if (f == "name" or meta.has_field(f))), "name")

    base_filters = {}
    if target_dt == "Project Unit" and filters.get("project"):
        base_filters["project"] = filters.get("project")

    order_by = f"{display_field} asc" if display_field != "name" else "modified desc"

    rows = frappe.get_all(
        target_dt,
        filters=base_filters,
        fields=["name", display_field],
        order_by=order_by,
        start=start,
        page_length=page_len,
    )

    import re
    def strip_num_prefix(s: str) -> str:
        return re.sub(r"^\s*\d+\s*[-–.:]*\s*", "", s or "").strip()

    out = []
    for r in rows:
        raw_label = (r.get(display_field) or r.get("name") or "").strip()
        label = strip_num_prefix(raw_label)
        if q and q not in label.lower():
            continue
        out.append([r["name"], label])
    return out

def get_dashboard_data():
    return {
        # reverse-link fieldnames in the *other* doctypes that point back to Reservation:
        "non_standard_fieldnames": {
            "Payment Plan": "reservation",  # <— IMPORTANT
            # Only add entries here if those doctypes actually have a Link to Reservation
            # "CRM Lead": "reservation",            # only if such a field exists
            # "Real Estate Project": "reservation", # only if such a field exists
        },
        # fields on Reservation that link OUT to other doctypes (no reverse counting needed)
        "internal_links": {
            "CRM Lead": ["lead"],
            "Real Estate Project": ["project"],
        },
        "transactions": [
            {"label": _("Payments"), "items": ["Payment Plan"]},
            # Add other groups/items only when reverse links exist
        ],
    }

class Reservation(Document):
    @classmethod
    def default_list_data(cls):
        """
        Columns for crm.api.doc.get_data / ViewControls.
        We keep 'name' in rows (needed for navigation) but we don't show it as a column.
        We show Lead Name and a human Payment Plan title instead.
        """
        meta = frappe.get_meta("Reservation")

        # pick whichever field you have added: plan_name or plan_label; else fall back to 'payment_plan'
        plan_col = "plan_name" if meta.has_field("plan_name") else (
            "plan_label" if meta.has_field("plan_label") else "payment_plan"
        )
        plan_type = "Data"

        return {
            "columns": [
                {"key": "lead_name",  "label": "Lead",          "type": "Data",                        "width": 240},
                {"key": plan_col,     "label": "Payment Plan",  "type": plan_type, "options": "Payment Plan", "width": 240},
                {"key": "project",    "label": "Project",       "type": "Link",  "options": "Real Estate Project", "width": 220},
                {"key": "unit",       "label": "Unit",          "type": "Data",                        "width": 220},
                {"key": "total_cost", "label": "Total Cost",    "type": "Currency",                    "width": 160},
                {"key": "modified",   "label": "Last Updated",  "type": "Datetime",                    "width": 180},
            ],
            # include 'name' silently so the list can open the doc
            "rows": [
                "name",
                "lead_name",
                plan_col,
                "project",
                "unit",
                "total_cost",
                "modified",
                "crm_lead",
                "payment_plan",
            ],
            "order_by": "modified desc",
            "page_length": 100,
        }
