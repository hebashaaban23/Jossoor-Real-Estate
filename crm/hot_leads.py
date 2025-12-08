import frappe

# ========= Weights =========
WEIGHTS = {
    "status_high": 40,
    "status_warm": 20,
    "status_cold": -20,
}

# ========= تصنيف الحالات =========
HIGH_INTENT_STATUSES = {"Qualified", "Property Visit", "Office Visit", "FollowUp"}
WARM_STATUSES = {"Contacted", "New"}
COLD_STATUSES = {"Unqualified", "Junk"}

def to_rating(score: int) -> str:
    if score >= 70: return "Hot"
    if score >= 40: return "Warm"
    return "Cold"

def validate(doc, method=None):
    """اختبار: احسب السكور من الـ Status فقط"""

    status = doc.get("status")
    score, reasons = 0, []

    if status in HIGH_INTENT_STATUSES:
        score += WEIGHTS["status_high"]; reasons.append(f"+{WEIGHTS['status_high']} high-intent status ({status})")
    elif status in WARM_STATUSES:
        score += WEIGHTS["status_warm"]; reasons.append(f"+{WEIGHTS['status_warm']} warm status ({status})")
    elif status in COLD_STATUSES:
        score += WEIGHTS["status_cold"]; reasons.append(f"{WEIGHTS['status_cold']} cold/negative status ({status})")

    doc.lead_score = int(score)
    doc.lead_rating = to_rating(score)
    doc.rating_reason = "; ".join(reasons)

    # debug log
    frappe.logger("hot_leads", allow_site=True).info(
        f"[TEST] Lead={doc.name} Status={status} Score={score} Rating={doc.lead_rating}"
    )
