# دليل البدء السريع - تحديث Team Leader في CRM Leads

## الخطوات السريعة

### 1️⃣ إضافة الحقل (إذا لم يكن موجوداً)

```bash
cd /home/frappe/frappe-bench-env/frappe-bench
bench --site [اسم الموقع] console
```

ثم في الـ console:

```python
from crm.scripts.add_team_leader_field import add_field
add_field()
```

أو مباشرة من terminal:

```bash
bench --site [اسم الموقع] execute crm.scripts.add_team_leader_field.add_field
```

### 2️⃣ التحقق من الحقل

```bash
bench --site [اسم الموقع] execute crm.scripts.add_team_leader_field.check_field_status
```

### 3️⃣ تحديث البيانات القديمة

**تجربة أولاً (بدون حفظ):**

```bash
bench --site [اسم الموقع] execute crm.scripts.quick_update_team_leader.run --kwargs "{'dry_run': True}"
```

**التحديث الفعلي:**

```bash
bench --site [اسم الموقع] execute crm.scripts.quick_update_team_leader.run
```

### 4️⃣ تفعيل التحديث التلقائي (اختياري)

أضف هذا الكود في ملف `/home/frappe/frappe-bench-env/frappe-bench/apps/crm/crm/hooks.py`:

```python
doc_events = {
    # ... الكود الموجود ...
    
    "CRM Lead": {
        # ... الـ hooks الموجودة ...
        "on_update": "crm.scripts.auto_update_team_leader.update_team_leader_on_lead_update",
        "after_insert": "crm.scripts.auto_update_team_leader.update_team_leader_on_lead_insert",
    },
    
    "ToDo": {
        # ... الـ hooks الموجودة ...
        "after_insert": "crm.scripts.auto_update_team_leader.update_team_leader_on_todo_insert",
    }
}
```

ثم أعد تشغيل bench:

```bash
bench restart
```

---

## أوامر مفيدة

### التحقق من عدد السجلات المحدثة
```python
import frappe
count = frappe.db.count("CRM Lead", {"team_leader": ["!=", ""]})
print(f"عدد Leads التي لها Team Leader: {count}")
```

### عرض نماذج من البيانات
```python
import frappe
leads = frappe.get_all("CRM Lead", 
    fields=["name", "lead_owner", "team_leader"],
    filters={"team_leader": ["!=", ""]},
    limit=10
)
for lead in leads:
    print(f"{lead.name}: Owner={lead.lead_owner}, Team Leader={lead.team_leader}")
```

### اختبار على lead واحد
```python
from crm.scripts.update_team_leader_in_leads import test_single_lead
test_single_lead("CRM-LEAD-2024-00001")
```

### التحقق من بنية الـ Teams
```python
import frappe
teams = frappe.get_all("Team", fields=["name", "team_leader"])
for team in teams:
    members = frappe.get_all("Member", 
        filters={"parent": team.name},
        fields=["member"]
    )
    print(f"Team: {team.name}")
    print(f"  Leader: {team.team_leader}")
    print(f"  Members: {[m.member for m in members]}")
    print()
```

---

## استكشاف الأخطاء

### ❌ خطأ: team_leader field does not exist
**الحل:** قم بتشغيل الخطوة 1 أعلاه لإضافة الحقل

### ⚠️ لا توجد تحديثات
**تحقق من:**
1. هل المستخدمون معينون للـ Leads؟
2. هل المستخدمون موجودون في Teams؟
3. هل Team Leaders محددون في Teams؟

### 🔍 فحص مستخدم معين
```python
from crm.scripts.update_team_leader_in_leads import get_team_leader_for_user
leader = get_team_leader_for_user("user@example.com")
print(f"Team Leader: {leader}")
```

### 🔍 فحص lead معين
```python
from crm.scripts.update_team_leader_in_leads import get_assigned_users_for_lead
users = get_assigned_users_for_lead("CRM-LEAD-2024-00001")
print(f"Assigned users: {users}")
```

---

## ملاحظات مهمة

✅ **قبل التشغيل:**
- خذ backup من قاعدة البيانات
- جرب على موقع تجريبي أولاً
- استخدم dry_run=True للتجربة

⚠️ **أثناء التشغيل:**
- لا تقاطع العملية
- راقب الـ output

✅ **بعد التشغيل:**
- تحقق من النتائج
- راجع Error Log

---

## المساعدة

للمزيد من التفاصيل، راجع:
- `README_AR.md` - دليل شامل بالعربية
- `README.md` - دليل شامل بالإنجليزية

