# ⚡ تحديث 14,000 Lead على Trust.com - دليل سريع

## 🚀 الأوامر الجاهزة (Copy & Paste)

### 1️⃣ اختبار (دقيقة واحدة)
```bash
cd /home/frappe/frappe-bench-env/frappe-bench
bench --site trust.com execute crm.scripts.test_team_leader_setup.run_all_tests
```

### 2️⃣ إضافة الحقل (إذا لزم)
```bash
bench --site trust.com execute crm.scripts.add_team_leader_field.add_field
```

### 3️⃣ Backup
```bash
bench --site trust.com backup
```

### 4️⃣ تجربة سريعة (10 leads)
```bash
bench --site trust.com console
```
ثم:
```python
from crm.scripts.update_large_dataset import quick_test
quick_test(10)
exit()
```

### 5️⃣ التحديث الكامل (12-15 دقيقة)
```bash
bench --site trust.com console
```
ثم:
```python
from crm.scripts.update_large_dataset import run_update
run_update(batch_size=100, dry_run=False)
exit()
```

### 6️⃣ التحقق من النتائج
```bash
bench --site trust.com execute crm.scripts.update_large_dataset.verify_results
```

---

## 📊 النتيجة المتوقعة

```
Total Leads:              14,000
✓ With Team Leader:       ~11,800 (84%)
⊘ Without Team Leader:    ~2,200 (16%)
⏱️  Total Time:            12-15 minutes
```

---

## ⚠️ ملاحظات سريعة

- ✅ خذ Backup أولاً
- ✅ شغّل في وقت قليل الاستخدام
- ✅ راقب الـ output
- ✅ لا تقاطع العملية

---

## 🆘 في حالة المشاكل

```bash
# تحقق من Error Log
bench --site trust.com console
```

```python
import frappe
errors = frappe.get_all("Error Log", 
    filters={"error": ["like", "%team_leader%"]},
    order_by="creation desc", 
    limit=5
)
print(f"Found {len(errors)} errors")
```

---

**للتفاصيل الكاملة:** راجع `TRUST_COM_GUIDE.md`

