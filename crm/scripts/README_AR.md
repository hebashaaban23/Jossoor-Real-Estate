# تحديث حقل Team Leader في CRM Leads

## الوصف
هذا السكريبت يقوم بملء حقل `team_leader` في جميع سجلات CRM Lead القديمة بناءً على المستخدمين المعينين (Assigned To).

## كيفية العمل
1. يحصل على جميع سجلات CRM Lead
2. لكل سجل، يبحث عن المستخدمين المعينين من:
   - حقل `_assign` (Assigned To)
   - حقل `lead_owner`
   - سجلات ToDo المفتوحة
3. يبحث عن Team Leader للمستخدم المعين من جدول Team
4. يملأ حقل `team_leader` في CRM Lead

## الاستخدام

### 1. التأكد من وجود حقل team_leader
قبل تشغيل السكريبت، يجب أن يكون حقل `team_leader` موجوداً في CRM Lead. 

إذا لم يكن موجوداً:
- افتح Doctype CRM Lead
- أضف حقل جديد:
  - Fieldname: `team_leader`
  - Label: `Team Leader`
  - Field Type: `Link`
  - Options: `User`

### 2. فتح Frappe Console
```bash
cd /home/frappe/frappe-bench-env/frappe-bench
bench --site [site-name] console
```

### 3. استيراد السكريبت
```python
from crm.scripts.update_team_leader_in_leads import *
```

### 4. تجربة على Lead واحد (اختياري)
```python
# اختبار على Lead واحد
test_single_lead("CRM-LEAD-2024-00001")
```

### 5. تشغيل تجريبي (Dry Run)
```python
# تشغيل تجريبي بدون حفظ التغييرات
result = update_all_leads(dry_run=True)
```

### 6. التحديث الفعلي
```python
# تحديث جميع السجلات
result = update_all_leads()
```

### 7. تحديث عدد محدود من السجلات
```python
# تحديث أول 100 سجل فقط
result = update_all_leads(limit=100)
```

## أمثلة إضافية

### تحديث Leads بحالة معينة
```python
# يمكنك تعديل السكريبت لإضافة filters
# مثال: تحديث Leads بحالة "New" فقط
```

### التحقق من النتائج
```python
# بعد التشغيل، يمكنك التحقق من النتائج
import frappe
leads_with_team_leader = frappe.db.count("CRM Lead", {"team_leader": ["!=", ""]})
print(f"عدد Leads التي تم تحديث Team Leader لها: {leads_with_team_leader}")
```

## الملاحظات
- السكريبت يستخدم `ignore_permissions=True` لذلك تأكد من تشغيله بحساب Administrator
- يتم commit التغييرات تلقائياً ما لم تستخدم `dry_run=True`
- السكريبت يطبع تقريراً مفصلاً بالنتائج بعد الانتهاء

## استكشاف الأخطاء

### خطأ: team_leader field does not exist
الحل: أضف الحقل يدوياً في Doctype CRM Lead أولاً

### لا توجد تحديثات
تحقق من:
1. هل المستخدمون معينون للـ Leads؟
2. هل المستخدمون موجودون في جدول Team كأعضاء؟
3. هل Team Leaders محددون في جدول Team؟

## الأمان
- تأكد من عمل backup للبيانات قبل التشغيل
- استخدم dry_run أولاً للتحقق من النتائج
- السكريبت يسجل الأخطاء في Error Log في حالة حدوث مشاكل

