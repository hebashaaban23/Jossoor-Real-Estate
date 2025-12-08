# تحديثات Dashboard الخاصة بـ Frappe CRM

## التغييرات التي تم إجراؤها

تم تعديل Dashboard الخاص بـ CRM ليعرض المعلومات التالية:

### 1. البطاقات الرقمية (Number Charts)

#### البطاقات الرئيسية:
- **إجمالي Leads**: عدد جميع الـ Leads
- **Delayed Leads**: عدد الـ Leads المتأخرة (من حقل delayed)
- **إجمالي Deals**: العدد الكلي للـ Deals من جدول CRM Deal

#### بطاقات حسب Status:
- **New Leads**: عدد الـ Leads بحالة "New"
- **Contacted Leads**: عدد الـ Leads بحالة "Contacted"
- **Nurture Leads**: عدد الـ Leads بحالة "Nurture"
- **Qualified Leads**: عدد الـ Leads بحالة "Qualified"
- **Unqualified Leads**: عدد الـ Leads بحالة "Unqualified"
- **Junk Leads**: عدد الـ Leads بحالة "Junk"

### 2. الرسوم البيانية (Charts)

- **Donut Chart**: يعرض توزيع الـ Leads حسب الـ Status
- **Bar Chart**: يعرض عدد الـ Leads لكل Status بشكل أعمدة

---

## الملفات التي تم تعديلها

### 1. `/crm/api/dashboard.py`
تم إضافة الدوال التالية:

#### دوال البيانات الرئيسية:
- `get_delayed_leads()`: لحساب عدد الـ Leads المتأخرة
- `get_total_deals()`: لحساب إجمالي عدد الـ Deals
- `get_leads_by_status()`: لإرجاع بيانات الـ Donut Chart
- `get_leads_by_status_chart()`: لإرجاع بيانات الـ Bar Chart

#### دوال Status المنفصلة:
- `get_lead_status_count()`: دالة مساعدة لحساب عدد leads لـ status معين
- `get_new_leads()`: عدد New leads
- `get_contacted_leads()`: عدد Contacted leads
- `get_nurture_leads()`: عدد Nurture leads
- `get_qualified_leads()`: عدد Qualified leads
- `get_unqualified_leads()`: عدد Unqualified leads
- `get_junk_leads()`: عدد Junk leads

### 2. `/crm/fcrm/doctype/crm_dashboard/crm_dashboard.py`
تم تحديث دالة `default_manager_dashboard_layout()` لتعرض التصميم الجديد.

---

## كيفية تطبيق التغييرات

### الطريقة الأولى: استخدام السكريبت المرفق

```bash
cd /home/frappe/frappe-bench-env/frappe-bench
bench --site site1.local execute crm.update_dashboard.update_dashboard
```

أو:

```bash
cd /home/frappe/frappe-bench-env/frappe-bench/apps/crm
python3 update_dashboard.py
```

### الطريقة الثانية: من واجهة المستخدم

1. قم بإعادة تشغيل الـ bench:
```bash
bench restart
```

2. افتح Dashboard في المتصفح
3. اضغط على زر **"Edit"** (يتطلب صلاحيات System Manager)
4. اضغط على زر **"Reset to default"**
5. قم بتحديث الصفحة

### الطريقة الثالثة: باستخدام Bench Console

```bash
bench --site site1.local console
```

ثم في الـ console:

```python
from crm.fcrm.doctype.crm_dashboard.crm_dashboard import create_default_manager_dashboard
create_default_manager_dashboard(force=True)
frappe.db.commit()
```

---

## ملاحظات مهمة

1. **حقل Delayed**: تأكد من وجود حقل `delayed` في doctype `CRM Lead` (موجود بالفعل كحقل Check)

2. **الـ Statuses**: الـ Statuses المستخدمة هي الافتراضية:
   - New
   - Contacted
   - Nurture
   - Qualified
   - Unqualified
   - Junk

3. **المقارنة مع الفترة السابقة**: كل بطاقة رقمية تعرض:
   - القيمة الحالية
   - النسبة المئوية للتغيير مقارنة بالفترة السابقة
   - السهم الأخضر (زيادة) أو الأحمر (نقصان)

4. **التصفية**: يمكن تصفية البيانات حسب:
   - الفترة الزمنية (آخر 7, 30, 60, 90 يوم أو Custom Range)
   - المستخدم (Sales User)

---

## التخطيط الجديد للـ Dashboard

```
الصف الأول (y=0):
[Total Leads] [Delayed Leads] [Total Deals] [New Leads]

الصف الثاني (y=3):
[Contacted] [Nurture] [Qualified] [Unqualified]

الصف الثالث (y=6):
[Junk] [Donut Chart - Leads by Status] [Bar Chart - Leads by Status]
```

---

## اختبار التغييرات

بعد تطبيق التغييرات، تأكد من:

1. ✓ ظهور جميع البطاقات الرقمية
2. ✓ عرض الأرقام الصحيحة
3. ✓ ظهور الـ Charts بشكل صحيح
4. ✓ عمل الفلترة (Date Range, User) بشكل سليم
5. ✓ المقارنة مع الفترة السابقة تعمل

---

## الدعم

في حالة وجود أي مشاكل:
1. تأكد من إعادة تشغيل الـ bench
2. تحقق من الـ console للأخطاء
3. تأكد من وجود بيانات في الجداول
4. تحقق من الصلاحيات (Sales User على الأقل)

---

تم التحديث: 30 نوفمبر 2025

