# CRM Mobile API - Test Suite Documentation

## نظرة عامة

هذا الملف يحتوي على test cases شاملة لجميع الـ APIs في `mobile_api.py`. الملف يغطي:

- ✅ **Task APIs**: create, edit, update, delete, get_all, home_tasks, main_page_buckets
- ✅ **Lead APIs**: create, edit, update, delete, get_all, get_by_id, home_leads
- ✅ **Helper APIs**: get_oauth_config, get_app_logo, get_current_user_role, get_my_team_members
- ✅ **Lookup APIs**: get_crm_leads, get_real_estate_projects, get_units, get_project_units
- ✅ **Comment APIs**: get_all_comments
- ✅ **Edge Cases**: Error handling, missing fields, non-existent records

## الملفات

- `test_mobile_api.py`: ملف الاختبارات الرئيسي
- `mobile_api.py`: ملف الـ APIs المراد اختباره

## كيفية تشغيل الاختبارات

### 1. تشغيل جميع الاختبارات

```bash
cd /home/frappe/frappe-bench-env/frappe-bench
bench --site [site-name] run-tests --module crm.api.test_mobile_api
```

### 2. تشغيل اختبار محدد

```bash
bench --site [site-name] run-tests --module crm.api.test_mobile_api.TestMobileAPI.test_create_task_minimal
```

### 3. تشغيل فئة اختبارات محددة

```bash
# اختبارات Tasks فقط
bench --site [site-name] run-tests --module crm.api.test_mobile_api.TestMobileAPI.test_create_task_minimal

# اختبارات Leads فقط
bench --site [site-name] run-tests --module crm.api.test_mobile_api.TestMobileAPI.test_create_lead_minimal
```

### 4. تشغيل من Python Console

```python
import frappe
frappe.connect()

# تشغيل جميع الاختبارات
import unittest
loader = unittest.TestLoader()
suite = loader.loadTestsFromModule(frappe.get_module("crm.api.test_mobile_api"))
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)
```

## بنية الاختبارات

### TestMobileAPI Class

كل الاختبارات موجودة في class واحد `TestMobileAPI` الذي يرث من `FrappeTestCase`.

#### Methods الرئيسية:

1. **setUp()**: يتم استدعاؤها قبل كل اختبار
   - إنشاء test data
   - إعداد المستخدم
   - تتبع السجلات المُنشأة للتنظيف

2. **tearDown()**: يتم استدعاؤها بعد كل اختبار
   - حذف السجلات المُنشأة
   - تنظيف البيانات

3. **setup_test_data()**: إنشاء البيانات الأساسية المطلوبة
   - Task Types
   - Lead Statuses
   - Lead Sources

## قائمة الاختبارات

### Task API Tests

1. ✅ `test_create_task_minimal` - إنشاء task بالحد الأدنى من الحقول
2. ✅ `test_create_task_full` - إنشاء task بكل الحقول
3. ✅ `test_edit_task` - تعديل task موجود
4. ✅ `test_update_task` - استخدام update_task (alias)
5. ✅ `test_delete_task` - حذف task
6. ✅ `test_update_status` - تحديث حالة task
7. ✅ `test_get_all_tasks` - جلب جميع tasks مع pagination
8. ✅ `test_home_tasks` - جلب tasks اليوم للصفحة الرئيسية
9. ✅ `test_main_page_buckets` - جلب buckets (today, late, upcoming)

### Lead API Tests

1. ✅ `test_create_lead_minimal` - إنشاء lead بالحد الأدنى من الحقول
2. ✅ `test_create_lead_full` - إنشاء lead بكل الحقول
3. ✅ `test_edit_lead` - تعديل lead موجود
4. ✅ `test_update_lead` - استخدام update_lead (alias)
5. ✅ `test_delete_lead` - حذف lead
6. ✅ `test_get_all_leads` - جلب جميع leads مع pagination
7. ✅ `test_get_lead_by_id` - جلب lead واحد بالـ ID
8. ✅ `test_home_leads` - جلب leads حديثة للصفحة الرئيسية

### Helper API Tests

1. ✅ `test_get_oauth_config` - جلب إعدادات OAuth
2. ✅ `test_get_app_logo` - جلب شعار التطبيق
3. ✅ `test_get_current_user_role` - جلب دور المستخدم الحالي
4. ✅ `test_get_my_team_members` - جلب أعضاء الفريق

### Lookup API Tests

1. ✅ `test_get_crm_leads` - جلب leads للبحث
2. ✅ `test_get_real_estate_projects` - جلب المشاريع العقارية
3. ✅ `test_get_units` - جلب الوحدات
4. ✅ `test_get_project_units` - جلب وحدات المشروع

### Comment API Tests

1. ✅ `test_get_all_comments` - جلب جميع التعليقات

### Edge Cases & Error Handling

1. ✅ `test_create_task_missing_required_field` - اختبار الحقول المطلوبة
2. ✅ `test_get_lead_by_id_nonexistent` - اختبار lead غير موجود
3. ✅ `test_delete_nonexistent_task` - اختبار حذف task غير موجود
4. ✅ `test_get_all_tasks_with_filters` - اختبار الفلاتر
5. ✅ `test_get_all_leads_with_filters` - اختبار فلاتر leads
6. ✅ `test_create_lead_with_comment` - إنشاء lead مع تعليق
7. ✅ `test_edit_lead_with_comment` - تعديل lead مع تعليق

## المتطلبات

- Frappe Framework مثبت
- CRM App مثبت
- Test data موجودة (Task Types, Lead Statuses, etc.)

## ملاحظات مهمة

1. **التنظيف التلقائي**: جميع السجلات المُنشأة أثناء الاختبارات يتم حذفها تلقائياً في `tearDown()`

2. **Test Data**: يتم إنشاء test data تلقائياً إذا لم تكن موجودة

3. **User Context**: جميع الاختبارات تعمل في سياق "Administrator"

4. **Database Transactions**: Frappe يقوم بعمل rollback تلقائي بعد كل اختبار

## استكشاف الأخطاء

### خطأ: "CRM Task Type not found"
- تأكد من وجود "Test Task Type" أو أن الكود يقوم بإنشائه تلقائياً

### خطأ: "Permission denied"
- تأكد من أن المستخدم له الصلاحيات المطلوبة

### خطأ: "Module not found"
- تأكد من أن المسار صحيح: `crm.api.test_mobile_api`

## أمثلة على النتائج المتوقعة

```bash
$ bench --site test-site run-tests --module crm.api.test_mobile_api

test_create_task_minimal ... ok
test_create_task_full ... ok
test_edit_task ... ok
test_update_task ... ok
test_delete_task ... ok
...
----------------------------------------------------------------------
Ran 30 tests in 15.234s

OK
```

## التطوير المستقبلي

يمكن إضافة المزيد من الاختبارات:
- اختبارات الأداء (Performance tests)
- اختبارات التكامل (Integration tests)
- اختبارات الأمان (Security tests)
- اختبارات التوافق (Compatibility tests)

## الدعم

للمساعدة أو الإبلاغ عن مشاكل:
1. راجع ملف `mobile_api.py` للفهم الكامل للـ APIs
2. راجع Frappe documentation للـ testing framework
3. تحقق من logs في Frappe

