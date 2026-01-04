# اختبار تغيير الحالة إلى Backlog

## طريقة التشغيل من Bench Console:

```bash
# 1. افتح bench console (استبدل Benchmark.com باسم الـ site الخاص بك)
bench --site Benchmark.com console

# 2. في الـ console، انسخ والصق هذا الكود:

from frappe.utils import add_days, now_datetime

print("=" * 60)
print("اختبار تغيير الحالة إلى Backlog عند تجاوز Due Date")
print("=" * 60)

# الاختبار 1: مهمة مع due_date في الماضي
print("\n📋 الاختبار 1: مهمة مع due_date في الماضي")
past_due = add_days(now_datetime(), days=-2)
task1 = frappe.get_doc({
    "doctype": "CRM Task",
    "task_type": "Meeting",
    "title": "TEST: مهمة مع due_date في الماضي",
    "status": "Todo",
    "priority": "High",
    "due_date": past_due
})

print(f"الحالة قبل الحفظ: {task1.status}")
print(f"Due Date: {past_due}")

task1.insert()

print(f"الحالة بعد الحفظ: {task1.status}")
if task1.status == "Backlog":
    print("✅ نجح! تغيرت إلى Backlog تلقائياً")
else:
    print(f"❌ فشل! الحالة: {task1.status}")

print(f"اسم المهمة: {task1.name}")

frappe.db.commit()
print("\n✅ تم الحفظ بنجاح!")
```

## أو استخدم الملف الجاهز:

```bash
bench --site Benchmark.com console
```

ثم:
```python
exec(open('apps/crm/test_backlog_console.py').read())
```

