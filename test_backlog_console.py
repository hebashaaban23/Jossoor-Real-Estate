# للاستخدام من bench console:
# bench --site your-site-name console
# ثم انسخ والصق هذا الكود:

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

# الاختبار 2: مهمة مع due_date في المستقبل
print("\n📋 الاختبار 2: مهمة مع due_date في المستقبل")
future_due = add_days(now_datetime(), days=2)
task2 = frappe.get_doc({
    "doctype": "CRM Task",
    "task_type": "Call",
    "title": "TEST: مهمة مع due_date في المستقبل",
    "status": "Todo",
    "priority": "Medium",
    "due_date": future_due
})

print(f"الحالة قبل الحفظ: {task2.status}")
task2.insert()
print(f"الحالة بعد الحفظ: {task2.status}")
if task2.status == "Todo":
    print("✅ نجح! بقيت Todo")
else:
    print(f"❌ فشل! الحالة: {task2.status}")

# الاختبار 3: مهمة Done لا تتغير
print("\n📋 الاختبار 3: مهمة Done لا تتغير")
task3 = frappe.get_doc({
    "doctype": "CRM Task",
    "task_type": "Property Showing",
    "title": "TEST: مهمة Done",
    "status": "Done",
    "priority": "Low",
    "due_date": add_days(now_datetime(), days=-3)
})

print(f"الحالة قبل الحفظ: {task3.status}")
task3.insert()
print(f"الحالة بعد الحفظ: {task3.status}")
if task3.status == "Done":
    print("✅ نجح! بقيت Done")
else:
    print(f"❌ فشل! الحالة: {task3.status}")

frappe.db.commit()
print("\n✅ تم الحفظ بنجاح!")

