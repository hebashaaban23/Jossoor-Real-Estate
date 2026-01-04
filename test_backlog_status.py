#!/usr/bin/env python3
"""
سكريبت اختبار لتجربة تغيير حالة CRM Task إلى Backlog عند تجاوز Due Date
"""
import frappe
from frappe.utils import get_datetime, now_datetime, add_days

def test_backlog_status():
    """اختبار تغيير الحالة إلى Backlog عند تجاوز Due Date"""
    
    print("=" * 60)
    print("اختبار تغيير الحالة إلى Backlog عند تجاوز Due Date")
    print("=" * 60)
    
    # تنظيف المهام التجريبية السابقة (اختياري)
    # frappe.db.sql("DELETE FROM `tabCRM Task` WHERE title LIKE 'TEST: %'")
    
    # الاختبار 1: إنشاء مهمة مع due_date في الماضي
    print("\n📋 الاختبار 1: إنشاء مهمة مع due_date في الماضي")
    print("-" * 60)
    
    past_due_date = add_days(now_datetime(), days=-2)  # قبل يومين
    task1 = frappe.get_doc({
        "doctype": "CRM Task",
        "task_type": "Meeting",
        "title": "TEST: مهمة مع due_date في الماضي",
        "status": "Todo",  # سنحاول تعيينها Todo
        "priority": "High",
        "due_date": past_due_date
    })
    
    print(f"الحالة قبل الحفظ: {task1.status}")
    print(f"Due Date: {past_due_date}")
    
    task1.insert()
    print(f"الحالة بعد الحفظ: {task1.status}")
    
    if task1.status == "Backlog":
        print("✅ نجح الاختبار! الحالة تغيرت تلقائياً إلى Backlog")
    else:
        print(f"❌ فشل الاختبار! الحالة كانت: {task1.status} (المتوقع: Backlog)")
    
    task1_name = task1.name
    print(f"اسم المهمة: {task1_name}")
    
    # الاختبار 2: إنشاء مهمة مع due_date في المستقبل
    print("\n📋 الاختبار 2: إنشاء مهمة مع due_date في المستقبل")
    print("-" * 60)
    
    future_due_date = add_days(now_datetime(), days=2)  # بعد يومين
    task2 = frappe.get_doc({
        "doctype": "CRM Task",
        "task_type": "Call",
        "title": "TEST: مهمة مع due_date في المستقبل",
        "status": "Todo",
        "priority": "Medium",
        "due_date": future_due_date
    })
    
    print(f"الحالة قبل الحفظ: {task2.status}")
    print(f"Due Date: {future_due_date}")
    
    task2.insert()
    print(f"الحالة بعد الحفظ: {task2.status}")
    
    if task2.status == "Todo":
        print("✅ نجح الاختبار! الحالة بقيت Todo (لأن due_date لم يعد بعد)")
    else:
        print(f"❌ فشل الاختبار! الحالة كانت: {task2.status} (المتوقع: Todo)")
    
    task2_name = task2.name
    print(f"اسم المهمة: {task2_name}")
    
    # الاختبار 3: تحديث مهمة موجودة مع due_date في الماضي
    print("\n📋 الاختبار 3: تحديث مهمة موجودة - تغيير due_date إلى الماضي")
    print("-" * 60)
    
    # استخدام المهمة الثانية وتغيير due_date إلى الماضي
    task2.reload()
    task2.due_date = add_days(now_datetime(), days=-1)
    task2.status = "In Progress"  # نحاول تعيينها In Progress
    
    print(f"الحالة قبل الحفظ: {task2.status}")
    print(f"Due Date الجديد: {task2.due_date}")
    
    task2.save()
    print(f"الحالة بعد الحفظ: {task2.status}")
    
    if task2.status == "Backlog":
        print("✅ نجح الاختبار! الحالة تغيرت تلقائياً إلى Backlog")
    else:
        print(f"❌ فشل الاختبار! الحالة كانت: {task2.status} (المتوقع: Backlog)")
    
    # الاختبار 4: مهمة Done لا تتغير
    print("\n📋 الاختبار 4: مهمة Done لا تتغير حتى مع due_date في الماضي")
    print("-" * 60)
    
    task3 = frappe.get_doc({
        "doctype": "CRM Task",
        "task_type": "Property Showing",
        "title": "TEST: مهمة Done مع due_date في الماضي",
        "status": "Done",
        "priority": "Low",
        "due_date": add_days(now_datetime(), days=-3)
    })
    
    print(f"الحالة قبل الحفظ: {task3.status}")
    
    task3.insert()
    print(f"الحالة بعد الحفظ: {task3.status}")
    
    if task3.status == "Done":
        print("✅ نجح الاختبار! الحالة بقيت Done (لأن المهمة مكتملة)")
    else:
        print(f"❌ فشل الاختبار! الحالة كانت: {task3.status} (المتوقع: Done)")
    
    task3_name = task3.name
    print(f"اسم المهمة: {task3_name}")
    
    print("\n" + "=" * 60)
    print("ملخص المهام المُنشأة:")
    print(f"  - {task1_name}: {task1.status}")
    print(f"  - {task2_name}: {task2.status}")
    print(f"  - {task3_name}: {task3.status}")
    print("=" * 60)
    
    return {
        "task1": task1_name,
        "task2": task2_name,
        "task3": task3_name
    }


if __name__ == "__main__":
    # للتشغيل من bench console:
    # bench --site your-site console
    # >>> exec(open('/path/to/test_backlog_status.py').read())
    # >>> test_backlog_status()
    
    # أو للتشغيل مباشرة:
    frappe.init(site="your-site-name")  # استبدل بـ اسم الـ site الخاص بك
    frappe.connect()
    test_backlog_status()
    frappe.db.commit()

