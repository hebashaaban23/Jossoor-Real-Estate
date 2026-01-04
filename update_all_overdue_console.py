# للاستخدام المباشر من bench console:
# bench --site Benchmark.com console
# ثم انسخ والصق هذا الكود:

import frappe
from frappe.utils import now_datetime

print("=" * 60)
print("تحويل جميع المهام المتأخرة إلى Backlog")
print("=" * 60)

# البحث عن جميع المهام المتأخرة
tasks = frappe.db.sql("""
    SELECT name, title, status, due_date
    FROM `tabCRM Task`
    WHERE due_date < %s
    AND status NOT IN ('Done', 'Backlog')
    AND due_date IS NOT NULL
    ORDER BY due_date ASC
""", (now_datetime(),), as_dict=True)

print(f"\nتم العثور على {len(tasks)} مهمة متأخرة")
print("-" * 60)

if len(tasks) == 0:
    print("✅ لا توجد مهام متأخرة!")
else:
    updated = 0
    skipped = 0
    
    for task in tasks:
        try:
            # تحديث الحالة إلى Backlog
            frappe.db.set_value("CRM Task", task.name, "status", "Backlog", update_modified=False)
            updated += 1
            print(f"✅ {task.name}: {task.title[:50] if task.title else 'بدون عنوان'}... ({task.status} → Backlog)")
        except Exception as e:
            skipped += 1
            print(f"❌ {task.name}: خطأ - {str(e)}")
    
    # حفظ التغييرات
    if updated > 0:
        frappe.db.commit()
        print("\n" + "=" * 60)
        print(f"✅ تم تحديث {updated} مهمة إلى Backlog")
        if skipped > 0:
            print(f"⚠️  تم تخطي {skipped} مهمة بسبب أخطاء")
    else:
        print("\n⚠️  لم يتم تحديث أي مهام")

print("=" * 60)

