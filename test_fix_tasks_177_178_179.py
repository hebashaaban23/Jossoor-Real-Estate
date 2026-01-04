# سكريبت لتحديث المهام 177, 178, 179 مباشرة
# للتشغيل: bench --site Benchmark.com console
# ثم: exec(open('apps/crm/test_fix_tasks_177_178_179.py').read())

import frappe
from frappe.utils import now_datetime, get_datetime

print("=" * 60)
print("تحديث المهام 177, 178, 179")
print("=" * 60)

tasks = frappe.db.sql("""
    SELECT name, title, status, due_date
    FROM `tabCRM Task`
    WHERE name IN ('177', '178', '179')
""", as_dict=True)

print(f"\nتم العثور على {len(tasks)} مهمة")
print("-" * 60)

current_dt = now_datetime()

for task in tasks:
    print(f"\nالمهمة {task.name}:")
    print(f"  Status الحالي: {task.status}")
    print(f"  Due Date: {task.due_date}")
    
    if task.due_date:
        due_dt = get_datetime(task.due_date)
        is_overdue = due_dt < current_dt
        print(f"  متأخرة: {is_overdue}")
        
        if is_overdue and task.status not in ["Done", "Backlog"]:
            try:
                frappe.db.set_value("CRM Task", task.name, "status", "Backlog", update_modified=False)
                print(f"  ✅ تم التحديث إلى Backlog")
            except Exception as e:
                print(f"  ❌ خطأ: {str(e)}")
        else:
            print(f"  ℹ️  لا تحتاج تحديث (Status: {task.status})")

frappe.db.commit()
print("\n" + "=" * 60)
print("✅ تم الانتهاء!")

