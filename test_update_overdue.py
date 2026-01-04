# سكريبت اختبار لتحديث المهام المتأخرة
# للتشغيل: bench --site your-site console
# ثم: exec(open('apps/crm/test_update_overdue.py').read())

from crm.api.task_status import update_overdue_tasks

print("=" * 60)
print("اختبار تحديث المهام المتأخرة تلقائياً")
print("=" * 60)

result = update_overdue_tasks()

print(f"\nالنتيجة: {result}")
print("\n✅ تم التحديث بنجاح!")

