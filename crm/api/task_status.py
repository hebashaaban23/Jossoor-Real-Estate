"""
تحديث حالة CRM Task تلقائياً عند تجاوز Due Date
"""
import frappe
from frappe.utils import get_datetime, now_datetime


def check_and_update_task_status(doc, method=None):
	"""
	التحقق من due_date وتحديث الحالة إلى late إذا تجاوزت
	تُستدعى من on_load event
	"""
	if not doc.due_date or not doc.name:
		return
	
	due_datetime = get_datetime(doc.due_date)
	current_datetime = now_datetime()
	
	# إذا تجاوز due_date والحالة ليست Done أو late، غيّرها إلى late
	if due_datetime < current_datetime:
		if doc.status not in ["Done", "late"]:
			try:
				# تحديث الحالة مباشرة في قاعدة البيانات
				frappe.db.set_value("CRM Task", doc.name, "status", "late", update_modified=False)
				# تحديث الحالة في الـ document object أيضاً
				doc.status = "late"
			except Exception:
				# في حالة وجود خطأ، تجاهل
				pass


def update_overdue_tasks():
	"""
	مهمة مجدولة: تحديث جميع المهام المتأخرة تلقائياً
	"""
	try:
		current_dt = now_datetime()
		# تحديث مباشر في قاعدة البيانات باستخدام SQL واحد (أسرع)
		result = frappe.db.sql("""
			UPDATE `tabCRM Task`
			SET status = 'late'
			WHERE due_date < %s
			AND status NOT IN ('Done', 'late')
			AND due_date IS NOT NULL
		""", (current_dt,))
		
		updated = frappe.db.affected_rows()
		
		if updated > 0:
			frappe.db.commit()
			frappe.logger().info(f"Updated {updated} CRM Tasks to late status via scheduled job")
		
		return {"updated": updated}
	except Exception as e:
		frappe.log_error(f"Error updating overdue tasks: {str(e)}")
		return {"error": str(e)}


def update_single_task_status(task_name):
	"""
	تحديث حالة مهمة واحدة
	"""
	try:
		task = frappe.get_doc("CRM Task", task_name)
		if not task.due_date:
			return False
		
		due_datetime = get_datetime(task.due_date)
		current_datetime = now_datetime()
		
		if due_datetime < current_datetime and task.status not in ["Done", "late"]:
			task.status = "late"
			task.save(ignore_permissions=True)
			return True
		
		return False
	except Exception as e:
		frappe.log_error(f"Error updating task {task_name}: {str(e)}")
		return False


def update_all_overdue_tasks_script():
	"""
	سكريبت لتحديث جميع المهام المتأخرة (يمكن استدعاؤها من bench execute)
	"""
	result = update_all_overdue_tasks()
	print(f"تم تحديث {result.get('updated', 0)} مهمة")
	return result


@frappe.whitelist()
def update_all_overdue_tasks_now():
	"""
	تحديث جميع المهام المتأخرة إلى late (يمكن استدعاؤها من API)
	"""
	try:
		# البحث عن جميع المهام المتأخرة
		tasks = frappe.db.sql("""
			SELECT name, title, status, due_date
			FROM `tabCRM Task`
			WHERE due_date < %s
			AND status NOT IN ('Done', 'late')
			AND due_date IS NOT NULL
			ORDER BY due_date ASC
		""", (now_datetime(),), as_dict=True)
		
		updated = 0
		skipped = 0
		errors = []
		
		for task in tasks:
			try:
				frappe.db.set_value("CRM Task", task.name, "status", "late", update_modified=False)
				updated += 1
			except Exception as e:
				skipped += 1
				errors.append(f"{task.name}: {str(e)}")
		
		if updated > 0:
			frappe.db.commit()
		
		return {
			"success": True,
			"total_found": len(tasks),
			"updated": updated,
			"skipped": skipped,
			"errors": errors if errors else None
		}
	except Exception as e:
		frappe.log_error(f"Error updating all overdue tasks: {str(e)}")
		return {
			"success": False,
			"error": str(e)
		}

