"""
Patch: Update Task Type options in CRM Task

This patch updates the task_type field options in CRM Task doctype
to the new set of options.

Run during: bench migrate
Idempotent: Yes (safe to re-run)
"""

import frappe


def execute():
	"""
	Execute the patch to update Task Type options in CRM Task.
	"""
	frappe.log("Running patch: update_task_type_options")
	
	try:
		# Get the DocType meta
		meta = frappe.get_meta("CRM Task")
		task_type_field = meta.get_field("task_type")
		
		if not task_type_field:
			frappe.log("⚠️  Task Type field not found in CRM Task")
			return
		
		# New options
		new_options = "call\nproperty showing\nwhatsapp message\nteam meeting\nlead meeting\nother"
		
		# Update the field
		frappe.db.set_value(
			"DocType Field",
			{"parent": "CRM Task", "fieldname": "task_type"},
			"options",
			new_options
		)
		
		# Commit the change
		frappe.db.commit()
		
		# Reload the doctype to apply changes
		frappe.reload_doctype("CRM Task")
		
		# Clear cache
		frappe.clear_cache(doctype="CRM Task")
		frappe.log("✅ Updated Task Type options in CRM Task")
		
	except Exception as e:
		# Log but don't fail migration
		frappe.log_error(f"Failed to update task type options: {str(e)}", "CRM Task Type Options Patch Error")
		frappe.log(f"⚠️  Failed to update task type options: {str(e)}")

