"""
Patch: Add "late" status option to CRM Task

This patch adds "late" to the status options in CRM Task doctype
if it doesn't already exist.

Run during: bench migrate
Idempotent: Yes (safe to re-run)
"""

import frappe


def execute():
	"""
	Execute the patch to add "late" status to CRM Task.
	"""
	frappe.log("Running patch: add_late_status_to_crm_task")
	
	try:
		# Get the DocType meta
		meta = frappe.get_meta("CRM Task")
		status_field = meta.get_field("status")
		
		if not status_field:
			frappe.log("⚠️  Status field not found in CRM Task")
			return
		
		# Get current options
		current_options = status_field.options or ""
		options_list = [opt.strip() for opt in current_options.split("\n") if opt.strip()]
		
		# Check if "late" already exists
		if "late" in options_list:
			frappe.log("✅ 'late' status already exists in CRM Task")
			return
		
		# Add "late" to options (at the end)
		options_list.append("late")
		new_options = "\n".join(options_list)
		
		# Update the field
		frappe.db.set_value(
			"DocType Field",
			{"parent": "CRM Task", "fieldname": "status"},
			"options",
			new_options
		)
		
		# Commit the change
		frappe.db.commit()
		
		# Reload the doctype to apply changes
		frappe.reload_doctype("CRM Task")
		
		# Clear cache
		frappe.clear_cache(doctype="CRM Task")
		frappe.log("✅ Added 'late' status to CRM Task")
		
	except Exception as e:
		# Log but don't fail migration
		frappe.log_error(f"Failed to add late status: {str(e)}", "CRM Task Status Patch Error")
		frappe.log(f"⚠️  Failed to add late status: {str(e)}")

