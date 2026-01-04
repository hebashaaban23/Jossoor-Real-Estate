"""
Patch: Add "Other" and "Showing" status options to CRM Lead Status

This patch adds "Other" and "Showing" status records to CRM Lead Status doctype
if they don't already exist.

Run during: bench migrate
Idempotent: Yes (safe to re-run)
"""

import frappe


def execute():
	"""
	Execute the patch to add "Other" and "Showing" statuses to CRM Lead Status.
	"""
	frappe.log("Running patch: add_other_and_showing_lead_statuses")
	
	try:
		# Get the maximum position value to add new statuses at the end
		max_position = frappe.db.sql(
			"""
			SELECT MAX(position) as max_pos
			FROM `tabCRM Lead Status`
			""",
			as_dict=1
		)
		
		next_position = (max_position[0].max_pos or 0) + 1
		
		# Statuses to add
		statuses_to_add = [
			{"name": "Other", "color": "gray"},
			{"name": "Showing", "color": "blue"}
		]
		
		for status_info in statuses_to_add:
			status_name = status_info["name"]
			
			# Check if status already exists (name is same as lead_status due to autoname rule)
			if frappe.db.exists("CRM Lead Status", status_name):
				frappe.log(f"✅ '{status_name}' status already exists in CRM Lead Status")
				continue
			
			# Create the status record
			status_doc = frappe.get_doc({
				"doctype": "CRM Lead Status",
				"lead_status": status_name,
				"color": status_info["color"],
				"position": next_position
			})
			
			status_doc.insert(ignore_permissions=True)
			frappe.log(f"✅ Added '{status_name}' status to CRM Lead Status")
			
			next_position += 1
		
		# Commit the changes
		frappe.db.commit()
		
		# Clear cache
		frappe.clear_cache(doctype="CRM Lead Status")
		frappe.log("✅ Successfully added new lead statuses")
		
	except Exception as e:
		# Log but don't fail migration
		frappe.log_error(f"Failed to add lead statuses: {str(e)}", "CRM Lead Status Patch Error")
		frappe.log(f"⚠️  Failed to add lead statuses: {str(e)}")

