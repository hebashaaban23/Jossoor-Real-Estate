# Copyright (c) 2025, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

"""
Script to add reference fields to CRM Task doctype:
- Lead (Link to CRM Lead)
- Project (Link to Real Estate Project)
- Unit (Link to Unit)
- Project Unit (Link to Project Unit)
"""

import frappe
from frappe.custom.doctype.custom_field.custom_field import create_custom_field


def field_exists(fieldname):
	"""Check if field exists in CRM Task doctype."""
	try:
		meta = frappe.get_meta("CRM Task", cached=False)
		return meta.has_field(fieldname)
	except Exception:
		return False


def add_all_fields():
	"""Add all reference fields to CRM Task."""
	frappe.set_user("Administrator")
	
	fields_to_add = [
		{
			"fieldname": "lead",
			"label": "Lead",
			"fieldtype": "Link",
			"options": "CRM Lead",
			"insert_after": "reference_docname",
			"description": "Reference to CRM Lead"
		},
		{
			"fieldname": "project",
			"label": "Project",
			"fieldtype": "Link",
			"options": "Real Estate Project",
			"insert_after": "lead",
			"description": "Reference to Real Estate Project"
		},
		{
			"fieldname": "unit",
			"label": "Unit",
			"fieldtype": "Link",
			"options": "Unit",
			"insert_after": "project",
			"description": "Reference to Unit"
		},
		{
			"fieldname": "project_unit",
			"label": "Project Unit",
			"fieldtype": "Link",
			"options": "Project Unit",
			"insert_after": "unit",
			"description": "Reference to Project Unit"
		}
	]
	
	results = []
	
	for field_config in fields_to_add:
		fieldname = field_config["fieldname"]
		
		if field_exists(fieldname):
			results.append({
				"fieldname": fieldname,
				"success": True,
				"message": f"Field '{fieldname}' already exists"
			})
			print(f"✓ Field '{fieldname}' already exists")
			continue
		
		try:
			print(f"Adding field '{fieldname}' to CRM Task...")
			create_custom_field("CRM Task", field_config)
			frappe.db.commit()
			
			results.append({
				"fieldname": fieldname,
				"success": True,
				"message": f"Successfully added field '{fieldname}'"
			})
			print(f"✓ Successfully added field '{fieldname}'")
		except Exception as e:
			error_msg = f"Error adding field '{fieldname}': {str(e)}"
			results.append({
				"fieldname": fieldname,
				"success": False,
				"message": error_msg
			})
			print(f"✗ {error_msg}")
			frappe.log_error(f"Error adding field {fieldname}", str(e))
	
	return results


if __name__ == "__main__":
	print("=" * 60)
	print("Adding Reference Fields to CRM Task")
	print("=" * 60)
	results = add_all_fields()
	print("\n" + "=" * 60)
	print("Summary:")
	print("=" * 60)
	for result in results:
		status = "✓" if result["success"] else "✗"
		print(f"{status} {result['fieldname']}: {result['message']}")

