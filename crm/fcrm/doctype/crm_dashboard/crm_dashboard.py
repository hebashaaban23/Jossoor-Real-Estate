# Copyright (c) 2025, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document


class CRMDashboard(Document):
	pass


def default_manager_dashboard_layout():
	"""
	Returns the default layout for the CRM Manager Dashboard.
	Dynamically creates cards for all lead statuses.
	"""
	import json
	
	# Base items (Total Leads, Delayed, Total Deals, Feedback Comments)
	layout = [
		{
			"name": "total_leads",
			"type": "number_chart",
			"tooltip": "Total number of leads",
			"link": {"name": "Leads"},  # Navigate to Leads page without filter
			"layout": {"x": 0, "y": 0, "w": 5, "h": 3, "i": "total_leads"}
		},
		{
			"name": "delayed_leads",
			"type": "number_chart",
			"tooltip": "Total number of delayed leads",
			"link": {"name": "Leads", "query": {"delayed": "1"}},  # Navigate to Leads with delayed filter
			"layout": {"x": 5, "y": 0, "w": 5, "h": 3, "i": "delayed_leads"}
		},
		{
			"name": "total_deals",
			"type": "number_chart",
			"tooltip": "Total number of deals",
			"link": {"name": "Deals"},  # Navigate to Deals page
			"layout": {"x": 10, "y": 0, "w": 5, "h": 3, "i": "total_deals"}
		},
		{
			"name": "feedback_comments",
			"type": "number_chart",
			"tooltip": "Total number of feedback",
			"layout": {"x": 15, "y": 0, "w": 5, "h": 3, "i": "feedback_comments"}
		}
	]
	
	# Get all lead statuses dynamically
	statuses = frappe.db.sql(
		"""
		SELECT lead_status, position
		FROM `tabCRM Lead Status`
		ORDER BY position ASC
		""",
		as_dict=1,
	)
	
	# Calculate grid positions for status cards
	# Grid is 20 columns wide, each card is 5 columns (4 cards per row)
	current_y = 3  # Start after first row
	current_x = 0
	card_width = 5
	card_height = 3
	cards_per_row = 4
	
	for idx, status in enumerate(statuses):
		status_name = status.lead_status
		# Create safe name for the card (remove spaces, lowercase)
		card_name = f"lead_status_{status_name.lower().replace(' ', '_').replace('-', '_')}"
		
		layout.append({
			"name": card_name,
			"type": "number_chart",
			"tooltip": f"{status_name} leads",
			"status": status_name,  # Store the actual status name
			"link": {"name": "Leads", "query": {"status": status_name}},  # Navigate to Leads with status filter
			"layout": {
				"x": current_x,
				"y": current_y,
				"w": card_width,
				"h": card_height,
				"i": card_name
			}
		})
		
		# Move to next position
		current_x += card_width
		if current_x >= 20:  # End of row
			current_x = 0
			current_y += card_height
	
	# Add charts after all status cards
	# Calculate Y position for charts
	charts_y = current_y if current_x == 0 else current_y + card_height
	
	layout.extend([
		{
			"name": "leads_by_status",
			"type": "donut_chart",
			"layout": {"x": 0, "y": charts_y, "w": 10, "h": 9, "i": "leads_by_status"}
		},
		{
			"name": "leads_by_status_chart",
			"type": "axis_chart",
			"layout": {"x": 10, "y": charts_y, "w": 10, "h": 9, "i": "leads_by_status_chart"}
		}
	])
	
	return json.dumps(layout)


def create_default_manager_dashboard(force=False):
	"""
	Creates the default CRM Manager Dashboard if it does not exist.
	"""
	if not frappe.db.exists("CRM Dashboard", "Manager Dashboard"):
		doc = frappe.new_doc("CRM Dashboard")
		doc.title = "Manager Dashboard"
		doc.layout = default_manager_dashboard_layout()
		doc.insert(ignore_permissions=True)
	elif force:
		doc = frappe.get_doc("CRM Dashboard", "Manager Dashboard")
		doc.layout = default_manager_dashboard_layout()
		doc.save(ignore_permissions=True)
	return doc.layout
