#!/usr/bin/env python3
"""
Script to update the CRM Dashboard with the new layout.
Run this script to apply the new dashboard changes.

Usage:
    python3 update_dashboard.py [site_name]
    
Example:
    python3 update_dashboard.py Trust.com
"""

import sys
import frappe
from frappe import _

def update_dashboard(site_name=None):
	"""Update the Manager Dashboard with the new layout."""
	if not site_name:
		print("Please provide site name as argument.")
		print("Usage: python3 update_dashboard.py [site_name]")
		print("\nAvailable sites:")
		import os
		sites_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'sites')
		sites = [d for d in os.listdir(sites_path) if os.path.isdir(os.path.join(sites_path, d)) and not d.startswith('.')]
		sites = [s for s in sites if s not in ['assets', 'common_site_config.json']]
		for site in sites:
			if '.' in site:
				print(f"  • {site}")
		sys.exit(1)
	
	frappe.init(site=site_name)
	frappe.connect()
	
	try:
		# Import the create function
		from crm.fcrm.doctype.crm_dashboard.crm_dashboard import create_default_manager_dashboard
		
		# Force reset to default
		print(f"Updating CRM Dashboard for site: {site_name}...")
		create_default_manager_dashboard(force=True)
		frappe.db.commit()
		
		print("✓ Dashboard updated successfully!")
		print("\nThe new dashboard includes:")
		print("  • Total Leads")
		print("  • Delayed Leads")
		print("  • Total Deals")
		print("  • New Leads")
		print("  • Contacted Leads")
		print("  • Nurture Leads")
		print("  • Qualified Leads")
		print("  • Unqualified Leads")
		print("  • Junk Leads")
		print("  • Leads by Status (Donut Chart)")
		print("  • Leads by Status (Bar Chart)")
		print("\nPlease refresh your browser to see the changes.")
		
	except Exception as e:
		print(f"✗ Error updating dashboard: {e}")
		import traceback
		traceback.print_exc()
		frappe.db.rollback()
	
	finally:
		frappe.destroy()

if __name__ == "__main__":
	site_name = sys.argv[1] if len(sys.argv) > 1 else None
	update_dashboard(site_name)

