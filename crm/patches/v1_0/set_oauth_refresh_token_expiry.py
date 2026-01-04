"""
Set OAuth refresh token expiry to 12 hours (43200 seconds) in site config.
This can be run via: bench --site <site> execute crm.patches.v1_0.set_oauth_refresh_token_expiry.execute
"""

import frappe
import json
import os


def execute():
	"""
	Set refresh token expiry to 12 hours (43200 seconds) in site config.
	"""
	try:
		# Set refresh token expiry to 12 hours (43200 seconds)
		site_config_path = frappe.get_site_path("site_config.json")
		
		# Read existing config
		if os.path.exists(site_config_path):
			with open(site_config_path, 'r') as f:
				config = json.load(f)
		else:
			config = {}
		
		# Update config
		old_value = config.get('oauth_refresh_token_expiry', 'Not set')
		config['oauth_refresh_token_expiry'] = 43200
		
		# Write back
		with open(site_config_path, 'w') as f:
			json.dump(config, f, indent=2)
		
		# Update frappe.conf
		frappe.conf.oauth_refresh_token_expiry = 43200
		
		print(f"✓ Success: oauth_refresh_token_expiry set to 43200 seconds (12 hours)")
		print(f"  Previous value: {old_value}")
		print("=== SUCCESS ===")
		return {"ok": True, "old_value": old_value, "new_value": 43200}
	except Exception as e:
		print(f"✗ Error: {str(e)}")
		print("=== ERROR ===")
		import traceback
		traceback.print_exc()
		return {"ok": False, "error": str(e)}

