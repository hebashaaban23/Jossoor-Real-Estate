"""
Patch to remove custom OAuth refresh token expiry and revert to Frappe defaults.

This patch removes the oauth_refresh_token_expiry setting from site_config.json
to restore Frappe's default refresh token behavior.
"""

import frappe
import json
import os


def execute():
	"""
	Remove oauth_refresh_token_expiry from site config to use Frappe defaults.
	"""
	site_config_path = frappe.get_site_path("site_config.json")
	
	# Read existing config
	if os.path.exists(site_config_path):
		with open(site_config_path, 'r') as f:
			config = json.load(f)
		
		# Remove oauth_refresh_token_expiry if it exists
		if 'oauth_refresh_token_expiry' in config:
			del config['oauth_refresh_token_expiry']
			
			# Write back
			with open(site_config_path, 'w') as f:
				json.dump(config, f, indent=2)
			
			# Also remove from frappe.conf
			if hasattr(frappe.conf, 'oauth_refresh_token_expiry'):
				delattr(frappe.conf, 'oauth_refresh_token_expiry')
			
			frappe.msgprint("OAuth refresh token expiry setting removed. Using Frappe defaults.")
		else:
			frappe.msgprint("OAuth refresh token expiry setting not found. Already using defaults.")

