"""
Patch to set refresh token expiry to 12 hours (43200 seconds).

This patch modifies the OAuth2 token generation to use refresh token expiry time of 12 hours.
"""

import frappe
import json
import os


def execute():
	"""
	Set refresh token expiry to 12 hours (43200 seconds) in site config.
	"""
	site_config_path = frappe.get_site_path("site_config.json")
	
	# Set refresh token expiry to 12 hours (43200 seconds)
	frappe.conf.oauth_refresh_token_expiry = 43200
	
	# Read existing config
	if os.path.exists(site_config_path):
		with open(site_config_path, 'r') as f:
			config = json.load(f)
	else:
		config = {}
	
	# Update config
	config['oauth_refresh_token_expiry'] = 43200
	
	# Write back
	with open(site_config_path, 'w') as f:
		json.dump(config, f, indent=2)
	
	frappe.msgprint("Refresh token expiry set to 12 hours (43200 seconds)")

