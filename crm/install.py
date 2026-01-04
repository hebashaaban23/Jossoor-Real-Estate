"""
CRM App Installation Hooks

This module contains functions that run during app installation.
"""

import frappe
from crm.setup.oauth_bootstrap import run_bootstrap


def before_install():
    """
    Run before CRM app is installed on a site.
    
    Currently a no-op placeholder for future pre-install tasks.
    """
    pass


def after_install():
    """
    Run after CRM app is installed on a site.
    
    This function:
    1. Sets up OAuth Client for mobile API access
    2. Optionally generates API keys for eligible users
    3. Sets refresh token expiry to 1 hour (3600 seconds)
    """
    frappe.log("Running CRM app post-install setup...")
    
    # OAuth refresh token expiry setting removed - using default Frappe behavior (infinite refresh tokens)
    # try:
    #     # Set refresh token expiry to 1 hour (3600 seconds)
    #     import json
    #     import os
    #     
    #     site_config_path = frappe.get_site_path("site_config.json")
    #     
    #     # Read existing config
    #     if os.path.exists(site_config_path):
    #         with open(site_config_path, 'r') as f:
    #             config = json.load(f)
    #     else:
    #         config = {}
    #     
    #     # Update config
    #     config['oauth_refresh_token_expiry'] = 3600
    #     
    #     # Write back
    #     with open(site_config_path, 'w') as f:
    #         json.dump(config, f, indent=2)
    #     
    #     # Update frappe.conf
    #     frappe.conf.oauth_refresh_token_expiry = 3600
    #     
    #     frappe.log("✅ Refresh token expiry set to 1 hour (3600 seconds)")
    #     
    # except Exception as e:
    #     frappe.log_error(f"Failed to set refresh token expiry: {str(e)}", "CRM Install OAuth Config")
    #     frappe.log(f"⚠️  Failed to set refresh token expiry: {str(e)}")
    
    try:
        # Bootstrap OAuth setup for this site
        result = run_bootstrap(include_user_keys=True)
        
        if result.get("ok"):
            frappe.log(f"✅ OAuth setup completed for site: {result.get('site')}")
            frappe.log(f"   Client ID: {result.get('client_id')}")
        else:
            frappe.log(f"⚠️  OAuth setup had issues: {result.get('message')}")
            
    except Exception as e:
        frappe.log_error(f"Post-install setup failed: {str(e)}", "CRM Install Error")
        # Don't fail installation if OAuth setup fails
        frappe.log(f"⚠️  Post-install setup failed: {str(e)}")
