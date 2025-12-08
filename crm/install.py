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
    """
    frappe.log("Running CRM app post-install setup...")
    
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
