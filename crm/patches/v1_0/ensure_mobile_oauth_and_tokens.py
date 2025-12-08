"""
Patch: Ensure Mobile OAuth Client and User Tokens

This patch ensures that existing sites have the OAuth Client configured
and optionally have API keys generated for eligible users.

Run during: bench migrate
Idempotent: Yes (safe to re-run)
"""

import frappe
from crm.setup.oauth_bootstrap import run_bootstrap


def execute():
    """
    Execute the patch to bootstrap OAuth setup on current site.
    """
    frappe.log("Running patch: ensure_mobile_oauth_and_tokens")
    
    try:
        # Run bootstrap for this site
        result = run_bootstrap(include_user_keys=True)
        
        if result.get("ok"):
            frappe.log(f"✅ OAuth Client configured: {result.get('client_id')}")
            
            if "user_keys" in result:
                created = len([k for k in result["user_keys"] if k.get("status") == "created"])
                existing = len([k for k in result["user_keys"] if k.get("status") == "existing"])
                frappe.log(f"   User API keys: {created} created, {existing} existing")
        else:
            frappe.log(f"⚠️  Patch completed with warnings: {result.get('message')}")
            
    except Exception as e:
        # Log but don't fail migration
        frappe.log_error(f"OAuth patch failed: {str(e)}", "OAuth Patch Error")
        frappe.log(f"⚠️  OAuth patch failed: {str(e)}")

