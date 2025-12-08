"""
OAuth2 Bootstrap Module for CRM Mobile API

This module handles:
1. Ensuring OAuth Provider is installed and active on the current site
2. Creating/updating OAuth Client with PKCE, Password, and Refresh Token grants
3. Optionally generating API keys for users with specific roles
4. Providing a whitelisted bootstrap function for manual/scripted setup

All operations are idempotent and safe to re-run.
"""

import frappe
from frappe import _
from frappe.utils import cstr
from crm.setup.config import (
    OAUTH_APP_NAME,
    OAUTH_CLIENT_NAME,
    DEFAULT_REDIRECT_URIS,
    DEFAULT_REDIRECT_URI,
    DEFAULT_SCOPES,
    GRANT_TYPES,
    ALLOWED_ROLES,
    SKIP_USERS,
)


def ensure_oauth_provider():
    """
    Ensure OAuth Provider app is installed and active on current site.
    
    Returns:
        bool: True if provider is ready, False otherwise
    """
    try:
        # Check if OAuth Provider doctype exists
        if not frappe.db.exists("DocType", "OAuth Client"):
            frappe.log_error(
                "OAuth Provider not installed. Please install frappe.integrations.oauth2_provider",
                "OAuth Bootstrap Error"
            )
            return False
        
        return True
    except Exception as e:
        frappe.log_error(f"Error checking OAuth Provider: {str(e)}", "OAuth Bootstrap Error")
        return False


def get_or_create_oauth_client(print_secret=False):
    """
    Create or update the OAuth Client for mobile app access.
    
    This function is idempotent - it will create the client if it doesn't exist,
    or update it if it does.
    
    Args:
        print_secret (bool): If True, include client_secret in response
    
    Returns:
        dict: {
            "client_id": str,
            "client_secret": str (only if print_secret=True),
            "redirect_uris": list,
            "grant_types": list
        }
    """
    if not ensure_oauth_provider():
        frappe.throw(_("OAuth Provider is not available on this site"))
    
    # Check if client already exists
    existing = frappe.db.get_value(
        "OAuth Client",
        {"app_name": OAUTH_APP_NAME},
        ["name", "client_id", "client_secret"],
        as_dict=True
    )
    
    if existing:
        # Update existing client
        client_doc = frappe.get_doc("OAuth Client", existing.name)
        _update_client_settings(client_doc)
        client_doc.save(ignore_permissions=True)
        
        result = {
            "client_id": client_doc.client_id,
            "redirect_uris": DEFAULT_REDIRECT_URIS,
            "grant_types": [k for k, v in GRANT_TYPES.items() if v],
        }
        
        if print_secret:
            result["client_secret"] = client_doc.get_password("client_secret")
        
        return result
    else:
        # Create new client
        client_doc = frappe.new_doc("OAuth Client")
        client_doc.app_name = OAUTH_APP_NAME
        client_doc.client_name = OAUTH_CLIENT_NAME
        
        _update_client_settings(client_doc)
        
        client_doc.insert(ignore_permissions=True)
        frappe.db.commit()
        
        result = {
            "client_id": client_doc.client_id,
            "redirect_uris": DEFAULT_REDIRECT_URIS,
            "grant_types": [k for k, v in GRANT_TYPES.items() if v],
        }
        
        if print_secret:
            result["client_secret"] = client_doc.get_password("client_secret")
        
        return result


def _update_client_settings(client_doc):
    """
    Update OAuth Client document with required settings.
    
    Args:
        client_doc: OAuth Client document instance
    """
    # Set redirect URIs
    client_doc.redirect_uris = "\n".join(DEFAULT_REDIRECT_URIS)
    client_doc.default_redirect_uri = DEFAULT_REDIRECT_URI
    
    # Set scopes
    client_doc.scopes = DEFAULT_SCOPES
    
    # Enable grant types
    client_doc.grant_type = "Authorization Code"  # This enables PKCE
    client_doc.response_type = "Code"
    
    # Enable skip authorization (for trusted first-party apps)
    client_doc.skip_authorization = 1
    
    # Note: Password grant and refresh token are handled by Frappe OAuth2 provider
    # based on the request parameters, not stored in OAuth Client settings


def generate_api_keys_for_eligible_users():
    """
    Generate API key/secret pairs for users with eligible roles.
    
    This is optional and can be used for direct API access without OAuth flow.
    Users must have at least one of the ALLOWED_ROLES.
    
    Returns:
        list: List of dicts with user and key info (no secrets logged)
    """
    results = []
    
    # Get users with eligible roles
    users_with_roles = frappe.db.sql("""
        SELECT DISTINCT parent as user
        FROM `tabHas Role`
        WHERE role IN %(roles)s
        AND parent NOT IN %(skip_users)s
        AND parenttype = 'User'
    """, {
        "roles": ALLOWED_ROLES,
        "skip_users": SKIP_USERS
    }, as_dict=True)
    
    for user_row in users_with_roles:
        user = user_row.user
        
        try:
            # Check if user already has API key
            existing_key = frappe.db.get_value(
                "User",
                user,
                "api_key"
            )
            
            if existing_key:
                results.append({
                    "user": user,
                    "status": "existing",
                    "message": "API key already exists"
                })
            else:
                # Generate new API key
                user_doc = frappe.get_doc("User", user)
                api_key = frappe.generate_hash(length=15)
                api_secret = frappe.generate_hash(length=15)
                
                user_doc.api_key = api_key
                user_doc.api_secret = api_secret
                user_doc.save(ignore_permissions=True)
                
                results.append({
                    "user": user,
                    "status": "created",
                    "message": "API key generated successfully"
                })
        except Exception as e:
            results.append({
                "user": user,
                "status": "error",
                "message": str(e)
            })
    
    return results


@frappe.whitelist()
def bootstrap_site(print_client_secret=0, generate_user_keys=0):
    """
    Bootstrap OAuth2 setup for the current site.
    
    This is the main entry point for setting up OAuth on a site.
    It can be called via bench console, API, or scripts.
    
    Args:
        print_client_secret (int/bool): If 1/True, include client_secret in response
        generate_user_keys (int/bool): If 1/True, generate API keys for eligible users
    
    Returns:
        dict: {
            "ok": bool,
            "site": str,
            "client_id": str,
            "client_secret": str (only if print_client_secret=1),
            "user_keys": list (only if generate_user_keys=1),
            "message": str
        }
    """
    # Convert to boolean
    print_secret = bool(int(print_client_secret))
    gen_keys = bool(int(generate_user_keys))
    
    try:
        site = frappe.local.site
        
        # Ensure OAuth Provider is ready
        if not ensure_oauth_provider():
            return {
                "ok": False,
                "site": site,
                "message": "OAuth Provider not available. Please install frappe.integrations."
            }
        
        # Create/update OAuth Client
        client_info = get_or_create_oauth_client(print_secret=print_secret)
        
        result = {
            "ok": True,
            "site": site,
            "client_id": client_info["client_id"],
            "redirect_uris": client_info["redirect_uris"],
            "grant_types": client_info["grant_types"],
            "message": "OAuth Client configured successfully"
        }
        
        # Include client_secret if requested
        if print_secret and "client_secret" in client_info:
            result["client_secret"] = client_info["client_secret"]
        
        # Generate user API keys if requested
        if gen_keys:
            user_keys = generate_api_keys_for_eligible_users()
            result["user_keys"] = user_keys
            result["message"] += f". Generated/verified {len(user_keys)} user API keys."
        
        frappe.db.commit()
        
        return result
        
    except Exception as e:
        frappe.log_error(f"OAuth bootstrap failed: {str(e)}", "OAuth Bootstrap Error")
        return {
            "ok": False,
            "site": frappe.local.site if frappe.local else "unknown",
            "message": f"Bootstrap failed: {str(e)}"
        }


def run_bootstrap(include_user_keys=False):
    """
    Run bootstrap without whitelisting (for internal use in hooks/patches).
    
    Args:
        include_user_keys (bool): Whether to generate user API keys
    
    Returns:
        dict: Bootstrap result
    """
    return bootstrap_site(
        print_client_secret=0,
        generate_user_keys=1 if include_user_keys else 0
    )

