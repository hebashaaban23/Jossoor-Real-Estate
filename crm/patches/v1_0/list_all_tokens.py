"""
Utility script to list all OAuth Bearer Tokens in database.
Run this to see all tokens and their status.
"""

import frappe
from frappe.utils import now_datetime
import datetime


def execute():
	"""
	List all OAuth Bearer Tokens for debugging.
	Usage: bench --site <site> console
	Then: from crm.patches.v1_0.list_all_tokens import execute; execute()
	"""
	# Get refresh token expiry from site config
	refresh_token_expiry = frappe.conf.get('oauth_refresh_token_expiry', 43200)
	print(f"\nRefresh Token Expiry Setting: {refresh_token_expiry} seconds ({refresh_token_expiry / 3600} hours)\n")
	
	# Get all tokens
	tokens = frappe.get_all(
		"OAuth Bearer Token",
		fields=["name", "creation", "status", "user", "client", "expiration_time", "refresh_token"],
		order_by="creation desc",
		limit=50
	)
	
	if not tokens:
		print("❌ No tokens found in database")
		return
	
	print(f"✅ Found {len(tokens)} token(s):\n")
	print("-" * 100)
	
	now = now_datetime()
	
	for i, token in enumerate(tokens, 1):
		# Check refresh token expiry
		refresh_expiry = token.creation + datetime.timedelta(seconds=refresh_token_expiry)
		is_expired = now > refresh_expiry
		
		status_icon = "✅" if token.status == "Active" and not is_expired else "❌"
		
		print(f"\n{status_icon} Token #{i}:")
		print(f"  Name: {token.name}")
		print(f"  Status: {token.status}")
		print(f"  User: {token.user}")
		print(f"  Client: {token.client}")
		print(f"  Created: {token.creation}")
		print(f"  Access Token Expiry: {token.expiration_time}")
		print(f"  Refresh Token Expiry: {refresh_expiry}")
		print(f"  Refresh Token: {token.refresh_token[:30] if token.refresh_token else 'None'}...")
		
		if is_expired:
			print(f"  ⚠️  Refresh Token EXPIRED (expired {(now - refresh_expiry).total_seconds() / 3600:.2f} hours ago)")
		elif token.status == "Active":
			remaining = (refresh_expiry - now).total_seconds() / 3600
			print(f"  ✅ Refresh Token VALID (valid for {remaining:.2f} more hours)")
		
		print("-" * 100)

