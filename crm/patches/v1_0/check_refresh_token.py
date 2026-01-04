"""
Utility script to check refresh token status in database.
Run this to debug refresh token issues.
"""

import frappe
from frappe.utils import now_datetime
import datetime


def execute():
	"""
	Check refresh token status for debugging.
	Usage: bench --site <site> console
	Then: from crm.patches.v1_0.check_refresh_token import execute; execute()
	"""
	refresh_token = input("Enter refresh token to check: ").strip()
	
	if not refresh_token:
		print("No refresh token provided")
		return
	
	# Get refresh token expiry from site config
	refresh_token_expiry = frappe.conf.get('oauth_refresh_token_expiry', 43200)
	print(f"\nRefresh Token Expiry Setting: {refresh_token_expiry} seconds ({refresh_token_expiry / 3600} hours)")
	
	# Search for token
	tokens = frappe.get_all(
		"OAuth Bearer Token",
		filters={
			"refresh_token": refresh_token
		},
		fields=["name", "creation", "status", "user", "client", "expiration_time"],
		limit=1
	)
	
	if not tokens:
		print(f"\n❌ Token NOT FOUND in database")
		print(f"Refresh token: {refresh_token[:20]}...")
		return
	
	token = tokens[0]
	print(f"\n✅ Token FOUND:")
	print(f"  Name: {token.name}")
	print(f"  Status: {token.status}")
	print(f"  User: {token.user}")
	print(f"  Client: {token.client}")
	print(f"  Created: {token.creation}")
	print(f"  Access Token Expiry: {token.expiration_time}")
	
	# Check if token is active
	if token.status != "Active":
		print(f"\n⚠️  Token is NOT Active (Status: {token.status})")
		return
	
	# Check refresh token expiry
	refresh_expiry = token.creation + datetime.timedelta(seconds=refresh_token_expiry)
	now = now_datetime()
	
	print(f"\nRefresh Token Expiry Check:")
	print(f"  Created: {token.creation}")
	print(f"  Expires: {refresh_expiry}")
	print(f"  Now: {now}")
	print(f"  Time Remaining: {refresh_expiry - now}")
	
	if now > refresh_expiry:
		print(f"\n❌ Refresh Token has EXPIRED")
		print(f"  Expired {(now - refresh_expiry).total_seconds() / 3600:.2f} hours ago")
	else:
		print(f"\n✅ Refresh Token is VALID")
		print(f"  Valid for {(refresh_expiry - now).total_seconds() / 3600:.2f} more hours")

