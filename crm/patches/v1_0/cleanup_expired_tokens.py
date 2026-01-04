"""
Utility script to clean up expired OAuth Bearer Tokens.
This script marks expired refresh tokens as Revoked.
"""

import frappe
from frappe.utils import now_datetime
import datetime


def execute():
	"""
	Clean up expired OAuth Bearer Tokens by marking them as Revoked.
	Usage: bench --site <site> console
	Then: from crm.patches.v1_0.cleanup_expired_tokens import execute; execute()
	"""
	# Get refresh token expiry from site config
	refresh_token_expiry = frappe.conf.get('oauth_refresh_token_expiry', 43200)
	print(f"\nRefresh Token Expiry Setting: {refresh_token_expiry} seconds ({refresh_token_expiry / 3600} hours)\n")
	
	# Get all active tokens
	all_tokens = frappe.get_all(
		"OAuth Bearer Token",
		filters={"status": "Active"},
		fields=["name", "creation", "status", "user", "refresh_token"],
		order_by="creation desc"
	)
	
	if not all_tokens:
		print("✅ No active tokens found")
		return
	
	print(f"Found {len(all_tokens)} active token(s)\n")
	
	now = now_datetime()
	expired_count = 0
	valid_count = 0
	
	for token in all_tokens:
		# Check refresh token expiry
		refresh_expiry = token.creation + datetime.timedelta(seconds=refresh_token_expiry)
		
		if now > refresh_expiry:
			# Token has expired, mark as Revoked
			try:
				frappe.db.set_value("OAuth Bearer Token", token.name, "status", "Revoked")
				expired_count += 1
				print(f"❌ Revoked expired token: {token.name} (expired {(now - refresh_expiry).total_seconds() / 3600:.2f} hours ago)")
			except Exception as e:
				print(f"⚠️  Error revoking token {token.name}: {str(e)}")
		else:
			valid_count += 1
	
	# Commit changes
	if expired_count > 0:
		frappe.db.commit()
		print(f"\n✅ Cleanup complete: {expired_count} expired token(s) revoked, {valid_count} valid token(s) remaining")
	else:
		print(f"\n✅ No expired tokens found. {valid_count} valid token(s) remaining")

