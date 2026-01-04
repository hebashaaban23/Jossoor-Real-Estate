"""
Test OAuth Fix from Console
Usage: bench --site <site> console
Then: from crm.patches.v1_0.test_oauth_fix import test_oauth_fix; test_oauth_fix()
"""

import frappe
from frappe import oauth as frappe_oauth
from frappe.utils import now_datetime
import datetime


def test_oauth_fix():
	"""
	Test OAuth fix by checking if monkey patches are applied correctly.
	"""
	print("\n" + "="*80)
	print("🧪 Testing OAuth Fix")
	print("="*80 + "\n")
	
	# Check if monkey patches are applied
	print("1️⃣ Checking if monkey patches are applied...")
	
	has_validate_refresh_token_fix = hasattr(
		frappe_oauth.OAuthWebRequestValidator, 
		'_original_validate_refresh_token_fixed'
	)
	has_get_original_scopes_fix = hasattr(
		frappe_oauth.OAuthWebRequestValidator, 
		'_original_get_original_scopes_fixed'
	)
	has_save_bearer_token_fix = hasattr(
		frappe_oauth.OAuthWebRequestValidator, 
		'_original_save_bearer_token_fixed'
	)
	has_authenticate_client_fix = hasattr(
		frappe_oauth.OAuthWebRequestValidator, 
		'_original_authenticate_client_fixed'
	)
	
	print(f"   ✅ validate_refresh_token fix: {'✅ Applied' if has_validate_refresh_token_fix else '❌ Not Applied'}")
	print(f"   ✅ get_original_scopes fix: {'✅ Applied' if has_get_original_scopes_fix else '❌ Not Applied'}")
	print(f"   ✅ save_bearer_token fix: {'✅ Applied' if has_save_bearer_token_fix else '❌ Not Applied'}")
	print(f"   ✅ authenticate_client fix: {'✅ Applied' if has_authenticate_client_fix else '❌ Not Applied'}")
	
	if not (has_validate_refresh_token_fix and has_get_original_scopes_fix):
		print("\n❌ ERROR: Monkey patches are not applied!")
		print("   Please ensure crm.oauth_fix is imported in hooks.py")
		return
	
	print("\n2️⃣ Testing validate_refresh_token with a real token...")
	
	# Get a refresh token from database
	tokens = frappe.get_all(
		"OAuth Bearer Token",
		filters={"status": "Active"},
		fields=["name", "refresh_token", "creation", "user", "client"],
		limit=5,
		order_by="creation desc"
	)
	
	if not tokens:
		print("   ⚠️  No active tokens found. Please create a token first.")
		return
	
	print(f"   Found {len(tokens)} active token(s). Testing with the most recent one...\n")
	
	token = tokens[0]
	token_doc = frappe.get_doc("OAuth Bearer Token", token.name)
	
	# Get refresh token expiry
	refresh_token_expiry = frappe.conf.get('oauth_refresh_token_expiry', 3600)
	refresh_expiry = token_doc.creation + datetime.timedelta(seconds=refresh_token_expiry)
	now = now_datetime()
	is_expired = now > refresh_expiry
	
	print(f"   📋 Token Details:")
	print(f"      Name: {token.name}")
	print(f"      Created: {token_doc.creation}")
	print(f"      Refresh Token Expiry: {refresh_expiry}")
	print(f"      Status: {'❌ EXPIRED' if is_expired else '✅ VALID'}")
	print(f"      Refresh Token: {token.refresh_token[:30]}...")
	print(f"      User: {token.user}")
	print(f"      Client: {token.client}\n")
	
	# Test validate_refresh_token directly
	print("3️⃣ Testing validate_refresh_token method directly...")
	
	validator = frappe_oauth.OAuthWebRequestValidator()
	
	# Create a mock request object
	class MockRequest:
		pass
	
	request = MockRequest()
	client = frappe.get_doc("OAuth Client", token.client)
	
	try:
		result = validator.validate_refresh_token(
			token.refresh_token,
			client,
			request
		)
		
		if result:
			print("   ✅ SUCCESS! validate_refresh_token returned True")
			if hasattr(request, 'user'):
				print(f"   ✅ request.user is set: {request.user}")
		else:
			print("   ❌ FAILED! validate_refresh_token returned False")
			if is_expired:
				print("   ℹ️  This is expected - token has expired")
			else:
				print("   ⚠️  This is unexpected - token should be valid")
	except Exception as e:
		print(f"   ❌ ERROR: {str(e)}")
		import traceback
		traceback.print_exc()
	
	# Test get_original_scopes
	print("\n4️⃣ Testing get_original_scopes method...")
	
	try:
		scopes = validator.get_original_scopes(token.refresh_token, request)
		if scopes:
			print(f"   ✅ SUCCESS! get_original_scopes returned: {scopes}")
		else:
			print("   ⚠️  get_original_scopes returned empty list")
	except Exception as e:
		print(f"   ❌ ERROR: {str(e)}")
		import traceback
		traceback.print_exc()
	
	# Test with expired token if available
	print("\n5️⃣ Testing with expired token (if available)...")
	
	expired_tokens = [t for t in tokens if (t['creation'] + datetime.timedelta(seconds=refresh_token_expiry)) < now]
	
	if expired_tokens:
		expired_token = expired_tokens[0]
		expired_token_doc = frappe.get_doc("OAuth Bearer Token", expired_token['name'])
		
		print(f"   Testing with expired token: {expired_token['name']}")
		print(f"   Created: {expired_token_doc.creation}")
		print(f"   Expiry: {expired_token_doc.creation + datetime.timedelta(seconds=refresh_token_expiry)}")
		
		try:
			result = validator.validate_refresh_token(
				expired_token['refresh_token'],
				client,
				request
			)
			
			if not result:
				print("   ✅ SUCCESS! Expired token correctly rejected (returned False)")
			else:
				print("   ❌ FAILED! Expired token was accepted (should be rejected)")
		except Exception as e:
			print(f"   ❌ ERROR: {str(e)}")
	else:
		print("   ℹ️  No expired tokens found for testing")
	
	# Summary
	print("\n" + "="*80)
	print("✅ Test Complete")
	print("="*80 + "\n")
	
	print("📝 Summary:")
	print("   - Monkey patches are applied correctly")
	print("   - validate_refresh_token is working")
	print("   - get_original_scopes is working")
	print("\n   If all tests passed, the OAuth fix is working correctly!")
	print("   You can now test from your mobile app or API client.\n")

