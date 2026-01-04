"""
Test script to check if refresh token validation works correctly.
This script tests the refresh token endpoint to see if invalid_grant error still exists.
"""

import frappe
import requests
import json


def execute():
	"""
	Test refresh token validation.
	Usage: bench --site <site> console
	Then: from crm.patches.v1_0.test_refresh_token import execute; execute()
	"""
	print("\n" + "="*80)
	print("🧪 Testing Refresh Token Validation")
	print("="*80 + "\n")
	
	# Get site URL - try multiple methods to get the correct URL
	site_url = None
	current_site = frappe.local.site
	
	# Method 1: Try from site config (most reliable)
	if frappe.conf.get('site_url'):
		site_url = frappe.conf.get('site_url')
		# Ensure it starts with http:// or https://
		if site_url and not site_url.startswith('http'):
			site_url = f"https://{site_url}"
	
	# Method 2: Try host_name from config
	if not site_url and frappe.conf.get('host_name'):
		protocol = 'https' if frappe.conf.get('ssl_certificate') else 'http'
		site_url = f"{protocol}://{frappe.conf.get('host_name')}"
	
	# Method 3: Try domains from config
	if not site_url:
		site_config = frappe.get_site_config()
		if 'domains' in site_config and site_config['domains']:
			domains = site_config['domains']
			if isinstance(domains, list) and len(domains) > 0:
				# Use first domain
				domain = domains[0].strip()
				site_url = f"https://{domain}"
			elif isinstance(domains, str):
				# Comma-separated
				domain = domains.split(',')[0].strip()
				site_url = f"https://{domain}"
	
	# Method 4: Construct from site name (fallback)
	if not site_url or not site_url.startswith('http'):
		# Try common patterns based on site name
		site_name_lower = current_site.lower()
		
		# Known mappings
		if 'trust' in site_name_lower:
			site_url = 'https://trust.jossoor.org'
		elif 'benchmark' in site_name_lower:
			site_url = 'https://benchmark.jossoor.org'
		elif 'jossoor' in site_name_lower and '.com' in current_site:
			site_url = 'https://jossoor.jossoor.org'
		else:
			# Try to construct: remove .com, .local, .org and add .jossoor.org
			site_name_clean = current_site.replace('.com', '').replace('.local', '').replace('.org', '').lower()
			# Skip if it's just a number or very short
			if len(site_name_clean) > 2:
				site_url = f"https://{site_name_clean}.jossoor.org"
			else:
				# Last resort: use site name as-is with https
				site_url = f"https://{current_site.lower()}"
	
	print(f"📍 Site URL: {site_url}\n")
	
	# Get a valid refresh token from database
	from frappe.utils import now_datetime
	import datetime
	
	refresh_token_expiry = frappe.conf.get('oauth_refresh_token_expiry', 3600)
	
	# Get all active tokens
	tokens = frappe.get_all(
		"OAuth Bearer Token",
		filters={"status": "Active"},
		fields=["name", "creation", "refresh_token", "client"],
		order_by="creation desc",
		limit=5
	)
	
	if not tokens:
		print("❌ No active tokens found in database")
		print("   Please login to the mobile app first to create a token")
		return
	
	print(f"Found {len(tokens)} active token(s). Testing with the most recent one...\n")
	
	# Test with the most recent token
	token = tokens[0]
	now = now_datetime()
	refresh_expiry = token.creation + datetime.timedelta(seconds=refresh_token_expiry)
	is_token_valid = now < refresh_expiry
	
	print(f"📋 Token Details:")
	print(f"   Name: {token.name}")
	print(f"   Created: {token.creation}")
	print(f"   Refresh Token Expiry: {refresh_expiry}")
	print(f"   Status: {'✅ VALID' if is_token_valid else '❌ EXPIRED'}")
	print(f"   Refresh Token: {token.refresh_token[:30]}...")
	print()
	
	# Get client_id
	client_id = token.client
	print(f"📋 Client ID: {client_id}\n")
	
	# Test refresh token endpoint
	print("🔄 Testing refresh token endpoint...\n")
	
	url = f"{site_url}/api/method/frappe.integrations.oauth2.get_token"
	
	data = {
		"grant_type": "refresh_token",
		"refresh_token": token.refresh_token,
		"client_id": client_id
	}
	
	headers = {
		"Content-Type": "application/x-www-form-urlencoded"
	}
	
	try:
		response = requests.post(url, data=data, headers=headers, timeout=10)
		
		print(f"📤 Request:")
		print(f"   URL: {url}")
		print(f"   Method: POST")
		print(f"   Grant Type: refresh_token")
		print(f"   Refresh Token: {token.refresh_token[:30]}...")
		print()
		
		print(f"📥 Response:")
		print(f"   Status Code: {response.status_code}")
		print(f"   Headers: {dict(response.headers)}")
		print()
		
		try:
			response_data = response.json()
			print(f"   Body: {json.dumps(response_data, indent=2)}")
			print()
			
			if response.status_code == 200:
				if "access_token" in response_data:
					print("✅ SUCCESS! Refresh token validation works correctly!")
					print(f"   New Access Token: {response_data.get('access_token', '')[:30]}...")
					print(f"   New Refresh Token: {response_data.get('refresh_token', '')[:30]}...")
					print(f"   Expires In: {response_data.get('expires_in', 'N/A')} seconds")
				else:
					print("⚠️  Response 200 but no access_token in response")
			elif response.status_code == 400:
				if "error" in response_data and response_data.get("error") == "invalid_grant":
					# Check if token is expired - if so, this is expected behavior
					if not is_token_valid:
						print("✅ EXPECTED! invalid_grant error for expired token.")
						print("   This is correct behavior - expired tokens should be rejected.")
						print("   The refresh token validation is working correctly!")
					else:
						print("❌ FAILED! invalid_grant error for VALID token!")
						print("   This means the refresh token validation is NOT working correctly.")
						print("   A valid token should not return invalid_grant error.")
				else:
					print(f"⚠️  Response 400 with error: {response_data.get('error', 'Unknown')}")
			else:
				print(f"⚠️  Unexpected status code: {response.status_code}")
				
		except json.JSONDecodeError:
			print(f"   Body (raw): {response.text[:500]}")
			print("⚠️  Response is not valid JSON")
			
	except requests.exceptions.RequestException as e:
		print(f"❌ Error making request: {str(e)}")
		print("   Please check:")
		print("   1. Site is running (bench start)")
		print("   2. Site URL is correct")
		print("   3. Network connectivity")
	
	print("\n" + "="*80)
	print("✅ Test Complete")
	print("="*80 + "\n")

