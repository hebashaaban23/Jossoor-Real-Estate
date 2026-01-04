"""
OAuth Fix Module - DISABLED

This module previously monkey-patched Frappe's OAuth behavior to enforce
refresh token expiry. It has been disabled to restore default Frappe behavior
(infinite refresh tokens).

All monkey patching logic has been commented out below.
"""

# import frappe
# from frappe.utils import now_datetime
# import datetime

# # Import oauth module lazily to avoid circular imports
# frappe_oauth = None

# def get_oauth_module():
# 	"""Get the oauth module, importing it if necessary."""
# 	global frappe_oauth
# 	if frappe_oauth is None:
# 		from frappe import oauth as frappe_oauth_module
# 		frappe_oauth = frappe_oauth_module
# 	return frappe_oauth


def setup_oauth_fix():
	"""
	OAuth fix disabled - using default Frappe behavior (infinite refresh tokens).
	All monkey patching logic has been commented out below.
	"""
	# DISABLED - Using default Frappe OAuth behavior
	pass
	
	# # Get oauth module
	# oauth_module = get_oauth_module()
	# 
	# # Store original method if not already stored
	# if not hasattr(oauth_module.OAuthWebRequestValidator, '_original_validate_refresh_token_fixed'):
	# 	oauth_module.OAuthWebRequestValidator._original_validate_refresh_token_fixed = oauth_module.OAuthWebRequestValidator.validate_refresh_token
	# 	
	# 	def validate_refresh_token_fixed(self, refresh_token, client, request, *args, **kwargs):
	# 		"""
	# 		Fixed version of validate_refresh_token that properly queries the database
	# 		and checks refresh token expiry (1 hour).
	# 		"""
	# 		try:
	# 			if not refresh_token:
	# 				return False
	# 			
	# 			# Get refresh token expiry from site config (default: 3600 seconds = 1 hour)
	# 			refresh_token_expiry = frappe.conf.get('oauth_refresh_token_expiry', 3600)
	# 			
	# 			# Use frappe.get_all with proper filters instead of frappe.get_doc with dict
	# 			tokens = frappe.get_all(
	# 				"OAuth Bearer Token",
	# 				filters={
	# 					"refresh_token": refresh_token,
	# 					"status": "Active"
	# 				},
	# 				fields=["name", "creation", "user"],
	# 				limit=1
	# 			)
	# 			
	# 			if not tokens:
	# 				return False
	# 			
	# 			# Check if refresh token has expired based on creation time
	# 			token_doc = frappe.get_doc("OAuth Bearer Token", tokens[0].name)
	# 			refresh_expiry = token_doc.creation + datetime.timedelta(seconds=refresh_token_expiry)
	# 			now = now_datetime()
	# 			
	# 			# Check if token has expired
	# 			if now > refresh_expiry:
	# 				# Mark token as revoked
	# 				try:
	# 					frappe.db.set_value("OAuth Bearer Token", token_doc.name, "status", "Revoked")
	# 					frappe.db.commit()
	# 				except Exception:
	# 					pass
	# 				return False
	# 			
	# 			# Set request.user for oauthlib
	# 			if hasattr(request, 'user') and tokens[0].get('user'):
	# 				request.user = tokens[0]['user']
	# 			
	# 			# Token exists, is active, and hasn't expired
	# 			return True
	# 			
	# 		except Exception as e:
	# 			# Only log actual errors, not expected failures
	# 			if "DoesNotExistError" not in str(type(e).__name__):
	# 				frappe.log_error(
	# 					f"Error validating refresh token: {str(e)}",
	# 					"OAuth Refresh Token Validation"
	# 				)
	# 			return False
	# 	
	# 	# Monkey patch the method
	# 	oauth_module.OAuthWebRequestValidator.validate_refresh_token = validate_refresh_token_fixed
	# 
	# # Also fix get_original_scopes which uses the same broken pattern
	# if not hasattr(oauth_module.OAuthWebRequestValidator, '_original_get_original_scopes_fixed'):
	# 	oauth_module.OAuthWebRequestValidator._original_get_original_scopes_fixed = oauth_module.OAuthWebRequestValidator.get_original_scopes
	# 	
	# 	def get_original_scopes_fixed(self, refresh_token, request, *args, **kwargs):
	# 		"""
	# 		Fixed version of get_original_scopes that properly queries the database.
	# 		"""
	# 		try:
	# 			if not refresh_token:
	# 				return []
	# 			
	# 			tokens = frappe.get_all(
	# 				"OAuth Bearer Token",
	# 				filters={"refresh_token": refresh_token},
	# 				fields=["name", "scopes"],
	# 				limit=1
	# 			)
	# 			
	# 			if not tokens:
	# 				return []
	# 			
	# 			token_doc = frappe.get_doc("OAuth Bearer Token", tokens[0].name)
	# 			from frappe.oauth import get_url_delimiter
	# 			return token_doc.scopes.split(get_url_delimiter()) if token_doc.scopes else []
	# 			
	# 		except Exception:
	# 			return []
	# 	
	# 	# Monkey patch the method
	# 	oauth_module.OAuthWebRequestValidator.get_original_scopes = get_original_scopes_fixed
	# 
	# # Also fix the save_bearer_token method which uses frappe.get_doc with dict filter
	# if not hasattr(oauth_module.OAuthWebRequestValidator, '_original_save_bearer_token_fixed'):
	# 	oauth_module.OAuthWebRequestValidator._original_save_bearer_token_fixed = oauth_module.OAuthWebRequestValidator.save_bearer_token
	# 	
	# 	def save_bearer_token_fixed(self, token, request, *args, **kwargs):
	# 		"""
	# 		Fixed version that handles refresh_token lookup properly.
	# 		"""
	# 		# Get the original method
	# 		original_save_bearer_token = oauth_module.OAuthWebRequestValidator._original_save_bearer_token_fixed
	# 		
	# 		# Fix the user lookup if needed
	# 		if not request.user and hasattr(request, 'body') and request.body:
	# 			refresh_token = request.body.get("refresh_token")
	# 			if refresh_token:
	# 				try:
	# 					tokens = frappe.get_all(
	# 						"OAuth Bearer Token",
	# 						filters={"refresh_token": refresh_token},
	# 						fields=["name", "user"],
	# 						limit=1
	# 					)
	# 					if tokens:
	# 						request.user = tokens[0].get('user')
	# 				except Exception:
	# 					pass
	# 		
	# 		# Call original method
	# 		result = original_save_bearer_token(self, token, request, *args, **kwargs)
	# 		return result
	# 	
	# 	# Monkey patch the method
	# 	oauth_module.OAuthWebRequestValidator.save_bearer_token = save_bearer_token_fixed
	# 
	# # Fix authenticate_client which also uses frappe.db.get_value with dict filter
	# if not hasattr(oauth_module.OAuthWebRequestValidator, '_original_authenticate_client_fixed'):
	# 	oauth_module.OAuthWebRequestValidator._original_authenticate_client_fixed = oauth_module.OAuthWebRequestValidator.authenticate_client
	# 	
	# 	def authenticate_client_fixed(self, request, *args, **kwargs):
	# 		"""
	# 		Fixed version that handles refresh_token lookup properly.
	# 		"""
	# 		# Get ClientID in URL
	# 		if request.client_id:
	# 			try:
	# 				oc = frappe.get_doc("OAuth Client", request.client_id)
	# 				request.client = request.client or oc.as_dict()
	# 				from frappe.oauth import get_cookie_dict_from_headers
	# 				cookie_dict = get_cookie_dict_from_headers(request)
	# 				from urllib.parse import unquote
	# 				user_id = unquote(cookie_dict.get("user_id").value) if "user_id" in cookie_dict else "Guest"
	# 				return frappe.session.user == user_id
	# 			except Exception:
	# 				return False
	# 		else:
	# 			# Extract token, instantiate OAuth Bearer Token and use clientid from there.
	# 			if hasattr(frappe, 'form_dict') and frappe.form_dict:
	# 				if "refresh_token" in frappe.form_dict:
	# 					refresh_token = frappe.form_dict["refresh_token"]
	# 					try:
	# 						tokens = frappe.get_all(
	# 							"OAuth Bearer Token",
	# 							filters={"refresh_token": refresh_token},
	# 							fields=["name", "client"],
	# 							limit=1
	# 						)
	# 						if tokens:
	# 							oc = frappe.get_doc("OAuth Client", tokens[0].client)
	# 							request.client = request.client or oc.as_dict()
	# 							from frappe.oauth import get_cookie_dict_from_headers
	# 							cookie_dict = get_cookie_dict_from_headers(request)
	# 							from urllib.parse import unquote
	# 							user_id = unquote(cookie_dict.get("user_id").value) if "user_id" in cookie_dict else "Guest"
	# 							return frappe.session.user == user_id
	# 					except Exception:
	# 						pass
	# 				elif "token" in frappe.form_dict:
	# 					try:
	# 						token = frappe.form_dict["token"]
	# 						tokens = frappe.get_all(
	# 							"OAuth Bearer Token",
	# 							filters={"access_token": token},
	# 							fields=["name", "client"],
	# 							limit=1
	# 						)
	# 						if tokens:
	# 							oc = frappe.get_doc("OAuth Client", tokens[0].client)
	# 							request.client = request.client or oc.as_dict()
	# 							from frappe.oauth import get_cookie_dict_from_headers
	# 							cookie_dict = get_cookie_dict_from_headers(request)
	# 							from urllib.parse import unquote
	# 							user_id = unquote(cookie_dict.get("user_id").value) if "user_id" in cookie_dict else "Guest"
	# 							return frappe.session.user == user_id
	# 					except Exception:
	# 						pass
	# 				else:
	# 					# Try Authorization header
	# 					try:
	# 						auth_header = frappe.get_request_header("Authorization")
	# 						if auth_header:
	# 							token = auth_header.split(" ")[1] if " " in auth_header else auth_header
	# 							tokens = frappe.get_all(
	# 								"OAuth Bearer Token",
	# 								filters={"access_token": token},
	# 								fields=["name", "client"],
	# 								limit=1
	# 							)
	# 							if tokens:
	# 								oc = frappe.get_doc("OAuth Client", tokens[0].client)
	# 								request.client = request.client or oc.as_dict()
	# 								from frappe.oauth import get_cookie_dict_from_headers
	# 								cookie_dict = get_cookie_dict_from_headers(request)
	# 								from urllib.parse import unquote
	# 								user_id = unquote(cookie_dict.get("user_id").value) if "user_id" in cookie_dict else "Guest"
	# 								return frappe.session.user == user_id
	# 					except Exception:
	# 						pass
	# 		
	# 		return False
	# 	
	# 	# Monkey patch the method
	# 	oauth_module.OAuthWebRequestValidator.authenticate_client = authenticate_client_fixed


def setup_oauth2_fix():
	"""
	OAuth2 fix disabled - using default Frappe behavior.
	All monkey patching logic has been commented out below.
	"""
	# DISABLED - Using default Frappe OAuth behavior
	pass
	
	# try:
	# 	from frappe.integrations import oauth2 as frappe_integrations_oauth2
	# except ImportError:
	# 	# Module not available yet
	# 	return
	# 
	# # Store original function if not already stored
	# if not hasattr(frappe_integrations_oauth2, '_original_introspect_token_fixed'):
	# 	frappe_integrations_oauth2._original_introspect_token_fixed = frappe_integrations_oauth2.introspect_token
	# 	
	# 	@frappe.whitelist(allow_guest=True)
	# 	def introspect_token_fixed(token=None, token_type_hint=None):
	# 		"""
	# 		Fixed version of introspect_token that properly queries the database.
	# 		"""
	# 		if token_type_hint not in ["access_token", "refresh_token"]:
	# 			token_type_hint = "access_token"
	# 		try:
	# 			bearer_token_doc = None
	# 			
	# 			# Use frappe.get_all with proper filters instead of frappe.get_doc with dict
	# 			if token_type_hint == "access_token":
	# 				tokens = frappe.get_all(
	# 					"OAuth Bearer Token",
	# 					filters={"access_token": token},
	# 					fields=["name"],
	# 					limit=1
	# 				)
	# 				if tokens:
	# 					bearer_token_doc = frappe.get_doc("OAuth Bearer Token", tokens[0].name)
	# 			elif token_type_hint == "refresh_token":
	# 				tokens = frappe.get_all(
	# 					"OAuth Bearer Token",
	# 					filters={"refresh_token": token},
	# 					fields=["name"],
	# 					limit=1
	# 				)
	# 				if tokens:
	# 					bearer_token_doc = frappe.get_doc("OAuth Bearer Token", tokens[0].name)
	# 			
	# 			if not bearer_token_doc:
	# 				frappe.local.response = frappe._dict({"active": False})
	# 				return
	# 			
	# 			client = frappe.get_doc("OAuth Client", bearer_token_doc.client)
	# 			
	# 			token_response = frappe._dict(
	# 				{
	# 					"client_id": client.client_id,
	# 					"trusted_client": client.skip_authorization,
	# 					"active": bearer_token_doc.status == "Active",
	# 					"exp": round(bearer_token_doc.expiration_time.timestamp()),
	# 					"scope": bearer_token_doc.scopes,
	# 				}
	# 			)
	# 			
	# 			if "openid" in bearer_token_doc.scopes:
	# 				sub = frappe.get_value(
	# 					"User Social Login",
	# 					{"provider": "frappe", "parent": bearer_token_doc.user},
	# 					"userid",
	# 				)
	# 				
	# 				if sub:
	# 					token_response.update({"sub": sub})
	# 					user = frappe.get_doc("User", bearer_token_doc.user)
	# 					from frappe.integrations.oauth2 import get_userinfo
	# 					userinfo = get_userinfo(user)
	# 					token_response.update(userinfo)
	# 			
	# 			frappe.local.response = token_response
	# 			
	# 		except Exception as e:
	# 			frappe.log_error(
	# 				f"Error in fixed introspect_token: {str(e)}\nToken: {token[:20] if token else 'None'}...",
	# 				"OAuth Introspect Token Fix"
	# 			)
	# 			frappe.local.response = frappe._dict({"active": False})
	# 	
	# 	# Monkey patch the function
	# 	frappe_integrations_oauth2.introspect_token = introspect_token_fixed


# Auto-setup disabled - using default Frappe behavior (infinite refresh tokens)
# try:
# 	# Only setup if frappe is initialized
# 	if hasattr(frappe, 'local') and hasattr(frappe.local, 'site'):
# 		setup_oauth_fix()
# 		setup_oauth2_fix()
# except Exception as e:
# 	# Don't log if Frappe is not initialized yet
# 	if hasattr(frappe, 'log_error'):
# 		frappe.log_error(f"Error setting up OAuth fix: {str(e)}", "OAuth Fix Setup")


def ensure_oauth_fix_applied():
	"""
	OAuth fix disabled - using default Frappe behavior (infinite refresh tokens).
	This function is kept for compatibility but does nothing.
	"""
	# DISABLED - Using default Frappe OAuth behavior
	pass
	
	# try:
	# 	# Re-apply the fix if needed (idempotent operation)
	# 	setup_oauth_fix()
	# 	setup_oauth2_fix()
	# except Exception as e:
	# 	# Log error if Frappe is initialized
	# 	if hasattr(frappe, 'log_error'):
	# 		frappe.log_error(f"Error ensuring OAuth fix applied: {str(e)}", "OAuth Fix Ensure")

