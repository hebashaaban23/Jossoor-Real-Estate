"""
Mobile API for CRM Task Management.
Provides REST endpoints for CRUD operations, filtering, and specialized views.

Most endpoints require authentication via Frappe session or OAuth token.
The `get_oauth_config` endpoint allows guest access for retrieving site-specific OAuth settings.
"""

import frappe
import os
from frappe import _
from frappe.utils import today, getdate, nowdate, cint


def _safe_fields(dt, want):
	"""
	Return only fields that exist on the given doctype.
	Prevents KeyErrors when querying fields that don't exist.
	"""
	meta = frappe.get_meta(dt)
	have = {f.fieldname for f in meta.fields}
	# standard meta fields we may use:
	have |= {"name", "modified"}
	return [f for f in want if f in have]


def _get_assigned_users(doctype, docname):
	"""
	Get all assigned users for a document with full user details.
	
	Args:
		doctype: Document type (e.g., "CRM Task")
		docname: Document name/ID
		
	Returns:
		List of user objects with email, name, profile_pic, and id
	"""
	assigned_users = []
	
	# Get assigned users from ToDo table (Frappe's assign_to system)
	todos = frappe.get_all(
		"ToDo",
		filters={
			"reference_type": doctype,
			"reference_name": docname,
			"status": "Open"
		},
		fields=["allocated_to"],
		distinct=True
	)
	
	# Collect unique user emails
	user_emails = set()
	for todo in todos:
		if todo.get("allocated_to"):
			user_emails.add(todo.get("allocated_to"))
	
	# If no users found in ToDo, check the assigned_to field directly
	if not user_emails:
		task_doc = frappe.get_doc(doctype, docname)
		if hasattr(task_doc, "assigned_to") and task_doc.assigned_to:
			user_emails.add(task_doc.assigned_to)
	
	# Get user details for each assigned user
	for email in user_emails:
		try:
			user = frappe.get_doc("User", email)
			user_data = {
				"email": user.email or email,
				"name": user.full_name or user.name,
				"id": user.name,
			}
			
			# Get profile picture if available
			if hasattr(user, "user_image") and user.user_image:
				user_data["profile_pic"] = user.user_image
			elif hasattr(user, "photo") and user.photo:
				user_data["profile_pic"] = user.photo
			else:
				user_data["profile_pic"] = None
			
			assigned_users.append(user_data)
		except frappe.DoesNotExistError:
			# User doesn't exist, skip
			continue
		except Exception:
			# Error fetching user, skip
			continue
	
	return assigned_users


def get_compact_task(task):
	"""
	Return compact task representation with core fields only.
	Accepts both Document objects and dict-like objects (frappe._dict).
	"""
	# Handle both dict-like and Document objects
	def _get(obj, key, default=None):
		if isinstance(obj, dict):
			return obj.get(key, default)
		return getattr(obj, key, default)
	
	# Get task name/id
	task_name = task.name if hasattr(task, "name") else task.get("name")
	
	result = {
		"name": task_name,
		"title": _get(task, "title") or (_get(task, "description", "")[:50] if _get(task, "description") else ""),
		"status": _get(task, "status"),
		"priority": _get(task, "priority"),
		"start_date": _get(task, "start_date"),
		"modified": _get(task, "modified")
	}
	
	# Add optional fields if they exist
	due_date = _get(task, "due_date")
	if due_date is not None:
		result["due_date"] = due_date
	
	# Get assigned users with full details
	try:
		assigned_users = _get_assigned_users("CRM Task", task_name)
		if assigned_users:
			result["assigned_to"] = assigned_users
		else:
			# Fallback: check if assigned_to field exists (single user)
			assigned_to = _get(task, "assigned_to")
			if assigned_to:
				# Try to get user details for single assigned user
				try:
					user = frappe.get_doc("User", assigned_to)
					result["assigned_to"] = [{
						"email": user.email or assigned_to,
						"name": user.full_name or user.name,
						"id": user.name,
						"profile_pic": getattr(user, "user_image", None) or getattr(user, "photo", None) or None
					}]
				except Exception:
					# If user doesn't exist or error, return empty array
					result["assigned_to"] = []
			else:
				result["assigned_to"] = []
	except Exception:
		# Error getting assigned users, return empty array
		result["assigned_to"] = []
	
	return result


def _validate_host():
	"""
	Validate that the request Host header belongs to the current site's configured domains.
	
	This prevents returning client_id for external domains that don't belong to the site.
	
	Returns:
		None (raises ValidationError if host is not allowed)
	
	Raises:
		frappe.exceptions.ValidationError: If host is not in site's allowed domains
	"""
	# Extract host from request headers
	# Priority: X-Forwarded-Host (if behind proxy) > Host header
	request = frappe.local.request if hasattr(frappe.local, 'request') else None
	if not request:
		frappe.log_error(
			"Request object not available in frappe.local",
			"OAuth Host Validation Error"
		)
		frappe.throw(_(
			"Request information is not available. Please ensure you are accessing a valid Frappe site."
		))
	
	# Get host from headers
	host = None
	if request.headers.get("X-Forwarded-Host"):
		host = request.headers.get("X-Forwarded-Host")
		# X-Forwarded-Host can contain multiple hosts, take the first one
		if "," in host:
			host = host.split(",")[0].strip()
	elif request.headers.get("Host"):
		host = request.headers.get("Host")
	
	if not host:
		frappe.log_error(
			"Host header not found in request",
			"OAuth Host Validation Error"
		)
		frappe.throw(_(
			"Host header is missing. Please ensure you are accessing via a valid domain."
		))
	
	# Normalize host: lowercase, remove port
	host = host.lower().split(":")[0].strip()
	
	# Get site configuration
	site_name = frappe.local.site if frappe.local else None
	if not site_name:
		frappe.log_error(
			"Site name not available for host validation",
			"OAuth Host Validation Error"
		)
		frappe.throw(_(
			"Site information is not available. Please contact system administrator."
		))
	
	# Get allowed domains from site config
	allowed_domains = []
	try:
		site_config = frappe.get_site_config()
		
		# Get domains list if available
		if "domains" in site_config and site_config["domains"]:
			if isinstance(site_config["domains"], list):
				allowed_domains.extend([d.lower().strip() for d in site_config["domains"]])
			elif isinstance(site_config["domains"], str):
				# Comma-separated or space-separated
				domains_str = site_config["domains"]
				allowed_domains.extend([d.lower().strip() for d in domains_str.replace(",", " ").split()])
		
		# Get host_name if available
		if "host_name" in site_config and site_config["host_name"]:
			host_name = site_config["host_name"].lower().strip()
			if host_name not in allowed_domains:
				allowed_domains.append(host_name)
		
		# SECURITY: If no domains configured, use site_name ONLY if it matches host
		# This is a minimal fallback for sites without explicit domain config
		# But we still require exact match - no wildcards, no other domains
		if not allowed_domains:
			site_name_lower = site_name.lower().strip()
			# Only allow if host exactly matches site_name (exact match required)
			if host == site_name_lower:
				allowed_domains.append(site_name_lower)
			else:
				# Host doesn't match site_name and no domains configured - DENY
				frappe.log_error(
					f"OAuth rejected: Site '{site_name}' has no domains config. Host '{host}' != site_name.",
					"OAuth Host Validation"
				)
				frappe.throw(_(
					"Access denied: Domain '{host}' is not configured for site '{site_name}'. "
					"Please configure 'domains' in site_config.json."
				).format(host=host, site_name=site_name))
	except Exception as e:
		# If we can't read config, use site_name as last resort ONLY if it matches host
		site_name_lower = site_name.lower().strip() if site_name else None
		if site_name_lower and host == site_name_lower:
			# Exact match - allow as minimal fallback
			allowed_domains = [site_name_lower]
		else:
			# No match or no site_name - DENY
			frappe.log_error(
				f"OAuth rejected: Config error for '{site_name}'. Host '{host}' not allowed.",
				"OAuth Host Validation"
			)
			frappe.throw(_(
				"Site configuration error. Access denied. Please contact administrator."
			))
	
	# Allow localhost and 127.0.0.1 ONLY if explicitly in allowed_domains
	# Do NOT allow them automatically - this is a security risk
	development_hosts = ["localhost", "127.0.0.1"]
	
	# Check if host is allowed
	host_allowed = False
	if host in allowed_domains:
		host_allowed = True
	elif host in development_hosts and host in allowed_domains:
		# Only allow development hosts if explicitly in allowed_domains
		host_allowed = True
	
	if not host_allowed:
		# Log the rejection for security tracking (short message to avoid truncation)
		frappe.log_error(
			f"OAuth rejected: Host '{host}' not in allowed domains for '{site_name}'.",
			"OAuth Host Validation"
		)
		frappe.throw(_(
			"Access denied: The domain '{host}' is not configured for this site. "
			"Please use a valid domain for site '{site_name}'."
		).format(host=host, site_name=site_name))


def _ensure_mobile_oauth_settings():
	"""
	Ensure Mobile OAuth Settings exist and are configured with a valid client_id.
	
	This function is idempotent:
	- If settings exist and have client_id, returns them unchanged
	- If settings exist but client_id is empty, creates OAuth Client and updates settings
	- If settings don't exist, creates them along with OAuth Client
	
	Returns:
		Mobile OAuth Settings document
	"""
	# Validate that the current site exists in Frappe bench
	# This prevents creating OAuth clients for non-existent sites (e.g., facebook.com)
	site_name = frappe.local.site if frappe.local else None
	if not site_name:
		frappe.log_error(
			"Site name not available in frappe.local",
			"OAuth Auto-Config Error"
		)
		frappe.throw(_(
			"Site information is not available. Please ensure you are accessing a valid Frappe site."
		))
	
	# Check if the site exists in the bench by verifying site directory exists
	# This prevents creating OAuth clients for domains that route to non-existent sites
	# Get bench path from frappe local
	if hasattr(frappe.local, 'site_path') and frappe.local.site_path:
		# site_path is typically: /path/to/bench/sites/site_name
		bench_path = os.path.dirname(os.path.dirname(frappe.local.site_path))
		site_path = frappe.local.site_path
		
		if not os.path.exists(site_path):
			frappe.log_error(
				f"Site '{site_name}' does not exist in Frappe bench. "
				f"Site directory not found at: {site_path}. "
				f"Requested domain may not be configured.",
				"OAuth Auto-Config Error"
			)
			frappe.throw(_(
				"Site '{site_name}' is not configured in this Frappe bench. "
				"Please contact system administrator or use a valid site domain."
			).format(site_name=site_name))
	
	# Additional check: verify database connection is valid
	# This helps catch cases where domain routing is wrong
	try:
		if not hasattr(frappe.conf, 'db_name') or not frappe.conf.db_name:
			frappe.log_error(
				f"Site '{site_name}' database configuration is missing.",
				"OAuth Auto-Config Error"
			)
			frappe.throw(_(
				"Site '{site_name}' is not properly configured. "
				"Database configuration is missing. Please contact system administrator."
			).format(site_name=site_name))
		
		# Test database connection
		frappe.db.sql("SELECT 1", as_dict=True)
	except Exception as db_error:
		frappe.log_error(
			f"Database connection failed for site '{site_name}': {str(db_error)}",
			"OAuth Auto-Config Error"
		)
		frappe.throw(_(
			"Site '{site_name}' is not properly configured. "
			"Database connection failed. Please contact system administrator."
		).format(site_name=site_name))
	
	# First check if the DocType exists in the database
	if not frappe.db.exists("DocType", "Mobile OAuth Settings"):
		frappe.log_error(
			f"Mobile OAuth Settings DocType not found on site '{site_name}'. "
			f"Please run 'bench --site {site_name} migrate' to create it.",
			"OAuth Auto-Config Error"
		)
		frappe.throw(_(
			"Mobile OAuth Settings DocType is not available on site '{site_name}'. "
			"Please run 'bench --site {site_name} migrate' to create it."
		).format(site_name=site_name))
	
	# Try to load the Mobile OAuth Settings single document
	try:
		settings = frappe.get_single("Mobile OAuth Settings")
	except Exception:
		# Document doesn't exist, create it
		settings = frappe.get_doc({
			"doctype": "Mobile OAuth Settings",
			"name": "Mobile OAuth Settings"
		})
		settings.insert(ignore_permissions=True)
		frappe.db.commit()
	
	# If client_id is already set, return settings
	if settings.client_id:
		return settings
	
	# client_id is empty, need to create OAuth Client
	# Check if OAuth Provider is available
	if not frappe.db.exists("DocType", "OAuth Client"):
		frappe.log_error(
			"OAuth Provider not installed. Please install frappe.integrations.oauth2_provider",
			"OAuth Auto-Config Error"
		)
		frappe.throw(_("OAuth Provider is not available on this site. Please contact system administrator."))
	
	# Get current site name
	site_name = frappe.local.site if frappe.local else "Unknown"
	
	# Create new OAuth Client
	client_doc = frappe.new_doc("OAuth Client")
	client_doc.app_name = "Mobile App"
	client_doc.client_name = f"Mobile App - {site_name}"
	
	# Set redirect URIs
	redirect_uri = "app.trust://oauth2redirect"
	client_doc.redirect_uris = redirect_uri
	client_doc.default_redirect_uri = redirect_uri
	
	# Set scopes
	client_doc.scopes = "all openid"
	
	# Set grant type
	# Note: Frappe's OAuth Client uses grant_type as single select field
	# Valid values are: "Authorization Code" or "Implicit"
	# Password and refresh token grants are handled by Frappe OAuth2 provider
	# automatically based on request parameters, not stored in OAuth Client settings
	if hasattr(client_doc, "grant_type"):
		client_doc.grant_type = "Authorization Code"  # This enables PKCE
	
	# Set response type for Authorization Code flow
	if hasattr(client_doc, "response_type"):
		client_doc.response_type = "Code"
	
	# Enable skip authorization for trusted first-party apps
	if hasattr(client_doc, "skip_authorization"):
		client_doc.skip_authorization = 1
	
	# Insert the OAuth Client (client_id and client_secret are auto-generated)
	client_doc.insert(ignore_permissions=True)
	frappe.db.commit()
	
	# Update Mobile OAuth Settings with the OAuth Client info
	settings.client_id = client_doc.client_id
	settings.scope = client_doc.scopes or "all openid"
	settings.redirect_uri = client_doc.default_redirect_uri or redirect_uri
	settings.save(ignore_permissions=True)
	frappe.db.commit()
	
	return settings


@frappe.whitelist(allow_guest=True)
def get_oauth_config():
	"""
	Get OAuth 2.0 configuration for the current site.
	This endpoint is called by mobile clients to retrieve the site-specific
	OAuth configuration (client_id, scope, redirect_uri) instead of hardcoding
	these values in the app.
	
	Security: Only returns client_id if the request Host belongs to the site's configured domains.
	Automatically creates OAuth Client and Mobile OAuth Settings if they don't exist.
	
	Returns:
		{
			"message": {
				"client_id": str,
				"scope": str,
				"redirect_uri": str
			}
		}
	
	Raises:
		frappe.exceptions.ValidationError: If host is not in site's allowed domains or OAuth setup fails
	"""
	# Step 1: Validate Host header (deny by default if not in allowed domains)
	# This MUST be the first check - no client_id should be returned if host is invalid
	_validate_host()
	
	# Step 2: Ensure OAuth settings exist and are configured
	# This will create OAuth Client automatically if needed (idempotent)
	try:
		settings = _ensure_mobile_oauth_settings()
	except Exception as e:
		# Log the error for debugging
		frappe.log_error(
			f"Failed to ensure OAuth settings: {str(e)}",
			"OAuth Config Error"
		)
		# Re-raise with user-friendly message
		if "OAuth Provider" in str(e):
			frappe.throw(_("OAuth Provider is not available on this site. Please contact system administrator."))
		else:
			frappe.throw(_("Failed to retrieve OAuth configuration. Please contact system administrator."))
	
	# Step 3: Return configuration ONLY if all validations passed
	# No fallback values, no default client_id - only return what's actually configured
	if not settings.client_id:
		frappe.log_error(
			"OAuth settings exist but client_id is empty after ensure_mobile_oauth_settings",
			"OAuth Config Error"
		)
		frappe.throw(_("OAuth configuration is incomplete. Please contact system administrator."))
	
	return {
		"client_id": settings.client_id,
		"scope": settings.scope or "all openid",
		"redirect_uri": settings.redirect_uri or "app.trust://oauth2redirect"
	}


@frappe.whitelist(allow_guest=True)
def test_host_validation(test_host=None, expected_result=None):
	"""
	Test function to verify Host validation.
	For testing purposes only - should be removed in production.
	
	Args:
		test_host: Host to test (default: "facebook.com")
		expected_result: "allow" or "reject" (default: "reject")
	"""
	# Mock request with test host
	class MockRequest:
		def __init__(self, host):
			self.headers = {"Host": host}
	
	original_request = getattr(frappe.local, 'request', None)
	test_host = test_host or "facebook.com"
	expected_result = expected_result or "reject"
	
	try:
		frappe.local.request = MockRequest(test_host)
		_validate_host()
		# Validation passed (host was allowed)
		if expected_result == "allow":
			return {
				"status": "PASSED",
				"message": f"Host '{test_host}' was ALLOWED as expected"
			}
		else:
			return {
				"status": "FAILED",
				"message": f"Host '{test_host}' was ALLOWED but should be REJECTED!"
			}
	except frappe.exceptions.ValidationError as e:
		# Validation failed (host was rejected)
		if expected_result == "reject":
			return {
				"status": "PASSED",
				"message": f"Host '{test_host}' was REJECTED as expected",
				"error": str(e)[:200]
			}
		else:
			return {
				"status": "FAILED",
				"message": f"Host '{test_host}' was REJECTED but should be ALLOWED!",
				"error": str(e)[:200]
			}
	except Exception as e:
		return {
			"status": "ERROR",
			"message": f"Unexpected error: {type(e).__name__}",
			"error": str(e)[:200]
		}
	finally:
		if original_request:
			frappe.local.request = original_request


@frappe.whitelist()
def create_task(title=None, status=None, priority=None, start_date=None, 
				task_type=None, description=None, assigned_to=None, due_date=None):
	"""
	Create a new CRM Task.
	
	Args:
		title: Task title (optional, can be derived from description)
		status: Task status (default: "Todo")
		priority: Task priority (default: "Medium")
		start_date: Task start date (default: today)
		task_type: Task type - required (Meeting/Property Showing/Call)
		description: Task description
		assigned_to: User to assign the task to
		due_date: Task due date
	
	Returns:
		Compact task JSON with core fields
	"""
	# Validate required fields
	if not task_type:
		frappe.throw(_("Task Type is required"))
	
	# Set defaults
	if not status:
		status = "Todo"
	if not priority:
		priority = "Medium"
	if not start_date:
		start_date = today()
	
	# Create task with only available fields
	task_doc = {
		"doctype": "CRM Task",
		"task_type": task_type,
		"status": status,
		"priority": priority,
		"start_date": start_date,
	}
	
	# Add optional fields if provided
	if title is not None:
		task_doc["title"] = title
	if description is not None:
		task_doc["description"] = description
	if assigned_to is not None:
		task_doc["assigned_to"] = assigned_to
	if due_date is not None:
		task_doc["due_date"] = due_date
	
	task = frappe.get_doc(task_doc)
	task.insert()
	frappe.db.commit()
	
	return get_compact_task(task)


@frappe.whitelist()
def edit_task(task_id=None, name=None, title=None, status=None, priority=None, start_date=None,
			  task_type=None, description=None, assigned_to=None, due_date=None):
	"""
	Edit an existing CRM Task.
	
	Args:
		task_id: Task ID (name) - required (can also use 'name')
		name: Task name (alias for task_id)
		title: Task title
		status: Task status
		priority: Task priority
		start_date: Task start date
		task_type: Task type
		description: Task description
		assigned_to: User to assign the task to
		due_date: Task due date
	
	Returns:
		Updated compact task JSON
	"""
	# Accept either task_id or name
	task_name = task_id or name
	if not task_name:
		frappe.throw(_("Task ID is required"))
	
	name = task_name
	
	# Get task (this respects permissions)
	task = frappe.get_doc("CRM Task", name)
	
	# Update fields if provided (only set fields that exist on the doctype)
	if title is not None:
		task.title = title
	if status is not None:
		task.status = status
	if priority is not None:
		task.priority = priority
	if start_date is not None:
		task.start_date = start_date
	if task_type is not None:
		task.task_type = task_type
	if description is not None and hasattr(task, "description"):
		task.description = description
	if assigned_to is not None and hasattr(task, "assigned_to"):
		task.assigned_to = assigned_to
	if due_date is not None and hasattr(task, "due_date"):
		task.due_date = due_date
	
	task.save()
	frappe.db.commit()
	
	return get_compact_task(task)


@frappe.whitelist()
def delete_task(task_id=None, name=None):
	"""
	Delete a CRM Task.
	
	Args:
		task_id: Task ID (name) - required (can also use 'name')
		name: Task name (alias for task_id)
	
	Returns:
		{"ok": true, "message": "Task deleted successfully"}
	"""
	# Accept either task_id or name
	task_name = task_id or name
	if not task_name:
		frappe.throw(_("Task ID is required"))
	
	name = task_name
	
	# Delete task (this respects permissions)
	frappe.delete_doc("CRM Task", name)
	frappe.db.commit()
	
	return {"ok": True, "message": f"Task {name} deleted successfully"}


@frappe.whitelist()
def update_status(task_id=None, name=None, status=None):
	"""
	Update task status.
	
	Args:
		task_id: Task ID (name) - required (can also use 'name')
		name: Task name (alias for task_id)
		status: New status (required)
	
	Returns:
		Updated compact task JSON
	"""
	# Accept either task_id or name
	task_name = task_id or name
	if not task_name:
		frappe.throw(_("Task ID is required"))
	if not status:
		frappe.throw(_("Status is required"))
	
	name = task_name
	
	task = frappe.get_doc("CRM Task", name)
	task.status = status
	task.save()
	frappe.db.commit()
	
	return get_compact_task(task)


@frappe.whitelist()
def filter_tasks(date_from=None, date_to=None, importance=None, status=None,
				 limit=20, page=1, order_by="modified desc"):
	"""
	Filter and search CRM Tasks with page-based pagination.
	
	Args:
		date_from: Start date filter (YYYY-MM-DD)
		date_to: End date filter (YYYY-MM-DD)
		importance: Comma-separated priority values (e.g., "High,Medium")
		status: Comma-separated status values (e.g., "Todo,In Progress")
		limit: Page size - maximum results per page (default: 20)
		page: Page number (1-based, default: 1)
		order_by: Sort order (default: "modified desc")
	
	Returns:
		{
			"message": {
				"data": [tasks...],
				"page": current_page,
				"page_size": limit,
				"total": total_matching_tasks,
				"has_next": boolean
			}
		}
	"""
	# Convert and validate pagination params safely
	limit = cint(limit) or 20
	page = cint(page) or 1
	if page < 1:
		page = 1
	
	# Compute offset from page number
	start = (page - 1) * limit
	
	# Build filters as a list of conditions
	filters = []
	if date_from:
		filters.append(["start_date", ">=", date_from])
	if date_to:
		filters.append(["start_date", "<=", date_to])
	
	if importance:
		priorities = [p.strip() for p in importance.split(",") if p.strip()]
		if priorities:
			filters.append(["priority", "in", priorities])
	
	if status:
		statuses = [s.strip() for s in status.split(",") if s.strip()]
		if statuses:
			filters.append(["status", "in", statuses])
	
	# Get safe fields for CRM Task
	base_fields = ["name", "title", "status", "priority", "start_date", "due_date", 
	               "assigned_to", "modified", "description"]
	fields = _safe_fields("CRM Task", base_fields)
	
	# Get tasks with pagination
	tasks = frappe.get_all(
		"CRM Task",
		filters=filters,
		fields=fields,
		order_by=order_by,
		limit_start=start,
		limit_page_length=limit
	)
	
	# Get total count of matching tasks
	total = frappe.db.count("CRM Task", filters=filters)
	
	# Format tasks using compact helper
	data = [get_compact_task(task) for task in tasks]
	
	# Calculate if there are more pages
	has_next = (start + len(data)) < total
	
	return {
		"message": {
			"data": data,
			"page": page,
			"page_size": limit,
			"total": total,
			"has_next": has_next
		}
	}


@frappe.whitelist()
def home_tasks(limit=5):
	"""
	Get today's top tasks for home screen.
	
	Args:
		limit: Maximum number of tasks to return (default: 5)
	
	Returns:
		{"today": [tasks...], "limit": N}
	"""
	today_date = today()
	
	# Get safe fields for CRM Task
	base_fields = ["name", "title", "status", "priority", "start_date", "due_date",
	               "assigned_to", "modified", "description"]
	fields = _safe_fields("CRM Task", base_fields)
	
	tasks = frappe.get_all(
		"CRM Task",
		filters={
			"start_date": today_date
		},
		fields=fields,
		order_by="priority desc, modified desc",
		page_length=cint(limit) or 5
	)
	
	# Format tasks using compact helper
	data = [get_compact_task(task) for task in tasks]
	
	return {
		"today": data,
		"limit": cint(limit) or 5
	}


@frappe.whitelist()
def main_page_buckets(min_each=5):
	"""
	Get tasks organized into today/late/upcoming buckets.
	Each bucket will contain at least min_each tasks (when available).
	
	Args:
		min_each: Minimum number of tasks per bucket (default: 5)
	
	Returns:
		{
			"today": [tasks...],
			"late": [tasks...],
			"upcoming": [tasks...],
			"min_each": N
		}
	"""
	today_date = today()
	min_count = cint(min_each) or 5
	
	# Active statuses (not Done or Canceled)
	active_statuses = ["Backlog", "Todo", "In Progress"]
	
	# Get safe fields for CRM Task
	base_fields = ["name", "title", "status", "priority", "start_date", "due_date",
	               "assigned_to", "modified", "description"]
	fields = _safe_fields("CRM Task", base_fields)
	
	# Today's tasks
	today_tasks = frappe.get_all(
		"CRM Task",
		filters={
			"start_date": today_date
		},
		fields=fields,
		order_by="priority desc, modified desc",
		page_length=min_count
	)
	
	# Late tasks (before today and still active)
	late_tasks = frappe.get_all(
		"CRM Task",
		filters={
			"start_date": ["<", today_date],
			"status": ["in", active_statuses]
		},
		fields=fields,
		order_by="start_date asc, priority desc",
		page_length=min_count
	)
	
	# Upcoming tasks (after today)
	upcoming_tasks = frappe.get_all(
		"CRM Task",
		filters={
			"start_date": [">", today_date]
		},
		fields=fields,
		order_by="start_date asc, priority desc",
		page_length=min_count
	)
	
	# Format all task lists using compact helper
	return {
		"today": [get_compact_task(task) for task in today_tasks],
		"late": [get_compact_task(task) for task in late_tasks],
		"upcoming": [get_compact_task(task) for task in upcoming_tasks],
		"min_each": min_count
	}

