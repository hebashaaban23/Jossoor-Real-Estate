# CRM Mobile API Documentation

## Overview

This document provides comprehensive information about the CRM Task Management API endpoints. The API is built on the Frappe framework and provides full CRUD operations for task management, along with specialized endpoints for mobile applications.

**Base URL:** `https://trust.jossoor.org`

**API Version:** 1.2.1

**Last Updated:** December 4, 2025

---

## Table of Contents

1. [Authentication](#authentication)
2. [API Endpoints](#api-endpoints)
3. [Data Models](#data-models)
4. [Error Handling](#error-handling)
5. [Testing & Credentials](#testing--credentials)

---

## Authentication

The API supports two authentication methods:

### 1. OAuth 2.0 (Recommended for Mobile Apps)

The API uses OAuth 2.0 with the following grant types:
- **Password Grant** (for direct login)
- **Authorization Code with PKCE** (for web flows)
- **Refresh Token** (for token renewal)

#### Getting OAuth Configuration (Multi-Site Support)

**⚠️ Important for Multi-Site Deployments:**

Instead of hardcoding the OAuth `client_id` in your mobile app, you should dynamically fetch the OAuth configuration from each site. This allows a single mobile app to support multiple Frappe sites/companies with different OAuth configurations.

**Endpoint:** `GET /api/method/crm.api.mobile_api.get_oauth_config`

**Authentication:** None required (guest access allowed)

**Example Request:**
```http
GET /api/method/crm.api.mobile_api.get_oauth_config
Host: trust.jossoor.org
```

**Example Response:**
```json
{
  "message": {
    "client_id": "3rcioodn8t",
    "scope": "all openid",
    "redirect_uri": "app.trust://oauth2redirect"
  }
}
```

**Usage Pattern:**
1. User enters their company's site URL (e.g., `https://company.example.com`)
2. App calls `GET https://company.example.com/api/method/crm.api.mobile_api.get_oauth_config`
3. **Handle errors**: Check for HTTP 417 or 500 errors indicating missing configuration
4. **Validate response**: Ensure `client_id` is present and not empty
5. App receives the `client_id` and other OAuth parameters for that specific site
6. App uses the received `client_id` for all subsequent OAuth requests to that site

**Example Error Handling (Flutter/Dart):**
```dart
try {
  final response = await http.get(
    Uri.parse('$siteUrl/api/method/crm.api.mobile_api.get_oauth_config'),
  );
  
  if (response.statusCode == 200) {
    final data = json.decode(response.body)['message'];
    if (data['client_id']?.isNotEmpty ?? false) {
      // Use client_id for OAuth
      return OAuthConfig.fromJson(data);
    } else {
      throw Exception('OAuth client_id is missing');
    }
  } else if (response.statusCode == 417 || response.statusCode == 500) {
    // Site not configured
    throw OAuthConfigException('Site is not configured for mobile access');
  }
} catch (e) {
  // Display user-friendly error message
  showErrorDialog('Unable to connect. Please contact your administrator.');
}
```

This pattern allows the same Flutter app binary to work with multiple sites without code changes.

#### OAuth Client Configuration (Example for Trust.com)

| Parameter | Value |
|-----------|-------|
| Client ID | `3rcioodn8t` (dynamically fetched via `get_oauth_config`) |
| Grant Types | Password, Authorization Code (PKCE), Refresh Token |
| Redirect URIs | `app.trust://oauth2redirect` |
| Scopes | `all openid` |

#### Getting Access Token (Password Grant)

**Endpoint:** `POST /api/method/frappe.integrations.oauth2.get_token`

**Request:**
```http
POST /api/method/frappe.integrations.oauth2.get_token
Content-Type: application/x-www-form-urlencoded

grant_type=password
&username=your_username
&password=your_password
&client_id=3rcioodn8t
&scope=all openid
```

**Response:**
```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "expires_in": 3600,
  "token_type": "Bearer",
  "scope": "all openid",
  "refresh_token": "9afhT4BMjUnabSGqEOU3HH..."
}
```

#### Using Access Token

Include the access token in the `Authorization` header:

```http
Authorization: Bearer {access_token}
```

#### Refreshing Token

**Endpoint:** `POST /api/method/frappe.integrations.oauth2.get_token`

**Request:**
```http
POST /api/method/frappe.integrations.oauth2.get_token
Content-Type: application/x-www-form-urlencoded

grant_type=refresh_token
&refresh_token={refresh_token}
&client_id=3rcioodn8t
```

### 2. Session-Based Authentication (Alternative)

You can also use traditional session-based authentication by logging in through:

```http
POST /api/method/login
Content-Type: application/x-www-form-urlencoded

usr=your_username&pwd=your_password
```

The session cookie will be automatically managed by the HTTP client.

---

## API Endpoints

### 0. Get OAuth Configuration (Multi-Site)

Retrieve OAuth 2.0 configuration for the current site. This endpoint should be called first to get the site-specific `client_id` before authentication.

**Endpoint:** `GET /api/method/crm.api.mobile_api.get_oauth_config`

**Authentication Required:** No (guest access allowed)

**Parameters:** None

**Example Request:**
```http
GET /api/method/crm.api.mobile_api.get_oauth_config
Host: trust.jossoor.org
```

**Example Response:**
```json
{
  "message": {
    "client_id": "3rcioodn8t",
    "scope": "all openid",
    "redirect_uri": "app.trust://oauth2redirect"
  }
}
```

**Response Fields:**
- `client_id`: OAuth 2.0 Client ID for this site
- `scope`: OAuth scopes (space-separated)
- `redirect_uri`: OAuth redirect URI for mobile app

**Error Handling:**
- **HTTP 417 (Expectation Failed)**: If `client_id` is not configured on the server, the endpoint will return an error:
  ```json
  {
    "exception": "Mobile OAuth Settings: client_id is missing."
  }
  ```
- **HTTP 500**: If Mobile OAuth Settings document doesn't exist or there's a server error:
  ```json
  {
    "exception": "Mobile OAuth Settings not configured. Please contact system administrator."
  }
  ```

**Important Notes:**
- ⚠️ **Always handle errors**: This endpoint will throw an error if `client_id` is missing. Your app should display a user-friendly message and guide users to contact their system administrator.
- ✅ **Validate response**: Always check that `client_id` is not empty before using it for OAuth requests.
- 🔒 **Security**: The endpoint allows guest access, but only returns non-sensitive OAuth configuration. The actual `client_id` is not a secret for public clients.

**Use Case:**
This endpoint enables multi-site support in mobile apps. Instead of hardcoding OAuth credentials for a single site, the app can:
1. Accept a site URL from the user (e.g., `https://company1.com`, `https://company2.com`)
2. Call this endpoint to fetch the OAuth configuration for that specific site
3. Handle errors gracefully if the site is not properly configured
4. Use the returned `client_id` for OAuth authentication with that site

**Browser Test:**
```
https://trust.jossoor.org/api/method/crm.api.mobile_api.get_oauth_config
```

---

### 2. Get Home Tasks

Retrieve the top tasks for today, sorted by priority and due date.

**Endpoint:** `GET /api/method/crm.api.mobile_api.home_tasks`

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| limit | integer | No | 5 | Maximum number of tasks to return |

**Example Request:**
```http
GET /api/method/crm.api.mobile_api.home_tasks?limit=5
Authorization: Bearer {access_token}
```

**Example Response:**
```json
{
  "message": {
    "today": [
      {
        "name": 105,
        "title": "Mobile QA",
        "status": "Todo",
        "priority": "Medium",
        "start_date": "2025-12-03",
        "modified": "2025-12-03 15:25:08.452656",
        "due_date": null,
        "assigned_to": null
      }
    ],
    "limit": 5
  }
}
```

---

### 3. Filter Tasks

Retrieve tasks with advanced filtering options and page-based pagination.

**Endpoint:** `GET /api/method/crm.api.mobile_api.filter_tasks`

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| date_from | string | No | - | Start date filter (YYYY-MM-DD) |
| date_to | string | No | - | End date filter (YYYY-MM-DD) |
| importance | string | No | - | Priority filter (comma-separated: High,Medium,Low) |
| status | string | No | - | Status filter (comma-separated) |
| limit | integer | No | 20 | Page size - maximum tasks per page |
| page | integer | No | 1 | Page number (1-based) |
| order_by | string | No | "modified desc" | Sort order |

**Pagination:**
- The API uses page-based pagination
- `page=1` returns tasks 1-20 (if limit=20)
- `page=2` returns tasks 21-40 (if limit=20)
- Response includes metadata: `total`, `has_next`, `page`, `page_size`

**Pagination Safety:**
- ✅ **Safe parameter handling**: The API safely handles invalid or missing pagination parameters
- ✅ **Default values**: If `page` is missing, invalid, or less than 1, it defaults to `1`
- ✅ **Default limit**: If `limit` is missing or invalid, it defaults to `20`
- ✅ **No crashes**: Invalid values (null, empty strings, non-numeric) are handled gracefully
- 💡 **Best practice**: Always validate parameters client-side, but the API will not crash if invalid values are passed

**Example Request (First Page):**
```http
GET /api/method/crm.api.mobile_api.filter_tasks?date_from=2025-12-01&date_to=2025-12-31&importance=High&page=1&limit=10
Authorization: Bearer {access_token}
```

**Example Request (Second Page):**
```http
GET /api/method/crm.api.mobile_api.filter_tasks?importance=High&page=2&limit=10
Authorization: Bearer {access_token}
```

**Example Response:**
```json
{
  "message": {
    "data": [
      {
        "name": 105,
        "title": "Mobile QA",
        "status": "Todo",
        "priority": "Medium",
        "start_date": "2025-12-03",
        "modified": "2025-12-03 15:25:08.452656",
        "due_date": null,
        "assigned_to": null
      },
      {
        "name": 104,
        "title": "Client Call",
        "status": "In Progress",
        "priority": "High",
        "start_date": "2025-12-02",
        "modified": "2025-12-02 10:15:30.123456",
        "due_date": "2025-12-05 16:00:00",
        "assigned_to": "user@example.com"
      }
    ],
    "page": 1,
    "page_size": 10,
    "total": 47,
    "has_next": true
  }
}
```

**Response Fields:**
- `data`: Array of task objects
- `page`: Current page number
- `page_size`: Number of items per page (same as limit)
- `total`: Total number of matching tasks across all pages
- `has_next`: Boolean indicating if there are more pages

---

### 4. Get Main Page Buckets

Retrieve tasks organized into three categories: Today, Late, and Upcoming.

**Endpoint:** `GET /api/method/crm.api.mobile_api.main_page_buckets`

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| min_each | integer | No | 5 | Minimum tasks to return per bucket |

**Example Request:**
```http
GET /api/method/crm.api.mobile_api.main_page_buckets?min_each=3
Authorization: Bearer {access_token}
```

**Example Response:**
```json
{
  "message": {
    "today": [
      {
        "name": 105,
        "title": "Mobile QA",
        "status": "Todo",
        "priority": "Medium",
        "start_date": "2025-12-03",
        "modified": "2025-12-03 15:25:08.452656",
        "due_date": null,
        "assigned_to": null
      }
    ],
    "late": [
      {
        "name": 94,
        "title": "Meeting",
        "status": "In Progress",
        "priority": "Medium",
        "start_date": null,
        "modified": "2025-09-10 14:05:08.915357",
        "due_date": "2025-09-11 14:04:55",
        "assigned_to": "Administrator"
      }
    ],
    "upcoming": [],
    "min_each": 3
  }
}
```

**Bucket Definitions:**
- **today**: Tasks with `start_date` = today OR `due_date` = today
- **late**: Tasks with `due_date` < today AND status NOT "Done" or "Canceled"
- **upcoming**: Tasks with `start_date` > today

---

### 5. Create Task

Create a new task.

**Endpoint:** `POST /api/method/crm.api.mobile_api.create_task`

**Request Body (JSON):**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| title | string | Yes | Task title |
| task_type | string | Yes | Task type: "Meeting", "Call", or "Property Showing" |
| priority | string | No | Priority: "Low", "Medium", or "High" |
| status | string | No | Status (default: "Todo") |
| start_date | string | No | Start date (YYYY-MM-DD) |
| due_date | string | No | Due date (YYYY-MM-DD HH:MM:SS) |
| description | string | No | Task description |
| assigned_to | string | No | User email to assign |

**Example Request:**
```http
POST /api/method/crm.api.mobile_api.create_task
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "title": "Meeting with client",
  "task_type": "Meeting",
  "priority": "High",
  "start_date": "2025-12-05",
  "description": "Discuss project requirements"
}
```

**Example Response:**
```json
{
  "message": {
    "name": 113,
    "title": "Meeting with client",
    "status": "Todo",
    "priority": "High",
    "start_date": "2025-12-05",
    "modified": "2025-12-03 23:10:15.123456",
    "due_date": null,
    "assigned_to": null
  }
}
```

---

### 6. Edit Task

Update an existing task.

**Endpoint:** `POST /api/method/crm.api.mobile_api.edit_task`

**Request Body (JSON):**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| task_id | integer | Yes | Task ID to update |
| title | string | No | New task title |
| task_type | string | No | New task type |
| priority | string | No | New priority |
| status | string | No | New status |
| start_date | string | No | New start date |
| due_date | string | No | New due date |
| description | string | No | New description |
| assigned_to | string | No | New assignee |

**Example Request:**
```http
POST /api/method/crm.api.mobile_api.edit_task
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "task_id": 113,
  "priority": "Medium",
  "description": "Updated description with more details"
}
```

**Example Response:**
```json
{
  "message": {
    "name": 113,
    "title": "Meeting with client",
    "status": "Todo",
    "priority": "Medium",
    "start_date": "2025-12-05",
    "modified": "2025-12-03 23:15:22.654321",
    "due_date": null,
    "assigned_to": null
  }
}
```

---

### 7. Update Task Status

Update only the status of a task.

**Endpoint:** `POST /api/method/crm.api.mobile_api.update_status`

**Request Body (JSON):**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| task_id | integer | Yes | Task ID to update |
| status | string | Yes | New status |

**Example Request:**
```http
POST /api/method/crm.api.mobile_api.update_status
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "task_id": 113,
  "status": "Done"
}
```

**Example Response:**
```json
{
  "message": {
    "name": 113,
    "title": "Meeting with client",
    "status": "Done",
    "priority": "Medium",
    "start_date": "2025-12-05",
    "modified": "2025-12-03 23:20:33.789012",
    "due_date": null,
    "assigned_to": null
  }
}
```

---

### 8. Delete Task

Delete a task permanently.

**Endpoint:** `POST /api/method/crm.api.mobile_api.delete_task`

**Request Body (JSON):**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| task_id | integer | Yes | Task ID to delete |

**Example Request:**
```http
POST /api/method/crm.api.mobile_api.delete_task
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "task_id": 113
}
```

**Example Response:**
```json
{
  "message": "Task 113 deleted successfully",
  "ok": true
}
```

---

## Data Models

### Task Object

| Field | Type | Description |
|-------|------|-------------|
| name | integer | Unique task ID |
| title | string | Task title |
| status | string | Current status |
| priority | string | Task priority |
| start_date | string | Start date (YYYY-MM-DD) |
| modified | string | Last modification timestamp |
| due_date | string/null | Due date and time |
| assigned_to | string/null | Assigned user email |
| description | string | Task description (only in detail views) |
| task_type | string | Task type |

### Valid Field Values

#### Task Types (Required for create)
- `"Meeting"`
- `"Call"`
- `"Property Showing"`

**Important:** These values must be in English. The UI may display translated labels, but the API accepts only English values.

#### Status Values
- `"Backlog"`
- `"Todo"`
- `"In Progress"`
- `"Done"`
- `"Canceled"`

#### Priority Values
- `"Low"`
- `"Medium"`
- `"High"`

---

## Error Handling

### HTTP Status Codes

| Code | Meaning | Description |
|------|---------|-------------|
| 200 | OK | Request successful |
| 400 | Bad Request | Invalid request parameters |
| 401 | Unauthorized | Missing or invalid authentication |
| 403 | Forbidden | Insufficient permissions |
| 404 | Not Found | Resource not found |
| 417 | Expectation Failed | Validation error (e.g., invalid field value) |
| 500 | Internal Server Error | Server error |

### Error Response Format

```json
{
  "exception": "frappe.exceptions.ValidationError: Task Type cannot be General...",
  "exc_type": "ValidationError",
  "_server_messages": "[...]"
}
```

### Common Errors

**1. Invalid Task Type**
```json
{
  "exception": "Task Type cannot be General. Must be one of 'Meeting', 'Property Showing', 'Call'"
}
```
**Solution:** Use one of the valid English task types: "Meeting", "Call", or "Property Showing"

**2. Invalid OAuth Token**
```json
{
  "exception": "Invalid or expired token"
}
```
**Solution:** Refresh your access token using the refresh token

**3. Missing Required Field**
```json
{
  "exception": "Task title is required"
}
```
**Solution:** Ensure all required fields are provided

**4. Insufficient Permissions**
```json
{
  "exception": "Not permitted to access this resource"
}
```
**Solution:** Check user permissions for CRM Task DocType

**5. Missing OAuth Client ID**
```json
{
  "exception": "Mobile OAuth Settings: client_id is missing."
}
```
**Solution:** This error occurs when calling `get_oauth_config` and the site administrator has not configured the OAuth Client ID. Display a user-friendly message: "This site is not configured for mobile access. Please contact your system administrator."

**6. OAuth Settings Not Configured**
```json
{
  "exception": "Mobile OAuth Settings not configured. Please contact system administrator."
}
```
**Solution:** The Mobile OAuth Settings document doesn't exist or there's a server error. Guide users to contact their system administrator.

---

## Testing & Credentials

### Test Environment

**Server:** Trust.com  
**Base URL:** `https://trust.jossoor.org`

### Test Credentials

| Parameter | Value |
|-----------|-------|
| Username | `Administrator` |
| Password | `1234` |
| OAuth Client ID | `3rcioodn8t` |

**Alternative Test User:**
- Username: `test@trust.com`
- Password: `test1234`

### Quick Test (Browser)

For GET endpoints, you can test directly in the browser:

**No Authentication Required:**
   - OAuth Config: `https://trust.jossoor.org/api/method/crm.api.mobile_api.get_oauth_config`

**After Login at `https://trust.jossoor.org/login`:**
   - Home Tasks: `https://trust.jossoor.org/api/method/crm.api.mobile_api.home_tasks?limit=5`
   - Filter Tasks (Page 1): `https://trust.jossoor.org/api/method/crm.api.mobile_api.filter_tasks?page=1&limit=10`
   - Filter Tasks (Page 2): `https://trust.jossoor.org/api/method/crm.api.mobile_api.filter_tasks?page=2&limit=10`
   - Main Page Buckets: `https://trust.jossoor.org/api/method/crm.api.mobile_api.main_page_buckets`

### Quick Test (Command Line)

```bash
# 1. Get OAuth configuration (no auth required)
curl "https://trust.jossoor.org/api/method/crm.api.mobile_api.get_oauth_config"

# 2. Get OAuth token (use client_id from step 1)
curl -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password&username=Administrator&password=1234&client_id=3rcioodn8t&scope=all openid"

# Use token in subsequent requests
curl "https://trust.jossoor.org/api/method/crm.api.mobile_api.home_tasks?limit=5" \
  -H "Authorization: Bearer {access_token}"

# Test pagination - Page 1
curl "https://trust.jossoor.org/api/method/crm.api.mobile_api.filter_tasks?page=1&limit=10" \
  -H "Authorization: Bearer {access_token}"

# Test pagination - Page 2
curl "https://trust.jossoor.org/api/method/crm.api.mobile_api.filter_tasks?page=2&limit=10" \
  -H "Authorization: Bearer {access_token}"

# Test with filters and pagination
curl "https://trust.jossoor.org/api/method/crm.api.mobile_api.filter_tasks?importance=High&status=Todo&page=1&limit=20" \
  -H "Authorization: Bearer {access_token}"
```

---

## Best Practices

### 1. Token Management
- Store access tokens securely
- Implement automatic token refresh before expiration (3600 seconds = 1 hour)
- Never store tokens in plain text or version control

### 2. Error Handling
- Always check HTTP status codes
- Parse error messages for user-friendly display
- Implement retry logic for network failures
- Handle token expiration gracefully
- **OAuth Config Errors**: When calling `get_oauth_config`, handle the case where `client_id` is missing:
  - Display a user-friendly message: "This site is not configured for mobile access. Please contact your administrator."
  - Do not proceed with OAuth authentication if `client_id` is missing
  - Log the error for debugging purposes

### 3. Data Validation
- Validate field values client-side before API calls
- Use English values for task_type, status, and priority
- Handle date formats correctly (YYYY-MM-DD for dates, YYYY-MM-DD HH:MM:SS for datetime)

### 4. Performance
- **Use pagination for `filter_tasks`**: Always use `page` and `limit` parameters
- Check `has_next` to determine if more pages are available
- **Safe pagination**: The API handles invalid pagination parameters gracefully:
  - Invalid `page` values default to `1`
  - Invalid `limit` values default to `20`
  - You can safely pass user input without extensive validation
- Cache frequently accessed data (with appropriate TTL)
- Use appropriate `limit` parameters to avoid over-fetching (default: 20)
- Consider implementing local database for offline support
- Example pagination implementation:
  ```
  // Load first page
  GET /api/method/crm.api.mobile_api.filter_tasks?page=1&limit=20
  
  // If has_next is true, load next page
  GET /api/method/crm.api.mobile_api.filter_tasks?page=2&limit=20
  ```

### 5. Security
- Always use HTTPS
- Implement certificate pinning for production apps
- Validate SSL certificates
- Use OAuth 2.0 with PKCE for web flows
- Implement proper session management

---

## Support & Resources

### API Versioning
The API follows semantic versioning. Breaking changes will result in a new major version.

### Rate Limiting
Currently, there are no enforced rate limits, but please implement reasonable request throttling in your application.

### CORS
CORS is configured to allow requests from the same domain. For cross-origin requests from mobile apps, use OAuth 2.0 Bearer tokens.

### Additional Documentation
- Frappe Framework: https://frappeframework.com
- OAuth 2.0 Specification: https://oauth.net/2/

---

## Changelog

### Version 1.2.1 (December 2025)
- ✅ **IMPROVED**: Enhanced error handling for `get_oauth_config` - now throws clear error if `client_id` is missing
- ✅ **IMPROVED**: Safer pagination parameter handling in `filter_tasks` - gracefully handles invalid values
- ✅ **IMPROVED**: Unified task formatting across all endpoints for consistency
- ✅ **IMPROVED**: Better error messages for misconfigured OAuth settings
- No breaking changes - all response formats remain the same

### Version 1.2 (December 2025)
- ✅ **NEW**: Added `get_oauth_config` endpoint for multi-site support
- ✅ **NEW**: Mobile apps can now dynamically fetch OAuth configuration per site
- ✅ **NEW**: Created "Mobile OAuth Settings" Single DocType for per-site configuration
- ✅ **FEATURE**: Single mobile app can now support multiple Frappe sites/companies
- No more hardcoded `client_id` in mobile apps

### Version 1.1 (December 2025)
- ✅ **NEW**: Added page-based pagination to `filter_tasks` endpoint
- ✅ **NEW**: Response now includes pagination metadata (`page`, `page_size`, `total`, `has_next`)
- ✅ **IMPROVED**: Better performance for large data sets
- Changed `filter_tasks` default limit from 50 to 20
- Removed `offset` parameter in favor of `page` parameter

### Version 1.0 (December 2025)
- Initial API release
- OAuth 2.0 support added
- All CRUD operations implemented
- Mobile-optimized endpoints added

---

**Document Version:** 1.2.1  
**Last Updated:** December 4, 2025  
**Maintained By:** CRM Development Team

