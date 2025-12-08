# CRM Task Mobile API

A Frappe app module providing REST endpoints for CRM Task management designed for mobile/Flutter applications.

## Table of Contents
- [Authentication Model](#authentication-model)
- [Installation](#installation)
- [Permissions](#permissions)
- [API Overview](#api-overview)
- [Quick Start](#quick-start)
- [Field Reference](#field-reference)

---

## Authentication Model

### Important: No Custom Login

This API **does not implement** any custom authentication or login endpoints. It relies entirely on Frappe's standard session-based authentication.

### How to Authenticate

1. **Login to obtain session cookie:**
   ```
   POST https://your-site.com/api/method/login
   Content-Type: application/x-www-form-urlencoded
   
   usr=user@example.com&pwd=password
   ```

2. **Extract session cookies from response:**
   - `sid` - Session ID cookie
   - `user_id` - User ID cookie
   - `full_name` - User's full name cookie

3. **Include cookies in all subsequent API requests:**
   ```
   GET https://your-site.com/api/method/crm.api.mobile_api.home_tasks
   Cookie: sid=<session-id>; user_id=<user-id>; full_name=<full-name>
   ```

### Session Management

- Sessions persist based on Frappe's configured session timeout
- Sessions are tied to the user who logged in
- All permissions are enforced based on the authenticated user's roles
- To logout: `POST https://your-site.com/api/method/logout`

---

## Installation

### Prerequisites

- Frappe Framework (v14 or later)
- Frappe CRM app already installed
- Site with CRM Task doctype enabled

### Installation Steps

This module is part of the CRM app, so no separate installation is required. If you have the CRM app installed, the mobile API endpoints are automatically available.

To verify the CRM app is installed:

```bash
# From frappe-bench directory
bench --site your-site.com list-apps
```

If CRM is not installed:

```bash
# Get the app
bench get-app crm

# Install on your site
bench --site your-site.com install-app crm

# Restart bench
bench restart
```

### Enabling the API

The API endpoints are whitelisted and available immediately. No additional configuration required.

---

## Permissions

### Required Roles

Users must have one of these roles to access CRM Task via the API:
- **Sales User** - Full CRUD access
- **Sales Manager** - Full CRUD access

### Permission Behavior

- All API endpoints respect standard Frappe permissions
- Users can only view/edit tasks they have permission for
- No permission bypass is used (`ignore_permissions=False` everywhere)
- Attempting to access unauthorized tasks returns `403 Forbidden`

### Assigning Roles to Users

```bash
# Via bench console
bench --site your-site.com console

# In console
frappe.get_doc("User", "user@example.com").add_roles("Sales User")
```

Or assign via Frappe UI: **User Management → User → Roles**

---

## API Overview

All endpoints are available at:
```
https://your-site.com/api/method/crm.api.mobile_api.<endpoint_name>
```

### Available Endpoints

1. **`create_task`** - Create a new CRM Task
2. **`edit_task`** - Edit an existing CRM Task
3. **`delete_task`** - Delete a CRM Task
4. **`update_status`** - Update task status only
5. **`filter_tasks`** - Filter and search tasks
6. **`home_tasks`** - Get today's top N tasks
7. **`main_page_buckets`** - Get tasks organized by today/late/upcoming

For detailed endpoint documentation, see **[API_ENDPOINTS.md](./API_ENDPOINTS.md)**

---

## Quick Start

### 1. Login and Get Session Cookie

```bash
curl -X POST https://your-site.com/api/method/login \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "usr=user@example.com&pwd=password" \
  -c cookies.txt
```

### 2. Create a Task

```bash
curl -X POST https://your-site.com/api/method/crm.api.mobile_api.create_task \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{
    "task_type": "Call",
    "title": "Follow up with client",
    "priority": "High",
    "status": "Todo"
  }'
```

### 3. Get Today's Tasks

```bash
curl -X GET "https://your-site.com/api/method/crm.api.mobile_api.home_tasks?limit=5" \
  -b cookies.txt
```

### 4. Get Main Page Buckets

```bash
curl -X GET "https://your-site.com/api/method/crm.api.mobile_api.main_page_buckets?min_each=5" \
  -b cookies.txt
```

---

## Field Reference

### CRM Task Fields Used by API

The API uses the following fields from the CRM Task doctype:

| Field | Type | Description | Required | Default |
|-------|------|-------------|----------|---------|
| `name` | String | Auto-generated task ID | Auto | - |
| `task_type` | Select | Meeting/Property Showing/Call | Yes | - |
| `title` | String | Task title | No | Derived from description |
| `status` | Select | Backlog/Todo/In Progress/Done/Canceled | No | Todo |
| `priority` | Select | Low/Medium/High | No | Medium |
| `start_date` | Date | Task start date | No | Today |
| `due_date` | Datetime | Task due date/time | No | - |
| `assigned_to` | Link | User assigned to task | No | - |
| `description` | Text | Task description | No | - |
| `modified` | Datetime | Last modified timestamp | Auto | - |

### Important Notes

1. **Date Field**: The API uses `start_date` (not `exp_start_date`) as this is the actual field name in the CRM Task doctype.

2. **Task Type**: This is a required field. Valid values:
   - `Meeting`
   - `Property Showing`
   - `Call`

3. **Status Values**:
   - `Backlog`
   - `Todo`
   - `In Progress`
   - `Done`
   - `Canceled`

4. **Priority Values**:
   - `Low`
   - `Medium`
   - `High`

5. **Active Statuses**: For "late tasks" filtering, active statuses are: `Backlog`, `Todo`, `In Progress`

---

## Response Format

All endpoints return compact, consistent JSON responses with core fields only:

```json
{
  "name": "12345",
  "title": "Follow up with client",
  "status": "Todo",
  "priority": "High",
  "start_date": "2025-12-03",
  "due_date": "2025-12-03 15:00:00",
  "assigned_to": "user@example.com",
  "modified": "2025-12-03 10:30:45"
}
```

List endpoints wrap tasks in a data array:
```json
{
  "data": [...]
}
```

---

## Error Handling

### Common Errors

**401 Unauthorized**
```json
{
  "exc_type": "AuthenticationError",
  "exception": "Not permitted"
}
```
**Solution**: Ensure you're logged in and including session cookies.

**403 Forbidden**
```json
{
  "exc_type": "PermissionError",
  "exception": "Not permitted"
}
```
**Solution**: User doesn't have required role (Sales User/Sales Manager).

**404 Not Found**
```json
{
  "exc_type": "DoesNotExistError",
  "exception": "CRM Task <name> not found"
}
```
**Solution**: Task doesn't exist or you don't have permission to view it.

**417 Validation Error**
```json
{
  "exc_type": "ValidationError",
  "exception": "Task Type is required"
}
```
**Solution**: Check required fields are provided.

---

## Testing

### Test with Postman/Insomnia

Import the provided collection: **[POSTMAN_COLLECTION.json](./POSTMAN_COLLECTION.json)**

Set environment variables:
- `base_url` - Your site URL (e.g., `https://your-site.com`)
- `session_sid` - Session ID from login
- `user_id` - User ID from login
- `full_name` - Full name from login

### Test with Python

```python
import requests

# Login
session = requests.Session()
response = session.post(
    'https://your-site.com/api/method/login',
    data={'usr': 'user@example.com', 'pwd': 'password'}
)

# Create task
response = session.post(
    'https://your-site.com/api/method/crm.api.mobile_api.create_task',
    json={
        'task_type': 'Call',
        'title': 'Test task',
        'priority': 'High'
    }
)
print(response.json())
```

---

## Flutter Integration Notes

For Flutter developers receiving this handover:

1. **Use the provided Postman collection** to understand request/response formats
2. **Implement session cookie management** in your HTTP client (dio, http, etc.)
3. **Store session cookies securely** after login (use flutter_secure_storage)
4. **Include cookies in every request** using your HTTP client's cookie manager
5. **Handle 401/403 errors** by redirecting to login screen
6. **Refer to API_ENDPOINTS.md** for complete parameter and response documentation
7. **See FLUTTER_HANDOVER.md** for integration checklist

---

## Support & Troubleshooting

### Common Issues

**Issue**: "Not permitted" errors
- **Check**: User has Sales User or Sales Manager role
- **Check**: User is logged in and session is valid
- **Check**: Cookies are being sent with requests

**Issue**: "CRM Task not found"
- **Check**: Task name/ID is correct
- **Check**: User has permission to view the task
- **Check**: Task hasn't been deleted

**Issue**: "Task Type is required"
- **Check**: `task_type` parameter is included in create_task
- **Check**: Value is one of: Meeting, Property Showing, Call

### Debug Mode

To see detailed error messages in responses:

```bash
# In site_config.json
{
  "developer_mode": 1
}
```

Then restart:
```bash
bench restart
```

---

## No Notifications or CRM Lead References

As per requirements:

- ✅ No FCM/device token logic
- ✅ No notification endpoints
- ✅ No CRM Lead references or linking
- ✅ No custom login/token authentication
- ✅ Standard Frappe session cookies only

---

## License

Same as parent CRM app (MIT License)

---

## Additional Documentation

- **[API_ENDPOINTS.md](./API_ENDPOINTS.md)** - Detailed endpoint documentation
- **[QA_CHECKLIST.md](./QA_CHECKLIST.md)** - Testing checklist
- **[FLUTTER_HANDOVER.md](./FLUTTER_HANDOVER.md)** - Flutter developer guide
- **[POSTMAN_COLLECTION.json](./POSTMAN_COLLECTION.json)** - API testing collection

