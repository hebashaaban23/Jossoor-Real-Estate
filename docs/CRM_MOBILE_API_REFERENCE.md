# CRM Mobile API Reference

**Version:** 2.0  
**Last Updated:** December 2025  
**Base URL:** `https://trust.jossoor.org/api/method/crm.api.mobile_api`

---

## Table of Contents

1. [Authentication](#authentication)
2. [Common Patterns](#common-patterns)
3. [Endpoints](#endpoints)
   - [GET /home_tasks](#get-home_tasks)
   - [GET /filter_tasks](#get-filter_tasks)
   - [GET /main_page_buckets](#get-main_page_buckets)
   - [POST /create_task](#post-create_task)
   - [POST /edit_task](#post-edit_task)
   - [POST /update_status](#post-update_status)
   - [POST /delete_task](#post-delete_task)
4. [Data Models](#data-models)
5. [Error Handling](#error-handling)
6. [Examples](#examples)

---

## Authentication

### OAuth2 Bearer Token (Recommended)

All endpoints support OAuth2 Bearer token authentication:

```http
GET /api/method/crm.api.mobile_api.home_tasks
Host: trust.jossoor.org
Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGc...
```

**How to get a token:**

```bash
# Password Grant (for testing)
curl -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "username=user@example.com" \
  -d "password=your_password" \
  -d "client_id=YOUR_CLIENT_ID" \
  -d "scope=all openid"
```

**Response:**
```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "refresh_token_here",
  "scope": "all openid"
}
```

For mobile apps, use **Authorization Code + PKCE**. See [OAuth2 Setup Guide](./OAUTH2_SETUP_AND_OPERATIONS.md).

### Session Cookies (Legacy)

Alternatively, you can use session-based authentication:

```bash
# Login
curl -X POST "https://trust.jossoor.org/api/method/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "usr=user@example.com" \
  --data-urlencode "pwd=your_password" \
  -c cookies.txt

# Use cookies for subsequent requests
curl "https://trust.jossoor.org/api/method/crm.api.mobile_api.home_tasks" \
  -b cookies.txt
```

**Note:** OAuth2 Bearer tokens are recommended for mobile apps due to better security and token management.

---

## Common Patterns

### Base URL Pattern

```
https://<site-domain>/api/method/crm.api.mobile_api/<endpoint>
```

Examples:
- `https://trust.jossoor.org/api/method/crm.api.mobile_api.home_tasks`
- `https://site2.example.com/api/method/crm.api.mobile_api.filter_tasks`

### Request Headers

**With OAuth Token:**
```http
Authorization: Bearer <access_token>
Content-Type: application/json  (for POST requests)
```

**With Session Cookies:**
```http
Cookie: sid=<session_id>; system_user=yes
Content-Type: application/json  (for POST requests)
```

### Response Format

All endpoints return JSON with a `message` envelope:

```json
{
  "message": {
    // Endpoint-specific data
  }
}
```

Errors return:
```json
{
  "exception": "Error description",
  "exc_type": "ErrorType",
  "_server_messages": "[...]"
}
```

---

## Endpoints

### GET /home_tasks

Get today's tasks for the home screen.

**Endpoint:**
```
GET /api/method/crm.api.mobile_api.home_tasks
```

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `limit` | integer | No | 5 | Maximum number of tasks to return |

**Example Request:**

```bash
curl "https://trust.jossoor.org/api/method/crm.api.mobile_api.home_tasks?limit=10" \
  -H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGc..."
```

**Example Response:**

```json
{
  "message": {
    "today": [
      {
        "name": 123,
        "title": "Call client regarding proposal",
        "status": "Open",
        "priority": "High",
        "start_date": "2025-12-03",
        "modified": "2025-12-03 09:30:00.123456",
        "due_date": "2025-12-03 17:00:00",
        "assigned_to": "sales@trust.com"
      },
      {
        "name": 124,
        "title": "Follow up with lead",
        "status": "In Progress",
        "priority": "Medium",
        "start_date": "2025-12-03",
        "modified": "2025-12-03 08:15:00.654321"
      }
    ],
    "limit": 10
  }
}
```

**Notes:**
- Returns tasks scheduled for today (`start_date` = today)
- Sorted by priority (High → Medium → Low) then by creation time
- Compact response includes only essential fields

---

### GET /filter_tasks

Filter and search tasks by date range, priority, and status.

**Endpoint:**
```
GET /api/method/crm.api.mobile_api.filter_tasks
```

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `date_from` | string | No | - | Start date (YYYY-MM-DD) |
| `date_to` | string | No | - | End date (YYYY-MM-DD) |
| `importance` | string | No | - | Comma-separated priorities: `High,Medium,Low` |
| `status` | string | No | - | Comma-separated statuses: `Open,In Progress,Completed,Cancelled` |
| `limit` | integer | No | 50 | Maximum results |
| `offset` | integer | No | 0 | Pagination offset |

**Example Request:**

```bash
curl "https://trust.jossoor.org/api/method/crm.api.mobile_api.filter_tasks?\
date_from=2025-12-01&\
date_to=2025-12-31&\
importance=High,Medium&\
status=Open,In%20Progress&\
limit=20&\
offset=0" \
  -H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGc..."
```

**Example Response:**

```json
{
  "message": {
    "data": [
      {
        "name": 125,
        "title": "Prepare quarterly report",
        "status": "Open",
        "priority": "High",
        "start_date": "2025-12-05",
        "modified": "2025-12-03 10:00:00.123456",
        "due_date": "2025-12-10 18:00:00",
        "assigned_to": "manager@trust.com",
        "description": "Compile Q4 sales data and create presentation"
      },
      {
        "name": 126,
        "title": "Schedule team meeting",
        "status": "In Progress",
        "priority": "Medium",
        "start_date": "2025-12-04",
        "modified": "2025-12-03 11:30:00.654321"
      }
    ]
  }
}
```

**Notes:**
- All parameters are optional; omitting them returns all tasks (up to `limit`)
- Date range filters on `start_date` field
- Multiple priorities/statuses separated by comma
- URL-encode spaces in status values (`In%20Progress`)
- Use `offset` for pagination

---

### GET /main_page_buckets

Get tasks organized into time-based buckets: today, late (overdue), and upcoming.

**Endpoint:**
```
GET /api/method/crm.api.mobile_api.main_page_buckets
```

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `min_each` | integer | No | 5 | Minimum tasks per bucket |

**Example Request:**

```bash
curl "https://trust.jossoor.org/api/method/crm.api.mobile_api.main_page_buckets?min_each=5" \
  -H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGc..."
```

**Example Response:**

```json
{
  "message": {
    "today": [
      {
        "name": 130,
        "title": "Morning standup",
        "status": "Open",
        "priority": "Medium",
        "start_date": "2025-12-03",
        "modified": "2025-12-03 08:00:00.123456"
      }
    ],
    "late": [
      {
        "name": 128,
        "title": "Overdue client callback",
        "status": "In Progress",
        "priority": "High",
        "start_date": "2025-12-01",
        "modified": "2025-12-02 15:30:00.654321",
        "due_date": "2025-12-02 17:00:00",
        "assigned_to": "sales@trust.com"
      },
      {
        "name": 127,
        "title": "Submit expense report",
        "status": "Open",
        "priority": "Low",
        "start_date": "2025-11-30",
        "modified": "2025-11-30 09:00:00.111111",
        "due_date": "2025-12-01 12:00:00",
        "assigned_to": "employee@trust.com"
      }
    ],
    "upcoming": [
      {
        "name": 131,
        "title": "Client presentation",
        "status": "Open",
        "priority": "High",
        "start_date": "2025-12-05",
        "modified": "2025-12-03 14:00:00.987654",
        "due_date": "2025-12-05 14:00:00",
        "assigned_to": "sales@trust.com"
      }
    ],
    "min_each": 5
  }
}
```

**Bucket Logic:**

- **today**: Tasks where `start_date` = today
- **late**: Tasks where `due_date` < now OR (`start_date` < today AND not completed)
- **upcoming**: Tasks where `start_date` > today

**Notes:**
- Each bucket returns at least `min_each` tasks (if available)
- Sorted by priority and creation time within each bucket
- Useful for dashboard/home screen widgets

---

### POST /create_task

Create a new CRM Task.

**Endpoint:**
```
POST /api/method/crm.api.mobile_api.create_task
```

**Content-Type:** `application/json`

**Required Fields:**
- `title` (string) - Task title

**Optional Fields:**

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `status` | string | `Open`, `In Progress`, `Completed`, `Cancelled` | `Open` |
| `priority` | string | `Low`, `Medium`, `High` | `Medium` |
| `start_date` | string | Date (YYYY-MM-DD) or datetime (YYYY-MM-DD HH:MM:SS) | Today |
| `due_date` | string | Datetime (YYYY-MM-DD HH:MM:SS) | - |
| `description` | text | Task details | - |
| `assigned_to` | string | User email | Current user |
| `task_type` | string | Task category/type | - |

**Example Request:**

```bash
curl -X POST "https://trust.jossoor.org/api/method/crm.api.mobile_api.create_task" \
  -H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGc..." \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Review contract with ABC Corp",
    "status": "Open",
    "priority": "High",
    "start_date": "2025-12-05",
    "due_date": "2025-12-05 17:00:00",
    "description": "Review and approve the partnership agreement",
    "assigned_to": "legal@trust.com",
    "task_type": "Contract Review"
  }'
```

**Example Response:**

```json
{
  "message": {
    "name": 135,
    "title": "Review contract with ABC Corp",
    "status": "Open",
    "priority": "High",
    "start_date": "2025-12-05",
    "modified": "2025-12-03 15:45:00.123456",
    "due_date": "2025-12-05 17:00:00",
    "assigned_to": "legal@trust.com"
  }
}
```

**Notes:**
- The `name` field in the response is the task ID (integer)
- User must have permission to create CRM Tasks
- Invalid field values return 400 Bad Request

---

### POST /edit_task

Update an existing task.

**Endpoint:**
```
POST /api/method/crm.api.mobile_api.edit_task
```

**Content-Type:** `application/json`

**Required Fields:**
- `task_id` (integer) - The task ID to update

**Optional Fields:**
Any field from the create endpoint can be updated: `title`, `status`, `priority`, `start_date`, `due_date`, `description`, `assigned_to`, `task_type`

**Example Request:**

```bash
curl -X POST "https://trust.jossoor.org/api/method/crm.api.mobile_api.edit_task" \
  -H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGc..." \
  -H "Content-Type: application/json" \
  -d '{
    "task_id": 135,
    "status": "In Progress",
    "priority": "Medium",
    "description": "Contract review in progress - awaiting legal team feedback"
  }'
```

**Example Response:**

```json
{
  "message": {
    "name": 135,
    "title": "Review contract with ABC Corp",
    "status": "In Progress",
    "priority": "Medium",
    "start_date": "2025-12-05",
    "modified": "2025-12-04 10:20:00.987654",
    "due_date": "2025-12-05 17:00:00",
    "assigned_to": "legal@trust.com"
  }
}
```

**Notes:**
- Only provided fields are updated; others remain unchanged
- User must have permission to edit the task
- Task must exist (404 if not found)

---

### POST /update_status

Quickly update only the status of a task (optimized endpoint).

**Endpoint:**
```
POST /api/method/crm.api.mobile_api.update_status
```

**Content-Type:** `application/json`

**Required Fields:**
- `task_id` (integer) - Task ID
- `status` (string) - New status: `Open`, `In Progress`, `Completed`, `Cancelled`

**Example Request:**

```bash
curl -X POST "https://trust.jossoor.org/api/method/crm.api.mobile_api.update_status" \
  -H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGc..." \
  -H "Content-Type: application/json" \
  -d '{
    "task_id": 135,
    "status": "Completed"
  }'
```

**Example Response:**

```json
{
  "message": {
    "name": 135,
    "title": "Review contract with ABC Corp",
    "status": "Completed",
    "priority": "Medium",
    "start_date": "2025-12-05",
    "modified": "2025-12-05 16:30:00.123456",
    "due_date": "2025-12-05 17:00:00",
    "assigned_to": "legal@trust.com"
  }
}
```

**Notes:**
- Faster than `edit_task` when only updating status
- Common use case: marking tasks complete from mobile UI
- Invalid status values return 400 Bad Request

---

### POST /delete_task

Permanently delete a task.

**Endpoint:**
```
POST /api/method/crm.api.mobile_api.delete_task
```

**Content-Type:** `application/json`

**Required Fields:**
- `task_id` (integer) - Task ID to delete

**Example Request:**

```bash
curl -X POST "https://trust.jossoor.org/api/method/crm.api.mobile_api.delete_task" \
  -H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGc..." \
  -H "Content-Type: application/json" \
  -d '{
    "task_id": 135
  }'
```

**Example Response:**

```json
{
  "message": "Task 135 deleted successfully"
}
```

**Notes:**
- User must have delete permission on CRM Tasks
- Deletion is permanent (no trash/undo)
- Returns 404 if task doesn't exist or was already deleted

---

## Data Models

### Task Object

Standard task representation returned by API endpoints:

```typescript
interface Task {
  name: number;              // Task ID (unique identifier)
  title: string;             // Task title
  status: TaskStatus;        // Current status
  priority: TaskPriority;    // Priority level
  start_date: string;        // Start date (YYYY-MM-DD)
  modified: string;          // Last modified timestamp
  due_date?: string;         // Due date (optional, YYYY-MM-DD HH:MM:SS)
  assigned_to?: string;      // Assigned user email (optional)
  description?: string;      // Task description (optional, in some endpoints)
  task_type?: string;        // Task type/category (optional)
}
```

### Enums

**TaskStatus:**
- `Open` - Not started
- `In Progress` - Currently being worked on
- `Completed` - Finished
- `Cancelled` - Cancelled/aborted

**TaskPriority:**
- `Low` - Low priority
- `Medium` - Medium priority (default)
- `High` - High priority

### Date Formats

**Date only:**
```
YYYY-MM-DD
Example: 2025-12-03
```

**Date and time:**
```
YYYY-MM-DD HH:MM:SS
Example: 2025-12-03 14:30:00
```

**Modified timestamp (server-generated):**
```
YYYY-MM-DD HH:MM:SS.microseconds
Example: 2025-12-03 14:30:00.123456
```

---

## Error Handling

### Error Response Format

```json
{
  "exception": "Human-readable error message",
  "exc_type": "PermissionError",
  "_server_messages": "[{\"message\": \"...\", \"title\": \"...\"}]"
}
```

### Common HTTP Status Codes

| Code | Meaning | Common Causes |
|------|---------|---------------|
| 200 | OK | Request succeeded |
| 400 | Bad Request | Invalid parameters, missing required fields |
| 401 | Unauthorized | Invalid/expired token, not logged in |
| 403 | Forbidden | No permission to access resource |
| 404 | Not Found | Task doesn't exist |
| 500 | Internal Server Error | Server-side error (check logs) |

### Common Errors

#### 1. Authentication Errors

**401 Unauthorized:**
```json
{
  "exception": "Invalid or expired token",
  "exc_type": "AuthenticationError"
}
```

**Solution:** Refresh your access token or re-authenticate.

#### 2. Permission Errors

**403 Forbidden:**
```json
{
  "exception": "You don't have permission to access this resource",
  "exc_type": "PermissionError"
}
```

**Solution:** Ensure user has appropriate role (Sales User, Sales Manager, etc.).

#### 3. Validation Errors

**400 Bad Request:**
```json
{
  "exception": "Invalid status value. Must be one of: Open, In Progress, Completed, Cancelled",
  "exc_type": "ValidationError"
}
```

**Solution:** Check parameter values match allowed enums.

#### 4. Not Found Errors

**404 Not Found:**
```json
{
  "exception": "CRM Task 999 not found",
  "exc_type": "DoesNotExistError"
}
```

**Solution:** Verify task ID exists and user has access.

---

## Examples

### Example 1: Complete Workflow with OAuth

```bash
#!/bin/bash

# Configuration
BASE_URL="https://trust.jossoor.org"
CLIENT_ID="your_client_id"
USERNAME="user@example.com"
PASSWORD="password"

# Step 1: Get OAuth token
echo "Getting OAuth token..."
TOKEN_RESPONSE=$(curl -s -X POST "$BASE_URL/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "username=$USERNAME" \
  -d "password=$PASSWORD" \
  -d "client_id=$CLIENT_ID" \
  -d "scope=all openid")

ACCESS_TOKEN=$(echo $TOKEN_RESPONSE | jq -r '.access_token')
REFRESH_TOKEN=$(echo $TOKEN_RESPONSE | jq -r '.refresh_token')

echo "Access token: ${ACCESS_TOKEN:0:20}..."
echo ""

# Step 2: Get today's tasks
echo "Fetching today's tasks..."
curl -s "$BASE_URL/api/method/crm.api.mobile_api.home_tasks?limit=5" \
  -H "Authorization: Bearer $ACCESS_TOKEN" | jq .
echo ""

# Step 3: Create a new task
echo "Creating new task..."
NEW_TASK=$(curl -s -X POST "$BASE_URL/api/method/crm.api.mobile_api.create_task" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Test task from API",
    "priority": "High",
    "start_date": "2025-12-05",
    "description": "This is a test task"
  }')

TASK_ID=$(echo $NEW_TASK | jq -r '.message.name')
echo "Created task ID: $TASK_ID"
echo $NEW_TASK | jq .
echo ""

# Step 4: Update task status
echo "Updating task status..."
curl -s -X POST "$BASE_URL/api/method/crm.api.mobile_api.update_status" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"task_id\": $TASK_ID,
    \"status\": \"In Progress\"
  }" | jq .
echo ""

# Step 5: Filter tasks
echo "Filtering tasks..."
curl -s "$BASE_URL/api/method/crm.api.mobile_api.filter_tasks?importance=High&status=In%20Progress&limit=10" \
  -H "Authorization: Bearer $ACCESS_TOKEN" | jq .
echo ""

# Step 6: Mark complete
echo "Marking task complete..."
curl -s -X POST "$BASE_URL/api/method/crm.api.mobile_api.update_status" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"task_id\": $TASK_ID,
    \"status\": \"Completed\"
  }" | jq .
echo ""

# Step 7: Delete task
echo "Deleting task..."
curl -s -X POST "$BASE_URL/api/method/crm.api.mobile_api.delete_task" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"task_id\": $TASK_ID
  }" | jq .

echo "Workflow complete!"
```

### Example 2: Token Refresh

```bash
#!/bin/bash

BASE_URL="https://trust.jossoor.org"
CLIENT_ID="your_client_id"
REFRESH_TOKEN="your_refresh_token"

# Refresh access token
echo "Refreshing token..."
NEW_TOKEN=$(curl -s -X POST "$BASE_URL/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=refresh_token" \
  -d "refresh_token=$REFRESH_TOKEN" \
  -d "client_id=$CLIENT_ID")

NEW_ACCESS_TOKEN=$(echo $NEW_TOKEN | jq -r '.access_token')
NEW_REFRESH_TOKEN=$(echo $NEW_TOKEN | jq -r '.refresh_token')

echo "New access token: ${NEW_ACCESS_TOKEN:0:20}..."
echo "New refresh token: ${NEW_REFRESH_TOKEN:0:20}..."

# Use new token
curl -s "$BASE_URL/api/method/crm.api.mobile_api.home_tasks" \
  -H "Authorization: Bearer $NEW_ACCESS_TOKEN" | jq .
```

### Example 3: Python Client

```python
import requests
from datetime import datetime, timedelta

class CRMClient:
    def __init__(self, base_url, client_id, username, password):
        self.base_url = base_url
        self.client_id = client_id
        self.username = username
        self.password = password
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = None
    
    def authenticate(self):
        """Get OAuth access token using password grant"""
        response = requests.post(
            f"{self.base_url}/api/method/frappe.integrations.oauth2.get_token",
            data={
                "grant_type": "password",
                "username": self.username,
                "password": self.password,
                "client_id": self.client_id,
                "scope": "all openid"
            }
        )
        response.raise_for_status()
        data = response.json()
        
        self.access_token = data["access_token"]
        self.refresh_token = data["refresh_token"]
        self.token_expiry = datetime.now() + timedelta(seconds=data["expires_in"])
        
    def refresh_access_token(self):
        """Refresh expired access token"""
        response = requests.post(
            f"{self.base_url}/api/method/frappe.integrations.oauth2.get_token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.client_id
            }
        )
        response.raise_for_status()
        data = response.json()
        
        self.access_token = data["access_token"]
        self.refresh_token = data["refresh_token"]
        self.token_expiry = datetime.now() + timedelta(seconds=data["expires_in"])
    
    def _get_headers(self):
        """Get headers with valid access token"""
        if not self.access_token or datetime.now() >= self.token_expiry:
            if self.refresh_token:
                self.refresh_access_token()
            else:
                self.authenticate()
        
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
    
    def get_home_tasks(self, limit=5):
        """Get today's tasks"""
        response = requests.get(
            f"{self.base_url}/api/method/crm.api.mobile_api.home_tasks",
            params={"limit": limit},
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()["message"]
    
    def filter_tasks(self, date_from=None, date_to=None, importance=None, 
                     status=None, limit=50, offset=0):
        """Filter tasks"""
        params = {"limit": limit, "offset": offset}
        if date_from:
            params["date_from"] = date_from
        if date_to:
            params["date_to"] = date_to
        if importance:
            params["importance"] = importance
        if status:
            params["status"] = status
        
        response = requests.get(
            f"{self.base_url}/api/method/crm.api.mobile_api.filter_tasks",
            params=params,
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()["message"]["data"]
    
    def create_task(self, title, **kwargs):
        """Create a new task"""
        data = {"title": title, **kwargs}
        response = requests.post(
            f"{self.base_url}/api/method/crm.api.mobile_api.create_task",
            json=data,
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()["message"]
    
    def update_status(self, task_id, status):
        """Update task status"""
        response = requests.post(
            f"{self.base_url}/api/method/crm.api.mobile_api.update_status",
            json={"task_id": task_id, "status": status},
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()["message"]
    
    def delete_task(self, task_id):
        """Delete a task"""
        response = requests.post(
            f"{self.base_url}/api/method/crm.api.mobile_api.delete_task",
            json={"task_id": task_id},
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()["message"]


# Usage example
if __name__ == "__main__":
    client = CRMClient(
        base_url="https://trust.jossoor.org",
        client_id="your_client_id",
        username="user@example.com",
        password="password"
    )
    
    # Authenticate
    client.authenticate()
    print("Authenticated successfully!")
    
    # Get today's tasks
    tasks = client.get_home_tasks(limit=10)
    print(f"Found {len(tasks['today'])} tasks for today")
    
    # Create a task
    new_task = client.create_task(
        title="API test task",
        priority="High",
        start_date="2025-12-05"
    )
    print(f"Created task {new_task['name']}")
    
    # Update status
    updated = client.update_status(new_task['name'], "Completed")
    print(f"Updated task status to {updated['status']}")
    
    # Delete task
    result = client.delete_task(new_task['name'])
    print(result)
```

---

## Appendix

### A. Postman Collection

Import this collection into Postman for quick testing:

**File:** `crm_mobile_api.postman_collection.json` (see project files)

### B. Rate Limits

No explicit rate limits are enforced by default. However:
- Token requests may be throttled by Frappe
- Consider implementing client-side rate limiting
- Monitor server logs for abuse

### C. Pagination

For endpoints supporting pagination (`filter_tasks`):
- Use `limit` and `offset` parameters
- Example: Get page 2 with 20 items per page: `limit=20&offset=20`

### D. Multi-Site Considerations

When working with multiple sites:
- Each site has its own OAuth Client and `client_id`
- Use the correct domain for each site in API requests
- Tokens are site-specific and cannot be shared

---

**Document Version:** 2.0  
**Last Updated:** December 2025  
**Related:** [OAuth2 Setup Guide](./OAUTH2_SETUP_AND_OPERATIONS.md)

