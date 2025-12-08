# CRM Task Mobile API - Endpoint Documentation

Complete reference for all API endpoints with request parameters and response formats.

## Base URL

```
https://your-site.com/api/method/crm.api.mobile_api
```

## Authentication

All endpoints require authentication via Frappe session cookies obtained from login.

**Login URL**: `https://your-site.com/api/method/login`

**Required Cookies**: `sid`, `user_id`, `full_name`

---

## Endpoints

### 1. Create Task

**Endpoint**: `/create_task`

**Method**: `POST` / `GET`

**Description**: Create a new CRM Task

**Parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `task_type` | String | **Yes** | - | Task type: "Meeting", "Property Showing", or "Call" |
| `title` | String | No | - | Task title |
| `status` | String | No | "Todo" | Task status: "Backlog", "Todo", "In Progress", "Done", "Canceled" |
| `priority` | String | No | "Medium" | Priority: "Low", "Medium", "High" |
| `start_date` | Date | No | Today | Task start date (YYYY-MM-DD) |
| `due_date` | Datetime | No | - | Task due date and time (YYYY-MM-DD HH:MM:SS) |
| `description` | Text | No | - | Task description |
| `assigned_to` | String | No | - | Email of user to assign task to |

**Request Example (JSON)**:
```json
{
  "task_type": "Call",
  "title": "Follow up with client",
  "priority": "High",
  "status": "Todo",
  "start_date": "2025-12-03",
  "description": "Call to discuss contract terms",
  "assigned_to": "sales@example.com"
}
```

**Request Example (Query String)**:
```
POST /api/method/crm.api.mobile_api.create_task?task_type=Call&title=Follow+up&priority=High
```

**Response**:
```json
{
  "message": {
    "name": "12345",
    "title": "Follow up with client",
    "status": "Todo",
    "priority": "High",
    "start_date": "2025-12-03",
    "due_date": null,
    "assigned_to": "sales@example.com",
    "modified": "2025-12-03 10:30:45"
  }
}
```

**Errors**:
- `417` - Task Type is required
- `403` - Not permitted (insufficient permissions)
- `401` - Not logged in

---

### 2. Edit Task

**Endpoint**: `/edit_task`

**Method**: `POST` / `GET`

**Description**: Edit an existing CRM Task

**Parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | String | **Yes** | - | Task ID/name to edit |
| `task_type` | String | No | - | Task type |
| `title` | String | No | - | Task title |
| `status` | String | No | - | Task status |
| `priority` | String | No | - | Priority level |
| `start_date` | Date | No | - | Task start date (YYYY-MM-DD) |
| `due_date` | Datetime | No | - | Task due date and time |
| `description` | Text | No | - | Task description |
| `assigned_to` | String | No | - | Email of user to assign task to |

**Request Example**:
```json
{
  "name": "12345",
  "title": "Updated title",
  "priority": "Medium",
  "status": "In Progress"
}
```

**Response**:
```json
{
  "message": {
    "name": "12345",
    "title": "Updated title",
    "status": "In Progress",
    "priority": "Medium",
    "start_date": "2025-12-03",
    "due_date": null,
    "assigned_to": "sales@example.com",
    "modified": "2025-12-03 11:45:20"
  }
}
```

**Errors**:
- `417` - Task name is required
- `404` - Task not found
- `403` - Not permitted to edit this task

---

### 3. Delete Task

**Endpoint**: `/delete_task`

**Method**: `POST` / `GET`

**Description**: Delete a CRM Task

**Parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | String | **Yes** | - | Task ID/name to delete |

**Request Example**:
```json
{
  "name": "12345"
}
```

**Response**:
```json
{
  "message": {
    "ok": true
  }
}
```

**Errors**:
- `417` - Task name is required
- `404` - Task not found
- `403` - Not permitted to delete this task

---

### 4. Update Status

**Endpoint**: `/update_status`

**Method**: `POST` / `GET`

**Description**: Update only the status of a task (quick status change)

**Parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | String | **Yes** | - | Task ID/name |
| `status` | String | **Yes** | - | New status value |

**Status Options**:
- `Backlog`
- `Todo`
- `In Progress`
- `Done`
- `Canceled`

**Request Example**:
```json
{
  "name": "12345",
  "status": "Done"
}
```

**Response**:
```json
{
  "message": {
    "name": "12345",
    "title": "Follow up with client",
    "status": "Done",
    "priority": "High",
    "start_date": "2025-12-03",
    "due_date": null,
    "assigned_to": "sales@example.com",
    "modified": "2025-12-03 12:00:00"
  }
}
```

**Errors**:
- `417` - Task name or status is required
- `404` - Task not found
- `403` - Not permitted to update this task

---

### 5. Filter Tasks

**Endpoint**: `/filter_tasks`

**Method**: `GET`

**Description**: Filter and search CRM Tasks with pagination and sorting

**Parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `date_from` | Date | No | - | Filter tasks from this start date (YYYY-MM-DD) |
| `date_to` | Date | No | - | Filter tasks up to this start date (YYYY-MM-DD) |
| `importance` | String | No | - | Comma-separated priorities: "Low,Medium,High" |
| `status` | String | No | - | Comma-separated statuses: "Todo,In Progress" |
| `limit` | Integer | No | 50 | Maximum number of results |
| `offset` | Integer | No | 0 | Number of results to skip (for pagination) |
| `order_by` | String | No | "modified desc" | Sort order (e.g., "priority desc", "start_date asc") |

**Request Example**:
```
GET /api/method/crm.api.mobile_api.filter_tasks?date_from=2025-12-01&date_to=2025-12-31&importance=High,Medium&status=Todo,In Progress&limit=20&offset=0&order_by=priority desc
```

**Response**:
```json
{
  "message": {
    "data": [
      {
        "name": "12345",
        "title": "Follow up with client",
        "status": "Todo",
        "priority": "High",
        "start_date": "2025-12-03",
        "due_date": "2025-12-03 15:00:00",
        "assigned_to": "sales@example.com",
        "modified": "2025-12-03 10:30:45"
      },
      {
        "name": "12346",
        "title": "Review proposal",
        "status": "In Progress",
        "priority": "Medium",
        "start_date": "2025-12-03",
        "due_date": null,
        "assigned_to": "manager@example.com",
        "modified": "2025-12-03 09:15:30"
      }
    ]
  }
}
```

**Pagination Example**:
- Page 1: `offset=0&limit=20`
- Page 2: `offset=20&limit=20`
- Page 3: `offset=40&limit=20`

**Sorting Options**:
- `modified desc` - Last modified first (default)
- `modified asc` - Oldest modified first
- `start_date desc` - Latest start date first
- `start_date asc` - Earliest start date first
- `priority desc` - High priority first
- `priority asc` - Low priority first

---

### 6. Home Tasks

**Endpoint**: `/home_tasks`

**Method**: `GET`

**Description**: Get today's top N tasks for home screen display

**Parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `limit` | Integer | No | 5 | Maximum number of tasks to return |

**Request Example**:
```
GET /api/method/crm.api.mobile_api.home_tasks?limit=5
```

**Response**:
```json
{
  "message": {
    "today": [
      {
        "name": "12345",
        "title": "Follow up with client",
        "status": "Todo",
        "priority": "High",
        "start_date": "2025-12-03",
        "due_date": "2025-12-03 15:00:00",
        "assigned_to": "sales@example.com",
        "modified": "2025-12-03 10:30:45"
      },
      {
        "name": "12346",
        "title": "Team meeting",
        "status": "Todo",
        "priority": "Medium",
        "start_date": "2025-12-03",
        "due_date": "2025-12-03 14:00:00",
        "assigned_to": "sales@example.com",
        "modified": "2025-12-03 09:00:00"
      }
    ],
    "limit": 5
  }
}
```

**Logic**:
- Returns only tasks where `start_date` equals today's date
- Sorted by priority (High → Low) then by modified date (newest first)
- Limited to specified number of tasks

---

### 7. Main Page Buckets

**Endpoint**: `/main_page_buckets`

**Method**: `GET`

**Description**: Get tasks organized into three buckets: today, late, and upcoming

**Parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `min_each` | Integer | No | 5 | Minimum number of tasks to return per bucket |

**Request Example**:
```
GET /api/method/crm.api.mobile_api.main_page_buckets?min_each=5
```

**Response**:
```json
{
  "message": {
    "today": [
      {
        "name": "12345",
        "title": "Follow up with client",
        "status": "Todo",
        "priority": "High",
        "start_date": "2025-12-03",
        "due_date": "2025-12-03 15:00:00",
        "assigned_to": "sales@example.com",
        "modified": "2025-12-03 10:30:45"
      }
    ],
    "late": [
      {
        "name": "12340",
        "title": "Overdue task",
        "status": "In Progress",
        "priority": "High",
        "start_date": "2025-12-01",
        "due_date": "2025-12-01 17:00:00",
        "assigned_to": "sales@example.com",
        "modified": "2025-12-01 14:00:00"
      }
    ],
    "upcoming": [
      {
        "name": "12350",
        "title": "Future meeting",
        "status": "Backlog",
        "priority": "Medium",
        "start_date": "2025-12-05",
        "due_date": "2025-12-05 10:00:00",
        "assigned_to": "manager@example.com",
        "modified": "2025-12-03 08:00:00"
      }
    ],
    "min_each": 5
  }
}
```

**Bucket Logic**:

1. **Today Bucket**:
   - Tasks where `start_date` = today
   - All statuses included
   - Sorted by priority desc, then modified desc

2. **Late Bucket**:
   - Tasks where `start_date` < today
   - Only active statuses: "Backlog", "Todo", "In Progress"
   - Excludes "Done" and "Canceled" tasks
   - Sorted by start_date asc (oldest first), then priority desc

3. **Upcoming Bucket**:
   - Tasks where `start_date` > today
   - All statuses included
   - Sorted by start_date asc (soonest first), then priority desc

**Note**: Each bucket will contain **at least** `min_each` tasks when available. If fewer tasks exist for a bucket, it returns what's available.

---

## Common Response Format

All successful responses follow Frappe's standard format:

```json
{
  "message": {
    // actual response data here
  }
}
```

For errors:
```json
{
  "exc_type": "ValidationError",
  "exception": "Task Type is required",
  "_server_messages": "[...]"
}
```

---

## HTTP Status Codes

| Code | Description |
|------|-------------|
| `200` | Success |
| `401` | Unauthorized - not logged in or session expired |
| `403` | Forbidden - insufficient permissions |
| `404` | Not Found - task doesn't exist |
| `417` | Validation Error - missing required fields or invalid data |
| `500` | Internal Server Error |

---

## Field Name Reference

**Important**: The actual field name in CRM Task doctype is `start_date`, not `exp_start_date`.

All date/time buckets and filters use `start_date` as the scheduling date field.

**Complete Field List**:
- `name` - Auto-generated task ID
- `task_type` - Meeting/Property Showing/Call (required)
- `title` - Task title
- `status` - Backlog/Todo/In Progress/Done/Canceled
- `priority` - Low/Medium/High
- `start_date` - Task start date (DATE field)
- `due_date` - Task due date and time (DATETIME field)
- `assigned_to` - User email
- `description` - Task description (text/HTML)
- `modified` - Last modified timestamp (auto-managed)

---

## Testing Examples

### Using cURL

**Create Task**:
```bash
curl -X POST 'https://your-site.com/api/method/crm.api.mobile_api.create_task' \
  -H 'Content-Type: application/json' \
  -H 'Cookie: sid=YOUR_SESSION_ID; user_id=YOUR_USER_ID' \
  -d '{
    "task_type": "Call",
    "title": "Test Task",
    "priority": "High"
  }'
```

**Get Home Tasks**:
```bash
curl -X GET 'https://your-site.com/api/method/crm.api.mobile_api.home_tasks?limit=5' \
  -H 'Cookie: sid=YOUR_SESSION_ID; user_id=YOUR_USER_ID'
```

**Filter Tasks**:
```bash
curl -X GET 'https://your-site.com/api/method/crm.api.mobile_api.filter_tasks?importance=High&status=Todo&limit=10' \
  -H 'Cookie: sid=YOUR_SESSION_ID; user_id=YOUR_USER_ID'
```

### Using JavaScript/Fetch

```javascript
// Login first
const loginResponse = await fetch('https://your-site.com/api/method/login', {
  method: 'POST',
  headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  body: 'usr=user@example.com&pwd=password',
  credentials: 'include' // Important: include cookies
});

// Create task
const response = await fetch('https://your-site.com/api/method/crm.api.mobile_api.create_task', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  credentials: 'include', // Important: send cookies
  body: JSON.stringify({
    task_type: 'Call',
    title: 'Test Task',
    priority: 'High'
  })
});

const data = await response.json();
console.log(data.message);
```

---

## Integration Checklist

- [ ] Implement login flow to obtain session cookies
- [ ] Store session cookies securely
- [ ] Include cookies in all API requests
- [ ] Handle 401 errors (redirect to login)
- [ ] Handle 403 errors (show permission error)
- [ ] Handle 404 errors (task not found)
- [ ] Handle validation errors (show user-friendly messages)
- [ ] Implement pagination for filter_tasks endpoint
- [ ] Display tasks from main_page_buckets in UI
- [ ] Implement quick status update using update_status
- [ ] Test all CRUD operations
- [ ] Test filtering with various combinations
- [ ] Test edge cases (no tasks, empty buckets, etc.)

---

## Notes for Flutter Developers

1. **Cookie Management**: Use a package like `dio_cookie_manager` or `cookie_jar` to automatically handle cookies

2. **Date Formatting**: Ensure dates are formatted as `YYYY-MM-DD` and datetimes as `YYYY-MM-DD HH:MM:SS`

3. **Error Handling**: Parse the `exc_type` and `exception` fields from error responses

4. **Pagination**: Implement infinite scroll using `offset` and `limit` parameters

5. **Refresh Logic**: Call `home_tasks` and `main_page_buckets` when user pulls to refresh

6. **Offline Support**: Consider caching task data locally and syncing when online

7. **Session Expiry**: Implement automatic session refresh or re-login on 401 errors

---

## Additional Resources

- [MOBILE_API_README.md](./MOBILE_API_README.md) - Installation and authentication guide
- [QA_CHECKLIST.md](./QA_CHECKLIST.md) - Testing checklist
- [FLUTTER_HANDOVER.md](./FLUTTER_HANDOVER.md) - Flutter integration guide
- [POSTMAN_COLLECTION.json](./POSTMAN_COLLECTION.json) - Postman collection for testing

