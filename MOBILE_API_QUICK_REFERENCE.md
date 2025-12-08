# CRM Task Mobile API - Quick Reference Card

## 🔗 Base URL
```
https://your-site.com/api/method/crm.api.mobile_api
```

## 🔐 Authentication

**Login:**
```bash
POST https://your-site.com/api/method/login
Body: usr=user@example.com&pwd=password
```
**Returns session cookies:** `sid`, `user_id`, `full_name`  
**Include cookies in all subsequent requests**

## 📍 Endpoints

### 1. Create Task
```
POST /create_task
Required: task_type (Meeting/Property Showing/Call)
Optional: title, status, priority, start_date, description, assigned_to, due_date
Defaults: status=Todo, priority=Medium, start_date=today
```

### 2. Edit Task
```
POST /edit_task
Required: name
Optional: title, status, priority, start_date, task_type, description, assigned_to, due_date
```

### 3. Delete Task
```
POST /delete_task
Required: name
Returns: {"ok": true}
```

### 4. Update Status
```
POST /update_status
Required: name, status
Status: Backlog|Todo|In Progress|Done|Canceled
```

### 5. Filter Tasks
```
GET /filter_tasks
Optional: date_from, date_to, importance, status, limit, offset, order_by
Defaults: limit=50, offset=0, order_by="modified desc"
Returns: {"data": [...]}
```

### 6. Home Tasks
```
GET /home_tasks
Optional: limit
Default: limit=5
Returns: {"today": [...], "limit": N}
Logic: start_date = today, sorted by priority desc
```

### 7. Main Page Buckets
```
GET /main_page_buckets
Optional: min_each
Default: min_each=5
Returns: {"today": [...], "late": [...], "upcoming": [...], "min_each": N}
Logic:
  - today: start_date = today
  - late: start_date < today AND status in [Backlog, Todo, In Progress]
  - upcoming: start_date > today
```

## 📋 Task Fields (Response)

```json
{
  "name": "12345",
  "title": "Task title",
  "status": "Todo",
  "priority": "High",
  "start_date": "2025-12-03",
  "due_date": "2025-12-03 15:00:00",
  "assigned_to": "user@example.com",
  "modified": "2025-12-03 10:30:45"
}
```

## 🎨 Status Values
- `Backlog`
- `Todo`
- `In Progress`
- `Done`
- `Canceled`

## 🚦 Priority Values
- `Low`
- `Medium`
- `High`

## 📅 Task Types
- `Meeting`
- `Property Showing`
- `Call`

## 📊 Filter Examples

**By date range:**
```
?date_from=2025-12-01&date_to=2025-12-31
```

**By priority:**
```
?importance=High,Medium
```

**By status:**
```
?status=Todo,In Progress
```

**Combined with pagination:**
```
?date_from=2025-12-01&importance=High&status=Todo&limit=20&offset=0&order_by=priority desc
```

## 🔑 Required Roles
- **Sales User** - Full CRUD access
- **Sales Manager** - Full CRUD access

## ⚠️ HTTP Status Codes

| Code | Meaning |
|------|---------|
| 200 | Success |
| 401 | Not logged in / session expired |
| 403 | Permission denied |
| 404 | Task not found |
| 417 | Validation error (missing required fields) |

## 🛠️ Quick cURL Examples

**Login & save cookies:**
```bash
curl -X POST https://your-site.com/api/method/login \
  -d "usr=user@example.com&pwd=password" \
  -c cookies.txt
```

**Create task:**
```bash
curl -X POST https://your-site.com/api/method/crm.api.mobile_api.create_task \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{"task_type":"Call","title":"Follow up","priority":"High"}'
```

**Get today's tasks:**
```bash
curl https://your-site.com/api/method/crm.api.mobile_api.home_tasks?limit=5 \
  -b cookies.txt
```

**Get buckets:**
```bash
curl https://your-site.com/api/method/crm.api.mobile_api.main_page_buckets?min_each=5 \
  -b cookies.txt
```

**Filter high priority tasks:**
```bash
curl "https://your-site.com/api/method/crm.api.mobile_api.filter_tasks?importance=High&status=Todo,In Progress" \
  -b cookies.txt
```

**Update status:**
```bash
curl -X POST https://your-site.com/api/method/crm.api.mobile_api.update_status \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{"name":"12345","status":"Done"}'
```

**Delete task:**
```bash
curl -X POST https://your-site.com/api/method/crm.api.mobile_api.delete_task \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{"name":"12345"}'
```

## 📐 Date Formats
- **start_date**: `YYYY-MM-DD` (e.g., "2025-12-03")
- **due_date**: `YYYY-MM-DD HH:MM:SS` (e.g., "2025-12-03 15:30:00")

## 🎯 Important Notes

- ✅ Field name is `start_date` (not `exp_start_date`)
- ✅ Session cookies required for all endpoints
- ✅ Standard Frappe permissions enforced (no bypass)
- ✅ All responses wrapped in `{"message": {...}}`
- ❌ No notifications or FCM
- ❌ No CRM Lead references
- ❌ No custom authentication

## 📚 Documentation Links

- **Full README**: `MOBILE_API_README.md`
- **Endpoint Details**: `API_ENDPOINTS.md`
- **Flutter Guide**: `FLUTTER_HANDOVER.md`
- **QA Checklist**: `QA_CHECKLIST.md`
- **Postman Collection**: `POSTMAN_COLLECTION.json`

## 🚀 Getting Started

1. Import `POSTMAN_COLLECTION.json` into Postman
2. Set `base_url` variable to your site URL
3. Run "Login" request to get session cookies
4. Test other endpoints

## 💡 Tips

- Test with Postman first before Flutter integration
- Session expires after configured timeout - handle 401 errors
- Use pagination for large result sets (limit + offset)
- All endpoints respect user permissions automatically
- Cookies are automatically managed by HTTP clients (Dio, etc.)

---

**Version**: 1.0  
**Last Updated**: 2025-12-03

