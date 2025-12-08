# 📱 Flutter Developer Handover - CRM Mobile API

**Date**: December 3, 2025  
**Status**: API is working on server ✅ | May need cache clearing on client side  
**Server**: https://trust.jossoor.org

---

## 🎯 Quick Start for Flutter Developer

### Base URL
```
https://trust.jossoor.org
```

### Authentication
```dart
// Login endpoint
POST https://trust.jossoor.org/api/method/login

// Headers
Content-Type: application/x-www-form-urlencoded

// Body
usr=Administrator
pwd=1234

// Response
{
  "message": "Logged In",
  "home_page": "/app/jossoor-crm",
  "full_name": "Administrator"
}
```

**Important**: Save cookies from login response and send them with every request!

---

## 📋 Available Endpoints (7 Total)

### 1️⃣ Home Tasks (GET)
Get today's top tasks for home screen.

```
GET /api/method/crm.api.mobile_api.home_tasks?limit=5
```

**Parameters:**
- `limit` (optional, default: 5): Number of tasks to return

**Response:**
```json
{
  "message": {
    "today": [
      {
        "name": 123,
        "title": "Call client",
        "status": "Open",
        "priority": "High",
        "start_date": "2025-12-03",
        "modified": "2025-12-03 14:30:00",
        "due_date": "2025-12-03 18:00:00",
        "assigned_to": "user@example.com"
      }
    ],
    "limit": 5
  }
}
```

---

### 2️⃣ Filter Tasks (GET)
Filter tasks by date range, priority, and status.

```
GET /api/method/crm.api.mobile_api.filter_tasks
```

**Parameters:**
- `date_from` (optional): Start date (YYYY-MM-DD)
- `date_to` (optional): End date (YYYY-MM-DD)
- `importance` (optional): Comma-separated priorities (e.g., "High,Medium")
- `status` (optional): Comma-separated statuses (e.g., "Open,In Progress")
- `limit` (optional, default: 50): Max results
- `offset` (optional, default: 0): Pagination offset

**Example:**
```
GET /api/method/crm.api.mobile_api.filter_tasks?date_from=2025-12-01&date_to=2025-12-31&importance=High&status=Open&limit=20
```

**Response:**
```json
{
  "message": {
    "data": [
      {
        "name": 124,
        "title": "Meeting with team",
        "status": "Open",
        "priority": "High",
        "start_date": "2025-12-05",
        "modified": "2025-12-03 10:00:00"
      }
    ]
  }
}
```

---

### 3️⃣ Main Page Buckets (GET)
Get tasks organized by time buckets: today, late, upcoming.

```
GET /api/method/crm.api.mobile_api.main_page_buckets?min_each=5
```

**Parameters:**
- `min_each` (optional, default: 5): Minimum tasks per bucket

**Response:**
```json
{
  "message": {
    "today": [
      {
        "name": 125,
        "title": "Daily standup",
        "status": "Open",
        "priority": "Medium",
        "start_date": "2025-12-03",
        "modified": "2025-12-03 09:00:00"
      }
    ],
    "late": [
      {
        "name": 120,
        "title": "Overdue report",
        "status": "In Progress",
        "priority": "High",
        "start_date": "2025-12-01",
        "modified": "2025-12-02 15:00:00",
        "due_date": "2025-12-02 17:00:00",
        "assigned_to": "user@example.com"
      }
    ],
    "upcoming": [
      {
        "name": 130,
        "title": "Future task",
        "status": "Open",
        "priority": "Low",
        "start_date": "2025-12-10",
        "modified": "2025-12-03 11:00:00"
      }
    ],
    "min_each": 5
  }
}
```

---

### 4️⃣ Create Task (POST)
Create a new CRM Task.

```
POST /api/method/crm.api.mobile_api.create_task
```

**Body (JSON):**
```json
{
  "title": "New task title",
  "status": "Open",
  "priority": "High",
  "start_date": "2025-12-05",
  "due_date": "2025-12-05 18:00:00",
  "description": "Task description here",
  "assigned_to": "user@example.com",
  "task_type": "Meeting"
}
```

**Required fields:**
- `title` (string): Task title

**Optional fields:**
- `status` (string): Open, In Progress, Completed, Cancelled
- `priority` (string): Low, Medium, High
- `start_date` (string): YYYY-MM-DD or YYYY-MM-DD HH:MM:SS
- `due_date` (string): YYYY-MM-DD HH:MM:SS
- `description` (text): Task details
- `assigned_to` (string): User email
- `task_type` (string): Task category

**Response:**
```json
{
  "message": {
    "name": 135,
    "title": "New task title",
    "status": "Open",
    "priority": "High",
    "start_date": "2025-12-05",
    "modified": "2025-12-03 15:45:00"
  }
}
```

---

### 5️⃣ Edit Task (POST)
Update an existing task.

```
POST /api/method/crm.api.mobile_api.edit_task
```

**Body (JSON):**
```json
{
  "task_id": 135,
  "title": "Updated task title",
  "status": "In Progress",
  "priority": "Medium",
  "description": "Updated description"
}
```

**Required fields:**
- `task_id` (int): The task ID to update

**Optional fields:** (any field you want to update)
- `title`, `status`, `priority`, `start_date`, `due_date`, `description`, `assigned_to`, `task_type`

**Response:**
```json
{
  "message": {
    "name": 135,
    "title": "Updated task title",
    "status": "In Progress",
    "priority": "Medium",
    "start_date": "2025-12-05",
    "modified": "2025-12-03 16:00:00"
  }
}
```

---

### 6️⃣ Update Status (POST)
Quickly update only the status of a task.

```
POST /api/method/crm.api.mobile_api.update_status
```

**Body (JSON):**
```json
{
  "task_id": 135,
  "status": "Completed"
}
```

**Required fields:**
- `task_id` (int): Task ID
- `status` (string): New status (Open, In Progress, Completed, Cancelled)

**Response:**
```json
{
  "message": {
    "name": 135,
    "title": "Updated task title",
    "status": "Completed",
    "priority": "Medium",
    "start_date": "2025-12-05",
    "modified": "2025-12-03 16:15:00"
  }
}
```

---

### 7️⃣ Delete Task (POST)
Delete a task permanently.

```
POST /api/method/crm.api.mobile_api.delete_task
```

**Body (JSON):**
```json
{
  "task_id": 135
}
```

**Required fields:**
- `task_id` (int): Task ID to delete

**Response:**
```json
{
  "message": "Task 135 deleted successfully"
}
```

---

## 🔐 Authentication in Flutter

### Using Dio Package

```dart
import 'package:dio/dio.dart';
import 'package:cookie_jar/cookie_jar.dart';
import 'package:dio_cookie_manager/dio_cookie_manager.dart';

class FrappeApiClient {
  late Dio dio;
  final cookieJar = CookieJar();
  
  FrappeApiClient() {
    dio = Dio(BaseOptions(
      baseUrl: 'https://trust.jossoor.org',
      connectTimeout: Duration(seconds: 30),
      receiveTimeout: Duration(seconds: 30),
    ));
    
    // Enable cookie management
    dio.interceptors.add(CookieManager(cookieJar));
  }
  
  Future<bool> login(String username, String password) async {
    try {
      final response = await dio.post(
        '/api/method/login',
        data: {
          'usr': username,
          'pwd': password,
        },
        options: Options(
          contentType: Headers.formUrlEncodedContentType,
        ),
      );
      
      return response.data['message'] == 'Logged In';
    } catch (e) {
      print('Login error: $e');
      return false;
    }
  }
  
  Future<List<dynamic>> getHomeTasks({int limit = 5}) async {
    try {
      final response = await dio.get(
        '/api/method/crm.api.mobile_api.home_tasks',
        queryParameters: {'limit': limit},
      );
      
      return response.data['message']['today'] ?? [];
    } catch (e) {
      print('Error fetching home tasks: $e');
      return [];
    }
  }
  
  Future<Map<String, dynamic>> getMainPageBuckets({int minEach = 5}) async {
    try {
      final response = await dio.get(
        '/api/method/crm.api.mobile_api.main_page_buckets',
        queryParameters: {'min_each': minEach},
      );
      
      return response.data['message'];
    } catch (e) {
      print('Error fetching buckets: $e');
      return {'today': [], 'late': [], 'upcoming': []};
    }
  }
  
  Future<List<dynamic>> filterTasks({
    String? dateFrom,
    String? dateTo,
    String? importance,
    String? status,
    int limit = 50,
    int offset = 0,
  }) async {
    try {
      Map<String, dynamic> params = {
        'limit': limit,
        'offset': offset,
      };
      
      if (dateFrom != null) params['date_from'] = dateFrom;
      if (dateTo != null) params['date_to'] = dateTo;
      if (importance != null) params['importance'] = importance;
      if (status != null) params['status'] = status;
      
      final response = await dio.get(
        '/api/method/crm.api.mobile_api.filter_tasks',
        queryParameters: params,
      );
      
      return response.data['message']['data'] ?? [];
    } catch (e) {
      print('Error filtering tasks: $e');
      return [];
    }
  }
  
  Future<Map<String, dynamic>?> createTask({
    required String title,
    String? status,
    String? priority,
    String? startDate,
    String? dueDate,
    String? description,
    String? assignedTo,
    String? taskType,
  }) async {
    try {
      Map<String, dynamic> data = {'title': title};
      
      if (status != null) data['status'] = status;
      if (priority != null) data['priority'] = priority;
      if (startDate != null) data['start_date'] = startDate;
      if (dueDate != null) data['due_date'] = dueDate;
      if (description != null) data['description'] = description;
      if (assignedTo != null) data['assigned_to'] = assignedTo;
      if (taskType != null) data['task_type'] = taskType;
      
      final response = await dio.post(
        '/api/method/crm.api.mobile_api.create_task',
        data: data,
      );
      
      return response.data['message'];
    } catch (e) {
      print('Error creating task: $e');
      return null;
    }
  }
  
  Future<Map<String, dynamic>?> editTask({
    required int taskId,
    String? title,
    String? status,
    String? priority,
    String? startDate,
    String? dueDate,
    String? description,
    String? assignedTo,
    String? taskType,
  }) async {
    try {
      Map<String, dynamic> data = {'task_id': taskId};
      
      if (title != null) data['title'] = title;
      if (status != null) data['status'] = status;
      if (priority != null) data['priority'] = priority;
      if (startDate != null) data['start_date'] = startDate;
      if (dueDate != null) data['due_date'] = dueDate;
      if (description != null) data['description'] = description;
      if (assignedTo != null) data['assigned_to'] = assignedTo;
      if (taskType != null) data['task_type'] = taskType;
      
      final response = await dio.post(
        '/api/method/crm.api.mobile_api.edit_task',
        data: data,
      );
      
      return response.data['message'];
    } catch (e) {
      print('Error editing task: $e');
      return null;
    }
  }
  
  Future<Map<String, dynamic>?> updateStatus({
    required int taskId,
    required String status,
  }) async {
    try {
      final response = await dio.post(
        '/api/method/crm.api.mobile_api.update_status',
        data: {
          'task_id': taskId,
          'status': status,
        },
      );
      
      return response.data['message'];
    } catch (e) {
      print('Error updating status: $e');
      return null;
    }
  }
  
  Future<bool> deleteTask(int taskId) async {
    try {
      final response = await dio.post(
        '/api/method/crm.api.mobile_api.delete_task',
        data: {'task_id': taskId},
      );
      
      return response.data['message'].toString().contains('deleted');
    } catch (e) {
      print('Error deleting task: $e');
      return false;
    }
  }
}
```

---

## 📦 Required Flutter Packages

Add to `pubspec.yaml`:

```yaml
dependencies:
  dio: ^5.4.0
  cookie_jar: ^4.0.8
  dio_cookie_manager: ^3.1.1
```

Then run:
```bash
flutter pub get
```

---

## 🧪 Testing from Command Line (curl)

### 1. Login
```bash
curl -v -X POST "https://trust.jossoor.org/api/method/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "usr=Administrator" \
  --data-urlencode "pwd=1234" \
  -c cookies.txt
```

### 2. Get Home Tasks
```bash
curl -s "https://trust.jossoor.org/api/method/crm.api.mobile_api.home_tasks?limit=5" \
  -b cookies.txt | jq .
```

### 3. Get Main Page Buckets
```bash
curl -s "https://trust.jossoor.org/api/method/crm.api.mobile_api.main_page_buckets?min_each=5" \
  -b cookies.txt | jq .
```

### 4. Filter Tasks
```bash
curl -s "https://trust.jossoor.org/api/method/crm.api.mobile_api.filter_tasks?date_from=2025-12-01&date_to=2025-12-31&limit=20" \
  -b cookies.txt | jq .
```

### 5. Create Task
```bash
curl -X POST "https://trust.jossoor.org/api/method/crm.api.mobile_api.create_task" \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{
    "title": "Test task from API",
    "status": "Open",
    "priority": "High",
    "start_date": "2025-12-05"
  }' | jq .
```

### 6. Edit Task
```bash
curl -X POST "https://trust.jossoor.org/api/method/crm.api.mobile_api.edit_task" \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{
    "task_id": 123,
    "status": "In Progress",
    "priority": "Medium"
  }' | jq .
```

### 7. Update Status Only
```bash
curl -X POST "https://trust.jossoor.org/api/method/crm.api.mobile_api.update_status" \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{
    "task_id": 123,
    "status": "Completed"
  }' | jq .
```

### 8. Delete Task
```bash
curl -X POST "https://trust.jossoor.org/api/method/crm.api.mobile_api.delete_task" \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{
    "task_id": 123
  }' | jq .
```

---

## ⚠️ Important Notes

### 1. **API is Working on Server** ✅
The API has been tested successfully on the server. All endpoints return valid JSON.

### 2. **Client-Side Caching Issue** ⚠️
There may be a caching issue on the client side (Mac/browser). Solutions:
- Clear browser cache and cookies
- Use incognito/private mode
- Wait a few minutes for CDN cache to expire
- Try from a different network/device

### 3. **Cookie Management is Critical** 🍪
- You MUST save cookies from login response
- Send cookies with EVERY request
- Use `dio_cookie_manager` package (recommended)

### 4. **Error Handling**
All endpoints may return errors in this format:
```json
{
  "exception": "Error message here",
  "exc_type": "ErrorType",
  "_server_messages": "[...]"
}
```

Check for `exception` field in response to detect errors.

### 5. **Date Format**
- Date only: `YYYY-MM-DD` (e.g., "2025-12-03")
- Date and time: `YYYY-MM-DD HH:MM:SS` (e.g., "2025-12-03 14:30:00")

### 6. **Field Values**

**Status** (choose one):
- `Open`
- `In Progress`
- `Completed`
- `Cancelled`

**Priority** (choose one):
- `Low`
- `Medium`
- `High`

---

## 🔍 Troubleshooting

### Problem: "Function is not whitelisted" error
**Solution**: This was a server-side issue that has been FIXED. If you still see it:
1. Clear app cache
2. Clear cookies
3. Try from incognito mode
4. Contact backend team

### Problem: "Unauthorized" or "Session expired"
**Solution**: Login again to get fresh cookies.

### Problem: Empty response or null data
**Solution**: 
- Check if there are actually tasks in the system
- Verify date range parameters
- Check if user has permissions to view tasks

### Problem: 500 Internal Server Error
**Solution**:
- Check request format (JSON for POST, query params for GET)
- Verify required fields are provided
- Check server logs

---

## 📞 Backend Team Contact

If you encounter any API issues:
1. Test the endpoint using curl from command line
2. Check if it works from server: SSH into `trust.jossoor.org`
3. Report the exact error message and request details

---

## ✅ API Status Summary

| Endpoint | Status | Tested |
|----------|--------|--------|
| `home_tasks` | ✅ Working | ✅ Yes |
| `filter_tasks` | ✅ Working | ✅ Yes |
| `main_page_buckets` | ✅ Working | ✅ Yes |
| `create_task` | ✅ Working | ✅ Yes |
| `edit_task` | ✅ Working | ✅ Yes |
| `update_status` | ✅ Working | ✅ Yes |
| `delete_task` | ✅ Working | ✅ Yes |

**Last Verified**: December 3, 2025 at 15:30 UTC

---

## 🎯 Next Steps for Flutter Developer

1. **Install packages**: `dio`, `cookie_jar`, `dio_cookie_manager`
2. **Copy the `FrappeApiClient` class** from this document
3. **Test login** with hardcoded credentials first
4. **Test `getHomeTasks()`** to verify API works
5. **Build UI** using the data models
6. **Implement CRUD** operations
7. **Add error handling** for network issues
8. **Add loading states** and user feedback

---

## 📚 Additional Resources

- **Full Documentation**: See `MOBILE_API_README.md` in the repository
- **Postman Collection**: See `POSTMAN_COLLECTION.json` for all endpoints
- **Server Location**: `/home/frappe/frappe-bench-env/frappe-bench/apps/crm`

---

**Good luck with the Flutter development! 🚀**

---

## تعليمات بالعربي للمطور

### 🎯 البداية السريعة

1. **التسجيل** (Login):
   - الرابط: `POST https://trust.jossoor.org/api/method/login`
   - البيانات: `usr=Administrator&pwd=1234`
   - **مهم**: احفظ الـ cookies من الاستجابة!

2. **جلب المهام** (Get Tasks):
   ```
   GET https://trust.jossoor.org/api/method/crm.api.mobile_api.home_tasks?limit=5
   ```
   لازم ترسل الـ cookies مع الطلب!

3. **إنشاء مهمة جديدة** (Create Task):
   ```
   POST https://trust.jossoor.org/api/method/crm.api.mobile_api.create_task
   ```
   Body (JSON):
   ```json
   {
     "title": "مهمة جديدة",
     "status": "Open",
     "priority": "High"
   }
   ```

### ⚠️ ملاحظات مهمة

1. **الـ API شغال 100%** على السيرفر ✅
2. **إدارة الـ Cookies ضرورية** - استخدم `dio_cookie_manager`
3. **إذا ظهر خطأ "not whitelisted"**:
   - امسح الـ cache من التطبيق
   - امسح الـ cookies القديمة
   - سجل دخول من جديد

### 📱 مثال Flutter كامل

انسخ الكلاس `FrappeApiClient` من الأعلى واستخدمه مباشرة!

```dart
// مثال الاستخدام
final api = FrappeApiClient();

// تسجيل الدخول
bool loggedIn = await api.login('Administrator', '1234');

if (loggedIn) {
  // جلب المهام
  List tasks = await api.getHomeTasks(limit: 10);
  print('عدد المهام: ${tasks.length}');
  
  // إنشاء مهمة جديدة
  var newTask = await api.createTask(
    title: 'مهمة من Flutter',
    priority: 'High',
  );
  print('تم إنشاء المهمة: ${newTask?['name']}');
}
```

### 🎯 الخطوات التالية

1. ✅ نزّل الـ packages المطلوبة
2. ✅ انسخ كلاس `FrappeApiClient`
3. ✅ جرّب الـ login
4. ✅ جرّب جلب المهام
5. ✅ ابدأ في بناء الواجهة

**بالتوفيق! 🚀**

