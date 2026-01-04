# Flutter API Fields Reference - CRM Mobile API

دليل شامل لجميع الحقول المتاحة في APIs للمطورين Flutter.

---

## 📋 جدول المحتويات

1. [CRM Task Fields](#crm-task-fields)
2. [create_task API](#create_task-api)
3. [edit_task API](#edit_task-api)
4. [User Object Fields](#user-object-fields)
5. [Response Fields](#response-fields)
6. [Reference Doc APIs](#reference-doc-apis)
7. [get_current_user_role API](#get_current_user_role-api)

---

## 🎯 CRM Task Fields

جميع الحقول المتاحة في CRM Task:

### الحقول الأساسية (Required/Optional)

| Field Name | Type | Required | Description | Options/Values |
|------------|------|----------|-------------|----------------|
| `task_type` | String | **Yes** | نوع المهمة | `"Meeting"`, `"Property Showing"`, `"Call"` |
| `title` | String | No | عنوان المهمة | أي نص |
| `status` | String | No | حالة المهمة | `"Backlog"`, `"Todo"`, `"In Progress"`, `"Done"`, `"Canceled"` |
| `priority` | String | No | الأولوية | `"Low"`, `"Medium"`, `"High"` |
| `start_date` | DateTime | No | تاريخ البدء | `"YYYY-MM-DD HH:MM:SS"` أو `"YYYY-MM-DD"` |
| `due_date` | DateTime | No | تاريخ الاستحقاق | `"YYYY-MM-DD HH:MM:SS"` |
| `description` | String | No | الوصف | أي نص (يدعم HTML) |

### حقول المرجع (Reference Fields)

| Field Name | Type | Required | Description | Example |
|------------|------|----------|-------------|---------|
| `reference_doctype` | String | No | نوع المستند المرجعي | `"CRM Lead"`, `"Real Estate Project"`, `"Unit"`, `"Project Unit"` |
| `reference_docname` | String | No | اسم المستند المرجعي | `"CRM-LEAD-2025-001"`, `"Project Name"` |

### حقول التعيين (Assignment Fields)

| Field Name | Type | Required | Description | Format |
|------------|------|----------|-------------|--------|
| `assigned_to` | String | No | مستخدم واحد (legacy) | `"user@example.com"` |
| `assigned_to_list` | List | No | قائمة المستخدمين | `["user1@example.com", "user2@example.com"]` أو `[{...user objects...}]` |
| `meeting_attendees` | List | No | قائمة الحضور | `[{...user objects...}]` |

---

## 📝 create_task API

### Endpoint
```
POST /api/method/crm.api.mobile_api.create_task
```

### Request Body Fields

```dart
{
  // Required Fields
  "task_type": String,  // "Meeting" | "Property Showing" | "Call"
  
  // Optional Basic Fields
  "title": String?,
  "status": String?,     // Default: "Todo"
  "priority": String?,   // Default: "Medium"
  "start_date": String?, // Default: today()
  "due_date": String?,
  "description": String?,
  
  // Reference Fields
  "reference_doctype": String?,
  "reference_docname": String?,
  
  // Assignment Fields
  "assigned_to": String?,                    // Single user email (legacy)
  "assigned_to_list": List<dynamic>?,       // List of users (new)
  "meeting_attendees": List<dynamic>?,     // List of attendees
  
  // Any other CRM Task fields
  // ... (any field from CRM Task doctype)
}
```

### Example Request (Dart/Flutter)

```dart
final requestBody = {
  "task_type": "Meeting",
  "title": "Team Meeting",
  "status": "Todo",
  "priority": "High",
  "start_date": "2025-12-10 10:00:00",
  "due_date": "2025-12-10 15:00:00",
  "description": "Discuss project progress",
  "reference_doctype": "CRM Lead",
  "reference_docname": "CRM-LEAD-2025-001",
  "assigned_to_list": [
    {
      "email": "user1@example.com",
      "name": "User One",
      "profile_pic": "https://example.com/pic.jpg",
      "id": "user1@example.com"
    },
    {
      "email": "user2@example.com",
      "name": "User Two",
      "profile_pic": null,
      "id": "user2@example.com"
    }
  ],
  "meeting_attendees": [
    {
      "email": "attendee1@example.com",
      "name": "Attendee One",
      "profile_pic": "https://example.com/pic2.jpg",
      "id": "attendee1@example.com"
    }
  ]
};

final response = await http.post(
  Uri.parse('https://your-site.com/api/method/crm.api.mobile_api.create_task'),
  headers: {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer $accessToken',
  },
  body: jsonEncode(requestBody),
);
```

---

## ✏️ edit_task API

### Endpoint
```
POST /api/method/crm.api.mobile_api.edit_task
```

### Request Body Fields

```dart
{
  // Required Fields
  "name": int,  // Task ID (required)
  
  // All fields from create_task are optional here
  "task_type": String?,
  "title": String?,
  "status": String?,
  "priority": String?,
  "start_date": String?,
  "due_date": String?,
  "description": String?,
  "reference_doctype": String?,
  "reference_docname": String?,
  "assigned_to": String?,
  "assigned_to_list": List<dynamic>?,
  "meeting_attendees": List<dynamic>?,
  
  // Any other CRM Task fields
  // ... (any field from CRM Task doctype)
}
```

### Example Request (Dart/Flutter)

```dart
final requestBody = {
  "name": 123,  // Task ID
  "status": "In Progress",
  "priority": "High",
  "assigned_to_list": [
    {
      "email": "newuser@example.com",
      "name": "New User",
      "profile_pic": null,
      "id": "newuser@example.com"
    }
  ]
};

final response = await http.post(
  Uri.parse('https://your-site.com/api/method/crm.api.mobile_api.edit_task'),
  headers: {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer $accessToken',
  },
  body: jsonEncode(requestBody),
);
```

---

## 👤 User Object Fields

عند إرسال `assigned_to_list` أو `meeting_attendees`، يمكنك استخدام User Objects:

### User Object Structure

```dart
{
  "email": String,      // Required: User email
  "name": String?,      // Optional: User full name
  "profile_pic": String?, // Optional: Profile picture URL or file path
  "id": String?        // Optional: User ID (can be same as email)
}
```

### Supported Formats

#### Format 1: List of Email Strings
```dart
"assigned_to_list": [
  "user1@example.com",
  "user2@example.com"
]
```

#### Format 2: List of User Objects
```dart
"assigned_to_list": [
  {
    "email": "user1@example.com",
    "name": "User One",
    "profile_pic": "https://example.com/pic.jpg",
    "id": "user1@example.com"
  },
  {
    "email": "user2@example.com",
    "name": "User Two",
    "id": "user2@example.com"
  }
]
```

#### Format 3: JSON String (if sending as form data)
```dart
"assigned_to_list": "[{\"email\":\"user@example.com\",\"name\":\"User\"}]"
```

---

## 📤 Response Fields

### create_task / edit_task Response

```dart
{
  "message": {
    // Basic Fields
    "name": int,                    // Task ID
    "title": String?,
    "status": String,
    "priority": String,
    "start_date": String?,
    "due_date": String?,
    "description": String?,
    "task_type": String,
    
    // Reference Fields
    "reference_doctype": String?,
    "reference_docname": String?,
    
    // Assignment Fields
    "assigned_to": List<UserObject>,  // List of assigned users
    
    // System Fields
    "modified": String,              // Last modified timestamp
    "creation": String?,             // Creation timestamp
    "owner": String?,                // Creator user
    "modified_by": String?,          // Last modifier user
    
    // All other CRM Task fields
    // ... (all fields from CRM Task doctype)
  }
}
```

### User Object in Response

```dart
{
  "email": String,      // User email
  "name": String,       // User full name
  "profile_pic": String? // Profile picture URL or null
}
```

---

## 🔍 Reference Doc APIs

### get_crm_leads

**Endpoint:** `GET /api/method/crm.api.mobile_api.get_crm_leads`

**Query Parameters:**
- `limit`: int (default: 100, max: 500)
- `search_term`: String?

**Response:**
```dart
{
  "message": [
    {
      "name": String,        // Lead ID (e.g., "CRM-LEAD-2025-001")
      "label": String,       // Display name
      "email": String?,
      "mobile_no": String?
    }
  ]
}
```

---

### get_real_estate_projects

**Endpoint:** `GET /api/method/crm.api.mobile_api.get_real_estate_projects`

**Query Parameters:**
- `limit`: int (default: 100, max: 500)
- `search_term`: String?

**Response:**
```dart
{
  "message": [
    {
      "name": String,        // Project name
      "label": String,       // Display name with location
      "project_name": String,
      "location": String?,
      "developer": String?
    }
  ]
}
```

---

### get_units

**Endpoint:** `GET /api/method/crm.api.mobile_api.get_units`

**Query Parameters:**
- `limit`: int (default: 100, max: 500)
- `search_term`: String?

**Response:**
```dart
{
  "message": [
    {
      "name": String,        // Unit name
      "label": String,       // Display name with type and city
      "unit_name": String,
      "type": String?,
      "city": String?,
      "price": double?
    }
  ]
}
```

---

### get_project_units

**Endpoint:** `GET /api/method/crm.api.mobile_api.get_project_units`

**Query Parameters:**
- `limit`: int (default: 100, max: 500)
- `search_term`: String?
- `project`: String? (filter by project name)

**Response:**
```dart
{
  "message": [
    {
      "name": String,        // Unit name
      "label": String,      // Display name with project and type
      "unit_name": String,
      "project": String,
      "type": String?,
      "price": double?
    }
  ]
}
```

---

## 👤 get_current_user_role API

### Endpoint
```
GET /api/method/crm.api.mobile_api.get_current_user_role
```

### Description
يرجع role المستخدم الحالي بناءً على الأولوية.

### Response

```dart
{
  "message": {
    "role": String,           // Primary role name
    "roles": List<String>,    // All user roles
    "user": String,           // User email
    "full_name": String?      // User full name
  }
}
```

### Role Priority Order

1. **System Manager** (أعلى أولوية)
2. **Sales Master Manager**
3. **Sales Manager**
4. **Sales User**
5. **Guest** (أقل أولوية)

### Example Request (Dart/Flutter)

```dart
final response = await http.get(
  Uri.parse('https://your-site.com/api/method/crm.api.mobile_api.get_current_user_role'),
  headers: {
    'Authorization': 'Bearer $accessToken',
  },
);

final data = jsonDecode(response.body)['message'];
final userRole = data['role'];  // "System Manager", "Sales Manager", etc.
final allRoles = data['roles'];  // List of all roles
final userName = data['user'];
final fullName = data['full_name'];

// Check role:
if (userRole == 'System Manager') {
  // Show admin features
} else if (userRole == 'Sales Manager' || userRole == 'Sales Master Manager') {
  // Show manager features
} else if (userRole == 'Sales User') {
  // Show user features
}
```

### Example Responses

#### System Manager
```json
{
  "message": {
    "role": "System Manager",
    "roles": ["System Manager", "Sales Manager", "Sales User"],
    "user": "admin@example.com",
    "full_name": "Administrator"
  }
}
```

#### Sales Master Manager
```json
{
  "message": {
    "role": "Sales Master Manager",
    "roles": ["Sales Master Manager", "Sales Manager", "Sales User"],
    "user": "master@example.com",
    "full_name": "Master Manager"
  }
}
```

#### Sales Manager
```json
{
  "message": {
    "role": "Sales Manager",
    "roles": ["Sales Manager", "Sales User"],
    "user": "manager@example.com",
    "full_name": "John Manager"
  }
}
```

#### Sales User
```json
{
  "message": {
    "role": "Sales User",
    "roles": ["Sales User"],
    "user": "user@example.com",
    "full_name": "Jane User"
  }
}
```

---

## 📋 Complete Field List Summary

### CRM Task Fields (All Available)

```dart
// Required
task_type: String  // "Meeting" | "Property Showing" | "Call"

// Optional Basic Fields
title: String?
status: String?           // "Backlog" | "Todo" | "In Progress" | "Done" | "Canceled"
priority: String?          // "Low" | "Medium" | "High"
start_date: DateTime?      // "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD"
due_date: DateTime?        // "YYYY-MM-DD HH:MM:SS"
description: String?        // Text (HTML supported)

// Reference Fields
reference_doctype: String?  // "CRM Lead" | "Real Estate Project" | "Unit" | "Project Unit"
reference_docname: String?  // Document name/ID

// Assignment Fields
assigned_to: String?                    // Single user email (legacy)
assigned_to_list: List<dynamic>?       // List of users (new)
meeting_attendees: List<dynamic>?     // List of attendees

// System Fields (read-only in response)
name: int                              // Task ID
modified: DateTime                     // Last modified timestamp
creation: DateTime?                   // Creation timestamp
owner: String?                         // Creator user email
modified_by: String?                   // Last modifier user email
```

### User Object Fields

```dart
{
  email: String,        // Required: User email
  name: String?,        // Optional: User full name
  profile_pic: String?, // Optional: Profile picture URL or file path
  id: String?          // Optional: User ID (can be same as email)
}
```

---

## 🎯 Quick Reference for Flutter Developers

### Create Task with Multiple Assignees

```dart
final taskData = {
  "task_type": "Meeting",
  "title": "Team Meeting",
  "assigned_to_list": [
    {
      "email": "user1@example.com",
      "name": "User One",
      "profile_pic": "https://example.com/pic.jpg",
      "id": "user1@example.com"
    }
  ]
};
```

### Update Task Status and Assignees

```dart
final updateData = {
  "name": 123,  // Task ID
  "status": "In Progress",
  "assigned_to_list": ["user1@example.com", "user2@example.com"]
};
```

### Set Reference Document

```dart
final taskData = {
  "task_type": "Call",
  "reference_doctype": "CRM Lead",
  "reference_docname": "CRM-LEAD-2025-001"
};
```

---

## ⚠️ Important Notes

1. **task_type is REQUIRED** for create_task
2. **name (Task ID) is REQUIRED** for edit_task
3. **assigned_to_list** replaces all existing assignments
4. **meeting_attendees** replaces all existing attendees
5. **User Objects** automatically create/update users if they don't exist
6. **All fields are optional** except `task_type` in create_task and `name` in edit_task
7. **Response always includes all fields** when using `return_all_fields=True`

---

## 📚 Related Documentation

- [API Endpoints Documentation](./API_ENDPOINTS.md)
- [OAuth Configuration](./FLUTTER_OAUTH_HANDOVER_AR.md)
- [Mobile API Reference](./MOBILE_API_README.md)

---

**Last Updated:** 2025-12-10
**Version:** 2.0

