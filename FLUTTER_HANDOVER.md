# Flutter Developer Handover Guide

Complete integration guide for Flutter developers to consume the CRM Task Mobile API.

## Overview

This API provides complete CRM Task management through REST endpoints. It uses **session cookie authentication** (standard Frappe login), no custom tokens or JWT.

---

## What You're Getting

1. **7 REST Endpoints** for CRM Task management (see [API_ENDPOINTS.md](./API_ENDPOINTS.md))
2. **Session-based auth** using standard Frappe login
3. **Postman collection** for reference ([POSTMAN_COLLECTION.json](./POSTMAN_COLLECTION.json))
4. **Complete documentation** with all parameters and responses

---

## Quick Integration Steps

### 1. Setup Base Configuration

```dart
class ApiConfig {
  static const String baseUrl = 'https://your-site.com';
  static const String apiBase = '$baseUrl/api/method/crm.api.mobile_api';
  
  // Endpoints
  static const String login = '$baseUrl/api/method/login';
  static const String logout = '$baseUrl/api/method/logout';
  
  static const String createTask = '$apiBase.create_task';
  static const String editTask = '$apiBase.edit_task';
  static const String deleteTask = '$apiBase.delete_task';
  static const String updateStatus = '$apiBase.update_status';
  static const String filterTasks = '$apiBase.filter_tasks';
  static const String homeTasks = '$apiBase.home_tasks';
  static const String mainPageBuckets = '$apiBase.main_page_buckets';
}
```

### 2. Setup HTTP Client with Cookie Management

**Using Dio (Recommended):**

```dart
import 'package:dio/dio.dart';
import 'package:dio_cookie_manager/dio_cookie_manager.dart';
import 'package:cookie_jar/cookie_jar.dart';
import 'package:path_provider/path_provider.dart';

class ApiClient {
  late Dio dio;
  late CookieJar cookieJar;
  
  Future<void> init() async {
    // Setup persistent cookie storage
    final appDocDir = await getApplicationDocumentsDirectory();
    final cookiePath = '${appDocDir.path}/.cookies/';
    
    cookieJar = PersistCookieJar(
      storage: FileStorage(cookiePath),
    );
    
    dio = Dio(BaseOptions(
      baseUrl: ApiConfig.baseUrl,
      connectTimeout: const Duration(seconds: 30),
      receiveTimeout: const Duration(seconds: 30),
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
    ));
    
    // Add cookie manager
    dio.interceptors.add(CookieManager(cookieJar));
    
    // Add logging (optional)
    dio.interceptors.add(LogInterceptor(
      requestBody: true,
      responseBody: true,
    ));
  }
  
  Dio get client => dio;
}
```

**Using HTTP Package:**

```dart
import 'package:http/http.dart' as http;
import 'dart:convert';

class ApiClient {
  String? sessionCookie;
  
  Map<String, String> get headers => {
    'Content-Type': 'application/json',
    if (sessionCookie != null) 'Cookie': sessionCookie!,
  };
  
  void saveCookies(http.Response response) {
    final cookies = response.headers['set-cookie'];
    if (cookies != null) {
      sessionCookie = cookies;
    }
  }
}
```

### 3. Implement Authentication

```dart
class AuthService {
  final ApiClient apiClient;
  
  AuthService(this.apiClient);
  
  Future<bool> login(String username, String password) async {
    try {
      final response = await apiClient.client.post(
        '/api/method/login',
        data: {
          'usr': username,
          'pwd': password,
        },
        options: Options(
          contentType: Headers.formUrlEncodedContentType,
        ),
      );
      
      // Cookies are automatically saved by CookieManager
      return response.statusCode == 200;
    } on DioException catch (e) {
      if (e.response?.statusCode == 401) {
        throw Exception('Invalid credentials');
      }
      rethrow;
    }
  }
  
  Future<void> logout() async {
    await apiClient.client.post('/api/method/logout');
    // Clear cookies
    await apiClient.cookieJar.deleteAll();
  }
  
  Future<bool> isLoggedIn() async {
    try {
      // Try a simple API call to check session
      final response = await apiClient.client.get(
        '/api/method/crm.api.mobile_api.home_tasks',
        queryParameters: {'limit': 1},
      );
      return response.statusCode == 200;
    } catch (e) {
      return false;
    }
  }
}
```

### 4. Create Task Model

```dart
class CrmTask {
  final String name;
  final String? title;
  final String status;
  final String priority;
  final String? startDate;
  final String? dueDate;
  final String? assignedTo;
  final String modified;
  
  CrmTask({
    required this.name,
    this.title,
    required this.status,
    required this.priority,
    this.startDate,
    this.dueDate,
    this.assignedTo,
    required this.modified,
  });
  
  factory CrmTask.fromJson(Map<String, dynamic> json) {
    return CrmTask(
      name: json['name'] as String,
      title: json['title'] as String?,
      status: json['status'] as String,
      priority: json['priority'] as String,
      startDate: json['start_date'] as String?,
      dueDate: json['due_date'] as String?,
      assignedTo: json['assigned_to'] as String?,
      modified: json['modified'] as String,
    );
  }
  
  Map<String, dynamic> toJson() {
    return {
      'name': name,
      'title': title,
      'status': status,
      'priority': priority,
      'start_date': startDate,
      'due_date': dueDate,
      'assigned_to': assignedTo,
      'modified': modified,
    };
  }
}
```

### 5. Implement API Service

```dart
class TaskService {
  final ApiClient apiClient;
  
  TaskService(this.apiClient);
  
  // Create Task
  Future<CrmTask> createTask({
    required String taskType,
    String? title,
    String? status,
    String? priority,
    String? startDate,
    String? description,
    String? assignedTo,
    String? dueDate,
  }) async {
    final response = await apiClient.client.post(
      '/api/method/crm.api.mobile_api.create_task',
      data: {
        'task_type': taskType,
        if (title != null) 'title': title,
        if (status != null) 'status': status,
        if (priority != null) 'priority': priority,
        if (startDate != null) 'start_date': startDate,
        if (description != null) 'description': description,
        if (assignedTo != null) 'assigned_to': assignedTo,
        if (dueDate != null) 'due_date': dueDate,
      },
    );
    
    return CrmTask.fromJson(response.data['message']);
  }
  
  // Edit Task
  Future<CrmTask> editTask({
    required String name,
    String? title,
    String? status,
    String? priority,
    String? startDate,
    String? taskType,
    String? description,
    String? assignedTo,
    String? dueDate,
  }) async {
    final response = await apiClient.client.post(
      '/api/method/crm.api.mobile_api.edit_task',
      data: {
        'name': name,
        if (title != null) 'title': title,
        if (status != null) 'status': status,
        if (priority != null) 'priority': priority,
        if (startDate != null) 'start_date': startDate,
        if (taskType != null) 'task_type': taskType,
        if (description != null) 'description': description,
        if (assignedTo != null) 'assigned_to': assignedTo,
        if (dueDate != null) 'due_date': dueDate,
      },
    );
    
    return CrmTask.fromJson(response.data['message']);
  }
  
  // Update Status
  Future<CrmTask> updateStatus(String name, String status) async {
    final response = await apiClient.client.post(
      '/api/method/crm.api.mobile_api.update_status',
      data: {
        'name': name,
        'status': status,
      },
    );
    
    return CrmTask.fromJson(response.data['message']);
  }
  
  // Delete Task
  Future<bool> deleteTask(String name) async {
    final response = await apiClient.client.post(
      '/api/method/crm.api.mobile_api.delete_task',
      data: {'name': name},
    );
    
    return response.data['message']['ok'] == true;
  }
  
  // Filter Tasks
  Future<List<CrmTask>> filterTasks({
    String? dateFrom,
    String? dateTo,
    String? importance,
    String? status,
    int limit = 50,
    int offset = 0,
    String orderBy = 'modified desc',
  }) async {
    final response = await apiClient.client.get(
      '/api/method/crm.api.mobile_api.filter_tasks',
      queryParameters: {
        if (dateFrom != null) 'date_from': dateFrom,
        if (dateTo != null) 'date_to': dateTo,
        if (importance != null) 'importance': importance,
        if (status != null) 'status': status,
        'limit': limit,
        'offset': offset,
        'order_by': orderBy,
      },
    );
    
    final data = response.data['message']['data'] as List;
    return data.map((json) => CrmTask.fromJson(json)).toList();
  }
  
  // Home Tasks (Today's Top N)
  Future<List<CrmTask>> getHomeTasks({int limit = 5}) async {
    final response = await apiClient.client.get(
      '/api/method/crm.api.mobile_api.home_tasks',
      queryParameters: {'limit': limit},
    );
    
    final data = response.data['message']['today'] as List;
    return data.map((json) => CrmTask.fromJson(json)).toList();
  }
  
  // Main Page Buckets
  Future<TaskBuckets> getMainPageBuckets({int minEach = 5}) async {
    final response = await apiClient.client.get(
      '/api/method/crm.api.mobile_api.main_page_buckets',
      queryParameters: {'min_each': minEach},
    );
    
    final message = response.data['message'];
    return TaskBuckets(
      today: (message['today'] as List)
          .map((json) => CrmTask.fromJson(json))
          .toList(),
      late: (message['late'] as List)
          .map((json) => CrmTask.fromJson(json))
          .toList(),
      upcoming: (message['upcoming'] as List)
          .map((json) => CrmTask.fromJson(json))
          .toList(),
      minEach: message['min_each'] as int,
    );
  }
}

class TaskBuckets {
  final List<CrmTask> today;
  final List<CrmTask> late;
  final List<CrmTask> upcoming;
  final int minEach;
  
  TaskBuckets({
    required this.today,
    required this.late,
    required this.upcoming,
    required this.minEach,
  });
}
```

### 6. Error Handling

```dart
class ApiException implements Exception {
  final String message;
  final String? excType;
  final int? statusCode;
  
  ApiException(this.message, {this.excType, this.statusCode});
  
  @override
  String toString() => message;
}

class ApiErrorHandler {
  static ApiException handleError(DioException e) {
    if (e.response != null) {
      final statusCode = e.response!.statusCode;
      final data = e.response!.data;
      
      String message = 'An error occurred';
      String? excType;
      
      if (data is Map<String, dynamic>) {
        message = data['exception'] ?? data['message'] ?? message;
        excType = data['exc_type'];
      }
      
      switch (statusCode) {
        case 401:
          return ApiException(
            'Not logged in. Please login again.',
            excType: excType,
            statusCode: statusCode,
          );
        case 403:
          return ApiException(
            'Permission denied. You don\'t have access to this resource.',
            excType: excType,
            statusCode: statusCode,
          );
        case 404:
          return ApiException(
            'Task not found.',
            excType: excType,
            statusCode: statusCode,
          );
        case 417:
          return ApiException(
            message,
            excType: excType,
            statusCode: statusCode,
          );
        default:
          return ApiException(
            message,
            excType: excType,
            statusCode: statusCode,
          );
      }
    }
    
    if (e.type == DioExceptionType.connectionTimeout ||
        e.type == DioExceptionType.receiveTimeout) {
      return ApiException('Connection timeout. Please try again.');
    }
    
    if (e.type == DioExceptionType.connectionError) {
      return ApiException('No internet connection.');
    }
    
    return ApiException('An unexpected error occurred.');
  }
}

// Usage in service methods
try {
  final response = await apiClient.client.post(...);
  return response.data;
} on DioException catch (e) {
  throw ApiErrorHandler.handleError(e);
}
```

### 7. State Management Integration

**Using Provider:**

```dart
class TaskProvider extends ChangeNotifier {
  final TaskService taskService;
  
  List<CrmTask> _tasks = [];
  TaskBuckets? _buckets;
  bool _loading = false;
  String? _error;
  
  List<CrmTask> get tasks => _tasks;
  TaskBuckets? get buckets => _buckets;
  bool get loading => _loading;
  String? get error => _error;
  
  TaskProvider(this.taskService);
  
  Future<void> loadHomeTasks({int limit = 5}) async {
    _loading = true;
    _error = null;
    notifyListeners();
    
    try {
      _tasks = await taskService.getHomeTasks(limit: limit);
      _error = null;
    } catch (e) {
      _error = e.toString();
      _tasks = [];
    } finally {
      _loading = false;
      notifyListeners();
    }
  }
  
  Future<void> loadBuckets({int minEach = 5}) async {
    _loading = true;
    _error = null;
    notifyListeners();
    
    try {
      _buckets = await taskService.getMainPageBuckets(minEach: minEach);
      _error = null;
    } catch (e) {
      _error = e.toString();
      _buckets = null;
    } finally {
      _loading = false;
      notifyListeners();
    }
  }
  
  Future<void> createTask({
    required String taskType,
    String? title,
    String? priority,
    String? status,
  }) async {
    _loading = true;
    notifyListeners();
    
    try {
      await taskService.createTask(
        taskType: taskType,
        title: title,
        priority: priority,
        status: status,
      );
      // Reload data
      await loadHomeTasks();
    } catch (e) {
      _error = e.toString();
      _loading = false;
      notifyListeners();
      rethrow;
    }
  }
  
  Future<void> updateTaskStatus(String name, String status) async {
    try {
      await taskService.updateStatus(name, status);
      // Update local task
      final index = _tasks.indexWhere((t) => t.name == name);
      if (index != -1) {
        _tasks[index] = await taskService.filterTasks(limit: 1, offset: 0)
            .then((tasks) => tasks.first);
        notifyListeners();
      }
    } catch (e) {
      _error = e.toString();
      notifyListeners();
      rethrow;
    }
  }
}
```

**Using Riverpod:**

```dart
final apiClientProvider = Provider((ref) => ApiClient()..init());

final taskServiceProvider = Provider((ref) {
  final apiClient = ref.watch(apiClientProvider);
  return TaskService(apiClient);
});

final homeTasksProvider = FutureProvider.family<List<CrmTask>, int>((ref, limit) async {
  final taskService = ref.watch(taskServiceProvider);
  return taskService.getHomeTasks(limit: limit);
});

final mainPageBucketsProvider = FutureProvider.family<TaskBuckets, int>((ref, minEach) async {
  final taskService = ref.watch(taskServiceProvider);
  return taskService.getMainPageBuckets(minEach: minEach);
});
```

### 8. UI Implementation Examples

**Home Screen with Today's Tasks:**

```dart
class HomeScreen extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: Text('Today\'s Tasks')),
      body: Consumer<TaskProvider>(
        builder: (context, provider, child) {
          if (provider.loading) {
            return Center(child: CircularProgressIndicator());
          }
          
          if (provider.error != null) {
            return Center(child: Text('Error: ${provider.error}'));
          }
          
          if (provider.tasks.isEmpty) {
            return Center(child: Text('No tasks for today'));
          }
          
          return RefreshIndicator(
            onRefresh: () => provider.loadHomeTasks(),
            child: ListView.builder(
              itemCount: provider.tasks.length,
              itemBuilder: (context, index) {
                final task = provider.tasks[index];
                return TaskCard(task: task);
              },
            ),
          );
        },
      ),
      floatingActionButton: FloatingActionButton(
        onPressed: () => _showCreateTaskDialog(context),
        child: Icon(Icons.add),
      ),
    );
  }
}
```

**Main Page with Buckets:**

```dart
class MainPage extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return Consumer<TaskProvider>(
      builder: (context, provider, child) {
        if (provider.buckets == null) {
          return Center(child: CircularProgressIndicator());
        }
        
        return SingleChildScrollView(
          child: Column(
            children: [
              _buildBucketSection('Today', provider.buckets!.today),
              _buildBucketSection('Late', provider.buckets!.late),
              _buildBucketSection('Upcoming', provider.buckets!.upcoming),
            ],
          ),
        );
      },
    );
  }
  
  Widget _buildBucketSection(String title, List<CrmTask> tasks) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Padding(
          padding: EdgeInsets.all(16),
          child: Text(
            '$title (${tasks.length})',
            style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
          ),
        ),
        ...tasks.map((task) => TaskCard(task: task)),
      ],
    );
  }
}
```

**Task Card Widget:**

```dart
class TaskCard extends StatelessWidget {
  final CrmTask task;
  
  const TaskCard({required this.task});
  
  @override
  Widget build(BuildContext context) {
    return Card(
      margin: EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      child: ListTile(
        leading: _buildPriorityIcon(),
        title: Text(task.title ?? 'No title'),
        subtitle: Text('${task.status} • ${task.startDate ?? 'No date'}'),
        trailing: PopupMenuButton<String>(
          onSelected: (value) => _handleAction(context, value),
          itemBuilder: (context) => [
            PopupMenuItem(value: 'edit', child: Text('Edit')),
            PopupMenuItem(value: 'status', child: Text('Change Status')),
            PopupMenuItem(value: 'delete', child: Text('Delete')),
          ],
        ),
        onTap: () => _showTaskDetails(context),
      ),
    );
  }
  
  Widget _buildPriorityIcon() {
    final color = task.priority == 'High'
        ? Colors.red
        : task.priority == 'Medium'
            ? Colors.orange
            : Colors.green;
    
    return Icon(Icons.flag, color: color);
  }
  
  void _handleAction(BuildContext context, String action) async {
    final provider = context.read<TaskProvider>();
    
    switch (action) {
      case 'edit':
        // Show edit dialog
        break;
      case 'status':
        // Show status picker
        final newStatus = await _showStatusPicker(context);
        if (newStatus != null) {
          await provider.updateTaskStatus(task.name, newStatus);
        }
        break;
      case 'delete':
        // Confirm and delete
        break;
    }
  }
}
```

---

## Important Notes

### Date Formatting
- **start_date**: Use `YYYY-MM-DD` format (e.g., "2025-12-03")
- **due_date**: Use `YYYY-MM-DD HH:MM:SS` format (e.g., "2025-12-03 15:30:00")

```dart
String formatDate(DateTime date) {
  return '${date.year}-${date.month.toString().padLeft(2, '0')}-${date.day.toString().padLeft(2, '0')}';
}

String formatDateTime(DateTime dateTime) {
  return '${formatDate(dateTime)} ${dateTime.hour.toString().padLeft(2, '0')}:${dateTime.minute.toString().padLeft(2, '0')}:${dateTime.second.toString().padLeft(2, '0')}';
}
```

### Session Expiry Handling
```dart
dio.interceptors.add(InterceptorsWrapper(
  onError: (DioException e, ErrorInterceptorHandler handler) async {
    if (e.response?.statusCode == 401) {
      // Session expired - redirect to login
      // Clear stored data
      await authService.logout();
      // Navigate to login screen
      navigatorKey.currentState?.pushReplacementNamed('/login');
    }
    handler.next(e);
  },
));
```

### Pagination Implementation
```dart
class TaskListPage extends StatefulWidget {
  @override
  _TaskListPageState createState() => _TaskListPageState();
}

class _TaskListPageState extends State<TaskListPage> {
  final ScrollController _scrollController = ScrollController();
  List<CrmTask> tasks = [];
  int offset = 0;
  final int limit = 20;
  bool loading = false;
  bool hasMore = true;
  
  @override
  void initState() {
    super.initState();
    _loadTasks();
    _scrollController.addListener(_onScroll);
  }
  
  void _onScroll() {
    if (_scrollController.position.pixels ==
        _scrollController.position.maxScrollExtent) {
      _loadMore();
    }
  }
  
  Future<void> _loadTasks() async {
    setState(() => loading = true);
    try {
      final newTasks = await taskService.filterTasks(
        limit: limit,
        offset: 0,
      );
      setState(() {
        tasks = newTasks;
        offset = limit;
        hasMore = newTasks.length == limit;
      });
    } finally {
      setState(() => loading = false);
    }
  }
  
  Future<void> _loadMore() async {
    if (loading || !hasMore) return;
    
    setState(() => loading = true);
    try {
      final newTasks = await taskService.filterTasks(
        limit: limit,
        offset: offset,
      );
      setState(() {
        tasks.addAll(newTasks);
        offset += limit;
        hasMore = newTasks.length == limit;
      });
    } finally {
      setState(() => loading = false);
    }
  }
  
  @override
  Widget build(BuildContext context) {
    return ListView.builder(
      controller: _scrollController,
      itemCount: tasks.length + (hasMore ? 1 : 0),
      itemBuilder: (context, index) {
        if (index == tasks.length) {
          return Center(child: CircularProgressIndicator());
        }
        return TaskCard(task: tasks[index]);
      },
    );
  }
}
```

---

## Testing Checklist

Before deploying to production:

- [ ] Login/logout flows work correctly
- [ ] Session cookies persist across app restarts
- [ ] Session expiry handled gracefully
- [ ] All CRUD operations work
- [ ] Pagination works smoothly
- [ ] Pull-to-refresh updates data
- [ ] Error messages are user-friendly
- [ ] Offline state handled (show cached data or message)
- [ ] Loading states displayed properly
- [ ] Date/time formatting consistent
- [ ] Task status changes reflect immediately
- [ ] Filter combinations work correctly

---

## Common Pitfalls & Solutions

### 1. Cookies Not Persisting
**Problem**: Session lost when app restarts  
**Solution**: Use `PersistCookieJar` with file storage

### 2. 401 Errors After Some Time
**Problem**: Session expired  
**Solution**: Implement auto re-login or redirect to login screen

### 3. Date Format Mismatches
**Problem**: API rejects dates  
**Solution**: Always use `YYYY-MM-DD` format, never locale-specific formats

### 4. Large Lists Slow Performance
**Problem**: Fetching too many tasks at once  
**Solution**: Implement pagination with reasonable limits (20-50 per page)

### 5. Network Errors Not Handled
**Problem**: App crashes on network issues  
**Solution**: Wrap all API calls in try-catch, show user-friendly errors

---

## Required Dependencies

Add to `pubspec.yaml`:

```yaml
dependencies:
  dio: ^5.4.0
  dio_cookie_manager: ^3.1.0
  cookie_jar: ^4.0.8
  path_provider: ^2.1.0
  provider: ^6.1.0  # or riverpod, bloc, etc.
  intl: ^0.18.0  # for date formatting
  
dev_dependencies:
  flutter_test:
    sdk: flutter
  mockito: ^5.4.0  # for testing
```

---

## Resources

- **[API_ENDPOINTS.md](./API_ENDPOINTS.md)** - Complete endpoint reference
- **[MOBILE_API_README.md](./MOBILE_API_README.md)** - Installation and auth guide
- **[POSTMAN_COLLECTION.json](./POSTMAN_COLLECTION.json)** - Test all endpoints
- **[QA_CHECKLIST.md](./QA_CHECKLIST.md)** - QA testing checklist

---

## Contact & Support

For API issues:
- Review documentation first
- Test with Postman collection
- Check server logs for detailed errors
- Verify user has required roles (Sales User/Sales Manager)

For integration questions:
- Refer to code examples above
- Check Flutter packages documentation
- Review Frappe API documentation: https://frappeframework.com/docs

---

## Final Notes

✅ **Authentication**: Standard Frappe session cookies only  
✅ **Permissions**: Standard Frappe roles (Sales User/Sales Manager)  
✅ **No custom logic**: Uses built-in Frappe features  
✅ **Field names**: Uses `start_date` (not `exp_start_date`)  
✅ **No notifications**: Not implemented (per requirements)  
✅ **No CRM Lead**: Tasks are standalone entities  

**Good luck with your Flutter integration! 🚀**

