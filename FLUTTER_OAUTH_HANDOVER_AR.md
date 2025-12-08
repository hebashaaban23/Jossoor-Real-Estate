# 📱 تسليم Flutter - CRM Mobile API مع OAuth2

**التاريخ**: 3 ديسمبر 2025  
**الحالة**: ✅ OAuth2 + API شغالين 100%  
**السيرفر**: https://trust.jossoor.org

---

## 🎯 ملخص سريع

تم إضافة **OAuth2 مع PKCE** للـ API. الآن عندك خيارين:

1. **OAuth2 Bearer Token** (موصى به للموبايل) ⭐
2. Session Cookies (الطريقة القديمة)

**كل الـ 7 endpoints شغالة مع OAuth2!**

---

## 🔐 طريقة OAuth2 (الموصى بها)

### معلومات الاتصال

```
Base URL:    https://trust.jossoor.org
Client ID:   3rcioodn8t
Scopes:      all openid
```

**ملاحظة مهمة**: مش محتاج `client_secret` للموبايل! استخدم **PKCE**.

---

## 📋 الـ Endpoints الأساسية

### 1️⃣ **Get Access Token** (Password Grant)

للاختبار السريع أو التطبيقات الموثوقة:

```http
POST /api/method/frappe.integrations.oauth2.get_token
Content-Type: application/x-www-form-urlencoded

grant_type=password
username=user@example.com
password=user_password
client_id=3rcioodn8t
scope=all openid
```

**Response:**
```json
{
  "access_token": "9dwbmZuYntp9pCHm1KzJpJNKBTHHw2",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "x8gj3joRkNNtMQpUCEFEW9vIf2rLy6",
  "scope": "all openid"
}
```

### 2️⃣ **Refresh Token** (تحديث الـ Token)

عشان تجدد الـ access token لما ينتهي (كل ساعة):

```http
POST /api/method/frappe.integrations.oauth2.get_token
Content-Type: application/x-www-form-urlencoded

grant_type=refresh_token
refresh_token=x8gj3joRkNNtMQpUCEFEW9vIf2rLy6
client_id=3rcioodn8t
```

**Response:**
```json
{
  "access_token": "hGN2sYJr9x75otSWRNPpbQTI48ucoC",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "yz1207uohSFgkG5Aco5m6iKHkFch0L",
  "scope": "all openid"
}
```

⚠️ **مهم**: الـ refresh token بيتغير مع كل تحديث! احفظ الجديد.

### 3️⃣ **استخدام الـ Token** في استدعاء API

```http
GET /api/method/crm.api.mobile_api.home_tasks?limit=5
Authorization: Bearer 9dwbmZuYntp9pCHm1KzJpJNKBTHHw2
```

---

## 🔄 OAuth2 Flow كامل (PKCE للموبايل)

### الخطوة 1: توليد PKCE Parameters

```dart
import 'dart:convert';
import 'dart:math';
import 'package:crypto/crypto.dart';

String generateCodeVerifier() {
  final random = Random.secure();
  final values = List<int>.generate(32, (i) => random.nextInt(256));
  return base64UrlEncode(values).replaceAll('=', '');
}

String generateCodeChallenge(String verifier) {
  final bytes = utf8.encode(verifier);
  final digest = sha256.convert(bytes);
  return base64UrlEncode(digest.bytes).replaceAll('=', '');
}

// استخدام
final codeVerifier = generateCodeVerifier();
final codeChallenge = generateCodeChallenge(codeVerifier);
```

### الخطوة 2: Authorization Request

افتح صفحة في المتصفح أو WebView:

```
https://trust.jossoor.org/api/method/frappe.integrations.oauth2.authorize?
  client_id=3rcioodn8t&
  response_type=code&
  redirect_uri=app.trust://oauth2redirect&
  scope=all%20openid&
  state=random_csrf_token&
  code_challenge=CODE_CHALLENGE&
  code_challenge_method=S256
```

### الخطوة 3: التقاط Authorization Code

المستخدم يسجل دخول ويوافق → يرجع للتطبيق:

```
app.trust://oauth2redirect?code=AUTH_CODE&state=random_csrf_token
```

### الخطوة 4: تبادل Code بـ Token

```http
POST /api/method/frappe.integrations.oauth2.get_token
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code
code=AUTH_CODE
redirect_uri=app.trust://oauth2redirect
client_id=3rcioodn8t
code_verifier=ORIGINAL_CODE_VERIFIER
```

**Response:**
```json
{
  "access_token": "...",
  "refresh_token": "...",
  "expires_in": 3600,
  "token_type": "Bearer"
}
```

---

## 📱 كود Flutter جاهز

### التثبيت

```yaml
dependencies:
  dio: ^5.4.0
  flutter_secure_storage: ^9.0.0
  crypto: ^3.0.3
```

### كلاس OAuth Manager

```dart
import 'package:dio/dio.dart';
import 'package:flutter_secure_storage/flutter_secure_storage.dart';
import 'dart:convert';
import 'dart:math';
import 'package:crypto/crypto.dart';

class OAuthManager {
  final Dio dio;
  final FlutterSecureStorage storage;
  final String baseUrl = 'https://trust.jossoor.org';
  final String clientId = '3rcioodn8t';
  
  String? _accessToken;
  String? _refreshToken;
  DateTime? _expiresAt;
  
  OAuthManager()
      : dio = Dio(),
        storage = const FlutterSecureStorage();
  
  // توليد PKCE parameters
  String _generateCodeVerifier() {
    final random = Random.secure();
    final values = List<int>.generate(32, (i) => random.nextInt(256));
    return base64UrlEncode(values).replaceAll('=', '');
  }
  
  String _generateCodeChallenge(String verifier) {
    final bytes = utf8.encode(verifier);
    final digest = sha256.convert(bytes);
    return base64UrlEncode(digest.bytes).replaceAll('=', '');
  }
  
  // Password Grant (للاختبار أو التطبيقات الموثوقة)
  Future<bool> loginWithPassword(String username, String password) async {
    try {
      final response = await dio.post(
        '$baseUrl/api/method/frappe.integrations.oauth2.get_token',
        data: {
          'grant_type': 'password',
          'username': username,
          'password': password,
          'client_id': clientId,
          'scope': 'all openid',
        },
        options: Options(
          contentType: Headers.formUrlEncodedContentType,
        ),
      );
      
      return _handleTokenResponse(response.data);
    } catch (e) {
      print('Login error: $e');
      return false;
    }
  }
  
  // Refresh Token
  Future<bool> refreshAccessToken() async {
    if (_refreshToken == null) {
      return false;
    }
    
    try {
      final response = await dio.post(
        '$baseUrl/api/method/frappe.integrations.oauth2.get_token',
        data: {
          'grant_type': 'refresh_token',
          'refresh_token': _refreshToken,
          'client_id': clientId,
        },
        options: Options(
          contentType: Headers.formUrlEncodedContentType,
        ),
      );
      
      return _handleTokenResponse(response.data);
    } catch (e) {
      print('Refresh token error: $e');
      return false;
    }
  }
  
  bool _handleTokenResponse(Map<String, dynamic> data) {
    _accessToken = data['access_token'];
    _refreshToken = data['refresh_token'];
    
    final expiresIn = data['expires_in'] ?? 3600;
    _expiresAt = DateTime.now().add(Duration(seconds: expiresIn));
    
    // حفظ الـ tokens بشكل آمن
    _saveTokens();
    
    return true;
  }
  
  Future<void> _saveTokens() async {
    if (_accessToken != null) {
      await storage.write(key: 'access_token', value: _accessToken);
    }
    if (_refreshToken != null) {
      await storage.write(key: 'refresh_token', value: _refreshToken);
    }
    if (_expiresAt != null) {
      await storage.write(key: 'expires_at', value: _expiresAt!.toIso8601String());
    }
  }
  
  Future<void> loadTokens() async {
    _accessToken = await storage.read(key: 'access_token');
    _refreshToken = await storage.read(key: 'refresh_token');
    
    final expiresAtStr = await storage.read(key: 'expires_at');
    if (expiresAtStr != null) {
      _expiresAt = DateTime.parse(expiresAtStr);
    }
  }
  
  Future<String?> getValidAccessToken() async {
    // تحميل الـ tokens من التخزين
    if (_accessToken == null) {
      await loadTokens();
    }
    
    // التحقق من الصلاحية
    if (_accessToken != null && _expiresAt != null) {
      // تجديد قبل انتهاء الصلاحية بـ 5 دقائق
      if (DateTime.now().isAfter(_expiresAt!.subtract(Duration(minutes: 5)))) {
        final refreshed = await refreshAccessToken();
        if (!refreshed) {
          return null;
        }
      }
      return _accessToken;
    }
    
    return null;
  }
  
  Future<void> logout() async {
    _accessToken = null;
    _refreshToken = null;
    _expiresAt = null;
    
    await storage.deleteAll();
  }
}
```

### كلاس CRM API

```dart
import 'package:dio/dio.dart';

class CRMApi {
  final Dio dio;
  final OAuthManager oauthManager;
  final String baseUrl = 'https://trust.jossoor.org';
  
  CRMApi(this.oauthManager) : dio = Dio();
  
  Future<Map<String, String>> _getHeaders() async {
    final token = await oauthManager.getValidAccessToken();
    if (token == null) {
      throw Exception('Not authenticated');
    }
    
    return {
      'Authorization': 'Bearer $token',
      'Content-Type': 'application/json',
    };
  }
  
  // 1. جلب مهام اليوم
  Future<Map<String, dynamic>> getHomeTasks({int limit = 5}) async {
    try {
      final response = await dio.get(
        '$baseUrl/api/method/crm.api.mobile_api.home_tasks',
        queryParameters: {'limit': limit},
        options: Options(headers: await _getHeaders()),
      );
      return response.data['message'];
    } catch (e) {
      print('Error fetching home tasks: $e');
      rethrow;
    }
  }
  
  // 2. تصنيف المهام (اليوم/متأخرة/قادمة)
  Future<Map<String, dynamic>> getMainPageBuckets({int minEach = 5}) async {
    try {
      final response = await dio.get(
        '$baseUrl/api/method/crm.api.mobile_api.main_page_buckets',
        queryParameters: {'min_each': minEach},
        options: Options(headers: await _getHeaders()),
      );
      return response.data['message'];
    } catch (e) {
      print('Error fetching buckets: $e');
      rethrow;
    }
  }
  
  // 3. البحث والفلترة
  Future<List<dynamic>> filterTasks({
    String? dateFrom,
    String? dateTo,
    String? importance,
    String? status,
    int limit = 50,
    int offset = 0,
  }) async {
    try {
      Map<String, dynamic> params = {'limit': limit, 'offset': offset};
      
      if (dateFrom != null) params['date_from'] = dateFrom;
      if (dateTo != null) params['date_to'] = dateTo;
      if (importance != null) params['importance'] = importance;
      if (status != null) params['status'] = status;
      
      final response = await dio.get(
        '$baseUrl/api/method/crm.api.mobile_api.filter_tasks',
        queryParameters: params,
        options: Options(headers: await _getHeaders()),
      );
      return response.data['message']['data'];
    } catch (e) {
      print('Error filtering tasks: $e');
      rethrow;
    }
  }
  
  // 4. إنشاء مهمة جديدة
  Future<Map<String, dynamic>> createTask({
    required String title,
    required String taskType,  // مطلوب!
    String? status,
    String? priority,
    String? startDate,
    String? dueDate,
    String? description,
    String? assignedTo,
  }) async {
    try {
      Map<String, dynamic> data = {
        'title': title,
        'task_type': taskType,  // مطلوب!
      };
      
      if (status != null) data['status'] = status;
      if (priority != null) data['priority'] = priority;
      if (startDate != null) data['start_date'] = startDate;
      if (dueDate != null) data['due_date'] = dueDate;
      if (description != null) data['description'] = description;
      if (assignedTo != null) data['assigned_to'] = assignedTo;
      
      final response = await dio.post(
        '$baseUrl/api/method/crm.api.mobile_api.create_task',
        data: data,
        options: Options(headers: await _getHeaders()),
      );
      return response.data['message'];
    } catch (e) {
      print('Error creating task: $e');
      rethrow;
    }
  }
  
  // 5. تعديل مهمة
  Future<Map<String, dynamic>> editTask({
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
        '$baseUrl/api/method/crm.api.mobile_api.edit_task',
        data: data,
        options: Options(headers: await _getHeaders()),
      );
      return response.data['message'];
    } catch (e) {
      print('Error editing task: $e');
      rethrow;
    }
  }
  
  // 6. تغيير حالة المهمة
  Future<Map<String, dynamic>> updateStatus({
    required int taskId,
    required String status,
  }) async {
    try {
      final response = await dio.post(
        '$baseUrl/api/method/crm.api.mobile_api.update_status',
        data: {'task_id': taskId, 'status': status},
        options: Options(headers: await _getHeaders()),
      );
      return response.data['message'];
    } catch (e) {
      print('Error updating status: $e');
      rethrow;
    }
  }
  
  // 7. حذف مهمة
  Future<String> deleteTask(int taskId) async {
    try {
      final response = await dio.post(
        '$baseUrl/api/method/crm.api.mobile_api.delete_task',
        data: {'task_id': taskId},
        options: Options(headers: await _getHeaders()),
      );
      return response.data['message'];
    } catch (e) {
      print('Error deleting task: $e');
      rethrow;
    }
  }
}
```

---

## 🧪 أمثلة الاستخدام

### مثال كامل

```dart
void main() async {
  // إنشاء OAuth manager
  final oauthManager = OAuthManager();
  
  // تسجيل الدخول
  final loggedIn = await oauthManager.loginWithPassword(
    'user@example.com',
    'password123',
  );
  
  if (!loggedIn) {
    print('فشل تسجيل الدخول');
    return;
  }
  
  print('✅ تم تسجيل الدخول بنجاح');
  
  // إنشاء API client
  final api = CRMApi(oauthManager);
  
  // جلب مهام اليوم
  final homeTasks = await api.getHomeTasks(limit: 10);
  print('مهام اليوم: ${homeTasks['today'].length}');
  
  // جلب التصنيفات
  final buckets = await api.getMainPageBuckets();
  print('اليوم: ${buckets['today'].length}');
  print('متأخرة: ${buckets['late'].length}');
  print('قادمة: ${buckets['upcoming'].length}');
  
  // البحث عن مهام
  final tasks = await api.filterTasks(
    dateFrom: '2025-12-01',
    dateTo: '2025-12-31',
    importance: 'High,Medium',
    status: 'Open,In Progress',
    limit: 20,
  );
  print('نتائج البحث: ${tasks.length}');
  
  // إنشاء مهمة جديدة
  final newTask = await api.createTask(
    title: 'مهمة من Flutter',
    taskType: 'General',  // مطلوب!
    priority: 'High',
    startDate: '2025-12-05',
    description: 'تم إنشاؤها من التطبيق',
  );
  print('تم إنشاء المهمة: ${newTask['name']}');
  
  // تعديل الحالة
  await api.updateStatus(
    taskId: newTask['name'],
    status: 'In Progress',
  );
  print('تم تغيير الحالة');
  
  // تسجيل الخروج
  await oauthManager.logout();
  print('تم تسجيل الخروج');
}
```

---

## 📊 شكل البيانات

### Task Object

```dart
class Task {
  final int name;           // Task ID
  final String title;       // عنوان المهمة
  final String status;      // Open, In Progress, Completed, Cancelled
  final String priority;    // Low, Medium, High
  final String? startDate;  // YYYY-MM-DD
  final String modified;    // آخر تعديل
  final String? dueDate;    // YYYY-MM-DD HH:MM:SS
  final String? assignedTo; // البريد الإلكتروني
  
  Task.fromJson(Map<String, dynamic> json)
      : name = json['name'],
        title = json['title'],
        status = json['status'],
        priority = json['priority'],
        startDate = json['start_date'],
        modified = json['modified'],
        dueDate = json['due_date'],
        assignedTo = json['assigned_to'];
}
```

### القيم المسموحة

**Status:**
- `Open` - مفتوحة
- `In Progress` - قيد التنفيذ
- `Completed` - مكتملة
- `Cancelled` - ملغاة

**Priority:**
- `Low` - منخفضة
- `Medium` - متوسطة
- `High` - عالية

**Task Type (مطلوب للإنشاء):**
- `General` - عامة
- أو أي نوع آخر موجود في النظام

---

## ⚠️ ملاحظات مهمة

### 1. **Task Type مطلوب**

عند إنشاء مهمة جديدة، لازم تمرر `task_type`:

```dart
await api.createTask(
  title: 'مهمة جديدة',
  taskType: 'General',  // مطلوب!
);
```

### 2. **Token Expiry**

الـ access token ينتهي بعد **ساعة واحدة**. الـ `OAuthManager` بيجدده تلقائياً.

### 3. **Refresh Token Rotation**

الـ refresh token **بيتغير** مع كل تحديث. احفظ الجديد دايماً.

### 4. **Secure Storage**

استخدم `flutter_secure_storage` لحفظ الـ tokens (مش SharedPreferences!).

### 5. **Error Handling**

معالجة الأخطاء:

```dart
try {
  final tasks = await api.getHomeTasks();
} on DioException catch (e) {
  if (e.response?.statusCode == 401) {
    // Token منتهي - جدد أو أعد تسجيل الدخول
    await oauthManager.refreshAccessToken();
  } else {
    print('Error: ${e.message}');
  }
}
```

---

## 🔒 الأمان

### ✅ افعل:
- استخدم `flutter_secure_storage` للـ tokens
- جدد الـ token قبل انتهائه بـ 5 دقائق
- احفظ الـ refresh token الجديد دايماً
- استخدم HTTPS فقط

### ❌ لا تفعل:
- لا تحفظ الـ tokens في SharedPreferences
- لا تطبع الـ tokens في الـ console في الإنتاج
- لا تشارك الـ tokens بين المستخدمين
- لا تتجاهل أخطاء الـ 401 (Unauthorized)

---

## 🧪 اختبار سريع

### من Terminal (curl):

```bash
# 1. Get Token
TOKEN_RESPONSE=$(curl -s -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "username=Administrator" \
  -d "password=1234" \
  -d "client_id=3rcioodn8t" \
  -d "scope=all openid")

ACCESS_TOKEN=$(echo $TOKEN_RESPONSE | jq -r '.access_token')
echo "Token: $ACCESS_TOKEN"

# 2. Test API
curl "https://trust.jossoor.org/api/method/crm.api.mobile_api.home_tasks?limit=5" \
  -H "Authorization: Bearer $ACCESS_TOKEN"

# 3. Refresh
REFRESH_TOKEN=$(echo $TOKEN_RESPONSE | jq -r '.refresh_token')
curl -s -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=refresh_token" \
  -d "refresh_token=$REFRESH_TOKEN" \
  -d "client_id=3rcioodn8t"
```

---

## 📞 الدعم

### الوثائق الكاملة:
- `docs/OAUTH2_SETUP_AND_OPERATIONS.md` - دليل OAuth2
- `docs/CRM_MOBILE_API_REFERENCE.md` - مرجع API

### معلومات الاتصال:
- **Base URL**: https://trust.jossoor.org
- **Client ID**: `3rcioodn8t`
- **OAuth Endpoints**:
  - Token: `/api/method/frappe.integrations.oauth2.get_token`
  - Authorize: `/api/method/frappe.integrations.oauth2.authorize`

---

## ✅ الخلاصة

- ✅ OAuth2 شغال 100%
- ✅ Password Grant شغال
- ✅ Refresh Token شغال
- ✅ كل الـ 7 endpoints شغالة
- ✅ كود Flutter جاهز
- ✅ Token management تلقائي
- ✅ Secure storage

**جاهز للتطوير! 🚀**

---

**ملاحظة أخيرة**: `task_type` مطلوب عند إنشاء مهمة جديدة. استخدم `"General"` أو أي نوع موجود في النظام.

