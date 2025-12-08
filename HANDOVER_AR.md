# 📱 تسليم المشروع - Mobile API للـ Flutter

**التاريخ**: 3 ديسمبر 2025  
**الحالة**: ✅ الـ API شغال 100% على السيرفر  
**السيرفر**: https://trust.jossoor.org

---

## 🎯 الملخص السريع

تم بناء **7 endpoints** لإدارة المهام (CRM Tasks) عبر REST API.

### الـ Endpoints المتاحة:

1. **Login** - تسجيل الدخول
2. **home_tasks** - مهام اليوم للصفحة الرئيسية
3. **filter_tasks** - البحث والفلترة
4. **main_page_buckets** - تصنيف المهام (اليوم/متأخرة/قادمة)
5. **create_task** - إنشاء مهمة جديدة
6. **edit_task** - تعديل مهمة موجودة
7. **update_status** - تغيير حالة المهمة
8. **delete_task** - حذف مهمة

---

## 🔐 التسجيل (Login)

```bash
POST https://trust.jossoor.org/api/method/login

Body:
usr=Administrator
pwd=1234

Response:
{
  "message": "Logged In",
  "full_name": "Administrator"
}
```

**⚠️ مهم جداً**: احفظ الـ **cookies** من الاستجابة وأرسلها مع كل طلب!

---

## 📋 أمثلة الاستخدام

### 1. جلب مهام اليوم
```
GET /api/method/crm.api.mobile_api.home_tasks?limit=5
```

### 2. البحث بالتاريخ والأولوية
```
GET /api/method/crm.api.mobile_api.filter_tasks?date_from=2025-12-01&date_to=2025-12-31&importance=High
```

### 3. تصنيف المهام (اليوم/متأخرة/قادمة)
```
GET /api/method/crm.api.mobile_api.main_page_buckets?min_each=5
```

### 4. إنشاء مهمة جديدة
```
POST /api/method/crm.api.mobile_api.create_task

Body (JSON):
{
  "title": "مهمة جديدة",
  "status": "Open",
  "priority": "High",
  "start_date": "2025-12-05"
}
```

### 5. تعديل مهمة
```
POST /api/method/crm.api.mobile_api.edit_task

Body (JSON):
{
  "task_id": 123,
  "title": "مهمة معدلة",
  "status": "In Progress"
}
```

### 6. تغيير حالة المهمة فقط
```
POST /api/method/crm.api.mobile_api.update_status

Body (JSON):
{
  "task_id": 123,
  "status": "Completed"
}
```

### 7. حذف مهمة
```
POST /api/method/crm.api.mobile_api.delete_task

Body (JSON):
{
  "task_id": 123
}
```

---

## 📱 كود Flutter جاهز

### التثبيت
أضف في `pubspec.yaml`:
```yaml
dependencies:
  dio: ^5.4.0
  cookie_jar: ^4.0.8
  dio_cookie_manager: ^3.1.1
```

### الكود (انسخه كاملاً)

```dart
import 'package:dio/dio.dart';
import 'package:cookie_jar/cookie_jar.dart';
import 'package:dio_cookie_manager/dio_cookie_manager.dart';

class CRMApi {
  late Dio dio;
  final cookieJar = CookieJar();
  
  CRMApi() {
    dio = Dio(BaseOptions(
      baseUrl: 'https://trust.jossoor.org',
      connectTimeout: Duration(seconds: 30),
      receiveTimeout: Duration(seconds: 30),
    ));
    
    // تفعيل حفظ الـ cookies تلقائياً
    dio.interceptors.add(CookieManager(cookieJar));
  }
  
  // 1. تسجيل الدخول
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
      print('خطأ في تسجيل الدخول: $e');
      return false;
    }
  }
  
  // 2. جلب مهام اليوم
  Future<List<dynamic>> getHomeTasks({int limit = 5}) async {
    try {
      final response = await dio.get(
        '/api/method/crm.api.mobile_api.home_tasks',
        queryParameters: {'limit': limit},
      );
      
      return response.data['message']['today'] ?? [];
    } catch (e) {
      print('خطأ في جلب المهام: $e');
      return [];
    }
  }
  
  // 3. تصنيف المهام (اليوم/متأخرة/قادمة)
  Future<Map<String, dynamic>> getBuckets({int minEach = 5}) async {
    try {
      final response = await dio.get(
        '/api/method/crm.api.mobile_api.main_page_buckets',
        queryParameters: {'min_each': minEach},
      );
      
      return response.data['message'];
    } catch (e) {
      print('خطأ في جلب التصنيفات: $e');
      return {'today': [], 'late': [], 'upcoming': []};
    }
  }
  
  // 4. البحث والفلترة
  Future<List<dynamic>> filterTasks({
    String? dateFrom,
    String? dateTo,
    String? importance,
    String? status,
    int limit = 50,
  }) async {
    try {
      Map<String, dynamic> params = {'limit': limit};
      
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
      print('خطأ في البحث: $e');
      return [];
    }
  }
  
  // 5. إنشاء مهمة جديدة
  Future<Map<String, dynamic>?> createTask({
    required String title,
    String? status,
    String? priority,
    String? startDate,
    String? dueDate,
    String? description,
  }) async {
    try {
      Map<String, dynamic> data = {'title': title};
      
      if (status != null) data['status'] = status;
      if (priority != null) data['priority'] = priority;
      if (startDate != null) data['start_date'] = startDate;
      if (dueDate != null) data['due_date'] = dueDate;
      if (description != null) data['description'] = description;
      
      final response = await dio.post(
        '/api/method/crm.api.mobile_api.create_task',
        data: data,
      );
      
      return response.data['message'];
    } catch (e) {
      print('خطأ في إنشاء المهمة: $e');
      return null;
    }
  }
  
  // 6. تعديل مهمة
  Future<Map<String, dynamic>?> editTask({
    required int taskId,
    String? title,
    String? status,
    String? priority,
    String? description,
  }) async {
    try {
      Map<String, dynamic> data = {'task_id': taskId};
      
      if (title != null) data['title'] = title;
      if (status != null) data['status'] = status;
      if (priority != null) data['priority'] = priority;
      if (description != null) data['description'] = description;
      
      final response = await dio.post(
        '/api/method/crm.api.mobile_api.edit_task',
        data: data,
      );
      
      return response.data['message'];
    } catch (e) {
      print('خطأ في تعديل المهمة: $e');
      return null;
    }
  }
  
  // 7. تغيير الحالة فقط
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
      print('خطأ في تحديث الحالة: $e');
      return null;
    }
  }
  
  // 8. حذف مهمة
  Future<bool> deleteTask(int taskId) async {
    try {
      final response = await dio.post(
        '/api/method/crm.api.mobile_api.delete_task',
        data: {'task_id': taskId},
      );
      
      return response.data['message'].toString().contains('deleted');
    } catch (e) {
      print('خطأ في حذف المهمة: $e');
      return false;
    }
  }
}
```

---

## 🧪 طريقة الاستخدام

```dart
void main() async {
  final api = CRMApi();
  
  // 1. تسجيل الدخول
  bool loggedIn = await api.login('Administrator', '1234');
  
  if (loggedIn) {
    print('✅ تم تسجيل الدخول بنجاح');
    
    // 2. جلب مهام اليوم
    List tasks = await api.getHomeTasks(limit: 10);
    print('عدد المهام: ${tasks.length}');
    
    // 3. جلب التصنيفات
    Map buckets = await api.getBuckets();
    print('مهام اليوم: ${buckets['today'].length}');
    print('مهام متأخرة: ${buckets['late'].length}');
    print('مهام قادمة: ${buckets['upcoming'].length}');
    
    // 4. إنشاء مهمة جديدة
    var newTask = await api.createTask(
      title: 'مهمة تجريبية من Flutter',
      status: 'Open',
      priority: 'High',
      startDate: '2025-12-05',
    );
    
    if (newTask != null) {
      print('✅ تم إنشاء المهمة رقم: ${newTask['name']}');
      
      // 5. تعديل المهمة
      await api.editTask(
        taskId: newTask['name'],
        status: 'In Progress',
      );
      print('✅ تم تعديل المهمة');
      
      // 6. تغيير الحالة
      await api.updateStatus(
        taskId: newTask['name'],
        status: 'Completed',
      );
      print('✅ تم تغيير الحالة إلى مكتملة');
      
      // 7. حذف المهمة
      await api.deleteTask(newTask['name']);
      print('✅ تم حذف المهمة');
    }
  } else {
    print('❌ فشل تسجيل الدخول');
  }
}
```

---

## 📊 شكل البيانات المرجعة

### مثال Task Object:
```json
{
  "name": 123,
  "title": "عنوان المهمة",
  "status": "Open",
  "priority": "High",
  "start_date": "2025-12-05",
  "modified": "2025-12-03 14:30:00",
  "due_date": "2025-12-05 18:00:00",
  "assigned_to": "user@example.com",
  "description": "تفاصيل المهمة"
}
```

### القيم المسموحة:

**Status** (الحالة):
- `Open` - مفتوحة
- `In Progress` - قيد التنفيذ
- `Completed` - مكتملة
- `Cancelled` - ملغاة

**Priority** (الأولوية):
- `Low` - منخفضة
- `Medium` - متوسطة
- `High` - عالية

**Date Format** (صيغة التاريخ):
- تاريخ فقط: `YYYY-MM-DD` مثل `2025-12-05`
- تاريخ ووقت: `YYYY-MM-DD HH:MM:SS` مثل `2025-12-05 14:30:00`

---

## ⚠️ ملاحظات مهمة

### 1. الـ API شغال 100% ✅
تم اختبار كل الـ endpoints على السيرفر ومشتغلة بنجاح.

### 2. مشكلة الـ Cache ⚠️
إذا ظهر خطأ "not whitelisted":
- امسح cache التطبيق
- امسح الـ cookies
- سجل دخول من جديد
- جرّب من شبكة تانية

### 3. إدارة الـ Cookies 🍪
**ضروري جداً**: 
- استخدم `dio_cookie_manager` (موجود في الكود)
- لا تنسى حفظ cookies بعد الـ login
- أرسل cookies مع كل طلب

### 4. معالجة الأخطاء
كل endpoint ممكن يرجع error بهذا الشكل:
```json
{
  "exception": "رسالة الخطأ",
  "exc_type": "نوع الخطأ"
}
```

تحقق من وجود `exception` في الاستجابة.

---

## 🎯 خطوات البداية

### خطوة 1: تثبيت Packages
```bash
flutter pub add dio cookie_jar dio_cookie_manager
```

### خطوة 2: نسخ الكود
انسخ كلاس `CRMApi` كاملاً في ملف `api.dart`

### خطوة 3: جرّب الـ Login
```dart
final api = CRMApi();
bool loggedIn = await api.login('Administrator', '1234');
```

### خطوة 4: جرّب جلب البيانات
```dart
if (loggedIn) {
  List tasks = await api.getHomeTasks();
  print('عدد المهام: ${tasks.length}');
}
```

### خطوة 5: ابني الواجهة
استخدم البيانات المرجعة في بناء الـ UI

---

## 📁 الملفات المرفقة

1. **FLUTTER_DEVELOPER_HANDOVER.md** - الوثائق الكاملة (إنجليزي)
2. **HANDOVER_AR.md** - هذا الملف (عربي)
3. **POSTMAN_COLLECTION.json** - مجموعة Postman لكل الـ endpoints
4. **MOBILE_API_README.md** - التوثيق التفصيلي

---

## 🧪 اختبار سريع من Terminal

### 1. تسجيل الدخول
```bash
curl -X POST "https://trust.jossoor.org/api/method/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "usr=Administrator" \
  --data-urlencode "pwd=1234" \
  -c cookies.txt
```

### 2. جلب مهام اليوم
```bash
curl "https://trust.jossoor.org/api/method/crm.api.mobile_api.home_tasks?limit=5" \
  -b cookies.txt
```

إذا شفت JSON صالح، معناها الـ API شغال ✅

---

## 🔧 حل المشاكل

### مشكلة: "Function is not whitelisted"
**الحل**:
1. امسح الـ cache
2. امسح الـ cookies
3. سجل دخول من جديد

### مشكلة: "Unauthorized" أو "Session expired"
**الحل**: سجل دخول مرة أخرى

### مشكلة: بيانات فارغة
**الحل**:
- تأكد من وجود مهام في النظام
- تحقق من صلاحيات المستخدم
- جرّب تصغير نطاق التاريخ في الفلترة

---

## ✅ حالة الـ Endpoints

| Endpoint | الحالة | تم الاختبار |
|----------|--------|-------------|
| login | ✅ شغال | ✅ نعم |
| home_tasks | ✅ شغال | ✅ نعم |
| filter_tasks | ✅ شغال | ✅ نعم |
| main_page_buckets | ✅ شغال | ✅ نعم |
| create_task | ✅ شغال | ✅ نعم |
| edit_task | ✅ شغال | ✅ نعم |
| update_status | ✅ شغال | ✅ نعم |
| delete_task | ✅ شغال | ✅ نعم |

**آخر تحديث**: 3 ديسمبر 2025

---

## 📞 للدعم الفني

إذا واجهت أي مشكلة:
1. جرّب الـ endpoint من curl أولاً
2. تحقق من الـ cookies والـ authentication
3. شوف الـ error message بالضبط
4. تواصل مع فريق الـ Backend

---

## 🎯 الخلاصة

- ✅ **7 endpoints** جاهزة ومختبرة
- ✅ **كود Flutter جاهز** للنسخ واللصق
- ✅ **Cookies management** تلقائي
- ✅ **أمثلة واضحة** لكل endpoint
- ✅ **معالجة الأخطاء** موجودة

**كل شيء جاهز للبدء! بالتوفيق 🚀**

---

**تم التسليم بتاريخ**: 3 ديسمبر 2025  
**الحالة النهائية**: ✅ جاهز للتطوير

