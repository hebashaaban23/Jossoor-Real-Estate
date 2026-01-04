# Mobile API Test Suite - دليل الاختبارات للموبايل أبليكشن

## نظرة عامة

هذا الدليل يشرح كيفية استخدام ملفات الاختبار للموبايل أبليكشن (Flutter) لاختبار جميع الـ APIs.

## الملفات المتوفرة

### 1. `mobile_api_test.dart`
ملف الاختبارات الرئيسي الذي يستخدم Flutter test framework (`package:test`).

**المميزات:**
- ✅ استخدام Flutter test framework
- ✅ يمكن تشغيله بـ `flutter test`
- ✅ تقارير مفصلة
- ✅ تنظيف تلقائي للبيانات

**الاستخدام:**
```bash
# في Flutter project
flutter test mobile_api_test.dart
```

### 2. `mobile_api_test_helper.dart`
ملف مساعد يمكن استخدامه كـ standalone test runner أو من داخل الموبايل أبليكشن.

**المميزات:**
- ✅ يمكن تشغيله مباشرة من الموبايل أبليكشن
- ✅ تقارير بسيطة وواضحة
- ✅ لا يحتاج Flutter test framework
- ✅ مناسب للاختبارات اليدوية

**الاستخدام:**
```dart
// في Flutter app
import 'mobile_api_test_helper.dart';

void main() async {
  final runner = MobileAPITestRunner();
  await runner.runAllTests();
}
```

## التثبيت والإعداد

### 1. إضافة Dependencies

أضف هذه الـ packages إلى `pubspec.yaml`:

```yaml
dependencies:
  dio: ^5.0.0
  dio_cookie_manager: ^3.0.0
  cookie_jar: ^4.0.0

dev_dependencies:
  test: ^1.24.0
```

ثم قم بتثبيتها:
```bash
flutter pub get
```

### 2. تحديث الإعدادات

افتح `mobile_api_test.dart` أو `mobile_api_test_helper.dart` وحدّث:

```dart
// تحديث BASE_URL
const String BASE_URL = 'https://your-site.com';

// تحديث بيانات الدخول
const String TEST_USERNAME = 'user@example.com';
const String TEST_PASSWORD = 'your_password';
```

### 3. نسخ الملفات

انسخ الملفات إلى مشروع Flutter:

```bash
# نسخ ملف الاختبارات
cp mobile_api_test.dart /path/to/flutter/project/test/

# أو نسخ ملف المساعد
cp mobile_api_test_helper.dart /path/to/flutter/project/lib/
```

## كيفية الاستخدام

### الطريقة 1: استخدام Flutter Test Framework

```bash
# تشغيل جميع الاختبارات
flutter test test/mobile_api_test.dart

# تشغيل اختبارات محددة
flutter test test/mobile_api_test.dart --name "Task API Tests"

# تشغيل مع verbose output
flutter test test/mobile_api_test.dart -v
```

### الطريقة 2: استخدام Test Helper (من داخل App)

```dart
import 'package:your_app/mobile_api_test_helper.dart';

class TestScreen extends StatefulWidget {
  @override
  _TestScreenState createState() => _TestScreenState();
}

class _TestScreenState extends State<TestScreen> {
  bool isRunning = false;
  String output = '';

  Future<void> runTests() async {
    setState(() {
      isRunning = true;
      output = 'Starting tests...\n';
    });

    final runner = MobileAPITestRunner();
    
    // Capture output (you may need to modify runner to accept callback)
    await runner.runAllTests();

    setState(() {
      isRunning = false;
      output += '\nTests completed!';
    });
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: Text('API Tests')),
      body: Column(
        children: [
          ElevatedButton(
            onPressed: isRunning ? null : runTests,
            child: Text('Run Tests'),
          ),
          Expanded(
            child: SingleChildScrollView(
              child: Text(output),
            ),
          ),
        ],
      ),
    );
  }
}
```

### الطريقة 3: استخدام Test Helper (Standalone)

أنشئ ملف `test_runner.dart`:

```dart
import 'mobile_api_test_helper.dart';

void main() async {
  final runner = MobileAPITestRunner();
  await runner.runAllTests();
}
```

ثم شغّله:
```bash
dart test_runner.dart
```

## الاختبارات المتوفرة

### Task API Tests (7 tests)
- ✅ Create task - minimal
- ✅ Create task - full
- ✅ Edit task
- ✅ Update task status
- ✅ Get all tasks
- ✅ Get home tasks
- ✅ Get main page buckets
- ✅ Delete task

### Lead API Tests (7 tests)
- ✅ Create lead - minimal
- ✅ Create lead - full
- ✅ Edit lead
- ✅ Get all leads
- ✅ Get lead by ID
- ✅ Get home leads
- ✅ Delete lead

### Helper API Tests (4 tests)
- ✅ Get OAuth config
- ✅ Get app logo
- ✅ Get current user role
- ✅ Get team members

**المجموع: 18+ test case**

## مثال على النتائج

```
╔══════════════════════════════════════════════════════════════╗
║         CRM Mobile API - Test Suite Runner                  ║
╚══════════════════════════════════════════════════════════════╝

🔐 Logging in...
✅ Login successful

┌──────────────────────────────────────────────────────────────┐
│ Task API Tests                                                │
└──────────────────────────────────────────────────────────────┘

  ✅ Create Task - Minimal
  ✅ Create Task - Full
  ✅ Get All Tasks
  ✅ Get Home Tasks
  ✅ Get Main Page Buckets

┌──────────────────────────────────────────────────────────────┐
│ Lead API Tests                                                 │
└──────────────────────────────────────────────────────────────┘

  ✅ Create Lead - Minimal
  ✅ Create Lead - Full
  ✅ Get All Leads
  ✅ Get Lead By ID
  ✅ Get Home Leads

┌──────────────────────────────────────────────────────────────┐
│ Helper API Tests                                               │
└──────────────────────────────────────────────────────────────┘

  ✅ Get OAuth Config
  ✅ Get App Logo
  ✅ Get Current User Role
  ✅ Get Team Members

╔══════════════════════════════════════════════════════════════╗
║                    Test Summary                              ║
╚══════════════════════════════════════════════════════════════╝

✅ Passed: 18
❌ Failed: 0
📊 Total:  18

🎉 All tests passed!
```

## التخصيص

### إضافة اختبارات جديدة

في `mobile_api_test.dart`:

```dart
test('My Custom Test', () async {
  final result = await tester.someMethod();
  expect(result, isNotNull);
});
```

### تعديل Test Helper

في `mobile_api_test_helper.dart`:

```dart
Future<void> runCustomTests() async {
  await test('Custom Test', () async {
    // Your test code
  });
}
```

## استكشاف الأخطاء

### خطأ: "Login failed"
- ✅ تحقق من BASE_URL
- ✅ تحقق من بيانات الدخول
- ✅ تأكد من اتصال الإنترنت
- ✅ تحقق من صلاحيات المستخدم

### خطأ: "Connection timeout"
- ✅ تحقق من BASE_URL
- ✅ تحقق من إعدادات Firewall
- ✅ زد timeout في Dio configuration

### خطأ: "Task type not found"
- ✅ تأكد من وجود Task Types في النظام
- ✅ استخدم task type موجود فعلاً

### خطأ: "Permission denied"
- ✅ تأكد من أن المستخدم له صلاحيات Sales User أو Sales Manager
- ✅ تحقق من إعدادات الصلاحيات في Frappe

## نصائح للاستخدام

1. **اختبار تدريجي**: ابدأ باختبار واحد ثم زد تدريجياً
2. **بيانات اختبار**: استخدم بيانات اختبار منفصلة عن البيانات الحقيقية
3. **التنظيف**: الملفات تقوم بالتنظيف التلقائي، لكن تأكد من عدم وجود بيانات مهمة
4. **المراقبة**: راقب logs في Frappe أثناء الاختبارات
5. **التوثيق**: سجّل أي أخطاء أو ملاحظات أثناء الاختبار

## الدعم

للمساعدة:
1. راجع `mobile_api.py` لفهم الـ APIs
2. راجع `API_ENDPOINTS.md` للتفاصيل
3. تحقق من Frappe logs
4. راجع Flutter documentation للـ testing

## التطوير المستقبلي

يمكن إضافة:
- ✅ Performance tests
- ✅ Load tests
- ✅ UI integration tests
- ✅ Automated CI/CD tests
- ✅ Test reports generation

