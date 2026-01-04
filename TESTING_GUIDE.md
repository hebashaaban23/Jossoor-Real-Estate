# دليل اختبار create_task API من Terminal

## ⚠️ المشكلة الشائعة: PermissionError

إذا ظهر لك هذا الخطأ:
```json
{
    "session_expired": 1,
    "exception": "frappe.exceptions.PermissionError: ...",
    "exc_type": "PermissionError"
}
```

## 🔍 الأسباب المحتملة:

### 1. Session Cookie منتهي
- الـ Session Cookie انتهت صلاحيته
- الحل: سجل دخول مرة أخرى واحصل على cookie جديد

### 2. المستخدم ليس لديه صلاحيات
- المستخدم يجب أن يكون لديه أحد الأدوار التالية:
  - `Sales User`
  - `Sales Manager`
  - `Sales Master Manager`
  - `System Manager`

### 3. الـ Workers لم يتم إعادة تحميلها
- بعد أي تغيير في الكود، يجب إعادة تحميل الـ workers

## ✅ الحلول:

### الحل 1: الحصول على Session Cookie جديد

**Chrome/Safari:**
1. افتح المتصفح → `https://trust.jossoor.org`
2. سجل دخول بحساب لديه صلاحيات
3. اضغط `Cmd+Option+I` (Mac) أو `F12`
4. اذهب إلى **Application** → **Cookies** → `https://trust.jossoor.org`
5. انسخ قيمة cookie اسمه **"sid"**

### الحل 2: استخدام Login API للحصول على Cookie

```bash
# 1. سجل دخول واحصل على Cookie
curl -X POST "https://trust.jossoor.org/api/method/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "usr=your_email@example.com" \
  --data-urlencode "pwd=your_password" \
  -c cookies.txt \
  -v

# 2. استخدم Cookie من ملف cookies.txt
curl -X POST "https://trust.jossoor.org/api/method/crm.api.mobile_api.create_task" \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{
    "task_type": "Meeting",
    "title": "Test Task"
  }' | python3 -m json.tool
```

### الحل 3: إعادة تحميل Workers (على السيرفر)

```bash
# على السيرفر
cd /home/frappe/frappe-bench-env/frappe-bench
bench restart

# أو
ps aux | grep gunicorn | grep -v grep | awk '{print $2}' | xargs kill -HUP
```

## 📋 مثال كامل مع Login:

```bash
#!/bin/bash

SITE_URL="https://trust.jossoor.org"
EMAIL="your_email@example.com"
PASSWORD="your_password"

# 1. Login واحصل على Cookie
echo "🔐 جاري تسجيل الدخول..."
LOGIN_RESPONSE=$(curl -s -X POST "${SITE_URL}/api/method/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "usr=${EMAIL}" \
  --data-urlencode "pwd=${PASSWORD}" \
  -c cookies.txt)

echo "Login Response: $LOGIN_RESPONSE"
echo ""

# 2. استخدم Cookie لإنشاء Task
echo "📋 جاري إنشاء Task..."
curl -X POST "${SITE_URL}/api/method/crm.api.mobile_api.create_task" \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{
    "task_type": "Meeting",
    "title": "Test Task from Script",
    "status": "Todo",
    "priority": "High",
    "description": "Created from terminal with login"
  }' | python3 -m json.tool

echo ""
echo "✅ انتهى!"
```

## 🔐 التحقق من الصلاحيات:

```bash
# تحقق من Role المستخدم الحالي
curl -X GET "https://trust.jossoor.org/api/method/crm.api.mobile_api.get_current_user_role" \
  -H "Cookie: sid=YOUR_SESSION_COOKIE_HERE" \
  | python3 -m json.tool
```

## 📝 ملاحظات مهمة:

1. **Session Cookie صالح لمدة محدودة** - إذا انتهت، سجل دخول مرة أخرى
2. **يجب أن يكون المستخدم لديه صلاحيات** - تحقق من Role
3. **بعد أي تغيير في الكود** - أعد تحميل الـ workers
4. **استخدم `-v` في curl** - لرؤية الـ headers والـ cookies

## 🚨 أخطاء شائعة:

### خطأ 1: session_expired
```json
{"session_expired": 1}
```
**الحل**: سجل دخول مرة أخرى

### خطأ 2: PermissionError
```json
{"exc_type": "PermissionError"}
```
**الحل**: 
- تحقق من Role المستخدم
- تأكد من أن المستخدم لديه أحد الأدوار المطلوبة

### خطأ 3: Method Not Allowed
```json
{"exc_type": "PermissionError", "message": "Function is not whitelisted"}
```
**الحل**: 
- أعد تحميل الـ workers
- تأكد من أن الملف `crm/api/__init__.py` يحتوي على `from . import mobile_api`

