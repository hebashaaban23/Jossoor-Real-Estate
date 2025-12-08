# 🔐 Trust.com - Updated Credentials

**Date**: December 3, 2025  
**Status**: ✅ Credentials Updated and Tested

---

## ⚠️ المشكلة

كنت تحصل على خطأ:
```json
{
  "message": "Invalid login credentials",
  "exception": "frappe.exceptions.AuthenticationError"
}
```

**السبب**: الـ password لم يكن `1234` كما كان متوقع.

---

## ✅ الحل

تم تحديث الـ credentials وإنشاء مستخدم اختبار جديد.

---

## 🔑 Credentials الصحيحة (Trust.com)

### الخيار 1️⃣: Administrator (تم التحديث)

```
Username: Administrator
Password: 1234
```

### الخيار 2️⃣: Test User (جديد)

```
Username: test@trust.com
Password: test1234
Role: System Manager
```

---

## 🧪 اختبار OAuth (مُحدّث)

### من Mac Terminal:

```bash
export BASE="https://trust.jossoor.org"
export CLIENT_ID="3rcioodn8t"
export USERNAME="Administrator"
export PASSWORD="1234"

# Get Token
curl -sS -X POST "$BASE/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=password" \
  --data-urlencode "username=$USERNAME" \
  --data-urlencode "password=$PASSWORD" \
  --data-urlencode "client_id=$CLIENT_ID" \
  --data-urlencode "scope=all openid" | jq .
```

**Expected Response:**
```json
{
  "access_token": "QGxVpz1NXT4pgGus1jcBczR7SaSEDF",
  "expires_in": 3600,
  "token_type": "Bearer",
  "scope": "all openid",
  "refresh_token": "3I4h2gPSPpqDjX7xWTrUCrWlET8HOH"
}
```

✅ **الآن يعمل!**

---

## 🎯 اختبار كامل

```bash
#!/bin/bash
BASE="https://trust.jossoor.org"
CLIENT_ID="3rcioodn8t"
USERNAME="Administrator"
PASSWORD="1234"

# Step 1: Get Token
echo "Getting access token..."
TOKEN_RESPONSE=$(curl -sS -X POST "$BASE/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=password" \
  --data-urlencode "username=$USERNAME" \
  --data-urlencode "password=$PASSWORD" \
  --data-urlencode "client_id=$CLIENT_ID" \
  --data-urlencode "scope=all openid")

ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.access_token')
echo "✅ Access Token: ${ACCESS_TOKEN:0:30}..."

# Step 2: Call API
echo ""
echo "Calling API..."
curl -sS "$BASE/api/method/crm.api.mobile_api.home_tasks?limit=5" \
  -H "Authorization: Bearer $ACCESS_TOKEN" | jq .

# Step 3: Refresh Token
echo ""
echo "Testing refresh token..."
REFRESH_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.refresh_token')
curl -sS -X POST "$BASE/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=refresh_token" \
  --data-urlencode "refresh_token=$REFRESH_TOKEN" \
  --data-urlencode "client_id=$CLIENT_ID" | jq .
```

---

## 📱 Flutter Code Update

### تحديث الكود:

```dart
// Trust.com Credentials (Updated)
class TrustConfig {
  static const String baseUrl = 'https://trust.jossoor.org';
  static const String clientId = '3rcioodn8t';
  
  // For testing (use secure storage in production!)
  static const String testUsername = 'Administrator';
  static const String testPassword = '1234';
  
  // Or use the test user
  static const String testUser = 'test@trust.com';
  static const String testUserPassword = 'test1234';
}
```

### مثال Login:

```dart
final oauth = OAuthManager();

// Option 1: Administrator
final loggedIn = await oauth.loginWithPassword(
  'Administrator',
  '1234',
);

// Option 2: Test User
final loggedIn = await oauth.loginWithPassword(
  'test@trust.com',
  'test1234',
);

if (loggedIn) {
  print('✅ Logged in successfully!');
}
```

---

## 🔒 Security Notes

### ⚠️ للإنتاج:

1. **لا تستخدم** `Administrator` في الإنتاج
2. **لا تحفظ** passwords في الكود
3. **استخدم** Flutter Secure Storage للـ credentials
4. **أنشئ** users مخصصين لكل مستخدم حقيقي

### ✅ Best Practice:

```dart
// Save credentials securely
final storage = FlutterSecureStorage();
await storage.write(key: 'username', value: username);
await storage.write(key: 'password', value: password);

// Read when needed
final savedUsername = await storage.read(key: 'username');
final savedPassword = await storage.read(key: 'password');
```

---

## 📊 Test Results (بعد التحديث)

### Trust.com:

```
✅ Administrator (1234)     → Working
✅ test@trust.com (test1234) → Working
✅ OAuth Token              → Working
✅ API Calls                → Working
✅ Refresh Token            → Working
```

---

## 🌐 ملخص كل الـ Sites

### 1. Trust.com
- **Domain**: https://trust.jossoor.org
- **Client ID**: `3rcioodn8t`
- **Users**:
  - Administrator / 1234 ✅
  - test@trust.com / test1234 ✅

### 2. Benchmark.com
- **Domain**: https://benchmark.jossoor.org
- **Client ID**: `da2f1j4l9f`
- **Users**:
  - Administrator / 1234 ✅

---

## 🔧 إذا احتجت تغيير Password

من السيرفر:

```bash
bench --site Trust.com console
```

```python
from frappe.utils.password import update_password
import frappe

# Update password
update_password("Administrator", "new_password")
frappe.db.commit()

print("✅ Password updated!")
```

---

## 📋 Quick Reference

| Site | Domain | Client ID | Username | Password |
|------|--------|-----------|----------|----------|
| Trust.com | trust.jossoor.org | 3rcioodn8t | Administrator | 1234 |
| Trust.com | trust.jossoor.org | 3rcioodn8t | test@trust.com | test1234 |
| Benchmark.com | benchmark.jossoor.org | da2f1j4l9f | Administrator | 1234 |

---

## ✅ الخلاصة

- ✅ تم تحديث الـ password لـ Trust.com
- ✅ تم إنشاء test user (test@trust.com)
- ✅ OAuth شغال الآن بنجاح
- ✅ كل الـ APIs شغالة

**جرّب الآن من Mac Terminal!** 🚀

---

**Updated**: December 3, 2025  
**Status**: ✅ Working - Credentials Verified

