# 🧪 اختبار Refresh Token من Mac Terminal

## 🚀 الاستخدام السريع

### 1️⃣ نسخ السكريبت إلى Mac

```bash
# من Mac Terminal
cd ~/Desktop  # أو أي مجلد تفضله
```

ثم انسخ محتوى الملف `test_refresh_token_mac.sh` إلى ملف جديد:

```bash
nano test_refresh_token_mac.sh
# الصق المحتوى ثم احفظ (Ctrl+X, Y, Enter)
chmod +x test_refresh_token_mac.sh
```

### 2️⃣ تثبيت jq (إذا لم يكن مثبتاً)

```bash
brew install jq
```

### 3️⃣ تشغيل الاختبار

#### الطريقة الأولى: اختبار مع refresh token محدد
```bash
./test_refresh_token_mac.sh "LhZamWqtjtfGfIxRayGR7dPqJ4hDBN" "3rcioodn8t"
```

#### الطريقة الثانية: اختبار تلقائي (يحصل على token جديد)
```bash
./test_refresh_token_mac.sh
```

## 📋 أمثلة

### مثال 1: اختبار refresh token من التطبيق
```bash
# احصل على refresh token من التطبيق ثم:
./test_refresh_token_mac.sh "YOUR_REFRESH_TOKEN_HERE"
```

### مثال 2: اختبار مع بيانات مختلفة
```bash
OAUTH_USERNAME="test@trust.com" \
OAUTH_PASSWORD="test1234" \
./test_refresh_token_mac.sh
```

## ✅ النتيجة المتوقعة

### عند النجاح:
```
✅ SUCCESS! Refresh token validation works correctly!
   New Access Token: abc123...
   New Refresh Token: xyz789...
   Expires In: 3600 seconds
```

### عند الفشل:
```
❌ Error: invalid_grant
```

## 🔍 استكشاف الأخطاء

- **jq not found**: `brew install jq`
- **invalid_grant**: تأكد أن refresh token صحيح ولم ينتهي
- **Connection refused**: تأكد أن الموقع يعمل

## 📍 الإعدادات الافتراضية

- **الموقع**: `https://trust.jossoor.org`
- **Client ID**: `3rcioodn8t`
- **Username**: `Administrator`
- **Password**: `1234`

