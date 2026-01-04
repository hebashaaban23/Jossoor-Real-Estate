# 🧪 اختبار Refresh Token من Mac Terminal

## 📋 الوصف

سكريبت bash بسيط لاختبار refresh token من ترمينال Mac مباشرة بدون الحاجة إلى bench console.

## 🚀 الاستخدام

### الطريقة 1: اختبار مع refresh token محدد

```bash
./test_refresh_token_mac.sh "your_refresh_token_here" "3rcioodn8t"
```

**مثال:**
```bash
./test_refresh_token_mac.sh "LhZamWqtjtfGfIxRayGR7dPqJ4hDBN" "3rcioodn8t"
```

### الطريقة 2: اختبار تلقائي (يحصل على token جديد)

```bash
./test_refresh_token_mac.sh
```

سيقوم السكريبت تلقائياً بـ:
1. تسجيل الدخول باستخدام بيانات افتراضية
2. الحصول على refresh token
3. اختبار refresh token

### الطريقة 3: استخدام متغيرات البيئة

```bash
export OAUTH_USERNAME="test@trust.com"
export OAUTH_PASSWORD="test1234"
./test_refresh_token_mac.sh
```

## 📦 المتطلبات

1. **jq** - لتحليل JSON
   ```bash
   brew install jq
   ```

2. **curl** - موجود افتراضياً على Mac

## 📝 المعاملات

- `refresh_token` (اختياري): Refresh token للاختبار
- `client_id` (اختياري): Client ID (افتراضي: `3rcioodn8t`)

## 🔍 أمثلة الاستخدام

### مثال 1: اختبار refresh token محدد
```bash
cd /path/to/crm
./test_refresh_token_mac.sh "LhZamWqtjtfGfIxRayGR7dPqJ4hDBN"
```

### مثال 2: اختبار مع client ID مختلف
```bash
./test_refresh_token_mac.sh "your_token" "different_client_id"
```

### مثال 3: اختبار تلقائي مع بيانات مخصصة
```bash
OAUTH_USERNAME="test@trust.com" \
OAUTH_PASSWORD="test1234" \
./test_refresh_token_mac.sh
```

## ✅ النتائج المتوقعة

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

💡 Possible reasons:
   1. Refresh token has expired
   2. Refresh token is invalid or revoked
   3. Client ID mismatch
   4. Token not found in database
```

## 🐛 استكشاف الأخطاء

### خطأ: jq is not installed
```bash
brew install jq
```

### خطأ: invalid_grant
- تأكد أن refresh token صحيح
- تأكد أن token لم ينتهي صلاحيته (12 ساعة)
- تأكد أن client_id صحيح

### خطأ: Connection refused
- تأكد أن الموقع يعمل: `https://trust.jossoor.org`
- تحقق من الاتصال بالإنترنت

## 📍 الموقع الافتراضي

- **Site URL**: `https://trust.jossoor.org`
- **Client ID**: `3rcioodn8t`
- **Username**: `Administrator` (أو من `OAUTH_USERNAME`)
- **Password**: `1234` (أو من `OAUTH_PASSWORD`)

## 🔐 الأمان

⚠️ **تحذير**: لا تشارك refresh tokens أو passwords في الكود أو الـ commits!

## 📞 الدعم

إذا واجهت مشاكل:
1. تحقق من أن الموقع يعمل
2. تحقق من صحة refresh token
3. راجع سجلات الأخطاء في Frappe

