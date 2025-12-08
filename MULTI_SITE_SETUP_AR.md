# 🌐 إعداد Multi-Site للـ OAuth API

**التاريخ**: 3 ديسمبر 2025  
**الحالة**: Multi-Site Ready ✅

---

## 🎯 نعم، الـ API تشتغل على كل الـ Sites!

كل site في الـ bench يقدر يستخدم الـ OAuth API، بس لازم:
1. يكون CRM app مثبت عليه
2. تعمل bootstrap للـ OAuth Client

---

## 🏗️ **معمارية Multi-Site**

### كل Site مستقل تماماً:

```
Bench: /home/frappe/frappe-bench-env/frappe-bench
│
├── Trust.com
│   ├── Domain: trust.jossoor.org
│   ├── Client ID: 3rcioodn8t
│   ├── Database: منفصلة
│   └── Users: منفصلين
│
├── Site2.com  
│   ├── Domain: site2.example.com
│   ├── Client ID: xyz789abc (مختلف!)
│   ├── Database: منفصلة
│   └── Users: منفصلين
│
└── ... (باقي الـ sites)
```

### التوجيه (Routing):

```
trust.jossoor.org      → Trust.com site
site2.example.com      → Site2.com site
demo3.jossoor.org      → demo3.jossoor.org site
```

كل domain يروح لـ site معين، وكل site له OAuth Client خاص.

---

## 📋 **خطوات Setup لكل Site**

### الطريقة 1: Setup Site واحد (يدوي)

```bash
# اختر الـ site
bench use Trust.com

# تأكد أن CRM مثبت
bench --site Trust.com list-apps

# شغّل الـ bootstrap
bench --site Trust.com console
```

```python
from crm.setup.oauth_bootstrap import bootstrap_site

# بدون client_secret
result = bootstrap_site()
print(f"Site: {result['site']}")
print(f"Client ID: {result['client_id']}")

# مع client_secret (إذا احتجت)
result = bootstrap_site(print_client_secret=1)
print(f"Client Secret: {result['client_secret']}")
```

### الطريقة 2: Setup كل الـ Sites (أوتوماتيكي)

⚠️ **ملاحظة**: هيشتغل فقط على الـ sites اللي عليها CRM app

```bash
cd /home/frappe/frappe-bench-env/frappe-bench

# بدون secrets
./apps/crm/scripts/bootstrap_all_sites.sh

# مع secrets (حذر!)
./apps/crm/scripts/bootstrap_all_sites.sh --print-secrets
```

### الطريقة 3: Setup عند التثبيت

لما تثبت CRM على site جديد، الـ OAuth Client بيتنشئ تلقائياً:

```bash
# تثبيت CRM على site جديد
bench --site NewSite.com install-app crm

# OAuth Client بينشأ تلقائياً!
```

---

## 🧪 **اختبار OAuth لكل Site**

### Test Trust.com

```bash
curl -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "username=user@trust.com" \
  -d "password=password" \
  -d "client_id=3rcioodn8t" \
  -d "scope=all openid"
```

### Test Site2.com

```bash
curl -X POST "https://site2.example.com/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "username=user@site2.com" \
  -d "password=password" \
  -d "client_id=xyz789abc" \
  -d "scope=all openid"
```

**ملاحظة**: كل site له `client_id` مختلف!

---

## 📱 **للمطور Flutter: معالجة Multi-Site**

### مشكلة: كيف المستخدم يختار الـ Site؟

#### الحل 1: Base URL ثابت (site واحد)

إذا التطبيق لـ site واحد بس:

```dart
class Config {
  static const String baseUrl = 'https://trust.jossoor.org';
  static const String clientId = '3rcioodn8t';
}
```

#### الحل 2: Site Picker (multiple sites)

إذا التطبيق يدعم أكثر من site:

```dart
class SiteConfig {
  final String name;
  final String baseUrl;
  final String clientId;
  
  SiteConfig({
    required this.name,
    required this.baseUrl,
    required this.clientId,
  });
}

// قائمة الـ sites المتاحة
final availableSites = [
  SiteConfig(
    name: 'Trust',
    baseUrl: 'https://trust.jossoor.org',
    clientId: '3rcioodn8t',
  ),
  SiteConfig(
    name: 'Demo',
    baseUrl: 'https://demo3.jossoor.org',
    clientId: 'demo_client_id_here',
  ),
];

// UI لاختيار الـ Site
class SitePickerScreen extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return ListView.builder(
      itemCount: availableSites.length,
      itemBuilder: (context, index) {
        final site = availableSites[index];
        return ListTile(
          title: Text(site.name),
          subtitle: Text(site.baseUrl),
          onTap: () {
            // حفظ الـ site المختار
            _selectSite(site);
          },
        );
      },
    );
  }
}
```

#### الحل 3: Custom Domain Entry

المستخدم يدخل الـ domain بتاعه:

```dart
class OAuthManager {
  String? baseUrl;
  String? clientId;
  
  // Discover client_id من الـ server
  Future<void> discoverSite(String domain) async {
    baseUrl = 'https://$domain';
    
    // استدعاء endpoint للحصول على client_id
    // (يحتاج endpoint جديد على الـ server)
    final response = await dio.get(
      '$baseUrl/api/method/crm.setup.oauth_bootstrap.get_client_info'
    );
    
    clientId = response.data['client_id'];
  }
}
```

---

## 🔍 **معرفة الـ Sites اللي عليها CRM**

### من Terminal:

```bash
cd /home/frappe/frappe-bench-env/frappe-bench

# قائمة كل الـ sites
ls -1 sites/ | grep -v "assets\|common_site_config.json"

# فحص site معين
bench --site Trust.com list-apps | grep crm
```

### من Python:

```python
import frappe
import os

# قائمة كل الـ sites
bench_path = '/home/frappe/frappe-bench-env/frappe-bench'
sites_path = os.path.join(bench_path, 'sites')
sites = [d for d in os.listdir(sites_path) 
         if os.path.isdir(os.path.join(sites_path, d))
         and d not in ['assets', 'common_site_config.json']]

print(f"Found {len(sites)} sites:")
for site in sites:
    print(f"  - {site}")
```

---

## 📊 **مثال: Setup لثلاث Sites**

### Site 1: Trust.com

```bash
bench --site Trust.com console
```
```python
from crm.setup.oauth_bootstrap import bootstrap_site
result = bootstrap_site()
# Client ID: 3rcioodn8t
```

**API Base URL**: `https://trust.jossoor.org/api/method/crm.api.mobile_api.*`

### Site 2: demo3.jossoor.org

```bash
bench --site demo3.jossoor.org console
```
```python
from crm.setup.oauth_bootstrap import bootstrap_site
result = bootstrap_site()
# Client ID: abc456def (مثال)
```

**API Base URL**: `https://demo3.jossoor.org/api/method/crm.api.mobile_api.*`

### Site 3: Jossoor.com

```bash
bench --site Jossoor.com console
```
```python
from crm.setup.oauth_bootstrap import bootstrap_site
result = bootstrap_site()
# Client ID: xyz789ghi (مثال)
```

**API Base URL**: `https://jossoor.com/api/method/crm.api.mobile_api.*`

---

## ⚠️ **نقاط مهمة**

### 1. **Client ID مختلف لكل Site**

```
Trust.com        → client_id: 3rcioodn8t
demo3.jossoor.org → client_id: xyz789 (مختلف!)
Jossoor.com      → client_id: abc123 (مختلف!)
```

### 2. **Users منفصلين**

```
user@trust.com      → يسجل دخول على Trust.com فقط
user@demo.com       → يسجل دخول على demo3.jossoor.org فقط
```

### 3. **Tokens منفصلة**

```
Token من Trust.com     → يشتغل على Trust.com فقط
Token من demo3         → يشتغل على demo3 فقط
```

### 4. **Redirect URIs نفسها** (قابلة للتخصيص)

```
كل الـ sites:
  - app.trust://oauth2redirect
  - https://<domain>/oauth/callback
```

إذا احتجت redirect URIs مختلفة لكل site، عدّل في `crm/setup/config.py`.

---

## 🛠️ **Troubleshooting Multi-Site**

### مشكلة: "OAuth Client not found"

**السبب**: الـ site مش عليه CRM أو OAuth Client مش منشأ.

**الحل**:
```bash
# تأكد أن CRM مثبت
bench --site SiteName.com list-apps | grep crm

# شغّل bootstrap
bench --site SiteName.com console
from crm.setup.oauth_bootstrap import bootstrap_site
bootstrap_site()
```

### مشكلة: "Wrong site accessed"

**السبب**: Domain routing مش صح.

**الحل**: تحقق من site mapping في Nginx:
```bash
cat /etc/nginx/sites-enabled/frappe-bench* | grep -A 5 "server_name"
```

### مشكلة: "Token not working on Site B"

**السبب**: استخدمت token من Site A على Site B.

**الحل**: كل site يحتاج token خاص بيه:
```dart
// خطأ
final tokenFromSiteA = '...';
api.baseUrl = 'https://siteB.com';  // مش هيشتغل!

// صح
await oauthManager.loginWithPassword(
  'user@siteB.com', 
  'password',
  baseUrl: 'https://siteB.com',
  clientId: 'siteB_client_id',
);
```

---

## 📋 **Checklist للـ Multi-Site Setup**

### لكل Site:

- [ ] تأكد أن CRM app مثبت
- [ ] شغّل OAuth bootstrap
- [ ] سجّل الـ Client ID
- [ ] اختبر token endpoint
- [ ] اختبر API endpoints
- [ ] وثّق الـ Base URL و Client ID

### للمطور Flutter:

- [ ] قرر: single-site أو multi-site؟
- [ ] إذا multi-site: أضف site picker UI
- [ ] احفظ site config لكل site
- [ ] عالج site switching في التطبيق
- [ ] اختبر على كل site

---

## 🎯 **الخلاصة**

### ✅ نعم، الـ API تشتغل على كل الـ Sites!

**بس افتكر**:
- كل site له OAuth Client خاص
- كل site له client_id مختلف
- كل site له users ومهام منفصلة
- Tokens مش بتنتقل بين الـ sites

**للمطور Flutter**:
- إذا single-site: استخدم base URL ثابت
- إذا multi-site: أضف site picker
- احفظ config لكل site منفصل

---

## 📞 **للمساعدة**

### معرفة Client ID لأي Site:

```bash
bench --site SiteName.com console
```
```python
import frappe
client = frappe.db.get_value(
    "OAuth Client", 
    {"app_name": "Mobile App"}, 
    ["client_id"]
)
print(f"Client ID: {client}")
```

### Setup Site جديد:

```bash
# 1. إنشاء الـ site
bench new-site NewSite.com

# 2. تثبيت CRM
bench --site NewSite.com install-app crm

# 3. OAuth Client بينشأ تلقائياً!

# 4. اجلب الـ Client ID
bench --site NewSite.com console
from crm.setup.oauth_bootstrap import bootstrap_site
print(bootstrap_site()['client_id'])
```

---

**كل الـ Sites جاهزة! 🚀**

