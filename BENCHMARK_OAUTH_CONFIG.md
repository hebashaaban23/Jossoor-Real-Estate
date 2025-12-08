# 🎯 Benchmark.com - OAuth Configuration

**Date**: December 3, 2025  
**Status**: ✅ Active and Tested

---

## 📊 OAuth Configuration

```
Site Name:     Benchmark.com
Domain:        https://benchmark.jossoor.org
Client ID:     da2f1j4l9f
Client Secret: d34b08286c
Status:        ✅ Active
```

---

## ✅ Grant Types Enabled

- ✅ **Authorization Code** (with PKCE) - للموبايل
- ✅ **Password Grant** - للاختبار
- ✅ **Refresh Token** - لتجديد الـ Token

---

## 🔗 Redirect URIs

```
app.trust://oauth2redirect
https://trust.jossoor.org/oauth/callback
```

---

## 🧪 Test Results

### ✅ OAuth Flow Tested:

```
✅ Password Grant      → Working
✅ Access Token        → Working  
✅ API Call            → Working
✅ Refresh Token       → Working
```

**Test Date**: December 3, 2025  
**Test User**: Administrator

---

## 📱 للمطور Flutter

### Configuration للتطبيق:

```dart
// Benchmark.com Configuration
class BenchmarkConfig {
  static const String baseUrl = 'https://benchmark.jossoor.org';
  static const String clientId = 'da2f1j4l9f';
  static const String siteName = 'Benchmark';
}
```

### أو Multi-Site:

```dart
final sites = [
  SiteConfig(
    name: 'Trust',
    baseUrl: 'https://trust.jossoor.org',
    clientId: '3rcioodn8t',
  ),
  SiteConfig(
    name: 'Benchmark',
    baseUrl: 'https://benchmark.jossoor.org',
    clientId: 'da2f1j4l9f',  // ✅ Benchmark Client ID
  ),
];
```

---

## 🔐 OAuth Endpoints

### 1. Get Access Token (Password Grant)

```bash
curl -X POST "https://benchmark.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "username=user@benchmark.com" \
  -d "password=user_password" \
  -d "client_id=da2f1j4l9f" \
  -d "scope=all openid"
```

**Response:**
```json
{
  "access_token": "x0Lhzsd3B7IB9ZbF6cuZjw3jGwtdr9",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "TxLQODyx8ES0ds8NQJ3OmGygMXfV8q",
  "scope": "all openid"
}
```

### 2. Refresh Token

```bash
curl -X POST "https://benchmark.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=refresh_token" \
  -d "refresh_token=TxLQODyx8ES0ds8NQJ3OmGygMXfV8q" \
  -d "client_id=da2f1j4l9f"
```

### 3. Use Token to Call API

```bash
curl "https://benchmark.jossoor.org/api/method/crm.api.mobile_api.home_tasks?limit=5" \
  -H "Authorization: Bearer x0Lhzsd3B7IB9ZbF6cuZjw3jGwtdr9"
```

---

## 📋 Mobile API Endpoints (7 Total)

All endpoints available at: `https://benchmark.jossoor.org/api/method/crm.api.mobile_api.*`

| Endpoint | Method | Description |
|----------|--------|-------------|
| `home_tasks` | GET | Today's tasks |
| `filter_tasks` | GET | Search/filter tasks |
| `main_page_buckets` | GET | Tasks by time (today/late/upcoming) |
| `create_task` | POST | Create new task |
| `edit_task` | POST | Update task |
| `update_status` | POST | Change task status |
| `delete_task` | POST | Delete task |

---

## 💡 مثال Flutter كامل

```dart
import 'package:dio/dio.dart';

void main() async {
  final oauth = OAuthManager();
  
  // Configure for Benchmark.com
  oauth.baseUrl = 'https://benchmark.jossoor.org';
  oauth.clientId = 'da2f1j4l9f';
  
  // Login
  final loggedIn = await oauth.loginWithPassword(
    'user@benchmark.com',
    'password',
  );
  
  if (loggedIn) {
    print('✅ Logged in to Benchmark.com');
    
    // Create API client
    final api = CRMApi(oauth);
    
    // Get tasks
    final tasks = await api.getHomeTasks(limit: 10);
    print('Tasks: ${tasks['today'].length}');
    
    // Create task
    final task = await api.createTask(
      title: 'Test from Benchmark',
      taskType: 'General',
      priority: 'High',
    );
    print('Created task: ${task['name']}');
  }
}
```

---

## ⚠️ Important Notes

### 1. Client Secret Protection

The client secret (`d34b08286c`) should be kept secure:
- ✅ Use for server-side apps only
- ❌ Never expose in mobile app code
- ✅ For mobile apps, use PKCE (no secret needed)

### 2. Token Expiry

- Access tokens expire after **3600 seconds (1 hour)**
- Use refresh token to get new access token
- Refresh token also rotates with each refresh

### 3. Site Isolation

- Tokens from Benchmark.com **only work** on Benchmark.com
- Cannot use tokens from Trust.com on Benchmark.com
- Each site is completely independent

### 4. User Management

- Users on Benchmark.com are separate from other sites
- `user@benchmark.com` logs in to Benchmark.com only
- Use appropriate credentials for each site

---

## 🔧 Troubleshooting

### Problem: "Invalid client_id"

**Solution**: Make sure you're using `da2f1j4l9f` for Benchmark.com

### Problem: Token not working

**Solution**: 
1. Check you're calling `https://benchmark.jossoor.org` (not trust.jossoor.org)
2. Verify Authorization header: `Bearer <token>`
3. Check token hasn't expired

### Problem: "User not found"

**Solution**: User must exist on Benchmark.com site (not Trust.com)

---

## 📞 Support

For issues specific to Benchmark.com:

1. Verify OAuth Client exists:
   ```bash
   bench --site Benchmark.com console
   ```
   ```python
   import frappe
   client = frappe.db.get_value("OAuth Client", {"app_name": "Mobile App"}, ["client_id"])
   print(f"Client ID: {client}")
   ```

2. Re-bootstrap if needed:
   ```python
   from crm.setup.oauth_bootstrap import bootstrap_site
   result = bootstrap_site(print_client_secret=1)
   print(result)
   ```

---

## ✅ Verification Checklist

- [x] OAuth Client created
- [x] Client ID: da2f1j4l9f
- [x] Grant types enabled (Password, PKCE, Refresh)
- [x] Redirect URIs configured
- [x] Password grant tested ✅
- [x] Refresh token tested ✅
- [x] API calls tested ✅
- [x] Domain confirmed: benchmark.jossoor.org

---

**Configuration Complete!** 🎉

**Last Tested**: December 3, 2025  
**Status**: ✅ All tests passed

