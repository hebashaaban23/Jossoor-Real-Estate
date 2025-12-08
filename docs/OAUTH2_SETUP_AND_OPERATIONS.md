# OAuth2 Setup and Operations Guide

**Version:** 1.0  
**Last Updated:** December 2025  
**Audience:** System Operators, DevOps Engineers

---

## Table of Contents

1. [Overview](#overview)
2. [What's Installed](#whats-installed)
3. [Multi-Site Architecture](#multi-site-architecture)
4. [OAuth2 Endpoints](#oauth2-endpoints)
5. [Installation and Setup](#installation-and-setup)
6. [Bootstrap Commands](#bootstrap-commands)
7. [OAuth2 Flow Examples](#oauth2-flow-examples)
8. [CORS and CSRF Configuration](#cors-and-csrf-configuration)
9. [Troubleshooting](#troubleshooting)
10. [Security Best Practices](#security-best-practices)

---

## Overview

This OAuth2 implementation enables secure, token-based authentication for the CRM Mobile API. It supports:

- **Authorization Code Grant with PKCE** (recommended for mobile apps)
- **Password Grant** (Resource Owner Password Credentials)
- **Refresh Token Grant** (token renewal)

All existing API endpoints under `crm.api.mobile_api.*` remain **unchanged** and continue to work with both OAuth tokens and legacy session cookies.

---

## What's Installed

The OAuth2 setup includes:

### 1. **OAuth Client** (per site)
- App Name: `Mobile App`
- Redirect URIs:
  - `app.trust://oauth2redirect` (mobile deep link)
  - `https://trust.jossoor.org/oauth/callback` (web test)
- Scopes: `all openid`
- Grant Types: Authorization Code (PKCE), Password, Refresh Token

### 2. **Bootstrap Modules**
- `crm/setup/config.py` - Configuration constants
- `crm/setup/oauth_bootstrap.py` - Bootstrap logic
- `crm/install.py` - Installation hooks
- `crm/patches/v1_0/ensure_mobile_oauth_and_tokens.py` - Migration patch

### 3. **Scripts**
- `apps/crm/scripts/bootstrap_all_sites.sh` - Bootstrap all sites

### 4. **Optional: User API Keys**
- Generated for users with roles: Sales User, Sales Manager, System Manager
- Skips: Administrator, Guest

---

## Multi-Site Architecture

### How It Works

In a Frappe multi-site bench:
- Each site is **independent** with its own database
- Each site gets its **own OAuth Client** with the same configuration
- Domain → Site mapping is handled by Nginx/Frappe routing

### Example Setup

```
Bench: /home/frappe/frappe-bench-env/frappe-bench
Sites:
  - Trust.com (primary domain: trust.jossoor.org)
  - Site2.com (domain: site2.example.com)
  - ...
```

Each site will have:
- Its own `client_id` and `client_secret`
- Same redirect URIs (configurable per site if needed)
- Independent user database and tokens

---

## OAuth2 Endpoints

Frappe provides built-in OAuth2 endpoints:

### 1. Authorization Endpoint

```
GET /api/method/frappe.integrations.oauth2.authorize
```

**Parameters:**
- `client_id` (required) - OAuth Client ID
- `response_type` (required) - Set to `code`
- `redirect_uri` (required) - Must match configured redirect URI
- `scope` (optional) - Default: `all`
- `state` (recommended) - CSRF protection token
- **PKCE Parameters:**
  - `code_challenge` (required) - SHA256 hash of code_verifier
  - `code_challenge_method` (required) - Set to `S256`

**Example:**
```bash
https://trust.jossoor.org/api/method/frappe.integrations.oauth2.authorize?\
  client_id=abc123&\
  response_type=code&\
  redirect_uri=app.trust://oauth2redirect&\
  scope=all%20openid&\
  state=random_state_token&\
  code_challenge=BASE64URL(SHA256(code_verifier))&\
  code_challenge_method=S256
```

### 2. Token Endpoint

```
POST /api/method/frappe.integrations.oauth2.get_token
```

**Content-Type:** `application/x-www-form-urlencoded`

#### A. Authorization Code Grant (with PKCE)

```bash
curl -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=authorization_code" \
  -d "code=AUTH_CODE_FROM_CALLBACK" \
  -d "redirect_uri=app.trust://oauth2redirect" \
  -d "client_id=YOUR_CLIENT_ID" \
  -d "code_verifier=ORIGINAL_CODE_VERIFIER"
```

**Response:**
```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "abc123refresh...",
  "scope": "all openid"
}
```

#### B. Password Grant (Resource Owner)

```bash
curl -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "username=user@example.com" \
  -d "password=user_password" \
  -d "client_id=YOUR_CLIENT_ID" \
  -d "scope=all openid"
```

**Response:**
```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "abc123refresh...",
  "scope": "all openid"
}
```

#### C. Refresh Token Grant

```bash
curl -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=refresh_token" \
  -d "refresh_token=YOUR_REFRESH_TOKEN" \
  -d "client_id=YOUR_CLIENT_ID"
```

**Response:**
```json
{
  "access_token": "NEW_ACCESS_TOKEN...",
  "token_type": "Bearer",
  "expires_in": 3600,
  "refresh_token": "NEW_REFRESH_TOKEN...",
  "scope": "all openid"
}
```

### 3. Revoke Token Endpoint (Optional)

```
POST /api/method/frappe.integrations.oauth2.revoke_token
```

```bash
curl -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.revoke_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "token=TOKEN_TO_REVOKE" \
  -d "client_id=YOUR_CLIENT_ID"
```

---

## Installation and Setup

### Fresh Installation

When installing the CRM app on a new site:

1. **Install the app:**
   ```bash
   bench --site Trust.com install-app crm
   ```

2. **OAuth Client is created automatically** via the `after_install` hook.

3. **Verify installation:**
   ```bash
   bench --site Trust.com console
   ```
   ```python
   import frappe
   frappe.db.get_value("OAuth Client", {"app_name": "Mobile App"}, ["client_id", "app_name"])
   ```

### Existing Sites (Migration)

For sites where CRM is already installed:

1. **Run migration:**
   ```bash
   bench --site Trust.com migrate
   ```

2. **The patch `ensure_mobile_oauth_and_tokens` runs automatically** and creates/updates the OAuth Client.

3. **Verify:**
   ```bash
   bench --site Trust.com console
   ```
   ```python
   from crm.setup.oauth_bootstrap import bootstrap_site
   result = bootstrap_site()
   print(result)
   ```

---

## Bootstrap Commands

### Single Site Bootstrap

#### Command Line (via bench console)

```bash
bench --site Trust.com console
```

```python
from crm.setup.oauth_bootstrap import bootstrap_site

# Basic bootstrap (no secrets printed)
result = bootstrap_site()
print(result)

# With client_secret (use with caution!)
result = bootstrap_site(print_client_secret=1)
print(result)

# With user API key generation
result = bootstrap_site(generate_user_keys=1)
print(result)

# Full bootstrap with secrets and user keys
result = bootstrap_site(print_client_secret=1, generate_user_keys=1)
print(result)
```

#### Via Whitelisted API (requires authentication)

```bash
curl -X POST "https://trust.jossoor.org/api/method/crm.setup.oauth_bootstrap.bootstrap_site" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"print_client_secret": 1, "generate_user_keys": 1}'
```

### All Sites Bootstrap

Use the provided script to bootstrap all sites in the bench:

```bash
cd /home/frappe/frappe-bench-env/frappe-bench

# Without secrets
./apps/crm/scripts/bootstrap_all_sites.sh

# With secrets (use with caution!)
./apps/crm/scripts/bootstrap_all_sites.sh --print-secrets
```

**Output Example:**
```
═══════════════════════════════════════════════════════════════
  OAuth2 Bootstrap for All Sites
═══════════════════════════════════════════════════════════════

Found 2 site(s): Trust.com Site2.com

─────────────────────────────────────────────────────────────
Processing site: Trust.com
─────────────────────────────────────────────────────────────

✅ SUCCESS for Trust.com

{
  "ok": true,
  "site": "Trust.com",
  "client_id": "abc123xyz789",
  "redirect_uris": [
    "app.trust://oauth2redirect",
    "https://trust.jossoor.org/oauth/callback"
  ],
  "grant_types": [
    "authorization_code",
    "password",
    "refresh_token"
  ],
  "message": "OAuth Client configured successfully"
}

...
```

### Restart Services

After bootstrap, restart Frappe services:

```bash
# If using supervisorctl
sudo supervisorctl restart frappe-bench-frappe-web:*
sudo supervisorctl restart frappe-bench-frappe-worker:*

# Or restart all bench processes
bench restart
```

---

## OAuth2 Flow Examples

### Example 1: Password Grant (Simple Testing)

```bash
# Step 1: Get access token
curl -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "username=user@example.com" \
  -d "password=SecurePass123" \
  -d "client_id=abc123xyz789" \
  -d "scope=all openid"

# Response:
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "refresh_token": "def456uvw...",
  "expires_in": 3600,
  "token_type": "Bearer"
}

# Step 2: Call Mobile API
curl "https://trust.jossoor.org/api/method/crm.api.mobile_api.home_tasks?limit=5" \
  -H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGc..."

# Response:
{
  "message": {
    "today": [
      {
        "name": 123,
        "title": "Call client",
        "status": "Open",
        "priority": "High",
        "start_date": "2025-12-03",
        "modified": "2025-12-03 14:30:00"
      }
    ],
    "limit": 5
  }
}
```

### Example 2: Refresh Token

```bash
# When access token expires, use refresh token
curl -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=refresh_token" \
  -d "refresh_token=def456uvw..." \
  -d "client_id=abc123xyz789"

# Response: New access token and refresh token
{
  "access_token": "NEW_TOKEN...",
  "refresh_token": "NEW_REFRESH...",
  "expires_in": 3600,
  "token_type": "Bearer"
}
```

### Example 3: Authorization Code with PKCE (Mobile App)

**Step 1: Generate PKCE parameters (client-side)**

```python
import hashlib
import base64
import secrets

# Generate code verifier (43-128 characters)
code_verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode('utf-8').rstrip('=')

# Generate code challenge
code_challenge = base64.urlsafe_b64encode(
    hashlib.sha256(code_verifier.encode()).digest()
).decode('utf-8').rstrip('=')
```

**Step 2: Redirect user to authorization page**

```
https://trust.jossoor.org/api/method/frappe.integrations.oauth2.authorize?
  client_id=abc123xyz789&
  response_type=code&
  redirect_uri=app.trust://oauth2redirect&
  scope=all%20openid&
  state=random_csrf_token&
  code_challenge=CODE_CHALLENGE_FROM_STEP1&
  code_challenge_method=S256
```

**Step 3: User authorizes → redirected to app**

```
app.trust://oauth2redirect?code=AUTH_CODE&state=random_csrf_token
```

**Step 4: Exchange code for tokens**

```bash
curl -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=authorization_code" \
  -d "code=AUTH_CODE" \
  -d "redirect_uri=app.trust://oauth2redirect" \
  -d "client_id=abc123xyz789" \
  -d "code_verifier=ORIGINAL_CODE_VERIFIER"
```

---

## CORS and CSRF Configuration

### CORS (Cross-Origin Resource Sharing)

If your mobile app or web client runs from a different origin, you may need to configure CORS:

**File:** `sites/Trust.com/site_config.json`

```json
{
  "allow_cors": "*",
  "cors_allow_credentials": true,
  "allow_cors_for_methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  "allow_cors_for_headers": ["Authorization", "Content-Type", "X-Frappe-CSRF-Token"]
}
```

**Or via bench:**

```bash
bench --site Trust.com set-config allow_cors '*'
bench --site Trust.com set-config cors_allow_credentials true
```

**Production:** Replace `*` with specific origins:

```json
{
  "allow_cors": ["https://app.trust.com", "https://mobile.trust.com"]
}
```

### CSRF Token Exemption

OAuth2 token endpoints should already be CSRF-exempt. If you encounter CSRF errors:

**Check:** `sites/Trust.com/site_config.json`

```json
{
  "ignore_csrf": [
    "/api/method/frappe.integrations.oauth2.get_token",
    "/api/method/frappe.integrations.oauth2.authorize",
    "/api/method/frappe.integrations.oauth2.revoke_token"
  ]
}
```

---

## Troubleshooting

### Problem: "OAuth Client not found"

**Symptoms:**
- Error when calling `/api/method/frappe.integrations.oauth2.authorize`
- Client ID not recognized

**Solutions:**
1. Verify OAuth Client exists:
   ```bash
   bench --site Trust.com console
   ```
   ```python
   frappe.db.get_all("OAuth Client", fields=["name", "app_name", "client_id"])
   ```

2. Re-run bootstrap:
   ```python
   from crm.setup.oauth_bootstrap import bootstrap_site
   bootstrap_site(print_client_secret=1)
   ```

### Problem: "401 Unauthorized" with Bearer token

**Symptoms:**
- API returns 401 even with valid access token
- Token appears valid but access denied

**Solutions:**
1. **Check token expiry:**
   ```bash
   # Decode JWT to check expiration (use jwt.io or jwt-cli)
   ```

2. **Verify Authorization header format:**
   ```
   Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGc...
   ```
   (Note the space after "Bearer")

3. **Check user permissions:**
   ```python
   frappe.get_doc("User", "user@example.com").get_all_permissions()
   ```

4. **Try refreshing token:**
   ```bash
   curl -X POST ".../get_token" -d "grant_type=refresh_token" -d "refresh_token=..."
   ```

### Problem: "403 Forbidden - Function not whitelisted"

**Symptoms:**
- OAuth works, but API endpoints return 403
- Error: "Function crm.api.mobile_api.* is not whitelisted"

**Solutions:**
1. **Ensure imports are loaded:**
   ```bash
   bench restart
   ```

2. **Check whitelisting:**
   ```bash
   bench --site Trust.com console
   ```
   ```python
   import frappe
   frappe.get_attr("crm.api.mobile_api.home_tasks")._is_whitelisted
   # Should return True
   ```

3. **Verify app is installed:**
   ```bash
   bench --site Trust.com list-apps
   ```

### Problem: CORS errors in browser

**Symptoms:**
- Browser console shows CORS policy error
- Preflight OPTIONS requests fail

**Solutions:**
1. **Configure CORS** (see [CORS Configuration](#cors-and-csrf-configuration))

2. **Restart services:**
   ```bash
   bench restart
   ```

3. **Check Nginx headers** (if using reverse proxy):
   ```nginx
   add_header Access-Control-Allow-Origin "https://app.trust.com";
   add_header Access-Control-Allow-Methods "GET, POST, OPTIONS";
   add_header Access-Control-Allow-Headers "Authorization, Content-Type";
   add_header Access-Control-Allow-Credentials "true";
   ```

### Problem: "Invalid client_id" during token exchange

**Symptoms:**
- Authorization works, but token exchange fails
- Error: "client_id does not match"

**Solutions:**
1. **Verify you're using the same client_id** in both authorize and token requests

2. **Check redirect_uri matches exactly:**
   ```python
   frappe.db.get_value("OAuth Client", {"app_name": "Mobile App"}, "redirect_uris")
   ```

3. **Re-bootstrap if client was recently updated:**
   ```python
   from crm.setup.oauth_bootstrap import bootstrap_site
   bootstrap_site()
   ```

### Problem: Multi-site domain mapping not working

**Symptoms:**
- Wrong site responds to OAuth requests
- Domain routes to incorrect site

**Solutions:**
1. **Check site mapping:**
   ```bash
   cat sites/currentsite.txt
   ```

2. **Verify Nginx configuration:**
   ```bash
   cat /etc/nginx/sites-enabled/frappe-bench*
   ```

3. **Use correct domain in requests:**
   ```
   https://trust.jossoor.org  → Trust.com site
   https://site2.example.com  → Site2.com site
   ```

---

## Security Best Practices

### 1. Protect Client Secrets
- Never expose `client_secret` in client-side code
- For mobile apps, use **PKCE** (no client secret needed)
- Store secrets in secure environment variables

### 2. Use PKCE for Mobile Apps
- Always use Authorization Code + PKCE for mobile/native apps
- Password grant should only be used for testing or trusted first-party apps

### 3. Limit Token Scope
- Request only necessary scopes
- Use `openid` scope for user identity
- Avoid `all` scope in production if possible

### 4. Token Rotation
- Implement refresh token rotation
- Set short expiry times for access tokens (default: 1 hour)
- Refresh tokens should have longer expiry (e.g., 30 days)

### 5. HTTPS Only
- Always use HTTPS in production
- Never send tokens over unencrypted connections
- Configure redirect URIs with HTTPS (except localhost for dev)

### 6. Rate Limiting
- Implement rate limiting on token endpoints
- Monitor for suspicious token request patterns
- Consider IP-based throttling

### 7. Audit Logs
- Enable Frappe audit logs
- Monitor OAuth token creation/usage
- Alert on unusual patterns

### 8. Regular Security Updates
- Keep Frappe and dependencies updated
- Review OAuth Client configurations periodically
- Rotate client secrets when compromised

---

## Appendix

### A. Configuration File Reference

**`crm/setup/config.py`**

```python
# OAuth Client Configuration
OAUTH_APP_NAME = "Mobile App"
DEFAULT_REDIRECT_URIS = [
    "app.trust://oauth2redirect",
    "https://trust.jossoor.org/oauth/callback",
]
DEFAULT_SCOPES = "all openid"
ALLOWED_ROLES = ["Sales User", "Sales Manager", "System Manager"]
```

Modify these values to customize OAuth setup for your environment.

### B. Useful Queries

**Get OAuth Client details:**
```sql
SELECT name, app_name, client_id, redirect_uris, scopes
FROM `tabOAuth Client`
WHERE app_name = 'Mobile App';
```

**Check active OAuth tokens:**
```sql
SELECT user, client, expiration_time, scope
FROM `tabOAuth Bearer Token`
WHERE status = 'Active'
ORDER BY creation DESC
LIMIT 10;
```

**List users with API keys:**
```sql
SELECT name, full_name, enabled
FROM `tabUser`
WHERE api_key IS NOT NULL;
```

### C. Related Documentation

- [Frappe OAuth2 Provider Documentation](https://frappeframework.com/docs/user/en/api/oauth)
- [RFC 6749 - OAuth 2.0 Framework](https://tools.ietf.org/html/rfc6749)
- [RFC 7636 - PKCE](https://tools.ietf.org/html/rfc7636)
- [CRM Mobile API Reference](./CRM_MOBILE_API_REFERENCE.md)

---

**Document Version:** 1.0  
**Last Updated:** December 2025  
**Maintainer:** Frappe CRM Team

