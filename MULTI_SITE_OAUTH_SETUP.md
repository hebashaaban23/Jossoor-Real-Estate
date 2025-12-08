# Multi-Site OAuth Configuration Setup Guide

## Overview

This guide explains how to configure OAuth settings for multiple Frappe sites to support a single mobile app across different companies/sites.

## What Was Implemented

### 1. Single DocType: "Mobile OAuth Settings"

A new Single DocType that stores OAuth configuration per site with the following fields:

- **Client ID** (Required): The OAuth 2.0 Client ID for the mobile app
- **Scope** (Default: "all openid"): OAuth scopes (space-separated)
- **Redirect URI** (Default: "app.trust://oauth2redirect"): OAuth redirect URI for mobile app

### 2. New API Endpoint: `get_oauth_config`

**Endpoint:** `GET /api/method/crm.api.mobile_api.get_oauth_config`

**Authentication:** None required (guest access allowed)

**Purpose:** Returns the OAuth configuration for the current site, allowing mobile apps to dynamically fetch credentials instead of hardcoding them.

## Setup Instructions

### Step 1: Install the DocType

After updating the CRM app, run migrations to create the new DocType:

```bash
cd /path/to/frappe-bench
bench --site [site-name] migrate
```

### Step 2: Configure OAuth Settings for Each Site

For each site, configure the Mobile OAuth Settings:

#### Option A: Using the Web UI

1. Login as Administrator or System Manager
2. Go to: **Settings → Mobile OAuth Settings**
3. Fill in the fields:
   - **Client ID**: Enter your OAuth Client ID (e.g., `3rcioodn8t`)
   - **Scope**: Leave default `all openid` or customize
   - **Redirect URI**: Leave default `app.trust://oauth2redirect` or customize
4. Click **Save**

#### Option B: Using Python Console

```bash
bench --site [site-name] console
```

```python
import frappe

# Get or create Mobile OAuth Settings
settings = frappe.get_single("Mobile OAuth Settings")
settings.client_id = "your_client_id_here"
settings.scope = "all openid"
settings.redirect_uri = "app.trust://oauth2redirect"
settings.save()
frappe.db.commit()

print(f"✅ Mobile OAuth Settings configured for {frappe.local.site}")
```

### Step 3: Verify Configuration

Test the endpoint using cURL or your browser:

```bash
# Replace with your site URL
curl "https://your-site.example.com/api/method/crm.api.mobile_api.get_oauth_config"
```

Expected response:
```json
{
  "message": {
    "client_id": "your_client_id_here",
    "scope": "all openid",
    "redirect_uri": "app.trust://oauth2redirect"
  }
}
```

## Multi-Site Setup Example

### Scenario: Supporting 3 Companies

You have one Flutter mobile app that needs to work with three different Frappe sites:

- **Company A:** `https://company-a.example.com`
- **Company B:** `https://company-b.example.com`
- **Company C:** `https://company-c.example.com`

### Configuration

For each site, configure different OAuth Client IDs:

**Company A:**
```python
settings = frappe.get_single("Mobile OAuth Settings")
settings.client_id = "client_id_company_a"
settings.save()
```

**Company B:**
```python
settings = frappe.get_single("Mobile OAuth Settings")
settings.client_id = "client_id_company_b"
settings.save()
```

**Company C:**
```python
settings = frappe.get_single("Mobile OAuth Settings")
settings.client_id = "client_id_company_c"
settings.save()
```

### Mobile App Flow

The mobile app implements this flow:

1. **User Input**: User enters their company site URL (e.g., `https://company-a.example.com`)
2. **Fetch Config**: App calls:
   ```
   GET https://company-a.example.com/api/method/crm.api.mobile_api.get_oauth_config
   ```
3. **Receive Config**: App receives:
   ```json
   {
     "message": {
       "client_id": "client_id_company_a",
       "scope": "all openid",
       "redirect_uri": "app.trust://oauth2redirect"
     }
   }
   ```
4. **Use Config**: App uses the received `client_id` for OAuth authentication:
   ```
   POST https://company-a.example.com/api/method/frappe.integrations.oauth2.get_token
   client_id=client_id_company_a&grant_type=password&username=...&password=...
   ```

## Benefits

✅ **Single App Binary**: One mobile app works with multiple sites  
✅ **No Hardcoding**: No need to rebuild the app for each company  
✅ **Centralized Config**: OAuth settings managed server-side  
✅ **Easy Updates**: Change OAuth credentials without app updates  
✅ **Secure**: Credentials never hardcoded in client code  

## Troubleshooting

### Error: "Mobile OAuth Settings not configured"

**Cause:** The Single DocType hasn't been configured for this site.

**Solution:**
1. Go to Settings → Mobile OAuth Settings
2. Enter the Client ID
3. Save

### Error: Empty client_id in response

**Cause:** The Client ID field is empty in Mobile OAuth Settings.

**Solution:**
1. Open Mobile OAuth Settings
2. Fill in the Client ID field
3. Save

### Cannot access Mobile OAuth Settings

**Cause:** Insufficient permissions.

**Solution:** Login as Administrator or System Manager.

## Security Considerations

1. **Guest Access**: The `get_oauth_config` endpoint allows guest access by design. It only returns the OAuth Client ID, which is not sensitive information.

2. **Client Secret**: If your OAuth configuration requires a client secret, do NOT add it to this DocType. Client secrets should only be used for confidential clients (backend services), not public clients (mobile apps).

3. **PKCE Flow**: For production mobile apps, use OAuth 2.0 Authorization Code flow with PKCE, which doesn't require a client secret.

## Migration from Hardcoded Client ID

If you're migrating from an app with hardcoded OAuth credentials:

### Old Flutter Code (Hardcoded):
```dart
final clientId = "3rcioodn8t"; // ❌ Hardcoded
```

### New Flutter Code (Dynamic):
```dart
// ✅ Fetch from server
Future<OAuthConfig> getOAuthConfig(String siteUrl) async {
  final response = await http.get(
    Uri.parse('$siteUrl/api/method/crm.api.mobile_api.get_oauth_config'),
  );
  
  final data = json.decode(response.body)['message'];
  return OAuthConfig(
    clientId: data['client_id'],
    scope: data['scope'],
    redirectUri: data['redirect_uri'],
  );
}
```

## Support

For issues or questions, refer to:
- API Documentation: `CRM_MOBILE_API_DOCUMENTATION.md`
- Frappe Framework Docs: https://frappeframework.com

---

**Version:** 1.0  
**Last Updated:** December 4, 2025  
**Maintained By:** CRM Development Team

