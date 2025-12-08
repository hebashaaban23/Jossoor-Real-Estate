# 🔧 Mac OAuth Troubleshooting Guide

**Issue**: OAuth works from server but fails from Mac  
**Error**: "Invalid login credentials"  
**Status**: Server ✅ | Mac ❌

---

## ✅ Verified Working (From Server)

```bash
# This works from the server:
curl -sS -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=password" \
  --data-urlencode "username=Administrator" \
  --data-urlencode "password=1234" \
  --data-urlencode "client_id=3rcioodn8t" \
  --data-urlencode "scope=all openid"

# Response: ✅ Access token returned
```

---

## ❌ Problem (From Mac)

Same command fails with:
```json
{
  "message": "Invalid login credentials",
  "exception": "frappe.exceptions.AuthenticationError"
}
```

---

## 🔍 Root Cause Analysis

### Possible Causes:

1. **DNS/Routing Issue**
   - Mac DNS cache pointing to old/wrong server
   - Request going to different site in multi-site setup
   - CDN/Proxy intercepting requests

2. **Multi-Site Routing**
   - Request hitting wrong site
   - Domain mapping not working from external network

3. **Password/Auth Issue**
   - Password recently changed but not propagated
   - Auth cache on client side

---

## 🛠️ Solutions (Try in Order)

### Solution 1: Clear DNS Cache (Mac)

```bash
# Flush DNS cache
sudo dscacheutil -flushcache
sudo killall -HUP mDNSResponder

# Wait a few seconds, then test again
sleep 3

curl -sS -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=password" \
  --data-urlencode "username=Administrator" \
  --data-urlencode "password=1234" \
  --data-urlencode "client_id=3rcioodn8t" \
  --data-urlencode "scope=all openid" | jq .
```

### Solution 2: Use Test User

The test user was created successfully on the server:

```bash
curl -sS -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=password" \
  --data-urlencode "username=test@trust.com" \
  --data-urlencode "password=test1234" \
  --data-urlencode "client_id=3rcioodn8t" \
  --data-urlencode "scope=all openid" | jq .
```

### Solution 3: Verbose Debug

Get detailed info about the request:

```bash
curl -v -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=password" \
  --data-urlencode "username=Administrator" \
  --data-urlencode "password=1234" \
  --data-urlencode "client_id=3rcioodn8t" \
  --data-urlencode "scope=all openid" 2>&1 | grep -E "Host:|Server:|Location:|HTTP/"
```

**Look for**:
- HTTP redirects (301/302)
- Different server responding
- SSL certificate mismatches

### Solution 4: Check DNS Resolution

```bash
# Check where trust.jossoor.org points
nslookup trust.jossoor.org

# Expected: Should show correct server IP
```

**If DNS is wrong:**
```bash
# Option A: Wait for DNS propagation (can take hours)
# Option B: Add to /etc/hosts temporarily

# Get server IP from server side
# Then on Mac:
sudo echo "SERVER_IP trust.jossoor.org" >> /etc/hosts
```

### Solution 5: Bypass DNS with Direct IP

```bash
# First, get the server IP
SERVER_IP=$(dig +short trust.jossoor.org | head -1)

# Then make request with Host header
curl -sS -X POST "https://$SERVER_IP/api/method/frappe.integrations.oauth2.get_token" \
  -H "Host: trust.jossoor.org" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=password" \
  --data-urlencode "username=Administrator" \
  --data-urlencode "password=1234" \
  --data-urlencode "client_id=3rcioodn8t" \
  --data-urlencode "scope=all openid" \
  --insecure | jq .
```

**Note**: `--insecure` skips SSL verification (use only for testing!)

### Solution 6: Test with Postman

1. Open Postman
2. Create new POST request
3. URL: `https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token`
4. Body → `x-www-form-urlencoded`:
   ```
   grant_type: password
   username: Administrator
   password: 1234
   client_id: 3rcioodn8t
   scope: all openid
   ```
5. Send

**Advantages**:
- Can see full request/response
- Can disable SSL verification
- Can follow redirects

### Solution 7: Use VPN/Different Network

If you're behind a corporate firewall or VPN:

```bash
# Disconnect VPN
# Switch to mobile hotspot
# Try curl command again
```

Sometimes corporate networks have:
- DNS overrides
- SSL inspection
- Request filtering

---

## 🔬 Diagnostic Commands

### Check Current Credentials on Server

```bash
# SSH to server
ssh user@trust.jossoor.org

# Check Administrator user
bench --site Trust.com console << EOF
import frappe
admin = frappe.get_doc("User", "Administrator")
print(f"Enabled: {admin.enabled}")
print(f"Email: {admin.email}")
EOF
```

### Verify OAuth Client

```bash
# On server
bench --site Trust.com console << EOF
import frappe
client = frappe.db.get_value(
    "OAuth Client", 
    {"app_name": "Mobile App"}, 
    ["client_id", "app_name"],
    as_dict=True
)
print(client)
EOF
```

### Test Login Directly (No OAuth)

```bash
# Test basic login endpoint
curl -sS -X POST "https://trust.jossoor.org/api/method/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "usr=Administrator" \
  --data-urlencode "pwd=1234" \
  -c cookies.txt

# If this works, OAuth should work too
```

---

## 📊 Expected vs Actual

### Expected Response (Working):
```json
{
  "access_token": "wnz50UUxW033lNnwqIuhkkvGUZ6Js2",
  "expires_in": 3600,
  "token_type": "Bearer",
  "scope": "all openid",
  "refresh_token": "51M49aYKWzodtzYEvaXlWXuKjRuVKb"
}
```

### Actual Response (Mac):
```json
{
  "message": "Invalid login credentials",
  "exception": "frappe.exceptions.AuthenticationError"
}
```

---

## 🎯 Most Likely Solution

Based on the fact that it works from server but not Mac:

**Try Solution 2 (Test User) first:**

```bash
curl -sS -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=password" \
  --data-urlencode "username=test@trust.com" \
  --data-urlencode "password=test1234" \
  --data-urlencode "client_id=3rcioodn8t" \
  --data-urlencode "scope=all openid" | jq .
```

If this works, it confirms:
- OAuth is working
- DNS/routing is correct
- Only Administrator password has an issue

If this also fails:
- DNS cache problem (try Solution 1)
- Request going to wrong server (try Solution 5)

---

## 🆘 If Nothing Works

### Contact Support

Provide this information:

1. **DNS Resolution**:
   ```bash
   nslookup trust.jossoor.org
   ```

2. **Traceroute**:
   ```bash
   traceroute trust.jossoor.org
   ```

3. **Curl Headers**:
   ```bash
   curl -I https://trust.jossoor.org
   ```

4. **Network Environment**:
   - Behind corporate firewall? (Yes/No)
   - Using VPN? (Yes/No)
   - Location/Country

---

## ✅ Success Checklist

Once it works, you should see:

- [ ] `access_token` in response
- [ ] `refresh_token` in response
- [ ] `expires_in: 3600`
- [ ] No error messages
- [ ] Can call API with token

---

## 📝 Alternative: Use Session-Based Auth (Temporary)

If OAuth continues to fail, use legacy session cookies:

```bash
# Login
curl -sS -X POST "https://trust.jossoor.org/api/method/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "usr=test@trust.com" \
  --data-urlencode "pwd=test1234" \
  -c cookies.txt

# Call API with cookies
curl -sS "https://trust.jossoor.org/api/method/crm.api.mobile_api.home_tasks" \
  -b cookies.txt | jq .
```

This works without OAuth but proves:
- Authentication is working
- API is accessible
- Network/DNS is correct

---

**Last Updated**: December 3, 2025  
**Status**: Server ✅ | Mac troubleshooting in progress

