# How to Retrieve OAuth Client Secret

**⚠️ SECURITY WARNING:** The client secret should only be retrieved when absolutely necessary and stored securely. For mobile apps, use **PKCE** (Authorization Code grant) which does NOT require a client secret.

---

## When You Need the Client Secret

You typically **DO NOT** need the client secret if:
- Building a **mobile app** (use PKCE instead)
- Building a **single-page web app** (use PKCE instead)

You **MAY** need the client secret if:
- Building a **confidential server-side application**
- Implementing **client credentials grant** (not enabled by default)

---

## Method 1: Via Bench Console (Secure)

```bash
# Set active site
bench use Trust.com

# Run console
bench --site Trust.com console
```

```python
from crm.setup.oauth_bootstrap import bootstrap_site

# Get client_id AND client_secret
result = bootstrap_site(print_client_secret=1)

print(f"Client ID: {result['client_id']}")
print(f"Client Secret: {result['client_secret']}")
```

---

## Method 2: Via Database Query (For Operators)

```bash
bench --site Trust.com console
```

```python
import frappe

# Get OAuth Client
client = frappe.get_doc("OAuth Client", {"app_name": "Mobile App"})

print(f"Client ID: {client.client_id}")
print(f"Client Secret: {client.get_password('client_secret')}")
```

---

## Method 3: Via All-Sites Bootstrap Script

```bash
cd /home/frappe/frappe-bench-env/frappe-bench

# This will print client_secret for ALL sites
./apps/crm/scripts/bootstrap_all_sites.sh --print-secrets
```

**⚠️ WARNING:** This prints secrets for all sites. Use with extreme caution!

---

## Method 4: Via Frappe UI (For Administrators)

1. Login to Frappe as Administrator
2. Go to: **Desk → Integrations → OAuth Client**
3. Open: **Mobile App**
4. Click on the password field for `client_secret`
5. Copy the revealed secret

---

## Storing the Client Secret Securely

### ✅ DO:
- Store in environment variables
- Use a secrets management system (Vault, AWS Secrets Manager, etc.)
- Encrypt before storing in config files
- Rotate periodically

### ❌ DON'T:
- Commit to version control (git)
- Store in plaintext config files
- Share via email or chat
- Hardcode in application code
- Log to console in production

---

## Using Client Secret (Server-Side Apps Only)

For server-side confidential clients using Authorization Code grant:

```bash
curl -X POST "https://trust.jossoor.org/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=authorization_code" \
  -d "code=AUTH_CODE" \
  -d "redirect_uri=https://your-app.com/callback" \
  -d "client_id=3rcioodn8t" \
  -d "client_secret=YOUR_CLIENT_SECRET"
```

---

## For Mobile Apps: Use PKCE Instead

Mobile apps should **NOT** use client secrets. Use PKCE:

```bash
# Step 1: Generate code_verifier and code_challenge (client-side)
code_verifier = random_string(43-128 chars)
code_challenge = base64url(sha256(code_verifier))

# Step 2: Authorization request (with code_challenge, NO client_secret)
GET /api/method/frappe.integrations.oauth2.authorize?
  client_id=3rcioodn8t&
  response_type=code&
  redirect_uri=app.trust://oauth2redirect&
  code_challenge=CODE_CHALLENGE&
  code_challenge_method=S256

# Step 3: Token exchange (with code_verifier, NO client_secret)
POST /api/method/frappe.integrations.oauth2.get_token
  grant_type=authorization_code&
  code=AUTH_CODE&
  redirect_uri=app.trust://oauth2redirect&
  client_id=3rcioodn8t&
  code_verifier=ORIGINAL_CODE_VERIFIER
```

---

## Client Secret Rotation

To rotate the client secret:

```bash
bench --site Trust.com console
```

```python
import frappe

# Get OAuth Client
client = frappe.get_doc("OAuth Client", {"app_name": "Mobile App"})

# Generate new secret
new_secret = frappe.generate_hash(length=15)
client.client_secret = new_secret
client.save(ignore_permissions=True)

frappe.db.commit()

print(f"New Client Secret: {new_secret}")
```

**Important:** Update all applications using the old secret immediately after rotation.

---

## Security Best Practices

1. **Treat client secrets like passwords**
   - Never expose publicly
   - Use secure transmission (HTTPS only)
   - Rotate regularly (e.g., every 90 days)

2. **Use PKCE for public clients**
   - Mobile apps
   - Single-page applications
   - Desktop applications

3. **Monitor for leaks**
   - Check logs for accidental secret exposure
   - Use secret scanning tools on repositories
   - Revoke immediately if compromised

4. **Limit secret access**
   - Only operators/admins should access
   - Use role-based access control
   - Audit secret retrieval

---

## Current Site Details

**Site:** Trust.com  
**Domain:** https://trust.jossoor.org  
**Client ID:** `3rcioodn8t`  
**Client Secret:** Use methods above to retrieve (never logged by default)

---

**Remember:** For mobile apps, you don't need the client secret - use PKCE! 🔒

