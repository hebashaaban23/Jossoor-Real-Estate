"""
Configuration constants for OAuth2 and Mobile API setup.

These constants are used during OAuth Client creation and token generation
for the CRM Mobile API across all sites in a multi-site bench.
"""

# OAuth Client Configuration
OAUTH_APP_NAME = "Mobile App"
OAUTH_CLIENT_NAME = "CRM Mobile OAuth Client"

# Redirect URIs (both mobile deep link and web callback)
DEFAULT_REDIRECT_URIS = [
    "app.trust://oauth2redirect",  # Mobile app deep link
    "https://trust.jossoor.org/oauth/callback",  # Web test callback
]

# Default redirect URI (used as primary)
DEFAULT_REDIRECT_URI = "app.trust://oauth2redirect"

# OAuth Scopes
DEFAULT_SCOPES = "all openid"

# Grant Types enabled
GRANT_TYPES = {
    "authorization_code": True,  # With PKCE
    "password": True,  # Resource Owner Password Credentials
    "refresh_token": True,  # Refresh token support
}

# Roles eligible for API key/secret generation
ALLOWED_ROLES = [
    "Sales User",
    "Sales Manager",
    "System Manager",
]

# Skip API key generation for system users
SKIP_USERS = [
    "Administrator",
    "Guest",
]

