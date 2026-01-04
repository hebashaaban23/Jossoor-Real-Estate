#!/bin/bash

# ============================================================================
# Test Refresh Token from Mac Terminal
# ============================================================================
# Usage:
#   ./test_refresh_token_mac.sh [refresh_token] [client_id]
#
# Examples:
#   # Test with specific refresh token
#   ./test_refresh_token_mac.sh "your_refresh_token_here" "3rcioodn8t"
#
#   # Test with new login (will get token automatically)
#   ./test_refresh_token_mac.sh
# ============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Site URL
SITE_URL="https://trust.jossoor.org"
TOKEN_ENDPOINT="${SITE_URL}/api/method/frappe.integrations.oauth2.get_token"

echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "🧪  Test Refresh Token - Mac Terminal"
echo "═══════════════════════════════════════════════════════════════════"
echo ""

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo -e "${YELLOW}⚠️  jq is not installed. Installing via brew...${NC}"
    if command -v brew &> /dev/null; then
        brew install jq
    else
        echo -e "${RED}❌ Please install jq first: brew install jq${NC}"
        exit 1
    fi
fi

# Get refresh token
REFRESH_TOKEN="${1}"
CLIENT_ID="${2:-3rcioodn8t}"

# If no refresh token provided, try to get one via login
if [ -z "$REFRESH_TOKEN" ]; then
    echo -e "${BLUE}ℹ️  No refresh token provided. Getting new token via login...${NC}"
    echo ""
    
    # Try to get credentials from environment or use defaults
    USERNAME="${OAUTH_USERNAME:-Administrator}"
    PASSWORD="${OAUTH_PASSWORD:-1234}"
    
    echo -e "${YELLOW}📝 Using credentials:${NC}"
    echo "   Username: $USERNAME"
    echo "   Client ID: $CLIENT_ID"
    echo ""
    
    echo -e "${BLUE}🔄 Getting access token...${NC}"
    
    LOGIN_RESPONSE=$(curl -sS -X POST "$TOKEN_ENDPOINT" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        --data-urlencode "grant_type=password" \
        --data-urlencode "username=$USERNAME" \
        --data-urlencode "password=$PASSWORD" \
        --data-urlencode "client_id=$CLIENT_ID" \
        --data-urlencode "scope=all openid" 2>&1)
    
    # Check if login was successful
    if echo "$LOGIN_RESPONSE" | jq -e '.error' > /dev/null 2>&1; then
        echo -e "${RED}❌ Login failed!${NC}"
        echo "$LOGIN_RESPONSE" | jq .
        exit 1
    fi
    
    REFRESH_TOKEN=$(echo "$LOGIN_RESPONSE" | jq -r '.refresh_token // empty')
    ACCESS_TOKEN=$(echo "$LOGIN_RESPONSE" | jq -r '.access_token // empty')
    
    if [ -z "$REFRESH_TOKEN" ] || [ "$REFRESH_TOKEN" = "null" ]; then
        echo -e "${RED}❌ Failed to get refresh token from login response${NC}"
        echo "Response:"
        echo "$LOGIN_RESPONSE" | jq .
        exit 1
    fi
    
    echo -e "${GREEN}✅ Login successful!${NC}"
    echo "   Access Token: ${ACCESS_TOKEN:0:30}..."
    echo "   Refresh Token: ${REFRESH_TOKEN:0:30}..."
    echo ""
    echo -e "${BLUE}🔄 Now testing refresh token...${NC}"
    echo ""
fi

# Test refresh token
echo "═══════════════════════════════════════════════════════════════════"
echo "📤 Request Details:"
echo "═══════════════════════════════════════════════════════════════════"
echo "   URL: $TOKEN_ENDPOINT"
echo "   Method: POST"
echo "   Grant Type: refresh_token"
echo "   Client ID: $CLIENT_ID"
echo "   Refresh Token: ${REFRESH_TOKEN:0:30}..."
echo ""

echo "═══════════════════════════════════════════════════════════════════"
echo "📥 Sending Request..."
echo "═══════════════════════════════════════════════════════════════════"
echo ""

RESPONSE=$(curl -sS -w "\n%{http_code}" -X POST "$TOKEN_ENDPOINT" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    --data-urlencode "grant_type=refresh_token" \
    --data-urlencode "refresh_token=$REFRESH_TOKEN" \
    --data-urlencode "client_id=$CLIENT_ID" 2>&1)

# Split response body and status code (Mac compatible)
HTTP_BODY=$(echo "$RESPONSE" | sed '$d')
HTTP_CODE=$(echo "$RESPONSE" | tail -n 1)

echo "═══════════════════════════════════════════════════════════════════"
echo "📥 Response:"
echo "═══════════════════════════════════════════════════════════════════"
echo "   Status Code: $HTTP_CODE"
echo ""

# Parse and display response
if echo "$HTTP_BODY" | jq . > /dev/null 2>&1; then
    # Valid JSON response
    echo "   Response Body:"
    echo "$HTTP_BODY" | jq .
    echo ""
    
    # Check for errors
    if echo "$HTTP_BODY" | jq -e '.error' > /dev/null 2>&1; then
        ERROR=$(echo "$HTTP_BODY" | jq -r '.error')
        echo -e "${RED}❌ Error: $ERROR${NC}"
        
        if [ "$ERROR" = "invalid_grant" ]; then
            echo ""
            echo -e "${YELLOW}💡 Possible reasons:${NC}"
            echo "   1. Refresh token has expired"
            echo "   2. Refresh token is invalid or revoked"
            echo "   3. Client ID mismatch"
            echo "   4. Token not found in database"
        fi
    elif [ "$HTTP_CODE" = "200" ]; then
        NEW_ACCESS_TOKEN=$(echo "$HTTP_BODY" | jq -r '.access_token // empty')
        NEW_REFRESH_TOKEN=$(echo "$HTTP_BODY" | jq -r '.refresh_token // empty')
        EXPIRES_IN=$(echo "$HTTP_BODY" | jq -r '.expires_in // "N/A"')
        
        if [ -n "$NEW_ACCESS_TOKEN" ] && [ "$NEW_ACCESS_TOKEN" != "null" ]; then
            echo -e "${GREEN}✅ SUCCESS! Refresh token validation works correctly!${NC}"
            echo ""
            echo "   New Access Token: ${NEW_ACCESS_TOKEN:0:30}..."
            echo "   New Refresh Token: ${NEW_REFRESH_TOKEN:0:30}..."
            echo "   Expires In: $EXPIRES_IN seconds"
        else
            echo -e "${YELLOW}⚠️  Response 200 but no access_token in response${NC}"
        fi
    else
        echo -e "${YELLOW}⚠️  Unexpected status code: $HTTP_CODE${NC}"
    fi
else
    # Not valid JSON
    echo -e "${YELLOW}⚠️  Response is not valid JSON${NC}"
    echo "   Raw Response:"
    echo "$HTTP_BODY" | head -20
fi

echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "✅ Test Complete"
echo "═══════════════════════════════════════════════════════════════════"
echo ""

