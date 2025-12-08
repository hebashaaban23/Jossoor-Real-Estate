#!/bin/bash
# Test script for OAuth API endpoints
# Usage: ./TEST_OAUTH_API.sh

BASE_URL="https://trust.jossoor.org"
USERNAME="Administrator"
PASSWORD="1234"

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                                                                  ║"
echo "║     🧪 OAuth API Test Script                                    ║"
echo "║                                                                  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Step 1: Get Client ID
echo "┌──────────────────────────────────────────────────────────────────┐"
echo "│ Step 1: Getting Client ID...                                      │"
echo "└──────────────────────────────────────────────────────────────────┘"
CLIENT_ID=$(curl -s "$BASE_URL/api/method/crm.api.mobile_api.get_oauth_config" | python3 -c "import sys, json; print(json.load(sys.stdin)['message']['client_id'])")
if [ -z "$CLIENT_ID" ] || [ "$CLIENT_ID" = "null" ]; then
    echo "❌ Failed to get Client ID"
    exit 1
fi
echo "✅ Client ID: $CLIENT_ID"
echo ""

# Step 2: Get Access Token
echo "┌──────────────────────────────────────────────────────────────────┐"
echo "│ Step 2: Getting Access Token...                                  │"
echo "└──────────────────────────────────────────────────────────────────┘"
TOKEN_RESPONSE=$(curl -s -X POST "$BASE_URL/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "username=$USERNAME" \
  -d "password=$PASSWORD" \
  -d "client_id=$CLIENT_ID" \
  -d "scope=all openid")

ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('access_token', ''))" 2>/dev/null)

if [ -z "$ACCESS_TOKEN" ] || [ "$ACCESS_TOKEN" = "null" ]; then
    echo "❌ Failed to get Access Token"
    echo "Response: $TOKEN_RESPONSE"
    exit 1
fi

REFRESH_TOKEN=$(echo "$TOKEN_RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('refresh_token', ''))" 2>/dev/null)
EXPIRES_IN=$(echo "$TOKEN_RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('expires_in', ''))" 2>/dev/null)

echo "✅ Access Token: ${ACCESS_TOKEN:0:20}..."
echo "✅ Refresh Token: ${REFRESH_TOKEN:0:20}..."
echo "✅ Expires In: $EXPIRES_IN seconds"
echo ""

# Step 3: Test API Endpoints
echo "┌──────────────────────────────────────────────────────────────────┐"
echo "│ Step 3: Testing API Endpoints...                                 │"
echo "└──────────────────────────────────────────────────────────────────┘"
echo ""

# Test 1: home_tasks
echo "📋 Test 1: home_tasks"
RESPONSE=$(curl -s "$BASE_URL/api/method/crm.api.mobile_api.home_tasks?limit=5" \
  -H "Authorization: Bearer $ACCESS_TOKEN")
if echo "$RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); exit(0 if 'message' in data else 1)" 2>/dev/null; then
    echo "✅ PASSED: home_tasks"
    echo "$RESPONSE" | python3 -m json.tool | head -10
else
    echo "❌ FAILED: home_tasks"
    echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
fi
echo ""

# Test 2: filter_tasks
echo "📋 Test 2: filter_tasks (page 1)"
RESPONSE=$(curl -s "$BASE_URL/api/method/crm.api.mobile_api.filter_tasks?page=1&limit=10&date_from=2025-12-01&date_to=2025-12-31&importance=High,Medium&status=Todo,In%20Progress" \
  -H "Authorization: Bearer $ACCESS_TOKEN")
if echo "$RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); exit(0 if 'message' in data else 1)" 2>/dev/null; then
    echo "✅ PASSED: filter_tasks"
    echo "$RESPONSE" | python3 -m json.tool | head -15
else
    echo "❌ FAILED: filter_tasks"
    echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
fi
echo ""

# Test 3: main_page_buckets
echo "📋 Test 3: main_page_buckets"
RESPONSE=$(curl -s "$BASE_URL/api/method/crm.api.mobile_api.main_page_buckets?min_each=3" \
  -H "Authorization: Bearer $ACCESS_TOKEN")
if echo "$RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); exit(0 if 'message' in data else 1)" 2>/dev/null; then
    echo "✅ PASSED: main_page_buckets"
    echo "$RESPONSE" | python3 -m json.tool | head -15
else
    echo "❌ FAILED: main_page_buckets"
    echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
fi
echo ""

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                                                                  ║"
echo "║     ✅ Testing Complete!                                         ║"
echo "║                                                                  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "💡 To use the token in other requests:"
echo "   export ACCESS_TOKEN=\"$ACCESS_TOKEN\""
echo "   curl -H \"Authorization: Bearer \$ACCESS_TOKEN\" \"$BASE_URL/api/method/...\""

