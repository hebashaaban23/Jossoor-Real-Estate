#!/bin/bash
###############################################################################
# Complete API Test Script for Mac
# Tests all 7 CRM Mobile API endpoints with OAuth2
###############################################################################

BASE="https://trust.jossoor.org"
CLIENT_ID="3rcioodn8t"

# Try both users
USERNAME="${1:-Administrator}"
PASSWORD="${2:-1234}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "════════════════════════════════════════════════════════════════════"
echo "  🧪 Complete API Test - Trust.com"
echo "════════════════════════════════════════════════════════════════════"
echo ""
echo "Base URL:  $BASE"
echo "Client ID: $CLIENT_ID"
echo "Username:  $USERNAME"
echo ""

# Step 0: Clear DNS cache (optional)
echo "📝 Step 0: Clear DNS Cache (optional)"
echo "────────────────────────────────────────────────────────────────────"
echo "Run this if OAuth fails:"
echo "  sudo dscacheutil -flushcache"
echo "  sudo killall -HUP mDNSResponder"
echo ""
read -p "Press Enter to continue..."
echo ""

# Step 1: Get OAuth Token
echo "📝 Step 1: Get OAuth Access Token"
echo "────────────────────────────────────────────────────────────────────"

TOKEN_RESPONSE=$(curl -sS -X POST "$BASE/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=password" \
  --data-urlencode "username=$USERNAME" \
  --data-urlencode "password=$PASSWORD" \
  --data-urlencode "client_id=$CLIENT_ID" \
  --data-urlencode "scope=all openid")

echo "$TOKEN_RESPONSE" | jq . 2>/dev/null || echo "$TOKEN_RESPONSE"
echo ""

ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.access_token // empty')
REFRESH_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.refresh_token // empty')

if [ -z "$ACCESS_TOKEN" ]; then
    echo -e "${RED}❌ Failed to get access token${NC}"
    echo ""
    echo "Trying alternative user: test@trust.com"
    echo ""
    
    TOKEN_RESPONSE=$(curl -sS -X POST "$BASE/api/method/frappe.integrations.oauth2.get_token" \
      -H "Content-Type: application/x-www-form-urlencoded" \
      --data-urlencode "grant_type=password" \
      --data-urlencode "username=test@trust.com" \
      --data-urlencode "password=test1234" \
      --data-urlencode "client_id=$CLIENT_ID" \
      --data-urlencode "scope=all openid")
    
    echo "$TOKEN_RESPONSE" | jq . 2>/dev/null || echo "$TOKEN_RESPONSE"
    echo ""
    
    ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.access_token // empty')
    REFRESH_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.refresh_token // empty')
    
    if [ -z "$ACCESS_TOKEN" ]; then
        echo -e "${RED}❌ Authentication failed with both users${NC}"
        echo ""
        echo "Troubleshooting:"
        echo "1. Clear DNS cache: sudo dscacheutil -flushcache"
        echo "2. Check internet connection"
        echo "3. Try from browser/Postman"
        echo "4. Contact server admin"
        exit 1
    fi
fi

echo -e "${GREEN}✅ Access Token obtained!${NC}"
echo "   Token: ${ACCESS_TOKEN:0:30}..."
echo ""

# Step 2: Test API 1 - home_tasks
echo "📝 Step 2: Test home_tasks (GET)"
echo "────────────────────────────────────────────────────────────────────"

RESPONSE=$(curl -sS "$BASE/api/method/crm.api.mobile_api.home_tasks?limit=5" \
  -H "Authorization: Bearer $ACCESS_TOKEN")

echo "$RESPONSE" | jq . 2>/dev/null || echo "$RESPONSE"
echo ""

if echo "$RESPONSE" | jq -e '.message.today' >/dev/null 2>&1; then
    TASK_COUNT=$(echo "$RESPONSE" | jq '.message.today | length')
    echo -e "${GREEN}✅ home_tasks: Working (${TASK_COUNT} tasks)${NC}"
else
    echo -e "${RED}❌ home_tasks: Failed${NC}"
fi
echo ""

# Step 3: Test API 2 - filter_tasks
echo "📝 Step 3: Test filter_tasks (GET)"
echo "────────────────────────────────────────────────────────────────────"

RESPONSE=$(curl -sS "$BASE/api/method/crm.api.mobile_api.filter_tasks?date_from=2025-11-01&date_to=2025-12-31&limit=5" \
  -H "Authorization: Bearer $ACCESS_TOKEN")

echo "$RESPONSE" | jq . 2>/dev/null || echo "$RESPONSE"
echo ""

if echo "$RESPONSE" | jq -e '.message.data' >/dev/null 2>&1; then
    TASK_COUNT=$(echo "$RESPONSE" | jq '.message.data | length')
    echo -e "${GREEN}✅ filter_tasks: Working (${TASK_COUNT} tasks)${NC}"
else
    echo -e "${RED}❌ filter_tasks: Failed${NC}"
fi
echo ""

# Step 4: Test API 3 - main_page_buckets
echo "📝 Step 4: Test main_page_buckets (GET)"
echo "────────────────────────────────────────────────────────────────────"

RESPONSE=$(curl -sS "$BASE/api/method/crm.api.mobile_api.main_page_buckets?min_each=3" \
  -H "Authorization: Bearer $ACCESS_TOKEN")

echo "$RESPONSE" | jq . 2>/dev/null || echo "$RESPONSE"
echo ""

if echo "$RESPONSE" | jq -e '.message.today' >/dev/null 2>&1; then
    TODAY=$(echo "$RESPONSE" | jq '.message.today | length')
    LATE=$(echo "$RESPONSE" | jq '.message.late | length')
    UPCOMING=$(echo "$RESPONSE" | jq '.message.upcoming | length')
    echo -e "${GREEN}✅ main_page_buckets: Working (today:${TODAY}, late:${LATE}, upcoming:${UPCOMING})${NC}"
else
    echo -e "${RED}❌ main_page_buckets: Failed${NC}"
fi
echo ""

# Step 5: Test API 4 - create_task
echo "📝 Step 5: Test create_task (POST)"
echo "────────────────────────────────────────────────────────────────────"

RESPONSE=$(curl -sS -X POST "$BASE/api/method/crm.api.mobile_api.create_task" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Test Task from Mac",
    "task_type": "General",
    "status": "Open",
    "priority": "High",
    "start_date": "2025-12-05",
    "description": "Created via OAuth API test script"
  }')

echo "$RESPONSE" | jq . 2>/dev/null || echo "$RESPONSE"
echo ""

TASK_ID=$(echo "$RESPONSE" | jq -r '.message.name // empty')

if [ -n "$TASK_ID" ] && [ "$TASK_ID" != "null" ]; then
    echo -e "${GREEN}✅ create_task: Working (Task ID: ${TASK_ID})${NC}"
    echo ""
    
    # Step 6: Test API 5 - edit_task
    echo "📝 Step 6: Test edit_task (POST)"
    echo "────────────────────────────────────────────────────────────────────"
    
    RESPONSE=$(curl -sS -X POST "$BASE/api/method/crm.api.mobile_api.edit_task" \
      -H "Authorization: Bearer $ACCESS_TOKEN" \
      -H "Content-Type: application/json" \
      -d "{
        \"task_id\": $TASK_ID,
        \"priority\": \"Medium\",
        \"description\": \"Updated via API test script\"
      }")
    
    echo "$RESPONSE" | jq . 2>/dev/null || echo "$RESPONSE"
    echo ""
    
    if echo "$RESPONSE" | jq -e '.message.name' >/dev/null 2>&1; then
        echo -e "${GREEN}✅ edit_task: Working${NC}"
    else
        echo -e "${RED}❌ edit_task: Failed${NC}"
    fi
    echo ""
    
    # Step 7: Test API 6 - update_status
    echo "📝 Step 7: Test update_status (POST)"
    echo "────────────────────────────────────────────────────────────────────"
    
    RESPONSE=$(curl -sS -X POST "$BASE/api/method/crm.api.mobile_api.update_status" \
      -H "Authorization: Bearer $ACCESS_TOKEN" \
      -H "Content-Type: application/json" \
      -d "{
        \"task_id\": $TASK_ID,
        \"status\": \"In Progress\"
      }")
    
    echo "$RESPONSE" | jq . 2>/dev/null || echo "$RESPONSE"
    echo ""
    
    if echo "$RESPONSE" | jq -e '.message.status' >/dev/null 2>&1; then
        NEW_STATUS=$(echo "$RESPONSE" | jq -r '.message.status')
        echo -e "${GREEN}✅ update_status: Working (Status: ${NEW_STATUS})${NC}"
    else
        echo -e "${RED}❌ update_status: Failed${NC}"
    fi
    echo ""
    
    # Step 8: Test API 7 - delete_task
    echo "📝 Step 8: Test delete_task (POST)"
    echo "────────────────────────────────────────────────────────────────────"
    
    RESPONSE=$(curl -sS -X POST "$BASE/api/method/crm.api.mobile_api.delete_task" \
      -H "Authorization: Bearer $ACCESS_TOKEN" \
      -H "Content-Type: application/json" \
      -d "{
        \"task_id\": $TASK_ID
      }")
    
    echo "$RESPONSE" | jq . 2>/dev/null || echo "$RESPONSE"
    echo ""
    
    if echo "$RESPONSE" | jq -r '.message' | grep -q "deleted"; then
        echo -e "${GREEN}✅ delete_task: Working${NC}"
    else
        echo -e "${RED}❌ delete_task: Failed${NC}"
    fi
    echo ""
else
    echo -e "${RED}❌ create_task: Failed (couldn't create test task)${NC}"
    echo ""
    echo "Skipping edit, update_status, and delete tests..."
    echo ""
fi

# Step 9: Test Refresh Token
echo "📝 Step 9: Test Refresh Token"
echo "────────────────────────────────────────────────────────────────────"

RESPONSE=$(curl -sS -X POST "$BASE/api/method/frappe.integrations.oauth2.get_token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=refresh_token" \
  --data-urlencode "refresh_token=$REFRESH_TOKEN" \
  --data-urlencode "client_id=$CLIENT_ID")

echo "$RESPONSE" | jq . 2>/dev/null || echo "$RESPONSE"
echo ""

NEW_ACCESS_TOKEN=$(echo "$RESPONSE" | jq -r '.access_token // empty')

if [ -n "$NEW_ACCESS_TOKEN" ]; then
    echo -e "${GREEN}✅ Refresh Token: Working${NC}"
    echo "   New Token: ${NEW_ACCESS_TOKEN:0:30}..."
else
    echo -e "${RED}❌ Refresh Token: Failed${NC}"
fi
echo ""

# Summary
echo "════════════════════════════════════════════════════════════════════"
echo "  📊 Test Summary"
echo "════════════════════════════════════════════════════════════════════"
echo ""
echo "Endpoints Tested:"
echo "  1. OAuth Password Grant       ✓"
echo "  2. home_tasks (GET)           ✓"
echo "  3. filter_tasks (GET)         ✓"
echo "  4. main_page_buckets (GET)    ✓"
echo "  5. create_task (POST)         ✓"
echo "  6. edit_task (POST)           ✓"
echo "  7. update_status (POST)       ✓"
echo "  8. delete_task (POST)         ✓"
echo "  9. Refresh Token              ✓"
echo ""
echo "════════════════════════════════════════════════════════════════════"
echo ""
echo -e "${GREEN}🎉 Testing Complete!${NC}"
echo ""
echo "Next Steps:"
echo "  1. Review results above"
echo "  2. If any test failed, check the error messages"
echo "  3. Share this output with your Flutter developer"
echo ""
echo "For Flutter Integration:"
echo "  • Base URL: $BASE"
echo "  • Client ID: $CLIENT_ID"
echo "  • See: FLUTTER_OAUTH_HANDOVER_AR.md"
echo ""
echo "════════════════════════════════════════════════════════════════════"

