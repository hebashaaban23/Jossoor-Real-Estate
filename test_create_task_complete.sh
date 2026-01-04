#!/bin/bash

# ============================================
# Complete Test Script for create_task API
# Tests all available fields
# ============================================

SITE_URL="https://trust.jossoor.org"
EMAIL="${1:-your_email@example.com}"
PASSWORD="${2:-your_password}"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                                                                  ║${NC}"
echo -e "${BLUE}║     🧪 Complete Test Suite for create_task API                  ║${NC}"
echo -e "${BLUE}║                                                                  ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check configuration
if [ "$EMAIL" == "your_email@example.com" ] || [ "$PASSWORD" == "your_password" ]; then
    echo -e "${RED}❌ خطأ: يجب توفير البريد الإلكتروني وكلمة المرور!${NC}"
    echo ""
    echo "الاستخدام:"
    echo "  ./test_create_task_complete.sh your_email@example.com your_password"
    exit 1
fi

# Login
echo -e "${YELLOW}🔐 جاري تسجيل الدخول...${NC}"
LOGIN_RESPONSE=$(curl -s -X POST "${SITE_URL}/api/method/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "usr=${EMAIL}" \
  --data-urlencode "pwd=${PASSWORD}" \
  -c cookies.txt \
  -w "\nHTTP_CODE:%{http_code}")

HTTP_CODE=$(echo "$LOGIN_RESPONSE" | grep "HTTP_CODE" | cut -d: -f2)
LOGIN_BODY=$(echo "$LOGIN_RESPONSE" | grep -v "HTTP_CODE")

if [ "$HTTP_CODE" != "200" ]; then
    echo -e "${RED}❌ فشل تسجيل الدخول!${NC}"
    echo "HTTP Code: $HTTP_CODE"
    echo "Response: $LOGIN_BODY"
    exit 1
fi

echo -e "${GREEN}✅ تم تسجيل الدخول بنجاح!${NC}"
echo ""

# Test counter
PASSED=0
FAILED=0

# Function to test create_task
test_create_task() {
    local test_name="$1"
    local data="$2"
    
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}📋 Test: $test_name${NC}"
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    RESPONSE=$(curl -s -X POST "${SITE_URL}/api/method/crm.api.mobile_api.create_task" \
      -H "Content-Type: application/json" \
      -b cookies.txt \
      -d "$data" \
      -w "\nHTTP_CODE:%{http_code}")
    
    HTTP_CODE=$(echo "$RESPONSE" | grep "HTTP_CODE" | cut -d: -f2)
    BODY=$(echo "$RESPONSE" | grep -v "HTTP_CODE")
    
    echo "Request Data:"
    echo "$data" | python3 -m json.tool 2>/dev/null || echo "$data"
    echo ""
    echo "Response:"
    echo "$BODY" | python3 -m json.tool 2>/dev/null || echo "$BODY"
    echo ""
    
    if echo "$BODY" | grep -q "exc_type\|session_expired\|PermissionError"; then
        echo -e "${RED}❌ FAILED${NC}"
        FAILED=$((FAILED + 1))
        return 1
    elif [ "$HTTP_CODE" == "200" ]; then
        echo -e "${GREEN}✅ PASSED${NC}"
        PASSED=$((PASSED + 1))
        return 0
    else
        echo -e "${RED}❌ FAILED (HTTP $HTTP_CODE)${NC}"
        FAILED=$((FAILED + 1))
        return 1
    fi
}

# Test 1: Minimal (task_type only)
echo ""
test_create_task "1. Minimal Fields" '{
  "task_type": "Meeting"
}'

# Test 2: Basic Fields
test_create_task "2. Basic Fields" '{
  "task_type": "Call",
  "title": "Basic Test Task",
  "status": "Todo",
  "priority": "Medium",
  "start_date": "2025-12-15 10:00:00",
  "due_date": "2025-12-15 18:00:00",
  "description": "Basic test task with standard fields"
}'

# Test 3: With Lead (uncomment and set actual lead ID)
# test_create_task "3. With Lead" '{
#   "task_type": "Call",
#   "title": "Task with Lead",
#   "lead": "CRM-LEAD-2025-001"
# }'

# Test 4: With Project Unit (uncomment and set actual project unit ID)
# test_create_task "4. With Project Unit" '{
#   "task_type": "Property Showing",
#   "title": "Task with Project Unit",
#   "project_unit": "PROJECT-UNIT-001"
# }'

# Test 5: With assigned_to_list
test_create_task "5. With assigned_to_list" '{
  "task_type": "Meeting",
  "title": "Task with Assignees",
  "assigned_to_list": [
    {
      "email": "user@example.com",
      "name": "Test User",
      "profile_pic": null
    }
  ]
}'

# Test 6: With meeting_attendees
test_create_task "6. With meeting_attendees" '{
  "task_type": "Meeting",
  "title": "Task with Attendees",
  "meeting_attendees": [
    {
      "email": "attendee@example.com",
      "name": "Test Attendee",
      "profile_pic": null
    }
  ]
}'

# Test 7: Complete (All Fields)
test_create_task "7. Complete (All Fields)" '{
  "task_type": "Meeting",
  "title": "Complete Test Task",
  "status": "Todo",
  "priority": "High",
  "start_date": "2025-12-15 10:00:00",
  "due_date": "2025-12-15 18:00:00",
  "description": "Complete test task with all available fields",
  "assigned_to": "user@example.com",
  "assigned_to_list": [
    {
      "email": "user1@example.com",
      "name": "User One",
      "profile_pic": null
    },
    {
      "email": "user2@example.com",
      "name": "User Two",
      "profile_pic": null
    }
  ],
  "meeting_attendees": [
    {
      "email": "attendee1@example.com",
      "name": "Attendee One",
      "profile_pic": null
    },
    {
      "email": "attendee2@example.com",
      "name": "Attendee Two",
      "profile_pic": null
    }
  ]
}'

# Summary
echo ""
echo -e "${BLUE}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                                                                  ║${NC}"
echo -e "${BLUE}║     📊 Test Summary                                              ║${NC}"
echo -e "${BLUE}║                                                                  ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""
TOTAL=$((PASSED + FAILED))
echo -e "Total Tests: ${TOTAL}"
echo -e "${GREEN}Passed: ${PASSED}${NC}"
echo -e "${RED}Failed: ${FAILED}${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}🎉 All tests passed!${NC}"
else
    echo -e "${YELLOW}⚠️  Some tests failed. Check the output above for details.${NC}"
fi

# Cleanup
rm -f cookies.txt

echo ""
echo -e "${BLUE}✅ Test suite completed!${NC}"

