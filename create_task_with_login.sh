#!/bin/bash

# ============================================
# Script لإنشاء Task مع Login تلقائي
# ============================================

SITE_URL="https://trust.jossoor.org"
EMAIL="${1:-your_email@example.com}"
PASSWORD="${2:-your_password}"

# ألوان للـ output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${YELLOW}║                                                                  ║${NC}"
echo -e "${YELLOW}║     🔐 تسجيل الدخول وإنشاء Task                                 ║${NC}"
echo -e "${YELLOW}║                                                                  ║${NC}"
echo -e "${YELLOW}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# التحقق من البريد الإلكتروني
if [ "$EMAIL" == "your_email@example.com" ]; then
    echo -e "${RED}❌ خطأ: يجب توفير البريد الإلكتروني!${NC}"
    echo ""
    echo "الاستخدام:"
    echo "  ./create_task_with_login.sh your_email@example.com your_password"
    echo ""
    echo "أو عدل السطرين 7-8 في الملف:"
    echo "  EMAIL=\"your_email@example.com\""
    echo "  PASSWORD=\"your_password\""
    exit 1
fi

# 1. Login واحصل على Cookie
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

# 2. التحقق من Role
echo -e "${YELLOW}🔍 التحقق من Role...${NC}"
ROLE_RESPONSE=$(curl -s -X GET "${SITE_URL}/api/method/crm.api.mobile_api.get_current_user_role" \
  -b cookies.txt)

ROLE=$(echo "$ROLE_RESPONSE" | python3 -c "import sys, json; print(json.load(sys.stdin).get('message', {}).get('role', 'Unknown'))" 2>/dev/null)

if [ -z "$ROLE" ] || [ "$ROLE" == "Unknown" ]; then
    echo -e "${YELLOW}⚠️  لم يتم العثور على Role، لكن سنتابع...${NC}"
else
    echo -e "${GREEN}✅ Role: $ROLE${NC}"
fi
echo ""

# 3. إنشاء Task
echo -e "${YELLOW}📋 جاري إنشاء Task...${NC}"
TASK_RESPONSE=$(curl -s -X POST "${SITE_URL}/api/method/crm.api.mobile_api.create_task" \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{
    "task_type": "Meeting",
    "title": "Test Task from Terminal Script",
    "status": "Todo",
    "priority": "High",
    "description": "Created from terminal script with automatic login"
  }')

# التحقق من النتيجة
if echo "$TASK_RESPONSE" | grep -q "session_expired\|PermissionError"; then
    echo -e "${RED}❌ خطأ في إنشاء Task!${NC}"
    echo "$TASK_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$TASK_RESPONSE"
    exit 1
fi

echo -e "${GREEN}✅ تم إنشاء Task بنجاح!${NC}"
echo ""
echo "Response:"
echo "$TASK_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$TASK_RESPONSE"
echo ""

# تنظيف
rm -f cookies.txt

echo -e "${GREEN}✅ انتهى!${NC}"

