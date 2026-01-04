#!/bin/bash

# ============================================
# Script لاختبار create_task API من Terminal
# ============================================

# قم بتغيير هذه القيم حسب احتياجك
SITE_URL="https://trust.jossoor.org"
SESSION_COOKIE="YOUR_SESSION_COOKIE_HERE"

# ألوان للـ output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${YELLOW}║                                                                  ║${NC}"
echo -e "${YELLOW}║     🧪 اختبار create_task API                                    ║${NC}"
echo -e "${YELLOW}║                                                                  ║${NC}"
echo -e "${YELLOW}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# التحقق من Session Cookie
if [ "$SESSION_COOKIE" == "YOUR_SESSION_COOKIE_HERE" ]; then
    echo -e "${RED}❌ خطأ: يجب تعيين SESSION_COOKIE أولاً!${NC}"
    echo ""
    echo "كيفية الحصول على Session Cookie:"
    echo "1. افتح المتصفح واذهب إلى: $SITE_URL"
    echo "2. سجل دخول بحسابك"
    echo "3. اضغط F12 (أو Cmd+Option+I على Mac)"
    echo "4. اذهب إلى Application → Cookies → $SITE_URL"
    echo "5. انسخ قيمة cookie اسمه 'sid'"
    echo "6. ضعها في متغير SESSION_COOKIE في هذا الملف"
    exit 1
fi

echo -e "${GREEN}✅ SESSION_COOKIE موجود${NC}"
echo ""

# مثال 1: إنشاء Task بسيط
echo -e "${YELLOW}📋 مثال 1: إنشاء Task بسيط${NC}"
echo ""

RESPONSE=$(curl -s -X POST "${SITE_URL}/api/method/crm.api.mobile_api.create_task" \
  -H "Content-Type: application/json" \
  -H "Cookie: sid=${SESSION_COOKIE}" \
  -d '{
    "title": "Test Task from Terminal",
    "task_type": "Meeting",
    "status": "Todo",
    "priority": "High",
    "description": "This is a test task created from Mac terminal"
  }')

echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
echo ""
echo ""

# مثال 2: إنشاء Task مع lead
echo -e "${YELLOW}📋 مثال 2: إنشاء Task مع lead (قم بتغيير CRM-LEAD-2025-001)${NC}"
echo ""

RESPONSE2=$(curl -s -X POST "${SITE_URL}/api/method/crm.api.mobile_api.create_task" \
  -H "Content-Type: application/json" \
  -H "Cookie: sid=${SESSION_COOKIE}" \
  -d '{
    "title": "Follow up with Lead",
    "task_type": "Call",
    "lead": "CRM-LEAD-2025-001"
  }')

echo "$RESPONSE2" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE2"
echo ""
echo ""

# مثال 3: إنشاء Task مع assigned_to_list
echo -e "${YELLOW}📋 مثال 3: إنشاء Task مع assigned_to_list (قم بتغيير user@example.com)${NC}"
echo ""

RESPONSE3=$(curl -s -X POST "${SITE_URL}/api/method/crm.api.mobile_api.create_task" \
  -H "Content-Type: application/json" \
  -H "Cookie: sid=${SESSION_COOKIE}" \
  -d '{
    "title": "Task with Assignees",
    "task_type": "Meeting",
    "assigned_to_list": [
      {
        "email": "user@example.com",
        "name": "John Doe",
        "profile_pic": null
      }
    ]
  }')

echo "$RESPONSE3" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE3"
echo ""
echo ""

echo -e "${GREEN}✅ انتهى الاختبار!${NC}"
echo ""
echo "ملاحظات:"
echo "- إذا ظهر خطأ 401، يعني Session منتهي - سجل دخول مرة أخرى"
echo "- إذا ظهر LinkValidationError، تأكد من أن القيم المرسلة موجودة فعلاً"
echo "- استخدم python3 -m json.tool لتنسيق الـ response بشكل جميل"

