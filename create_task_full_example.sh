#!/bin/bash

# ============================================
# مثال كامل لـ create_task API بكل الحقول
# ============================================

SITE_URL="https://trust.jossoor.org"
SESSION_COOKIE="YOUR_SESSION_COOKIE_HERE"

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                                                                  ║"
echo "║     📋 مثال كامل لـ create_task بكل الحقول                      ║"
echo "║                                                                  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# مثال كامل بكل الحقول
curl -X POST "${SITE_URL}/api/method/crm.api.mobile_api.create_task" \
  -H "Content-Type: application/json" \
  -H "Cookie: sid=${SESSION_COOKIE}" \
  -d '{
    "task_type": "Meeting",
    "title": "Complete Task Example",
    "status": "Todo",
    "priority": "High",
    "start_date": "2025-12-15 10:00:00",
    "due_date": "2025-12-15 18:00:00",
    "description": "This is a complete example with all available fields",
    "lead": "CRM-LEAD-2025-001",
    "project": "PROJECT-001",
    "unit": "UNIT-001",
    "project_unit": "PROJECT-UNIT-001",
    "assigned_to": "user@example.com",
    "assigned_to_list": [
      {
        "email": "user1@example.com",
        "name": "John Doe",
        "profile_pic": null
      },
      {
        "email": "user2@example.com",
        "name": "Jane Smith",
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
    ],
    "reference_doctype": "CRM Lead",
    "reference_docname": "CRM-LEAD-2025-001"
  }' | python3 -m json.tool

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                                                                  ║"
echo "║     📋 قائمة بكل الحقول المتاحة                                  ║"
echo "║                                                                  ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "✅ الحقول المطلوبة:"
echo "   - task_type: \"Meeting\" | \"Property Showing\" | \"Call\""
echo ""
echo "✅ الحقول الاختيارية الأساسية:"
echo "   - title: String"
echo "   - status: \"Backlog\" | \"Todo\" | \"In Progress\" | \"Done\" | \"late\""
echo "   - priority: \"Low\" | \"Medium\" | \"High\""
echo "   - start_date: \"YYYY-MM-DD HH:MM:SS\" أو \"YYYY-MM-DD\""
echo "   - due_date: \"YYYY-MM-DD HH:MM:SS\""
echo "   - description: String (يدعم HTML)"
echo ""
echo "✅ حقول المرجع (Reference Fields):"
echo "   - lead: \"CRM-LEAD-2025-001\" (ID من CRM Lead)"
echo "   - project: \"PROJECT-001\" (ID من Real Estate Project)"
echo "   - unit: \"UNIT-001\" (ID من Unit)"
echo "   - project_unit: \"PROJECT-UNIT-001\" (ID من Project Unit)"
echo "   - reference_doctype: \"CRM Lead\" | \"Real Estate Project\" | \"Unit\" | \"Project Unit\""
echo "   - reference_docname: Document name/ID"
echo ""
echo "✅ حقول التعيين (Assignment Fields):"
echo "   - assigned_to: \"user@example.com\" (مستخدم واحد - legacy)"
echo "   - assigned_to_list: [{\"email\": \"...\", \"name\": \"...\", \"profile_pic\": null}]"
echo "   - meeting_attendees: [{\"email\": \"...\", \"name\": \"...\", \"profile_pic\": null}]"
echo ""

