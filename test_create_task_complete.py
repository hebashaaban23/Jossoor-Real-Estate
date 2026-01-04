#!/usr/bin/env python3
"""
Test script for create_task API with all available fields.
This script tests the create_task API with all possible fields.
"""

import requests
import json
from datetime import datetime, timedelta

# Configuration
SITE_URL = "https://trust.jossoor.org"
LOGIN_EMAIL = "your_email@example.com"  # Change this
LOGIN_PASSWORD = "your_password"  # Change this

# Test data with all fields
def get_test_data():
    """Generate test data with all available fields."""
    now = datetime.now()
    start_date = now.strftime("%Y-%m-%d %H:%M:%S")
    due_date = (now + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    
    return {
        # Required fields
        "task_type": "Meeting",
        
        # Basic fields
        "title": f"Complete Test Task - {now.strftime('%Y-%m-%d %H:%M:%S')}",
        "status": "Todo",
        "priority": "High",
        "start_date": start_date,
        "due_date": due_date,
        "description": "This is a complete test task with all available fields. Created by automated test script.",
        
        # Reference fields (direct links)
        "lead": None,  # Set to actual CRM Lead ID if available
        "project": None,  # Set to actual Real Estate Project ID if available
        "unit": None,  # Set to actual Unit ID if available
        "project_unit": None,  # Set to actual Project Unit ID if available
        
        # Legacy reference fields (deprecated, but still supported)
        "reference_doctype": None,  # Set if not using direct link fields
        "reference_docname": None,  # Set if not using direct link fields
        
        # Assignment fields
        "assigned_to": None,  # Single user email (legacy)
        "assigned_to_list": [
            {
                "email": "user1@example.com",  # Change to actual user email
                "name": "Test User One",
                "profile_pic": None
            },
            {
                "email": "user2@example.com",  # Change to actual user email
                "name": "Test User Two",
                "profile_pic": None
            }
        ],
        "meeting_attendees": [
            {
                "email": "attendee1@example.com",  # Change to actual user email
                "name": "Attendee One",
                "profile_pic": None
            },
            {
                "email": "attendee2@example.com",  # Change to actual user email
                "name": "Attendee Two",
                "profile_pic": None
            }
        ]
    }

def login_and_get_session():
    """Login and return session with cookies."""
    session = requests.Session()
    
    print("🔐 جاري تسجيل الدخول...")
    login_url = f"{SITE_URL}/api/method/login"
    login_data = {
        "usr": LOGIN_EMAIL,
        "pwd": LOGIN_PASSWORD
    }
    
    response = session.post(login_url, data=login_data)
    
    if response.status_code == 200:
        result = response.json()
        if result.get("message") == "Logged In":
            print("✅ تم تسجيل الدخول بنجاح!")
            return session
        else:
            print(f"❌ فشل تسجيل الدخول: {result}")
            return None
    else:
        print(f"❌ خطأ في تسجيل الدخول: HTTP {response.status_code}")
        print(response.text)
        return None

def check_user_role(session):
    """Check current user role."""
    print("\n🔍 التحقق من Role...")
    try:
        response = session.get(f"{SITE_URL}/api/method/crm.api.mobile_api.get_current_user_role")
        if response.status_code == 200:
            result = response.json()
            role = result.get("message", {}).get("role", "Unknown")
            print(f"✅ Role: {role}")
            return role
        else:
            print(f"⚠️  لم يتم التحقق من Role: HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"⚠️  خطأ في التحقق من Role: {e}")
        return None

def test_create_task_minimal(session):
    """Test 1: Minimal required fields only."""
    print("\n" + "="*60)
    print("📋 Test 1: Minimal Fields (task_type only)")
    print("="*60)
    
    data = {
        "task_type": "Meeting"
    }
    
    return test_create_task(session, data, "Minimal")

def test_create_task_basic(session):
    """Test 2: Basic fields."""
    print("\n" + "="*60)
    print("📋 Test 2: Basic Fields")
    print("="*60)
    
    now = datetime.now()
    data = {
        "task_type": "Call",
        "title": f"Basic Test Task - {now.strftime('%Y-%m-%d %H:%M:%S')}",
        "status": "Todo",
        "priority": "Medium",
        "start_date": now.strftime("%Y-%m-%d %H:%M:%S"),
        "due_date": (now + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
        "description": "Basic test task with standard fields"
    }
    
    return test_create_task(session, data, "Basic")

def test_create_task_with_lead(session, lead_id=None):
    """Test 3: With Lead field."""
    print("\n" + "="*60)
    print("📋 Test 3: With Lead Field")
    print("="*60)
    
    if not lead_id:
        print("⚠️  Skipping: No lead_id provided")
        return None
    
    now = datetime.now()
    data = {
        "task_type": "Call",
        "title": f"Task with Lead - {now.strftime('%Y-%m-%d %H:%M:%S')}",
        "lead": lead_id
    }
    
    return test_create_task(session, data, "With Lead")

def test_create_task_with_project_unit(session, project_unit_id=None):
    """Test 4: With Project Unit field."""
    print("\n" + "="*60)
    print("📋 Test 4: With Project Unit Field")
    print("="*60)
    
    if not project_unit_id:
        print("⚠️  Skipping: No project_unit_id provided")
        return None
    
    now = datetime.now()
    data = {
        "task_type": "Property Showing",
        "title": f"Task with Project Unit - {now.strftime('%Y-%m-%d %H:%M:%S')}",
        "project_unit": project_unit_id
    }
    
    return test_create_task(session, data, "With Project Unit")

def test_create_task_with_assigned_to_list(session, user_emails=None):
    """Test 5: With assigned_to_list."""
    print("\n" + "="*60)
    print("📋 Test 5: With assigned_to_list")
    print("="*60)
    
    if not user_emails:
        print("⚠️  Skipping: No user_emails provided")
        return None
    
    now = datetime.now()
    assigned_to_list = [
        {
            "email": email,
            "name": f"User {i+1}",
            "profile_pic": None
        }
        for i, email in enumerate(user_emails[:2])  # Max 2 users for test
    ]
    
    data = {
        "task_type": "Meeting",
        "title": f"Task with Assignees - {now.strftime('%Y-%m-%d %H:%M:%S')}",
        "assigned_to_list": assigned_to_list
    }
    
    return test_create_task(session, data, "With assigned_to_list")

def test_create_task_with_meeting_attendees(session, attendee_emails=None):
    """Test 6: With meeting_attendees."""
    print("\n" + "="*60)
    print("📋 Test 6: With meeting_attendees")
    print("="*60)
    
    if not attendee_emails:
        print("⚠️  Skipping: No attendee_emails provided")
        return None
    
    now = datetime.now()
    meeting_attendees = [
        {
            "email": email,
            "name": f"Attendee {i+1}",
            "profile_pic": None
        }
        for i, email in enumerate(attendee_emails[:2])  # Max 2 attendees for test
    ]
    
    data = {
        "task_type": "Meeting",
        "title": f"Task with Attendees - {now.strftime('%Y-%m-%d %H:%M:%S')}",
        "meeting_attendees": meeting_attendees
    }
    
    return test_create_task(session, data, "With meeting_attendees")

def test_create_task_complete(session):
    """Test 7: Complete test with all fields."""
    print("\n" + "="*60)
    print("📋 Test 7: Complete Test (All Fields)")
    print("="*60)
    
    data = get_test_data()
    # Remove None values
    data = {k: v for k, v in data.items() if v is not None}
    
    return test_create_task(session, data, "Complete")

def test_create_task(session, data, test_name):
    """Execute create_task API call."""
    url = f"{SITE_URL}/api/method/crm.api.mobile_api.create_task"
    
    print(f"\n📤 Sending request...")
    print(f"Data: {json.dumps(data, indent=2, ensure_ascii=False)}")
    
    try:
        response = session.post(url, json=data)
        
        print(f"\n📥 Response Status: {response.status_code}")
        
        try:
            result = response.json()
            print(f"Response: {json.dumps(result, indent=2, ensure_ascii=False)}")
            
            if response.status_code == 200 and not result.get("exc_type"):
                print(f"\n✅ {test_name} Test: SUCCESS")
                if "message" in result:
                    task_data = result.get("message", {})
                    if isinstance(task_data, dict) and "name" in task_data:
                        print(f"   Task Created: {task_data.get('name')}")
                return True
            else:
                print(f"\n❌ {test_name} Test: FAILED")
                if result.get("exc_type"):
                    print(f"   Error Type: {result.get('exc_type')}")
                if result.get("exception"):
                    print(f"   Exception: {result.get('exception')}")
                return False
        except json.JSONDecodeError:
            print(f"\n❌ {test_name} Test: FAILED (Invalid JSON)")
            print(f"Response Text: {response.text[:500]}")
            return False
            
    except Exception as e:
        print(f"\n❌ {test_name} Test: ERROR")
        print(f"   Exception: {str(e)}")
        return False

def main():
    """Main test function."""
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║                                                                  ║")
    print("║     🧪 Complete Test Suite for create_task API                  ║")
    print("║                                                                  ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print()
    
    # Check configuration
    if LOGIN_EMAIL == "your_email@example.com" or LOGIN_PASSWORD == "your_password":
        print("⚠️  Warning: Please update LOGIN_EMAIL and LOGIN_PASSWORD in the script!")
        print("   Current values are placeholders.")
        print()
    
    # Login
    session = login_and_get_session()
    if not session:
        print("\n❌ Cannot proceed without login. Exiting.")
        return
    
    # Check role
    role = check_user_role(session)
    
    # Run tests
    results = {}
    
    # Test 1: Minimal
    results["Minimal"] = test_create_task_minimal(session)
    
    # Test 2: Basic
    results["Basic"] = test_create_task_basic(session)
    
    # Test 3: With Lead (skip if no lead_id)
    # results["With Lead"] = test_create_task_with_lead(session, "CRM-LEAD-2025-001")
    
    # Test 4: With Project Unit (skip if no project_unit_id)
    # results["With Project Unit"] = test_create_task_with_project_unit(session, "PROJECT-UNIT-001")
    
    # Test 5: With assigned_to_list (skip if no user emails)
    # results["With assigned_to_list"] = test_create_task_with_assigned_to_list(session, ["user1@example.com", "user2@example.com"])
    
    # Test 6: With meeting_attendees (skip if no attendee emails)
    # results["With meeting_attendees"] = test_create_task_with_meeting_attendees(session, ["attendee1@example.com", "attendee2@example.com"])
    
    # Test 7: Complete
    results["Complete"] = test_create_task_complete(session)
    
    # Summary
    print("\n" + "="*60)
    print("📊 Test Summary")
    print("="*60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name}: {status}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed!")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")

if __name__ == "__main__":
    main()

