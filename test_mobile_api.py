#!/usr/bin/env python3
"""
Simple test script for CRM Task Mobile API
Demonstrates how to use all endpoints with Python requests library
"""

import requests
import json
from datetime import datetime, timedelta

# Configuration
BASE_URL = "https://your-site.com"
USERNAME = "user@example.com"
PASSWORD = "your_password"

# API Endpoints
API_BASE = f"{BASE_URL}/api/method/crm.api.mobile_api"

class MobileAPITester:
    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.created_tasks = []
    
    def login(self):
        """Login and obtain session cookies"""
        print("🔐 Logging in...")
        response = self.session.post(
            f"{self.base_url}/api/method/login",
            data={
                'usr': self.username,
                'pwd': self.password
            }
        )
        
        if response.status_code == 200:
            print("✅ Login successful")
            print(f"   Cookies: {list(self.session.cookies.keys())}")
            return True
        else:
            print(f"❌ Login failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
    
    def create_task(self, task_type="Call", title="Test Task", priority="High"):
        """Test create_task endpoint"""
        print(f"\n📝 Creating task: {title}")
        
        response = self.session.post(
            f"{API_BASE}.create_task",
            json={
                'task_type': task_type,
                'title': title,
                'priority': priority,
                'status': 'Todo',
                'start_date': datetime.now().strftime('%Y-%m-%d'),
                'description': f'Test task created by mobile API test script'
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            task = data.get('message', {})
            print(f"✅ Task created: {task.get('name')}")
            print(f"   Title: {task.get('title')}")
            print(f"   Status: {task.get('status')}")
            print(f"   Priority: {task.get('priority')}")
            self.created_tasks.append(task.get('name'))
            return task
        else:
            print(f"❌ Failed to create task: {response.status_code}")
            print(f"   Response: {response.text}")
            return None
    
    def edit_task(self, task_name, new_title=None, new_status=None):
        """Test edit_task endpoint"""
        print(f"\n✏️  Editing task: {task_name}")
        
        data = {'name': task_name}
        if new_title:
            data['title'] = new_title
        if new_status:
            data['status'] = new_status
        
        response = self.session.post(
            f"{API_BASE}.edit_task",
            json=data
        )
        
        if response.status_code == 200:
            task = response.json().get('message', {})
            print(f"✅ Task updated")
            print(f"   Title: {task.get('title')}")
            print(f"   Status: {task.get('status')}")
            return task
        else:
            print(f"❌ Failed to edit task: {response.status_code}")
            print(f"   Response: {response.text}")
            return None
    
    def update_status(self, task_name, status):
        """Test update_status endpoint"""
        print(f"\n🔄 Updating status: {task_name} → {status}")
        
        response = self.session.post(
            f"{API_BASE}.update_status",
            json={
                'name': task_name,
                'status': status
            }
        )
        
        if response.status_code == 200:
            task = response.json().get('message', {})
            print(f"✅ Status updated to: {task.get('status')}")
            return task
        else:
            print(f"❌ Failed to update status: {response.status_code}")
            print(f"   Response: {response.text}")
            return None
    
    def filter_tasks(self, **kwargs):
        """Test filter_tasks endpoint"""
        print(f"\n🔍 Filtering tasks with: {kwargs}")
        
        response = self.session.get(
            f"{API_BASE}.filter_tasks",
            params=kwargs
        )
        
        if response.status_code == 200:
            data = response.json().get('message', {})
            tasks = data.get('data', [])
            print(f"✅ Found {len(tasks)} tasks")
            for task in tasks[:3]:  # Show first 3
                print(f"   - {task.get('name')}: {task.get('title')} ({task.get('status')})")
            if len(tasks) > 3:
                print(f"   ... and {len(tasks) - 3} more")
            return tasks
        else:
            print(f"❌ Failed to filter tasks: {response.status_code}")
            print(f"   Response: {response.text}")
            return []
    
    def get_home_tasks(self, limit=5):
        """Test home_tasks endpoint"""
        print(f"\n🏠 Getting today's top {limit} tasks")
        
        response = self.session.get(
            f"{API_BASE}.home_tasks",
            params={'limit': limit}
        )
        
        if response.status_code == 200:
            data = response.json().get('message', {})
            tasks = data.get('today', [])
            print(f"✅ Found {len(tasks)} tasks for today")
            for task in tasks:
                print(f"   - {task.get('name')}: {task.get('title')} ({task.get('priority')})")
            return tasks
        else:
            print(f"❌ Failed to get home tasks: {response.status_code}")
            print(f"   Response: {response.text}")
            return []
    
    def get_main_page_buckets(self, min_each=5):
        """Test main_page_buckets endpoint"""
        print(f"\n📊 Getting main page buckets (min {min_each} each)")
        
        response = self.session.get(
            f"{API_BASE}.main_page_buckets",
            params={'min_each': min_each}
        )
        
        if response.status_code == 200:
            data = response.json().get('message', {})
            today = data.get('today', [])
            late = data.get('late', [])
            upcoming = data.get('upcoming', [])
            
            print(f"✅ Buckets loaded:")
            print(f"   📅 Today: {len(today)} tasks")
            print(f"   ⏰ Late: {len(late)} tasks")
            print(f"   🔮 Upcoming: {len(upcoming)} tasks")
            
            return data
        else:
            print(f"❌ Failed to get buckets: {response.status_code}")
            print(f"   Response: {response.text}")
            return None
    
    def delete_task(self, task_name):
        """Test delete_task endpoint"""
        print(f"\n🗑️  Deleting task: {task_name}")
        
        response = self.session.post(
            f"{API_BASE}.delete_task",
            json={'name': task_name}
        )
        
        if response.status_code == 200:
            result = response.json().get('message', {})
            if result.get('ok'):
                print(f"✅ Task deleted successfully")
                return True
        
        print(f"❌ Failed to delete task: {response.status_code}")
        print(f"   Response: {response.text}")
        return False
    
    def cleanup(self):
        """Delete all tasks created during testing"""
        print("\n🧹 Cleaning up test tasks...")
        for task_name in self.created_tasks:
            self.delete_task(task_name)
        print("✅ Cleanup complete")
    
    def run_full_test(self):
        """Run complete test suite"""
        print("=" * 60)
        print("CRM TASK MOBILE API - FULL TEST SUITE")
        print("=" * 60)
        
        # Login
        if not self.login():
            print("\n❌ Cannot proceed without login")
            return
        
        # Test 1: Create tasks
        print("\n" + "=" * 60)
        print("TEST 1: CREATE TASKS")
        print("=" * 60)
        
        task1 = self.create_task("Call", "Follow up with client", "High")
        task2 = self.create_task("Meeting", "Team standup", "Medium")
        task3 = self.create_task("Property Showing", "Show property to prospect", "Low")
        
        if not task1:
            print("\n❌ Task creation failed, stopping tests")
            return
        
        # Test 2: Edit task
        print("\n" + "=" * 60)
        print("TEST 2: EDIT TASK")
        print("=" * 60)
        
        self.edit_task(task1['name'], new_title="Updated: Follow up with client")
        
        # Test 3: Update status
        print("\n" + "=" * 60)
        print("TEST 3: UPDATE STATUS")
        print("=" * 60)
        
        self.update_status(task1['name'], "In Progress")
        
        # Test 4: Filter tasks
        print("\n" + "=" * 60)
        print("TEST 4: FILTER TASKS")
        print("=" * 60)
        
        today_str = datetime.now().strftime('%Y-%m-%d')
        self.filter_tasks(
            date_from=today_str,
            date_to=today_str,
            importance="High,Medium",
            limit=10
        )
        
        # Test 5: Home tasks
        print("\n" + "=" * 60)
        print("TEST 5: HOME TASKS")
        print("=" * 60)
        
        self.get_home_tasks(limit=5)
        
        # Test 6: Main page buckets
        print("\n" + "=" * 60)
        print("TEST 6: MAIN PAGE BUCKETS")
        print("=" * 60)
        
        self.get_main_page_buckets(min_each=3)
        
        # Test 7: Delete tasks
        print("\n" + "=" * 60)
        print("TEST 7: DELETE TASKS")
        print("=" * 60)
        
        self.cleanup()
        
        print("\n" + "=" * 60)
        print("✅ ALL TESTS COMPLETED")
        print("=" * 60)


def main():
    """Main entry point"""
    print("""
╔══════════════════════════════════════════════════════════════╗
║         CRM TASK MOBILE API - TEST SCRIPT                    ║
╚══════════════════════════════════════════════════════════════╝

⚠️  CONFIGURATION REQUIRED:
   Edit the script and set:
   - BASE_URL (your Frappe site URL)
   - USERNAME (your user email)
   - PASSWORD (your password)

   Current settings:
   - BASE_URL: {BASE_URL}
   - USERNAME: {USERNAME}
   - PASSWORD: {'*' * len(PASSWORD)}

""")
    
    if BASE_URL == "https://your-site.com":
        print("❌ Please configure BASE_URL before running tests")
        print("   Edit this script and set your Frappe site URL\n")
        return
    
    response = input("Press Enter to start tests (or Ctrl+C to cancel)...")
    
    # Run tests
    tester = MobileAPITester(BASE_URL, USERNAME, PASSWORD)
    tester.run_full_test()
    
    print("\n✅ Test script completed. Review results above.\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Tests cancelled by user\n")
    except Exception as e:
        print(f"\n\n❌ Error: {str(e)}\n")

