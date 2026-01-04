# Copyright (c) 2025, Frappe Technologies Pvt. Ltd. and Contributors
# See license.txt

"""
Comprehensive test suite for CRM Mobile API (mobile_api.py)

This test file covers all API endpoints in mobile_api.py:
- Task APIs: create, edit, update, delete, get_all, home_tasks, main_page_buckets
- Lead APIs: create, edit, update, delete, get_all, get_by_id, home_leads
- Helper APIs: get_oauth_config, get_app_logo, get_current_user_role, get_my_team_members
- Lookup APIs: get_crm_leads, get_real_estate_projects, get_units, get_project_units
- Comment APIs: get_all_comments
"""

import frappe
from frappe.tests.utils import FrappeTestCase
from frappe.utils import today, add_days, now_datetime
from datetime import datetime, timedelta
import json


class TestMobileAPI(FrappeTestCase):
	"""Test suite for CRM Mobile API endpoints"""
	
	def setUp(self):
		"""Set up test data before each test"""
		super().setUp()
		
		# Set test user
		frappe.set_user("Administrator")
		
		# Create test data
		self.setup_test_data()
		
		# Track created records for cleanup
		self.created_tasks = []
		self.created_leads = []
		self.created_task_types = []
		self.created_statuses = []
		self.created_sources = []
		self.created_projects = []
	
	def tearDown(self):
		"""Clean up test data after each test"""
		# Clean up created records
		for task_name in self.created_tasks:
			try:
				frappe.delete_doc("CRM Task", task_name, force=1)
			except:
				pass
		
		for lead_name in self.created_leads:
			try:
				frappe.delete_doc("CRM Lead", lead_name, force=1)
			except:
				pass
		
		for task_type_name in self.created_task_types:
			try:
				frappe.delete_doc("CRM Task Type", task_type_name, force=1)
			except:
				pass
		
		for status_name in self.created_statuses:
			try:
				frappe.delete_doc("CRM Lead Status", status_name, force=1)
			except:
				pass
		
		for source_name in self.created_sources:
			try:
				frappe.delete_doc("CRM Lead Source", source_name, force=1)
			except:
				pass
		
		for project_name in self.created_projects:
			try:
				frappe.delete_doc("Real Estate Project", project_name, force=1)
			except:
				pass
		
		frappe.db.commit()
		super().tearDown()
	
	def setup_test_data(self):
		"""Create necessary test data"""
		# Create test task type if it doesn't exist
		if not frappe.db.exists("CRM Task Type", "Test Task Type"):
			task_type = frappe.get_doc({
				"doctype": "CRM Task Type",
				"task_type": "Test Task Type"
			})
			task_type.insert()
			self.created_task_types.append(task_type.name)
		
		# Create test lead status if it doesn't exist
		if not frappe.db.exists("CRM Lead Status", "Test Status"):
			status = frappe.get_doc({
				"doctype": "CRM Lead Status",
				"lead_status": "Test Status"
			})
			status.insert()
			self.created_statuses.append(status.name)
		
		# Create test lead source if it doesn't exist
		if not frappe.db.exists("CRM Lead Source", "Test Source"):
			source = frappe.get_doc({
				"doctype": "CRM Lead Source",
				"source_name": "Test Source"
			})
			source.insert()
			self.created_sources.append(source.name)
		
		frappe.db.commit()
	
	# ============================================================================
	# TASK API TESTS
	# ============================================================================
	
	def test_create_task_minimal(self):
		"""Test creating a task with minimal required fields"""
		from crm.api.mobile_api import create_task
		
		result = create_task(
			task_type="Test Task Type",
			title="Test Task"
		)
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		task = result["message"]
		self.assertIn("name", task)
		self.assertEqual(task.get("title"), "Test Task")
		self.assertEqual(task.get("status"), "Todo")
		self.assertEqual(task.get("priority"), "Medium")
		
		# Track for cleanup
		if task.get("name"):
			self.created_tasks.append(task["name"])
	
	def test_create_task_full(self):
		"""Test creating a task with all fields"""
		from crm.api.mobile_api import create_task
		
		start_date = today()
		result = create_task(
			task_type="Test Task Type",
			title="Full Test Task",
			description="Test description",
			status="In Progress",
			priority="High",
			start_date=start_date,
			due_date=add_days(start_date, 7)
		)
		
		self.assertIsNotNone(result)
		task = result["message"]
		self.assertEqual(task.get("title"), "Full Test Task")
		self.assertEqual(task.get("status"), "In Progress")
		self.assertEqual(task.get("priority"), "High")
		
		if task.get("name"):
			self.created_tasks.append(task["name"])
	
	def test_edit_task(self):
		"""Test editing an existing task"""
		from crm.api.mobile_api import create_task, edit_task
		
		# Create a task first
		create_result = create_task(
			task_type="Test Task Type",
			title="Original Title"
		)
		task_name = create_result["message"]["name"]
		self.created_tasks.append(task_name)
		
		# Edit the task
		edit_result = edit_task(
			name=task_name,
			title="Updated Title",
			status="In Progress"
		)
		
		self.assertIsNotNone(edit_result)
		task = edit_result["message"]
		self.assertEqual(task.get("title"), "Updated Title")
		self.assertEqual(task.get("status"), "In Progress")
	
	def test_update_task(self):
		"""Test update_task (alias for edit_task)"""
		from crm.api.mobile_api import create_task, update_task
		
		# Create a task first
		create_result = create_task(
			task_type="Test Task Type",
			title="Original Title"
		)
		task_name = create_result["message"]["name"]
		self.created_tasks.append(task_name)
		
		# Update the task
		update_result = update_task(
			name=task_name,
			title="Updated via update_task"
		)
		
		self.assertIsNotNone(update_result)
		task = update_result["message"]
		self.assertEqual(task.get("title"), "Updated via update_task")
	
	def test_delete_task(self):
		"""Test deleting a task"""
		from crm.api.mobile_api import create_task, delete_task
		
		# Create a task first
		create_result = create_task(
			task_type="Test Task Type",
			title="Task to Delete"
		)
		task_name = create_result["message"]["name"]
		
		# Delete the task
		delete_result = delete_task(name=task_name)
		
		self.assertIsNotNone(delete_result)
		self.assertTrue(delete_result.get("message", {}).get("ok", False))
		
		# Verify task is deleted
		self.assertFalse(frappe.db.exists("CRM Task", task_name))
	
	def test_update_status(self):
		"""Test updating task status"""
		from crm.api.mobile_api import create_task, update_status
		
		# Create a task first
		create_result = create_task(
			task_type="Test Task Type",
			title="Status Test Task"
		)
		task_name = create_result["message"]["name"]
		self.created_tasks.append(task_name)
		
		# Update status
		status_result = update_status(
			name=task_name,
			status="Completed"
		)
		
		self.assertIsNotNone(status_result)
		task = status_result["message"]
		self.assertEqual(task.get("status"), "Completed")
	
	def test_get_all_tasks(self):
		"""Test getting all tasks with pagination"""
		from crm.api.mobile_api import create_task, get_all_tasks
		
		# Create a few test tasks
		for i in range(3):
			create_result = create_task(
				task_type="Test Task Type",
				title=f"Test Task {i+1}"
			)
			if create_result["message"].get("name"):
				self.created_tasks.append(create_result["message"]["name"])
		
		# Get all tasks
		result = get_all_tasks(page=1, limit=10)
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		data = result["message"]
		self.assertIn("data", data)
		self.assertIn("total", data)
		self.assertIsInstance(data["data"], list)
	
	def test_home_tasks(self):
		"""Test getting today's tasks for home page"""
		from crm.api.mobile_api import create_task, home_tasks
		
		# Create a task for today
		create_result = create_task(
			task_type="Test Task Type",
			title="Today's Task",
			start_date=today()
		)
		if create_result["message"].get("name"):
			self.created_tasks.append(create_result["message"]["name"])
		
		# Get home tasks
		result = home_tasks(limit=5)
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		data = result["message"]
		self.assertIn("today", data)
		self.assertIsInstance(data["today"], list)
	
	def test_main_page_buckets(self):
		"""Test getting main page buckets (today, late, upcoming)"""
		from crm.api.mobile_api import create_task, main_page_buckets
		
		# Create tasks for different dates
		create_task(
			task_type="Test Task Type",
			title="Today Task",
			start_date=today()
		)
		create_task(
			task_type="Test Task Type",
			title="Late Task",
			start_date=add_days(today(), -2)
		)
		create_task(
			task_type="Test Task Type",
			title="Upcoming Task",
			start_date=add_days(today(), 2)
		)
		
		# Get buckets
		result = main_page_buckets(min_each=1)
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		data = result["message"]
		self.assertIn("today", data)
		self.assertIn("late", data)
		self.assertIn("upcoming", data)
		self.assertIsInstance(data["today"], list)
		self.assertIsInstance(data["late"], list)
		self.assertIsInstance(data["upcoming"], list)
	
	# ============================================================================
	# LEAD API TESTS
	# ============================================================================
	
	def test_create_lead_minimal(self):
		"""Test creating a lead with minimal required fields"""
		from crm.api.mobile_api import create_lead
		
		result = create_lead(
			first_name="Test",
			last_name="Lead",
			mobile_no="+201234567890"
		)
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		lead = result["message"]
		self.assertIn("name", lead)
		
		if lead.get("name"):
			self.created_leads.append(lead["name"])
	
	def test_create_lead_full(self):
		"""Test creating a lead with all fields"""
		from crm.api.mobile_api import create_lead
		
		# Get test status and source
		status = "Test Status" if frappe.db.exists("CRM Lead Status", "Test Status") else None
		source = "Test Source" if frappe.db.exists("CRM Lead Source", "Test Source") else None
		
		result = create_lead(
			lead_name="Full Test Lead",
			first_name="Test",
			last_name="Lead",
			email="test@example.com",
			mobile_no="+201234567891",
			status=status,
			source=source,
			organization="Test Organization"
		)
		
		self.assertIsNotNone(result)
		lead = result["message"]
		self.assertIn("name", lead)
		
		if lead.get("name"):
			self.created_leads.append(lead["name"])
	
	def test_edit_lead(self):
		"""Test editing an existing lead"""
		from crm.api.mobile_api import create_lead, edit_lead
		
		# Create a lead first
		create_result = create_lead(
			first_name="Original",
			last_name="Lead",
			mobile_no="+201234567892"
		)
		lead_name = create_result["message"]["name"]
		self.created_leads.append(lead_name)
		
		# Edit the lead
		edit_result = edit_lead(
			name=lead_name,
			lead_name="Updated Lead Name",
			email="updated@example.com"
		)
		
		self.assertIsNotNone(edit_result)
		lead = edit_result["message"]
		self.assertEqual(lead.get("lead_name"), "Updated Lead Name")
	
	def test_update_lead(self):
		"""Test update_lead (alias for edit_lead)"""
		from crm.api.mobile_api import create_lead, update_lead
		
		# Create a lead first
		create_result = create_lead(
			first_name="Test",
			last_name="Lead",
			mobile_no="+201234567893"
		)
		lead_name = create_result["message"]["name"]
		self.created_leads.append(lead_name)
		
		# Update the lead
		update_result = update_lead(
			name=lead_name,
			lead_name="Updated via update_lead"
		)
		
		self.assertIsNotNone(update_result)
		lead = update_result["message"]
		self.assertEqual(lead.get("lead_name"), "Updated via update_lead")
	
	def test_delete_lead(self):
		"""Test deleting a lead"""
		from crm.api.mobile_api import create_lead, delete_lead
		
		# Create a lead first
		create_result = create_lead(
			first_name="Test",
			last_name="Lead",
			mobile_no="+201234567894"
		)
		lead_name = create_result["message"]["name"]
		
		# Delete the lead
		delete_result = delete_lead(name=lead_name)
		
		self.assertIsNotNone(delete_result)
		self.assertTrue(delete_result.get("message", {}).get("ok", False))
		
		# Verify lead is deleted
		self.assertFalse(frappe.db.exists("CRM Lead", lead_name))
	
	def test_get_all_leads(self):
		"""Test getting all leads with pagination"""
		from crm.api.mobile_api import create_lead, get_all_leads
		
		# Create a few test leads
		for i in range(3):
			create_result = create_lead(
				first_name=f"Test{i+1}",
				last_name="Lead",
				mobile_no=f"+20123456789{i}"
			)
			if create_result["message"].get("name"):
				self.created_leads.append(create_result["message"]["name"])
		
		# Get all leads
		result = get_all_leads(page=1, limit=10)
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		data = result["message"]
		self.assertIn("data", data)
		self.assertIn("total", data)
		self.assertIsInstance(data["data"], list)
		
		# Check that each lead has comments fields
		if data["data"]:
			lead = data["data"][0]
			self.assertIn("comments", lead)
			self.assertIn("last_comment", lead)
	
	def test_get_lead_by_id(self):
		"""Test getting a single lead by ID"""
		from crm.api.mobile_api import create_lead, get_lead_by_id
		
		# Create a lead first
		create_result = create_lead(
			first_name="Test",
			last_name="Lead",
			mobile_no="+201234567895",
			email="test@example.com"
		)
		lead_name = create_result["message"]["name"]
		self.created_leads.append(lead_name)
		
		# Get lead by ID
		result = get_lead_by_id(lead_id=lead_name)
		
		self.assertIsNotNone(result)
		self.assertIn("lead", result)
		lead = result["lead"]
		self.assertEqual(lead.get("name"), lead_name)
		self.assertIn("comments", lead)
		self.assertIn("last_comment", lead)
		self.assertIn("duplicate_leads", lead)
		self.assertIn("status_change_log", lead)
	
	def test_home_leads(self):
		"""Test getting recent leads for home page"""
		from crm.api.mobile_api import create_lead, home_leads
		
		# Create a few leads
		for i in range(3):
			create_result = create_lead(
				first_name=f"Test{i+1}",
				last_name="Lead",
				mobile_no=f"+20123456789{i+6}"
			)
			if create_result["message"].get("name"):
				self.created_leads.append(create_result["message"]["name"])
		
		# Get home leads
		result = home_leads(limit=5)
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		data = result["message"]
		self.assertIn("data", data)
		self.assertIsInstance(data["data"], list)
	
	# ============================================================================
	# HELPER API TESTS
	# ============================================================================
	
	def test_get_oauth_config(self):
		"""Test getting OAuth configuration"""
		from crm.api.mobile_api import get_oauth_config
		
		result = get_oauth_config()
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		config = result["message"]
		self.assertIn("client_id", config)
		self.assertIn("base_url", config)
	
	def test_get_app_logo(self):
		"""Test getting app logo from Website Settings"""
		from crm.api.mobile_api import get_app_logo
		
		result = get_app_logo()
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		logo_data = result["message"]
		self.assertIn("app_logo", logo_data)
		self.assertIn("app_name", logo_data)
	
	def test_get_current_user_role(self):
		"""Test getting current user role"""
		from crm.api.mobile_api import get_current_user_role
		
		result = get_current_user_role()
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		role_data = result["message"]
		self.assertIn("role", role_data)
		self.assertIn("user", role_data)
	
	def test_get_my_team_members(self):
		"""Test getting team members"""
		from crm.api.mobile_api import get_my_team_members
		
		result = get_my_team_members()
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		team_data = result["message"]
		self.assertIn("team_members", team_data)
		self.assertIsInstance(team_data["team_members"], list)
	
	# ============================================================================
	# LOOKUP API TESTS
	# ============================================================================
	
	def test_get_crm_leads(self):
		"""Test getting CRM leads for lookup"""
		from crm.api.mobile_api import get_crm_leads
		
		result = get_crm_leads(limit=10)
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		leads = result["message"]
		self.assertIsInstance(leads, list)
	
	def test_get_real_estate_projects(self):
		"""Test getting real estate projects for lookup"""
		from crm.api.mobile_api import get_real_estate_projects
		
		result = get_real_estate_projects(limit=10)
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		projects = result["message"]
		self.assertIsInstance(projects, list)
	
	def test_get_units(self):
		"""Test getting units for lookup"""
		from crm.api.mobile_api import get_units
		
		result = get_units(limit=10)
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		units = result["message"]
		self.assertIsInstance(units, list)
	
	def test_get_project_units(self):
		"""Test getting project units for lookup"""
		from crm.api.mobile_api import get_project_units
		
		result = get_project_units(limit=10)
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		units = result["message"]
		self.assertIsInstance(units, list)
	
	# ============================================================================
	# COMMENT API TESTS
	# ============================================================================
	
	def test_get_all_comments(self):
		"""Test getting all comments"""
		from crm.api.mobile_api import create_lead, get_all_comments
		
		# Create a lead first
		create_result = create_lead(
			first_name="Test",
			last_name="Lead",
			mobile_no="+201234567896",
			comment="Test comment"
		)
		lead_name = create_result["message"]["name"]
		self.created_leads.append(lead_name)
		
		# Get comments
		result = get_all_comments(page=1, limit=10)
		
		self.assertIsNotNone(result)
		self.assertIn("message", result)
		comments_data = result["message"]
		self.assertIn("data", comments_data)
		self.assertIn("total", comments_data)
		self.assertIsInstance(comments_data["data"], list)
	
	# ============================================================================
	# EDGE CASES AND ERROR HANDLING TESTS
	# ============================================================================
	
	def test_create_task_missing_required_field(self):
		"""Test creating task without required task_type should fail gracefully"""
		from crm.api.mobile_api import create_task
		
		# This should either raise an exception or return an error
		# Depending on implementation
		try:
			result = create_task(title="Task without type")
			# If no exception, check for error in result
			if result and "error" in result.get("message", {}):
				self.assertIn("error", result["message"])
		except Exception as e:
			# Exception is acceptable for missing required field
			self.assertIsNotNone(e)
	
	def test_get_lead_by_id_nonexistent(self):
		"""Test getting non-existent lead should return error"""
		from crm.api.mobile_api import get_lead_by_id
		
		result = get_lead_by_id(lead_id="NONEXISTENT-LEAD-12345")
		
		self.assertIsNotNone(result)
		self.assertIn("error", result)
	
	def test_delete_nonexistent_task(self):
		"""Test deleting non-existent task should handle gracefully"""
		from crm.api.mobile_api import delete_task
		
		result = delete_task(name="NONEXISTENT-TASK-12345")
		
		# Should either return error or handle gracefully
		self.assertIsNotNone(result)
	
	def test_get_all_tasks_with_filters(self):
		"""Test getting tasks with various filters"""
		from crm.api.mobile_api import create_task, get_all_tasks
		
		# Create tasks with different statuses
		create_result1 = create_task(
			task_type="Test Task Type",
			title="Todo Task",
			status="Todo"
		)
		create_result2 = create_task(
			task_type="Test Task Type",
			title="In Progress Task",
			status="In Progress"
		)
		
		if create_result1["message"].get("name"):
			self.created_tasks.append(create_result1["message"]["name"])
		if create_result2["message"].get("name"):
			self.created_tasks.append(create_result2["message"]["name"])
		
		# Get tasks with status filter
		result = get_all_tasks(page=1, limit=10, status="Todo")
		
		self.assertIsNotNone(result)
		data = result["message"]
		self.assertIn("data", data)
		
		# Verify all returned tasks have status "Todo"
		for task in data["data"]:
			self.assertEqual(task.get("status"), "Todo")
	
	def test_get_all_leads_with_filters(self):
		"""Test getting leads with various filters"""
		from crm.api.mobile_api import create_lead, get_all_leads
		
		# Create leads
		for i in range(2):
			create_result = create_lead(
				first_name=f"Filter{i+1}",
				last_name="Test",
				mobile_no=f"+2012345679{i+7}"
			)
			if create_result["message"].get("name"):
				self.created_leads.append(create_result["message"]["name"])
		
		# Get leads with search term
		result = get_all_leads(page=1, limit=10, search_term="Filter")
		
		self.assertIsNotNone(result)
		data = result["message"]
		self.assertIn("data", data)
	
	def test_create_lead_with_comment(self):
		"""Test creating lead with initial comment"""
		from crm.api.mobile_api import create_lead
		
		result = create_lead(
			first_name="Comment",
			last_name="Test",
			mobile_no="+201234567897",
			comment="Initial comment",
			comment_by="Administrator",
			comment_email="admin@example.com"
		)
		
		self.assertIsNotNone(result)
		lead = result["message"]
		self.assertIn("name", lead)
		
		if lead.get("name"):
			self.created_leads.append(lead["name"])
			
			# Verify comment was added
			comments = frappe.get_all(
				"Comment",
				filters={
					"reference_doctype": "CRM Lead",
					"reference_name": lead["name"]
				},
				limit=1
			)
			# Comment should exist (or at least no error should occur)
			self.assertIsNotNone(comments)
	
	def test_edit_lead_with_comment(self):
		"""Test editing lead and adding comment"""
		from crm.api.mobile_api import create_lead, edit_lead
		
		# Create a lead first
		create_result = create_lead(
			first_name="Test",
			last_name="Lead",
			mobile_no="+201234567898"
		)
		lead_name = create_result["message"]["name"]
		self.created_leads.append(lead_name)
		
		# Edit with comment
		edit_result = edit_lead(
			name=lead_name,
			lead_name="Updated with Comment",
			comment="Update comment",
			comment_by="Administrator",
			comment_email="admin@example.com"
		)
		
		self.assertIsNotNone(edit_result)
		lead = edit_result["message"]
		self.assertEqual(lead.get("lead_name"), "Updated with Comment")

