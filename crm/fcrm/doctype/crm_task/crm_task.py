# Copyright (c) 2023, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.desk.form.assign_to import add as assign, remove as unassign
from frappe.utils import get_datetime, now_datetime
from crm.fcrm.doctype.crm_notification.crm_notification import notify_user


class CRMTask(Document):
	def after_insert(self):
		self.assign_to()

	def onload(self):
		"""التحقق من due_date وتحديث الحالة عند تحميل المستند"""
		if self.due_date and self.name:
			due_datetime = get_datetime(self.due_date)
			current_datetime = now_datetime()
			# إذا تجاوز due_date والحالة ليست Done أو late، غيّرها إلى late
			if due_datetime < current_datetime and self.status not in ["Done", "late"]:
				try:
					frappe.db.set_value("CRM Task", self.name, "status", "late", update_modified=False)
					self.status = "late"
				except Exception:
					pass

	def validate(self):
		# Check if due_date has passed and update status to late
		if self.due_date:
			due_datetime = get_datetime(self.due_date)
			current_datetime = now_datetime()
			# If due date is in the past and status is not Done, set to late
			if due_datetime < current_datetime and self.status != "Done":
				self.status = "late"

		if self.is_new() or not self.assigned_to:
			return

		if self.get_doc_before_save().assigned_to != self.assigned_to:
			self.unassign_from_previous_user(self.get_doc_before_save().assigned_to)
			self.assign_to()

	def unassign_from_previous_user(self, user):
		unassign(self.doctype, self.name, user)

	def assign_to(self):
		if self.assigned_to:
			assign({
				"assign_to": [self.assigned_to],
				"doctype": self.doctype,
				"name": self.name,
				"description": self.title or self.description,
			})


	@staticmethod
	def default_list_data():
		columns = [
			{
				'label': 'Title',
				'type': 'Data',
				'key': 'title',
				'width': '16rem',
			},
			{
				'label': 'Status',
				'type': 'Select',
				'key': 'status',
				'width': '8rem',
			},
			{
				'label': 'Priority',
				'type': 'Select',
				'key': 'priority',
				'width': '8rem',
			},
			{
				'label': 'Due Date',
				'type': 'Date',
				'key': 'due_date',
				'width': '8rem',
			},
			{
				'label': 'Assigned To',
				'type': 'Link',
				'key': 'assigned_to',
				'width': '10rem',
			},
			{
				'label': 'Last Modified',
				'type': 'Datetime',
				'key': 'modified',
				'width': '8rem',
			},
		]

		rows = [
			"name",
			"title",
			"description",
			"assigned_to",
			"due_date",
			"status",
			"priority",
			"reference_doctype",
			"reference_docname",
			"modified",
		]
		return {'columns': columns, 'rows': rows}

	@staticmethod
	def default_kanban_settings():
		return {
			"column_field": "status",
			"title_field": "title",
			"kanban_fields": '["description", "priority", "creation"]'
		}

	@staticmethod
	def parse_list_data(data):
		"""
		التحقق من due_date وتحديث الحالة في البيانات المعروضة مباشرة
		"""
		if not data:
			return data
		
		current_datetime = now_datetime()
		
		# تحديث قاعدة البيانات لجميع المهام المتأخرة أولاً
		task_names = [task.get("name") for task in data if task.get("name")]
		if task_names:
			try:
				result = frappe.db.sql("""
					UPDATE `tabCRM Task`
					SET status = 'late'
					WHERE name IN %s
					AND due_date < %s
					AND status NOT IN ('Done', 'late')
					AND due_date IS NOT NULL
				""", (task_names, current_datetime))
				frappe.db.commit()
				# مسح الـ cache بعد التحديث
				frappe.clear_cache(doctype="CRM Task")
			except Exception:
				pass
		
		# تحديث البيانات المعروضة مباشرة بناءً على due_date
		for task in data:
			if task.get("due_date") and task.get("name"):
				try:
					due_datetime = get_datetime(task["due_date"])
					if due_datetime < current_datetime:
						current_status = task.get("status")
						# إذا كانت الحالة ليست Done أو late، حدّثها مباشرة
						if current_status and current_status not in ["Done", "late"]:
							task["status"] = "late"
				except Exception:
					pass
		
		# إعادة قراءة الحالة المحدثة من قاعدة البيانات للتأكد
		if task_names:
			try:
				updated_statuses = frappe.db.sql("""
					SELECT name, status
					FROM `tabCRM Task`
					WHERE name IN %s
				""", (task_names,), as_dict=True)
				
				status_map = {t["name"]: t["status"] for t in updated_statuses}
				
				# تحديث البيانات المعروضة بالحالة المحدثة من قاعدة البيانات
				for task in data:
					task_name = task.get("name")
					if task_name in status_map:
						task["status"] = status_map[task_name]
			except Exception:
				pass
		
		return data
