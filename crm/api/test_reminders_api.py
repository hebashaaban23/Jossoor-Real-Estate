# -*- coding: utf-8 -*-
from __future__ import annotations
import frappe
from frappe.utils import now_datetime, add_to_date
from frappe.tests.utils import FrappeTestCase

from crm.crm.api.reminders import get_delayed_map, recalc_delayed_for_doc


class TestDelayedFlag(FrappeTestCase):
    def setUp(self):
        self.addCleanup(self._restore_user)
        self.original_user = frappe.session.user
        self.created_docs: list[frappe.model.document.Document] = []

        self.lead = frappe.get_doc(
            {
                "doctype": "CRM Lead",
                "lead_name": "Delayed Flag Test Lead",
            }
        ).insert(ignore_permissions=True)
        self.created_docs.append(self.lead)

        self.initial_comment = self._add_comment("Initial comment (placeholder).")

        self.user_a = self._ensure_user("delayed.agent.a@example.com")
        self.user_b = self._ensure_user("delayed.agent.b@example.com")

    def tearDown(self):
        frappe.set_user(self.original_user)
        for doc in reversed(self.created_docs):
            if doc and doc.name and frappe.db.exists(doc.doctype, doc.name):
                doc.delete(ignore_permissions=True)

    def _restore_user(self):
        frappe.set_user(self.original_user)

    def _ensure_user(self, email: str) -> str:
        if frappe.db.exists("User", email):
            return email
        user = frappe.get_doc(
            {
                "doctype": "User",
                "email": email,
                "first_name": email.split("@")[0],
                "enabled": 1,
                "send_welcome_email": 0,
                "roles": [{"role": "System Manager"}],
            }
        ).insert(ignore_permissions=True)
        self.created_docs.append(user)
        return user.name

    def _add_comment(self, text: str, *, hours_offset: int | None = None):
        comment = frappe.get_doc(
            {
                "doctype": "Comment",
                "comment_type": "Comment",
                "reference_doctype": "CRM Lead",
                "reference_name": self.lead.name,
                "content": text,
            }
        ).insert(ignore_permissions=True)
        self.created_docs.append(comment)
        if hours_offset is not None:
            frappe.db.set_value(
                "Comment",
                comment.name,
                "creation",
                add_to_date(now_datetime(), hours=hours_offset),
                update_modified=False,
            )
        return comment

    def _make_overdue_reminder(self, *, user: str, hours_offset: int = -1):
        remind_at = add_to_date(now_datetime(), hours=hours_offset)
        data = {
            "doctype": "Reminder",
            "reference_doctype": "CRM Lead",
            "reference_name": self.lead.name,
            "remind_at": remind_at,
            "description": "Follow-up for delayed flag test",
        }
        if frappe.db.has_column("Reminder", "user"):
            data["user"] = user
        if frappe.db.has_column("Reminder", "status"):
            data["status"] = "Open"
        reminder = frappe.get_doc(data).insert(ignore_permissions=True)
        self.created_docs.append(reminder)
        return reminder

    def test_per_user_delayed_flow(self):
        """
        سيناريو كامل:
        - مستخدم A لديه Reminder متأخر وآخر Comment أقدم منه → delayed=1.
        - إضافة Comment أحدث من Reminder → delayed=0.
        - مستخدم B (بدون Reminder) لا يرى العلامة إطلاقًا.
        """
        frappe.set_user(self.user_a)

        # اجعل أول تعليق أقدم من التذكير
        frappe.db.set_value(
            "Comment",
            self.initial_comment.name,
            "creation",
            add_to_date(now_datetime(), hours=-4),
            update_modified=False,
        )

        self._make_overdue_reminder(user=self.user_a, hours_offset=-2)
        recalc_delayed_for_doc("CRM Lead", self.lead.name)

        delayed_for_a = get_delayed_map([self.lead.name])
        self.assertEqual(
            delayed_for_a.get(self.lead.name),
            1,
            "Lead should be delayed for User A when no newer comment exists.",
        )
        self.assertEqual(
            frappe.db.get_value("CRM Lead", self.lead.name, "delayed"),
            1,
            "CRM Lead.delayed field should mirror delayed state for User A.",
        )

        frappe.set_user(self.user_b)
        delayed_for_b = get_delayed_map([self.lead.name])
        self.assertEqual(
            delayed_for_b.get(self.lead.name),
            0,
            "User B should not inherit User A reminders.",
        )

        frappe.set_user(self.user_a)
        self._add_comment("Fresh comment clearing the delay.")
        recalc_delayed_for_doc("CRM Lead", self.lead.name)

        delayed_after_comment = get_delayed_map([self.lead.name])
        self.assertEqual(
            delayed_after_comment.get(self.lead.name),
            0,
            "Latest comment after reminder should clear delayed flag for User A.",
        )
        self.assertEqual(
            frappe.db.get_value("CRM Lead", self.lead.name, "delayed"),
            0,
            "CRM Lead.delayed should clear once a newer comment exists.",
        )

        frappe.set_user(self.user_b)
        delayed_for_b_after = get_delayed_map([self.lead.name])
        self.assertEqual(
            delayed_for_b_after.get(self.lead.name),
            0,
            "User B stays unaffected even after User A resolves their reminder.",
        )
