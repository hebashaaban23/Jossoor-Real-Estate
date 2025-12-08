# Copyright (c) 2025, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt
import frappe
from frappe.model.document import Document


#class DuplicateLeadEntry(Document):

	
#	pass



from frappe import _
class DuplicateLeadEntry(Document):
    def validate(self):
        # امنع أي self-link من المصدر نفسه
        if self.parent == self.lead:
            frappe.throw(_("A lead cannot be added as a duplicate of itself."))
