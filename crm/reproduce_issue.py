import frappe
from crm.api.doc import remove_multiple_assignments
from frappe.desk.form.assign_to import add as add_assignment
import json

def run():
    # Get a lead
    leads = frappe.get_all("CRM Lead", limit=1)
    if not leads:
        print("No leads found")
        return

    lead_name = leads[0].name
    print(f"Using lead: {lead_name}")

    # Assign to Administrator
    try:
        add_assignment({
            "doctype": "CRM Lead",
            "name": lead_name,
            "assign_to": ["Administrator"],
            "description": "Test assignment",
            "priority": "Low"
        })
        print("Assigned to Administrator")
    except Exception as e:
        print(f"Assignment failed (might already be assigned): {e}")

    # Inspect get_all return type
    todos = frappe.get_all(
        "ToDo",
        filters={
            "reference_type": "CRM Lead",
            "reference_name": ["in", [lead_name]],
            "status": ["!=", "Cancelled"],
        },
        fields=["name", "allocated_to", "reference_name"],
    )
    print(f"Todos found: {len(todos)}")
    if todos:
        print(f"Type of first todo: {type(todos[0])}")
        print(f"First todo content: {todos[0]}")
        try:
            print(f"Accessing attribute: {todos[0].reference_name}")
        except AttributeError:
            print("Attribute access failed!")

    # Try to remove assignment with ignore_permissions=True
    try:
        remove_multiple_assignments("CRM Lead", json.dumps([lead_name]), ignore_permissions=True)
        print("Successfully removed assignments")
    except Exception as e:
        print(f"Error removing assignments: {e}")
        import traceback
        traceback.print_exc()
