#!/usr/bin/env python3
"""
Create/Update Client Script for Lead to use assign_without_rule correctly
"""
import frappe
import json

def create_or_update_client_script():
    """Create or update the Client Script"""
    
    script_name = "Filtered Buttons"
    doctype = "Lead"
    
    # Check if script exists
    if frappe.db.exists("Client Script", {"name": script_name, "dt": doctype}):
        script = frappe.get_doc("Client Script", script_name)
        print(f"Found existing Client Script: {script_name}")
    else:
        script = frappe.new_doc("Client Script")
        script.name = script_name
        script.dt = doctype
        script.view = "List"
        script.enabled = 1
        script.module = "FCRM"
        print(f"Creating new Client Script: {script_name}")
    
    # Read the script from fixtures
    fixtures_path = frappe.get_app_path("crm", "fixtures", "client_script.json")
    with open(fixtures_path, 'r') as f:
        fixtures = json.load(f)
    
    # Find the script in fixtures
    for fixture in fixtures:
        if fixture.get("name") == script_name and fixture.get("dt") == doctype:
            script.script = fixture["script"]
            script.docstatus = fixture.get("docstatus", 0)
            script.modified = fixture.get("modified")
            break
    
    script.save()
    frappe.db.commit()
    print(f"✓ Client Script '{script_name}' saved successfully")
    print(f"  - Script contains 'assign_without_rule': {'assign_without_rule' in script.script}")
    print(f"  - Script contains 'names: validNames': {'names: validNames' in script.script}")

if __name__ == '__main__':
    import sys
    site = sys.argv[1] if len(sys.argv) > 1 else 'jossoor.local'
    frappe.init(site=site)
    frappe.connect()
    create_or_update_client_script()
    frappe.destroy()

