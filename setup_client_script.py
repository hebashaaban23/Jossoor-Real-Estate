#!/usr/bin/env python3
"""
Setup Client Script for Lead
Run with: bench --site <site> console < setup_client_script.py
"""
import frappe
import json
import os

script_name = "Filtered Buttons"
doctype = "Lead"

# Get the script from fixtures
fixtures_path = os.path.join(frappe.get_app_path("crm"), "fixtures", "client_script.json")
with open(fixtures_path, 'r') as f:
    fixtures = json.load(f)

# Find the script
script_data = None
for fixture in fixtures:
    if fixture.get("name") == script_name and fixture.get("dt") == doctype:
        script_data = fixture
        break

if not script_data:
    print(f"✗ Script '{script_name}' not found in fixtures")
    exit(1)

# Create or update Client Script
if frappe.db.exists("Client Script", {"name": script_name, "dt": doctype}):
    script = frappe.get_doc("Client Script", script_name)
    print(f"Updating existing Client Script: {script_name}")
else:
    script = frappe.new_doc("Client Script")
    script.name = script_name
    script.dt = doctype
    script.view = "List"
    script.enabled = 1
    script.module = "FCRM"
    print(f"Creating new Client Script: {script_name}")

# Update script content
script.script = script_data["script"]
script.docstatus = script_data.get("docstatus", 0)

script.save()
frappe.db.commit()

print(f"✓ Client Script '{script_name}' saved successfully")
print(f"  - Contains 'assign_without_rule': {'assign_without_rule' in script.script}")
print(f"  - Contains 'names: validNames': {'names: validNames' in script.script}")

