#!/usr/bin/env python3
"""
Script to add team_leader field to CRM Lead doctype if it doesn't exist.

This will create a custom field that links to the User doctype.

Usage:
    bench --site [site-name] execute crm.scripts.add_team_leader_field.add_field
"""

import frappe
from frappe.custom.doctype.custom_field.custom_field import create_custom_field


def field_exists():
    """Check if team_leader field already exists."""
    # Check in custom fields
    exists_custom = frappe.db.exists("Custom Field", {
        "dt": "CRM Lead",
        "fieldname": "team_leader"
    })
    
    # Check in doctype fields (in case it's a standard field)
    meta = frappe.get_meta("CRM Lead")
    exists_standard = meta.has_field("team_leader")
    
    return exists_custom or exists_standard


def add_field():
    """
    Add team_leader field to CRM Lead doctype.
    
    Returns:
        dict: Status of the operation
    """
    frappe.set_user("Administrator")
    
    result = {
        "success": False,
        "message": "",
        "field_name": "team_leader"
    }
    
    try:
        # Check if field already exists
        if field_exists():
            result["message"] = "Field 'team_leader' already exists in CRM Lead"
            result["success"] = True
            print("✓ " + result["message"])
            return result
        
        # Create the custom field
        print("Adding 'team_leader' field to CRM Lead...")
        
        create_custom_field("CRM Lead", {
            "label": "Team Leader",
            "fieldname": "team_leader",
            "fieldtype": "Link",
            "options": "User",
            "insert_after": "lead_owner",
            "in_list_view": 1,
            "in_standard_filter": 1,
            "in_filter": 1,
            "read_only": 0,
            "allow_on_submit": 0,
            "permlevel": 0,
            "description": "Team leader of the assigned user (auto-populated from Team doctype)"
        })
        
        frappe.db.commit()
        
        result["success"] = True
        result["message"] = "Successfully added 'team_leader' field to CRM Lead"
        
        print("✓ " + result["message"])
        print("\nField Details:")
        print("  - Label: Team Leader")
        print("  - Fieldname: team_leader")
        print("  - Type: Link (User)")
        print("  - Location: After 'lead_owner'")
        print("  - Visible in: List View, Filters")
        
        return result
        
    except Exception as e:
        result["message"] = f"Error adding field: {str(e)}"
        print("✗ " + result["message"])
        frappe.log_error("Error adding team_leader field", str(e))
        return result


def remove_field():
    """
    Remove team_leader field from CRM Lead doctype.
    Use with caution - this will delete data!
    
    Returns:
        dict: Status of the operation
    """
    frappe.set_user("Administrator")
    
    result = {
        "success": False,
        "message": "",
        "field_name": "team_leader"
    }
    
    try:
        # Check if field exists
        if not field_exists():
            result["message"] = "Field 'team_leader' does not exist in CRM Lead"
            result["success"] = True
            print(result["message"])
            return result
        
        # Find the custom field
        custom_field = frappe.db.get_value("Custom Field", {
            "dt": "CRM Lead",
            "fieldname": "team_leader"
        }, "name")
        
        if custom_field:
            print("Removing 'team_leader' field from CRM Lead...")
            frappe.delete_doc("Custom Field", custom_field)
            frappe.db.commit()
            
            result["success"] = True
            result["message"] = "Successfully removed 'team_leader' field from CRM Lead"
            print("✓ " + result["message"])
        else:
            result["message"] = "Field exists but is not a custom field (standard field)"
            print("⚠ " + result["message"])
        
        return result
        
    except Exception as e:
        result["message"] = f"Error removing field: {str(e)}"
        print("✗ " + result["message"])
        frappe.log_error("Error removing team_leader field", str(e))
        return result


def check_field_status():
    """
    Check the status of team_leader field.
    
    Returns:
        dict: Field status information
    """
    frappe.set_user("Administrator")
    
    status = {
        "exists": False,
        "is_custom": False,
        "field_type": None,
        "options": None,
        "in_list_view": False,
        "in_filter": False,
        "records_with_value": 0
    }
    
    try:
        # Check if field exists
        status["exists"] = field_exists()
        
        if status["exists"]:
            # Check if it's a custom field
            custom_field = frappe.db.get_value("Custom Field", {
                "dt": "CRM Lead",
                "fieldname": "team_leader"
            }, ["name", "fieldtype", "options", "in_list_view", "in_filter"], as_dict=True)
            
            if custom_field:
                status["is_custom"] = True
                status["field_type"] = custom_field.fieldtype
                status["options"] = custom_field.options
                status["in_list_view"] = custom_field.in_list_view
                status["in_filter"] = custom_field.in_filter
            else:
                # It's a standard field
                meta = frappe.get_meta("CRM Lead")
                field = meta.get_field("team_leader")
                if field:
                    status["field_type"] = field.fieldtype
                    status["options"] = field.options
                    status["in_list_view"] = field.in_list_view
                    status["in_filter"] = field.in_filter
            
            # Count records with team_leader value
            try:
                status["records_with_value"] = frappe.db.count("CRM Lead", {
                    "team_leader": ["!=", ""]
                })
            except:
                pass
        
        # Print status
        print("\n" + "="*60)
        print("Team Leader Field Status")
        print("="*60)
        print(f"Field Exists: {status['exists']}")
        
        if status["exists"]:
            print(f"Field Type: {status['is_custom'] and 'Custom Field' or 'Standard Field'}")
            print(f"Data Type: {status['field_type']}")
            print(f"Options: {status['options']}")
            print(f"In List View: {status['in_list_view']}")
            print(f"In Filter: {status['in_filter']}")
            print(f"Records with Value: {status['records_with_value']}")
        else:
            print("⚠ Field does not exist. Run add_field() to create it.")
        
        print("="*60 + "\n")
        
        return status
        
    except Exception as e:
        print(f"Error checking field status: {str(e)}")
        frappe.log_error("Error checking team_leader field status", str(e))
        return status


if __name__ == "__main__":
    print("Please run this script through bench:")
    print("  bench --site [site-name] execute crm.scripts.add_team_leader_field.add_field")
    print("  bench --site [site-name] execute crm.scripts.add_team_leader_field.check_field_status")

