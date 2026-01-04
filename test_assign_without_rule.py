#!/usr/bin/env python3
"""
Test script for assign_without_rule function
"""
import frappe
import json

def test_assign_without_rule():
    """Test the assign_without_rule function with different parameter combinations"""
    
    # Test 1: With names as list
    print("Test 1: With names as list")
    try:
        result = frappe.call(
            'crm.api.doc.assign_without_rule',
            doctype='Lead',
            assign_to=['test@example.com'],
            names=['LEAD-001', 'LEAD-002'],
            description='Test assignment'
        )
        print(f"✓ Success: {result}")
    except Exception as e:
        print(f"✗ Failed: {e}")
    
    # Test 2: With names as JSON string
    print("\nTest 2: With names as JSON string")
    try:
        result = frappe.call(
            'crm.api.doc.assign_without_rule',
            doctype='Lead',
            assign_to=['test@example.com'],
            names=json.dumps(['LEAD-001', 'LEAD-002']),
            description='Test assignment'
        )
        print(f"✓ Success: {result}")
    except Exception as e:
        print(f"✗ Failed: {e}")
    
    # Test 3: With name (single document)
    print("\nTest 3: With name (single document)")
    try:
        result = frappe.call(
            'crm.api.doc.assign_without_rule',
            doctype='Lead',
            assign_to=['test@example.com'],
            name='LEAD-001',
            description='Test assignment'
        )
        print(f"✓ Success: {result}")
    except Exception as e:
        print(f"✗ Failed: {e}")
    
    # Test 4: Without names (should fail with helpful error)
    print("\nTest 4: Without names (should fail)")
    try:
        result = frappe.call(
            'crm.api.doc.assign_without_rule',
            doctype='Lead',
            assign_to=['test@example.com'],
            description='Test assignment'
        )
        print(f"✗ Unexpected success: {result}")
    except Exception as e:
        print(f"✓ Expected error: {e}")

if __name__ == '__main__':
    frappe.init(site='your-site-name')
    frappe.connect()
    test_assign_without_rule()
    frappe.destroy()

