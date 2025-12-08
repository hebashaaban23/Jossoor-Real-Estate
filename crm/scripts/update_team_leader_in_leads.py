#!/usr/bin/env python3
"""
Script to update team_leader field in CRM Lead based on assigned users.

This script:
1. Gets all CRM Leads
2. For each lead, finds the assigned user(s) from _assign field or lead_owner
3. Finds the team_leader for that user from Team doctype
4. Updates the team_leader field in CRM Lead

Run this script using:
    bench --site [your-site] execute crm.scripts.update_team_leader_in_leads.update_all_leads
"""

import frappe
import json
from typing import Optional, Set, List


def get_team_leader_for_user(user: str) -> Optional[str]:
    """
    Get the team leader for a given user.
    
    Args:
        user: Email/username of the user
    
    Returns:
        Team leader email/username or None if user has no team leader
    """
    if not user:
        return None
    
    # Find the team where this user is a member
    team_leader = frappe.db.sql("""
        SELECT t.team_leader
        FROM `tabTeam` t
        INNER JOIN `tabMember` m ON m.parent = t.name
        WHERE m.member = %s AND m.parenttype = 'Team'
        LIMIT 1
    """, (user,), as_dict=True)
    
    if team_leader and len(team_leader) > 0:
        return team_leader[0].get('team_leader')
    
    return None


def get_assigned_users_for_lead(lead_name: str) -> List[str]:
    """
    Get all assigned users for a lead.
    Checks both _assign field and lead_owner.
    
    Args:
        lead_name: Name of the CRM Lead document
    
    Returns:
        List of assigned user emails
    """
    lead = frappe.get_doc("CRM Lead", lead_name)
    assigned_users = []
    
    # Get users from _assign field
    if lead._assign:
        try:
            assigned_list = json.loads(lead._assign)
            assigned_users.extend(assigned_list)
        except (json.JSONDecodeError, TypeError):
            pass
    
    # Get lead_owner
    if hasattr(lead, 'lead_owner') and lead.lead_owner:
        if lead.lead_owner not in assigned_users:
            assigned_users.append(lead.lead_owner)
    
    # Also check ToDo assignments
    todos = frappe.get_all(
        "ToDo",
        filters={
            "reference_type": "CRM Lead",
            "reference_name": lead_name,
            "status": "Open"
        },
        fields=["allocated_to"]
    )
    
    for todo in todos:
        if todo.allocated_to and todo.allocated_to not in assigned_users:
            assigned_users.append(todo.allocated_to)
    
    return list(set(filter(None, assigned_users)))  # Remove duplicates and None values


def update_team_leader_for_lead(lead_name: str, commit: bool = True) -> dict:
    """
    Update team_leader field for a specific lead.
    
    Args:
        lead_name: Name of the CRM Lead document
        commit: Whether to commit the transaction
    
    Returns:
        Dictionary with status and details
    """
    result = {
        "lead": lead_name,
        "success": False,
        "team_leader": None,
        "assigned_users": [],
        "message": ""
    }
    
    try:
        # Get assigned users
        assigned_users = get_assigned_users_for_lead(lead_name)
        result["assigned_users"] = assigned_users
        
        if not assigned_users:
            result["message"] = "No assigned users found"
            return result
        
        # Try to find team leader for any of the assigned users
        team_leader = None
        for user in assigned_users:
            team_leader = get_team_leader_for_user(user)
            if team_leader:
                break
        
        if not team_leader:
            result["message"] = "No team leader found for assigned users"
            return result
        
        # Update the lead
        lead = frappe.get_doc("CRM Lead", lead_name)
        
        # Check if team_leader field exists
        if not hasattr(lead, 'team_leader'):
            result["message"] = "team_leader field does not exist in CRM Lead"
            return result
        
        # Only update if different
        if lead.team_leader != team_leader:
            lead.team_leader = team_leader
            lead.save(ignore_permissions=True)
            
            if commit:
                frappe.db.commit()
            
            result["success"] = True
            result["team_leader"] = team_leader
            result["message"] = f"Updated team_leader to {team_leader}"
        else:
            result["success"] = True
            result["team_leader"] = team_leader
            result["message"] = "Team leader already set correctly"
        
    except Exception as e:
        result["message"] = f"Error: {str(e)}"
        frappe.log_error(f"Error updating team leader for {lead_name}", str(e))
    
    return result


def update_all_leads(limit: int = None, dry_run: bool = False) -> dict:
    """
    Update team_leader for all CRM Leads.
    
    Args:
        limit: Maximum number of leads to process (None for all)
        dry_run: If True, don't commit changes
    
    Returns:
        Dictionary with summary statistics
    """
    frappe.set_user("Administrator")
    
    summary = {
        "total_leads": 0,
        "processed": 0,
        "updated": 0,
        "skipped_no_assignment": 0,
        "skipped_no_team_leader": 0,
        "errors": 0,
        "failed_leads": []
    }
    
    # Get all CRM Leads
    filters = {}
    leads = frappe.get_all(
        "CRM Lead",
        filters=filters,
        fields=["name"],
        limit=limit
    )
    
    summary["total_leads"] = len(leads)
    
    print(f"\n{'=' * 60}")
    print(f"Starting Team Leader Update for CRM Leads")
    print(f"Total Leads to Process: {summary['total_leads']}")
    print(f"Dry Run: {dry_run}")
    print(f"{'=' * 60}\n")
    
    for idx, lead in enumerate(leads, 1):
        lead_name = lead.name
        
        # Show progress every 10 leads
        if idx % 10 == 0:
            print(f"Processing {idx}/{summary['total_leads']}...")
        
        result = update_team_leader_for_lead(lead_name, commit=not dry_run)
        summary["processed"] += 1
        
        if result["success"]:
            if result["team_leader"]:
                summary["updated"] += 1
                print(f"✓ {lead_name}: {result['message']}")
        else:
            if "No assigned users" in result["message"]:
                summary["skipped_no_assignment"] += 1
            elif "No team leader found" in result["message"]:
                summary["skipped_no_team_leader"] += 1
            elif "does not exist" in result["message"]:
                print(f"\n❌ ERROR: {result['message']}")
                print(f"Please add 'team_leader' field to CRM Lead doctype first!")
                return summary
            else:
                summary["errors"] += 1
                summary["failed_leads"].append({
                    "lead": lead_name,
                    "error": result["message"]
                })
                print(f"✗ {lead_name}: {result['message']}")
    
    # Print summary
    print(f"\n{'=' * 60}")
    print(f"Summary:")
    print(f"{'=' * 60}")
    print(f"Total Leads:                {summary['total_leads']}")
    print(f"Processed:                  {summary['processed']}")
    print(f"Successfully Updated:       {summary['updated']}")
    print(f"Skipped (No Assignment):    {summary['skipped_no_assignment']}")
    print(f"Skipped (No Team Leader):   {summary['skipped_no_team_leader']}")
    print(f"Errors:                     {summary['errors']}")
    print(f"{'=' * 60}\n")
    
    if summary["failed_leads"]:
        print("Failed Leads:")
        for failed in summary["failed_leads"][:10]:  # Show first 10
            print(f"  - {failed['lead']}: {failed['error']}")
        if len(summary["failed_leads"]) > 10:
            print(f"  ... and {len(summary['failed_leads']) - 10} more")
    
    if not dry_run:
        frappe.db.commit()
        print("\n✓ All changes committed to database")
    else:
        print("\n⚠ DRY RUN - No changes were committed")
    
    return summary


def update_leads_by_filter(filters: dict = None, dry_run: bool = False) -> dict:
    """
    Update team_leader for CRM Leads matching specific filters.
    
    Args:
        filters: Frappe filters dict (e.g., {"status": "New"})
        dry_run: If True, don't commit changes
    
    Returns:
        Dictionary with summary statistics
    """
    if filters is None:
        filters = {}
    
    return update_all_leads(limit=None, dry_run=dry_run)


# Quick test function
def test_single_lead(lead_name: str):
    """Test the script on a single lead."""
    frappe.set_user("Administrator")
    result = update_team_leader_for_lead(lead_name, commit=False)
    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    # This allows running the script directly for testing
    print("This script should be run from Frappe bench console")
    print("Example commands:")
    print("  bench --site [site-name] console")
    print("  >>> from crm.scripts.update_team_leader_in_leads import *")
    print("  >>> update_all_leads(dry_run=True)  # Test run")
    print("  >>> update_all_leads()  # Actual update")

