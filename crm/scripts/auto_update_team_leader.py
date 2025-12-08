#!/usr/bin/env python3
"""
Auto-update team_leader field when a CRM Lead is assigned.

This module provides functions to automatically update the team_leader field
whenever a lead is assigned to a user or when ToDo is created.

To enable auto-update, add these hooks to hooks.py:

doc_events = {
    "CRM Lead": {
        "on_update": "crm.scripts.auto_update_team_leader.update_team_leader_on_lead_update",
        "after_insert": "crm.scripts.auto_update_team_leader.update_team_leader_on_lead_insert",
    },
    "ToDo": {
        "after_insert": "crm.scripts.auto_update_team_leader.update_team_leader_on_todo_insert",
    }
}
"""

import frappe
import json
from typing import Optional


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
    
    # Check cache first
    cache_key = f"team_leader_for_{user}"
    cached = frappe.cache().get(cache_key)
    if cached:
        return cached
    
    # Find the team where this user is a member
    team_leader = frappe.db.sql("""
        SELECT t.team_leader
        FROM `tabTeam` t
        INNER JOIN `tabMember` m ON m.parent = t.name
        WHERE m.member = %s AND m.parenttype = 'Team'
        LIMIT 1
    """, (user,), as_dict=True)
    
    if team_leader and len(team_leader) > 0:
        result = team_leader[0].get('team_leader')
        # Cache for 10 minutes
        frappe.cache().setex(cache_key, result, 600)
        return result
    
    return None


def update_team_leader_for_lead(doc, method=None):
    """
    Update team_leader field based on assigned users.
    
    Args:
        doc: CRM Lead document
        method: Hook method name (not used)
    """
    # Skip if field doesn't exist
    if not hasattr(doc, 'team_leader'):
        return
    
    # Get assigned users
    assigned_users = []
    
    # From _assign field
    if doc._assign:
        try:
            assigned_list = json.loads(doc._assign)
            assigned_users.extend(assigned_list)
        except (json.JSONDecodeError, TypeError):
            pass
    
    # From lead_owner
    if hasattr(doc, 'lead_owner') and doc.lead_owner:
        if doc.lead_owner not in assigned_users:
            assigned_users.append(doc.lead_owner)
    
    # Find team leader for any assigned user
    team_leader = None
    for user in assigned_users:
        if user:
            team_leader = get_team_leader_for_user(user)
            if team_leader:
                break
    
    # Update only if changed
    if team_leader and doc.team_leader != team_leader:
        doc.team_leader = team_leader
        # Don't call save() here to avoid recursion
        # The field will be saved by the calling context


def update_team_leader_on_lead_update(doc, method=None):
    """
    Hook for CRM Lead on_update event.
    
    Args:
        doc: CRM Lead document
        method: Hook method name
    """
    # Only update if lead_owner or _assign changed
    if doc.has_value_changed('lead_owner') or doc.has_value_changed('_assign'):
        update_team_leader_for_lead(doc, method)


def update_team_leader_on_lead_insert(doc, method=None):
    """
    Hook for CRM Lead after_insert event.
    
    Args:
        doc: CRM Lead document
        method: Hook method name
    """
    update_team_leader_for_lead(doc, method)


def update_team_leader_on_todo_insert(doc, method=None):
    """
    Hook for ToDo after_insert event.
    Updates team_leader in CRM Lead when a ToDo is created.
    
    Args:
        doc: ToDo document
        method: Hook method name
    """
    # Only process if it's for a CRM Lead
    if doc.reference_type != "CRM Lead" or not doc.reference_name:
        return
    
    if not doc.allocated_to:
        return
    
    try:
        # Get the lead
        lead = frappe.get_doc("CRM Lead", doc.reference_name)
        
        # Skip if field doesn't exist
        if not hasattr(lead, 'team_leader'):
            return
        
        # Get team leader for the assigned user
        team_leader = get_team_leader_for_user(doc.allocated_to)
        
        # Update only if we found a team leader and it's different
        if team_leader and lead.team_leader != team_leader:
            lead.team_leader = team_leader
            lead.save(ignore_permissions=True)
            frappe.db.commit()
            
    except Exception as e:
        frappe.log_error(
            f"Error updating team leader for {doc.reference_name}",
            str(e)
        )


def update_team_leader_on_assign(doc, method=None):
    """
    Alternative hook that can be used with _assign field changes.
    
    Args:
        doc: CRM Lead document
        method: Hook method name
    """
    update_team_leader_for_lead(doc, method)


# Utility function to enable/disable auto-update
def set_auto_update(enabled=True):
    """
    Enable or disable auto-update of team_leader field.
    This is stored in CRM Settings.
    
    Args:
        enabled: True to enable, False to disable
    """
    # You can store this in a Settings doctype
    # For now, we'll use a simple flag
    frappe.db.set_value("CRM Settings", None, "auto_update_team_leader", enabled)
    frappe.db.commit()
    
    status = "enabled" if enabled else "disabled"
    frappe.msgprint(f"Auto-update team leader is now {status}")


def is_auto_update_enabled():
    """
    Check if auto-update is enabled.
    
    Returns:
        True if enabled, False otherwise
    """
    try:
        return frappe.db.get_single_value("CRM Settings", "auto_update_team_leader") or False
    except:
        return True  # Default to enabled if field doesn't exist

