#!/usr/bin/env python3
"""
Quick script to update team_leader in CRM Leads.
Can be run directly from command line.

Usage:
    bench --site [site-name] execute crm.scripts.quick_update_team_leader.run --kwargs "{'dry_run': True}"
    bench --site [site-name] execute crm.scripts.quick_update_team_leader.run
"""

import frappe


def run(dry_run=False, limit=None):
    """
    Quick execution function.
    
    Args:
        dry_run: If True, don't commit changes (default: False)
        limit: Maximum number of leads to process (default: None = all)
    """
    from crm.scripts.update_team_leader_in_leads import update_all_leads
    
    print("\n" + "="*60)
    print("تحديث Team Leader في CRM Leads")
    print("="*60 + "\n")
    
    result = update_all_leads(limit=limit, dry_run=dry_run)
    
    return result


if __name__ == "__main__":
    print("يرجى تشغيل هذا السكريبت من خلال bench:")
    print("bench --site [site-name] execute crm.scripts.quick_update_team_leader.run")

