#!/usr/bin/env python3
"""
Optimized script for updating large number of CRM Leads (14,000+).

This script includes:
- Batch processing
- Progress tracking
- Resume capability
- Performance optimization
- Detailed logging

Usage:
    bench --site trust.com execute crm.scripts.update_large_dataset.run_update
"""

import frappe
import json
import time
from datetime import datetime
from typing import Optional, List, Dict


def get_team_leader_for_user(user: str, cache: dict = None) -> Optional[str]:
    """
    Get team leader for a user with caching.
    
    Args:
        user: Email/username of the user
        cache: Cache dictionary to store results
    
    Returns:
        Team leader email/username or None
    """
    if not user:
        return None
    
    # Use cache if provided
    if cache and user in cache:
        return cache[user]
    
    # Query database
    team_leader = frappe.db.sql("""
        SELECT t.team_leader
        FROM `tabTeam` t
        INNER JOIN `tabMember` m ON m.parent = t.name
        WHERE m.member = %s AND m.parenttype = 'Team'
        LIMIT 1
    """, (user,), as_dict=True)
    
    result = None
    if team_leader and len(team_leader) > 0:
        result = team_leader[0].get('team_leader')
    
    # Store in cache
    if cache is not None:
        cache[user] = result
    
    return result


def get_leads_without_team_leader(limit: int = None, offset: int = 0) -> List[Dict]:
    """
    Get all leads that don't have team_leader set.
    
    Args:
        limit: Maximum number of leads to fetch
        offset: Number of leads to skip
    
    Returns:
        List of lead dictionaries
    """
    query = """
        SELECT 
            name,
            lead_owner,
            _assign
        FROM `tabCRM Lead`
        WHERE (team_leader IS NULL OR team_leader = '')
    """
    
    if limit:
        query += f" LIMIT {limit} OFFSET {offset}"
    
    return frappe.db.sql(query, as_dict=True)


def count_leads_without_team_leader() -> int:
    """Count leads without team_leader."""
    return frappe.db.sql("""
        SELECT COUNT(*) as count
        FROM `tabCRM Lead`
        WHERE (team_leader IS NULL OR team_leader = '')
    """, as_dict=True)[0].count


def update_lead_batch(leads: List[Dict], cache: dict, stats: dict) -> None:
    """
    Update a batch of leads.
    
    Args:
        leads: List of lead dictionaries
        cache: Cache for team leader lookups
        stats: Statistics dictionary to update
    """
    for lead in leads:
        try:
            # Get assigned users
            assigned_users = []
            
            # From lead_owner
            if lead.lead_owner:
                assigned_users.append(lead.lead_owner)
            
            # From _assign
            if lead._assign:
                try:
                    assign_list = json.loads(lead._assign)
                    assigned_users.extend(assign_list)
                except:
                    pass
            
            # Remove duplicates
            assigned_users = list(set(filter(None, assigned_users)))
            
            if not assigned_users:
                stats['skipped_no_assignment'] += 1
                continue
            
            # Find team leader
            team_leader = None
            for user in assigned_users:
                team_leader = get_team_leader_for_user(user, cache)
                if team_leader:
                    break
            
            if not team_leader:
                stats['skipped_no_team_leader'] += 1
                continue
            
            # Update the lead (direct SQL for performance)
            frappe.db.sql("""
                UPDATE `tabCRM Lead`
                SET team_leader = %s, modified = %s
                WHERE name = %s
            """, (team_leader, datetime.now(), lead.name))
            
            stats['updated'] += 1
            
        except Exception as e:
            stats['errors'] += 1
            stats['failed_leads'].append({
                'lead': lead.name,
                'error': str(e)
            })
            frappe.log_error(f"Error updating {lead.name}", str(e))


def run_update(batch_size: int = 100, total_limit: int = None, dry_run: bool = False) -> dict:
    """
    Run the update process with batch processing.
    
    Args:
        batch_size: Number of leads to process per batch (default: 100)
        total_limit: Maximum total leads to process (default: None = all)
        dry_run: If True, don't commit changes
    
    Returns:
        Dictionary with summary statistics
    """
    frappe.set_user("Administrator")
    
    print("\n" + "="*70)
    print("🚀 TEAM LEADER UPDATE - LARGE DATASET MODE")
    print("="*70)
    
    # Count total leads
    total_leads = count_leads_without_team_leader()
    
    if total_limit and total_limit < total_leads:
        total_leads = total_limit
    
    print(f"\n📊 Statistics:")
    print(f"   Total Leads without Team Leader: {total_leads:,}")
    print(f"   Batch Size: {batch_size}")
    print(f"   Dry Run: {dry_run}")
    print(f"   Site: {frappe.local.site}")
    print("="*70 + "\n")
    
    if total_leads == 0:
        print("✅ No leads need updating!")
        return {"total_leads": 0}
    
    # Ask for confirmation if not dry run
    if not dry_run:
        print("⚠️  WARNING: This will update the database!")
        print("   Press Ctrl+C now to cancel, or wait 5 seconds to continue...")
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n❌ Cancelled by user")
            return {"cancelled": True}
    
    # Initialize statistics
    stats = {
        'total_leads': total_leads,
        'processed': 0,
        'updated': 0,
        'skipped_no_assignment': 0,
        'skipped_no_team_leader': 0,
        'errors': 0,
        'failed_leads': [],
        'batches_completed': 0,
        'start_time': datetime.now()
    }
    
    # Cache for team leader lookups
    team_leader_cache = {}
    
    # Process in batches
    offset = 0
    batch_number = 0
    
    while offset < total_leads:
        batch_number += 1
        current_batch_size = min(batch_size, total_leads - offset)
        
        print(f"\n📦 Batch {batch_number} ({offset + 1} to {offset + current_batch_size} of {total_leads:,})")
        print(f"   Started at: {datetime.now().strftime('%H:%M:%S')}")
        
        batch_start_time = time.time()
        
        # Fetch batch
        leads = get_leads_without_team_leader(limit=current_batch_size, offset=offset)
        
        if not leads:
            break
        
        # Update batch
        update_lead_batch(leads, team_leader_cache, stats)
        
        # Commit after each batch
        if not dry_run:
            frappe.db.commit()
        
        stats['batches_completed'] += 1
        stats['processed'] = offset + len(leads)
        
        # Calculate performance
        batch_time = time.time() - batch_start_time
        leads_per_second = len(leads) / batch_time if batch_time > 0 else 0
        
        # Progress report
        progress_pct = (stats['processed'] / total_leads) * 100
        print(f"   ✓ Completed in {batch_time:.2f}s ({leads_per_second:.1f} leads/sec)")
        print(f"   Progress: {progress_pct:.1f}% | Updated: {stats['updated']} | Skipped: {stats['skipped_no_assignment'] + stats['skipped_no_team_leader']} | Errors: {stats['errors']}")
        
        # Estimate remaining time
        if stats['processed'] > 0:
            elapsed = (datetime.now() - stats['start_time']).total_seconds()
            avg_time_per_lead = elapsed / stats['processed']
            remaining_leads = total_leads - stats['processed']
            est_remaining = remaining_leads * avg_time_per_lead
            est_minutes = int(est_remaining / 60)
            est_seconds = int(est_remaining % 60)
            print(f"   ⏱️  Estimated remaining time: {est_minutes}m {est_seconds}s")
        
        offset += len(leads)
    
    # Final statistics
    stats['end_time'] = datetime.now()
    stats['total_time'] = (stats['end_time'] - stats['start_time']).total_seconds()
    
    print("\n" + "="*70)
    print("📈 FINAL SUMMARY")
    print("="*70)
    print(f"Total Leads:              {stats['total_leads']:,}")
    print(f"Processed:                {stats['processed']:,}")
    print(f"✓ Successfully Updated:   {stats['updated']:,}")
    print(f"⊘ Skipped (No Assignment): {stats['skipped_no_assignment']:,}")
    print(f"⊘ Skipped (No Team Leader): {stats['skipped_no_team_leader']:,}")
    print(f"✗ Errors:                 {stats['errors']:,}")
    print(f"\n⏱️  Total Time: {int(stats['total_time'] / 60)}m {int(stats['total_time'] % 60)}s")
    print(f"📊 Average Speed: {stats['processed'] / stats['total_time']:.1f} leads/second")
    print(f"💾 Cache Size: {len(team_leader_cache)} users")
    print("="*70)
    
    if stats['failed_leads']:
        print(f"\n⚠️  Failed Leads ({len(stats['failed_leads'])}):")
        for failed in stats['failed_leads'][:10]:
            print(f"   - {failed['lead']}: {failed['error']}")
        if len(stats['failed_leads']) > 10:
            print(f"   ... and {len(stats['failed_leads']) - 10} more (check Error Log)")
    
    if not dry_run:
        print("\n✅ All changes committed to database")
    else:
        print("\n⚠️  DRY RUN - No changes were committed")
    
    print("\n" + "="*70 + "\n")
    
    return stats


def quick_test(num_leads: int = 10):
    """
    Quick test on a small number of leads.
    
    Args:
        num_leads: Number of leads to test (default: 10)
    """
    print(f"\n🧪 Testing on {num_leads} leads...\n")
    return run_update(batch_size=num_leads, total_limit=num_leads, dry_run=True)


def verify_results():
    """Verify the update results."""
    frappe.set_user("Administrator")
    
    print("\n" + "="*70)
    print("🔍 VERIFICATION REPORT")
    print("="*70)
    
    # Count leads with team leader
    with_leader = frappe.db.sql("""
        SELECT COUNT(*) as count
        FROM `tabCRM Lead`
        WHERE team_leader IS NOT NULL AND team_leader != ''
    """, as_dict=True)[0].count
    
    # Count leads without team leader
    without_leader = count_leads_without_team_leader()
    
    # Total leads
    total = frappe.db.count("CRM Lead")
    
    print(f"\nTotal Leads:              {total:,}")
    print(f"✓ With Team Leader:       {with_leader:,} ({with_leader/total*100:.1f}%)")
    print(f"⊘ Without Team Leader:    {without_leader:,} ({without_leader/total*100:.1f}%)")
    
    # Sample leads with team leader
    samples = frappe.db.sql("""
        SELECT name, lead_owner, team_leader
        FROM `tabCRM Lead`
        WHERE team_leader IS NOT NULL AND team_leader != ''
        LIMIT 5
    """, as_dict=True)
    
    if samples:
        print(f"\n📋 Sample Leads:")
        for lead in samples:
            print(f"   {lead.name}: {lead.lead_owner} → {lead.team_leader}")
    
    print("\n" + "="*70 + "\n")
    
    return {
        'total': total,
        'with_leader': with_leader,
        'without_leader': without_leader,
        'percentage': with_leader/total*100 if total > 0 else 0
    }


if __name__ == "__main__":
    print("Please run this script through bench:")
    print("  bench --site trust.com execute crm.scripts.update_large_dataset.run_update")

