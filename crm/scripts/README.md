# Team Leader Auto-Update Scripts for CRM Leads

## Overview
This package provides scripts to automatically populate and maintain the `team_leader` field in CRM Leads based on user assignments and Team structure.

## Files

### 1. `update_team_leader_in_leads.py`
Main script for batch updating existing CRM Leads with team leader information.

**Features:**
- Updates all historical CRM Lead records
- Finds team leaders from Team doctype
- Supports dry-run mode
- Detailed progress reporting
- Error logging

### 2. `quick_update_team_leader.py`
Quick execution wrapper for command-line usage.

### 3. `auto_update_team_leader.py`
Automatic update hooks for real-time team leader assignment.

**Features:**
- Auto-updates when leads are assigned
- Auto-updates when ToDo is created
- Caching for performance
- Can be enabled/disabled via settings

## Prerequisites

### 1. Team Leader Field
The `team_leader` field must exist in CRM Lead doctype.

**Option A: Add via Customize Form (UI)**
1. Go to CRM Lead doctype
2. Click "Customize Form"
3. Add new field:
   - Label: `Team Leader`
   - Fieldname: `team_leader`
   - Field Type: `Link`
   - Options: `User`
   - Insert After: `lead_owner` (recommended)
4. Save

**Option B: Add via bench console**
```python
from frappe.custom.doctype.custom_field.custom_field import create_custom_field

create_custom_field("CRM Lead", {
    "label": "Team Leader",
    "fieldname": "team_leader",
    "fieldtype": "Link",
    "options": "User",
    "insert_after": "lead_owner",
    "in_list_view": 1,
    "in_standard_filter": 1,
})
```

### 2. Team Structure
Ensure your Team doctype is properly configured:
- Team Leader (Link to User)
- Team Members (Table with Member child doctype)

## Usage

### Method 1: Batch Update (For Historical Data)

#### Step 1: Open Frappe Console
```bash
cd /path/to/frappe-bench
bench --site your-site-name console
```

#### Step 2: Import and Test
```python
# Import the script
from crm.scripts.update_team_leader_in_leads import *

# Test on a single lead
test_single_lead("CRM-LEAD-2024-00001")

# Dry run (no changes saved)
result = update_all_leads(dry_run=True)

# Review the results, then run actual update
result = update_all_leads()
```

### Method 2: Command Line Execution
```bash
# Dry run
bench --site your-site-name execute crm.scripts.quick_update_team_leader.run --kwargs "{'dry_run': True}"

# Actual update
bench --site your-site-name execute crm.scripts.quick_update_team_leader.run

# Update with limit
bench --site your-site-name execute crm.scripts.quick_update_team_leader.run --kwargs "{'limit': 100}"
```

### Method 3: Enable Auto-Update (For Future Leads)

To enable automatic team leader assignment for new/updated leads:

#### Step 1: Update hooks.py
Add to `/path/to/frappe-bench/apps/crm/crm/hooks.py`:

```python
doc_events = {
    # ... existing events ...
    
    "CRM Lead": {
        # ... existing hooks ...
        "on_update": "crm.scripts.auto_update_team_leader.update_team_leader_on_lead_update",
        "after_insert": "crm.scripts.auto_update_team_leader.update_team_leader_on_lead_insert",
    },
    
    "ToDo": {
        # ... existing hooks ...
        "after_insert": "crm.scripts.auto_update_team_leader.update_team_leader_on_todo_insert",
    }
}
```

#### Step 2: Restart bench
```bash
bench restart
```

Now team_leader will be automatically updated when:
- A new lead is created with an assignment
- A lead's assignment changes (lead_owner or _assign field)
- A ToDo is created for a lead

## How It Works

### Team Leader Resolution Logic

The scripts use the following logic to find team leaders:

```
1. Get assigned users from:
   - CRM Lead._assign (JSON field)
   - CRM Lead.lead_owner
   - Open ToDo records

2. For each assigned user:
   - Query Team doctype
   - Find team where user is a member
   - Get team_leader from that team
   
3. Update CRM Lead.team_leader with found team leader
```

**SQL Query Used:**
```sql
SELECT t.team_leader
FROM `tabTeam` t
INNER JOIN `tabMember` m ON m.parent = t.name
WHERE m.member = %s AND m.parenttype = 'Team'
LIMIT 1
```

## Examples

### Example 1: Update All Leads
```python
from crm.scripts.update_team_leader_in_leads import update_all_leads

# Update all leads
result = update_all_leads()

# Output:
# ============================================================
# Starting Team Leader Update for CRM Leads
# Total Leads to Process: 1250
# ============================================================
# Processing 10/1250...
# ✓ CRM-LEAD-2024-00001: Updated team_leader to user1@example.com
# ...
```

### Example 2: Update Specific Leads
```python
from crm.scripts.update_team_leader_in_leads import update_team_leader_for_lead

# Update single lead
result = update_team_leader_for_lead("CRM-LEAD-2024-00001")
print(result)
# {
#   "lead": "CRM-LEAD-2024-00001",
#   "success": True,
#   "team_leader": "manager@example.com",
#   "assigned_users": ["sales@example.com"],
#   "message": "Updated team_leader to manager@example.com"
# }
```

### Example 3: Verify Results
```python
import frappe

# Count leads with team leader
count = frappe.db.count("CRM Lead", {"team_leader": ["!=", ""]})
print(f"Leads with team leader: {count}")

# Get sample leads
leads = frappe.get_all("CRM Lead", 
    fields=["name", "lead_owner", "team_leader"],
    filters={"team_leader": ["!=", ""]},
    limit=10
)

for lead in leads:
    print(f"{lead.name}: Owner={lead.lead_owner}, Team Leader={lead.team_leader}")
```

## Performance

### Batch Update Performance
- ~100-200 leads per minute (depends on server)
- Uses database transactions for consistency
- Caching enabled in auto-update mode

### Optimization Tips
1. Run during off-peak hours
2. Use `limit` parameter for large datasets
3. Enable caching in auto-update mode

## Troubleshooting

### Issue: "team_leader field does not exist"
**Solution:** Add the field to CRM Lead doctype (see Prerequisites)

### Issue: No updates happening
**Check:**
1. Are leads assigned to users? (`_assign` or `lead_owner` must be set)
2. Are users members of teams? (Check Team doctype)
3. Are team leaders assigned in Team doctype?

**Debug Query:**
```python
import frappe

# Check team structure
teams = frappe.get_all("Team", fields=["name", "team_leader"])
for team in teams:
    members = frappe.get_all("Member", 
        filters={"parent": team.name},
        fields=["member"]
    )
    print(f"Team: {team.name}, Leader: {team.leader}, Members: {[m.member for m in members]}")
```

### Issue: Some leads not updating
**Check:**
```python
from crm.scripts.update_team_leader_in_leads import get_assigned_users_for_lead, get_team_leader_for_user

lead_name = "CRM-LEAD-2024-00001"

# Get assigned users
users = get_assigned_users_for_lead(lead_name)
print(f"Assigned users: {users}")

# Check team leader for each user
for user in users:
    leader = get_team_leader_for_user(user)
    print(f"User: {user}, Team Leader: {leader}")
```

## Safety & Best Practices

### Before Running
1. ✅ Backup your database
2. ✅ Test on a staging/development site first
3. ✅ Run with `dry_run=True` first
4. ✅ Verify team structure is correct

### During Execution
1. Monitor progress output
2. Check for errors in Error Log
3. Don't interrupt the process

### After Running
1. Verify results with sample queries
2. Check Error Log for any issues
3. Test lead assignment workflow

## Advanced Usage

### Custom Filters
Modify the script to update only specific leads:

```python
# In update_team_leader_in_leads.py, modify update_all_leads():

# Add filters
filters = {
    "status": "New",  # Only new leads
    "creation": [">", "2024-01-01"],  # Created after date
}

leads = frappe.get_all(
    "CRM Lead",
    filters=filters,  # Use custom filters
    fields=["name"],
    limit=limit
)
```

### Scheduled Updates
Create a scheduled job in hooks.py:

```python
scheduler_events = {
    "daily": [
        "crm.scripts.update_team_leader_in_leads.update_all_leads"
    ]
}
```

### Integration with Other Systems
The auto-update hooks integrate seamlessly with:
- Frappe's assignment system
- ToDo management
- Custom workflows
- API assignments

## API Reference

### update_all_leads(limit=None, dry_run=False)
Update team_leader for all CRM Leads.

**Parameters:**
- `limit` (int, optional): Maximum number of leads to process
- `dry_run` (bool, default=False): If True, don't commit changes

**Returns:** dict with summary statistics

### get_team_leader_for_user(user)
Get team leader for a specific user.

**Parameters:**
- `user` (str): User email/username

**Returns:** str (team leader email) or None

### update_team_leader_for_lead(lead_name, commit=True)
Update team_leader for a specific lead.

**Parameters:**
- `lead_name` (str): Name of CRM Lead document
- `commit` (bool, default=True): Whether to commit transaction

**Returns:** dict with update details

## Support

For issues or questions:
1. Check Error Log in Frappe
2. Enable developer mode for detailed errors
3. Check bench logs: `tail -f /path/to/bench/logs/bench.log`

## License
Same as Frappe CRM (MIT License)

