# Team Leader Scripts - Complete Index

## 📋 Overview
This package provides scripts to automatically populate the `team_leader` field in CRM Leads based on user assignments from the Team doctype.

## 📁 Files

### Core Scripts

| File | Purpose | Usage |
|------|---------|-------|
| `add_team_leader_field.py` | Add team_leader field to CRM Lead | `bench --site [site] execute crm.scripts.add_team_leader_field.add_field` |
| `update_team_leader_in_leads.py` | Main batch update script | Import in console or use quick_update |
| `quick_update_team_leader.py` | Quick command-line wrapper | `bench --site [site] execute crm.scripts.quick_update_team_leader.run` |
| `auto_update_team_leader.py` | Auto-update hooks for real-time updates | Add to hooks.py |
| `test_team_leader_setup.py` | Comprehensive test suite | `bench --site [site] execute crm.scripts.test_team_leader_setup.run_all_tests` |

### Documentation

| File | Language | Content |
|------|----------|---------|
| `README.md` | English | Complete documentation |
| `README_AR.md` | Arabic | Complete documentation (Arabic) |
| `QUICK_START_AR.md` | Arabic | Quick start guide (Arabic) |
| `INDEX.md` | English | This file |

## 🚀 Quick Start

### Step 1: Test Your Setup
```bash
bench --site [site-name] execute crm.scripts.test_team_leader_setup.run_all_tests
```

### Step 2: Add Field (if needed)
```bash
bench --site [site-name] execute crm.scripts.add_team_leader_field.add_field
```

### Step 3: Dry Run
```bash
bench --site [site-name] execute crm.scripts.quick_update_team_leader.run --kwargs "{'dry_run': True}"
```

### Step 4: Actual Update
```bash
bench --site [site-name] execute crm.scripts.quick_update_team_leader.run
```

### Step 5: Enable Auto-Update (Optional)
Edit `crm/hooks.py` and add the hooks from `auto_update_team_leader.py`

## 🔧 Function Reference

### add_team_leader_field.py
- `add_field()` - Add team_leader field
- `check_field_status()` - Check field status
- `remove_field()` - Remove field (use with caution)

### update_team_leader_in_leads.py
- `update_all_leads(limit=None, dry_run=False)` - Update all leads
- `update_team_leader_for_lead(lead_name, commit=True)` - Update single lead
- `get_team_leader_for_user(user)` - Get team leader for user
- `get_assigned_users_for_lead(lead_name)` - Get assigned users
- `test_single_lead(lead_name)` - Test on single lead

### auto_update_team_leader.py
- `update_team_leader_on_lead_update(doc, method)` - Hook for lead updates
- `update_team_leader_on_lead_insert(doc, method)` - Hook for new leads
- `update_team_leader_on_todo_insert(doc, method)` - Hook for todo creation

### test_team_leader_setup.py
- `run_all_tests()` - Run all tests
- `test_field_exists()` - Test field existence
- `test_team_structure()` - Test team structure
- `test_lead_assignments()` - Test lead assignments
- `test_team_leader_resolution()` - Test resolution logic
- `test_update_functionality()` - Test update process

## 📊 How It Works

```
┌─────────────────┐
│   CRM Lead      │
│  - lead_owner   │
│  - _assign      │
│  - team_leader  │ ← We populate this
└────────┬────────┘
         │
         │ Find assigned user
         ▼
┌─────────────────┐
│   User (Sales)  │
└────────┬────────┘
         │
         │ Query Team membership
         ▼
┌─────────────────┐
│   Team          │
│  - team_leader  │ ← Get this value
│  - team_member  │
│    └─ Member    │
│       └─ member │ = User (Sales)
└─────────────────┘
```

## 🔍 SQL Query Logic

```sql
-- Find team leader for a user
SELECT t.team_leader
FROM `tabTeam` t
INNER JOIN `tabMember` m ON m.parent = t.name
WHERE m.member = 'user@example.com' 
  AND m.parenttype = 'Team'
LIMIT 1
```

## ⚙️ Configuration

### Enable Auto-Update
Add to `crm/hooks.py`:

```python
doc_events = {
    "CRM Lead": {
        "on_update": "crm.scripts.auto_update_team_leader.update_team_leader_on_lead_update",
        "after_insert": "crm.scripts.auto_update_team_leader.update_team_leader_on_lead_insert",
    },
    "ToDo": {
        "after_insert": "crm.scripts.auto_update_team_leader.update_team_leader_on_todo_insert",
    }
}
```

Then restart:
```bash
bench restart
```

## 🧪 Testing

### Run All Tests
```bash
bench --site [site] execute crm.scripts.test_team_leader_setup.run_all_tests
```

### Test Single Lead
```python
from crm.scripts.update_team_leader_in_leads import test_single_lead
test_single_lead("CRM-LEAD-2024-00001")
```

### Check Field Status
```bash
bench --site [site] execute crm.scripts.add_team_leader_field.check_field_status
```

## 📈 Monitoring

### Count Updated Leads
```python
import frappe
count = frappe.db.count("CRM Lead", {"team_leader": ["!=", ""]})
print(f"Leads with team leader: {count}")
```

### View Sample Data
```python
import frappe
leads = frappe.get_all("CRM Lead", 
    fields=["name", "lead_owner", "team_leader"],
    filters={"team_leader": ["!=", ""]},
    limit=10
)
for lead in leads:
    print(f"{lead.name}: {lead.lead_owner} → {lead.team_leader}")
```

### Check Error Log
```python
import frappe
errors = frappe.get_all("Error Log",
    filters={
        "error": ["like", "%team_leader%"],
        "creation": [">", "2024-01-01"]
    },
    fields=["name", "creation", "error"],
    order_by="creation desc",
    limit=10
)
```

## ⚠️ Troubleshooting

| Issue | Solution |
|-------|----------|
| Field doesn't exist | Run `add_team_leader_field.add_field()` |
| No updates happening | Check team structure and assignments |
| Some users not resolved | Ensure users are in Team members |
| Permission errors | Run as Administrator |

### Debug Commands

```python
# Check user's team leader
from crm.scripts.update_team_leader_in_leads import get_team_leader_for_user
get_team_leader_for_user("user@example.com")

# Check lead assignments
from crm.scripts.update_team_leader_in_leads import get_assigned_users_for_lead
get_assigned_users_for_lead("CRM-LEAD-2024-00001")

# Check team structure
import frappe
teams = frappe.get_all("Team", fields=["name", "team_leader"])
for team in teams:
    members = frappe.get_all("Member", 
        filters={"parent": team.name},
        pluck="member"
    )
    print(f"{team.name}: Leader={team.team_leader}, Members={members}")
```

## 🔐 Security

- Always backup before running
- Test on staging first
- Use dry_run mode
- Run as Administrator
- Review Error Log after

## 📞 Support

For detailed documentation:
- English: `README.md`
- Arabic: `README_AR.md`, `QUICK_START_AR.md`

For issues:
1. Run test suite
2. Check Error Log
3. Enable developer mode
4. Check bench logs

## 📝 License
MIT License (same as Frappe CRM)

---

**Created for:** CRM Team Leader Management  
**Version:** 1.0  
**Last Updated:** December 2025

