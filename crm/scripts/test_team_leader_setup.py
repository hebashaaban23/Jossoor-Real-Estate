#!/usr/bin/env python3
"""
Comprehensive test script for team_leader functionality.

This script tests:
1. Field existence
2. Team structure
3. User assignments
4. Team leader resolution
5. Update functionality

Usage:
    bench --site [site-name] execute crm.scripts.test_team_leader_setup.run_all_tests
"""

import frappe
import json
from typing import Dict, List


def test_field_exists() -> Dict:
    """Test if team_leader field exists in CRM Lead."""
    print("\n" + "="*60)
    print("TEST 1: Checking team_leader field existence")
    print("="*60)
    
    result = {
        "test_name": "Field Existence",
        "passed": False,
        "message": ""
    }
    
    try:
        meta = frappe.get_meta("CRM Lead")
        field_exists = meta.has_field("team_leader")
        
        if field_exists:
            field = meta.get_field("team_leader")
            result["passed"] = True
            result["message"] = "✓ Field exists"
            print(f"✓ PASSED: team_leader field exists")
            print(f"  - Field Type: {field.fieldtype}")
            print(f"  - Options: {field.options}")
        else:
            result["message"] = "✗ Field does not exist"
            print(f"✗ FAILED: team_leader field does not exist")
            print(f"  → Run: bench --site [site] execute crm.scripts.add_team_leader_field.add_field")
    
    except Exception as e:
        result["message"] = f"✗ Error: {str(e)}"
        print(f"✗ ERROR: {str(e)}")
    
    return result


def test_team_structure() -> Dict:
    """Test if Team doctype has proper structure."""
    print("\n" + "="*60)
    print("TEST 2: Checking Team structure")
    print("="*60)
    
    result = {
        "test_name": "Team Structure",
        "passed": False,
        "message": "",
        "teams_count": 0,
        "teams_with_members": 0
    }
    
    try:
        # Get all teams
        teams = frappe.get_all("Team", fields=["name", "team_leader"])
        result["teams_count"] = len(teams)
        
        if len(teams) == 0:
            result["message"] = "⚠ No teams found"
            print(f"⚠ WARNING: No teams found in the system")
            print(f"  → Create teams with team leaders and members")
            return result
        
        print(f"Found {len(teams)} team(s)")
        
        # Check each team
        teams_with_members = 0
        for team in teams[:5]:  # Show first 5
            members = frappe.get_all("Member", 
                filters={"parent": team.name, "parenttype": "Team"},
                fields=["member"]
            )
            
            if len(members) > 0:
                teams_with_members += 1
            
            print(f"\n  Team: {team.name}")
            print(f"  Leader: {team.team_leader}")
            print(f"  Members: {len(members)}")
            if len(members) > 0:
                for member in members[:3]:  # Show first 3 members
                    print(f"    - {member.member}")
                if len(members) > 3:
                    print(f"    ... and {len(members) - 3} more")
        
        if len(teams) > 5:
            print(f"\n  ... and {len(teams) - 5} more teams")
        
        result["teams_with_members"] = teams_with_members
        
        if teams_with_members > 0:
            result["passed"] = True
            result["message"] = f"✓ Found {teams_with_members} teams with members"
            print(f"\n✓ PASSED: Team structure is valid")
        else:
            result["message"] = "⚠ Teams exist but have no members"
            print(f"\n⚠ WARNING: Teams exist but have no members")
            print(f"  → Add members to teams")
    
    except Exception as e:
        result["message"] = f"✗ Error: {str(e)}"
        print(f"✗ ERROR: {str(e)}")
    
    return result


def test_lead_assignments() -> Dict:
    """Test if CRM Leads have assignments."""
    print("\n" + "="*60)
    print("TEST 3: Checking CRM Lead assignments")
    print("="*60)
    
    result = {
        "test_name": "Lead Assignments",
        "passed": False,
        "message": "",
        "total_leads": 0,
        "leads_with_owner": 0,
        "leads_with_assign": 0
    }
    
    try:
        # Count total leads
        total = frappe.db.count("CRM Lead")
        result["total_leads"] = total
        
        if total == 0:
            result["message"] = "⚠ No leads found"
            print(f"⚠ WARNING: No CRM Leads found in the system")
            return result
        
        print(f"Total Leads: {total}")
        
        # Count leads with lead_owner
        with_owner = frappe.db.count("CRM Lead", {"lead_owner": ["!=", ""]})
        result["leads_with_owner"] = with_owner
        print(f"Leads with lead_owner: {with_owner}")
        
        # Count leads with _assign
        with_assign = frappe.db.sql("""
            SELECT COUNT(*) as count
            FROM `tabCRM Lead`
            WHERE `_assign` IS NOT NULL AND `_assign` != '[]' AND `_assign` != ''
        """, as_dict=True)[0].count
        result["leads_with_assign"] = with_assign
        print(f"Leads with _assign: {with_assign}")
        
        # Show some examples
        sample_leads = frappe.get_all("CRM Lead",
            filters={"lead_owner": ["!=", ""]},
            fields=["name", "lead_owner", "_assign"],
            limit=3
        )
        
        if sample_leads:
            print(f"\nSample Leads:")
            for lead in sample_leads:
                assign_list = []
                if lead._assign:
                    try:
                        assign_list = json.loads(lead._assign)
                    except:
                        pass
                print(f"  {lead.name}")
                print(f"    Owner: {lead.lead_owner}")
                if assign_list:
                    print(f"    Assigned: {', '.join(assign_list)}")
        
        if with_owner > 0 or with_assign > 0:
            result["passed"] = True
            result["message"] = "✓ Leads have assignments"
            print(f"\n✓ PASSED: Leads have assignments")
        else:
            result["message"] = "⚠ No leads have assignments"
            print(f"\n⚠ WARNING: No leads have assignments")
            print(f"  → Assign leads to users")
    
    except Exception as e:
        result["message"] = f"✗ Error: {str(e)}"
        print(f"✗ ERROR: {str(e)}")
    
    return result


def test_team_leader_resolution() -> Dict:
    """Test if team leaders can be resolved for assigned users."""
    print("\n" + "="*60)
    print("TEST 4: Testing team leader resolution")
    print("="*60)
    
    result = {
        "test_name": "Team Leader Resolution",
        "passed": False,
        "message": "",
        "tested_users": [],
        "successful_resolutions": 0
    }
    
    try:
        from crm.scripts.update_team_leader_in_leads import get_team_leader_for_user
        
        # Get some users who are lead owners
        users = frappe.db.sql("""
            SELECT DISTINCT lead_owner as user
            FROM `tabCRM Lead`
            WHERE lead_owner IS NOT NULL AND lead_owner != ''
            LIMIT 5
        """, as_dict=True)
        
        if not users:
            result["message"] = "⚠ No users to test"
            print(f"⚠ WARNING: No users found with lead assignments")
            return result
        
        print(f"Testing team leader resolution for {len(users)} user(s):")
        
        successful = 0
        for user_row in users:
            user = user_row.user
            team_leader = get_team_leader_for_user(user)
            
            result["tested_users"].append({
                "user": user,
                "team_leader": team_leader
            })
            
            print(f"\n  User: {user}")
            if team_leader:
                print(f"    Team Leader: {team_leader} ✓")
                successful += 1
            else:
                print(f"    Team Leader: Not found ✗")
        
        result["successful_resolutions"] = successful
        
        if successful > 0:
            result["passed"] = True
            result["message"] = f"✓ Resolved {successful}/{len(users)} users"
            print(f"\n✓ PASSED: Team leader resolution working ({successful}/{len(users)})")
        else:
            result["message"] = "✗ Could not resolve any team leaders"
            print(f"\n✗ FAILED: Could not resolve any team leaders")
            print(f"  → Ensure users are added as members in Team doctype")
    
    except Exception as e:
        result["message"] = f"✗ Error: {str(e)}"
        print(f"✗ ERROR: {str(e)}")
    
    return result


def test_update_functionality() -> Dict:
    """Test the update functionality on a sample lead."""
    print("\n" + "="*60)
    print("TEST 5: Testing update functionality")
    print("="*60)
    
    result = {
        "test_name": "Update Functionality",
        "passed": False,
        "message": "",
        "tested_lead": None
    }
    
    try:
        from crm.scripts.update_team_leader_in_leads import update_team_leader_for_lead
        
        # Get a lead with assignment
        lead = frappe.db.sql("""
            SELECT name
            FROM `tabCRM Lead`
            WHERE lead_owner IS NOT NULL AND lead_owner != ''
            LIMIT 1
        """, as_dict=True)
        
        if not lead:
            result["message"] = "⚠ No suitable lead found for testing"
            print(f"⚠ WARNING: No suitable lead found for testing")
            return result
        
        lead_name = lead[0].name
        result["tested_lead"] = lead_name
        
        print(f"Testing update on lead: {lead_name}")
        
        # Run update (with commit=False for safety)
        update_result = update_team_leader_for_lead(lead_name, commit=False)
        
        print(f"\nUpdate Result:")
        print(f"  Success: {update_result['success']}")
        print(f"  Team Leader: {update_result['team_leader']}")
        print(f"  Assigned Users: {update_result['assigned_users']}")
        print(f"  Message: {update_result['message']}")
        
        if update_result["success"]:
            result["passed"] = True
            result["message"] = "✓ Update functionality working"
            print(f"\n✓ PASSED: Update functionality working")
        else:
            result["message"] = f"✗ Update failed: {update_result['message']}"
            print(f"\n✗ FAILED: {update_result['message']}")
    
    except Exception as e:
        result["message"] = f"✗ Error: {str(e)}"
        print(f"✗ ERROR: {str(e)}")
    
    return result


def run_all_tests() -> Dict:
    """
    Run all tests and generate a comprehensive report.
    
    Returns:
        dict: Test results summary
    """
    frappe.set_user("Administrator")
    
    print("\n" + "="*60)
    print("TEAM LEADER FUNCTIONALITY - COMPREHENSIVE TESTS")
    print("="*60)
    
    results = []
    
    # Run all tests
    results.append(test_field_exists())
    results.append(test_team_structure())
    results.append(test_lead_assignments())
    results.append(test_team_leader_resolution())
    results.append(test_update_functionality())
    
    # Generate summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for r in results if r["passed"])
    total = len(results)
    
    for i, result in enumerate(results, 1):
        status = "✓ PASS" if result["passed"] else "✗ FAIL"
        print(f"{i}. {result['test_name']}: {status}")
        print(f"   {result['message']}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! You're ready to run the update script.")
        print("\nNext step:")
        print("  bench --site [site] execute crm.scripts.quick_update_team_leader.run --kwargs \"{'dry_run': True}\"")
    else:
        print("\n⚠ Some tests failed. Please fix the issues before running the update script.")
        print("\nRecommended actions:")
        if not results[0]["passed"]:
            print("  1. Add team_leader field: bench --site [site] execute crm.scripts.add_team_leader_field.add_field")
        if not results[1]["passed"]:
            print("  2. Create teams with leaders and members in Team doctype")
        if not results[2]["passed"]:
            print("  3. Assign leads to users")
        if not results[3]["passed"]:
            print("  4. Add users to teams as members")
    
    print("="*60 + "\n")
    
    return {
        "total_tests": total,
        "passed": passed,
        "failed": total - passed,
        "all_passed": passed == total,
        "results": results
    }


def quick_test():
    """Quick test - just check if everything is ready."""
    result = run_all_tests()
    return result["all_passed"]


if __name__ == "__main__":
    print("Please run this script through bench:")
    print("  bench --site [site-name] execute crm.scripts.test_team_leader_setup.run_all_tests")

