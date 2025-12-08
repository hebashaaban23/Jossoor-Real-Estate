# Verification Plan
## CRM Mobile API Fix - Testing & Validation

**Date**: December 3, 2025  
**Fix Applied**: ✅ Added `from . import mobile_api  # noqa` to `crm/api/__init__.py` (line 10)  
**Status**: Ready for Testing

---

## Fix Summary

**What Changed**: ONE line added to `/home/frappe/frappe-bench-env/frappe-bench/apps/crm/crm/api/__init__.py`

```diff
 from .lead_filters import lead_filter_options  # noqa
+from . import mobile_api  # noqa
```

**Why This Fixes It**: Forces import of `mobile_api` module at web worker startup, causing `@frappe.whitelist()` decorators to execute and register functions in Frappe's whitelist registry.

**Risk Level**: Minimal (one import line, no logic changes)

---

## MANDATORY: Restart Workers

**Before any HTTP testing**, workers MUST be restarted for the import to take effect:

```bash
cd /home/frappe/frappe-bench-env/frappe-bench
bench restart
```

**Expected Output**:
```
$ bench restart
Restarting bench...
```

**Wait**: 10-15 seconds for workers to fully restart before testing.

---

## Verification Steps

### Step 1: Verify File Change ✅

```bash
# Confirm the import was added
grep -n "mobile_api" /home/frappe/frappe-bench-env/frappe-bench/apps/crm/crm/api/__init__.py

# Expected output:
# 10:from . import mobile_api  # noqa
```

**Status**: ✅ Verified (line 10)

---

### Step 2: Verify No Linter Errors ✅

```bash
cd /home/frappe/frappe-bench-env/frappe-bench/apps/crm
python -m py_compile crm/api/__init__.py

# Expected: No output (success)
# If errors, they would print
```

**Status**: ✅ No linter errors

---

### Step 3: Restart Bench Workers (REQUIRED)

```bash
cd /home/frappe/frappe-bench-env/frappe-bench
bench restart

# Wait 10 seconds for workers to restart
sleep 10
```

**Status**: ⏳ Awaiting execution (you must run this)

---

### Step 4: Verify Module Import in Console

```bash
bench --site trust.jossoor.org console
```

**Test 1: Check module in sys.modules**
```python
import sys
print('crm.api.mobile_api' in sys.modules)
# Expected: True ✅
```

**Test 2: Get function via frappe.get_attr**
```python
import frappe
func = frappe.get_attr('crm.api.mobile_api.home_tasks')
print(func)
# Expected: <function home_tasks at 0x...> ✅
print(type(func))
# Expected: <class 'function'> ✅
```

**Test 3: Verify decorator attributes**
```python
print(hasattr(func, '_allow_guest'))
# Expected: True ✅
print(func._allow_guest)
# Expected: False ✅
print(hasattr(func, '_request_methods'))
# Expected: True ✅
print(func._request_methods)
# Expected: ['GET'] ✅
```

**Test 4: Call function directly in console**
```python
result = func(limit=2)
print(result)
# Expected: {'today': [...], 'limit': 2} ✅
```

**Exit console**: `exit()`

---

### Step 5: Test HTTP Endpoints

#### 5.1: Login and Get Session Cookie

```bash
SITE="https://trust.jossoor.org"

# Login (replace with real credentials)
curl -X POST "$SITE/api/method/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "usr=admin@example.com&pwd=yourpassword" \
  -c cookies.txt \
  -v 2>&1 | grep -E "HTTP/|Set-Cookie:"

# Expected:
# < HTTP/2 200
# < Set-Cookie: sid=...
# < Set-Cookie: user_id=...
# < Set-Cookie: full_name=...
```

**Verify cookies saved**:
```bash
cat cookies.txt | grep -E "sid|user_id"
# Expected: Lines containing session cookies
```

---

#### 5.2: Test home_tasks Endpoint

```bash
SITE="https://trust.jossoor.org"

# Test home_tasks with limit=5
curl -s "$SITE/api/method/crm.api.mobile_api.home_tasks?limit=5" \
  -b cookies.txt \
  -H "Accept: application/json" | jq .
```

**Expected Success Response**:
```json
{
  "message": {
    "today": [
      {
        "name": "12345",
        "title": "Task Title",
        "status": "Todo",
        "priority": "High",
        "start_date": "2025-12-03",
        "modified": "2025-12-03 10:30:00"
      }
    ],
    "limit": 5
  }
}
```

**Check Response**:
- ✅ HTTP 200 status
- ✅ No "not whitelisted" error
- ✅ Valid JSON structure
- ✅ Has `message.today` array
- ✅ Has `message.limit` integer

**If Still Getting "Not Whitelisted"**:
- Did you restart bench? (Step 3)
- Check if you're hitting the correct site
- Check nginx routing (see troubleshooting below)

---

#### 5.3: Test filter_tasks with Date Range

```bash
# Test with both date_from and date_to (the critical test!)
curl -s "$SITE/api/method/crm.api.mobile_api.filter_tasks?date_from=2025-12-01&date_to=2025-12-31&limit=10" \
  -b cookies.txt \
  -H "Accept: application/json" | jq .
```

**Expected Success Response**:
```json
{
  "message": {
    "data": [
      {
        "name": "12345",
        "title": "Task Title",
        "status": "Todo",
        "priority": "High",
        "start_date": "2025-12-15",
        "modified": "2025-12-15 09:00:00"
      }
    ]
  }
}
```

**Verify**:
- ✅ HTTP 200 status
- ✅ No "not whitelisted" error
- ✅ Valid JSON with `message.data` array
- ✅ Tasks returned are within date range (2025-12-01 to 2025-12-31)
- ✅ Both date filters applied (not just one)

---

#### 5.4: Test filter_tasks with Multiple Filters

```bash
# Test combined filters
curl -s "$SITE/api/method/crm.api.mobile_api.filter_tasks?date_from=2025-12-01&importance=High,Medium&status=Todo,In%20Progress&limit=20" \
  -b cookies.txt \
  -H "Accept: application/json" | jq .
```

**Expected**:
- ✅ Returns tasks matching ALL filters
- ✅ Date: >= 2025-12-01
- ✅ Priority: High OR Medium
- ✅ Status: Todo OR In Progress

---

#### 5.5: Test main_page_buckets

```bash
curl -s "$SITE/api/method/crm.api.mobile_api.main_page_buckets?min_each=5" \
  -b cookies.txt \
  -H "Accept: application/json" | jq .
```

**Expected Success Response**:
```json
{
  "message": {
    "today": [ /* tasks with start_date = today */ ],
    "late": [ /* tasks with start_date < today, active status */ ],
    "upcoming": [ /* tasks with start_date > today */ ],
    "min_each": 5
  }
}
```

**Verify**:
- ✅ HTTP 200 status
- ✅ Three arrays: `today`, `late`, `upcoming`
- ✅ Each has >= 0 tasks (depending on data)
- ✅ `min_each` value returned

---

#### 5.6: Test POST Endpoints (Optional but Recommended)

**Create Task**:
```bash
curl -X POST "$SITE/api/method/crm.api.mobile_api.create_task" \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{
    "task_type": "Call",
    "title": "Test Task from API",
    "priority": "High",
    "status": "Todo"
  }' | jq .
```

**Expected**:
```json
{
  "message": {
    "name": "12346",
    "title": "Test Task from API",
    "status": "Todo",
    "priority": "High",
    "start_date": "2025-12-03",
    "modified": "2025-12-03 11:00:00"
  }
}
```

---

### Step 6: Verify Other Endpoints Still Work

**Test existing CRM API endpoint** (sanity check):

```bash
curl -s "$SITE/api/method/crm.api.get_translations" \
  -b cookies.txt | jq . | head -20
```

**Expected**: Returns translations JSON (proves we didn't break existing functionality)

---

### Step 7: Check Error Logs

```bash
# Check for any Python errors during worker restart
tail -50 /home/frappe/frappe-bench-env/frappe-bench/logs/bench-start.log | grep -i error

# Expected: No import errors related to mobile_api
```

---

## Success Criteria Checklist

After completing all tests above:

- ✅ File change applied (line 10 of `crm/api/__init__.py`)
- ✅ No linter errors
- ✅ Workers restarted successfully
- ✅ Module in `sys.modules` (console test)
- ✅ Functions have correct decorator attributes
- ✅ HTTP 200 on `home_tasks` endpoint
- ✅ HTTP 200 on `filter_tasks` with date range
- ✅ Both `date_from` AND `date_to` filters apply correctly
- ✅ HTTP 200 on `main_page_buckets` endpoint
- ✅ No "not whitelisted" errors
- ✅ Other CRM endpoints still work
- ✅ No import errors in logs

**ALL MUST BE ✅ FOR FIX TO BE VERIFIED**

---

## Troubleshooting

### Issue 1: Still Getting "Not Whitelisted" After Restart

**Check 1: Verify restart worked**
```bash
ps aux | grep frappe | grep -v grep
# Look for recent start times
```

**Check 2: Force reload**
```bash
bench restart
# OR
supervisorctl restart all
```

**Check 3: Clear Python bytecode cache**
```bash
find /home/frappe/frappe-bench-env/frappe-bench/apps/crm -name "*.pyc" -delete
find /home/frappe/frappe-bench-env/frappe-bench/apps/crm -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null
bench restart
```

---

### Issue 2: Import Error on Restart

**Check logs**:
```bash
tail -100 /home/frappe/frappe-bench-env/frappe-bench/logs/bench-start.log
```

**If circular import**:
- Rollback: Remove line 10 from `crm/api/__init__.py`
- Restart: `bench restart`

---

### Issue 3: Works in Console but Not HTTP

**Check nginx routing**:
```bash
# Check which bench nginx points to
sudo cat /etc/nginx/sites-enabled/* | grep -A5 "trust.jossoor.org"

# Look for upstream and proxy_pass
```

**Verify site exists on bench**:
```bash
bench --site trust.jossoor.org list-apps | grep crm
# Expected: crm listed
```

---

### Issue 4: 401 Unauthorized

**Issue**: Session cookies not being sent

**Solution**:
```bash
# Re-login and get fresh cookies
rm cookies.txt
curl -X POST "$SITE/api/method/login" \
  -d "usr=admin@example.com&pwd=password" \
  -c cookies.txt
```

---

## Rollback Procedure (If Needed)

### If Fix Causes Problems

```bash
cd /home/frappe/frappe-bench-env/frappe-bench/apps/crm

# Remove the added line
# Edit crm/api/__init__.py and delete line 10:
# from . import mobile_api  # noqa

# OR use git if under version control:
git checkout crm/api/__init__.py

# Restart workers
cd /home/frappe/frappe-bench-env/frappe-bench
bench restart
```

**Verify rollback worked**:
```bash
grep "mobile_api" apps/crm/crm/api/__init__.py
# Expected: No output (line removed)
```

**System should return to previous state** (not whitelisted, but at least not broken further).

---

## Post-Verification Actions

### 1. Document Success

Record test results:
```bash
# Create success report
cat > /home/frappe/frappe-bench-env/frappe-bench/apps/crm/VERIFICATION_RESULTS.txt <<EOF
Verification Date: $(date)
Fix Applied: Yes (line 10 of crm/api/__init__.py)
Workers Restarted: Yes
All Tests Passed: Yes/No

Test Results:
- Module import in console: Pass/Fail
- home_tasks HTTP: Pass/Fail  
- filter_tasks HTTP: Pass/Fail
- main_page_buckets HTTP: Pass/Fail
- Date range filters: Pass/Fail
- Other endpoints still work: Pass/Fail

Status: SUCCESS / FAILED
Notes: [Add any relevant notes]
EOF
```

---

### 2. Clean Up Diagnostic Files (No Temp Files Created)

All investigation was file-based, no temporary diagnostic code was added. Nothing to clean up.

---

### 3. Update Documentation

No documentation updates needed - the fix is transparent to users.

---

## Final Validation Commands (Quick Copy-Paste)

```bash
# STEP 1: Restart bench (REQUIRED)
cd /home/frappe/frappe-bench-env/frappe-bench && bench restart && sleep 10

# STEP 2: Login
SITE="https://trust.jossoor.org"
curl -X POST "$SITE/api/method/login" -d "usr=admin@example.com&pwd=password" -c cookies.txt

# STEP 3: Test home_tasks
curl -s "$SITE/api/method/crm.api.mobile_api.home_tasks?limit=5" -b cookies.txt | jq .

# STEP 4: Test filter_tasks with date range
curl -s "$SITE/api/method/crm.api.mobile_api.filter_tasks?date_from=2025-12-01&date_to=2025-12-31&limit=10" -b cookies.txt | jq .

# STEP 5: Test main_page_buckets
curl -s "$SITE/api/method/crm.api.mobile_api.main_page_buckets?min_each=5" -b cookies.txt | jq .

# If all return valid JSON (not "not whitelisted"): SUCCESS ✅
```

---

## Contact & Support

**If tests fail after following this plan**:
1. Check error logs
2. Verify bench restart completed
3. Confirm you're testing the correct site
4. Review troubleshooting section above

---

**Status**: Fix Applied ✅  
**Awaiting**: User to restart bench and run verification tests  
**Expected Result**: All endpoints return 200 OK with valid JSON  
**ETA**: 5 minutes to verify

---

*Created: December 3, 2025*  
*Fix Applied: Line 10 of crm/api/__init__.py*  
*Ready for Testing: YES*

