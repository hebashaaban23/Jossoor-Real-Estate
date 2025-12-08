# Minimal Fix Proposal
## CRM Mobile API "Not Whitelisted" Error

**Date**: December 3, 2025  
**Root Cause**: Module not imported at web worker startup (see DIAGNOSTIC_REPORT.md)

---

## The Fix

### ONE LINE ADDITION

**File**: `/home/frappe/frappe-bench-env/frappe-bench/apps/crm/crm/api/__init__.py`  
**Location**: After existing imports (line 9)  
**Change**: Add ONE import statement

```python
from . import mobile_api  # noqa
```

**That's it.** One line.

---

## Why This Fix Works

### Current Behavior (Broken)

```
Web Worker Starts
  → Import frappe
  → Import crm (crm/__init__.py)
  → Import crm.api (crm/api/__init__.py)
    ✅ Imports frappe, BeautifulSoup, etc.
    ❌ Does NOT import mobile_api
  → mobile_api.py never executed
  → Decorators never run
  → Functions never registered
  → HTTP requests fail with "not whitelisted"
```

### After Fix (Working)

```
Web Worker Starts
  → Import frappe
  → Import crm (crm/__init__.py)
  → Import crm.api (crm/api/__init__.py)
    ✅ Imports frappe, BeautifulSoup, etc.
    ✅ Imports mobile_api (from . import mobile_api)
      → mobile_api.py executes
      → All @frappe.whitelist() decorators run
      → Functions register in whitelist
      → HTTP requests work ✅
```

### Mechanism

1. **Python Import Semantics**: When `crm.api.__init__.py` is imported, any `from . import X` statements execute
2. **Decorator Execution**: When `mobile_api.py` is imported, Python executes all module-level code, including decorators
3. **Frappe Registration**: `@frappe.whitelist()` decorator adds function paths to Frappe's internal registry
4. **HTTP Routing**: When HTTP request arrives, Frappe looks up path in registry → finds it → calls function

**Critical**: This happens at import time, not request time. Web workers must import the module once at startup.

---

## Why This Is Minimal

### What We're NOT Doing
- ❌ NOT modifying business logic
- ❌ NOT changing function signatures
- ❌ NOT altering response formats
- ❌ NOT adding new hooks
- ❌ NOT creating new files
- ❌ NOT changing decorators
- ❌ NOT modifying nginx/gunicorn config
- ❌ NOT touching site config
- ❌ NOT adding guest access

### What We ARE Doing
- ✅ ONE line import statement
- ✅ Standard Python import pattern
- ✅ Same pattern used for other submodules (e.g., `from .lead_filters import lead_filter_options`)
- ✅ No functional changes
- ✅ No security changes
- ✅ No API changes

---

## Precedent in Same File

**File**: `crm/api/__init__.py`  
**Line 9**: `from .lead_filters import lead_filter_options  # noqa`

**Why it works**: This imports `lead_filters` module, making its functions available via API. We're doing the exact same thing for `mobile_api`.

**Pattern**: Import submodules in package `__init__.py` to ensure they load at package import time.

---

## Implementation

### Before (Lines 1-11 of crm/api/__init__.py)

```python
import frappe
from bs4 import BeautifulSoup
from frappe.core.api.file import get_max_file_size
from frappe.translate import get_all_translations
from frappe.utils import cstr, split_emails, validate_email_address
#from frappe.utils.modules import get_modules_from_all_apps_for_user
from frappe.config import get_modules_from_all_apps_for_user
from frappe.utils.telemetry import POSTHOG_HOST_FIELD, POSTHOG_PROJECT_FIELD
from .lead_filters import lead_filter_options  # noqa


```

### After (Lines 1-12 of crm/api/__init__.py)

```python
import frappe
from bs4 import BeautifulSoup
from frappe.core.api.file import get_max_file_size
from frappe.translate import get_all_translations
from frappe.utils import cstr, split_emails, validate_email_address
#from frappe.utils.modules import get_modules_from_all_apps_for_user
from frappe.config import get_modules_from_all_apps_for_user
from frappe.utils.telemetry import POSTHOG_HOST_FIELD, POSTHOG_PROJECT_FIELD
from .lead_filters import lead_filter_options  # noqa
from . import mobile_api  # noqa


```

**Diff**:
```diff
 from frappe.utils.telemetry import POSTHOG_HOST_FIELD, POSTHOG_PROJECT_FIELD
 from .lead_filters import lead_filter_options  # noqa
+from . import mobile_api  # noqa
 
 
```

---

## Safety Analysis

### Risk: Import Errors
**Mitigation**: `mobile_api.py` already exists and has no syntax errors (verified by linter)

### Risk: Circular Imports
**Mitigation**: `mobile_api.py` only imports `frappe`, `frappe._`, `frappe.utils` - no circular dependencies

### Risk: Performance Impact
**Impact**: One additional module imported at worker startup (~1ms)  
**Assessment**: Negligible - happens once per worker initialization, not per request

### Risk: Breaking Changes
**Assessment**: None - we're adding functionality, not changing existing behavior

### Risk: Side Effects
**Assessment**: None - `mobile_api.py` only defines functions and helpers, no module-level side effects

---

## Rollback Plan

### If Fix Doesn't Work
1. Remove the added line
2. Restart workers: `bench restart`
3. System returns to previous state (broken, but known)

### If Fix Causes Issues
```bash
# Immediate rollback (< 1 minute)
cd /home/frappe/frappe-bench-env/frappe-bench/apps/crm
git checkout crm/api/__init__.py
bench restart
```

### Validation After Rollback
```bash
# Verify other API endpoints still work
curl "https://trust.jossoor.org/api/method/crm.api.get_translations" -b cookies.txt
```

---

## Verification Plan

### Step 1: Apply Fix
```bash
cd /home/frappe/frappe-bench-env/frappe-bench/apps/crm

# Add the import line to crm/api/__init__.py
# (Use editor or sed/echo)
```

### Step 2: Restart Workers
```bash
cd /home/frappe/frappe-bench-env/frappe-bench
bench restart
```

**Expected**: Workers restart, import chain executes, `mobile_api` loaded, decorators run, functions register.

### Step 3: Verify Registration (Console)
```bash
bench --site trust.jossoor.org console
```

```python
# Verify module is imported in worker's sys.modules
import sys
'crm.api.mobile_api' in sys.modules
# Expected: True ✅

# Verify functions are whitelisted
import frappe
frappe.get_attr('crm.api.mobile_api.home_tasks')
# Expected: <function home_tasks at 0x...> ✅

# Check whitelist registry (if accessible)
# frappe.get_attr internally checks if function is whitelisted
```

### Step 4: Test Over HTTP (After Login)
```bash
SITE="https://trust.jossoor.org"

# Login first
curl -X POST "$SITE/api/method/login" \
  -d "usr=admin@example.com&pwd=password" \
  -c cookies.txt -v

# Test home_tasks endpoint
curl -s "$SITE/api/method/crm.api.mobile_api.home_tasks?limit=5" \
  -b cookies.txt | jq .

# Expected:
# {
#   "message": {
#     "today": [...],
#     "limit": 5
#   }
# }

# Test filter_tasks with date range
curl -s "$SITE/api/method/crm.api.mobile_api.filter_tasks?date_from=2025-12-01&date_to=2025-12-31&limit=10" \
  -b cookies.txt | jq .

# Expected:
# {
#   "message": {
#     "data": [...]
#   }
# }

# Test main_page_buckets
curl -s "$SITE/api/method/crm.api.mobile_api.main_page_buckets?min_each=5" \
  -b cookies.txt | jq .

# Expected:
# {
#   "message": {
#     "today": [...],
#     "late": [...],
#     "upcoming": [...],
#     "min_each": 5
#   }
# }
```

### Step 5: Verify Other Endpoints Still Work
```bash
# Ensure we didn't break existing functionality
curl -s "$SITE/api/method/crm.api.get_translations" \
  -b cookies.txt | jq . | head -20

# Expected: Returns translations JSON ✅
```

---

## Success Criteria

After applying fix and restarting:

- ✅ No "not whitelisted" errors for `crm.api.mobile_api.*` endpoints
- ✅ All 7 endpoints return 200 OK with valid JSON
- ✅ Other CRM API endpoints continue to work
- ✅ Date range filters work correctly (both `date_from` AND `date_to`)
- ✅ No KeyErrors on missing fields
- ✅ Response format unchanged (core + optional fields)
- ✅ No new linter errors
- ✅ No security changes (still requires authentication)

---

## Why Not Alternative Fixes?

### Alternative 1: Import in hooks.py
```python
# hooks.py
startup = ["crm.api.mobile_api.startup_hook"]
```

**Why Not**: 
- Requires adding a dummy `startup_hook()` function in `mobile_api.py`
- More complex than simple import
- Hooks are for side effects, not just registration

### Alternative 2: Create separate whitelisted functions file
**Why Not**: 
- More invasive - requires moving code
- Changes file structure
- Not minimal

### Alternative 3: Use boot_session hook
**Why Not**: 
- Runs per-session, not per-worker (wasteful)
- Overkill for simple import need

### Alternative 4: Modify nginx/gunicorn
**Why Not**: 
- Problem is in Python import chain, not HTTP routing
- Other endpoints work fine, proving routing is correct

### The Chosen Fix Is Best Because:
- ✅ One line
- ✅ Standard Python pattern
- ✅ Already used in same file for other modules
- ✅ Loads once per worker (efficient)
- ✅ No functional changes
- ✅ Easy to understand and maintain

---

## Implementation Approval

### Checklist Before Applying

- ✅ Root cause identified and documented
- ✅ Fix is minimal (one line)
- ✅ Fix follows existing patterns in codebase
- ✅ No business logic changes
- ✅ No security changes
- ✅ No API contract changes
- ✅ Rollback plan documented
- ✅ Verification plan documented
- ✅ Success criteria defined

### Ready to Apply: YES ✅

---

## Post-Fix Actions

### 1. Remove Diagnostic Files (If Any Created)
```bash
# Remove temporary diagnostic code if any was added
# (None in this case - investigation was file-based only)
```

### 2. Update Documentation
Update `MOBILE_API_README.md` to note:
- Module is now auto-imported at startup
- No special installation steps needed

### 3. Verify Production
Test on production site after deployment.

### 4. Monitor
Watch error logs for any unexpected issues:
```bash
tail -f /home/frappe/frappe-bench-env/frappe-bench/logs/bench-start.log
```

---

## Timeline

- **Investigation**: 30 minutes ✅ Complete
- **Fix Implementation**: 1 minute (add one line)
- **Worker Restart**: 30 seconds
- **Verification Testing**: 5 minutes
- **Total**: ~7 minutes from fix to verified

---

## Stakeholder Communication

**Message**: 
"Root cause identified: mobile_api module not imported at web worker startup. Fix: Add one import line to crm/api/__init__.py. No business logic changes. Expected downtime: 30 seconds for worker restart."

---

**Status**: Ready for Implementation ✅  
**Risk Level**: Very Low  
**Complexity**: Trivial (1 line)  
**Confidence**: 100%

---

**Approve and apply fix?** Awaiting authorization.

