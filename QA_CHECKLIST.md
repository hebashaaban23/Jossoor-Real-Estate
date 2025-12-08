# CRM Task Mobile API - QA Checklist

Comprehensive testing checklist to verify all functionality works correctly.

## Setup & Prerequisites

- [ ] CRM app is installed on the test site
- [ ] Test user has **Sales User** or **Sales Manager** role
- [ ] Can login via `/api/method/login` successfully
- [ ] Session cookies are obtained and persist
- [ ] Base URL is correctly configured in test environment

---

## 1. Authentication Tests

### Login Flow
- [ ] Can login with valid credentials
- [ ] Receives `sid`, `user_id`, and `full_name` cookies
- [ ] Invalid credentials return 401/403 error
- [ ] Session cookies persist across requests

### Permission Tests
- [ ] User with Sales User role can access all endpoints
- [ ] User with Sales Manager role can access all endpoints
- [ ] User without required roles gets 403 Forbidden
- [ ] Unauthenticated requests get 401 Unauthorized

---

## 2. CRUD Operations Tests

### Create Task (`create_task`)
- [ ] Can create task with all required fields (task_type)
- [ ] Can create task with optional fields (title, description, priority, etc.)
- [ ] Default values applied correctly:
  - [ ] status = "Todo"
  - [ ] priority = "Medium"
  - [ ] start_date = today
- [ ] Returns compact task JSON with all core fields
- [ ] Task appears in Frappe UI after creation
- [ ] Missing task_type returns 417 validation error
- [ ] Task is created with correct ownership/permissions

**Test Cases:**
```
✓ Minimal: task_type only
✓ Full: all fields provided
✓ Invalid: missing task_type (should fail)
✓ Edge case: empty title (should use description or default)
```

### Edit Task (`edit_task`)
- [ ] Can update single field (e.g., title only)
- [ ] Can update multiple fields at once
- [ ] Unchanged fields remain unchanged
- [ ] Returns updated compact task JSON
- [ ] Missing task name returns 417 error
- [ ] Non-existent task returns 404 error
- [ ] Cannot edit task without permission (403)
- [ ] Changes appear in Frappe UI

**Test Cases:**
```
✓ Update title only
✓ Update status and priority together
✓ Update all editable fields
✓ Invalid task name (should fail with 404)
✓ Edit without permission (should fail with 403)
```

### Update Status (`update_status`)
- [ ] Can change status to any valid value:
  - [ ] Backlog
  - [ ] Todo
  - [ ] In Progress
  - [ ] Done
  - [ ] Canceled
- [ ] Returns updated compact task JSON
- [ ] Missing name or status returns 417 error
- [ ] Invalid status value returns validation error
- [ ] Status change appears in Frappe UI

**Test Cases:**
```
✓ Todo → In Progress
✓ In Progress → Done
✓ Todo → Canceled
✓ Done → Todo (reopen)
✓ Invalid status value (should fail)
```

### Delete Task (`delete_task`)
- [ ] Can delete existing task
- [ ] Returns `{"ok": true}`
- [ ] Task removed from Frappe UI
- [ ] Missing task name returns 417 error
- [ ] Non-existent task returns 404 error
- [ ] Cannot delete without permission (403)
- [ ] Deleted task cannot be retrieved

**Test Cases:**
```
✓ Delete valid task
✓ Delete non-existent task (should fail with 404)
✓ Delete without permission (should fail with 403)
✓ Attempt to retrieve deleted task (should fail with 404)
```

---

## 3. Filtering & Search Tests

### Filter Tasks (`filter_tasks`)

#### Date Range Filtering
- [ ] `date_from` only - returns tasks from date onwards
- [ ] `date_to` only - returns tasks up to date
- [ ] Both `date_from` and `date_to` - returns tasks in range
- [ ] Date format YYYY-MM-DD is accepted
- [ ] Invalid date format returns error

**Test Cases:**
```
✓ date_from=2025-12-01 (all tasks from Dec 1 onwards)
✓ date_to=2025-12-31 (all tasks up to Dec 31)
✓ date_from=2025-12-01&date_to=2025-12-07 (one week range)
✓ Invalid date format (should fail gracefully)
```

#### Priority Filtering (importance)
- [ ] Single priority: `importance=High`
- [ ] Multiple priorities: `importance=High,Medium`
- [ ] All priorities: `importance=High,Medium,Low`
- [ ] Invalid priority value handled gracefully

**Test Cases:**
```
✓ importance=High (only high priority tasks)
✓ importance=High,Medium (high and medium priority)
✓ importance=Low (only low priority)
✓ Invalid priority value (should return empty or error)
```

#### Status Filtering
- [ ] Single status: `status=Todo`
- [ ] Multiple statuses: `status=Todo,In Progress`
- [ ] All statuses work: Backlog, Todo, In Progress, Done, Canceled
- [ ] Invalid status value handled gracefully

**Test Cases:**
```
✓ status=Todo (only Todo tasks)
✓ status=Todo,In Progress (active tasks)
✓ status=Done (completed tasks)
✓ status=Canceled (canceled tasks)
```

#### Combined Filters
- [ ] Date + Priority
- [ ] Date + Status
- [ ] Priority + Status
- [ ] All three filters together
- [ ] Filters work correctly in combination

**Test Cases:**
```
✓ date_from=2025-12-01&importance=High
✓ date_from=2025-12-01&date_to=2025-12-31&status=Todo
✓ importance=High&status=Todo,In Progress
✓ All filters combined
```

#### Pagination
- [ ] `limit` parameter controls number of results
- [ ] `offset` parameter skips correct number of results
- [ ] Default limit is 50
- [ ] Default offset is 0
- [ ] Pagination works with filters
- [ ] Can paginate through large result sets

**Test Cases:**
```
✓ limit=10 (returns max 10 results)
✓ limit=5&offset=0 (page 1)
✓ limit=5&offset=5 (page 2)
✓ limit=5&offset=10 (page 3)
✓ offset larger than total results (returns empty)
```

#### Sorting (order_by)
- [ ] Default sorting: `modified desc` (newest first)
- [ ] `modified asc` (oldest first)
- [ ] `start_date desc` (latest date first)
- [ ] `start_date asc` (earliest date first)
- [ ] `priority desc` (High → Low)
- [ ] `priority asc` (Low → High)
- [ ] Sorting works with filters and pagination

**Test Cases:**
```
✓ order_by=modified desc (default)
✓ order_by=start_date asc (earliest first)
✓ order_by=priority desc (high priority first)
✓ Combined with filters and pagination
```

#### Response Format
- [ ] Returns `{"data": [...]}`
- [ ] Each task has all compact fields:
  - name, title, status, priority, start_date, due_date, assigned_to, modified
- [ ] Empty result returns `{"data": []}`
- [ ] Title derived from description if not set

---

## 4. Home Tasks Tests (`home_tasks`)

### Basic Functionality
- [ ] Returns tasks where `start_date = today`
- [ ] Default limit is 5
- [ ] Custom limit parameter works (e.g., `limit=10`)
- [ ] Returns `{"today": [...], "limit": N}`
- [ ] Tasks sorted by priority desc, then modified desc
- [ ] Only includes today's tasks (not yesterday or tomorrow)

### Edge Cases
- [ ] No tasks for today - returns empty array
- [ ] Fewer tasks than limit - returns available tasks
- [ ] More tasks than limit - returns only limit number
- [ ] Tasks with no priority - handled gracefully

**Test Cases:**
```
✓ Default: limit=5 (returns up to 5 today's tasks)
✓ Custom: limit=10 (returns up to 10 today's tasks)
✓ No tasks today (returns empty array)
✓ 3 tasks exist, limit=5 (returns 3 tasks)
✓ 10 tasks exist, limit=5 (returns 5 tasks)
```

### Data Verification
- [ ] All tasks have start_date = today
- [ ] High priority tasks appear first
- [ ] Recent tasks appear before older tasks (when same priority)
- [ ] All compact fields present in response

---

## 5. Main Page Buckets Tests (`main_page_buckets`)

### Bucket Logic

#### Today Bucket
- [ ] Contains tasks where `start_date = today`
- [ ] All statuses included
- [ ] Sorted by priority desc, modified desc
- [ ] Returns at least `min_each` tasks (default 5)

#### Late Bucket
- [ ] Contains tasks where `start_date < today`
- [ ] Only active statuses: Backlog, Todo, In Progress
- [ ] Excludes Done and Canceled tasks
- [ ] Sorted by start_date asc (oldest first), priority desc
- [ ] Returns at least `min_each` tasks

#### Upcoming Bucket
- [ ] Contains tasks where `start_date > today`
- [ ] All statuses included
- [ ] Sorted by start_date asc (soonest first), priority desc
- [ ] Returns at least `min_each` tasks

### Parameters
- [ ] Default `min_each = 5`
- [ ] Custom `min_each` parameter works (e.g., `min_each=10`)
- [ ] Returns `{"today": [...], "late": [...], "upcoming": [...], "min_each": N}`

### Edge Cases
- [ ] No tasks in a bucket - returns empty array
- [ ] Fewer tasks than min_each - returns available tasks
- [ ] More tasks than min_each - returns min_each tasks
- [ ] All buckets empty - returns three empty arrays

**Test Cases:**
```
✓ Default: min_each=5 (returns 5+ per bucket)
✓ Custom: min_each=10 (returns 10+ per bucket)
✓ No late tasks (late bucket empty)
✓ No upcoming tasks (upcoming bucket empty)
✓ All buckets empty
✓ Today has 3, late has 7, upcoming has 2 (with min_each=5)
```

### Data Verification
- [ ] Today bucket: all have start_date = today
- [ ] Late bucket: all have start_date < today AND status in [Backlog, Todo, In Progress]
- [ ] Upcoming bucket: all have start_date > today
- [ ] No overlap between buckets
- [ ] Sorting correct in each bucket
- [ ] All compact fields present

---

## 6. Response Format Tests

### Compact Task Format
All endpoints return tasks with these fields:
- [ ] `name` - task ID
- [ ] `title` - task title (or derived from description)
- [ ] `status` - current status
- [ ] `priority` - priority level
- [ ] `start_date` - start date (YYYY-MM-DD format)
- [ ] `due_date` - due date/time (YYYY-MM-DD HH:MM:SS format or null)
- [ ] `assigned_to` - assigned user email (or null)
- [ ] `modified` - last modified timestamp

### Response Wrapping
- [ ] Single task endpoints: `{"message": {...}}`
- [ ] List endpoints: `{"message": {"data": [...]}}`
- [ ] Specialized views: `{"message": {"today": [...], ...}}`

### Error Response Format
- [ ] Contains `exc_type` field
- [ ] Contains `exception` field with error message
- [ ] HTTP status code matches error type (401, 403, 404, 417)

---

## 7. Permission & Security Tests

### Standard Frappe Permissions
- [ ] User can only see tasks they have permission for
- [ ] Cannot access other users' tasks without permission
- [ ] Role-based access control working (Sales User, Sales Manager)
- [ ] No permission bypass used (`ignore_permissions=False`)

### Session Management
- [ ] Session expires after configured timeout
- [ ] Expired session returns 401 Unauthorized
- [ ] Multiple sessions for same user work independently
- [ ] Logout invalidates session

---

## 8. Integration Tests

### Complete Workflow
- [ ] Login → Create Task → View in home_tasks → Edit → Update Status → Delete
- [ ] Create multiple tasks → Filter by criteria → Paginate results
- [ ] Create tasks with different dates → Verify buckets contain correct tasks
- [ ] Create task → Assign to user → Verify appears for that user

### Data Consistency
- [ ] Changes via API appear in Frappe UI
- [ ] Changes in Frappe UI appear in API responses
- [ ] Transactions committed properly (no data loss)
- [ ] Concurrent requests handled correctly

---

## 9. Edge Cases & Error Handling

### Input Validation
- [ ] Empty strings handled gracefully
- [ ] Null values handled gracefully
- [ ] Very long strings (title, description) handled
- [ ] Special characters in text fields
- [ ] Invalid date formats return clear errors
- [ ] Invalid enum values (status, priority) return clear errors

### Boundary Conditions
- [ ] limit=0 returns empty results
- [ ] limit=1000 (very large) works or returns reasonable error
- [ ] offset beyond total results returns empty array
- [ ] Filtering with no matches returns empty array

### Network & Server
- [ ] API works over HTTPS
- [ ] Large result sets don't timeout
- [ ] Proper error messages (not stack traces) returned
- [ ] Database connections closed properly

---

## 10. Performance Tests

### Response Times
- [ ] Create task: < 500ms
- [ ] Edit task: < 500ms
- [ ] Delete task: < 500ms
- [ ] Filter tasks (50 results): < 1s
- [ ] Home tasks: < 500ms
- [ ] Main page buckets: < 1s

### Load Testing
- [ ] Can handle 10 concurrent requests
- [ ] Can handle 100+ tasks in database
- [ ] Pagination works efficiently with large datasets

---

## 11. Specific Requirement Verification

### No Notifications
- [x] No FCM/device token endpoints
- [x] No notification logic in code
- [x] No notification settings or configuration

### No CRM Lead References
- [x] No CRM Lead doctype references in endpoints
- [x] No lead linking or cross-references
- [x] Tasks are standalone entities

### No Custom Login
- [x] Uses standard Frappe login endpoint
- [x] Session cookie authentication only
- [x] No custom token or JWT logic

### Field Name Verification
- [x] Uses `start_date` (not `exp_start_date`)
- [x] Documentation matches actual field names
- [x] API parameters match doctype fields

---

## 12. Documentation Verification

### README
- [ ] Installation steps are clear and accurate
- [ ] Authentication flow explained correctly
- [ ] All endpoints listed with descriptions
- [ ] Example requests provided

### API_ENDPOINTS.md
- [ ] All 7 endpoints documented
- [ ] Parameters clearly defined (required/optional, defaults)
- [ ] Response formats shown with examples
- [ ] Error cases documented

### Postman Collection
- [ ] All endpoints included
- [ ] Variables configured correctly (base_url, session cookies)
- [ ] Example requests work out of the box
- [ ] Test scripts extract session cookies automatically

### Flutter Handover
- [ ] Integration checklist provided
- [ ] Common patterns documented
- [ ] Error handling guidance included
- [ ] Code examples for Flutter provided

---

## Quick QA Confirmation (Per Requirements)

**Required Confirmations:**

✅ **"Home tasks" returns 5 for today by default**
- [ ] Verified: `/home_tasks` with no parameters returns max 5 tasks
- [ ] Verified: Only tasks with start_date = today are returned
- [ ] Verified: Response format is `{"today": [...], "limit": 5}`

✅ **"Buckets" returns at least `min_each` per bucket when data exists**
- [ ] Verified: `/main_page_buckets` with no parameters returns 5+ per bucket
- [ ] Verified: Today bucket has >= 5 tasks (or all available)
- [ ] Verified: Late bucket has >= 5 tasks (or all available)
- [ ] Verified: Upcoming bucket has >= 5 tasks (or all available)
- [ ] Verified: Response includes all three buckets and min_each value

✅ **"Filter" works with date/importance/status + pagination + ordering**
- [ ] Verified: Can filter by date range
- [ ] Verified: Can filter by importance (priority)
- [ ] Verified: Can filter by status
- [ ] Verified: Can combine multiple filters
- [ ] Verified: Pagination works (limit + offset)
- [ ] Verified: Sorting works (order_by parameter)
- [ ] Verified: All combinations work together

✅ **No references to CRM Lead**
- [ ] Code review: No "CRM Lead" references in mobile_api.py
- [ ] Code review: No lead linking in any endpoint
- [ ] Documentation: No mention of CRM Lead

✅ **No notifications**
- [ ] Code review: No notification logic in mobile_api.py
- [ ] Code review: No FCM/device token code
- [ ] Documentation: Explicitly states "No notifications"

✅ **No custom login**
- [ ] Code review: No custom authentication code
- [ ] Documentation: States "Use standard Frappe login"
- [ ] API uses session cookies only
- [ ] No JWT/token logic implemented

---

## Final Sign-Off

### Functional Tests
- [ ] All CRUD operations work correctly
- [ ] Filtering and search work as expected
- [ ] Home tasks returns correct data
- [ ] Main page buckets returns correct data
- [ ] All edge cases handled properly

### Non-Functional Tests
- [ ] Performance acceptable
- [ ] Security/permissions enforced
- [ ] Error handling clear and consistent
- [ ] Documentation complete and accurate

### Requirements Compliance
- [ ] All 7 endpoints implemented
- [ ] Session-based authentication only
- [ ] Standard Frappe permissions used
- [ ] Compact, consistent responses
- [ ] No notifications
- [ ] No CRM Lead references
- [ ] No custom login

### Deliverables Complete
- [ ] mobile_api.py with all endpoints
- [ ] MOBILE_API_README.md
- [ ] API_ENDPOINTS.md
- [ ] POSTMAN_COLLECTION.json
- [ ] QA_CHECKLIST.md (this file)
- [ ] FLUTTER_HANDOVER.md

---

## Test Data Setup

For thorough testing, create test data:

**Tasks for Today** (at least 10)
```
- Various priorities (High, Medium, Low)
- Various statuses (Backlog, Todo, In Progress, Done)
- Different task types (Meeting, Call, Property Showing)
```

**Tasks for Past Dates** (at least 10)
```
- Mix of active statuses (Backlog, Todo, In Progress)
- Some completed/canceled (should not appear in "late" bucket)
- Various dates in the past
```

**Tasks for Future Dates** (at least 10)
```
- Various dates in the future
- Mix of priorities and statuses
- Near future and far future dates
```

This ensures all buckets have sufficient data for testing.

---

## Notes

- Use Postman/Insomnia collection for manual testing
- Consider automated test scripts for CI/CD
- Test on staging environment before production
- Document any issues or edge cases discovered
- Verify with real Flutter integration before final sign-off

---

**QA Completed By:** _______________  
**Date:** _______________  
**Sign-Off:** _______________

