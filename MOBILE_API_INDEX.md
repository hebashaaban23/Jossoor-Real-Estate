# CRM Task Mobile API - Complete Documentation Index

**Version**: 1.0  
**Date**: December 3, 2025  
**Purpose**: REST API for CRM Task management (mobile/Flutter apps)

---

## 📂 Quick Navigation

### 🚀 **Start Here**
- [**MOBILE_API_SUMMARY.md**](./MOBILE_API_SUMMARY.md) - Executive summary of what was delivered
- [**MOBILE_API_QUICK_REFERENCE.md**](./MOBILE_API_QUICK_REFERENCE.md) - Quick reference card (cheat sheet)

### 📖 **Core Documentation**
1. [**MOBILE_API_README.md**](./MOBILE_API_README.md) - Main documentation
   - Authentication model (session cookies)
   - Installation instructions
   - Permissions and roles
   - Quick start guide
   - Field reference
   - Error handling

2. [**API_ENDPOINTS.md**](./API_ENDPOINTS.md) - Complete endpoint reference
   - All 7 endpoints documented
   - Request parameters (required/optional)
   - Response formats
   - Examples and test cases
   - HTTP status codes

### 👨‍💻 **For Developers**
3. [**FLUTTER_HANDOVER.md**](./FLUTTER_HANDOVER.md) - Flutter integration guide
   - Step-by-step setup
   - Complete code examples
   - Authentication implementation
   - State management patterns
   - Error handling
   - Common pitfalls & solutions

### 🧪 **For QA/Testing**
4. [**QA_CHECKLIST.md**](./QA_CHECKLIST.md) - Comprehensive testing checklist
   - All test scenarios
   - Edge cases
   - Performance tests
   - Requirements verification
   - Sign-off checklist

5. [**POSTMAN_COLLECTION.json**](./POSTMAN_COLLECTION.json) - Postman/Insomnia collection
   - All endpoints pre-configured
   - Variable management
   - Auto cookie extraction
   - Import and test immediately

---

## 💻 Source Code

**Main API File**: `crm/api/mobile_api.py`

Contains 7 whitelisted endpoints:
- `create_task` - Create new CRM Task
- `edit_task` - Edit existing task
- `delete_task` - Delete task
- `update_status` - Quick status update
- `filter_tasks` - Filter/search with pagination
- `home_tasks` - Today's top N tasks
- `main_page_buckets` - Today/late/upcoming buckets

**Location**: `/home/frappe/frappe-bench-env/frappe-bench/apps/crm/crm/api/mobile_api.py`

---

## 🎯 Use Cases & Documentation Map

### **"I need to understand what was built"**
→ Start with [MOBILE_API_SUMMARY.md](./MOBILE_API_SUMMARY.md)

### **"I need to test the API"**
1. Import [POSTMAN_COLLECTION.json](./POSTMAN_COLLECTION.json)
2. Follow [MOBILE_API_README.md](./MOBILE_API_README.md) Quick Start
3. Use [MOBILE_API_QUICK_REFERENCE.md](./MOBILE_API_QUICK_REFERENCE.md) for syntax

### **"I need to integrate with Flutter"**
1. Read [FLUTTER_HANDOVER.md](./FLUTTER_HANDOVER.md) completely
2. Reference [API_ENDPOINTS.md](./API_ENDPOINTS.md) for details
3. Test with [POSTMAN_COLLECTION.json](./POSTMAN_COLLECTION.json) first

### **"I need to QA this before deployment"**
1. Follow [QA_CHECKLIST.md](./QA_CHECKLIST.md) systematically
2. Use [POSTMAN_COLLECTION.json](./POSTMAN_COLLECTION.json) for manual testing
3. Reference [API_ENDPOINTS.md](./API_ENDPOINTS.md) for expected behavior

### **"I need a quick reference while coding"**
→ Keep [MOBILE_API_QUICK_REFERENCE.md](./MOBILE_API_QUICK_REFERENCE.md) open

### **"I need to understand authentication"**
→ See [MOBILE_API_README.md](./MOBILE_API_README.md) - Authentication Model section

### **"I need to see request/response examples"**
→ See [API_ENDPOINTS.md](./API_ENDPOINTS.md) - Each endpoint has examples

---

## 📋 All Files Overview

| File | Type | Purpose | Audience |
|------|------|---------|----------|
| `MOBILE_API_SUMMARY.md` | Overview | Delivery summary & checklist | All |
| `MOBILE_API_README.md` | Documentation | Main documentation | All |
| `API_ENDPOINTS.md` | Reference | Complete endpoint specs | Developers, QA |
| `FLUTTER_HANDOVER.md` | Guide | Flutter integration | Flutter Developers |
| `QA_CHECKLIST.md` | Checklist | Testing scenarios | QA, Testers |
| `POSTMAN_COLLECTION.json` | Test Suite | API testing collection | Developers, QA |
| `MOBILE_API_QUICK_REFERENCE.md` | Cheat Sheet | Quick syntax reference | Developers |
| `MOBILE_API_INDEX.md` | Index | This file - navigation | All |
| `crm/api/mobile_api.py` | Source Code | API implementation | Backend Developers |

---

## 🔑 Key Information

### Base URL
```
https://your-site.com/api/method/crm.api.mobile_api
```

### Authentication
- Standard Frappe login: `POST https://your-site.com/api/method/login`
- Session cookies: `sid`, `user_id`, `full_name`
- Include cookies in all API requests

### Required Roles
- **Sales User** - Full CRUD access
- **Sales Manager** - Full CRUD access

### Field Name (Important!)
- Uses `start_date` (not `exp_start_date`)
- This is the actual field name in CRM Task doctype

### No Custom Features
- ❌ No notifications or FCM
- ❌ No CRM Lead references
- ❌ No custom authentication
- ✅ Pure CRM Task management only

---

## 📊 Endpoints Summary

| # | Endpoint | Method | Purpose |
|---|----------|--------|---------|
| 1 | `create_task` | POST | Create new task |
| 2 | `edit_task` | POST | Edit existing task |
| 3 | `delete_task` | POST | Delete task |
| 4 | `update_status` | POST | Change task status |
| 5 | `filter_tasks` | GET | Filter/search tasks |
| 6 | `home_tasks` | GET | Today's top N tasks |
| 7 | `main_page_buckets` | GET | Today/late/upcoming |

Full details in [API_ENDPOINTS.md](./API_ENDPOINTS.md)

---

## 🎓 Learning Path

### Backend Developer / Tester
1. **Day 1**: Read [MOBILE_API_README.md](./MOBILE_API_README.md)
2. **Day 1**: Import [POSTMAN_COLLECTION.json](./POSTMAN_COLLECTION.json) and test all endpoints
3. **Day 2**: Run through [QA_CHECKLIST.md](./QA_CHECKLIST.md)
4. **Reference**: [API_ENDPOINTS.md](./API_ENDPOINTS.md) and [MOBILE_API_QUICK_REFERENCE.md](./MOBILE_API_QUICK_REFERENCE.md)

### Flutter Developer
1. **Day 1**: Read [MOBILE_API_SUMMARY.md](./MOBILE_API_SUMMARY.md)
2. **Day 1**: Test endpoints with [POSTMAN_COLLECTION.json](./POSTMAN_COLLECTION.json)
3. **Day 2**: Read [FLUTTER_HANDOVER.md](./FLUTTER_HANDOVER.md) and implement authentication
4. **Day 3-5**: Implement API service layer using code examples
5. **Reference**: [API_ENDPOINTS.md](./API_ENDPOINTS.md) for endpoint specs

### Project Manager / Stakeholder
1. **Read**: [MOBILE_API_SUMMARY.md](./MOBILE_API_SUMMARY.md) - Complete overview
2. **Review**: Deliverables checklist (all ✅)
3. **Verify**: Requirements met (no notifications, no CRM Lead, session auth)

---

## ✅ Requirements Verification

### Original Requirements
1. ✅ CRUD operations for CRM Task
2. ✅ Status change endpoint
3. ✅ Filtering (date, importance, status)
4. ✅ Home list (top 5 for today)
5. ✅ Main page buckets (today/late/upcoming with min count)
6. ✅ No notifications
7. ✅ No custom login (use standard Frappe)
8. ✅ Standard permissions (no bypass)
9. ✅ Compact, consistent responses

### Deliverables
1. ✅ Working Frappe app (mobile_api.py)
2. ✅ README with auth model and installation
3. ✅ Endpoint index with all details
4. ✅ Environment/permissions notes
5. ✅ Postman collection
6. ✅ QA checklist
7. ✅ Flutter handover guide

**All requirements met and documented.**

---

## 🚀 Quick Start (30 seconds)

```bash
# 1. Login
curl -X POST https://your-site.com/api/method/login \
  -d "usr=user@example.com&pwd=password" \
  -c cookies.txt

# 2. Get today's tasks
curl "https://your-site.com/api/method/crm.api.mobile_api.home_tasks?limit=5" \
  -b cookies.txt

# 3. Get buckets
curl "https://your-site.com/api/method/crm.api.mobile_api.main_page_buckets?min_each=5" \
  -b cookies.txt
```

---

## 🐛 Troubleshooting

### "401 Unauthorized"
→ Not logged in or session expired. Login again to get fresh cookies.

### "403 Forbidden"
→ User doesn't have required role. Assign Sales User or Sales Manager role.

### "404 Not Found"
→ Task doesn't exist or you don't have permission to view it.

### "417 Validation Error"
→ Missing required field or invalid value. Check error message for details.

### Need more help?
→ See [MOBILE_API_README.md](./MOBILE_API_README.md) - Troubleshooting section

---

## 📞 Support Resources

1. **API Testing**: Use [POSTMAN_COLLECTION.json](./POSTMAN_COLLECTION.json)
2. **Endpoint Specs**: See [API_ENDPOINTS.md](./API_ENDPOINTS.md)
3. **Flutter Help**: Check [FLUTTER_HANDOVER.md](./FLUTTER_HANDOVER.md)
4. **Quick Syntax**: See [MOBILE_API_QUICK_REFERENCE.md](./MOBILE_API_QUICK_REFERENCE.md)
5. **QA Issues**: Follow [QA_CHECKLIST.md](./QA_CHECKLIST.md)

---

## 📝 Changelog

**Version 1.0** (2025-12-03)
- Initial release
- 7 REST endpoints for CRM Task management
- Complete documentation suite
- Postman collection
- Flutter integration guide
- QA checklist

---

## 📄 License

Same as parent CRM app (MIT License)

---

## 🎉 Ready to Go!

Everything you need is documented and ready to use:
- ✅ API is implemented and available
- ✅ Documentation is complete
- ✅ Testing collection is ready
- ✅ Flutter guide is comprehensive
- ✅ QA checklist is thorough

**Pick your role above and follow the learning path. Happy coding! 🚀**

---

**Document Index Version**: 1.0  
**Last Updated**: December 3, 2025  
**Status**: Complete & Ready for Handover

