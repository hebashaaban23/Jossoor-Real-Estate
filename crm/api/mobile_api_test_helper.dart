/// Mobile API Test Helper - Standalone Test Runner
/// 
/// This file can be used to run tests directly from Flutter app
/// or as a standalone test script.
/// 
/// Usage in Flutter app:
/// ```dart
/// import 'mobile_api_test_helper.dart';
/// 
/// void main() async {
///   final runner = MobileAPITestRunner();
///   await runner.runAllTests();
/// }
/// ```

import 'dart:io';
import 'package:dio/dio.dart';
import 'package:dio_cookie_manager/dio_cookie_manager.dart';
import 'package:cookie_jar/cookie_jar.dart';

// Import the tester class
import 'mobile_api_test.dart';

/// Test Runner for Mobile API Tests
class MobileAPITestRunner {
  final MobileAPITester tester = MobileAPITester();
  int passedTests = 0;
  int failedTests = 0;
  List<String> errors = [];
  
  /// Run all test cases
  Future<void> runAllTests() async {
    print('\n╔══════════════════════════════════════════════════════════════╗');
    print('║         CRM Mobile API - Test Suite Runner                  ║');
    print('╚══════════════════════════════════════════════════════════════╝\n');
    
    // Login
    print('🔐 Logging in...');
    final loggedIn = await tester.login();
    if (!loggedIn) {
      print('❌ Login failed! Cannot proceed with tests.');
      return;
    }
    print('✅ Login successful\n');
    
    // Run test groups
    await runTaskTests();
    await runLeadTests();
    await runHelperTests();
    
    // Print summary
    printSummary();
    
    // Cleanup
    await tester.cleanup();
    await tester.logout();
  }
  
  /// Run Task API tests
  Future<void> runTaskTests() async {
    print('┌──────────────────────────────────────────────────────────────┐');
    print('│ Task API Tests                                                │');
    print('└──────────────────────────────────────────────────────────────┘\n');
    
    // Test 1: Create task minimal
    await test('Create Task - Minimal', () async {
      final task = await tester.createTask(
        taskType: 'Call',
        title: 'Test Task Minimal',
      );
      assert(task != null, 'Task should be created');
      assert(task!['name'] != null, 'Task should have name');
      assert(task['title'] == 'Test Task Minimal');
    });
    
    // Test 2: Create task full
    await test('Create Task - Full', () async {
      final task = await tester.createTask(
        taskType: 'Meeting',
        title: 'Test Task Full',
        status: 'In Progress',
        priority: 'High',
      );
      assert(task != null);
      assert(task!['status'] == 'In Progress');
      assert(task['priority'] == 'High');
    });
    
    // Test 3: Get all tasks
    await test('Get All Tasks', () async {
      final result = await tester.getAllTasks(page: 1, limit: 10);
      assert(result != null);
      assert(result!['data'] is List);
      assert(result['total'] is int);
    });
    
    // Test 4: Get home tasks
    await test('Get Home Tasks', () async {
      final result = await tester.getHomeTasks(limit: 5);
      assert(result != null);
      assert(result!['today'] is List);
    });
    
    // Test 5: Get main page buckets
    await test('Get Main Page Buckets', () async {
      final result = await tester.getMainPageBuckets(minEach: 3);
      assert(result != null);
      assert(result!['today'] is List);
      assert(result['late'] is List);
      assert(result['upcoming'] is List);
    });
    
    print('');
  }
  
  /// Run Lead API tests
  Future<void> runLeadTests() async {
    print('┌──────────────────────────────────────────────────────────────┐');
    print('│ Lead API Tests                                                 │');
    print('└──────────────────────────────────────────────────────────────┘\n');
    
    // Test 1: Create lead minimal
    await test('Create Lead - Minimal', () async {
      final lead = await tester.createLead(
        firstName: 'Test',
        lastName: 'Lead',
        mobileNo: '+201234567890',
      );
      assert(lead != null);
      assert(lead!['name'] != null);
    });
    
    // Test 2: Create lead full
    await test('Create Lead - Full', () async {
      final lead = await tester.createLead(
        leadName: 'Full Test Lead',
        firstName: 'Test',
        lastName: 'Lead',
        email: 'test@example.com',
        mobileNo: '+201234567891',
      );
      assert(lead != null);
      assert(lead!['name'] != null);
    });
    
    // Test 3: Get all leads
    await test('Get All Leads', () async {
      final result = await tester.getAllLeads(page: 1, limit: 10);
      assert(result != null);
      assert(result!['data'] is List);
      assert(result['total'] is int);
    });
    
    // Test 4: Get lead by ID
    await test('Get Lead By ID', () async {
      // Create lead first
      final created = await tester.createLead(
        firstName: 'Test',
        lastName: 'Lead',
        mobileNo: '+201234567892',
      );
      assert(created != null);
      
      final lead = await tester.getLeadById(created!['name']);
      assert(lead != null);
      assert(lead!['name'] == created['name']);
      assert(lead['comments'] is List);
    });
    
    // Test 5: Get home leads
    await test('Get Home Leads', () async {
      final result = await tester.getHomeLeads(limit: 5);
      assert(result != null);
      assert(result!['data'] is List);
    });
    
    print('');
  }
  
  /// Run Helper API tests
  Future<void> runHelperTests() async {
    print('┌──────────────────────────────────────────────────────────────┐');
    print('│ Helper API Tests                                              │');
    print('└──────────────────────────────────────────────────────────────┘\n');
    
    // Test 1: Get OAuth config
    await test('Get OAuth Config', () async {
      final config = await tester.getOAuthConfig();
      assert(config != null);
      assert(config!['client_id'] != null);
    });
    
    // Test 2: Get app logo
    await test('Get App Logo', () async {
      final logo = await tester.getAppLogo();
      assert(logo != null);
      assert(logo!['app_logo'] != null || logo['app_name'] != null);
    });
    
    // Test 3: Get current user role
    await test('Get Current User Role', () async {
      final role = await tester.getCurrentUserRole();
      assert(role != null);
      assert(role!['role'] != null);
    });
    
    // Test 4: Get team members
    await test('Get Team Members', () async {
      final team = await tester.getMyTeamMembers();
      assert(team != null);
      assert(team!['team_members'] is List);
    });
    
    print('');
  }
  
  /// Run a single test
  Future<void> test(String testName, Future<void> Function() testFn) async {
    try {
      await testFn();
      passedTests++;
      print('  ✅ $testName');
    } catch (e, stackTrace) {
      failedTests++;
      errors.add('$testName: $e');
      print('  ❌ $testName');
      print('     Error: $e');
    }
  }
  
  /// Print test summary
  void printSummary() {
    print('\n╔══════════════════════════════════════════════════════════════╗');
    print('║                    Test Summary                              ║');
    print('╚══════════════════════════════════════════════════════════════╝\n');
    
    print('✅ Passed: $passedTests');
    print('❌ Failed: $failedTests');
    print('📊 Total:  ${passedTests + failedTests}\n');
    
    if (errors.isNotEmpty) {
      print('Errors:');
      for (final error in errors) {
        print('  - $error');
      }
      print('');
    }
    
    if (failedTests == 0) {
      print('🎉 All tests passed!\n');
    } else {
      print('⚠️  Some tests failed. Please review errors above.\n');
    }
  }
}

/// Main entry point for standalone execution
void main() async {
  final runner = MobileAPITestRunner();
  await runner.runAllTests();
  
  // Exit with appropriate code
  exit(runner.failedTests == 0 ? 0 : 1);
}

