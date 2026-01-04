/// Mobile API Test Suite for Flutter Application
/// 
/// This file contains comprehensive test cases for all mobile APIs.
/// Can be used for integration testing in Flutter app.
/// 
/// Usage:
/// 1. Copy this file to your Flutter project's test directory
/// 2. Update BASE_URL and credentials
/// 3. Run: flutter test mobile_api_test.dart

import 'package:dio/dio.dart';
import 'package:dio_cookie_manager/dio_cookie_manager.dart';
import 'package:cookie_jar/cookie_jar.dart';
import 'package:test/test.dart';

// ============================================================================
// CONFIGURATION
// ============================================================================

const String BASE_URL = 'https://your-site.com';
const String API_BASE = '$BASE_URL/api/method/crm.api.mobile_api';
const String LOGIN_URL = '$BASE_URL/api/method/login';

// Test credentials - UPDATE THESE
const String TEST_USERNAME = 'user@example.com';
const String TEST_PASSWORD = 'your_password';

// ============================================================================
// TEST HELPER CLASS
// ============================================================================

class MobileAPITester {
  late Dio dio;
  late CookieJar cookieJar;
  
  // Track created records for cleanup
  List<String> createdTasks = [];
  List<String> createdLeads = [];
  
  MobileAPITester() {
    dio = Dio(BaseOptions(
      baseUrl: BASE_URL,
      connectTimeout: const Duration(seconds: 30),
      receiveTimeout: const Duration(seconds: 30),
    ));
    
    cookieJar = CookieJar();
    dio.interceptors.add(CookieManager(cookieJar));
  }
  
  /// Login and get session cookies
  Future<bool> login() async {
    try {
      final response = await dio.post(
        LOGIN_URL,
        data: {
          'usr': TEST_USERNAME,
          'pwd': TEST_PASSWORD,
        },
        options: Options(
          followRedirects: false,
          validateStatus: (status) => status! < 500,
        ),
      );
      
      return response.statusCode == 200 || response.statusCode == 302;
    } catch (e) {
      print('Login error: $e');
      return false;
    }
  }
  
  /// Logout
  Future<void> logout() async {
    try {
      await dio.get('$BASE_URL/api/method/logout');
    } catch (e) {
      print('Logout error: $e');
    }
  }
  
  // ============================================================================
  // TASK API METHODS
  // ============================================================================
  
  /// Create a task
  Future<Map<String, dynamic>?> createTask({
    required String taskType,
    String? title,
    String? status,
    String? priority,
    String? startDate,
    String? description,
  }) async {
    try {
      final response = await dio.post(
        '$API_BASE.create_task',
        data: {
          'task_type': taskType,
          if (title != null) 'title': title,
          if (status != null) 'status': status,
          if (priority != null) 'priority': priority,
          if (startDate != null) 'start_date': startDate,
          if (description != null) 'description': description,
        },
      );
      
      if (response.statusCode == 200) {
        final task = response.data['message'];
        if (task != null && task['name'] != null) {
          createdTasks.add(task['name']);
        }
        return task;
      }
      return null;
    } catch (e) {
      print('Create task error: $e');
      return null;
    }
  }
  
  /// Edit a task
  Future<Map<String, dynamic>?> editTask({
    required String name,
    String? title,
    String? status,
    String? priority,
  }) async {
    try {
      final response = await dio.post(
        '$API_BASE.edit_task',
        data: {
          'name': name,
          if (title != null) 'title': title,
          if (status != null) 'status': status,
          if (priority != null) 'priority': priority,
        },
      );
      
      if (response.statusCode == 200) {
        return response.data['message'];
      }
      return null;
    } catch (e) {
      print('Edit task error: $e');
      return null;
    }
  }
  
  /// Update task status
  Future<Map<String, dynamic>?> updateTaskStatus({
    required String name,
    required String status,
  }) async {
    try {
      final response = await dio.post(
        '$API_BASE.update_status',
        data: {
          'name': name,
          'status': status,
        },
      );
      
      if (response.statusCode == 200) {
        return response.data['message'];
      }
      return null;
    } catch (e) {
      print('Update status error: $e');
      return null;
    }
  }
  
  /// Delete a task
  Future<bool> deleteTask(String name) async {
    try {
      final response = await dio.post(
        '$API_BASE.delete_task',
        data: {'name': name},
      );
      
      if (response.statusCode == 200) {
        final result = response.data['message'];
        return result['ok'] == true;
      }
      return false;
    } catch (e) {
      print('Delete task error: $e');
      return false;
    }
  }
  
  /// Get all tasks
  Future<Map<String, dynamic>?> getAllTasks({
    int page = 1,
    int limit = 20,
    String? status,
    String? priority,
  }) async {
    try {
      final queryParams = {
        'page': page.toString(),
        'limit': limit.toString(),
        if (status != null) 'status': status,
        if (priority != null) 'priority': priority,
      };
      
      final response = await dio.get(
        '$API_BASE.get_all_tasks',
        queryParameters: queryParams,
      );
      
      if (response.statusCode == 200) {
        return response.data['message'];
      }
      return null;
    } catch (e) {
      print('Get all tasks error: $e');
      return null;
    }
  }
  
  /// Get home tasks (today's tasks)
  Future<Map<String, dynamic>?> getHomeTasks({int limit = 5}) async {
    try {
      final response = await dio.get(
        '$API_BASE.home_tasks',
        queryParameters: {'limit': limit.toString()},
      );
      
      if (response.statusCode == 200) {
        return response.data['message'];
      }
      return null;
    } catch (e) {
      print('Get home tasks error: $e');
      return null;
    }
  }
  
  /// Get main page buckets
  Future<Map<String, dynamic>?> getMainPageBuckets({int minEach = 5}) async {
    try {
      final response = await dio.get(
        '$API_BASE.main_page_buckets',
        queryParameters: {'min_each': minEach.toString()},
      );
      
      if (response.statusCode == 200) {
        return response.data['message'];
      }
      return null;
    } catch (e) {
      print('Get main page buckets error: $e');
      return null;
    }
  }
  
  // ============================================================================
  // LEAD API METHODS
  // ============================================================================
  
  /// Create a lead
  Future<Map<String, dynamic>?> createLead({
    String? leadName,
    String? firstName,
    String? lastName,
    String? email,
    String? mobileNo,
    String? status,
    String? source,
    String? organization,
  }) async {
    try {
      final response = await dio.post(
        '$API_BASE.create_lead',
        data: {
          if (leadName != null) 'lead_name': leadName,
          if (firstName != null) 'first_name': firstName,
          if (lastName != null) 'last_name': lastName,
          if (email != null) 'email': email,
          if (mobileNo != null) 'mobile_no': mobileNo,
          if (status != null) 'status': status,
          if (source != null) 'source': source,
          if (organization != null) 'organization': organization,
        },
      );
      
      if (response.statusCode == 200) {
        final lead = response.data['message'];
        if (lead != null && lead['name'] != null) {
          createdLeads.add(lead['name']);
        }
        return lead;
      }
      return null;
    } catch (e) {
      print('Create lead error: $e');
      return null;
    }
  }
  
  /// Edit a lead
  Future<Map<String, dynamic>?> editLead({
    required String name,
    String? leadName,
    String? email,
    String? mobileNo,
  }) async {
    try {
      final response = await dio.post(
        '$API_BASE.edit_lead',
        data: {
          'name': name,
          if (leadName != null) 'lead_name': leadName,
          if (email != null) 'email': email,
          if (mobileNo != null) 'mobile_no': mobileNo,
        },
      );
      
      if (response.statusCode == 200) {
        return response.data['message'];
      }
      return null;
    } catch (e) {
      print('Edit lead error: $e');
      return null;
    }
  }
  
  /// Delete a lead
  Future<bool> deleteLead(String name) async {
    try {
      final response = await dio.post(
        '$API_BASE.delete_lead',
        data: {'name': name},
      );
      
      if (response.statusCode == 200) {
        final result = response.data['message'];
        return result['ok'] == true;
      }
      return false;
    } catch (e) {
      print('Delete lead error: $e');
      return false;
    }
  }
  
  /// Get all leads
  Future<Map<String, dynamic>?> getAllLeads({
    int page = 1,
    int limit = 20,
    String? searchTerm,
  }) async {
    try {
      final queryParams = {
        'page': page.toString(),
        'limit': limit.toString(),
        if (searchTerm != null) 'search_term': searchTerm,
      };
      
      final response = await dio.get(
        '$API_BASE.get_all_leads',
        queryParameters: queryParams,
      );
      
      if (response.statusCode == 200) {
        return response.data['message'];
      }
      return null;
    } catch (e) {
      print('Get all leads error: $e');
      return null;
    }
  }
  
  /// Get lead by ID
  Future<Map<String, dynamic>?> getLeadById(String leadId) async {
    try {
      final response = await dio.get(
        '$API_BASE.get_lead_by_id',
        queryParameters: {'lead_id': leadId},
      );
      
      if (response.statusCode == 200) {
        return response.data['lead'];
      }
      return null;
    } catch (e) {
      print('Get lead by ID error: $e');
      return null;
    }
  }
  
  /// Get home leads
  Future<Map<String, dynamic>?> getHomeLeads({int limit = 5}) async {
    try {
      final response = await dio.get(
        '$API_BASE.home_leads',
        queryParameters: {'limit': limit.toString()},
      );
      
      if (response.statusCode == 200) {
        return response.data['message'];
      }
      return null;
    } catch (e) {
      print('Get home leads error: $e');
      return null;
    }
  }
  
  // ============================================================================
  // HELPER API METHODS
  // ============================================================================
  
  /// Get OAuth config
  Future<Map<String, dynamic>?> getOAuthConfig() async {
    try {
      final response = await dio.get('$API_BASE.get_oauth_config');
      
      if (response.statusCode == 200) {
        return response.data['message'];
      }
      return null;
    } catch (e) {
      print('Get OAuth config error: $e');
      return null;
    }
  }
  
  /// Get app logo
  Future<Map<String, dynamic>?> getAppLogo() async {
    try {
      final response = await dio.get('$API_BASE.get_app_logo');
      
      if (response.statusCode == 200) {
        return response.data['message'];
      }
      return null;
    } catch (e) {
      print('Get app logo error: $e');
      return null;
    }
  }
  
  /// Get current user role
  Future<Map<String, dynamic>?> getCurrentUserRole() async {
    try {
      final response = await dio.get('$API_BASE.get_current_user_role');
      
      if (response.statusCode == 200) {
        return response.data['message'];
      }
      return null;
    } catch (e) {
      print('Get current user role error: $e');
      return null;
    }
  }
  
  /// Get team members
  Future<Map<String, dynamic>?> getMyTeamMembers() async {
    try {
      final response = await dio.get('$API_BASE.get_my_team_members');
      
      if (response.statusCode == 200) {
        return response.data['message'];
      }
      return null;
    } catch (e) {
      print('Get team members error: $e');
      return null;
    }
  }
  
  // ============================================================================
  // CLEANUP
  // ============================================================================
  
  /// Clean up all created test records
  Future<void> cleanup() async {
    print('🧹 Cleaning up test records...');
    
    // Delete all created tasks
    for (final taskName in createdTasks) {
      await deleteTask(taskName);
    }
    
    // Delete all created leads
    for (final leadName in createdLeads) {
      await deleteLead(leadName);
    }
    
    createdTasks.clear();
    createdLeads.clear();
    
    print('✅ Cleanup complete');
  }
}

// ============================================================================
// TEST CASES
// ============================================================================

void main() {
  group('Mobile API Tests', () {
    late MobileAPITester tester;
    
    setUpAll(() async {
      tester = MobileAPITester();
      final loggedIn = await tester.login();
      expect(loggedIn, true, reason: 'Must be able to login');
    });
    
    tearDownAll(() async {
      await tester.cleanup();
      await tester.logout();
    });
    
    // ========================================================================
    // TASK API TESTS
    // ========================================================================
    
    group('Task API Tests', () {
      test('Create task - minimal', () async {
        final task = await tester.createTask(
          taskType: 'Call',
          title: 'Test Task Minimal',
        );
        
        expect(task, isNotNull, reason: 'Task should be created');
        expect(task!['name'], isNotNull, reason: 'Task should have name');
        expect(task['title'], 'Test Task Minimal');
        expect(task['status'], 'Todo');
      });
      
      test('Create task - full', () async {
        final task = await tester.createTask(
          taskType: 'Meeting',
          title: 'Test Task Full',
          status: 'In Progress',
          priority: 'High',
          description: 'Test description',
        );
        
        expect(task, isNotNull);
        expect(task!['title'], 'Test Task Full');
        expect(task['status'], 'In Progress');
        expect(task['priority'], 'High');
      });
      
      test('Edit task', () async {
        // Create task first
        final created = await tester.createTask(
          taskType: 'Call',
          title: 'Original Title',
        );
        expect(created, isNotNull);
        
        // Edit task
        final edited = await tester.editTask(
          name: created!['name'],
          title: 'Updated Title',
          status: 'In Progress',
        );
        
        expect(edited, isNotNull);
        expect(edited!['title'], 'Updated Title');
        expect(edited['status'], 'In Progress');
      });
      
      test('Update task status', () async {
        // Create task first
        final created = await tester.createTask(
          taskType: 'Call',
          title: 'Status Test',
        );
        expect(created, isNotNull);
        
        // Update status
        final updated = await tester.updateTaskStatus(
          name: created!['name'],
          status: 'Completed',
        );
        
        expect(updated, isNotNull);
        expect(updated!['status'], 'Completed');
      });
      
      test('Get all tasks', () async {
        final result = await tester.getAllTasks(page: 1, limit: 10);
        
        expect(result, isNotNull);
        expect(result!['data'], isA<List>());
        expect(result['total'], isA<int>());
      });
      
      test('Get home tasks', () async {
        final result = await tester.getHomeTasks(limit: 5);
        
        expect(result, isNotNull);
        expect(result!['today'], isA<List>());
      });
      
      test('Get main page buckets', () async {
        final result = await tester.getMainPageBuckets(minEach: 3);
        
        expect(result, isNotNull);
        expect(result!['today'], isA<List>());
        expect(result['late'], isA<List>());
        expect(result['upcoming'], isA<List>());
      });
      
      test('Delete task', () async {
        // Create task first
        final created = await tester.createTask(
          taskType: 'Call',
          title: 'Task to Delete',
        );
        expect(created, isNotNull);
        final taskName = created!['name'];
        
        // Remove from cleanup list since we're deleting it
        tester.createdTasks.remove(taskName);
        
        // Delete task
        final deleted = await tester.deleteTask(taskName);
        expect(deleted, true);
      });
    });
    
    // ========================================================================
    // LEAD API TESTS
    // ========================================================================
    
    group('Lead API Tests', () {
      test('Create lead - minimal', () async {
        final lead = await tester.createLead(
          firstName: 'Test',
          lastName: 'Lead',
          mobileNo: '+201234567890',
        );
        
        expect(lead, isNotNull);
        expect(lead!['name'], isNotNull);
      });
      
      test('Create lead - full', () async {
        final lead = await tester.createLead(
          leadName: 'Full Test Lead',
          firstName: 'Test',
          lastName: 'Lead',
          email: 'test@example.com',
          mobileNo: '+201234567891',
          organization: 'Test Org',
        );
        
        expect(lead, isNotNull);
        expect(lead!['name'], isNotNull);
      });
      
      test('Edit lead', () async {
        // Create lead first
        final created = await tester.createLead(
          firstName: 'Test',
          lastName: 'Lead',
          mobileNo: '+201234567892',
        );
        expect(created, isNotNull);
        
        // Edit lead
        final edited = await tester.editLead(
          name: created!['name'],
          leadName: 'Updated Lead Name',
          email: 'updated@example.com',
        );
        
        expect(edited, isNotNull);
        expect(edited!['lead_name'], 'Updated Lead Name');
      });
      
      test('Get all leads', () async {
        final result = await tester.getAllLeads(page: 1, limit: 10);
        
        expect(result, isNotNull);
        expect(result!['data'], isA<List>());
        expect(result['total'], isA<int>());
        
        // Check that leads have comments fields
        if (result['data'].isNotEmpty) {
          final lead = result['data'][0];
          expect(lead['comments'], isA<List>());
          expect(lead['last_comment'], anything);
        }
      });
      
      test('Get lead by ID', () async {
        // Create lead first
        final created = await tester.createLead(
          firstName: 'Test',
          lastName: 'Lead',
          mobileNo: '+201234567893',
        );
        expect(created, isNotNull);
        
        // Get lead by ID
        final lead = await tester.getLeadById(created!['name']);
        
        expect(lead, isNotNull);
        expect(lead!['name'], created['name']);
        expect(lead['comments'], isA<List>());
        expect(lead['last_comment'], anything);
        expect(lead['duplicate_leads'], isA<List>());
      });
      
      test('Get home leads', () async {
        final result = await tester.getHomeLeads(limit: 5);
        
        expect(result, isNotNull);
        expect(result!['data'], isA<List>());
      });
      
      test('Delete lead', () async {
        // Create lead first
        final created = await tester.createLead(
          firstName: 'Test',
          lastName: 'Lead',
          mobileNo: '+201234567894',
        );
        expect(created, isNotNull);
        final leadName = created!['name'];
        
        // Remove from cleanup list
        tester.createdLeads.remove(leadName);
        
        // Delete lead
        final deleted = await tester.deleteLead(leadName);
        expect(deleted, true);
      });
    });
    
    // ========================================================================
    // HELPER API TESTS
    // ========================================================================
    
    group('Helper API Tests', () {
      test('Get OAuth config', () async {
        final config = await tester.getOAuthConfig();
        
        expect(config, isNotNull);
        expect(config!['client_id'], isNotNull);
        expect(config['base_url'], isNotNull);
      });
      
      test('Get app logo', () async {
        final logo = await tester.getAppLogo();
        
        expect(logo, isNotNull);
        expect(logo!['app_logo'], anything);
        expect(logo['app_name'], anything);
      });
      
      test('Get current user role', () async {
        final role = await tester.getCurrentUserRole();
        
        expect(role, isNotNull);
        expect(role!['role'], isNotNull);
        expect(role['user'], isNotNull);
      });
      
      test('Get team members', () async {
        final team = await tester.getMyTeamMembers();
        
        expect(team, isNotNull);
        expect(team!['team_members'], isA<List>());
      });
    });
  });
}

