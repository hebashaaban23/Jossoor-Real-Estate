// Test script for browser console
// Copy and paste this into browser console (F12)

async function testAssignableUsersAPI() {
  console.log('Testing get_assignable_users API...');
  
  try {
    // Test 1: Using frappe.call if available
    if (window.frappe && typeof window.frappe.call === 'function') {
      console.log('Method 1: Using window.frappe.call');
      const result = await window.frappe.call({
        method: 'crm.fcrm.permissions.assign_to.get_assignable_users',
        args: {
          doctype: 'CRM Lead',
          name: 'LEAD-00001' // Replace with actual lead name
        }
      });
      console.log('✅ Success:', result);
      return result;
    }
    
    // Test 2: Using fetch
    console.log('Method 2: Using fetch');
    const siteUrl = window.location.origin;
    const url = `${siteUrl}/api/method/crm.fcrm.permissions.assign_to.get_assignable_users`;
    const csrfToken = window.frappe?.csrf_token || '';
    
    const headers = {
      'Content-Type': 'application/json'
    };
    if (csrfToken) {
      headers['X-Frappe-CSRF-Token'] = csrfToken;
    }
    
    const response = await fetch(url, {
      method: 'POST',
      credentials: 'include',
      headers: headers,
      body: JSON.stringify({
        doctype: 'CRM Lead',
        name: 'LEAD-00001' // Replace with actual lead name
      })
    });
    
    const data = await response.json();
    console.log('Response status:', response.status);
    console.log('Response data:', data);
    
    if (response.ok && data.message) {
      console.log('✅ Success:', data.message);
      return data.message;
    } else {
      console.error('❌ Error:', data);
      return null;
    }
  } catch (error) {
    console.error('❌ Exception:', error);
    return null;
  }
}

// Run the test
testAssignableUsersAPI();

