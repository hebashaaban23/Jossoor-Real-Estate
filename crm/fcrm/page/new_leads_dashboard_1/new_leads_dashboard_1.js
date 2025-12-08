frappe.pages['new-leads-dashboard-1'].on_page_load = function(wrapper) {
	var page = frappe.ui.make_app_page({
		parent: wrapper,
		title: 'New Leads Dashboard',
		single_column: true
	});
}