#!/usr/bin/env python3
"""
Script to update Client Script for Lead to use assign_without_rule correctly
"""
import frappe

def update_client_script():
    """Update the Client Script to pass names parameter"""
    
    # Find the Client Script
    script_name = "Filtered Buttons"
    if frappe.db.exists("Client Script", {"name": script_name, "dt": "Lead"}):
        script = frappe.get_doc("Client Script", script_name)
        
        # Check if script already has assign_without_rule with names
        if "assign_without_rule" in script.script and "names: validNames" in script.script:
            print(f"✓ Client Script '{script_name}' already updated")
            return
        
        # Update the script
        old_script = script.script
        
        # Replace the assignment part
        if "frappe.desk.form.assign_to.add" in old_script:
            new_script = old_script.replace(
                """if (assignees.length > 0) {
                                // Assign new users - filter out any invalid names
                                const promises = names
                                    .filter(name => name && (typeof name === 'string' ? name.trim() !== '' : true))
                                    .map(name => {
                                    return frappe.call({
                                        method: 'frappe.desk.form.assign_to.add',
                                        args: {
                                            doctype: 'Lead',
                                            name: name,
                                            assign_to: assignees,
                                            description: ''
                                        }
                                    });
                                });
                                
                                Promise.all(promises).then(() => {
                                    d.hide();
                                    frappe.show_alert({
                                        message: __('Assigned successfully'),
                                        indicator: 'green'
                                    });
                                    listview.refresh();
                                });""",
                """if (assignees.length > 0) {
                                // Use assign_without_rule for bulk assignment to bypass assignment rules
                                const validNames = names.filter(name => name && (typeof name === 'string' ? name.trim() !== '' : true));
                                
                                if (validNames.length === 0) {
                                    frappe.msgprint(__('No valid document names found'));
                                    return;
                                }
                                
                                frappe.call({
                                    method: 'crm.api.doc.assign_without_rule',
                                    args: {
                                        doctype: 'Lead',
                                        assign_to: assignees,
                                        names: validNames,
                                        description: ''
                                    },
                                    callback: function(r) {
                                        if (r.exc) {
                                            frappe.msgprint(__('Error assigning: {0}', [r.exc]));
                                            return;
                                        }
                                        d.hide();
                                        frappe.show_alert({
                                            message: __('Assigned successfully'),
                                            indicator: 'green'
                                        });
                                        listview.refresh();
                                    }
                                });"""
            )
            
            script.script = new_script
            script.save()
            frappe.db.commit()
            print(f"✓ Updated Client Script '{script_name}'")
        else:
            print(f"⚠ Client Script '{script_name}' doesn't contain expected code")
    else:
        print(f"✗ Client Script '{script_name}' not found")

if __name__ == '__main__':
    frappe.init(site='your-site-name')  # Replace with your site name
    frappe.connect()
    update_client_script()
    frappe.destroy()

