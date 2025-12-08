#!/bin/bash

# Script to apply CRM Dashboard changes
# Usage: ./apply_dashboard_changes.sh [site_name]

BENCH_PATH="/home/frappe/frappe-bench-env/frappe-bench"
SITE_NAME=$1

echo "=========================================="
echo "CRM Dashboard Update Script"
echo "=========================================="
echo ""

if [ -z "$SITE_NAME" ]; then
    echo "Please provide the site name as an argument."
    echo "Usage: ./apply_dashboard_changes.sh [site_name]"
    echo ""
    echo "Available sites in your bench:"
    ls -1 $BENCH_PATH/sites/ | grep -E '\.(com|local|org)$'
    echo ""
    exit 1
fi

cd $BENCH_PATH

echo "Site: $SITE_NAME"
echo ""

# Method 1: Using bench console
echo "Applying dashboard changes..."
bench --site $SITE_NAME console << EOF
from crm.fcrm.doctype.crm_dashboard.crm_dashboard import create_default_manager_dashboard
result = create_default_manager_dashboard(force=True)
frappe.db.commit()
print("✓ Dashboard updated successfully!")
EOF

if [ $? -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "✓ Dashboard Updated Successfully!"
    echo "=========================================="
    echo ""
    echo "The new dashboard includes:"
    echo "  • Total Leads"
    echo "  • Delayed Leads"  
    echo "  • Total Deals"
    echo "  • New Leads"
    echo "  • Contacted Leads"
    echo "  • Nurture Leads"
    echo "  • Qualified Leads"
    echo "  • Unqualified Leads"
    echo "  • Junk Leads"
    echo "  • Leads by Status (Donut Chart)"
    echo "  • Leads by Status (Bar Chart)"
    echo ""
    echo "Please refresh your browser to see the changes."
    echo ""
else
    echo ""
    echo "✗ Error updating dashboard. Please check the error messages above."
    echo ""
    exit 1
fi

