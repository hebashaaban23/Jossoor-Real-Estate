#!/bin/bash

# Script to apply CRM Dashboard changes to ALL sites
# تطبيق تغييرات Dashboard على جميع المواقع

BENCH_PATH="/home/frappe/frappe-bench-env/frappe-bench"

echo "=========================================="
echo "CRM Dashboard Update - All Sites"
echo "تحديث Dashboard لجميع المواقع"
echo "=========================================="
echo ""

cd $BENCH_PATH

# Get list of all sites with .com, .local, or .org extensions
SITES=$(ls -1 sites/ | grep -E '\.(com|local|org)$')

if [ -z "$SITES" ]; then
    echo "✗ No sites found!"
    echo "✗ لم يتم العثور على مواقع!"
    exit 1
fi

echo "Found sites / المواقع الموجودة:"
echo "$SITES"
echo ""
echo "=========================================="
echo ""

SUCCESS_COUNT=0
FAIL_COUNT=0
FAILED_SITES=""

for SITE in $SITES; do
    echo "Processing site: $SITE"
    echo "معالجة الموقع: $SITE"
    echo "---"
    
    # Check if CRM app is installed on this site
    if bench --site $SITE list-apps 2>/dev/null | grep -q "crm"; then
        echo "✓ CRM app found on $SITE"
        
        # Apply dashboard changes
        bench --site $SITE console << EOF 2>&1 | grep -v "^$"
try:
    from crm.fcrm.doctype.crm_dashboard.crm_dashboard import create_default_manager_dashboard
    result = create_default_manager_dashboard(force=True)
    frappe.db.commit()
    print("✓ Dashboard updated successfully for $SITE")
except Exception as e:
    print(f"✗ Error: {str(e)}")
    frappe.db.rollback()
EOF
        
        if [ $? -eq 0 ]; then
            echo "✓ Success for $SITE"
            ((SUCCESS_COUNT++))
        else
            echo "✗ Failed for $SITE"
            ((FAIL_COUNT++))
            FAILED_SITES="$FAILED_SITES\n  - $SITE"
        fi
    else
        echo "⊘ CRM app not installed on $SITE (skipping)"
        echo "⊘ تطبيق CRM غير مثبت على $SITE (تم التخطي)"
    fi
    
    echo ""
    echo "=========================================="
    echo ""
done

echo ""
echo "=========================================="
echo "Summary / الملخص"
echo "=========================================="
echo ""
echo "✓ Successfully updated: $SUCCESS_COUNT site(s)"
echo "✓ تم التحديث بنجاح: $SUCCESS_COUNT موقع"
echo ""

if [ $FAIL_COUNT -gt 0 ]; then
    echo "✗ Failed: $FAIL_COUNT site(s)"
    echo "✗ فشل: $FAIL_COUNT موقع"
    echo ""
    echo "Failed sites:"
    echo -e "$FAILED_SITES"
    echo ""
fi

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
echo "الرجاء تحديث المتصفح لرؤية التغييرات."
echo ""
echo "=========================================="

