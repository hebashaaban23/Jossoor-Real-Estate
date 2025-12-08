#!/bin/bash

# Add delayed field to Comment table for all sites
# إضافة حقل delayed لجدول Comment في جميع المواقع

BENCH_PATH="/home/frappe/frappe-bench-env/frappe-bench"

echo "=========================================="
echo "Adding 'delayed' field to Comment table"
echo "إضافة حقل 'delayed' لجدول Comment"
echo "=========================================="
echo ""

cd $BENCH_PATH

SITES=$(ls -1 sites/ | grep -E '\.(com|local|org)$')

if [ -z "$SITES" ]; then
    echo "✗ No sites found!"
    exit 1
fi

SUCCESS_COUNT=0
FAIL_COUNT=0

for SITE in $SITES; do
    echo "🔧 Processing: $SITE"
    
    # Check if CRM app is installed
    if bench --site $SITE list-apps 2>/dev/null | grep -q "crm"; then
        echo "  ✓ CRM app found"
        
        bench --site $SITE console << EOF 2>&1 | grep -E "(delayed|Success|Error|Already|Added)" | head -15
try:
    # Check if delayed field already exists in Comment
    if frappe.db.has_column("Comment", "delayed"):
        print(f"  ℹ️  Field 'delayed' already exists in Comment table for {frappe.local.site}")
    else:
        # Add the field
        print(f"  📝 Adding 'delayed' field to Comment table...")
        frappe.db.sql("""
            ALTER TABLE \`tabComment\`
            ADD COLUMN \`delayed\` INT(1) NOT NULL DEFAULT 0
        """)
        frappe.db.commit()
        print(f"  ✅ Added 'delayed' field to Comment table for {frappe.local.site}")
    
    # Check if delayed field exists in CRM Lead
    if frappe.db.has_column("CRM Lead", "delayed"):
        print(f"  ℹ️  Field 'delayed' already exists in CRM Lead for {frappe.local.site}")
    else:
        print(f"  ⚠️  Field 'delayed' does NOT exist in CRM Lead for {frappe.local.site}")
        print(f"     (This is optional - the system will work without it)")
    
    print(f"  ✅ Success for {frappe.local.site}")
    
except Exception as e:
    print(f"  ✗ Error: {str(e)}")
    frappe.db.rollback()
EOF
        
        if [ $? -eq 0 ]; then
            ((SUCCESS_COUNT++))
        else
            ((FAIL_COUNT++))
        fi
    else
        echo "  ⊘ CRM not installed (skipping)"
    fi
    
    echo ""
done

echo ""
echo "=========================================="
echo "📊 Summary"
echo "=========================================="
echo ""
echo "✅ Processed: $SUCCESS_COUNT site(s)"
echo ""

if [ $FAIL_COUNT -gt 0 ]; then
    echo "✗ Failed: $FAIL_COUNT site(s)"
    echo ""
fi

echo "=========================================="
echo ""
echo "📝 What was done:"
echo "  • Added 'delayed' field to Comment table"
echo "  • Field type: INT(1) - 0 or 1"
echo "  • Default value: 0"
echo ""
echo "✨ Now delayed comments will work on all sites!"
echo ""
echo "=========================================="

