#!/bin/bash

# Force update dashboard for all sites with cache clearing
# تحديث إجباري لـ Dashboard مع مسح Cache لجميع المواقع

BENCH_PATH="/home/frappe/frappe-bench-env/frappe-bench"

echo "=========================================="
echo "Force Update Dashboard - All Sites"
echo "تحديث إجباري للـ Dashboard - جميع المواقع"
echo "=========================================="
echo ""

cd $BENCH_PATH

# Get list of all sites
SITES=$(ls -1 sites/ | grep -E '\.(com|local|org)$')

if [ -z "$SITES" ]; then
    echo "✗ No sites found!"
    exit 1
fi

echo "Sites found / المواقع:"
echo "$SITES"
echo ""
echo "=========================================="
echo ""

SUCCESS_COUNT=0
FAIL_COUNT=0
FAILED_SITES=""

for SITE in $SITES; do
    echo "🔄 Processing: $SITE"
    echo "معالجة: $SITE"
    echo "---"
    
    # Check if CRM app is installed
    if bench --site $SITE list-apps 2>/dev/null | grep -q "crm"; then
        echo "✓ CRM app found"
        
        # Clear cache first
        echo "  📦 Clearing cache..."
        bench --site $SITE clear-cache 2>&1 | grep -v "^$"
        bench --site $SITE clear-website-cache 2>&1 | grep -v "^$"
        
        # Check current Lead Statuses count
        echo "  🔍 Checking Lead Statuses..."
        bench --site $SITE console << EOF 2>&1 | grep -E "(Found|statuses|Dashboard)" | head -5
statuses = frappe.db.sql("SELECT COUNT(*) as count FROM \`tabCRM Lead Status\`", as_dict=1)
print(f"Found {statuses[0].count} Lead Statuses in {frappe.local.site}")
EOF
        
        # Force update dashboard
        echo "  🔨 Force updating Dashboard..."
        bench --site $SITE console << EOF 2>&1 | grep -E "(Updated|Success|Error|items)" | head -10
try:
    from crm.fcrm.doctype.crm_dashboard.crm_dashboard import create_default_manager_dashboard
    import json
    
    # Force recreate
    result = create_default_manager_dashboard(force=True)
    frappe.db.commit()
    
    # Verify
    layout = json.loads(result)
    status_cards = [item for item in layout if item['name'].startswith('lead_status_')]
    
    print(f"✓ Dashboard updated successfully for $SITE")
    print(f"  Total items: {len(layout)}")
    print(f"  Status cards: {len(status_cards)}")
    
except Exception as e:
    print(f"✗ Error: {str(e)}")
    frappe.db.rollback()
EOF
        
        if [ $? -eq 0 ]; then
            echo "  ✓ Success!"
            ((SUCCESS_COUNT++))
        else
            echo "  ✗ Failed!"
            ((FAIL_COUNT++))
            FAILED_SITES="$FAILED_SITES\n  - $SITE"
        fi
    else
        echo "⊘ CRM not installed (skipping)"
    fi
    
    echo ""
    echo "=========================================="
    echo ""
done

# Clear cache again after all updates
echo "🧹 Final cache clearing for all sites..."
bench clear-cache 2>&1 | grep -v "^$"

echo ""
echo "=========================================="
echo "📊 Summary / الملخص"
echo "=========================================="
echo ""
echo "✓ Updated: $SUCCESS_COUNT site(s)"
echo "✓ تم التحديث: $SUCCESS_COUNT موقع"
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
echo "⚡ Next Steps / الخطوات التالية:"
echo ""
echo "1. Open each site in browser"
echo "   افتح كل موقع في المتصفح"
echo ""
echo "2. Hard Refresh (Ctrl+Shift+R)"
echo "   تحديث قوي (Ctrl+Shift+R)"
echo ""
echo "3. Check Dashboard has correct number of cards"
echo "   تحقق من عدد البطاقات الصحيح"
echo ""
echo "=========================================="

