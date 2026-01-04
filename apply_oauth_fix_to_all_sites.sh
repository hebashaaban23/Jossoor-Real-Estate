#!/bin/bash

###############################################################################
# Apply OAuth Fix to All Sites
#
# This script applies the OAuth refresh token fix to ALL sites in the bench.
# It sets oauth_refresh_token_expiry to 1 hour (3600 seconds) in each site.
#
# Usage:
#   ./apply_oauth_fix_to_all_sites.sh
#
###############################################################################

set -e  # Exit on error

# Get bench root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Move to bench root
cd "$BENCH_ROOT"

# Get list of sites
SITES_DIR="sites"
SITES=()

for site_dir in "$SITES_DIR"/*; do
    if [[ -d "$site_dir" ]]; then
        site_name=$(basename "$site_dir")
        # Skip non-site directories
        if [[ "$site_name" != "assets" && "$site_name" != "common_site_config.json" ]]; then
            # Check if site.config.json exists (valid site)
            if [[ -f "$site_dir/site_config.json" ]]; then
                SITES+=("$site_name")
            fi
        fi
    fi
done

if [[ ${#SITES[@]} -eq 0 ]]; then
    echo "❌ No sites found in $SITES_DIR"
    exit 1
fi

echo "═══════════════════════════════════════════════════════════════"
echo "  Apply OAuth Fix to All Sites"
echo "  تطبيق إصلاح OAuth على جميع المواقع"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "Found ${#SITES[@]} site(s): ${SITES[*]}"
echo ""

FAILED_SITES=()
SUCCESS_COUNT=0

# Process each site
for site in "${SITES[@]}"; do
    echo "─────────────────────────────────────────────────────────────"
    echo "Processing site: $site"
    echo "─────────────────────────────────────────────────────────────"
    
    # Check if CRM app is installed on this site
    if ! bench --site "$site" list-apps 2>/dev/null | grep -q "crm"; then
        echo "⊘ CRM app not installed on $site (skipping)"
        echo ""
        continue
    fi
    
    echo "✓ CRM app found"
    echo "🔄 Applying OAuth fix..."
    
    # Apply fix via bench execute using the dedicated function
    RESULT=$(bench --site "$site" execute crm.patches.v1_0.set_refresh_token_expiry_1_hour.execute 2>&1)
    EXIT_CODE=$?
    
    # Check result
    if [ $EXIT_CODE -eq 0 ] && echo "$RESULT" | grep -q "=== SUCCESS ==="; then
        echo "✅ Successfully applied OAuth fix to $site"
        echo "$RESULT" | grep -E "✓|Previous value" || true
        ((SUCCESS_COUNT++))
    else
        echo "❌ Failed to apply OAuth fix to $site"
        echo "$RESULT" | tail -15
        FAILED_SITES+=("$site")
    fi
    
    echo ""
done

# Summary
echo "═══════════════════════════════════════════════════════════════"
echo "  Summary"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "✅ Successfully updated: $SUCCESS_COUNT site(s)"
echo ""

if [[ ${#FAILED_SITES[@]} -gt 0 ]]; then
    echo "❌ Failed sites (${#FAILED_SITES[@]}):"
    for failed_site in "${FAILED_SITES[@]}"; do
        echo "   - $failed_site"
    done
    echo ""
    exit 1
else
    echo "🎉 All sites updated successfully!"
    echo ""
    echo "📝 Next steps:"
    echo "   1. Restart bench: bench restart"
    echo "   2. The OAuth fix will work automatically on all sites"
    echo ""
    exit 0
fi

