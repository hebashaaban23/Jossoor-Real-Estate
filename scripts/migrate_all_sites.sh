#!/bin/bash

###############################################################################
# Migrate All Sites - Add "late" Status to CRM Task
#
# This script runs bench migrate on all sites in the bench to apply
# the patch that adds "late" status to CRM Task doctype.
#
# Usage:
#   ./apps/crm/scripts/migrate_all_sites.sh
#
###############################################################################

set -e  # Exit on error

# Get bench root (assume script is in apps/crm/scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Move to bench root
cd "$BENCH_ROOT"

# Get list of sites (exclude common_site_config.json and assets)
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
echo "  Migrate All Sites - Add 'late' Status to CRM Task"
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
    echo "🔄 Running migrate..."
    
    # Run migrate
    if bench --site "$site" migrate 2>&1; then
        echo "✅ Successfully migrated $site"
        ((SUCCESS_COUNT++))
    else
        echo "❌ Failed to migrate $site"
        FAILED_SITES+=("$site")
    fi
    
    echo ""
done

# Summary
echo "═══════════════════════════════════════════════════════════════"
echo "  Summary"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "✅ Successfully migrated: $SUCCESS_COUNT site(s)"
echo ""

if [[ ${#FAILED_SITES[@]} -gt 0 ]]; then
    echo "❌ Failed sites (${#FAILED_SITES[@]}):"
    for failed_site in "${FAILED_SITES[@]}"; do
        echo "   - $failed_site"
    done
    echo ""
    exit 1
else
    echo "🎉 All sites migrated successfully!"
    exit 0
fi

