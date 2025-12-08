#!/bin/bash
###############################################################################
# Bootstrap OAuth2 for All Sites
#
# This script bootstraps OAuth Client configuration for all sites in the bench.
# It iterates through each site directory and runs the bootstrap function.
#
# Usage:
#   ./apps/crm/scripts/bootstrap_all_sites.sh [--print-secrets]
#
# Options:
#   --print-secrets    Include client_secret in output (use with caution!)
#
# Output:
#   Prints client_id (and optionally client_secret) for each site
#
###############################################################################

set -e  # Exit on error

# Get bench root (assume script is in apps/crm/scripts/)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Check if --print-secrets flag is passed
PRINT_SECRETS=0
if [[ "$1" == "--print-secrets" ]]; then
    PRINT_SECRETS=1
    echo "⚠️  WARNING: Client secrets will be printed to stdout!"
    echo ""
fi

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
echo "  OAuth2 Bootstrap for All Sites"
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
    
    # Run bootstrap via bench console
    RESULT=$(bench --site "$site" console <<EOF 2>&1
import frappe
from crm.setup.oauth_bootstrap import bootstrap_site

result = bootstrap_site(print_client_secret=$PRINT_SECRETS)
print("=== RESULT START ===")
import json
print(json.dumps(result, indent=2))
print("=== RESULT END ===")
EOF
)
    
    # Extract JSON result from output
    if echo "$RESULT" | grep -q "=== RESULT START ==="; then
        JSON_OUTPUT=$(echo "$RESULT" | sed -n '/=== RESULT START ===/,/=== RESULT END ===/p' | sed '1d;$d')
        
        # Parse and display result
        OK=$(echo "$JSON_OUTPUT" | python3 -c "import sys, json; print(json.load(sys.stdin).get('ok', False))" 2>/dev/null || echo "false")
        
        if [[ "$OK" == "True" ]]; then
            echo ""
            echo "✅ SUCCESS for $site"
            echo ""
            echo "$JSON_OUTPUT" | python3 -m json.tool
            echo ""
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        else
            echo ""
            echo "❌ FAILED for $site"
            echo ""
            echo "$JSON_OUTPUT" | python3 -m json.tool
            echo ""
            FAILED_SITES+=("$site")
        fi
    else
        echo ""
        echo "❌ FAILED for $site (could not parse result)"
        echo "Output:"
        echo "$RESULT"
        echo ""
        FAILED_SITES+=("$site")
    fi
done

# Summary
echo "═══════════════════════════════════════════════════════════════"
echo "  Bootstrap Summary"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "Total sites: ${#SITES[@]}"
echo "Successful: $SUCCESS_COUNT"
echo "Failed: ${#FAILED_SITES[@]}"

if [[ ${#FAILED_SITES[@]} -gt 0 ]]; then
    echo ""
    echo "Failed sites:"
    for failed_site in "${FAILED_SITES[@]}"; do
        echo "  - $failed_site"
    done
    echo ""
    exit 1
else
    echo ""
    echo "🎉 All sites bootstrapped successfully!"
    echo ""
    exit 0
fi

