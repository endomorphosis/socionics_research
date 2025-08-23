#!/usr/bin/env bash
set -euo pipefail

# Comprehensive Personality Database Scraping Script
# Goal: Systematically collect tens of thousands of personality profiles using keyword-based search
# and iterative relationship expansion with robust state management and progress tracking.

echo "=== Comprehensive Personality Database Scraping ==="
echo "Goal: Collect tens of thousands of personality profiles systematically"
echo ""

# Configuration variables with sensible defaults
RPM=${RPM:-120}                              # Requests per minute
CONCURRENCY=${CONCURRENCY:-4}                # Parallel requests  
TIMEOUT=${TIMEOUT:-30}                       # HTTP timeout seconds
PAGES=${PAGES:-5}                           # Pages per keyword search
SWEEP_PAGES=${SWEEP_PAGES:-30}              # Pages per sweep query (a-z, 0-9)
MAX_NO_PROGRESS_PAGES=${MAX_NO_PROGRESS_PAGES:-5}  # Stop after N no-progress pages
INITIAL_FRONTIER_SIZE=${INITIAL_FRONTIER_SIZE:-2000}  # Larger initial frontier

# Keyword strategy configuration
KEYWORDS_FILE=${KEYWORDS_FILE:-data/bot_store/large_keywords.txt}
ADDITIONAL_KEYWORDS_FILE=${ADDITIONAL_KEYWORDS_FILE:-data/bot_store/comprehensive_keywords.txt}
USE_FRANCHISE_EXPANSION=${USE_FRANCHISE_EXPANSION:-1}  # Expand franchise keywords
USE_CHARACTER_FILTERING=${USE_CHARACTER_FILTERING:-1}  # Focus on character profiles

# API endpoints
V2_BASE_URL=${PDB_API_BASE_URL:-https://api.personality-database.com/api/v2}
V1_BASE_URL=${PDB_V1_BASE_URL:-https://api.personality-database.com/api/v1}
HEADERS_FILE=${HEADERS_FILE:-.secrets/pdb_headers.json}

# Output configuration
INDEX_OUT=${INDEX_OUT:-data/bot_store/pdb_faiss.index}
PROGRESS_LOG=${PROGRESS_LOG:-data/bot_store/comprehensive_scrape_progress.log}
STATE_FILE=${STATE_FILE:-data/bot_store/comprehensive_scan_state.json}

# Validation
if [[ ! -f "$HEADERS_FILE" ]]; then
  echo "ERROR: Missing headers file: $HEADERS_FILE" >&2
  echo "Create it with browser-like headers and cookies from an active PDB session." >&2
  echo "See data/bot_store/headers.template.json for format." >&2
  exit 1
fi

if [[ ! -f "$KEYWORDS_FILE" ]]; then
  echo "ERROR: Missing keywords file: $KEYWORDS_FILE" >&2
  exit 1
fi

# Enable caching for efficiency
export PDB_CACHE=1

# Setup progress logging
mkdir -p "$(dirname "$PROGRESS_LOG")"
echo "$(date): Starting comprehensive personality scrape" | tee -a "$PROGRESS_LOG"
echo "Configuration: RPM=$RPM, CONCURRENCY=$CONCURRENCY, PAGES=$PAGES" | tee -a "$PROGRESS_LOG"

# Trap for clean interruption
trap 'echo; echo "Interrupted. Progress saved. Resume by re-running this script." | tee -a "$PROGRESS_LOG"' INT TERM

# Function to log progress with timestamp
log_progress() {
    echo "$(date): $1" | tee -a "$PROGRESS_LOG"
}

# Function to get current profile count
get_profile_count() {
    if [[ -f "data/bot_store/pdb_profiles.parquet" ]]; then
        python3 -c "import pandas as pd; df = pd.read_parquet('data/bot_store/pdb_profiles.parquet'); print(df['cid'].nunique())" 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

# Initial status
initial_count=$(get_profile_count)
log_progress "Starting with $initial_count unique profiles"

echo ""
echo "Phase 1: Systematic Keyword-Based Search"
echo "========================================"

# Prepare comprehensive keyword list by combining base keywords with additional ones
combined_keywords_file="/tmp/comprehensive_keywords_combined.txt"
cat "$KEYWORDS_FILE" > "$combined_keywords_file"

# Add additional keywords if file exists
if [[ -f "$ADDITIONAL_KEYWORDS_FILE" ]]; then
    cat "$ADDITIONAL_KEYWORDS_FILE" >> "$combined_keywords_file"
    log_progress "Added additional keywords from $ADDITIONAL_KEYWORDS_FILE"
fi

# Count total keywords
total_keywords=$(grep -v '^#' "$combined_keywords_file" | grep -v '^[[:space:]]*$' | wc -l)
log_progress "Processing $total_keywords keywords systematically"

# Build search-keywords arguments
search_args=(
    --rpm "$RPM" 
    --concurrency "$CONCURRENCY" 
    --timeout "$TIMEOUT"
    --base-url "$V2_BASE_URL"
    --headers "$(tr -d '\n' < "$HEADERS_FILE")"
    search-keywords
    --query-file "$combined_keywords_file"
    --limit 40
    --pages "$PAGES"
    --until-empty
    --max-no-progress-pages "$MAX_NO_PROGRESS_PAGES"
    --only-profiles
    --auto-embed
    --auto-index
    --index-out "$INDEX_OUT"
    --expand-subcategories
    --expand-max 10
    --expand-boards
    --boards-max 5
    --chase-hints
    --hints-max 5
    --log-file "$PROGRESS_LOG"
)

# Add character filtering if enabled
if [[ "$USE_CHARACTER_FILTERING" == "1" ]]; then
    search_args+=(--filter-characters --characters-relaxed --force-character-group)
    log_progress "Character filtering enabled for personality focus"
fi

# Add franchise expansion if enabled
if [[ "$USE_FRANCHISE_EXPANSION" == "1" ]]; then
    search_args+=(--expand-characters --expand-pages 3 --append-terms "characters,cast,protagonists,heroes,villains")
    log_progress "Franchise expansion enabled for broader coverage"
fi

log_progress "Starting systematic keyword search phase..."

# Run the comprehensive keyword search
PYTHONPATH=bot/src python -u -m bot.pdb_cli "${search_args[@]}" || {
    log_progress "Keyword search phase encountered errors but continuing..."
}

# Check progress after keyword phase
keyword_phase_count=$(get_profile_count)
keyword_gain=$((keyword_phase_count - initial_count))
log_progress "Keyword phase complete. Gained $keyword_gain profiles (total: $keyword_phase_count)"

echo ""
echo "Phase 2: Iterative Relationship Expansion"
echo "========================================="

# Now run scan-all for comprehensive expansion using discovered profiles as seeds
log_progress "Starting relationship expansion phase..."

scan_all_args=(
    --rpm "$RPM"
    --concurrency "$CONCURRENCY"
    --timeout "$TIMEOUT"
    --base-url "$V2_BASE_URL"
    --headers "$(tr -d '\n' < "$HEADERS_FILE")"
    scan-all
    --max-iterations 0  # Until exhaustion
    --initial-frontier-size "$INITIAL_FRONTIER_SIZE"
    --search-names
    --limit 30
    --pages 3
    --until-empty
    --sweep-queries "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,0,1,2,3,4,5,6,7,8,9"
    --sweep-pages "$SWEEP_PAGES"
    --sweep-until-empty
    --sweep-into-frontier
    --max-no-progress-pages "$MAX_NO_PROGRESS_PAGES"
    --auto-embed
    --auto-index
    --index-out "$INDEX_OUT"
    --scrape-v1
    --v1-base-url "$V1_BASE_URL"
    --v1-headers "$(tr -d '\n' < "$HEADERS_FILE")"
    --use-state
    --state-file "$STATE_FILE"
)

# Add character filtering to expansion phase too
if [[ "$USE_CHARACTER_FILTERING" == "1" ]]; then
    scan_all_args+=(--filter-characters --characters-relaxed --expand-subcategories --expand-max 10 --force-character-group)
fi

PYTHONPATH=bot/src python -u -m bot.pdb_cli "${scan_all_args[@]}" || {
    log_progress "Relationship expansion encountered errors but continuing..."
}

# Check final progress
final_count=$(get_profile_count)
expansion_gain=$((final_count - keyword_phase_count))
total_gain=$((final_count - initial_count))

echo ""
echo "Phase 3: Coverage Analysis and Reporting"
echo "======================================="

log_progress "Expansion phase complete. Gained $expansion_gain profiles (total: $final_count)"
log_progress "TOTAL COLLECTION: $total_gain new profiles collected this run"

# Generate comprehensive coverage report
log_progress "Generating coverage report..."
PYTHONPATH=bot/src python -m bot.pdb_cli coverage --sample 15 | tee -a "$PROGRESS_LOG" || true

# Export normalized data for analysis
log_progress "Exporting normalized profile data..."
PYTHONPATH=bot/src python -m bot.pdb_cli export --out "data/bot_store/pdb_profiles_normalized_$(date +%Y%m%d).parquet" || true

# Character extraction if we have substantial data
if [[ "$final_count" -gt 5000 ]]; then
    log_progress "Extracting character-focused dataset..."
    PYTHONPATH=bot/src python -m bot.pdb_cli export-characters --sample 25 || true
    PYTHONPATH=bot/src python -m bot.pdb_cli index-characters || true
fi

echo ""
echo "=== COMPREHENSIVE SCRAPING COMPLETE ==="
echo "Started with:  $initial_count profiles"
echo "Ended with:    $final_count profiles"
echo "Gained:        $total_gain profiles"
echo ""

if [[ "$final_count" -ge 10000 ]]; then
    echo "🎉 SUCCESS: Achieved goal of 10,000+ profiles!"
    log_progress "SUCCESS: Reached $final_count profiles (10,000+ goal achieved)"
elif [[ "$final_count" -ge 5000 ]]; then
    echo "📈 PROGRESS: Good progress with $final_count profiles. Consider additional keyword expansion."
    log_progress "PROGRESS: $final_count profiles collected (halfway to 10,000+ goal)"
else
    echo "📊 FOUNDATION: Built solid foundation with $final_count profiles. Consider running longer or adding more keywords."
    log_progress "FOUNDATION: $final_count profiles collected (building towards 10,000+ goal)"
fi

echo ""
echo "Progress log: $PROGRESS_LOG"
echo "State file:   $STATE_FILE" 
echo "Index file:   $INDEX_OUT"
echo ""
echo "To continue scraping, run this script again. State is preserved."
echo "To check progress: pdb-cli coverage --sample 10"
echo "To search: pdb-cli search-faiss 'your query' --top 10"

log_progress "Comprehensive scraping session completed successfully"