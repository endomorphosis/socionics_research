#!/usr/bin/env bash
set -euo pipefail

# Progress monitoring script for comprehensive personality database scraping
# Provides detailed statistics and progress tracking towards the goal of tens of thousands of profiles

echo "=== Personality Database Scraping Progress Monitor ==="
echo ""

# Configuration
DATA_DIR="data/bot_store"
PROFILES_FILE="$DATA_DIR/pdb_profiles.parquet"
VECTORS_FILE="$DATA_DIR/pdb_profile_vectors.parquet"
NORMALIZED_FILE="$DATA_DIR/pdb_profiles_normalized.parquet"
EDGES_FILE="$DATA_DIR/pdb_profile_edges.parquet"
PROGRESS_LOG="$DATA_DIR/comprehensive_scrape_progress.log"
TARGET_PROFILES=${TARGET_PROFILES:-10000}

# Check if data files exist
if [[ ! -f "$PROFILES_FILE" ]]; then
    echo "❌ No profiles data found at $PROFILES_FILE"
    echo "Run the scraping script first: ./scripts/comprehensive_personality_scrape.sh"
    exit 1
fi

# Function to run Python analysis
run_analysis() {
    PYTHONPATH=bot/src python3 -c "$1" 2>/dev/null || echo "N/A"
}

echo "📊 COLLECTION STATISTICS"
echo "========================"

# Basic counts
total_profiles=$(run_analysis "
import pandas as pd
df = pd.read_parquet('$PROFILES_FILE')
print(len(df))
")

unique_profiles=$(run_analysis "
import pandas as pd
df = pd.read_parquet('$PROFILES_FILE')
print(df['cid'].nunique())
")

echo "Total profile entries: $total_profiles"
echo "Unique profiles (CIDs): $unique_profiles"

# Progress towards goal
if [[ "$unique_profiles" != "N/A" ]] && [[ "$unique_profiles" =~ ^[0-9]+$ ]]; then
    progress_percent=$((unique_profiles * 100 / TARGET_PROFILES))
    remaining=$((TARGET_PROFILES - unique_profiles))
    
    echo "Target goal: $TARGET_PROFILES profiles"
    echo "Progress: $progress_percent% complete"
    echo "Remaining: $remaining profiles needed"
    echo ""
    
    # Progress bar
    filled=$((progress_percent / 5))
    empty=$((20 - filled))
    printf "Progress: ["
    printf "%0.s█" $(seq 1 $filled)
    printf "%0.s░" $(seq 1 $empty)
    printf "] $progress_percent%%\n"
    echo ""
fi

# Vector coverage
if [[ -f "$VECTORS_FILE" ]]; then
    vectorized_profiles=$(run_analysis "
    import pandas as pd
    df = pd.read_parquet('$VECTORS_FILE')
    print(df['cid'].nunique())
    ")
    echo "Profiles with vectors: $vectorized_profiles"
    
    if [[ "$vectorized_profiles" != "N/A" ]] && [[ "$unique_profiles" != "N/A" ]] && [[ "$vectorized_profiles" =~ ^[0-9]+$ ]] && [[ "$unique_profiles" =~ ^[0-9]+$ ]] && [[ "$unique_profiles" -gt 0 ]]; then
        vector_percent=$((vectorized_profiles * 100 / unique_profiles))
        echo "Vector coverage: $vector_percent%"
    fi
else
    echo "Profiles with vectors: 0 (no vectors file found)"
fi

echo ""

# Profile type analysis
echo "🎭 PROFILE TYPE ANALYSIS"
echo "========================"

profile_types=$(run_analysis "
import pandas as pd
import json

try:
    df = pd.read_parquet('$PROFILES_FILE')
    
    # Parse JSON payloads and extract profile types
    character_count = 0
    person_count = 0
    other_count = 0
    
    for idx, row in df.head(1000).iterrows():  # Sample first 1000 for performance
        try:
            if isinstance(row['payload_bytes'], bytes):
                payload = json.loads(row['payload_bytes'])
            else:
                payload = row['payload_bytes'] if isinstance(row['payload_bytes'], dict) else json.loads(str(row['payload_bytes']))
            
            is_character = payload.get('isCharacter', False)
            category_name = str(payload.get('categoryName', '')).lower()
            
            if is_character or 'character' in category_name:
                character_count += 1
            elif any(term in category_name for term in ['real', 'person', 'celebrity', 'politician', 'athlete']):
                person_count += 1
            else:
                other_count += 1
        except:
            other_count += 1
    
    total_sampled = character_count + person_count + other_count
    if total_sampled > 0:
        print(f'Sample analysis (first 1000 profiles):')
        print(f'Characters: {character_count} ({character_count*100//total_sampled}%)')
        print(f'Real people: {person_count} ({person_count*100//total_sampled}%)')
        print(f'Other/Unknown: {other_count} ({other_count*100//total_sampled}%)')
        
        # Extrapolate to full dataset
        if total_sampled > 0:
            estimated_chars = int(character_count * len(df) / total_sampled)
            estimated_people = int(person_count * len(df) / total_sampled)
            print(f'\\nEstimated totals:')
            print(f'Characters: ~{estimated_chars:,}')
            print(f'Real people: ~{estimated_people:,}')
    else:
        print('Could not analyze profile types')
        
except Exception as e:
    print(f'Analysis error: {e}')
")

echo "$profile_types"
echo ""

# Source analysis
echo "📈 DATA SOURCES"
echo "==============="

sources=$(run_analysis "
import pandas as pd
import json
from collections import Counter

try:
    df = pd.read_parquet('$PROFILES_FILE')
    sources = []
    
    for idx, row in df.head(2000).iterrows():  # Sample for performance
        try:
            if '_source' in row:
                sources.append(str(row['_source']))
            elif isinstance(row['payload_bytes'], bytes):
                payload = json.loads(row['payload_bytes'])
                if '_source' in payload:
                    sources.append(str(payload['_source']))
        except:
            pass
    
    if sources:
        counter = Counter(sources)
        total = sum(counter.values())
        print(f'Data sources (sample of {total}):')
        for source, count in counter.most_common(10):
            percentage = count * 100 // total
            print(f'  {source}: {count} ({percentage}%)')
    else:
        print('Could not determine data sources')
        
except Exception as e:
    print(f'Source analysis error: {e}')
")

echo "$sources"
echo ""

# Recent activity
echo "🕒 RECENT ACTIVITY" 
echo "=================="

if [[ -f "$PROGRESS_LOG" ]]; then
    echo "Last 10 log entries:"
    tail -n 10 "$PROGRESS_LOG" | while read -r line; do
        echo "  $line"
    done
    echo ""
    
    # Count scraping sessions
    sessions=$(grep -c "Starting comprehensive personality scrape" "$PROGRESS_LOG" 2>/dev/null || echo "0")
    echo "Total scraping sessions: $sessions"
else
    echo "No progress log found at $PROGRESS_LOG"
fi

echo ""

# Performance metrics
echo "⚡ PERFORMANCE METRICS"
echo "====================="

# File sizes
if [[ -f "$PROFILES_FILE" ]]; then
    profiles_size=$(du -h "$PROFILES_FILE" | cut -f1)
    echo "Profiles data size: $profiles_size"
fi

if [[ -f "$VECTORS_FILE" ]]; then
    vectors_size=$(du -h "$VECTORS_FILE" | cut -f1)
    echo "Vectors data size: $vectors_size"
fi

# Index status
index_file="$DATA_DIR/pdb_faiss.index"
if [[ -f "$index_file" ]]; then
    index_size=$(du -h "$index_file" | cut -f1)
    echo "Search index size: $index_size"
    echo "Search index: ✅ Available"
else
    echo "Search index: ❌ Not found"
fi

echo ""

# Recommendations
echo "💡 RECOMMENDATIONS"
echo "=================="

if [[ "$unique_profiles" != "N/A" ]] && [[ "$unique_profiles" =~ ^[0-9]+$ ]]; then
    if [[ "$unique_profiles" -lt 1000 ]]; then
        echo "🚀 Run comprehensive scraping: ./scripts/comprehensive_personality_scrape.sh"
        echo "📝 Check headers file has valid authentication cookies"
        echo "🔧 Consider increasing RPM and CONCURRENCY for faster collection"
    elif [[ "$unique_profiles" -lt 5000 ]]; then
        echo "📈 Good progress! Continue running comprehensive scraping"
        echo "🎯 Focus on character-rich franchises and popular media"
        echo "🔄 Consider adding more specific keywords to comprehensive_keywords.txt"
    elif [[ "$unique_profiles" -lt 10000 ]]; then
        echo "🎉 Excellent progress! You're halfway to the goal"
        echo "🔍 Consider expanding sweep queries (longer runs)"
        echo "📊 Run character extraction: pdb-cli export-characters"
    else
        echo "🎊 SUCCESS! You've achieved the goal of 10,000+ profiles!"
        echo "📚 Consider exporting normalized data for research"
        echo "🧠 Build specialized indices for character analysis"
        echo "📈 Continue scraping for even more comprehensive coverage"
    fi
fi

echo ""
echo "📋 QUICK COMMANDS"
echo "================="
echo "Check detailed coverage:     pdb-cli coverage --sample 15"
echo "Search profiles:            pdb-cli search-faiss 'query' --top 10"
echo "Export characters:          pdb-cli export-characters --sample 20"
echo "Continue scraping:          ./scripts/comprehensive_personality_scrape.sh"
echo "View recent progress:       tail -f $PROGRESS_LOG"
echo ""