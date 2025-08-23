#!/usr/bin/env bash
set -euo pipefail

# Advanced keyword discovery and optimization for comprehensive PDB scraping
# Analyzes existing data to identify high-yield keywords and expansion opportunities

echo "🔍 Advanced Keyword Discovery & Scraping Optimization"
echo "====================================================="
echo ""

# Configuration
DISCOVERY_LIMIT=${DISCOVERY_LIMIT:-50}
MIN_PROFILES_PER_KEYWORD=${MIN_PROFILES_PER_KEYWORD:-5}
OUTPUT_FILE=${OUTPUT_FILE:-data/bot_store/discovered_keywords.txt}

echo "📊 Analyzing current collection for keyword opportunities..."

# Analyze collected profiles to discover high-value terms
PYTHONPATH=bot/src python3 -c "
import pandas as pd
import json
from collections import Counter
import re

try:
    # Load existing profiles
    df = pd.read_parquet('data/bot_store/pdb_profiles.parquet')
    print(f'Analyzing {len(df)} profiles for keyword discovery...')
    
    # Extract terms from profile data
    all_terms = []
    franchise_terms = []
    character_names = []
    
    for idx, row in df.iterrows():
        try:
            # Parse profile payload
            if isinstance(row['payload_bytes'], bytes):
                payload = json.loads(row['payload_bytes'])
            else:
                payload = row['payload_bytes'] if isinstance(row['payload_bytes'], dict) else json.loads(str(row['payload_bytes']))
            
            # Extract valuable terms
            name = payload.get('name', '')
            if name:
                character_names.append(name.strip())
                # Extract individual words from character names
                words = re.findall(r'\b[A-Za-z]{3,}\b', name)
                all_terms.extend(words)
            
            # Extract from category/subcategory
            category = payload.get('categoryName', '')
            if category:
                all_terms.append(category.strip())
                # Look for franchise indicators
                if any(term in category.lower() for term in ['movie', 'tv', 'series', 'game', 'book', 'comic', 'anime', 'manga']):
                    franchise_terms.append(category.strip())
            
            subcategory = payload.get('subcategoryName', '') 
            if subcategory:
                all_terms.append(subcategory.strip())
                franchise_terms.append(subcategory.strip())
            
            # Extract from description/bio if available
            desc = payload.get('description', '') or payload.get('bio', '')
            if desc and len(desc) > 10:
                # Extract capitalized terms that might be names or franchises
                caps_terms = re.findall(r'\b[A-Z][a-z]{2,}\b', desc)
                all_terms.extend(caps_terms[:10])  # Limit to avoid noise
                
        except Exception as e:
            continue
    
    print(f'\\nExtracted {len(all_terms)} total terms, {len(set(franchise_terms))} franchise terms, {len(set(character_names))} character names')
    
    # Count term frequencies and find high-value keywords
    term_counts = Counter(all_terms)
    franchise_counts = Counter(franchise_terms)
    
    print(f'\\n🎯 Top potential franchise keywords (appearing in ${MIN_PROFILES_PER_KEYWORD}+ profiles):')
    high_value_franchises = []
    for term, count in franchise_counts.most_common($DISCOVERY_LIMIT):
        if count >= $MIN_PROFILES_PER_KEYWORD and len(term) > 2:
            print(f'  {term}: {count} profiles')
            high_value_franchises.append(term)
    
    print(f'\\n⭐ Top character/general keywords:')
    high_value_general = []
    for term, count in term_counts.most_common($DISCOVERY_LIMIT):
        if count >= $MIN_PROFILES_PER_KEYWORD and len(term) > 2 and term not in high_value_franchises:
            print(f'  {term}: {count} profiles') 
            high_value_general.append(term)
    
    # Write discovered keywords to file
    with open('$OUTPUT_FILE', 'w') as f:
        f.write('# Auto-discovered high-value keywords\\n')
        f.write('# Generated from analysis of existing profile collection\\n\\n')
        
        f.write('# High-yield franchises and media properties\\n')
        for term in high_value_franchises:
            f.write(f'{term}\\n')
        
        f.write('\\n# Character names and general terms\\n')
        for term in high_value_general[:25]:  # Limit general terms
            f.write(f'{term}\\n')
    
    print(f'\\n💾 Saved {len(high_value_franchises) + min(25, len(high_value_general))} discovered keywords to $OUTPUT_FILE')
    
    # Analyze gaps in coverage
    print(f'\\n🔍 Coverage gap analysis:')
    
    # Look for underrepresented character types
    character_types = []
    for idx, row in df.head(1000).iterrows():  # Sample for performance
        try:
            if isinstance(row['payload_bytes'], bytes):
                payload = json.loads(row['payload_bytes'])
            else:
                payload = row['payload_bytes'] if isinstance(row['payload_bytes'], dict) else json.loads(str(row['payload_bytes']))
            
            name = payload.get('name', '').lower()
            category = payload.get('categoryName', '').lower()
            
            # Classify character types
            if any(term in name + category for term in ['villain', 'evil', 'bad', 'dark']):
                character_types.append('villain')
            elif any(term in name + category for term in ['hero', 'good', 'protagonist']):
                character_types.append('hero')
            elif any(term in name + category for term in ['detective', 'investigator', 'cop']):
                character_types.append('detective')
            elif any(term in name + category for term in ['scientist', 'doctor', 'professor']):
                character_types.append('scientist')
            elif any(term in name + category for term in ['magic', 'wizard', 'witch', 'mage']):
                character_types.append('magic_user')
            elif any(term in name + category for term in ['warrior', 'fighter', 'soldier']):
                character_types.append('warrior')
        except:
            continue
    
    type_counts = Counter(character_types)
    print('Character type distribution (sample):')
    for char_type, count in type_counts.most_common():
        print(f'  {char_type}: {count}')
    
    # Suggest expansion strategies
    print(f'\\n💡 Recommended expansion strategies:')
    
    if len(high_value_franchises) > 10:
        print('✅ Good franchise coverage detected')
    else:
        print('📈 Consider adding more popular franchises and media properties')
    
    total_chars = sum(1 for t in character_types if t in ['hero', 'villain'])
    if total_chars < 100:
        print('🎭 Consider targeting character-rich franchises (superhero, fantasy, anime)')
    else:
        print('✅ Good character representation detected')
        
    if len(set(character_names)) < len(df) * 0.3:
        print('📛 Many profiles may not have clear character names - consider broader search terms')
    else:
        print('✅ Good character name coverage')

except Exception as e:
    print(f'Analysis failed: {e}')
    import traceback
    traceback.print_exc()
"

echo ""
echo "🚀 Optimization Recommendations"
echo "==============================="

# Analyze current scraping effectiveness
current_profiles=$(python3 -c "
try:
    import pandas as pd
    df = pd.read_parquet('data/bot_store/pdb_profiles.parquet')
    print(df['cid'].nunique())
except:
    print('0')
" 2>/dev/null)

if [[ "$current_profiles" -lt 5000 ]]; then
    echo "📈 Low coverage detected ($current_profiles profiles). Recommendations:"
    echo "   • Use discovered keywords file: ADDITIONAL_KEYWORDS_FILE=$OUTPUT_FILE"
    echo "   • Increase search depth: PAGES=10 SWEEP_PAGES=50"
    echo "   • Run longer sessions: Remove timeouts, use stateful scanning"
    echo "   • Focus on character filtering: USE_CHARACTER_FILTERING=1"
elif [[ "$current_profiles" -lt 10000 ]]; then
    echo "📊 Moderate coverage ($current_profiles profiles). Recommendations:"
    echo "   • Combine discovered keywords with existing lists"
    echo "   • Use relationship expansion: Run scan-all with larger frontier"  
    echo "   • Target specific genres: Add anime, gaming, literature keywords"
    echo "   • Verify authentication: Ensure headers are current"
else
    echo "🎉 Excellent coverage ($current_profiles profiles)! Optimization tips:"
    echo "   • Focus on quality: Export character-only datasets"
    echo "   • Build specialized indices: pdb-cli index-characters"
    echo "   • Analyze relationships: pdb-cli edges-analyze"
    echo "   • Continue expansion: Use discovered keywords for niche content"
fi

echo ""
echo "🛠️ Next Steps"
echo "============="
echo ""
echo "1. Review discovered keywords:"
echo "   cat $OUTPUT_FILE"
echo ""
echo "2. Run optimized comprehensive scraping:"
echo "   ADDITIONAL_KEYWORDS_FILE=$OUTPUT_FILE ./scripts/comprehensive_personality_scrape.sh"
echo ""
echo "3. Monitor progress:"
echo "   ./scripts/scrape_progress_monitor.sh"
echo ""
echo "4. For maximum coverage, run intensive mode:"
echo "   RPM=120 CONCURRENCY=5 PAGES=8 SWEEP_PAGES=40 ./scripts/comprehensive_personality_scrape.sh"
echo ""

if [[ -f "$OUTPUT_FILE" ]]; then
    discovered_count=$(wc -l < "$OUTPUT_FILE")
    echo "✅ Generated $discovered_count new keyword suggestions"
    echo "💡 Use these keywords to target underrepresented content areas"
fi

echo ""
echo "🎯 Goal Progress: $current_profiles / 10,000+ profiles collected"
remaining=$((10000 - current_profiles))
if [[ "$current_profiles" -ge 10000 ]]; then
    echo "🎊 GOAL ACHIEVED! Consider expanding to 25,000+ for comprehensive coverage"
else
    echo "📈 $remaining profiles remaining to reach goal"
fi