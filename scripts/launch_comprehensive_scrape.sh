#!/usr/bin/env bash
set -euo pipefail

# Quick launcher for comprehensive personality database scraping
# Provides easy access to comprehensive scraping with good defaults

echo "🎭 Comprehensive Personality Database Scraper"
echo "============================================="
echo ""

# Check for required files
if [[ ! -f ".secrets/pdb_headers.json" ]]; then
    echo "❌ Missing authentication file: .secrets/pdb_headers.json"
    echo ""
    echo "To set up authentication:"
    echo "1. Copy data/bot_store/headers.template.json to .secrets/pdb_headers.json"
    echo "2. Update with your browser headers from an active PDB session"
    echo "3. Include cookies, User-Agent, and other browser headers"
    echo ""
    echo "Example setup:"
    echo "  mkdir -p .secrets"
    echo "  cp data/bot_store/headers.template.json .secrets/pdb_headers.json"
    echo "  # Edit .secrets/pdb_headers.json with real headers"
    echo ""
    exit 1
fi

# Get current status
current_profiles=$(python3 -c "
try:
    import pandas as pd
    df = pd.read_parquet('data/bot_store/pdb_profiles.parquet')
    print(df['cid'].nunique())
except:
    print('0')
" 2>/dev/null)

echo "Current collection: $current_profiles unique profiles"
echo "Target goal: 10,000+ profiles"
echo ""

# Menu options
echo "Choose scraping strategy:"
echo ""
echo "1) 🚀 Quick start (moderate resources, ~2-4 hours)"
echo "   - 90 RPM, 3 concurrent requests"
echo "   - Focus on popular franchises and characters"
echo "   - Good for initial collection"
echo ""
echo "2) ⚡ Intensive (high resources, ~4-8 hours)" 
echo "   - 120 RPM, 5 concurrent requests"
echo "   - Comprehensive keyword expansion"
echo "   - Maximum coverage strategy"
echo ""
echo "3) 🐌 Conservative (low resources, ~8-12 hours)"
echo "   - 60 RPM, 2 concurrent requests"
echo "   - Careful rate limiting"
echo "   - Good for avoiding rate limits"
echo ""
echo "4) 📊 Progress monitor only"
echo "   - Check current status and statistics"
echo "   - No scraping performed"
echo ""
echo "5) 🛠️ Custom configuration"
echo "   - Set your own parameters"
echo ""

read -p "Select option (1-5): " choice

case $choice in
    1)
        echo "🚀 Starting quick comprehensive scraping..."
        RPM=90 CONCURRENCY=3 PAGES=3 SWEEP_PAGES=15 ./scripts/comprehensive_personality_scrape.sh
        ;;
    2) 
        echo "⚡ Starting intensive comprehensive scraping..."
        RPM=120 CONCURRENCY=5 PAGES=5 SWEEP_PAGES=25 INITIAL_FRONTIER_SIZE=3000 ./scripts/comprehensive_personality_scrape.sh
        ;;
    3)
        echo "🐌 Starting conservative comprehensive scraping..."
        RPM=60 CONCURRENCY=2 PAGES=2 SWEEP_PAGES=10 ./scripts/comprehensive_personality_scrape.sh
        ;;
    4)
        echo "📊 Showing progress monitor..."
        ./scripts/scrape_progress_monitor.sh
        ;;
    5)
        echo "🛠️ Custom configuration:"
        echo ""
        read -p "Requests per minute (default 90): " rpm
        read -p "Concurrent requests (default 3): " concurrency  
        read -p "Pages per keyword (default 3): " pages
        read -p "Sweep pages per query (default 15): " sweep_pages
        
        rpm=${rpm:-90}
        concurrency=${concurrency:-3}
        pages=${pages:-3}
        sweep_pages=${sweep_pages:-15}
        
        echo "Starting with RPM=$rpm, CONCURRENCY=$concurrency, PAGES=$pages, SWEEP_PAGES=$sweep_pages..."
        RPM=$rpm CONCURRENCY=$concurrency PAGES=$pages SWEEP_PAGES=$sweep_pages ./scripts/comprehensive_personality_scrape.sh
        ;;
    *)
        echo "Invalid option. Please run again and choose 1-5."
        exit 1
        ;;
esac

# Show final progress
echo ""
echo "📈 Final status:"
./scripts/scrape_progress_monitor.sh