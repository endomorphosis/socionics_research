#!/usr/bin/env python3
"""
Get latest results script for personality database viewer.
Retrieves the most recent scraped profiles for feedback review.
"""

import sys
import json
import os
import pandas as pd

def main():
    if len(sys.argv) < 3:
        print(json.dumps({"error": "Usage: get_latest_results.py <dataset_dir> <limit>"}))
        sys.exit(1)
        
    dataset_dir = sys.argv[1]
    limit = int(sys.argv[2])
    
    # Try to load the main profiles file
    profiles_file = os.path.join(dataset_dir, 'pdb_profiles.parquet')
    
    if not os.path.exists(profiles_file):
        print(json.dumps([]))
        return
    
    try:
        df = pd.read_parquet(profiles_file)
        
        # Sort by ID or timestamp if available, otherwise use row order
        if 'id' in df.columns:
            df_sorted = df.sort_values('id', ascending=False)
        elif 'cid' in df.columns:
            df_sorted = df.sort_values('cid', ascending=False)
        else:
            df_sorted = df.tail(limit * 2)  # Get more in case some are invalid
        
        # Take the latest entries
        latest_df = df_sorted.head(limit)
        
        # Convert to list of dictionaries for JSON response
        results = []
        for _, row in latest_df.iterrows():
            result = {
                'cid': row.get('cid', ''),
                'name': row.get('name', ''),
                'mbti': row.get('mbti', ''),
                'socionics': row.get('socionics', ''),
                'description': row.get('description', '')[:500] if row.get('description') else '',  # Truncate long descriptions
                'source': row.get('source', 'unknown'),
                'category': row.get('category', ''),
                'subcategory': row.get('subcategory', ''),
                'votes': row.get('votes', 0),
            }
            
            # Add any other relevant fields
            for col in df.columns:
                if col not in result and col not in ['payload_bytes', 'vector']:  # Skip binary/large fields
                    value = row.get(col)
                    if pd.notna(value):
                        result[col] = value
            
            results.append(result)
        
        print(json.dumps(results, indent=2, default=str))
        
    except Exception as e:
        print(json.dumps({"error": f"Failed to load latest results: {str(e)}"}))
        sys.exit(1)

if __name__ == "__main__":
    main()