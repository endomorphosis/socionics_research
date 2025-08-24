#!/usr/bin/env python3
"""
Data consolidation script for personality database viewer.
Consolidates multiple parquet files into clean, unified datasets.
"""

import sys
import json
import os
from pathlib import Path
import pandas as pd
import time

def consolidate_profiles(dataset_dir):
    """Consolidate all profile-related parquet files."""
    profiles_files = [
        'pdb_profiles.parquet',
        'pdb_profiles_normalized.parquet'
    ]
    
    consolidated_profiles = []
    
    for filename in profiles_files:
        filepath = os.path.join(dataset_dir, filename)
        if os.path.exists(filepath):
            try:
                df = pd.read_parquet(filepath)
                print(f"Loaded {len(df)} profiles from {filename}", file=sys.stderr)
                consolidated_profiles.append(df)
            except Exception as e:
                print(f"Error loading {filename}: {e}", file=sys.stderr)
    
    if not consolidated_profiles:
        return {"success": False, "message": "No profile files found"}
    
    # Combine all profile dataframes
    combined_df = pd.concat(consolidated_profiles, ignore_index=True)
    
    # Remove duplicates based on 'cid' if available
    if 'cid' in combined_df.columns:
        before_count = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=['cid'], keep='first')
        after_count = len(combined_df)
        duplicates_removed = before_count - after_count
        print(f"Removed {duplicates_removed} duplicate profiles", file=sys.stderr)
    
    # Save consolidated profiles
    output_file = os.path.join(dataset_dir, 'pdb_profiles_consolidated.parquet')
    combined_df.to_parquet(output_file, index=False)
    
    return {
        "success": True,
        "message": f"Consolidated {len(combined_df)} profiles",
        "output_file": output_file,
        "total_profiles": len(combined_df)
    }

def consolidate_vectors(dataset_dir):
    """Consolidate vector embedding files."""
    vectors_file = os.path.join(dataset_dir, 'pdb_profile_vectors.parquet')
    
    if not os.path.exists(vectors_file):
        return {"success": False, "message": "No vector file found"}
    
    try:
        df = pd.read_parquet(vectors_file)
        
        # Remove duplicates and invalid vectors
        if 'cid' in df.columns:
            before_count = len(df)
            df = df.drop_duplicates(subset=['cid'], keep='first')
            after_count = len(df)
            print(f"Removed {before_count - after_count} duplicate vectors", file=sys.stderr)
        
        # Remove rows with null vectors
        if 'vector' in df.columns:
            before_count = len(df)
            df = df.dropna(subset=['vector'])
            after_count = len(df)
            print(f"Removed {before_count - after_count} null vectors", file=sys.stderr)
        
        # Save consolidated vectors
        output_file = os.path.join(dataset_dir, 'pdb_vectors_consolidated.parquet')
        df.to_parquet(output_file, index=False)
        
        return {
            "success": True,
            "message": f"Consolidated {len(df)} vectors",
            "output_file": output_file,
            "total_vectors": len(df)
        }
        
    except Exception as e:
        return {"success": False, "message": f"Error consolidating vectors: {e}"}

def consolidate_cache(dataset_dir):
    """Consolidate API cache into a single parquet file."""
    cache_dir = os.path.join(dataset_dir, 'pdb_api_cache')
    
    if not os.path.exists(cache_dir):
        return {"success": False, "message": "No cache directory found"}
    
    cache_entries = []
    processed = 0
    
    for filename in os.listdir(cache_dir):
        if not filename.endswith('.json'):
            continue
            
        filepath = os.path.join(cache_dir, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Extract metadata from cache entry
            cache_entry = {
                'cache_key': filename.replace('.json', ''),
                'cached_at': os.path.getmtime(filepath),
                'file_size': os.path.getsize(filepath),
                'has_data': 'data' in data if isinstance(data, dict) else False,
                'response_type': type(data).__name__
            }
            
            # Add some content indicators if it's a dict
            if isinstance(data, dict):
                cache_entry['keys_count'] = len(data.keys())
                if 'profiles' in data:
                    cache_entry['profiles_count'] = len(data['profiles']) if isinstance(data['profiles'], list) else 0
                if 'results' in data:
                    cache_entry['results_count'] = len(data['results']) if isinstance(data['results'], list) else 0
            
            cache_entries.append(cache_entry)
            processed += 1
            
            if processed % 100 == 0:
                print(f"Processed {processed} cache files", file=sys.stderr)
                
        except Exception as e:
            print(f"Error processing {filename}: {e}", file=sys.stderr)
    
    if not cache_entries:
        return {"success": False, "message": "No valid cache entries found"}
    
    # Create DataFrame and save
    cache_df = pd.DataFrame(cache_entries)
    output_file = os.path.join(dataset_dir, 'pdb_cache_consolidated.parquet')
    cache_df.to_parquet(output_file, index=False)
    
    return {
        "success": True,
        "message": f"Consolidated {len(cache_entries)} cache entries",
        "output_file": output_file,
        "total_entries": len(cache_entries)
    }

def main():
    if len(sys.argv) < 3:
        print(json.dumps({"error": "Usage: consolidate_data.py <type> <dataset_dir>"}))
        sys.exit(1)
    
    consolidation_type = sys.argv[1]
    dataset_dir = sys.argv[2]
    
    print(f"Starting {consolidation_type} consolidation...", file=sys.stderr)
    
    if consolidation_type == 'profiles':
        result = consolidate_profiles(dataset_dir)
    elif consolidation_type == 'vectors':
        result = consolidate_vectors(dataset_dir)
    elif consolidation_type == 'cache':
        result = consolidate_cache(dataset_dir)
    else:
        result = {"success": False, "message": f"Unknown consolidation type: {consolidation_type}"}
    
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()