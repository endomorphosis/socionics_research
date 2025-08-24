#!/usr/bin/env python3
"""
Cleanup duplicates script for personality database viewer.
Removes duplicate entries across all parquet files.
"""

import sys
import json
import os
import pandas as pd

def cleanup_duplicates_in_file(filepath, key_columns=None):
    """Remove duplicates from a single parquet file."""
    if not os.path.exists(filepath):
        return 0, 0
        
    try:
        df = pd.read_parquet(filepath)
        original_count = len(df)
        
        if key_columns:
            # Use specified columns for duplicate detection
            available_cols = [col for col in key_columns if col in df.columns]
            if available_cols:
                df_clean = df.drop_duplicates(subset=available_cols, keep='first')
            else:
                df_clean = df.drop_duplicates(keep='first')
        else:
            # Default duplicate removal
            df_clean = df.drop_duplicates(keep='first')
        
        cleaned_count = len(df_clean)
        removed_count = original_count - cleaned_count
        
        if removed_count > 0:
            # Backup original file
            backup_file = filepath + '.backup_' + str(int(pd.Timestamp.now().timestamp()))
            df.to_parquet(backup_file, index=False)
            
            # Save cleaned version
            df_clean.to_parquet(filepath, index=False)
            print(f"Cleaned {filepath}: removed {removed_count} duplicates", file=sys.stderr)
        
        return original_count, removed_count
        
    except Exception as e:
        print(f"Error cleaning {filepath}: {e}", file=sys.stderr)
        return 0, 0

def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Dataset directory required"}))
        sys.exit(1)
        
    dataset_dir = sys.argv[1]
    
    # Files to clean and their key columns
    files_to_clean = [
        ('pdb_profiles.parquet', ['cid']),
        ('pdb_profile_vectors.parquet', ['cid']),
        ('pdb_profiles_normalized.parquet', ['cid']),
        ('pdb_profiles_consolidated.parquet', ['cid']),
        ('pdb_vectors_consolidated.parquet', ['cid']),
        ('pdb_cache_consolidated.parquet', ['cache_key'])
    ]
    
    total_removed = 0
    total_original = 0
    cleaned_files = []
    
    for filename, key_cols in files_to_clean:
        filepath = os.path.join(dataset_dir, filename)
        original, removed = cleanup_duplicates_in_file(filepath, key_cols)
        
        if original > 0:
            total_original += original
            total_removed += removed
            cleaned_files.append({
                'file': filename,
                'original_count': original,
                'removed_count': removed,
                'final_count': original - removed
            })
    
    result = {
        "success": True,
        "removedCount": total_removed,
        "originalCount": total_original,
        "finalCount": total_original - total_removed,
        "cleanedFiles": cleaned_files,
        "message": f"Removed {total_removed} duplicate entries across {len(cleaned_files)} files"
    }
    
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()