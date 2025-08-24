#!/usr/bin/env python3
"""
Export unified dataset script for personality database viewer.
Creates a single, clean dataset with profiles, vectors, and metadata.
"""

import sys
import json
import os
import pandas as pd
from pathlib import Path

def main():
    if len(sys.argv) < 3:
        print(json.dumps({"error": "Usage: export_unified.py <dataset_dir> <output_filename>"}))
        sys.exit(1)
        
    dataset_dir = sys.argv[1]
    output_filename = sys.argv[2]
    
    # Files to merge
    profiles_file = os.path.join(dataset_dir, 'pdb_profiles.parquet')
    vectors_file = os.path.join(dataset_dir, 'pdb_profile_vectors.parquet')
    normalized_file = os.path.join(dataset_dir, 'pdb_profiles_normalized.parquet')
    
    # Try consolidated files first
    consolidated_profiles = os.path.join(dataset_dir, 'pdb_profiles_consolidated.parquet')
    consolidated_vectors = os.path.join(dataset_dir, 'pdb_vectors_consolidated.parquet')
    
    if os.path.exists(consolidated_profiles):
        profiles_file = consolidated_profiles
    if os.path.exists(consolidated_vectors):
        vectors_file = consolidated_vectors
    
    try:
        # Load profiles
        if os.path.exists(profiles_file):
            profiles_df = pd.read_parquet(profiles_file)
            print(f"Loaded {len(profiles_df)} profiles", file=sys.stderr)
        else:
            print("No profiles file found", file=sys.stderr)
            print(json.dumps({"success": False, "error": "No profiles data available"}))
            sys.exit(1)
        
        # Load vectors if available
        vectors_df = None
        if os.path.exists(vectors_file):
            vectors_df = pd.read_parquet(vectors_file)
            print(f"Loaded {len(vectors_df)} vectors", file=sys.stderr)
        
        # Merge data
        if vectors_df is not None and 'cid' in profiles_df.columns and 'cid' in vectors_df.columns:
            # Merge profiles with vectors
            unified_df = profiles_df.merge(vectors_df, on='cid', how='left', suffixes=('', '_vector'))
            print(f"Merged profiles with vectors: {len(unified_df)} records", file=sys.stderr)
        else:
            unified_df = profiles_df
            print("Using profiles only (no vector data to merge)", file=sys.stderr)
        
        # Clean up the unified dataset
        # Remove rows with no meaningful data
        if 'name' in unified_df.columns:
            unified_df = unified_df.dropna(subset=['name'])
        
        # Remove duplicates if cid exists
        if 'cid' in unified_df.columns:
            before_count = len(unified_df)
            unified_df = unified_df.drop_duplicates(subset=['cid'], keep='first')
            after_count = len(unified_df)
            if before_count != after_count:
                print(f"Removed {before_count - after_count} duplicate records", file=sys.stderr)
        
        # Add metadata columns
        unified_df['export_timestamp'] = pd.Timestamp.now()
        unified_df['source_files'] = 'profiles'
        if vectors_df is not None:
            unified_df.loc[unified_df['vector'].notna(), 'source_files'] += ',vectors'
        
        # Create exports directory
        exports_dir = os.path.join(dataset_dir, 'exports')
        os.makedirs(exports_dir, exist_ok=True)
        
        # Save unified dataset
        output_path = os.path.join(exports_dir, output_filename)
        unified_df.to_parquet(output_path, index=False)
        
        result = {
            "success": True,
            "filename": output_filename,
            "output_path": output_path,
            "total_records": len(unified_df),
            "columns": list(unified_df.columns),
            "has_vectors": 'vector' in unified_df.columns and unified_df['vector'].notna().any(),
            "message": f"Unified dataset with {len(unified_df)} records exported to {output_filename}"
        }
        
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        print(json.dumps({
            "success": False,
            "error": f"Export failed: {str(e)}"
        }))
        sys.exit(1)

if __name__ == "__main__":
    main()