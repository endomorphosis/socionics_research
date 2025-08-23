#!/usr/bin/env python3
"""
Utility to clean up parquet databases by removing duplicates and empty rows.

This script provides functions to:
1. Remove duplicate rows based on key columns
2. Remove rows with missing essential data
3. Remove completely empty rows
4. Backup original files before cleanup
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import List, Optional, Tuple
import pandas as pd


def backup_file(file_path: Path) -> Path:
    """Create a backup of the original file before cleanup."""
    backup_path = file_path.with_suffix(file_path.suffix + '.backup')
    shutil.copy2(file_path, backup_path)
    print(f"Created backup: {backup_path}")
    return backup_path


def clean_profiles_normalized(file_path: Path) -> Tuple[int, int, int]:
    """
    Clean the normalized profiles parquet file.
    
    Returns:
        Tuple of (rows_removed_missing_pid, rows_removed_empty_profile, total_removed)
    """
    if not file_path.exists():
        print(f"File does not exist: {file_path}")
        return 0, 0, 0
    
    df = pd.read_parquet(file_path)
    original_count = len(df)
    print(f"Original rows: {original_count}")
    
    # Track removed rows
    removed_missing_pid = 0
    removed_empty_profile = 0
    
    # Remove rows with missing PIDs
    missing_pid_mask = df['pid'].isnull()
    removed_missing_pid = missing_pid_mask.sum()
    if removed_missing_pid > 0:
        print(f"Removing {removed_missing_pid} rows with missing PIDs")
        df = df[~missing_pid_mask]
    
    # Remove rows where all essential profile fields are empty/null
    # Essential fields: name, mbti, socionics, big5 (at least one should have data)
    essential_fields = ['name', 'mbti', 'socionics', 'big5']
    
    def is_empty_or_null(value) -> bool:
        if pd.isna(value):
            return True
        if isinstance(value, str) and not value.strip():
            return True
        return False
    
    # Create mask for rows where ALL essential fields are empty
    empty_profile_mask = df[essential_fields].apply(
        lambda row: all(is_empty_or_null(val) for val in row), 
        axis=1
    )
    
    removed_empty_profile = empty_profile_mask.sum()
    if removed_empty_profile > 0:
        print(f"Removing {removed_empty_profile} rows with all essential fields empty")
        df = df[~empty_profile_mask]
    
    total_removed = removed_missing_pid + removed_empty_profile
    final_count = len(df)
    
    if total_removed > 0:
        print(f"Final rows: {final_count} (removed {total_removed} total)")
        
        # Create backup first
        backup_file(file_path)
        
        # Save cleaned data
        df.to_parquet(file_path, index=False)
        print(f"Cleaned file saved: {file_path}")
    else:
        print("No rows needed cleaning")
    
    return removed_missing_pid, removed_empty_profile, total_removed


def check_and_remove_duplicates(file_path: Path, key_columns: List[str]) -> int:
    """
    Check for and remove duplicate rows based on key columns.
    
    Args:
        file_path: Path to parquet file
        key_columns: List of column names to use for duplicate detection
        
    Returns:
        Number of duplicate rows removed
    """
    if not file_path.exists():
        print(f"File does not exist: {file_path}")
        return 0
    
    df = pd.read_parquet(file_path)
    original_count = len(df)
    
    # Check for duplicates based on key columns
    duplicates = df.duplicated(subset=key_columns)
    duplicate_count = duplicates.sum()
    
    if duplicate_count > 0:
        print(f"Found {duplicate_count} duplicate rows in {file_path}")
        
        # Create backup first
        backup_file(file_path)
        
        # Remove duplicates
        df_cleaned = df[~duplicates]
        df_cleaned.to_parquet(file_path, index=False)
        
        print(f"Removed {duplicate_count} duplicates, {len(df_cleaned)} rows remaining")
        return duplicate_count
    else:
        print(f"No duplicates found in {file_path}")
        return 0


def remove_empty_rows(file_path: Path) -> int:
    """
    Remove completely empty rows (all NaN values).
    
    Returns:
        Number of empty rows removed
    """
    if not file_path.exists():
        print(f"File does not exist: {file_path}")
        return 0
    
    df = pd.read_parquet(file_path)
    original_count = len(df)
    
    # Find rows that are completely empty (all NaN)
    empty_rows = df.isnull().all(axis=1)
    empty_count = empty_rows.sum()
    
    if empty_count > 0:
        print(f"Found {empty_count} completely empty rows in {file_path}")
        
        # Create backup first  
        backup_file(file_path)
        
        # Remove empty rows
        df_cleaned = df[~empty_rows]
        df_cleaned.to_parquet(file_path, index=False)
        
        print(f"Removed {empty_count} empty rows, {len(df_cleaned)} rows remaining")
        return empty_count
    else:
        print(f"No empty rows found in {file_path}")
        return 0


def cleanup_all_parquet_files(data_dir: Optional[Path] = None) -> dict:
    """
    Clean up all parquet files in the data directory.
    
    Returns:
        Dictionary with cleanup results for each file
    """
    if data_dir is None:
        from .pdb_storage import _ensure_dir
        data_dir = _ensure_dir()
    
    results = {}
    
    # Define files and their cleanup strategies
    file_configs = [
        {
            'path': data_dir / 'pdb_profiles.parquet',
            'key_columns': ['cid'],
            'name': 'profiles'
        },
        {
            'path': data_dir / 'pdb_profile_vectors.parquet', 
            'key_columns': ['cid'],
            'name': 'vectors'
        },
        {
            'path': data_dir / 'pdb_profiles_normalized.parquet',
            'key_columns': ['cid'],
            'name': 'normalized',
            'special_cleanup': True  # This file needs special handling
        }
    ]
    
    for config in file_configs:
        file_path = config['path']
        file_name = config['name']
        
        print(f"\n=== Cleaning {file_name} ({file_path}) ===")
        
        if not file_path.exists():
            print(f"File does not exist, skipping: {file_path}")
            results[file_name] = {'error': 'File not found'}
            continue
            
        try:
            file_results = {
                'duplicates_removed': 0,
                'empty_rows_removed': 0,
                'special_cleanup': {}
            }
            
            # Check for duplicates
            file_results['duplicates_removed'] = check_and_remove_duplicates(
                file_path, config['key_columns']
            )
            
            # Check for empty rows
            file_results['empty_rows_removed'] = remove_empty_rows(file_path)
            
            # Special cleanup for normalized profiles
            if config.get('special_cleanup'):
                missing_pid, empty_profile, total = clean_profiles_normalized(file_path)
                file_results['special_cleanup'] = {
                    'missing_pid_removed': missing_pid,
                    'empty_profile_removed': empty_profile, 
                    'total_special_removed': total
                }
            
            results[file_name] = file_results
            
        except Exception as e:
            print(f"Error cleaning {file_name}: {e}")
            results[file_name] = {'error': str(e)}
    
    return results


def main():
    """Main function to run cleanup on all parquet files."""
    print("Starting parquet database cleanup...")
    
    results = cleanup_all_parquet_files()
    
    print("\n=== CLEANUP SUMMARY ===")
    total_duplicates = 0
    total_empty = 0
    total_special = 0
    
    for file_name, file_results in results.items():
        print(f"\n{file_name}:")
        
        if 'error' in file_results:
            print(f"  Error: {file_results['error']}")
            continue
            
        duplicates = file_results.get('duplicates_removed', 0)
        empty = file_results.get('empty_rows_removed', 0) 
        special = file_results.get('special_cleanup', {})
        
        print(f"  Duplicates removed: {duplicates}")
        print(f"  Empty rows removed: {empty}")
        
        if special:
            special_total = special.get('total_special_removed', 0)
            print(f"  Missing PIDs removed: {special.get('missing_pid_removed', 0)}")
            print(f"  Empty profiles removed: {special.get('empty_profile_removed', 0)}")
            print(f"  Total special cleanup: {special_total}")
            total_special += special_total
            
        total_duplicates += duplicates
        total_empty += empty
    
    print(f"\nOVERALL TOTALS:")
    print(f"  Total duplicates removed: {total_duplicates}")
    print(f"  Total empty rows removed: {total_empty}")  
    print(f"  Total special cleanup: {total_special}")
    print(f"  Grand total rows removed: {total_duplicates + total_empty + total_special}")


if __name__ == '__main__':
    main()