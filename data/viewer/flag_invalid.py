#!/usr/bin/env python3
"""
Flag invalid entries script for personality database viewer.
Identifies and flags potentially invalid or corrupted profile entries.
"""

import sys
import json
import os
import pandas as pd
import re

def is_valid_mbti(mbti_str):
    """Check if MBTI string is valid."""
    if not mbti_str or not isinstance(mbti_str, str):
        return False
    
    mbti_pattern = r'^(E|I)(N|S)(F|T)(J|P)$'
    return bool(re.match(mbti_pattern, mbti_str.upper().strip()))

def is_valid_socionics(socionics_str):
    """Check if Socionics string looks valid."""
    if not socionics_str or not isinstance(socionics_str, str):
        return False
    
    # Common Socionics patterns
    socionics_patterns = [
        r'^(LII|LSI|SLE|ILE|ESE|LSE|EIE|LIE|SEI|IEI|EII|ILI|SLI|ESI|LSI|ILE)$',  # 3-letter codes
        r'^(INTj|INTp|ENTp|ENTj|ESFp|ESFj|ESFp|ENFj|ISFp|INFp|INFj|INFp|ISTp|ESTj|ESTp|ENTj)$',  # Alternative format
    ]
    
    clean_str = socionics_str.strip().replace('-', '').replace(' ', '')
    return any(re.match(pattern, clean_str, re.IGNORECASE) for pattern in socionics_patterns)

def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Dataset directory required"}))
        sys.exit(1)
        
    dataset_dir = sys.argv[1]
    profiles_file = os.path.join(dataset_dir, 'pdb_profiles.parquet')
    
    if not os.path.exists(profiles_file):
        print(json.dumps({"flaggedCount": 0, "error": "No profiles file found"}))
        return
    
    try:
        df = pd.read_parquet(profiles_file)
        flagged_count = 0
        
        # Create flags column if it doesn't exist
        if 'flags' not in df.columns:
            df['flags'] = ''
        
        for idx, row in df.iterrows():
            flags = []
            
            # Check for missing required fields
            if pd.isna(row.get('name')) or not str(row.get('name', '')).strip():
                flags.append('missing_name')
            
            # Check MBTI validity
            mbti = row.get('mbti')
            if mbti and not is_valid_mbti(mbti):
                flags.append('invalid_mbti')
            
            # Check Socionics validity
            socionics = row.get('socionics')
            if socionics and not is_valid_socionics(socionics):
                flags.append('invalid_socionics')
            
            # Check for suspiciously short descriptions
            description = row.get('description', '')
            if description and len(str(description).strip()) < 10:
                flags.append('short_description')
            
            # Check for placeholder or test data
            name = str(row.get('name', '')).lower()
            if any(word in name for word in ['test', 'placeholder', 'example', 'dummy']):
                flags.append('test_data')
            
            # Update flags if any issues found
            if flags:
                df.at[idx, 'flags'] = ','.join(flags)
                flagged_count += 1
        
        # Save updated dataframe with flags
        if flagged_count > 0:
            backup_file = profiles_file + '.backup_flagged_' + str(int(pd.Timestamp.now().timestamp()))
            # Create backup before modifying
            pd.read_parquet(profiles_file).to_parquet(backup_file, index=False)
            
            # Save with flags
            df.to_parquet(profiles_file, index=False)
        
        result = {
            "success": True,
            "flaggedCount": flagged_count,
            "totalRecords": len(df),
            "flagTypes": {
                "missing_name": (df['flags'].str.contains('missing_name', na=False)).sum(),
                "invalid_mbti": (df['flags'].str.contains('invalid_mbti', na=False)).sum(),
                "invalid_socionics": (df['flags'].str.contains('invalid_socionics', na=False)).sum(),
                "short_description": (df['flags'].str.contains('short_description', na=False)).sum(),
                "test_data": (df['flags'].str.contains('test_data', na=False)).sum()
            },
            "message": f"Flagged {flagged_count} potentially invalid entries"
        }
        
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        print(json.dumps({
            "success": False,
            "flaggedCount": 0,
            "error": f"Flagging failed: {str(e)}"
        }))
        sys.exit(1)

if __name__ == "__main__":
    main()