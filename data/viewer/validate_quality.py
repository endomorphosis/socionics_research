#!/usr/bin/env python3
"""
Validate quality script for personality database viewer.
Performs comprehensive quality validation on profile data.
"""

import sys
import json
import os
import pandas as pd
import re

def validate_profile(row):
    """Validate a single profile record and return quality score."""
    score = 0
    max_score = 10
    issues = []
    
    # Name validation (2 points)
    name = row.get('name', '')
    if name and str(name).strip():
        if len(str(name).strip()) > 2:
            score += 2
        else:
            score += 1
            issues.append('short_name')
    else:
        issues.append('missing_name')
    
    # MBTI validation (2 points)
    mbti = row.get('mbti', '')
    if mbti:
        mbti_pattern = r'^(E|I)(N|S)(F|T)(J|P)$'
        if re.match(mbti_pattern, str(mbti).upper().strip()):
            score += 2
        else:
            score += 1
            issues.append('invalid_mbti_format')
    else:
        issues.append('missing_mbti')
    
    # Description validation (2 points)
    description = row.get('description', '')
    if description and str(description).strip():
        desc_len = len(str(description).strip())
        if desc_len > 100:
            score += 2
        elif desc_len > 20:
            score += 1
            issues.append('short_description')
        else:
            issues.append('very_short_description')
    else:
        issues.append('missing_description')
    
    # Category/Source validation (2 points)
    category = row.get('category', '')
    subcategory = row.get('subcategory', '')
    if category and str(category).strip():
        score += 1
    if subcategory and str(subcategory).strip():
        score += 1
    if not category and not subcategory:
        issues.append('missing_category')
    
    # Data consistency (1 point)
    if row.get('votes', 0) and pd.notna(row.get('votes')) and row.get('votes') > 0:
        score += 1
    elif row.get('votes', 0) == 0:
        issues.append('no_votes')
    
    # Uniqueness check (1 point) - this would need to be done at dataset level
    score += 1  # Assume unique for now
    
    return score / max_score * 100, issues

def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Dataset directory required"}))
        sys.exit(1)
        
    dataset_dir = sys.argv[1]
    profiles_file = os.path.join(dataset_dir, 'pdb_profiles.parquet')
    
    if not os.path.exists(profiles_file):
        print(json.dumps({"validCount": 0, "invalidCount": 0, "error": "No profiles file found"}))
        return
    
    try:
        df = pd.read_parquet(profiles_file)
        
        quality_scores = []
        all_issues = []
        
        for idx, row in df.iterrows():
            score, issues = validate_profile(row)
            quality_scores.append(score)
            all_issues.extend(issues)
        
        # Add quality scores to dataframe
        df['quality_score'] = quality_scores
        
        # Calculate statistics
        valid_count = sum(1 for score in quality_scores if score >= 70)  # 70% threshold
        invalid_count = len(quality_scores) - valid_count
        avg_score = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        
        # Count issue types
        issue_counts = {}
        for issue in set(all_issues):
            issue_counts[issue] = all_issues.count(issue)
        
        # Save updated dataframe with quality scores
        backup_file = profiles_file + '.backup_quality_' + str(int(pd.Timestamp.now().timestamp()))
        pd.read_parquet(profiles_file).to_parquet(backup_file, index=False)
        df.to_parquet(profiles_file, index=False)
        
        result = {
            "success": True,
            "validCount": valid_count,
            "invalidCount": invalid_count,
            "totalRecords": len(df),
            "averageScore": round(avg_score, 2),
            "qualityThreshold": 70,
            "issueTypes": issue_counts,
            "scoreDistribution": {
                "excellent": sum(1 for score in quality_scores if score >= 90),
                "good": sum(1 for score in quality_scores if 70 <= score < 90),
                "fair": sum(1 for score in quality_scores if 50 <= score < 70),
                "poor": sum(1 for score in quality_scores if score < 50)
            },
            "message": f"Validated {len(df)} records: {valid_count} valid, {invalid_count} need improvement"
        }
        
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        print(json.dumps({
            "success": False,
            "validCount": 0,
            "invalidCount": 0,
            "error": f"Validation failed: {str(e)}"
        }))
        sys.exit(1)

if __name__ == "__main__":
    main()