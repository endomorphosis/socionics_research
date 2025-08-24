#!/usr/bin/env python3
"""
Export feedback report script for personality database viewer.
Creates a CSV report of data quality issues and feedback.
"""

import sys
import json
import os
import pandas as pd
import csv

def main():
    if len(sys.argv) < 3:
        print(json.dumps({"error": "Usage: export_feedback.py <dataset_dir> <output_file>"}))
        sys.exit(1)
        
    dataset_dir = sys.argv[1]
    output_file = sys.argv[2]
    
    profiles_file = os.path.join(dataset_dir, 'pdb_profiles.parquet')
    
    if not os.path.exists(profiles_file):
        # Create empty report
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['error', 'No profiles file found'])
        return
    
    try:
        df = pd.read_parquet(profiles_file)
        
        # Prepare feedback report data
        report_data = []
        
        for idx, row in df.iterrows():
            record = {
                'ID': row.get('cid', idx),
                'Name': row.get('name', ''),
                'MBTI': row.get('mbti', ''),
                'Socionics': row.get('socionics', ''),
                'Category': row.get('category', ''),
                'Subcategory': row.get('subcategory', ''),
                'Votes': row.get('votes', 0),
                'Quality_Score': row.get('quality_score', 'N/A'),
                'Flags': row.get('flags', ''),
                'Description_Length': len(str(row.get('description', ''))),
                'Has_Description': 'Yes' if row.get('description') else 'No',
                'Source': row.get('source', 'Unknown'),
                'Issues': []
            }
            
            # Identify issues for feedback
            issues = []
            
            if not record['Name']:
                issues.append('Missing Name')
            
            if not record['MBTI']:
                issues.append('Missing MBTI')
            elif len(str(record['MBTI'])) != 4:
                issues.append('Invalid MBTI Format')
            
            if not record['Socionics']:
                issues.append('Missing Socionics')
            
            if record['Description_Length'] < 20:
                issues.append('Very Short Description')
            elif record['Description_Length'] < 100:
                issues.append('Short Description')
            
            if record['Votes'] == 0:
                issues.append('No Community Votes')
            
            if 'test' in str(record['Name']).lower():
                issues.append('Possible Test Data')
            
            record['Issues'] = '; '.join(issues) if issues else 'None'
            record['Issue_Count'] = len(issues)
            record['Needs_Review'] = 'Yes' if issues else 'No'
            
            report_data.append(record)
        
        # Create DataFrame and sort by issue count (most problematic first)
        report_df = pd.DataFrame(report_data)
        report_df = report_df.sort_values('Issue_Count', ascending=False)
        
        # Export to CSV
        report_df.to_csv(output_file, index=False, encoding='utf-8')
        
        print(f"Feedback report exported to {output_file}")
        print(f"Total records: {len(report_df)}")
        print(f"Records needing review: {(report_df['Needs_Review'] == 'Yes').sum()}")
        
    except Exception as e:
        # Create error report
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Error', f'Failed to export feedback report: {str(e)}'])
        
        print(json.dumps({
            "success": False,
            "error": f"Export failed: {str(e)}"
        }))
        sys.exit(1)

if __name__ == "__main__":
    main()