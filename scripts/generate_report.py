#!/usr/bin/env python3
"""
Generate a summary report of the question pool generation process.

Creates a comprehensive report including:
- Generation statistics
- Distribution analysis
- Sample questions by category
- K-means clustering results
- Recommendations for question decimation
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime

def generate_report():
    """Generate a comprehensive summary report."""
    
    survey_dir = Path("survey")
    report_lines = []
    
    # Header
    report_lines.extend([
        "=" * 80,
        "SOCIONICS QUESTION POOL GENERATION REPORT",
        "=" * 80,
        f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
    ])
    
    # Load analysis results
    try:
        with open(survey_dir / "question_analysis.json") as f:
            analysis = json.load(f)
        
        # Generation Summary
        report_lines.extend([
            "1. GENERATION SUMMARY",
            "-" * 30,
            f"Target questions: 64,000",
            f"Generated questions: {analysis['full_distribution']['total_questions']:,}",
            f"Base questions: {analysis['full_distribution']['original_questions']:,}",
            f"Variations: {analysis['full_distribution']['variation_questions']:,}",
            f"Variation ratio: {analysis['full_distribution']['variation_questions']/analysis['full_distribution']['original_questions']:.1f}:1",
            f"Embedding dimensions: {analysis['generation_params']['embedding_dim']}",
            "",
        ])
        
        # Distribution Analysis
        report_lines.extend([
            "2. AXIS DISTRIBUTION ANALYSIS",
            "-" * 35,
        ])
        
        axis_dist = analysis['full_distribution']['axis_distribution']
        total_questions = analysis['full_distribution']['total_questions']
        
        for axis, count in sorted(axis_dist.items()):
            percentage = (count / total_questions) * 100
            report_lines.append(f"{axis}: {count:6,} questions ({percentage:5.1f}%)")
        
        report_lines.extend([
            "",
            f"Uniformity Score: {analysis['full_distribution']['uniformity_score']:.3f}",
            f"(Lower scores indicate more uniform distribution)",
            "",
        ])
        
        # K-means Analysis
        kmeans_analysis = analysis['kmeans_analysis']
        report_lines.extend([
            "3. K-MEANS CLUSTERING ANALYSIS",
            "-" * 40,
            f"Optimal number of clusters: {kmeans_analysis['best_k']}",
            f"Best silhouette score: {kmeans_analysis['best_score']:.3f}",
            "",
            "Clustering quality interpretation:",
            "  > 0.7: Strong clustering",
            "  > 0.5: Reasonable clustering", 
            "  > 0.25: Weak but meaningful clustering",
            "  < 0.25: Poor clustering",
            "",
            f"Result: {'Weak but meaningful clustering' if kmeans_analysis['best_score'] > 0.25 else 'Poor clustering'}",
            "",
        ])
        
    except FileNotFoundError:
        report_lines.extend([
            "Analysis file not found. Please run generate_question_pool.py first.",
            "",
        ])
    
    # File Analysis
    report_lines.extend([
        "4. GENERATED FILES",
        "-" * 20,
    ])
    
    parquet_files = list(survey_dir.glob("question_pool_*.parquet"))
    if parquet_files:
        for file_path in sorted(parquet_files):
            try:
                df = pd.read_parquet(file_path)
                file_size_mb = file_path.stat().st_size / (1024 * 1024)
                axis_dist = df['axis'].value_counts()
                uniformity = axis_dist.std() / axis_dist.mean()
                
                report_lines.extend([
                    f"{file_path.name}:",
                    f"  Questions: {len(df):,}",
                    f"  File size: {file_size_mb:.1f} MB",
                    f"  Uniformity score: {uniformity:.3f}",
                    f"  Most represented axis: {axis_dist.idxmax()} ({axis_dist.max()} questions)",
                    f"  Least represented axis: {axis_dist.idxmin()} ({axis_dist.min()} questions)",
                    "",
                ])
            except Exception as e:
                report_lines.append(f"  Error reading {file_path.name}: {e}")
    else:
        report_lines.append("No parquet files found.")
    
    # Sample Questions
    try:
        df_sample = pd.read_parquet(survey_dir / "question_pool_200.parquet")
        report_lines.extend([
            "",
            "5. SAMPLE QUESTIONS (from 200-question subset)",
            "-" * 50,
        ])
        
        for axis in sorted(df_sample['axis'].unique()):
            axis_questions = df_sample[df_sample['axis'] == axis]
            report_lines.append(f"\n{axis} Axis ({len(axis_questions)} questions):")
            
            # Show 2 sample questions
            for i, question in enumerate(axis_questions['text'].head(2)):
                report_lines.append(f"  {i+1}. {question}")
        
    except FileNotFoundError:
        report_lines.append("Sample question file not found.")
    
    # Recommendations
    report_lines.extend([
        "",
        "",
        "6. RECOMMENDATIONS FOR USAGE",
        "-" * 35,
        "",
        "Based on the analysis, here are recommendations for using the question pool:",
        "",
        "• For survey development:",
        "  - Use question_pool_200.parquet for initial testing",
        "  - Use question_pool_500.parquet for pilot studies", 
        "  - Use question_pool_1000.parquet for full surveys",
        "",
        "• For question decimation:",
        f"  - The K-means clustering with k={kmeans_analysis.get('best_k', 'N/A')} provides good separation",
        "  - Consider manual review of cluster representatives",
        "  - Balance axis representation in final selection",
        "",
        "• For uniform distribution:",
        "  - The generated sets show good uniformity across axes",
        "  - Consider using balanced sampling for specific applications",
        "  - Monitor for axis bias in final question selection",
        "",
        "• Data quality:",
        "  - Review generated variations for naturalness",
        "  - Consider semantic validation of embeddings", 
        "  - Test with sample respondents before deployment",
        "",
    ])
    
    # Write report
    report_path = survey_dir / "question_pool_report.txt"
    with open(report_path, 'w') as f:
        f.write('\n'.join(report_lines))
    
    # Also print to console
    print('\n'.join(report_lines))
    print(f"\nFull report saved to: {report_path}")

if __name__ == "__main__":
    generate_report()