#!/usr/bin/env python3
"""
Utility script to analyze and explore the generated question pool.

Provides functionality to:
- Examine question distributions
- Find similar questions using embeddings
- Analyze K-means clustering results
- Export subsets for specific use cases
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import json
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import KMeans
import sys

def load_question_data(parquet_file: str) -> pd.DataFrame:
    """Load question data from parquet file."""
    return pd.read_parquet(parquet_file)

def analyze_similarity_removal(analysis_file: str = "survey/question_analysis.json", 
                             removal_file: str = "survey/similarity_removal_report.json"):
    """Analyze the results of similarity-based question removal."""
    print("=== Similarity Removal Analysis ===")
    
    try:
        # Load analysis results
        with open(analysis_file, 'r') as f:
            analysis = json.load(f)
        
        with open(removal_file, 'r') as f:
            removal_report = json.load(f)
        
        original_dist = analysis['original_distribution']
        filtered_dist = analysis['filtered_distribution'] 
        removal_stats = removal_report['removal_stats']
        
        print(f"\nRemoval Statistics:")
        print(f"  Original questions: {removal_stats['original_count']:,}")
        print(f"  Questions kept: {removal_stats['kept_count']:,}")
        print(f"  Questions removed: {removal_stats['removed_count']:,}")
        print(f"  Removal rate: {removal_stats['removal_rate']:.1%}")
        
        print(f"\nClustering Details:")
        print(f"  Clusters analyzed: {removal_stats['clusters_analyzed']}")
        print(f"  Average cluster size: {removal_stats['avg_cluster_size']:.1f}")
        print(f"  Similarity threshold: {removal_stats['similarity_threshold']}")
        print(f"  Min cluster size for removal: {removal_stats['min_cluster_size']}")
        
        print(f"\nDistribution Impact:")
        print(f"  Uniformity score: {original_dist['uniformity_score']:.3f} -> {filtered_dist['uniformity_score']:.3f}")
        
        print(f"\nAxis Distribution Changes:")
        for axis in original_dist['axis_distribution']:
            orig_count = original_dist['axis_distribution'][axis]
            filtered_count = filtered_dist['axis_distribution'][axis]
            change_pct = (filtered_count - orig_count) / orig_count * 100
            print(f"  {axis}: {orig_count:,} -> {filtered_count:,} ({change_pct:+.1f}%)")
        
        print(f"\nSample Removed Questions:")
        for i, q in enumerate(removal_report['sample_removed_questions'][:10]):
            reason = q['removal_reason']
            print(f"  {i+1}. [{q['axis']}] {q['text']}")
            if reason:
                print(f"     Reason: Similar to question {reason.get('representative_id', 'N/A')} in cluster {reason.get('cluster_id', 'N/A')}")
        
        # Cluster analysis summary
        cluster_analysis = removal_report['cluster_analysis']
        actions = {}
        for cluster_info in cluster_analysis.values():
            action = cluster_info['action']
            actions[action] = actions.get(action, 0) + 1
        
        print(f"\nCluster Actions:")
        for action, count in actions.items():
            print(f"  {action.replace('_', ' ').title()}: {count} clusters")
            
    except FileNotFoundError as e:
        print(f"Analysis files not found: {e}")
        print("Run generate_question_pool.py first to create similarity analysis files")


def find_similar_questions(df: pd.DataFrame, query_text: str, n_similar: int = 10):
    """Find questions most similar to a given query text."""
    # Convert embeddings to numpy array
    embeddings = np.array(df['embedding'].tolist())
    
    # For simplicity, find the question that best matches the query text
    # In a real scenario, you'd embed the query_text using the same model
    query_similarities = []
    for idx, row in df.iterrows():
        # Simple text similarity (could be improved with actual embedding)
        text_sim = len(set(query_text.lower().split()) & set(row['text'].lower().split()))
        query_similarities.append((idx, text_sim, row['text']))
    
    # Sort by similarity
    query_similarities.sort(key=lambda x: x[1], reverse=True)
    
    return [(idx, text) for idx, sim, text in query_similarities[:n_similar]]

def analyze_embedding_clusters(df: pd.DataFrame, n_clusters: int = 10):
    """Analyze embeddings using K-means clustering."""
    embeddings = np.array(df['embedding'].tolist())
    
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    cluster_labels = kmeans.fit_predict(embeddings)
    
    # Add cluster labels to dataframe
    df_clustered = df.copy()
    df_clustered['cluster'] = cluster_labels
    
    print(f"\nClustering Analysis (k={n_clusters}):")
    print("=" * 50)
    
    for cluster_id in range(n_clusters):
        cluster_questions = df_clustered[df_clustered['cluster'] == cluster_id]
        print(f"\nCluster {cluster_id} ({len(cluster_questions)} questions):")
        
        # Show axis distribution in this cluster
        axis_dist = cluster_questions['axis'].value_counts()
        print(f"  Axes: {dict(axis_dist)}")
        
        # Show sample questions
        sample_questions = cluster_questions['text'].head(3).tolist()
        for i, q in enumerate(sample_questions):
            print(f"  {i+1}. {q[:70]}{'...' if len(q) > 70 else ''}")
    
    return df_clustered

def export_balanced_subset(df: pd.DataFrame, questions_per_axis: int, output_file: str):
    """Export a balanced subset with equal questions per axis."""
    balanced_questions = []
    
    for axis in df['axis'].unique():
        axis_questions = df[df['axis'] == axis]
        
        # Sample questions from this axis
        n_to_sample = min(questions_per_axis, len(axis_questions))
        sampled = axis_questions.sample(n=n_to_sample, random_state=42)
        balanced_questions.append(sampled)
    
    balanced_df = pd.concat(balanced_questions, ignore_index=True)
    balanced_df.to_parquet(output_file, engine='pyarrow')
    print(f"Exported {len(balanced_df)} balanced questions to {output_file}")
    return balanced_df

def show_distribution_stats(df: pd.DataFrame):
    """Show detailed distribution statistics."""
    print("\nDistribution Analysis:")
    print("=" * 50)
    
    # Axis distribution
    axis_dist = df['axis'].value_counts().sort_index()
    print(f"\nAxis Distribution:")
    for axis, count in axis_dist.items():
        percentage = (count / len(df)) * 100
        print(f"  {axis}: {count:4d} ({percentage:5.1f}%)")
    
    print(f"\nUniformity Score (std/mean): {axis_dist.std() / axis_dist.mean():.3f}")
    print(f"Most represented: {axis_dist.idxmax()} ({axis_dist.max()} questions)")
    print(f"Least represented: {axis_dist.idxmin()} ({axis_dist.min()} questions)")
    
    # Variation analysis
    if 'is_variation' in df.columns:
        original_count = len(df[df['is_variation'] != True])
        variation_count = len(df) - original_count
        print(f"\nOriginal questions: {original_count}")
        print(f"Generated variations: {variation_count}")
        print(f"Variation ratio: {variation_count/original_count:.1f}:1")
    
    # Embedding analysis
    embeddings = np.array(df['embedding'].tolist())
    print(f"\nEmbedding Analysis:")
    print(f"  Dimensions: {embeddings.shape[1]}")
    print(f"  Average non-zero elements: {np.mean([np.count_nonzero(emb) for emb in embeddings]):.1f}")
    print(f"  Average norm: {np.mean([np.linalg.norm(emb) for emb in embeddings]):.3f}")

def search_questions(df: pd.DataFrame, search_terms: str, max_results: int = 20):
    """Search questions by text content."""
    search_terms_lower = search_terms.lower().split()
    
    matches = []
    for idx, row in df.iterrows():
        text_lower = row['text'].lower()
        match_count = sum(1 for term in search_terms_lower if term in text_lower)
        if match_count > 0:
            matches.append((match_count, idx, row))
    
    # Sort by relevance (number of matching terms)
    matches.sort(key=lambda x: x[0], reverse=True)
    
    print(f"\nSearch Results for '{search_terms}' ({len(matches)} matches):")
    print("=" * 70)
    
    for i, (match_count, idx, row) in enumerate(matches[:max_results]):
        print(f"{i+1:2d}. [{row['axis']}] {row['text']}")
        if match_count > 1:
            print(f"    (matched {match_count} terms)")

def main():
    parser = argparse.ArgumentParser(description="Analyze socionics question pool")
    parser.add_argument("command", choices=['stats', 'cluster', 'search', 'export', 'similar', 'similarity-analysis'], 
                       help="Analysis command to run")
    parser.add_argument("--file", default="survey/question_pool_1000.parquet",
                       help="Question pool parquet file to analyze")
    parser.add_argument("--query", help="Query text for search/similarity operations")
    parser.add_argument("--clusters", type=int, default=10, help="Number of clusters for analysis")
    parser.add_argument("--limit", type=int, default=20, help="Maximum results to show")
    parser.add_argument("--per-axis", type=int, default=25, help="Questions per axis for balanced export")
    parser.add_argument("--output", help="Output file for export operations")
    
    args = parser.parse_args()
    
    if args.command == 'similarity-analysis':
        analyze_similarity_removal()
        return
    
    # Load data for other commands
    df = load_question_data(args.file)
    print(f"Loaded {len(df)} questions from {args.file}")
    
    if args.command == 'stats':
        show_distribution_stats(df)
    
    elif args.command == 'cluster':
        analyze_embedding_clusters(df, args.clusters)
    
    elif args.command == 'search':
        if not args.query:
            print("Please provide --query for search command")
            return
        search_questions(df, args.query, args.limit)
    
    elif args.command == 'similar':
        if not args.query:
            print("Please provide --query for similarity command")
            return
        similar = find_similar_questions(df, args.query, args.limit)
        print(f"\nQuestions similar to '{args.query}':")
        print("=" * 50)
        for i, (idx, text) in enumerate(similar):
            print(f"{i+1:2d}. {text}")
    
    elif args.command == 'export':
        if not args.output:
            print("Please provide --output for export command")
            return
        export_balanced_subset(df, args.per_axis, args.output)

if __name__ == "__main__":
    main()