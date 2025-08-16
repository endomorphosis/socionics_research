#!/usr/bin/env python3
"""
Demo script showing the personality database collector functionality.

This script demonstrates:
1. Creating sample data
2. Storing it in parquet files with IPFS CIDs
3. Generating vector embeddings
4. Performing vector search
"""

import sys
from pathlib import Path
import json

# Add current directory to path for imports
sys.path.append(str(Path(__file__).parent))

from personality_db_collector import (
    ParquetStorage,
    VectorSearchEngine,
    PersonData,
    RatingData,
    ProfileData,
    VectorData
)

def main():
    print("🚀 Personality Database Collector Demo")
    print("=" * 50)
    
    # Initialize storage and search engine
    storage = ParquetStorage("./demo_data")
    search_engine = VectorSearchEngine(storage, model_name="demo-model")
    
    print("\n1. Creating sample personality data...")
    
    # Create sample persons (fictional characters)
    persons = [
        PersonData(
            id=1,
            name="Sherlock Holmes",
            description="Brilliant detective with exceptional deductive reasoning abilities. Analytical, logical, and observant.",
            category_name="Literature",
            subcategory_name="Detective Fiction"
        ),
        PersonData(
            id=2,
            name="Tony Stark",
            description="Genius inventor and businessman. Innovative, confident, and technologically driven.",
            category_name="Movies",
            subcategory_name="Marvel Comics"
        ),
        PersonData(
            id=3,
            name="Hermione Granger", 
            description="Brilliant witch with exceptional academic abilities. Logical, studious, and rule-following.",
            category_name="Literature",
            subcategory_name="Fantasy"
        )
    ]
    
    # Create sample ratings
    ratings = [
        RatingData(person_id=1, personality_type="LII", personality_system="socionics", confidence=0.85, votes_count=42),
        RatingData(person_id=1, personality_type="INTP", personality_system="mbti", confidence=0.80, votes_count=38),
        RatingData(person_id=2, personality_type="ILE", personality_system="socionics", confidence=0.75, votes_count=35),
        RatingData(person_id=2, personality_type="ENTP", personality_system="mbti", confidence=0.78, votes_count=33),
        RatingData(person_id=3, personality_type="LSI", personality_system="socionics", confidence=0.82, votes_count=45),
        RatingData(person_id=3, personality_type="ISTJ", personality_system="mbti", confidence=0.85, votes_count=40)
    ]
    
    print(f"   Created {len(persons)} persons and {len(ratings)} ratings")
    
    print("\n2. Storing data in parquet format with IPFS CIDs...")
    
    # Store persons and ratings
    person_cids = storage.store_persons(persons)
    rating_cids = storage.store_ratings(ratings)
    
    print(f"   Stored persons with CIDs: {list(person_cids.values())[:2]}...")
    print(f"   Stored ratings with {len(rating_cids)} CIDs")
    
    # Create and store profiles
    profiles = []
    for person in persons:
        person_ratings = [r for r in ratings if r.person_id == person.id]
        socionics_ratings = [r for r in person_ratings if r.personality_system == "socionics"]
        mbti_ratings = [r for r in person_ratings if r.personality_system == "mbti"]
        
        profile = ProfileData(
            person=person,
            ratings=person_ratings,
            socionics_ratings=socionics_ratings,
            mbti_ratings=mbti_ratings,
            big5_ratings=[],
            enneagram_ratings=[]
        )
        profiles.append(profile)
    
    profile_cids = storage.store_profiles(profiles)
    print(f"   Stored profiles with CIDs: {list(profile_cids.values())}")
    
    print("\n3. Generating vector embeddings...")
    
    # Load data and generate embeddings
    persons_df = storage.load_persons()
    ratings_df = storage.load_ratings()
    
    person_vectors = search_engine.embed_persons(persons_df)
    rating_vectors = search_engine.embed_ratings(ratings_df)
    
    all_vectors = person_vectors + rating_vectors
    storage.store_vectors(all_vectors)
    
    print(f"   Generated {len(person_vectors)} person embeddings")
    print(f"   Generated {len(rating_vectors)} rating embeddings")
    print(f"   Total: {len(all_vectors)} embeddings stored")
    
    print("\n4. Building search index...")
    
    if search_engine.build_search_index():
        print("   ✓ Search index built successfully")
    else:
        print("   ⚠ Failed to build search index")
        return
    
    print("\n5. Performing vector searches...")
    
    queries = [
        "brilliant detective logical reasoning",
        "genius inventor technology",
        "studious academic rule-following",
        "socionics LII personality type",
        "ENTP mbti innovative"
    ]
    
    for query in queries:
        print(f"\n   Query: '{query}'")
        results = search_engine.search(query, top_k=2, min_score=0.0)
        
        if results:
            for i, result in enumerate(results, 1):
                print(f"     {i}. Score: {result['score']:.4f} | Type: {result['source_type']}")
                print(f"        Text: {result['source_text'][:80]}...")
                print(f"        CID: {result['ipfs_cid']}")
        else:
            print("     No results found")
    
    print("\n6. Finding similar persons...")
    
    # Find similar persons to Sherlock Holmes
    sherlock_cid = person_cids['1']  # Sherlock Holmes
    similar_persons = search_engine.find_similar_persons(sherlock_cid, top_k=2)
    
    print(f"   Similar to Sherlock Holmes (CID: {sherlock_cid}):")
    for result in similar_persons:
        print(f"     Score: {result['score']:.4f} | {result['source_text'][:60]}...")
    
    print("\n7. Storage statistics:")
    
    stats = storage.get_storage_stats()
    for entity_type, stat_data in stats.items():
        if 'count' in stat_data:
            print(f"   {entity_type.capitalize()}: {stat_data['count']} records, {stat_data['file_size_mb']:.2f} MB")
    
    search_stats = search_engine.get_search_stats()
    print(f"   Vectors: {search_stats['total_vectors']} indexed")
    
    print("\n8. Demonstrating CID-based data joining...")
    
    # Show how data can be joined using IPFS CIDs
    print("   Example: Finding all data for Sherlock Holmes using CID joins:")
    
    # Load person by CID
    person_df = storage.load_persons([sherlock_cid])
    if not person_df.empty:
        person_name = person_df.iloc[0]['name']
        person_id = person_df.iloc[0]['id']
        
        print(f"     Person: {person_name} (ID: {person_id})")
        
        # Find ratings for this person
        all_ratings_df = storage.load_ratings()
        person_ratings_df = all_ratings_df[all_ratings_df['person_id'] == person_id]
        
        print(f"     Ratings: {len(person_ratings_df)} found")
        for _, rating_row in person_ratings_df.iterrows():
            print(f"       - {rating_row['personality_system']}: {rating_row['personality_type']} (CID: {rating_row['ipfs_cid']})")
        
        # Find vectors for this person
        all_vectors_df = storage.load_vectors()
        person_vectors_df = all_vectors_df[all_vectors_df['ipfs_cid'] == sherlock_cid]
        
        print(f"     Vectors: {len(person_vectors_df)} found")
        for _, vector_row in person_vectors_df.iterrows():
            print(f"       - {vector_row['source_type']}: {len(vector_row['vector']) if isinstance(vector_row['vector'], list) else 'N/A'} dimensions")
    
    print(f"\n✅ Demo completed! Data stored in './demo_data/'")
    print(f"   You can explore the parquet files and use the CLI tool for more operations.")
    
    # Show CLI usage examples
    print(f"\n💡 Try these CLI commands:")
    print(f"   python personality_db_collector/cli.py stats --storage-path ./demo_data")
    print(f"   python personality_db_collector/cli.py search 'detective logic' --storage-path ./demo_data")


if __name__ == "__main__":
    main()