#!/usr/bin/env python3
"""CLI tool for collecting and managing personality database data."""

import argparse
import logging
import sys
import json
from pathlib import Path
from typing import Optional, List

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from personality_db_collector import (
    PersonalityDBClient,
    ParquetStorage, 
    VectorSearchEngine,
    ProfileData,
    PersonData,
    RatingData
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def collect_profiles(client: PersonalityDBClient, storage: ParquetStorage,
                    max_profiles: Optional[int] = None,
                    category_id: Optional[int] = None) -> int:
    """Collect profile data from the API and store in parquet format.
    
    Args:
        client: API client instance
        storage: Storage instance
        max_profiles: Maximum number of profiles to collect
        category_id: Optional category ID filter
        
    Returns:
        Number of profiles collected
    """
    logger.info("Starting profile collection...")
    
    total_collected = 0
    batch_size = 100
    
    filters = {}
    if category_id is not None:
        filters['cat_id'] = category_id
    
    try:
        for batch in client.get_all_profiles(
            batch_size=batch_size,
            max_profiles=max_profiles,
            **filters
        ):
            # Convert API data to our data models
            persons = []
            all_ratings = []
            profiles = []
            
            for raw_profile in batch:
                try:
                    # Convert person data
                    person = client.convert_raw_to_person_data(raw_profile)
                    persons.append(person)
                    
                    # Get ratings for this person if available
                    ratings = []
                    if 'ratings' in raw_profile:
                        for raw_rating in raw_profile['ratings']:
                            rating = client.convert_raw_to_rating_data(raw_rating, person.id)
                            ratings.append(rating)
                            all_ratings.append(rating)
                    
                    # Try to fetch additional ratings from API
                    try:
                        additional_ratings_data = client.get_ratings(person.id)
                        for raw_rating in additional_ratings_data:
                            rating = client.convert_raw_to_rating_data(raw_rating, person.id)
                            ratings.append(rating)
                            all_ratings.append(rating)
                    except Exception as e:
                        logger.debug(f"Could not fetch additional ratings for person {person.id}: {e}")
                    
                    # Create profile
                    socionics_ratings = [r for r in ratings if 'socionics' in r.personality_system.lower()]
                    mbti_ratings = [r for r in ratings if 'mbti' in r.personality_system.lower()]
                    big5_ratings = [r for r in ratings if 'big' in r.personality_system.lower()]
                    enneagram_ratings = [r for r in ratings if 'enneagram' in r.personality_system.lower()]
                    
                    profile = ProfileData(
                        person=person,
                        ratings=ratings,
                        socionics_ratings=socionics_ratings,
                        mbti_ratings=mbti_ratings,
                        big5_ratings=big5_ratings,
                        enneagram_ratings=enneagram_ratings
                    )
                    profiles.append(profile)
                    
                except Exception as e:
                    logger.error(f"Error processing profile data: {e}")
                    continue
            
            # Store in parquet files
            if persons:
                storage.store_persons(persons)
            if all_ratings:
                storage.store_ratings(all_ratings)
            if profiles:
                storage.store_profiles(profiles)
            
            total_collected += len(batch)
            logger.info(f"Collected and stored {len(batch)} profiles (total: {total_collected})")
    
    except KeyboardInterrupt:
        logger.info("Collection interrupted by user")
    except Exception as e:
        logger.error(f"Error during collection: {e}")
        raise
    
    logger.info(f"Collection completed. Total profiles collected: {total_collected}")
    return total_collected


def generate_embeddings(storage: ParquetStorage, search_engine: VectorSearchEngine) -> int:
    """Generate embeddings for stored data.
    
    Args:
        storage: Storage instance
        search_engine: Vector search engine instance
        
    Returns:
        Number of embeddings generated
    """
    logger.info("Generating embeddings...")
    
    total_vectors = 0
    
    # Generate embeddings for persons
    persons_df = storage.load_persons()
    if not persons_df.empty:
        person_vectors = search_engine.embed_persons(persons_df)
        if person_vectors:
            storage.store_vectors(person_vectors)
            total_vectors += len(person_vectors)
            logger.info(f"Generated {len(person_vectors)} person embeddings")
    
    # Generate embeddings for ratings
    ratings_df = storage.load_ratings()
    if not ratings_df.empty:
        rating_vectors = search_engine.embed_ratings(ratings_df)
        if rating_vectors:
            storage.store_vectors(rating_vectors)
            total_vectors += len(rating_vectors)
            logger.info(f"Generated {len(rating_vectors)} rating embeddings")
    
    logger.info(f"Total embeddings generated: {total_vectors}")
    return total_vectors


def search_data(search_engine: VectorSearchEngine, query: str, top_k: int = 10):
    """Search the stored data using vector similarity.
    
    Args:
        search_engine: Vector search engine instance
        query: Search query text
        top_k: Number of results to return
    """
    logger.info(f"Searching for: '{query}'")
    
    # Build search index if needed
    if not search_engine.build_search_index():
        logger.error("Failed to build search index")
        return
    
    results = search_engine.search(query, top_k=top_k)
    
    if not results:
        print("No results found.")
        return
    
    print(f"\nFound {len(results)} results:\n")
    
    for i, result in enumerate(results, 1):
        print(f"{i}. Score: {result['score']:.4f}")
        print(f"   Type: {result['source_type']}")
        print(f"   Text: {result['source_text'][:200]}...")
        print(f"   CID: {result['ipfs_cid']}")
        
        if result.get('metadata'):
            print(f"   Metadata: {json.dumps(result['metadata'], indent=2)}")
        print()


def show_stats(storage: ParquetStorage, search_engine: VectorSearchEngine):
    """Show statistics about stored data.
    
    Args:
        storage: Storage instance
        search_engine: Vector search engine instance
    """
    print("Storage Statistics:")
    storage_stats = storage.get_storage_stats()
    for entity_type, stats in storage_stats.items():
        print(f"  {entity_type.capitalize()}:")
        if 'error' in stats:
            print(f"    Error: {stats['error']}")
        else:
            print(f"    Count: {stats['count']}")
            print(f"    File Size: {stats['file_size_mb']:.2f} MB")
    
    print("\nSearch Engine Statistics:")
    search_stats = search_engine.get_search_stats()
    for key, value in search_stats.items():
        print(f"  {key}: {value}")


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(description="Personality Database Collector CLI")
    
    parser.add_argument('--storage-path', default='./data/personality_db',
                       help='Path to store data files')
    parser.add_argument('--rate-limit', type=float, default=0.12,
                       help='Rate limit delay between API requests (seconds)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Collect command
    collect_parser = subparsers.add_parser('collect', help='Collect data from API')
    collect_parser.add_argument('--max-profiles', type=int,
                              help='Maximum number of profiles to collect')
    collect_parser.add_argument('--category-id', type=int,
                              help='Filter by category ID')
    
    # Embed command
    embed_parser = subparsers.add_parser('embed', help='Generate embeddings')
    embed_parser.add_argument('--model', default='all-MiniLM-L6-v2',
                            help='Sentence transformer model to use')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search stored data')
    search_parser.add_argument('query', help='Search query text')
    search_parser.add_argument('--top-k', type=int, default=10,
                             help='Number of results to return')
    search_parser.add_argument('--model', default='all-MiniLM-L6-v2',
                             help='Sentence transformer model to use')
    
    # Stats command
    subparsers.add_parser('stats', help='Show storage and search statistics')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize components
    client = PersonalityDBClient(rate_limit_delay=args.rate_limit)
    storage = ParquetStorage(args.storage_path)
    search_engine = VectorSearchEngine(storage, model_name=getattr(args, 'model', 'all-MiniLM-L6-v2'))
    
    try:
        if args.command == 'collect':
            collect_profiles(
                client, storage,
                max_profiles=args.max_profiles,
                category_id=args.category_id
            )
        
        elif args.command == 'embed':
            generate_embeddings(storage, search_engine)
        
        elif args.command == 'search':
            search_data(search_engine, args.query, args.top_k)
        
        elif args.command == 'stats':
            show_stats(storage, search_engine)
    
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Operation failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()