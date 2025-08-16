"""Parquet storage layer with IPFS CID primary keys."""

import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
import logging
import sys

from .data_models import PersonData, RatingData, ProfileData, VectorData, to_dict

# Add the ipdb directory to the path to import IPFS multiformats
sys.path.append(str(Path(__file__).parent.parent / 'ipdb'))
from ipfs_multiformats import ipfs_multiformats_py, create_cid_from_bytes

logger = logging.getLogger(__name__)


class ParquetStorage:
    """Storage layer for personality database data using parquet files with IPFS CIDs."""
    
    def __init__(self, storage_path: str = "./data/personality_db"):
        """Initialize the storage layer.
        
        Args:
            storage_path: Base directory for storing parquet files
        """
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize subdirectories
        self.persons_path = self.storage_path / "persons"
        self.ratings_path = self.storage_path / "ratings"
        self.profiles_path = self.storage_path / "profiles"
        self.vectors_path = self.storage_path / "vectors"
        self.categories_path = self.storage_path / "categories"
        
        for path in [self.persons_path, self.ratings_path, self.profiles_path, 
                    self.vectors_path, self.categories_path]:
            path.mkdir(exist_ok=True)
        
        # Initialize IPFS multiformats handler
        self.ipfs_handler = ipfs_multiformats_py(metadata={'testing': False})
    
    def generate_cid(self, data: Any) -> str:
        """Generate IPFS CID for given data.
        
        Args:
            data: Data to generate CID for (will be JSON serialized)
            
        Returns:
            IPFS CID string
        """
        if isinstance(data, str):
            content_bytes = data.encode('utf-8')
        else:
            # Serialize to JSON for consistent CID generation
            json_str = json.dumps(data, sort_keys=True, ensure_ascii=False)
            content_bytes = json_str.encode('utf-8')
        
        return create_cid_from_bytes(content_bytes)
    
    def store_persons(self, persons: List[PersonData]) -> Dict[str, str]:
        """Store person data in parquet format.
        
        Args:
            persons: List of PersonData objects
            
        Returns:
            Dictionary mapping person IDs to their IPFS CIDs
        """
        if not persons:
            return {}
        
        # Convert to dictionaries and generate CIDs
        records = []
        id_to_cid = {}
        
        for person in persons:
            person_dict = to_dict(person)
            cid = self.generate_cid(person_dict)
            person_dict['ipfs_cid'] = cid
            person.ipfs_cid = cid  # Update the original object
            records.append(person_dict)
            id_to_cid[str(person.id)] = cid
        
        # Create DataFrame
        df = pd.DataFrame(records)
        
        # Store as parquet
        file_path = self.persons_path / "persons.parquet"
        
        # Load existing data if it exists
        if file_path.exists():
            existing_df = pd.read_parquet(file_path)
            # Merge with existing data, removing duplicates by ipfs_cid
            df = pd.concat([existing_df, df], ignore_index=True)
            df = df.drop_duplicates(subset=['ipfs_cid'], keep='last')
        
        df.to_parquet(file_path, index=False)
        logger.info(f"Stored {len(records)} persons to {file_path}")
        
        return id_to_cid
    
    def store_ratings(self, ratings: List[RatingData]) -> Dict[str, str]:
        """Store rating data in parquet format.
        
        Args:
            ratings: List of RatingData objects
            
        Returns:
            Dictionary mapping rating IDs to their IPFS CIDs
        """
        if not ratings:
            return {}
        
        records = []
        id_to_cid = {}
        
        for rating in ratings:
            rating_dict = to_dict(rating)
            cid = self.generate_cid(rating_dict)
            rating_dict['ipfs_cid'] = cid
            rating.ipfs_cid = cid
            records.append(rating_dict)
            if rating.id:
                id_to_cid[str(rating.id)] = cid
        
        df = pd.DataFrame(records)
        
        file_path = self.ratings_path / "ratings.parquet"
        
        if file_path.exists():
            existing_df = pd.read_parquet(file_path)
            df = pd.concat([existing_df, df], ignore_index=True)
            df = df.drop_duplicates(subset=['ipfs_cid'], keep='last')
        
        df.to_parquet(file_path, index=False)
        logger.info(f"Stored {len(records)} ratings to {file_path}")
        
        return id_to_cid
    
    def store_profiles(self, profiles: List[ProfileData]) -> Dict[str, str]:
        """Store complete profile data in parquet format.
        
        Args:
            profiles: List of ProfileData objects
            
        Returns:
            Dictionary mapping person IDs to profile IPFS CIDs
        """
        if not profiles:
            return {}
        
        records = []
        id_to_cid = {}
        
        for profile in profiles:
            profile_dict = to_dict(profile)
            cid = self.generate_cid(profile_dict)
            profile_dict['ipfs_cid'] = cid
            profile.ipfs_cid = cid
            records.append(profile_dict)
            id_to_cid[str(profile.person.id)] = cid
        
        df = pd.DataFrame(records)
        
        file_path = self.profiles_path / "profiles.parquet"
        
        if file_path.exists():
            existing_df = pd.read_parquet(file_path)
            df = pd.concat([existing_df, df], ignore_index=True)
            df = df.drop_duplicates(subset=['ipfs_cid'], keep='last')
        
        df.to_parquet(file_path, index=False)
        logger.info(f"Stored {len(records)} profiles to {file_path}")
        
        return id_to_cid
    
    def store_vectors(self, vectors: List[VectorData]) -> List[str]:
        """Store vector embeddings in parquet format.
        
        Args:
            vectors: List of VectorData objects
            
        Returns:
            List of IPFS CIDs for the stored vectors
        """
        if not vectors:
            return []
        
        records = []
        cids = []
        
        for vector in vectors:
            vector_dict = to_dict(vector)
            records.append(vector_dict)
            cids.append(vector.ipfs_cid)
        
        df = pd.DataFrame(records)
        
        file_path = self.vectors_path / "vectors.parquet"
        
        if file_path.exists():
            existing_df = pd.read_parquet(file_path)
            df = pd.concat([existing_df, df], ignore_index=True)
            df = df.drop_duplicates(subset=['ipfs_cid'], keep='last')
        
        df.to_parquet(file_path, index=False)
        logger.info(f"Stored {len(records)} vectors to {file_path}")
        
        return cids
    
    def load_persons(self, cids: Optional[List[str]] = None) -> pd.DataFrame:
        """Load person data from parquet file.
        
        Args:
            cids: Optional list of IPFS CIDs to filter by
            
        Returns:
            DataFrame containing person data
        """
        file_path = self.persons_path / "persons.parquet"
        
        if not file_path.exists():
            return pd.DataFrame()
        
        df = pd.read_parquet(file_path)
        
        if cids:
            df = df[df['ipfs_cid'].isin(cids)]
        
        return df
    
    def load_ratings(self, person_cids: Optional[List[str]] = None) -> pd.DataFrame:
        """Load rating data from parquet file.
        
        Args:
            person_cids: Optional list of person IPFS CIDs to filter by
            
        Returns:
            DataFrame containing rating data
        """
        file_path = self.ratings_path / "ratings.parquet"
        
        if not file_path.exists():
            return pd.DataFrame()
        
        df = pd.read_parquet(file_path)
        
        # If filtering by person CIDs, we need to join with persons table
        if person_cids:
            persons_df = self.load_persons(person_cids)
            if not persons_df.empty:
                # Get person IDs for the CIDs
                person_ids = persons_df['id'].tolist()
                df = df[df['person_id'].isin(person_ids)]
        
        return df
    
    def load_profiles(self, cids: Optional[List[str]] = None) -> pd.DataFrame:
        """Load profile data from parquet file.
        
        Args:
            cids: Optional list of IPFS CIDs to filter by
            
        Returns:
            DataFrame containing profile data
        """
        file_path = self.profiles_path / "profiles.parquet"
        
        if not file_path.exists():
            return pd.DataFrame()
        
        df = pd.read_parquet(file_path)
        
        if cids:
            df = df[df['ipfs_cid'].isin(cids)]
        
        return df
    
    def load_vectors(self, cids: Optional[List[str]] = None) -> pd.DataFrame:
        """Load vector data from parquet file.
        
        Args:
            cids: Optional list of IPFS CIDs to filter by
            
        Returns:
            DataFrame containing vector data
        """
        file_path = self.vectors_path / "vectors.parquet"
        
        if not file_path.exists():
            return pd.DataFrame()
        
        df = pd.read_parquet(file_path)
        
        if cids:
            df = df[df['ipfs_cid'].isin(cids)]
        
        return df
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics.
        
        Returns:
            Dictionary with storage statistics
        """
        stats = {}
        
        for entity_type, path in [
            ('persons', self.persons_path / "persons.parquet"),
            ('ratings', self.ratings_path / "ratings.parquet"),
            ('profiles', self.profiles_path / "profiles.parquet"),
            ('vectors', self.vectors_path / "vectors.parquet")
        ]:
            if path.exists():
                try:
                    df = pd.read_parquet(path)
                    stats[entity_type] = {
                        'count': len(df),
                        'file_size_mb': path.stat().st_size / (1024 * 1024),
                        'columns': df.columns.tolist()
                    }
                except Exception as e:
                    stats[entity_type] = {'error': str(e)}
            else:
                stats[entity_type] = {'count': 0, 'file_size_mb': 0}
        
        return stats
    
    def cleanup_duplicates(self):
        """Remove duplicate entries based on IPFS CIDs."""
        for entity_type, file_path in [
            ('persons', self.persons_path / "persons.parquet"),
            ('ratings', self.ratings_path / "ratings.parquet"),
            ('profiles', self.profiles_path / "profiles.parquet"),
            ('vectors', self.vectors_path / "vectors.parquet")
        ]:
            if file_path.exists():
                try:
                    df = pd.read_parquet(file_path)
                    initial_count = len(df)
                    df_clean = df.drop_duplicates(subset=['ipfs_cid'], keep='last')
                    final_count = len(df_clean)
                    
                    if initial_count != final_count:
                        df_clean.to_parquet(file_path, index=False)
                        logger.info(f"Cleaned {entity_type}: removed {initial_count - final_count} duplicates")
                    
                except Exception as e:
                    logger.error(f"Error cleaning duplicates in {entity_type}: {e}")