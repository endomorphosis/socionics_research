"""Tests for the parquet storage functionality."""

import pytest
import tempfile
import shutil
import pandas as pd
from pathlib import Path
from datetime import datetime

from personality_db_collector.storage import ParquetStorage
from personality_db_collector.data_models import PersonData, RatingData, VectorData


class TestParquetStorage:
    """Test the parquet storage functionality."""
    
    @pytest.fixture
    def temp_storage(self):
        """Create a temporary storage directory."""
        temp_dir = tempfile.mkdtemp()
        storage = ParquetStorage(temp_dir)
        yield storage
        shutil.rmtree(temp_dir)
    
    def test_storage_initialization(self, temp_storage):
        """Test storage initialization and directory creation."""
        assert temp_storage.storage_path.exists()
        assert temp_storage.persons_path.exists()
        assert temp_storage.ratings_path.exists()
        assert temp_storage.profiles_path.exists()
        assert temp_storage.vectors_path.exists()
    
    def test_cid_generation(self, temp_storage):
        """Test IPFS CID generation."""
        test_data = {"id": 123, "name": "Test Person"}
        cid1 = temp_storage.generate_cid(test_data)
        cid2 = temp_storage.generate_cid(test_data)
        
        # Same data should generate same CID
        assert cid1 == cid2
        assert cid1.startswith("bafy")  # CIDv1 format
        
        # Different data should generate different CID
        different_data = {"id": 124, "name": "Different Person"}
        cid3 = temp_storage.generate_cid(different_data)
        assert cid1 != cid3
    
    def test_store_and_load_persons(self, temp_storage):
        """Test storing and loading person data."""
        persons = [
            PersonData(id=1, name="Person 1", description="First test person"),
            PersonData(id=2, name="Person 2", description="Second test person")
        ]
        
        # Store persons
        id_to_cid = temp_storage.store_persons(persons)
        
        assert len(id_to_cid) == 2
        assert "1" in id_to_cid
        assert "2" in id_to_cid
        
        # Check that CIDs were assigned
        assert persons[0].ipfs_cid is not None
        assert persons[1].ipfs_cid is not None
        
        # Load persons
        persons_df = temp_storage.load_persons()
        
        assert len(persons_df) == 2
        assert set(persons_df['name']) == {"Person 1", "Person 2"}
        assert 'ipfs_cid' in persons_df.columns
    
    def test_store_and_load_ratings(self, temp_storage):
        """Test storing and loading rating data."""
        ratings = [
            RatingData(person_id=1, personality_type="ILE", personality_system="socionics"),
            RatingData(person_id=1, personality_type="ENTP", personality_system="mbti"),
            RatingData(person_id=2, personality_type="ESI", personality_system="socionics")
        ]
        
        # Store ratings
        id_to_cid = temp_storage.store_ratings(ratings)
        
        # Check CIDs were assigned
        for rating in ratings:
            assert rating.ipfs_cid is not None
        
        # Load ratings
        ratings_df = temp_storage.load_ratings()
        
        assert len(ratings_df) == 3
        assert set(ratings_df['personality_system']) == {"socionics", "mbti"}
        assert 'ipfs_cid' in ratings_df.columns
    
    def test_store_and_load_vectors(self, temp_storage):
        """Test storing and loading vector data."""
        vectors = [
            VectorData(
                ipfs_cid="QmVector1",
                vector=[0.1, 0.2, 0.3],
                vector_model="test-model",
                source_text="Test text 1",
                source_type="person_profile",
                created_at=datetime.now()
            ),
            VectorData(
                ipfs_cid="QmVector2",
                vector=[0.4, 0.5, 0.6],
                vector_model="test-model",
                source_text="Test text 2",
                source_type="person_profile",
                created_at=datetime.now()
            )
        ]
        
        # Store vectors
        cids = temp_storage.store_vectors(vectors)
        
        assert len(cids) == 2
        assert "QmVector1" in cids
        assert "QmVector2" in cids
        
        # Load vectors
        vectors_df = temp_storage.load_vectors()
        
        assert len(vectors_df) == 2
        assert set(vectors_df['ipfs_cid']) == {"QmVector1", "QmVector2"}
        assert 'vector' in vectors_df.columns
    
    def test_load_with_cid_filter(self, temp_storage):
        """Test loading data with CID filtering."""
        persons = [
            PersonData(id=1, name="Person 1"),
            PersonData(id=2, name="Person 2")
        ]
        
        id_to_cid = temp_storage.store_persons(persons)
        
        # Load only one person by CID
        target_cid = id_to_cid["1"]
        filtered_df = temp_storage.load_persons([target_cid])
        
        assert len(filtered_df) == 1
        assert filtered_df.iloc[0]['name'] == "Person 1"
    
    def test_duplicate_handling(self, temp_storage):
        """Test that duplicates are handled correctly."""
        person = PersonData(id=1, name="Person 1")
        
        # Store the same person twice
        temp_storage.store_persons([person])
        temp_storage.store_persons([person])  # Should not create duplicate
        
        persons_df = temp_storage.load_persons()
        
        # Should only have one record due to CID-based deduplication
        assert len(persons_df) == 1
    
    def test_storage_stats(self, temp_storage):
        """Test storage statistics functionality."""
        # Add some data
        persons = [PersonData(id=1, name="Test Person")]
        temp_storage.store_persons(persons)
        
        ratings = [RatingData(person_id=1, personality_type="ILE", personality_system="socionics")]
        temp_storage.store_ratings(ratings)
        
        # Get stats
        stats = temp_storage.get_storage_stats()
        
        assert 'persons' in stats
        assert 'ratings' in stats
        assert stats['persons']['count'] == 1
        assert stats['ratings']['count'] == 1
        assert stats['persons']['file_size_mb'] > 0
    
    def test_cleanup_duplicates(self, temp_storage):
        """Test the duplicate cleanup functionality."""
        # This test verifies the cleanup method runs without error
        # The actual duplicate removal is tested in test_duplicate_handling
        temp_storage.cleanup_duplicates()
        # Should not raise any exceptions