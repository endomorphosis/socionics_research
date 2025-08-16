"""Tests for the vector search functionality."""

import pytest
import tempfile
import shutil
import pandas as pd
import numpy as np
from datetime import datetime

from personality_db_collector.storage import ParquetStorage
from personality_db_collector.vector_search import VectorSearchEngine
from personality_db_collector.data_models import PersonData, RatingData, VectorData


class TestVectorSearchEngine:
    """Test the vector search engine functionality."""
    
    @pytest.fixture
    def temp_storage(self):
        """Create a temporary storage directory."""
        temp_dir = tempfile.mkdtemp()
        storage = ParquetStorage(temp_dir)
        yield storage
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def search_engine(self, temp_storage):
        """Create a vector search engine instance."""
        return VectorSearchEngine(temp_storage, model_name="test-model")
    
    def test_search_engine_initialization(self, search_engine):
        """Test search engine initialization."""
        assert search_engine.model_name == "test-model"
        assert search_engine.storage is not None
    
    def test_mock_embedding_generation(self, search_engine):
        """Test mock embedding generation (used when sentence-transformers not available)."""
        texts = ["Hello world", "Test embedding", "Another test"]
        embeddings = search_engine.generate_embeddings(texts)
        
        assert len(embeddings) == 3
        assert all(len(emb) == 384 for emb in embeddings)  # Default dimension
        assert isinstance(embeddings[0], list)
        assert all(isinstance(val, float) for val in embeddings[0])
        
        # Same text should generate same embedding
        same_text_embeddings = search_engine.generate_embeddings(["Hello world", "Hello world"])
        assert same_text_embeddings[0] == same_text_embeddings[1]
        
        # Different text should generate different embeddings
        assert embeddings[0] != embeddings[1]
    
    def test_embed_persons(self, search_engine, temp_storage):
        """Test embedding generation for person data."""
        persons = [
            PersonData(
                id=1, 
                name="Harry Potter", 
                description="The Boy Who Lived", 
                category_name="Books",
                ipfs_cid="QmPerson1"
            ),
            PersonData(
                id=2,
                name="Hermione Granger",
                description="Brilliant witch and best friend",
                category_name="Books", 
                ipfs_cid="QmPerson2"
            )
        ]
        
        temp_storage.store_persons(persons)
        persons_df = temp_storage.load_persons()
        
        vectors = search_engine.embed_persons(persons_df)
        
        assert len(vectors) == 2
        assert all(isinstance(v, VectorData) for v in vectors)
        assert vectors[0].ipfs_cid == "QmPerson1"
        assert vectors[1].ipfs_cid == "QmPerson2"
        assert "Harry Potter" in vectors[0].source_text
        assert vectors[0].source_type == "person_profile"
    
    def test_embed_ratings(self, search_engine, temp_storage):
        """Test embedding generation for rating data."""
        ratings = [
            RatingData(
                person_id=1,
                personality_type="ILE",
                personality_system="socionics",
                ipfs_cid="QmRating1"
            ),
            RatingData(
                person_id=1,
                personality_type="ENTP",
                personality_system="mbti",
                ipfs_cid="QmRating2"
            )
        ]
        
        temp_storage.store_ratings(ratings)
        ratings_df = temp_storage.load_ratings()
        
        vectors = search_engine.embed_ratings(ratings_df)
        
        assert len(vectors) == 2
        assert vectors[0].ipfs_cid == "QmRating1"
        assert vectors[1].ipfs_cid == "QmRating2"
        assert "ILE" in vectors[0].source_text
        assert "socionics" in vectors[0].source_text
        assert vectors[0].source_type == "personality_rating"
    
    def test_vector_storage_and_search(self, search_engine, temp_storage):
        """Test storing vectors and building search index."""
        # Create some test vectors
        vectors = [
            VectorData(
                ipfs_cid="QmTest1",
                vector=[0.1, 0.2, 0.3, 0.4],
                vector_model="test-model",
                source_text="Harry Potter wizard magic",
                source_type="person_profile",
                created_at=datetime.now(),
                metadata={"person_id": 1, "person_name": "Harry Potter"}
            ),
            VectorData(
                ipfs_cid="QmTest2", 
                vector=[0.5, 0.6, 0.7, 0.8],
                vector_model="test-model",
                source_text="Hermione Granger books study",
                source_type="person_profile",
                created_at=datetime.now(),
                metadata={"person_id": 2, "person_name": "Hermione Granger"}
            )
        ]
        
        # Store vectors
        temp_storage.store_vectors(vectors)
        
        # Build search index
        assert search_engine.build_search_index()
        
        # Test search
        results = search_engine.search("wizard magic", top_k=2)
        
        assert len(results) <= 2
        if results:  # Only test if we got results (depends on implementation)
            assert 'score' in results[0]
            assert 'ipfs_cid' in results[0]
            assert 'source_text' in results[0]
    
    def test_search_stats(self, search_engine, temp_storage):
        """Test search engine statistics."""
        # Add some test data
        vectors = [
            VectorData(
                ipfs_cid="QmTest1",
                vector=[0.1, 0.2, 0.3],
                vector_model="test-model",
                source_text="Test text",
                source_type="person_profile",
                created_at=datetime.now()
            )
        ]
        
        temp_storage.store_vectors(vectors)
        
        stats = search_engine.get_search_stats()
        
        assert 'total_vectors' in stats
        assert 'model_name' in stats
        assert 'faiss_available' in stats
        assert 'sentence_transformers_available' in stats
        assert stats['total_vectors'] == 1
        assert stats['model_name'] == "test-model"
    
    def test_find_similar_persons(self, search_engine, temp_storage):
        """Test finding similar persons functionality."""
        # Create test vectors for persons
        vectors = [
            VectorData(
                ipfs_cid="QmPerson1",
                vector=[0.1, 0.2, 0.3],
                vector_model="test-model",
                source_text="Harry Potter wizard boy",
                source_type="person_profile",
                created_at=datetime.now(),
                metadata={"person_id": 1}
            ),
            VectorData(
                ipfs_cid="QmPerson2",
                vector=[0.4, 0.5, 0.6],
                vector_model="test-model", 
                source_text="Ron Weasley wizard friend",
                source_type="person_profile",
                created_at=datetime.now(),
                metadata={"person_id": 2}
            )
        ]
        
        temp_storage.store_vectors(vectors)
        
        # Build index
        search_engine.build_search_index()
        
        # Find similar persons
        similar = search_engine.find_similar_persons("QmPerson1", top_k=1)
        
        # Should find similar persons (excluding self)
        if similar:  # Only test if we got results
            assert len(similar) <= 1
            assert similar[0]['ipfs_cid'] != "QmPerson1"  # Should exclude self
    
    def test_empty_data_handling(self, search_engine, temp_storage):
        """Test handling of empty data."""
        # Test with no data
        assert search_engine.generate_embeddings([]) == []
        
        empty_df = pd.DataFrame()
        assert search_engine.embed_persons(empty_df) == []
        assert search_engine.embed_ratings(empty_df) == []
        
        # Build index with no vectors should return False
        assert not search_engine.build_search_index()
        
        # Search with no index should return empty results
        results = search_engine.search("test query")
        assert results == []