"""Tests for data models."""

import pytest
from datetime import datetime
import json

from personality_db_collector.data_models import (
    PersonData, RatingData, ProfileData, VectorData, CategoryData,
    to_dict, from_dict
)


class TestDataModels:
    """Test data model classes and serialization."""
    
    def test_person_data_creation(self):
        """Test PersonData creation and basic functionality."""
        person = PersonData(
            id=123,
            name="Test Character",
            description="A test character for socionics research",
            category_id=15,
            category_name="TV Shows",
            ipfs_cid="QmTest123"
        )
        
        assert person.id == 123
        assert person.name == "Test Character"
        assert person.ipfs_cid == "QmTest123"
    
    def test_rating_data_creation(self):
        """Test RatingData creation."""
        rating = RatingData(
            person_id=123,
            personality_type="ILE",
            personality_system="socionics",
            rating_value="ILE",
            confidence=0.85,
            votes_count=42,
            ipfs_cid="QmRating123"
        )
        
        assert rating.person_id == 123
        assert rating.personality_type == "ILE"
        assert rating.personality_system == "socionics"
        assert rating.confidence == 0.85
    
    def test_profile_data_creation(self):
        """Test ProfileData creation with nested data."""
        person = PersonData(id=123, name="Test Person")
        
        ratings = [
            RatingData(person_id=123, personality_type="ILE", personality_system="socionics"),
            RatingData(person_id=123, personality_type="ENTP", personality_system="mbti")
        ]
        
        profile = ProfileData(
            person=person,
            ratings=ratings,
            socionics_ratings=[ratings[0]],
            mbti_ratings=[ratings[1]],
            big5_ratings=[],
            enneagram_ratings=[]
        )
        
        assert profile.person.name == "Test Person"
        assert len(profile.ratings) == 2
        assert len(profile.socionics_ratings) == 1
        assert profile.socionics_ratings[0].personality_type == "ILE"
    
    def test_vector_data_creation(self):
        """Test VectorData creation."""
        vector = VectorData(
            ipfs_cid="QmVector123",
            vector=[0.1, 0.2, 0.3, 0.4],
            vector_model="test-model",
            source_text="Test text for embedding",
            source_type="person_description",
            created_at=datetime.now()
        )
        
        assert vector.ipfs_cid == "QmVector123"
        assert len(vector.vector) == 4
        assert vector.source_type == "person_description"
    
    def test_to_dict_conversion(self):
        """Test conversion of dataclass to dictionary."""
        person = PersonData(
            id=123,
            name="Test Person",
            created_at=datetime(2023, 1, 1, 12, 0, 0)
        )
        
        person_dict = to_dict(person)
        
        assert isinstance(person_dict, dict)
        assert person_dict['id'] == 123
        assert person_dict['name'] == "Test Person"
        assert person_dict['created_at'] == "2023-01-01T12:00:00"
    
    def test_from_dict_conversion(self):
        """Test conversion of dictionary to dataclass."""
        person_dict = {
            'id': 123,
            'name': "Test Person",
            'description': "Test description",
            'created_at': "2023-01-01T12:00:00"
        }
        
        person = from_dict(PersonData, person_dict)
        
        assert person.id == 123
        assert person.name == "Test Person"
        assert person.created_at == datetime(2023, 1, 1, 12, 0, 0)
    
    def test_nested_to_dict_conversion(self):
        """Test conversion of nested dataclasses to dictionaries."""
        person = PersonData(id=123, name="Test Person")
        ratings = [RatingData(person_id=123, personality_type="ILE", personality_system="socionics")]
        
        profile = ProfileData(
            person=person,
            ratings=ratings,
            socionics_ratings=ratings,
            mbti_ratings=[],
            big5_ratings=[],
            enneagram_ratings=[]
        )
        
        profile_dict = to_dict(profile)
        
        assert isinstance(profile_dict, dict)
        assert isinstance(profile_dict['person'], dict)
        assert profile_dict['person']['name'] == "Test Person"
        assert isinstance(profile_dict['ratings'], list)
        assert len(profile_dict['ratings']) == 1
        assert profile_dict['ratings'][0]['personality_type'] == "ILE"