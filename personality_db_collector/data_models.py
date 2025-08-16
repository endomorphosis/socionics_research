"""Data models for personality database entities."""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Union
from datetime import datetime


@dataclass
class PersonData:
    """Represents a person/character in the personality database."""
    id: int
    name: str
    description: Optional[str] = None
    image_url: Optional[str] = None
    category_id: Optional[int] = None
    category_name: Optional[str] = None
    subcategory_id: Optional[int] = None
    subcategory_name: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None
    ipfs_cid: Optional[str] = None  # Primary key generated from content


@dataclass
class RatingData:
    """Represents a personality type rating for a person."""
    id: Optional[int] = None
    person_id: int = 0
    personality_type: str = ""
    personality_system: str = ""  # e.g., "socionics", "mbti", "big5"
    rating_value: Optional[Union[str, float]] = None
    confidence: Optional[float] = None
    votes_count: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None
    ipfs_cid: Optional[str] = None


@dataclass
class ProfileData:
    """Complete profile data including person and all their ratings."""
    person: PersonData
    ratings: List[RatingData]
    socionics_ratings: List[RatingData]
    mbti_ratings: List[RatingData]
    big5_ratings: List[RatingData]
    enneagram_ratings: List[RatingData]
    created_at: Optional[datetime] = None
    ipfs_cid: Optional[str] = None  # Primary key for the complete profile


@dataclass
class CategoryData:
    """Represents a category (e.g., TV shows, movies, books)."""
    id: int
    name: str
    description: Optional[str] = None
    parent_id: Optional[int] = None
    subcategories: Optional[List['CategoryData']] = None
    metadata: Optional[Dict[str, Any]] = None
    ipfs_cid: Optional[str] = None


@dataclass
class VectorData:
    """Vector embedding data for search functionality."""
    ipfs_cid: str  # Links to the original data
    vector: List[float]
    vector_model: str  # e.g., "sentence-transformers/all-MiniLM-L6-v2"
    source_text: str  # The text that was vectorized
    source_type: str  # e.g., "person_description", "category_name", etc.
    created_at: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None


def to_dict(obj) -> Dict[str, Any]:
    """Convert dataclass to dictionary for serialization."""
    if hasattr(obj, '__dataclass_fields__'):
        result = {}
        for field_name in obj.__dataclass_fields__:
            value = getattr(obj, field_name)
            if value is None:
                continue
            elif isinstance(value, datetime):
                result[field_name] = value.isoformat()
            elif isinstance(value, list):
                result[field_name] = [to_dict(item) if hasattr(item, '__dataclass_fields__') else item for item in value]
            elif hasattr(value, '__dataclass_fields__'):
                result[field_name] = to_dict(value)
            else:
                result[field_name] = value
        return result
    else:
        return obj


def from_dict(data_class, data: Dict[str, Any]):
    """Convert dictionary to dataclass instance."""
    if not data:
        return None
    
    # Handle datetime fields
    datetime_fields = []
    for field_name, field_type in data_class.__dataclass_fields__.items():
        if field_type.type == Optional[datetime] or field_type.type == datetime:
            datetime_fields.append(field_name)
    
    processed_data = data.copy()
    for field_name in datetime_fields:
        if field_name in processed_data and isinstance(processed_data[field_name], str):
            processed_data[field_name] = datetime.fromisoformat(processed_data[field_name])
    
    return data_class(**processed_data)