"""Vector search engine for personality database data."""

import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Any, Tuple
import logging
from pathlib import Path
from datetime import datetime
import json

from .data_models import VectorData, to_dict
from .storage import ParquetStorage

logger = logging.getLogger(__name__)

try:
    # Try to import sentence transformers for embedding generation
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    logger.warning("sentence-transformers not available, using mock embeddings")

try:
    # Try to import faiss for efficient similarity search
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    FAISS_AVAILABLE = False
    logger.warning("faiss not available, falling back to numpy-based similarity search")


class VectorSearchEngine:
    """Vector search engine for personality database content."""
    
    def __init__(self, storage: ParquetStorage, model_name: str = "all-MiniLM-L6-v2"):
        """Initialize the vector search engine.
        
        Args:
            storage: ParquetStorage instance for data persistence
            model_name: Name of the sentence transformer model to use
        """
        self.storage = storage
        self.model_name = model_name
        self.model = None
        self.vector_index = None
        self.vector_dimension = None
        
        # Initialize embedding model if available
        if SENTENCE_TRANSFORMERS_AVAILABLE:
            try:
                self.model = SentenceTransformer(model_name)
                # Get dimension by encoding a test string
                test_embedding = self.model.encode(["test"])
                self.vector_dimension = test_embedding.shape[1]
                logger.info(f"Initialized sentence transformer model: {model_name}")
            except Exception as e:
                logger.error(f"Failed to initialize sentence transformer: {e}")
                self.model = None
    
    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a list of texts.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embedding vectors
        """
        if not texts:
            return []
        
        if self.model is not None:
            try:
                embeddings = self.model.encode(texts)
                return embeddings.tolist()
            except Exception as e:
                logger.error(f"Error generating embeddings: {e}")
        
        # Fallback to mock embeddings for testing
        logger.warning("Using mock embeddings - not suitable for production")
        return [self._generate_mock_embedding(text, self.vector_dimension) for text in texts]
    
    def _generate_mock_embedding(self, text: str, dimension: Optional[int] = None) -> List[float]:
        """Generate a deterministic mock embedding for testing purposes."""
        if dimension is None:
            dimension = self.vector_dimension if self.vector_dimension else 384
            
        # Use a simple hash-based approach for consistent results
        import hashlib
        hash_obj = hashlib.md5(text.encode())
        hash_bytes = hash_obj.digest()
        
        # Extend hash to desired dimension
        extended_bytes = hash_bytes
        while len(extended_bytes) < dimension * 4:  # 4 bytes per float
            extended_bytes += hash_bytes
        
        # Convert to floats in range [-1, 1]
        floats = []
        for i in range(dimension):
            byte_idx = i * 4
            if byte_idx + 3 < len(extended_bytes):
                int_val = int.from_bytes(extended_bytes[byte_idx:byte_idx+4], 'little')
                float_val = (int_val / (2**32)) * 2 - 1  # Normalize to [-1, 1]
                floats.append(float_val)
            else:
                floats.append(0.0)
        
        return floats
    
    def embed_persons(self, persons_df: pd.DataFrame) -> List[VectorData]:
        """Generate embeddings for person data.
        
        Args:
            persons_df: DataFrame containing person data
            
        Returns:
            List of VectorData objects with embeddings
        """
        if persons_df.empty:
            return []
        
        vectors = []
        
        for _, row in persons_df.iterrows():
            # Create text representation for embedding
            text_parts = []
            
            if pd.notna(row.get('name')):
                text_parts.append(f"Name: {row['name']}")
            
            if pd.notna(row.get('description')):
                text_parts.append(f"Description: {row['description']}")
            
            if pd.notna(row.get('category_name')):
                text_parts.append(f"Category: {row['category_name']}")
            
            if pd.notna(row.get('subcategory_name')):
                text_parts.append(f"Subcategory: {row['subcategory_name']}")
            
            source_text = " | ".join(text_parts)
            
            if source_text:
                embedding = self.generate_embeddings([source_text])[0]
                
                vector_data = VectorData(
                    ipfs_cid=row['ipfs_cid'],
                    vector=embedding,
                    vector_model=self.model_name,
                    source_text=source_text,
                    source_type="person_profile",
                    created_at=datetime.now(),
                    metadata={
                        'person_id': row.get('id'),
                        'person_name': row.get('name'),
                        'category_name': row.get('category_name')
                    }
                )
                vectors.append(vector_data)
        
        return vectors
    
    def embed_ratings(self, ratings_df: pd.DataFrame) -> List[VectorData]:
        """Generate embeddings for rating data.
        
        Args:
            ratings_df: DataFrame containing rating data
            
        Returns:
            List of VectorData objects with embeddings
        """
        if ratings_df.empty:
            return []
        
        vectors = []
        
        for _, row in ratings_df.iterrows():
            text_parts = []
            
            if pd.notna(row.get('personality_system')):
                text_parts.append(f"System: {row['personality_system']}")
            
            if pd.notna(row.get('personality_type')):
                text_parts.append(f"Type: {row['personality_type']}")
            
            if pd.notna(row.get('rating_value')):
                text_parts.append(f"Rating: {row['rating_value']}")
            
            source_text = " | ".join(text_parts)
            
            if source_text:
                embedding = self.generate_embeddings([source_text])[0]
                
                vector_data = VectorData(
                    ipfs_cid=row['ipfs_cid'],
                    vector=embedding,
                    vector_model=self.model_name,
                    source_text=source_text,
                    source_type="personality_rating",
                    created_at=datetime.now(),
                    metadata={
                        'person_id': row.get('person_id'),
                        'personality_system': row.get('personality_system'),
                        'personality_type': row.get('personality_type')
                    }
                )
                vectors.append(vector_data)
        
        return vectors
    
    def build_search_index(self, force_rebuild: bool = False) -> bool:
        """Build or rebuild the vector search index.
        
        Args:
            force_rebuild: Whether to force rebuilding even if index exists
            
        Returns:
            True if index was built successfully
        """
        try:
            # Load all vectors
            vectors_df = self.storage.load_vectors()
            
            if vectors_df.empty:
                logger.warning("No vectors found, cannot build search index")
                return False
            
            if not force_rebuild and self.vector_index is not None:
                logger.info("Search index already exists, skipping rebuild")
                return True
            
            # Extract vectors
            vector_list = []
            for _, row in vectors_df.iterrows():
                vector_data = row.get('vector')
                if vector_data is not None:
                    try:
                        if isinstance(vector_data, str):
                            # Parse JSON string
                            vector = json.loads(vector_data)
                        elif isinstance(vector_data, (list, tuple)):
                            vector = list(vector_data)
                        elif isinstance(vector_data, np.ndarray):
                            vector = vector_data.tolist()
                        elif hasattr(vector_data, 'tolist'):
                            vector = vector_data.tolist()
                        else:
                            # Try to convert to list
                            vector = list(vector_data)
                        
                        if isinstance(vector, list) and len(vector) > 0:
                            vector_list.append(vector)
                    except (json.JSONDecodeError, TypeError, ValueError) as e:
                        logger.warning(f"Could not parse vector: {e}")
                        continue
                
            if not vector_list:
                logger.warning("No valid vectors found")
                return False
            
            vectors_array = np.array(vector_list).astype('float32')
            logger.debug(f"Created vectors array with shape: {vectors_array.shape}")
            
            if self.vector_dimension is None:
                self.vector_dimension = vectors_array.shape[1]
            
            # Build FAISS index if available
            if FAISS_AVAILABLE:
                self.vector_index = faiss.IndexFlatIP(self.vector_dimension)  # Inner product (cosine similarity)
                # Normalize vectors for cosine similarity
                faiss.normalize_L2(vectors_array)
                self.vector_index.add(vectors_array)
                logger.info(f"Built FAISS index with {len(vector_list)} vectors")
            else:
                # Store vectors for numpy-based search
                self.vector_index = vectors_array
                logger.info(f"Stored {len(vector_list)} vectors for numpy-based search")
            
            return True
            
        except Exception as e:
            logger.error(f"Error building search index: {e}")
            return False
    
    def search(self, query_text: str, top_k: int = 10, 
               min_score: float = 0.0) -> List[Dict[str, Any]]:
        """Search for similar content using vector similarity.
        
        Args:
            query_text: Text to search for
            top_k: Number of top results to return
            min_score: Minimum similarity score threshold
            
        Returns:
            List of search results with metadata and scores
        """
        if not query_text:
            return []
        
        # Generate query embedding
        query_embedding = self.generate_embeddings([query_text])[0]
        query_vector = np.array(query_embedding).astype('float32').reshape(1, -1)
        
        # Normalize for cosine similarity
        if FAISS_AVAILABLE and self.vector_index is not None:
            faiss.normalize_L2(query_vector)
        
        # Perform search
        if FAISS_AVAILABLE and hasattr(self.vector_index, 'search'):
            # FAISS-based search
            scores, indices = self.vector_index.search(query_vector, top_k)
            scores = scores[0]  # Remove batch dimension
            indices = indices[0]
        elif isinstance(self.vector_index, np.ndarray):
            # Numpy-based cosine similarity search
            # Normalize query vector for cosine similarity
            query_norm = np.linalg.norm(query_vector)
            if query_norm > 0:
                query_vector = query_vector / query_norm
            
            # Normalize stored vectors for cosine similarity
            index_norms = np.linalg.norm(self.vector_index, axis=1, keepdims=True)
            index_norms[index_norms == 0] = 1  # Avoid division by zero
            normalized_index = self.vector_index / index_norms
            
            similarities = np.dot(normalized_index, query_vector.T).flatten()
            top_indices = np.argsort(similarities)[::-1][:top_k]
            scores = similarities[top_indices]
            indices = top_indices
        else:
            logger.error("No search index available")
            return []
        
        # Filter by minimum score and get metadata
        results = []
        vectors_df = self.storage.load_vectors()
        
        for idx, score in zip(indices, scores):
            if score >= min_score and idx < len(vectors_df):
                row = vectors_df.iloc[idx]
                
                result = {
                    'ipfs_cid': row.get('ipfs_cid'),
                    'score': float(score),
                    'source_text': row.get('source_text', ''),
                    'source_type': row.get('source_type', ''),
                    'vector_model': row.get('vector_model', ''),
                    'metadata': json.loads(row.get('metadata', '{}')) if isinstance(row.get('metadata'), str) else row.get('metadata', {})
                }
                results.append(result)
        
        return results
    
    def find_similar_persons(self, person_cid: str, top_k: int = 10) -> List[Dict[str, Any]]:
        """Find persons similar to a given person based on their embeddings.
        
        Args:
            person_cid: IPFS CID of the person to find similar persons for
            top_k: Number of similar persons to return
            
        Returns:
            List of similar persons with similarity scores
        """
        # Get the person's vector
        vectors_df = self.storage.load_vectors()
        person_vector_row = vectors_df[
            (vectors_df['ipfs_cid'] == person_cid) & 
            (vectors_df['source_type'] == 'person_profile')
        ]
        
        if person_vector_row.empty:
            logger.warning(f"No vector found for person CID: {person_cid}")
            return []
        
        # Use the person's source text as query
        source_text = person_vector_row.iloc[0]['source_text']
        results = self.search(source_text, top_k + 1)  # +1 to exclude self
        
        # Filter out the original person and keep only person profiles
        filtered_results = []
        for result in results:
            if (result['ipfs_cid'] != person_cid and 
                result['source_type'] == 'person_profile'):
                filtered_results.append(result)
        
        return filtered_results[:top_k]
    
    def get_search_stats(self) -> Dict[str, Any]:
        """Get statistics about the search index.
        
        Returns:
            Dictionary with search statistics
        """
        vectors_df = self.storage.load_vectors()
        
        stats = {
            'total_vectors': len(vectors_df),
            'vector_dimension': self.vector_dimension,
            'model_name': self.model_name,
            'faiss_available': FAISS_AVAILABLE,
            'sentence_transformers_available': SENTENCE_TRANSFORMERS_AVAILABLE,
            'index_built': self.vector_index is not None
        }
        
        if not vectors_df.empty:
            stats['source_types'] = vectors_df['source_type'].value_counts().to_dict()
            stats['vector_models'] = vectors_df['vector_model'].value_counts().to_dict()
        
        return stats