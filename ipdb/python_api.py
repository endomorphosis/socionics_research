"""
Python API bindings for IPDB Enhanced Features
==============================================

This module provides Python bindings for the enhanced IPDB functionality
including DuckDB integration, vector search, and multi-language compatibility.
"""

import json
import requests
import numpy as np
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass, asdict
import subprocess
import sys
import os

@dataclass
class IPDBConfig:
    """Configuration for IPDB connections."""
    api_url: str = "http://localhost:3001/api"
    use_duckdb: bool = True
    vector_dimension: int = 384
    timeout: int = 30

class IPDBPythonClient:
    """Python client for IPDB with enhanced features."""
    
    def __init__(self, config: Optional[IPDBConfig] = None):
        self.config = config or IPDBConfig()
        self.session = requests.Session()
        self.session.timeout = self.config.timeout
        
    def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request to IPDB API."""
        url = f"{self.config.api_url}/{endpoint.lstrip('/')}"
        
        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise IPDBError(f"API request failed: {e}")
    
    def health_check(self) -> Dict[str, Any]:
        """Check API health and availability."""
        return self._request('GET', 'health')
    
    def get_api_info(self) -> Dict[str, Any]:
        """Get API information and capabilities."""
        return self._request('GET', 'api/info')
    
    def get_entities(self, 
                    limit: int = 10, 
                    offset: int = 0,
                    entity_type: Optional[str] = None,
                    search: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get entities with optional filtering."""
        params = {'limit': limit, 'offset': offset}
        if entity_type:
            params['entityType'] = entity_type
        if search:
            params['search'] = search
            
        result = self._request('GET', 'entities', params=params)
        return result.get('entities', [])
    
    def create_entity(self, 
                     name: str,
                     entity_type: str = 'person',
                     description: Optional[str] = None,
                     metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a new entity."""
        data = {
            'name': name,
            'entity_type': entity_type,
            'description': description,
            'metadata': metadata or {}
        }
        
        result = self._request('POST', 'entities', json=data)
        return result.get('entity', {})
    
    def get_entity_typings(self, entity_id: str, system: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get personality typings for an entity."""
        params = {'system': system} if system else {}
        result = self._request('GET', f'entities/{entity_id}/typings', params=params)
        return result.get('typings', [])
    
    def add_entity_embedding(self, entity_id: str, embedding: Union[List[float], np.ndarray]) -> Dict[str, Any]:
        """Add vector embedding for an entity."""
        if isinstance(embedding, np.ndarray):
            embedding = embedding.tolist()
        
        data = {'embedding': embedding}
        return self._request('POST', f'entities/{entity_id}/embeddings', json=data)
    
    def vector_search(self, 
                     embedding: Union[List[float], np.ndarray],
                     k: int = 10,
                     entity_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search for similar entities using vector similarity."""
        if isinstance(embedding, np.ndarray):
            embedding = embedding.tolist()
        
        data = {
            'embedding': embedding,
            'k': k,
            'entityType': entity_type
        }
        
        result = self._request('POST', 'search/vector', json=data)
        return result.get('results', [])
    
    def text_search(self, 
                   query: str,
                   limit: int = 10,
                   entity_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search entities by text query."""
        data = {
            'query': query,
            'limit': limit,
            'entityType': entity_type
        }
        
        result = self._request('POST', 'search/text', json=data)
        return result.get('results', [])
    
    def import_parquet(self, file_path: str, embeddings_path: Optional[str] = None) -> Dict[str, Any]:
        """Import data from Parquet file."""
        data = {
            'filePath': file_path,
            'embeddingsPath': embeddings_path
        }
        
        return self._request('POST', 'import/parquet', json=data)
    
    def create_user(self, 
                   username: str,
                   role: str = 'annotator',
                   experience_level: str = 'novice') -> Dict[str, Any]:
        """Create a new user."""
        data = {
            'username': username,
            'role': role,
            'experienceLevel': experience_level
        }
        
        result = self._request('POST', 'users', json=data)
        return result.get('user', {})

class IPDBNumpyIntegration:
    """Integration utilities for NumPy and scientific computing."""
    
    @staticmethod
    def generate_embedding(text: str, method: str = 'simple_hash', dimension: int = 384) -> np.ndarray:
        """
        Generate a simple embedding from text for demo purposes.
        In production, use proper embedding models like sentence-transformers.
        """
        if method == 'simple_hash':
            # Simple hash-based embedding for demo
            import hashlib
            hash_obj = hashlib.md5(text.encode())
            seed = int(hash_obj.hexdigest()[:8], 16)
            np.random.seed(seed)
            
            embedding = np.random.normal(0, 1, dimension).astype(np.float32)
            # Normalize
            embedding = embedding / np.linalg.norm(embedding)
            return embedding
        else:
            raise ValueError(f"Unknown embedding method: {method}")
    
    @staticmethod
    def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
        """Calculate cosine similarity between two vectors."""
        return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
    
    @staticmethod
    def batch_similarity(query: np.ndarray, embeddings: List[np.ndarray]) -> np.ndarray:
        """Calculate similarity between query and batch of embeddings."""
        similarities = []
        for embedding in embeddings:
            sim = IPDBNumpyIntegration.cosine_similarity(query, embedding)
            similarities.append(sim)
        return np.array(similarities)

class IPDBError(Exception):
    """Custom exception for IPDB-related errors."""
    pass

def start_ipdb_server(port: int = 3001, background: bool = True) -> subprocess.Popen:
    """Start the IPDB API server."""
    import subprocess
    import os
    
    # Get the directory of this script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Command to start the TypeScript server
    cmd = [
        'node',
        '--loader', 'ts-node/esm',
        os.path.join(current_dir, 'api.ts')
    ]
    
    env = os.environ.copy()
    env['PORT'] = str(port)
    
    if background:
        return subprocess.Popen(cmd, env=env)
    else:
        subprocess.run(cmd, env=env)

def demo_python_integration():
    """Demonstrate Python integration with IPDB."""
    print("🐍 IPDB Python Integration Demo")
    print("=" * 50)
    
    # Start server in background (optional)
    print("Starting IPDB API server...")
    # server_process = start_ipdb_server(background=True)
    # time.sleep(3)  # Wait for server to start
    
    try:
        # Initialize client
        client = IPDBPythonClient()
        
        # Health check
        print("\n1. Health Check...")
        health = client.health_check()
        print(f"✅ API Status: {health['status']}")
        
        # Get API info
        print("\n2. API Information...")
        info = client.get_api_info()
        print(f"📋 API: {info['name']} v{info['version']}")
        print(f"🚀 Features: {len(info['features'])} available")
        
        # Create sample entities
        print("\n3. Creating Entities...")
        
        entities = [
            {
                'name': 'Albert Einstein',
                'entity_type': 'person',
                'description': 'Theoretical physicist, developer of relativity theory',
                'metadata': {'field': 'physics', 'nationality': 'German-American'}
            },
            {
                'name': 'Marie Curie', 
                'entity_type': 'person',
                'description': 'Physicist and chemist, Nobel Prize winner',
                'metadata': {'field': 'chemistry', 'nationality': 'Polish-French'}
            }
        ]
        
        created_entities = []
        for entity_data in entities:
            entity = client.create_entity(**entity_data)
            created_entities.append(entity)
            print(f"✅ Created: {entity['name']}")
        
        # Generate and add embeddings
        print("\n4. Adding Vector Embeddings...")
        
        numpy_helper = IPDBNumpyIntegration()
        
        for entity in created_entities:
            # Generate embedding from entity description
            text = f"{entity['name']} {entity.get('description', '')}"
            embedding = numpy_helper.generate_embedding(text)
            
            result = client.add_entity_embedding(entity['id'], embedding)
            print(f"✅ Added embedding for {entity['name']}")
        
        # Vector search demo
        print("\n5. Vector Similarity Search...")
        
        query_text = "brilliant scientist researcher"
        query_embedding = numpy_helper.generate_embedding(query_text)
        
        search_results = client.vector_search(query_embedding, k=5)
        print(f"🔍 Found {len(search_results)} similar entities:")
        
        for result in search_results:
            entity = result['entity']
            similarity = result['similarity']
            print(f"  • {entity['name']}: {similarity:.3f} similarity")
        
        # Text search demo
        print("\n6. Text Search...")
        
        text_results = client.text_search("physicist", limit=5)
        print(f"📝 Text search results: {len(text_results)}")
        
        for entity in text_results:
            print(f"  • {entity['name']}")
        
        # Get all entities
        print("\n7. Entity Listing...")
        
        all_entities = client.get_entities(limit=20)
        print(f"📋 Total entities: {len(all_entities)}")
        
        # Demonstrate numpy integration
        print("\n8. NumPy Integration...")
        
        # Generate sample embeddings
        sample_embeddings = [
            numpy_helper.generate_embedding("scientific researcher"),
            numpy_helper.generate_embedding("creative artist"),
            numpy_helper.generate_embedding("business leader")
        ]
        
        # Calculate similarities
        query_emb = numpy_helper.generate_embedding("academic scholar")
        similarities = numpy_helper.batch_similarity(query_emb, sample_embeddings)
        
        print("🔢 Similarity scores:")
        labels = ["Scientific", "Creative", "Business"] 
        for label, sim in zip(labels, similarities):
            print(f"  • {label}: {sim:.3f}")
        
        print("\n✨ Python integration demo completed!")
        print("🌟 Available Features:")
        print("   • REST API client")
        print("   • NumPy integration")
        print("   • Vector search")
        print("   • Parquet import")
        print("   • Cross-language compatibility")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
    finally:
        # Clean up server process if started
        # if 'server_process' in locals():
        #     server_process.terminate()
        pass

if __name__ == "__main__":
    demo_python_integration()