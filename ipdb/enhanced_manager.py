#!/usr/bin/env python3
"""
Enhanced IPDB Python Integration
Comprehensive integration with DuckDB, hnswlib, and cross-language compatibility
"""

import os
import sys
import json
import sqlite3
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple
import logging

# Optional dependencies - gracefully handle missing packages
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

try:
    import hnswlib
    import numpy as np
    HNSWLIB_AVAILABLE = True
except ImportError:
    HNSWLIB_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

class EnhancedIPDBManager:
    """
    Enhanced IPDB Manager with multi-database support, vector search, and APIs
    """
    
    def __init__(self, db_path: str = "ipdb_enhanced.db", use_duckdb: bool = True):
        self.db_path = db_path
        self.use_duckdb = use_duckdb and DUCKDB_AVAILABLE
        self.conn = None
        self.vector_index = None
        self.embedding_dim = 384
        self.logger = self._setup_logger()
        
        # Initialize database connection
        self._init_database()
        
        # Initialize vector search if available
        if HNSWLIB_AVAILABLE:
            self._init_vector_search()
    
    def _setup_logger(self):
        """Setup logging for the IPDB manager"""
        logger = logging.getLogger('ipdb_enhanced')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _init_database(self):
        """Initialize database connection with DuckDB preference"""
        try:
            if self.use_duckdb and DUCKDB_AVAILABLE:
                self.conn = duckdb.connect(self.db_path if self.db_path != ":memory:" else None)
                self.db_type = "DuckDB"
                self.logger.info("✅ Connected to DuckDB database")
            else:
                self.conn = sqlite3.connect(self.db_path)
                self.conn.row_factory = sqlite3.Row
                self.db_type = "SQLite"
                self.logger.info("✅ Connected to SQLite database (DuckDB not available)")
            
            self._create_core_schema()
            
        except Exception as e:
            self.logger.error(f"❌ Database initialization failed: {e}")
            raise
    
    def _init_vector_search(self):
        """Initialize HNSW vector index for similarity search"""
        try:
            self.vector_index = hnswlib.Index(space='cosine', dim=self.embedding_dim)
            self.vector_index.init_index(max_elements=100000, ef_construction=200, M=16)
            self.vector_index.set_ef(50)  # Query time accuracy/speed tradeoff
            self.logger.info("✅ Vector search index initialized with hnswlib")
        except Exception as e:
            self.logger.warning(f"⚠️ Vector search initialization failed: {e}")
            self.vector_index = None
    
    def _create_core_schema(self):
        """Create essential database schema"""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS entities (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            entity_type TEXT DEFAULT 'person',
            description TEXT,
            metadata TEXT,  -- JSON metadata
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS entity_embeddings (
            entity_id TEXT,
            embedding_data BLOB,
            embedding_dim INTEGER,
            model_name TEXT DEFAULT 'default',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (entity_id) REFERENCES entities(id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(name);
        CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type);
        CREATE INDEX IF NOT EXISTS idx_embeddings_entity ON entity_embeddings(entity_id);
        """
        
        if self.db_type == "DuckDB":
            self.conn.execute(schema_sql)
        else:
            self.conn.executescript(schema_sql)
            self.conn.commit()
        
        self.logger.info("✅ Database schema initialized")
    
    def create_entity(self, name: str, entity_type: str = "person", 
                     description: str = None, metadata: Dict = None) -> str:
        """Create a new entity"""
        import uuid
        
        entity_id = str(uuid.uuid4())
        metadata_json = json.dumps(metadata or {})
        
        sql = """
        INSERT INTO entities (id, name, entity_type, description, metadata)
        VALUES (?, ?, ?, ?, ?)
        """
        
        try:
            if self.db_type == "DuckDB":
                self.conn.execute(sql, [entity_id, name, entity_type, description, metadata_json])
            else:
                self.conn.execute(sql, (entity_id, name, entity_type, description, metadata_json))
                self.conn.commit()
            
            self.logger.info(f"✅ Created entity: {name} ({entity_id})")
            return entity_id
            
        except Exception as e:
            self.logger.error(f"❌ Failed to create entity: {e}")
            raise
    
    def get_entities(self, limit: int = 10, entity_type: str = None) -> List[Dict]:
        """Retrieve entities from database"""
        sql = "SELECT * FROM entities"
        params = []
        
        if entity_type:
            sql += " WHERE entity_type = ?"
            params.append(entity_type)
        
        sql += f" LIMIT {limit}"
        
        try:
            if self.db_type == "DuckDB":
                result = self.conn.execute(sql, params).fetchall()
                columns = [desc[0] for desc in self.conn.description]
                entities = []
                for row in result:
                    entity = dict(zip(columns, row))
                    if entity.get('metadata'):
                        try:
                            entity['metadata'] = json.loads(entity['metadata'])
                        except:
                            entity['metadata'] = {}
                    entities.append(entity)
            else:
                cursor = self.conn.execute(sql, params)
                entities = []
                for row in cursor:
                    entity = dict(row)
                    if entity.get('metadata'):
                        try:
                            entity['metadata'] = json.loads(entity['metadata'])
                        except:
                            entity['metadata'] = {}
                    entities.append(entity)
            
            return entities
            
        except Exception as e:
            self.logger.error(f"❌ Failed to retrieve entities: {e}")
            return []
    
    def add_embedding(self, entity_id: str, embedding: List[float], model_name: str = "default"):
        """Add vector embedding for an entity"""
        if not HNSWLIB_AVAILABLE:
            self.logger.warning("⚠️ Cannot add embedding: hnswlib not available")
            return False
        
        try:
            # Store in database
            embedding_blob = np.array(embedding, dtype=np.float32).tobytes()
            sql = """
            INSERT OR REPLACE INTO entity_embeddings 
            (entity_id, embedding_data, embedding_dim, model_name)
            VALUES (?, ?, ?, ?)
            """
            
            if self.db_type == "DuckDB":
                self.conn.execute(sql, [entity_id, embedding_blob, len(embedding), model_name])
            else:
                self.conn.execute(sql, (entity_id, embedding_blob, len(embedding), model_name))
                self.conn.commit()
            
            # Add to vector index if available
            if self.vector_index and len(embedding) == self.embedding_dim:
                # Get current index size to use as label
                current_size = self.vector_index.get_current_count()
                self.vector_index.add_items(np.array([embedding]), [current_size])
                
                # Store mapping of index -> entity_id
                # This is a simplification; production would need proper mapping table
                
            self.logger.info(f"✅ Added embedding for entity {entity_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to add embedding: {e}")
            return False
    
    def vector_search(self, query_embedding: List[float], k: int = 5) -> List[Dict]:
        """Perform k-nearest neighbor search"""
        if not self.vector_index or not HNSWLIB_AVAILABLE:
            self.logger.warning("⚠️ Vector search not available")
            return []
        
        try:
            if len(query_embedding) != self.embedding_dim:
                raise ValueError(f"Query embedding must be {self.embedding_dim} dimensions")
            
            labels, distances = self.vector_index.knn_query(np.array([query_embedding]), k=k)
            
            results = []
            for i, (label, distance) in enumerate(zip(labels[0], distances[0])):
                results.append({
                    'rank': i + 1,
                    'label': int(label),
                    'distance': float(distance),
                    'similarity': 1.0 - float(distance)  # Convert distance to similarity
                })
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Vector search failed: {e}")
            return []
    
    def import_parquet(self, file_path: str, table_name: str = "imported_data") -> Dict:
        """Import Parquet file using DuckDB or pandas"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Parquet file not found: {file_path}")
        
        try:
            start_time = __import__('time').time()
            
            if self.db_type == "DuckDB":
                # DuckDB can read Parquet directly
                self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM '{file_path}'")
                result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                record_count = result[0]
                
            elif PANDAS_AVAILABLE:
                # Use pandas for SQLite
                df = pd.read_parquet(file_path)
                df.to_sql(table_name, self.conn, if_exists='replace', index=False)
                record_count = len(df)
                
            else:
                raise Exception("Neither DuckDB nor pandas available for Parquet import")
            
            import_time = (__import__('time').time() - start_time) * 1000
            
            result = {
                'success': True,
                'file_path': file_path,
                'table_name': table_name,
                'records_imported': record_count,
                'import_time_ms': round(import_time, 2),
                'database_type': self.db_type
            }
            
            self.logger.info(f"✅ Imported {record_count} records from {file_path} in {import_time:.2f}ms")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ Parquet import failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def get_api_client(self, base_url: str = "http://localhost:3000"):
        """Get API client for remote IPDB server"""
        if not REQUESTS_AVAILABLE:
            self.logger.warning("⚠️ API client not available: requests library not installed")
            return None
        
        return IPDBAPIClient(base_url, logger=self.logger)
    
    def export_browser_sdk(self, output_path: str = "ipdb_browser_sdk.js"):
        """Export JavaScript SDK for browser use"""
        sdk_code = '''
// IPDB Browser SDK - Auto-generated
class IPDBBrowserSDK {
    constructor(baseUrl = 'http://localhost:3000') {
        this.baseUrl = baseUrl;
    }
    
    async getEntities(limit = 10) {
        const response = await fetch(`${this.baseUrl}/api/entities?limit=${limit}`);
        return await response.json();
    }
    
    async createEntity(data) {
        const response = await fetch(`${this.baseUrl}/api/entities`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });
        return await response.json();
    }
    
    async vectorSearch(embedding, k = 5) {
        const response = await fetch(`${this.baseUrl}/api/search/vector`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query_embedding: embedding, k })
        });
        return await response.json();
    }
    
    async importParquet(filePath) {
        const response = await fetch(`${this.baseUrl}/api/import/parquet`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ file_path: filePath })
        });
        return await response.json();
    }
}

// Auto-export
if (typeof window !== 'undefined') {
    window.IPDBBrowserSDK = IPDBBrowserSDK;
}
if (typeof module !== 'undefined' && module.exports) {
    module.exports = IPDBBrowserSDK;
}
'''
        
        with open(output_path, 'w') as f:
            f.write(sdk_code)
        
        self.logger.info(f"✅ Browser SDK exported to {output_path}")
        return output_path
    
    def get_status(self) -> Dict:
        """Get comprehensive system status"""
        return {
            'database': {
                'type': self.db_type,
                'path': self.db_path,
                'connected': self.conn is not None
            },
            'features': {
                'duckdb_available': DUCKDB_AVAILABLE,
                'hnswlib_available': HNSWLIB_AVAILABLE,
                'pandas_available': PANDAS_AVAILABLE,
                'requests_available': REQUESTS_AVAILABLE,
                'vector_search_enabled': self.vector_index is not None
            },
            'vector_index': {
                'enabled': self.vector_index is not None,
                'dimension': self.embedding_dim,
                'current_count': self.vector_index.get_current_count() if self.vector_index else 0
            }
        }
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.logger.info("✅ Database connection closed")


class IPDBAPIClient:
    """HTTP API client for remote IPDB server"""
    
    def __init__(self, base_url: str, logger=None):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session() if REQUESTS_AVAILABLE else None
        self.logger = logger or logging.getLogger('ipdb_client')
    
    def get_entities(self, limit: int = 10) -> Dict:
        """Get entities from remote API"""
        url = f"{self.base_url}/api/entities?limit={limit}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()
    
    def create_entity(self, data: Dict) -> Dict:
        """Create entity via API"""
        url = f"{self.base_url}/api/entities"
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()
    
    def vector_search(self, embedding: List[float], k: int = 5) -> Dict:
        """Perform vector search via API"""
        url = f"{self.base_url}/api/search/vector"
        data = {'query_embedding': embedding, 'k': k}
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()
    
    def get_info(self) -> Dict:
        """Get API information"""
        url = f"{self.base_url}/api/info"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()


def demo_enhanced_ipdb():
    """Comprehensive demo of Enhanced IPDB features"""
    print("🧪 Enhanced IPDB Demonstration")
    print("=" * 50)
    
    # Initialize manager
    db = EnhancedIPDBManager(":memory:")  # Use in-memory database for demo
    
    # Show status
    status = db.get_status()
    print("\n📊 System Status:")
    print(f"Database: {status['database']['type']}")
    print(f"Vector Search: {'✅' if status['vector_index']['enabled'] else '❌'}")
    print(f"DuckDB Available: {'✅' if status['features']['duckdb_available'] else '❌'}")
    print(f"hnswlib Available: {'✅' if status['features']['hnswlib_available'] else '❌'}")
    
    # Create sample entities
    print("\n🏗️ Creating Sample Entities:")
    entities = [
        ("Albert Einstein", "person", "Theoretical physicist", {"field": "physics"}),
        ("Sherlock Holmes", "fictional_character", "Detective", {"author": "Arthur Conan Doyle"}),
        ("Leonardo da Vinci", "person", "Renaissance polymath", {"field": "art_science"})
    ]
    
    entity_ids = []
    for name, entity_type, desc, metadata in entities:
        entity_id = db.create_entity(name, entity_type, desc, metadata)
        entity_ids.append(entity_id)
    
    # Add sample embeddings (if hnswlib available)
    if HNSWLIB_AVAILABLE:
        print("\n🔍 Adding Vector Embeddings:")
        for i, entity_id in enumerate(entity_ids):
            # Generate dummy embedding
            embedding = np.random.rand(384).tolist()
            db.add_embedding(entity_id, embedding, "demo_model")
        
        # Perform vector search
        print("\n🔎 Vector Search Demo:")
        query_embedding = np.random.rand(384).tolist()
        results = db.vector_search(query_embedding, k=2)
        for result in results:
            print(f"  Rank {result['rank']}: Similarity {result['similarity']:.3f}")
    
    # Retrieve entities
    print("\n📋 Retrieved Entities:")
    retrieved = db.get_entities(limit=5)
    for entity in retrieved:
        print(f"  • {entity['name']} ({entity['entity_type']})")
    
    # Export browser SDK
    print("\n🌐 Exporting Browser SDK:")
    sdk_path = db.export_browser_sdk("/tmp/ipdb_sdk_demo.js")
    print(f"  SDK exported to: {sdk_path}")
    
    # Show API client usage (if requests available)
    if REQUESTS_AVAILABLE:
        print("\n🌍 API Client Available:")
        print("  Use: client = db.get_api_client('http://localhost:3000')")
        print("  Then: client.get_entities()")
    
    db.close()
    print("\n✅ Enhanced IPDB demo completed!")
    
    return {
        'entities_created': len(entity_ids),
        'vector_search_available': HNSWLIB_AVAILABLE,
        'api_client_available': REQUESTS_AVAILABLE,
        'database_type': status['database']['type']
    }


if __name__ == "__main__":
    # Run demo
    demo_enhanced_ipdb()