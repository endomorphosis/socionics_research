"""
IPDB Database Manager for Socionics Research
===========================================

This module provides database management and ORM-like functionality for the 
Socionics Research personality typing and rating system with enhanced support
for DuckDB, vector search, and multi-language APIs.

Main features:
- Database schema management (SQLite/DuckDB)
- Data migration from existing Parquet files  
- User and entity management
- Typing session management
- Inter-rater reliability calculations
- Consensus tracking
- Vector similarity search with hnswlib
- Multi-language API compatibility

Usage:
    from ipdb.database_manager import IPDBManager
    
    db = IPDBManager(connection_string="mysql://user:pass@localhost/socionics_research")
    db.initialize_database()
    db.import_pdb_data("data/bot_store/pdb_profiles_normalized.parquet")
"""

import uuid
import json
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Tuple, Union
from dataclasses import dataclass
from enum import Enum
import sqlite3
import logging

# Optional imports for enhanced functionality
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    np = None

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    
try:
    import hnswlib
    HNSWLIB_AVAILABLE = True
except ImportError:
    HNSWLIB_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None

logger = logging.getLogger(__name__)


class EntityType(Enum):
    PERSON = "person"
    FICTIONAL_CHARACTER = "fictional_character"
    PUBLIC_FIGURE = "public_figure"


class UserRole(Enum):
    ANNOTATOR = "annotator"
    PANEL_RATER = "panel_rater"
    ADJUDICATOR = "adjudicator"
    ADMIN = "admin"


class ExperienceLevel(Enum):
    NOVICE = "novice"
    INTERMEDIATE = "intermediate"
    EXPERT = "expert"


@dataclass
class Entity:
    """Represents an entity that can be personality typed."""
    id: str
    name: str
    description: Optional[str] = None
    entity_type: EntityType = EntityType.PERSON
    source: Optional[str] = None
    external_id: Optional[str] = None
    external_source: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class VectorSearchResult:
    """Result from vector similarity search."""
    entity_id: str
    entity_name: str
    entity_type: str
    similarity: float
    distance: float
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class User:
    """Represents a user who can perform ratings/typings."""
    id: str
    username: str
    email: Optional[str] = None
    display_name: Optional[str] = None
    role: UserRole = UserRole.ANNOTATOR
    experience_level: ExperienceLevel = ExperienceLevel.NOVICE
    qualifications: Optional[Dict[str, Any]] = None
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class TypingJudgment:
    """Represents a personality type judgment by a rater."""
    id: str
    entity_id: str
    rater_id: str
    session_id: Optional[str] = None
    system_id: int = None
    type_id: Optional[int] = None
    confidence: Optional[float] = None
    method: Optional[str] = None
    notes: Optional[str] = None
    rationale: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class IPDBManager:
    """Enhanced Database manager for IPDB with DuckDB and vector search support."""
    
    def __init__(self, db_path: str = None, connection_string: str = None, 
                 use_duckdb: bool = None, vector_dimension: int = 384):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to SQLite/DuckDB database file
            connection_string: Full database connection string (for compatibility)
            use_duckdb: Force DuckDB usage. If None, auto-detect based on availability
            vector_dimension: Dimension of vector embeddings for similarity search
        """
        self.db_path = db_path or "/tmp/socionics_research.db"
        self.connection_string = connection_string
        self.vector_dimension = vector_dimension
        
        # Determine which database backend to use
        if use_duckdb is None:
            self.use_duckdb = DUCKDB_AVAILABLE
        else:
            self.use_duckdb = use_duckdb and DUCKDB_AVAILABLE
            
        if self.use_duckdb and not DUCKDB_AVAILABLE:
            logger.warning("DuckDB requested but not available, falling back to SQLite")
            self.use_duckdb = False
            
        self._connection = None
        
        # Initialize vector search if available
        self.vector_index = None
        self.entity_embeddings: Dict[str, np.ndarray] = {}
        self.entity_id_to_index: Dict[str, int] = {}
        self.index_to_entity_id: Dict[int, str] = {}
        self._init_vector_search()
        
    def get_connection(self):
        """Get database connection."""
        self._init_vector_search()
        
    def _init_vector_search(self):
        """Initialize vector search capabilities if hnswlib is available."""
        if HNSWLIB_AVAILABLE:
            try:
                self.vector_index = hnswlib.Index(space='cosine', dim=self.vector_dimension)
                self.vector_index.init_index(max_elements=10000, ef_construction=200, M=16)
                logger.info("Vector search initialized with hnswlib")
            except Exception as e:
                logger.warning(f"Failed to initialize vector search: {e}")
                self.vector_index = None
        else:
            logger.info("hnswlib not available, vector search disabled")
        
    def get_connection(self):
        """Get database connection (SQLite or DuckDB)."""
        if self._connection is None:
            if self.connection_string:
                # For future MySQL/PostgreSQL support
                raise NotImplementedError("Full database connection strings not yet supported")
            else:
                # Use DuckDB or SQLite
                if self.use_duckdb:
                    self._connection = duckdb.connect(self.db_path)
                    logger.info("Connected to DuckDB database")
                else:
                    self._connection = sqlite3.connect(self.db_path)
                    self._connection.row_factory = sqlite3.Row  # Enable dict-like access
                    logger.info("Connected to SQLite database")
        return self._connection
    
    def initialize_database(self):
        """Initialize database with schema."""
        conn = self.get_connection()
        
        # Read and execute schema file
        import os
        schema_path = os.path.join(os.path.dirname(__file__), "database_schema.sql")
        
        # Convert MySQL schema to SQLite-compatible schema
        sqlite_schema = self._convert_mysql_to_sqlite_schema()
        
        try:
            conn.executescript(sqlite_schema)
            conn.commit()
            logger.info("Database schema initialized successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to initialize database schema: {e}")
            raise
    
    def _convert_mysql_to_sqlite_schema(self) -> str:
        """Convert MySQL schema to SQLite-compatible schema."""
        # For now, create a simplified SQLite schema
        return """
        -- SQLite version of the schema
        
        -- Core entity tables
        CREATE TABLE IF NOT EXISTS entities (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            entity_type TEXT NOT NULL CHECK (entity_type IN ('person', 'fictional_character', 'public_figure')),
            source TEXT,
            external_id TEXT,
            external_source TEXT,
            metadata TEXT, -- JSON as TEXT in SQLite
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE,
            display_name TEXT,
            role TEXT NOT NULL CHECK (role IN ('annotator', 'panel_rater', 'adjudicator', 'admin')),
            experience_level TEXT DEFAULT 'novice' CHECK (experience_level IN ('novice', 'intermediate', 'expert')),
            qualifications TEXT, -- JSON as TEXT
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Personality typing systems
        CREATE TABLE IF NOT EXISTS personality_systems (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            display_name TEXT NOT NULL,
            description TEXT,
            version TEXT DEFAULT '1.0',
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS personality_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            system_id INTEGER NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            description TEXT,
            FOREIGN KEY (system_id) REFERENCES personality_systems(id),
            UNIQUE(system_id, code)
        );
        
        CREATE TABLE IF NOT EXISTS personality_functions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            system_id INTEGER NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            description TEXT,
            function_order INTEGER,
            FOREIGN KEY (system_id) REFERENCES personality_systems(id),
            UNIQUE(system_id, code)
        );
        
        -- Rating sessions
        CREATE TABLE IF NOT EXISTS rating_sessions (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            methodology TEXT NOT NULL CHECK (methodology IN ('structured_interview', 'video_analysis', 'text_analysis', 'composite_review')),
            session_type TEXT NOT NULL CHECK (session_type IN ('individual', 'panel', 'consensus')),
            created_by TEXT NOT NULL,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            status TEXT DEFAULT 'active' CHECK (status IN ('active', 'completed', 'cancelled')),
            metadata TEXT, -- JSON
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
        
        CREATE TABLE IF NOT EXISTS session_entities (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            added_by TEXT NOT NULL,
            FOREIGN KEY (session_id) REFERENCES rating_sessions(id),
            FOREIGN KEY (entity_id) REFERENCES entities(id),
            FOREIGN KEY (added_by) REFERENCES users(id),
            UNIQUE(session_id, entity_id)
        );
        
        -- Typing judgments and ratings
        CREATE TABLE IF NOT EXISTS typing_judgments (
            id TEXT PRIMARY KEY,
            entity_id TEXT NOT NULL,
            rater_id TEXT NOT NULL,
            session_id TEXT,
            system_id INTEGER NOT NULL,
            type_id INTEGER,
            confidence REAL CHECK (confidence >= 0 AND confidence <= 1),
            method TEXT,
            notes TEXT,
            rationale TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (entity_id) REFERENCES entities(id),
            FOREIGN KEY (rater_id) REFERENCES users(id),
            FOREIGN KEY (session_id) REFERENCES rating_sessions(id),
            FOREIGN KEY (system_id) REFERENCES personality_systems(id),
            FOREIGN KEY (type_id) REFERENCES personality_types(id)
        );
        
        CREATE TABLE IF NOT EXISTS type_probability_distributions (
            id TEXT PRIMARY KEY,
            judgment_id TEXT NOT NULL,
            type_id INTEGER NOT NULL,
            probability REAL NOT NULL CHECK (probability >= 0 AND probability <= 1),
            FOREIGN KEY (judgment_id) REFERENCES typing_judgments(id),
            FOREIGN KEY (type_id) REFERENCES personality_types(id),
            UNIQUE(judgment_id, type_id)
        );
        
        CREATE TABLE IF NOT EXISTS function_confidence_scores (
            id TEXT PRIMARY KEY,
            judgment_id TEXT NOT NULL,
            function_id INTEGER NOT NULL,
            confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
            FOREIGN KEY (judgment_id) REFERENCES typing_judgments(id),
            FOREIGN KEY (function_id) REFERENCES personality_functions(id),
            UNIQUE(judgment_id, function_id)
        );
        
        -- Data sources for integration
        CREATE TABLE IF NOT EXISTS data_sources (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            source_type TEXT NOT NULL CHECK (source_type IN ('parquet', 'json', 'csv', 'api')),
            file_path TEXT,
            connection_string TEXT,
            description TEXT,
            schema_version TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        );
        
        CREATE TABLE IF NOT EXISTS entity_data_mappings (
            id TEXT PRIMARY KEY,
            entity_id TEXT NOT NULL,
            data_source_id TEXT NOT NULL,
            external_key TEXT NOT NULL,
            external_value TEXT NOT NULL,
            mapping_confidence REAL DEFAULT 1.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (entity_id) REFERENCES entities(id),
            FOREIGN KEY (data_source_id) REFERENCES data_sources(id),
            UNIQUE(data_source_id, external_key, external_value)
        );
        
        -- Insert initial data
        INSERT OR IGNORE INTO personality_systems (name, display_name, description) VALUES
        ('socionics', 'Socionics', 'Information Metabolism theory of personality types'),
        ('mbti', 'MBTI', 'Myers-Briggs Type Indicator'),
        ('big5', 'Big Five', 'Five-factor model of personality'),
        ('enneagram', 'Enneagram', 'Nine personality types system');
        
        -- Insert Socionics types
        INSERT OR IGNORE INTO personality_types (system_id, code, name) 
        SELECT s.id, t.code, t.name FROM personality_systems s, (
            SELECT 'ILE' as code, 'Intuitive Logical Extravert (Don Quixote)' as name UNION
            SELECT 'SEI', 'Sensory Ethical Introvert (Dumas)' UNION
            SELECT 'ESE', 'Ethical Sensory Extravert (Hugo)' UNION
            SELECT 'LII', 'Logical Intuitive Introvert (Robespierre)' UNION
            SELECT 'EIE', 'Ethical Intuitive Extravert (Hamlet)' UNION
            SELECT 'LSI', 'Logical Sensory Introvert (Maxim)' UNION
            SELECT 'SLE', 'Sensory Logical Extravert (Zhukov)' UNION
            SELECT 'IEI', 'Intuitive Ethical Introvert (Yesenin)' UNION
            SELECT 'SEE', 'Sensory Ethical Extravert (Napoleon)' UNION
            SELECT 'ILI', 'Intuitive Logical Introvert (Balzac)' UNION
            SELECT 'LIE', 'Logical Intuitive Extravert (Jack)' UNION
            SELECT 'ESI', 'Ethical Sensory Introvert (Dreiser)' UNION
            SELECT 'LSE', 'Logical Sensory Extravert (Stirlitz)' UNION
            SELECT 'EII', 'Ethical Intuitive Introvert (Dostoyevsky)' UNION
            SELECT 'IEE', 'Intuitive Ethical Extravert (Huxley)' UNION
            SELECT 'SLI', 'Sensory Logical Introvert (Gabin)'
        ) t WHERE s.name = 'socionics';
        
        -- Insert MBTI types
        INSERT OR IGNORE INTO personality_types (system_id, code, name)
        SELECT s.id, t.code, t.name FROM personality_systems s, (
            SELECT 'INTJ' as code, 'Architect' as name UNION
            SELECT 'INTP', 'Thinker' UNION
            SELECT 'ENTJ', 'Commander' UNION
            SELECT 'ENTP', 'Debater' UNION
            SELECT 'INFJ', 'Advocate' UNION
            SELECT 'INFP', 'Mediator' UNION
            SELECT 'ENFJ', 'Protagonist' UNION
            SELECT 'ENFP', 'Campaigner' UNION
            SELECT 'ISTJ', 'Logistician' UNION
            SELECT 'ISFJ', 'Protector' UNION
            SELECT 'ESTJ', 'Executive' UNION
            SELECT 'ESFJ', 'Consul' UNION
            SELECT 'ISTP', 'Virtuoso' UNION
            SELECT 'ISFP', 'Adventurer' UNION
            SELECT 'ESTP', 'Entrepreneur' UNION
            SELECT 'ESFP', 'Entertainer'
        ) t WHERE s.name = 'mbti';
        
        -- Insert Socionics functions
        INSERT OR IGNORE INTO personality_functions (system_id, code, name)
        SELECT s.id, f.code, f.name FROM personality_systems s, (
            SELECT 'Ne' as code, 'Extraverted Intuition' as name UNION
            SELECT 'Ni', 'Introverted Intuition' UNION
            SELECT 'Se', 'Extraverted Sensing' UNION
            SELECT 'Si', 'Introverted Sensing' UNION
            SELECT 'Te', 'Extraverted Thinking' UNION
            SELECT 'Ti', 'Introverted Thinking' UNION
            SELECT 'Fe', 'Extraverted Feeling' UNION
            SELECT 'Fi', 'Introverted Feeling'
        ) f WHERE s.name = 'socionics';
        """
    
    def import_pdb_data(self, normalized_parquet_path: str, vectors_parquet_path: str = None):
        """
        Import data from the existing PDB parquet files.
        
        Args:
            normalized_parquet_path: Path to pdb_profiles_normalized.parquet
            vectors_parquet_path: Path to pdb_profile_vectors.parquet (optional)
        """
        if not PANDAS_AVAILABLE:
            logger.warning("Parquet import requires pandas. Install with: pip install pandas")
            return
            
        logger.info(f"Importing PDB data from {normalized_parquet_path}")
        
        # Read the normalized profiles
        df = pd.read_parquet(normalized_parquet_path)
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Create a data source record
            data_source_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT OR REPLACE INTO data_sources (id, name, source_type, file_path, description)
                VALUES (?, ?, ?, ?, ?)
            """, (data_source_id, "PDB Normalized Profiles", "parquet", normalized_parquet_path,
                  "Personality Database normalized profile data"))
            
            imported_count = 0
            
            for _, row in df.iterrows():
                entity_id = str(uuid.uuid4())
                
                # Create entity
                cursor.execute("""
                    INSERT OR IGNORE INTO entities (id, name, entity_type, source, external_id, external_source)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (entity_id, row.get('name', ''), 'fictional_character', 'pdb', 
                      str(row.get('pid', '')) if (PANDAS_AVAILABLE and pd.notna(row.get('pid'))) else None, 'personality_database'))
                
                # Create mapping to CID
                if PANDAS_AVAILABLE and pd.notna(row.get('cid')):
                    mapping_id = str(uuid.uuid4())
                    cursor.execute("""
                        INSERT OR IGNORE INTO entity_data_mappings (id, entity_id, data_source_id, external_key, external_value)
                        VALUES (?, ?, ?, ?, ?)
                    """, (mapping_id, entity_id, data_source_id, 'cid', row['cid']))
                
                # Create typing judgments for available systems
                systems = ['mbti', 'socionics', 'big5']
                for system in systems:
                    if (not PANDAS_AVAILABLE or pd.notna(row.get(system))) and row.get(system):
                        # Get system ID
                        cursor.execute("SELECT id FROM personality_systems WHERE name = ?", (system,))
                        system_result = cursor.fetchone()
                        if system_result:
                            system_id = system_result[0]
                            
                            # Get or create type
                            cursor.execute("""
                                SELECT id FROM personality_types WHERE system_id = ? AND code = ?
                            """, (system_id, row[system]))
                            type_result = cursor.fetchone()
                            
                            if not type_result:
                                # Create new type if it doesn't exist
                                cursor.execute("""
                                    INSERT INTO personality_types (system_id, code, name)
                                    VALUES (?, ?, ?)
                                """, (system_id, row[system], row[system]))
                                type_id = cursor.lastrowid
                            else:
                                type_id = type_result[0]
                            
                            # Create a system judgment
                            judgment_id = str(uuid.uuid4())
                            cursor.execute("""
                                INSERT OR IGNORE INTO typing_judgments (id, entity_id, rater_id, system_id, type_id, confidence, method, notes)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """, (judgment_id, entity_id, 'system', system_id, type_id, 1.0, 'pdb_import', 
                                  f'Imported from PDB with CID: {row.get("cid", "")}'))
                
                imported_count += 1
                if imported_count % 100 == 0:
                    logger.info(f"Imported {imported_count} entities...")
            
            conn.commit()
            logger.info(f"Successfully imported {imported_count} entities from PDB data")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to import PDB data: {e}")
            raise
    
    def create_user(self, username: str, email: str = None, role: UserRole = UserRole.ANNOTATOR,
                   experience_level: ExperienceLevel = ExperienceLevel.NOVICE) -> User:
        """Create a new user."""
        user = User(
            id=str(uuid.uuid4()),
            username=username,
            email=email,
            role=role,
            experience_level=experience_level
        )
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO users (id, username, email, role, experience_level)
                VALUES (?, ?, ?, ?, ?)
            """, (user.id, user.username, user.email, user.role.value, user.experience_level.value))
            conn.commit()
            logger.info(f"Created user: {username}")
            return user
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to create user {username}: {e}")
            raise
    
    def create_rating_session(self, name: str, description: str, methodology: str, 
                             session_type: str, created_by: str) -> str:
        """Create a new rating session."""
        session_id = str(uuid.uuid4())
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO rating_sessions (id, name, description, methodology, session_type, created_by)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (session_id, name, description, methodology, session_type, created_by))
            conn.commit()
            logger.info(f"Created rating session: {name}")
            return session_id
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to create rating session {name}: {e}")
            raise
    
    def get_entities(self, limit: int = 100, offset: int = 0) -> List[Dict]:
        """Get entities with pagination."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT e.*, 
                   COUNT(tj.id) as typing_count,
                   MAX(tj.updated_at) as last_typed
            FROM entities e
            LEFT JOIN typing_judgments tj ON e.id = tj.entity_id
            GROUP BY e.id
            ORDER BY e.name
            LIMIT ? OFFSET ?
        """, (limit, offset))
        
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    def get_typing_summary(self, entity_id: str) -> List[Dict]:
        """Get typing summary for an entity."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT ps.name as system_name, ps.display_name as system_display,
                   pt.code as type_code, pt.name as type_name,
                   COUNT(tj.id) as judgment_count,
                   AVG(tj.confidence) as avg_confidence,
                   MAX(tj.updated_at) as last_updated
            FROM typing_judgments tj
            JOIN personality_systems ps ON tj.system_id = ps.id
            LEFT JOIN personality_types pt ON tj.type_id = pt.id
            WHERE tj.entity_id = ?
            GROUP BY ps.id, pt.id
            ORDER BY ps.name, judgment_count DESC
        """, (entity_id,))
        
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    def add_entity_embedding(self, entity_id: str, embedding: Union[List[float], Any]) -> None:
        """
        Add vector embedding for an entity to enable similarity search.
        
        Args:
            entity_id: Entity ID to add embedding for
            embedding: Vector embedding (numpy array or list)
        """
        if not HNSWLIB_AVAILABLE:
            logger.warning("Vector embeddings require hnswlib package")
            return
            
        # Convert to list if numpy array
        if NUMPY_AVAILABLE and isinstance(embedding, np.ndarray):
            embedding_list = embedding.tolist()
        elif isinstance(embedding, list):
            embedding_list = embedding
        else:
            embedding_list = list(embedding)
            
        if len(embedding_list) != self.vector_dimension:
            raise ValueError(f"Embedding dimension {len(embedding_list)} doesn't match expected {self.vector_dimension}")
            
        # Convert to numpy array for hnswlib
        if NUMPY_AVAILABLE:
            embedding_array = np.array(embedding_list, dtype=np.float32)
        else:
            logger.warning("NumPy not available, vector operations may be slower")
            # Use built-in array operations
            embedding_array = embedding_list
        
        # Store in memory for quick access
        self.entity_embeddings[entity_id] = embedding_array
        
        # Add to vector index if available
        if self.vector_index is not None:
            current_index = len(self.entity_id_to_index)
            self.entity_id_to_index[entity_id] = current_index
            self.index_to_entity_id[current_index] = entity_id
            
            # Resize index if needed
            if current_index >= self.vector_index.get_max_elements():
                new_size = max(1000, current_index * 2)
                self.vector_index.resize_index(new_size)
            
            if NUMPY_AVAILABLE:
                self.vector_index.add_items(embedding_array.reshape(1, -1), np.array([current_index]))
            else:
                # Use list format for hnswlib
                self.vector_index.add_items([embedding_array], [current_index])
            logger.info(f"Added embedding for entity {entity_id}")
    
    def vector_search(self, query_embedding: Union[List[float], Any], 
                     k: int = 10, entity_type: str = None) -> List[VectorSearchResult]:
        """
        Search for similar entities using vector similarity.
        
        Args:
            query_embedding: Query vector
            k: Number of results to return
            entity_type: Optional filter by entity type
            
        Returns:
            List of VectorSearchResult objects
        """
        if not HNSWLIB_AVAILABLE or self.vector_index is None:
            logger.warning("Vector search requires hnswlib and initialized index")
            return []
            
        if len(self.entity_embeddings) == 0:
            logger.info("No entity embeddings available for search")
            return []
            
        # Convert query to appropriate format
        if NUMPY_AVAILABLE and isinstance(query_embedding, np.ndarray):
            query_array = query_embedding
        elif isinstance(query_embedding, list):
            if NUMPY_AVAILABLE:
                query_array = np.array(query_embedding, dtype=np.float32)
            else:
                query_array = query_embedding
        else:
            if NUMPY_AVAILABLE:
                query_array = np.array(query_embedding, dtype=np.float32)
            else:
                query_array = list(query_embedding)
            
        if len(query_array) != self.vector_dimension:
            raise ValueError(f"Query embedding dimension {len(query_array)} doesn't match expected {self.vector_dimension}")
        
        # Perform vector search
        try:
            if NUMPY_AVAILABLE:
                labels, distances = self.vector_index.knn_query(query_array.reshape(1, -1), k=k)
            else:
                labels, distances = self.vector_index.knn_query([query_array], k=k)
            labels = labels[0]  # Extract from batch
            distances = distances[0]
        except RuntimeError as e:
            logger.error(f"Vector search failed: {e}")
            return []
        
        # Get entity details and build results
        results = []
        conn = self.get_connection()
        cursor = conn.cursor()
        
        for i, (label, distance) in enumerate(zip(labels, distances)):
            entity_id = self.index_to_entity_id.get(label)
            if entity_id is None:
                continue
                
            # Get entity details from database
            cursor.execute("""
                SELECT id, name, entity_type, description, metadata
                FROM entities WHERE id = ?
            """, (entity_id,))
            
            row = cursor.fetchone()
            if row:
                entity_dict = dict(row)
                
                # Apply entity type filter if specified
                if entity_type and entity_dict['entity_type'] != entity_type:
                    continue
                    
                result = VectorSearchResult(
                    entity_id=entity_dict['id'],
                    entity_name=entity_dict['name'],
                    entity_type=entity_dict['entity_type'],
                    similarity=1.0 - distance,  # Convert distance to similarity (cosine)
                    distance=distance,
                    metadata=json.loads(entity_dict['metadata']) if entity_dict.get('metadata') else None
                )
                results.append(result)
        
        return results
    
    def import_parquet_with_duckdb(self, parquet_path: str, 
                                  embeddings_path: str = None) -> int:
        """
        Enhanced Parquet import using DuckDB's native Parquet support.
        
        Args:
            parquet_path: Path to main Parquet file
            embeddings_path: Optional path to embeddings Parquet file
            
        Returns:
            Number of entities imported
        """
        if not self.use_duckdb:
            logger.warning("DuckDB not available, falling back to pandas import")
            return self.import_pdb_data(parquet_path)
            
        conn = self.get_connection()
        
        # Use DuckDB's efficient Parquet reading
        try:
            # Read entities directly from Parquet
            result = conn.execute(f"""
                SELECT 
                    COALESCE(pid, row_number() OVER ()) as external_id,
                    name,
                    COALESCE(cid, '') as cid,
                    COALESCE(mbti, '') as mbti_type,
                    COALESCE(socionics, '') as socionics_type,
                    COALESCE(big5, '') as big5_type,
                    COALESCE(enneagram, '') as enneagram_type
                FROM read_parquet('{parquet_path}')
                WHERE name IS NOT NULL AND name != ''
            """).fetchall()
            
            imported_count = 0
            
            # Create data source record
            data_source_id = str(uuid.uuid4())
            conn.execute("""
                INSERT OR REPLACE INTO data_sources (id, name, source_type, file_path, description)
                VALUES (?, ?, ?, ?, ?)
            """, (data_source_id, "DuckDB Parquet Import", "parquet", parquet_path,
                  "Imported via DuckDB native Parquet reader"))
            
            for row in result:
                entity_id = str(uuid.uuid4())
                
                # Create entity
                conn.execute("""
                    INSERT OR IGNORE INTO entities (id, name, entity_type, source, external_id, external_source)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (entity_id, row[1], 'fictional_character', 'parquet_import', 
                      str(row[0]) if row[0] else None, 'duckdb_parquet'))
                
                # Process personality typings
                systems_data = [
                    ('mbti', row[3]),
                    ('socionics', row[4]), 
                    ('big5', row[5]),
                    ('enneagram', row[6])
                ]
                
                for system_name, type_value in systems_data:
                    if type_value and str(type_value).strip():
                        self._create_typing_judgment(conn, entity_id, system_name, 
                                                   str(type_value).strip(), 'duckdb_import')
                
                imported_count += 1
                
                if imported_count % 500 == 0:
                    logger.info(f"DuckDB imported {imported_count} entities...")
            
            # Import embeddings if available
            if embeddings_path and HNSWLIB_AVAILABLE:
                self._import_embeddings_from_parquet(conn, embeddings_path)
            
            conn.commit()
            logger.info(f"DuckDB successfully imported {imported_count} entities")
            return imported_count
            
        except Exception as e:
            conn.rollback()
            logger.error(f"DuckDB Parquet import failed: {e}")
            raise
    
    def _create_typing_judgment(self, conn, entity_id: str, system_name: str, 
                               type_value: str, method: str):
        """Helper to create typing judgment records."""
        # Get or create system
        result = conn.execute("SELECT id FROM personality_systems WHERE name = ?", 
                            [system_name]).fetchone()
        if result:
            system_id = result[0]
        else:
            conn.execute("""
                INSERT INTO personality_systems (name, display_name, description)
                VALUES (?, ?, ?)
            """, [system_name, system_name.upper(), f"Auto-created for {system_name} types"])
            system_id = conn.lastrowid
        
        # Get or create type
        result = conn.execute("""
            SELECT id FROM personality_types WHERE system_id = ? AND code = ?
        """, [system_id, type_value]).fetchone()
        
        if result:
            type_id = result[0]
        else:
            conn.execute("""
                INSERT INTO personality_types (system_id, code, name)
                VALUES (?, ?, ?)
            """, [system_id, type_value, type_value])
            type_id = conn.lastrowid
        
        # Create judgment
        judgment_id = str(uuid.uuid4())
        conn.execute("""
            INSERT OR IGNORE INTO typing_judgments 
            (id, entity_id, rater_id, system_id, type_id, confidence, method)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [judgment_id, entity_id, 'system', system_id, type_id, 1.0, method])
    
    def _import_embeddings_from_parquet(self, conn, embeddings_path: str):
        """Import vector embeddings from Parquet file."""
        try:
            # Read embeddings using DuckDB
            result = conn.execute(f"""
                SELECT cid, embedding
                FROM read_parquet('{embeddings_path}')
                WHERE embedding IS NOT NULL
            """).fetchall()
            
            # Map CID to entity_id and add embeddings
            for cid, embedding_blob in result:
                # Find entity by CID mapping
                entity_result = conn.execute("""
                    SELECT em.entity_id 
                    FROM entity_data_mappings em
                    WHERE em.external_key = 'cid' AND em.external_value = ?
                """, [str(cid)]).fetchone()
                
                if entity_result:
                    entity_id = entity_result[0]
                    # Convert blob to numpy array (assuming it's serialized properly)
                    try:
                        if isinstance(embedding_blob, (bytes, bytearray)):
                            embedding = np.frombuffer(embedding_blob, dtype=np.float32)
                        else:
                            # Handle other formats as needed
                            continue
                            
                        if len(embedding) == self.vector_dimension:
                            self.add_entity_embedding(entity_id, embedding)
                        else:
                            logger.warning(f"Embedding dimension mismatch for CID {cid}")
                    except Exception as e:
                        logger.warning(f"Failed to process embedding for CID {cid}: {e}")
            
            logger.info(f"Imported {len(self.entity_embeddings)} embeddings")
            
        except Exception as e:
            logger.error(f"Failed to import embeddings from Parquet: {e}")
    
    def _create_entity_simple(self, name: str, description: str, entity_type: EntityType) -> str:
        """Simple entity creation for testing."""
        entity_id = str(uuid.uuid4())
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO entities (id, name, description, entity_type, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (entity_id, name, description, entity_type.value, datetime.now().isoformat()))
            conn.commit()
            return entity_id
        except Exception as e:
            conn.rollback()
            raise e
    
    def close(self):
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None


# Example usage and testing functions
def example_usage():
    """Example of how to use the IPDBManager."""
    
    # Initialize database
    db = IPDBManager("/tmp/socionics_research_example.db")
    db.initialize_database()
    
    # Create a test user
    user = db.create_user("test_rater", "test@example.com", UserRole.PANEL_RATER, ExperienceLevel.INTERMEDIATE)
    print(f"Created user: {user.username} with ID: {user.id}")
    
    # Import PDB data (uncomment when you have the files)
    # db.import_pdb_data("data/bot_store/pdb_profiles_normalized.parquet")
    
    # Get entities
    entities = db.get_entities(limit=10)
    print(f"Found {len(entities)} entities")
    for entity in entities[:3]:
        print(f"- {entity['name']} ({entity['entity_type']})")
    
    # Get typing summary for first entity if available
    if entities:
        entity_id = entities[0]['id']
        typing_summary = db.get_typing_summary(entity_id)
        print(f"Typing summary for {entities[0]['name']}:")
        for summary in typing_summary:
            print(f"- {summary['system_display']}: {summary['type_code']} (avg confidence: {summary['avg_confidence']:.2f})")
    
    db.close()
    print("Example completed successfully!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    example_usage()