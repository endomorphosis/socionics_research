"""
IPDB Database Manager for Socionics Research
===========================================

This module provides database management and ORM-like functionality for the 
Socionics Research personality typing and rating system.

Main features:
- Database schema management
- Data migration from existing Parquet files  
- User and entity management
- Typing session management
- Inter-rater reliability calculations
- Consensus tracking

Usage:
    from ipdb.database_manager import IPDBManager
    
    db = IPDBManager(connection_string="mysql://user:pass@localhost/socionics_research")
    db.initialize_database()
    db.import_pdb_data("data/bot_store/pdb_profiles_normalized.parquet")
"""

import uuid
import json
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import pandas as pd
import sqlite3
import logging

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
    """Database manager for the Socionics Research IPDB system."""
    
    def __init__(self, db_path: str = None, connection_string: str = None):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to SQLite database file (for SQLite)
            connection_string: Full database connection string (for other DBs)
        """
        self.db_path = db_path or "/tmp/socionics_research.db"
        self.connection_string = connection_string
        self._connection = None
        
    def get_connection(self):
        """Get database connection."""
        if self._connection is None:
            if self.connection_string:
                # For future MySQL/PostgreSQL support
                raise NotImplementedError("Full database connection strings not yet supported")
            else:
                # Use SQLite for now
                self._connection = sqlite3.connect(self.db_path)
                self._connection.row_factory = sqlite3.Row  # Enable dict-like access
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
                      str(row.get('pid', '')) if pd.notna(row.get('pid')) else None, 'personality_database'))
                
                # Create mapping to CID
                if pd.notna(row.get('cid')):
                    mapping_id = str(uuid.uuid4())
                    cursor.execute("""
                        INSERT OR IGNORE INTO entity_data_mappings (id, entity_id, data_source_id, external_key, external_value)
                        VALUES (?, ?, ?, ?, ?)
                    """, (mapping_id, entity_id, data_source_id, 'cid', row['cid']))
                
                # Create typing judgments for available systems
                systems = ['mbti', 'socionics', 'big5']
                for system in systems:
                    if pd.notna(row.get(system)) and row.get(system):
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