"""
IPDB - Integrated Personality Database for Socionics Research
============================================================

This package provides a comprehensive database schema and management system
for collecting, organizing, and analyzing personality type ratings and judgments
from multiple users and rating systems.

Key Components:
--------------
- database_schema.sql: Complete SQL schema definition
- database_manager.py: Python ORM-like interface and data management
- README.md: Comprehensive documentation
- test_database.py: Test suite for validation
- demo.py: Demonstration of functionality with real data

Main Features:
-------------
- Multi-system personality typing (Socionics, MBTI, Big Five, Enneagram)
- User management with role-based access control
- Rating session organization and workflow management
- Inter-rater reliability tracking and consensus processes
- Integration with existing Parquet data files from PDB
- Behavioral indicator annotation support
- Audit trails and data provenance tracking

Quick Start:
-----------
    from ipdb import IPDBManager, UserRole, ExperienceLevel
    
    # Initialize database
    db = IPDBManager("/path/to/database.db")
    db.initialize_database()
    
    # Import existing PDB data
    db.import_pdb_data("data/bot_store/pdb_profiles_normalized.parquet")
    
    # Create users and rating sessions
    admin = db.create_user("admin", role=UserRole.ADMIN)
    session_id = db.create_rating_session("Panel Rating #1", 
                                         methodology="composite_review",
                                         session_type="panel", 
                                         created_by=admin.id)

For more detailed examples, see demo.py and the documentation in README.md.
"""

__version__ = "1.0.0"
__author__ = "Socionics Research Team"

# Main exports
from .database_manager import (
    IPDBManager,
    Entity,
    User,
    TypingJudgment,
    EntityType,
    UserRole,
    ExperienceLevel
)

__all__ = [
    'IPDBManager',
    'Entity',
    'User', 
    'TypingJudgment',
    'EntityType',
    'UserRole',
    'ExperienceLevel'
]