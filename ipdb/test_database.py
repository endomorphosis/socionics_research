#!/usr/bin/env python3
"""
Test script for IPDB Database Schema
===================================

This script tests the functionality of the IPDB database schema and manager.
It creates a test database, imports sample data, and validates the core functionality.
"""

import os
import sys
import logging
import tempfile
import pandas as pd
from pathlib import Path

# Add the ipdb module to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database_manager import IPDBManager, UserRole, ExperienceLevel

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_sample_parquet_data(temp_dir):
    """Create sample parquet data for testing."""
    
    # Create sample normalized profiles data
    sample_data = {
        'cid': [
            'QmZMY37vF57M7hTeeD557P4X8XVaGr2r5vMrZiu2o6BsYD',
            'QmV6AFq2Fyus1LJ2An6RdQ1rAWP3CXHM8DTDLSL5x5iNEm',
            'QmX1Y2Z3A4B5C6D7E8F9G0H1I2J3K4L5M6N7O8P9Q0R1S2',
            'QmA1B2C3D4E5F6G7H8I9J0K1L2M3N4O5P6Q7R8S9T0U1V2W',
            'QmTest1Test2Test3Test4Test5Test6Test7Test8Test9Test0'
        ],
        'name': [
            'Sherlock Holmes',
            'Dr. Watson', 
            'Hermione Granger',
            'Harry Potter',
            'Elizabeth Bennet'
        ],
        'mbti': [
            'INTJ',
            'ISFJ', 
            'INTP',
            'ISFP',
            'ENFJ'
        ],
        'socionics': [
            'LII',
            'ESI',
            'ILE', 
            'SEI',
            'EIE'
        ],
        'big5': [
            'RCOEI',
            'SCOEI',
            'RCUEI',
            'SCOAN',
            'SCOAI'
        ],
        'pid': [101, 102, 103, 104, 105],
        'has_vector': [True, True, True, True, False]
    }
    
    df = pd.DataFrame(sample_data)
    parquet_path = os.path.join(temp_dir, 'sample_pdb_profiles_normalized.parquet')
    df.to_parquet(parquet_path)
    
    return parquet_path


def test_database_initialization():
    """Test database schema initialization."""
    logger.info("Testing database initialization...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, 'test.db')
        db = IPDBManager(db_path)
        
        try:
            db.initialize_database()
            logger.info("✓ Database initialization successful")
            
            # Verify tables were created
            conn = db.get_connection()
            cursor = conn.cursor()
            
            # Check that essential tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            expected_tables = [
                'entities', 'users', 'personality_systems', 'personality_types',
                'rating_sessions', 'typing_judgments', 'data_sources'
            ]
            
            for table in expected_tables:
                if table in tables:
                    logger.info(f"✓ Table '{table}' created successfully")
                else:
                    logger.error(f"✗ Table '{table}' not found")
                    return False
            
            # Check that initial data was inserted
            cursor.execute("SELECT COUNT(*) FROM personality_systems")
            systems_count = cursor.fetchone()[0]
            logger.info(f"✓ Found {systems_count} personality systems")
            
            cursor.execute("SELECT COUNT(*) FROM personality_types")
            types_count = cursor.fetchone()[0]
            logger.info(f"✓ Found {types_count} personality types")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Database initialization failed: {e}")
            return False
        finally:
            db.close()


def test_user_management():
    """Test user creation and management."""
    logger.info("Testing user management...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, 'test.db')
        db = IPDBManager(db_path)
        
        try:
            db.initialize_database()
            
            # Create test users
            admin = db.create_user("admin", "admin@test.com", UserRole.ADMIN, ExperienceLevel.EXPERT)
            rater1 = db.create_user("rater1", "rater1@test.com", UserRole.PANEL_RATER, ExperienceLevel.INTERMEDIATE)
            annotator = db.create_user("annotator", "annotator@test.com", UserRole.ANNOTATOR, ExperienceLevel.NOVICE)
            
            logger.info(f"✓ Created admin user: {admin.username}")
            logger.info(f"✓ Created rater user: {rater1.username}")
            logger.info(f"✓ Created annotator user: {annotator.username}")
            
            # Verify users were created
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            user_count = cursor.fetchone()[0]
            
            if user_count == 3:
                logger.info(f"✓ All {user_count} users created successfully")
                return True
            else:
                logger.error(f"✗ Expected 3 users, found {user_count}")
                return False
                
        except Exception as e:
            logger.error(f"✗ User management test failed: {e}")
            return False
        finally:
            db.close()


def test_data_import():
    """Test importing data from parquet files."""
    logger.info("Testing data import from parquet files...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, 'test.db')
        parquet_path = create_sample_parquet_data(temp_dir)
        
        db = IPDBManager(db_path)
        
        try:
            db.initialize_database()
            
            # Import the sample data
            db.import_pdb_data(parquet_path)
            
            # Verify data was imported
            entities = db.get_entities(limit=10)
            logger.info(f"✓ Imported {len(entities)} entities")
            
            # Check specific entities
            expected_names = ['Sherlock Holmes', 'Dr. Watson', 'Hermione Granger', 'Harry Potter', 'Elizabeth Bennet']
            imported_names = [e['name'] for e in entities if e['name']]
            
            for name in expected_names:
                if name in imported_names:
                    logger.info(f"✓ Found imported entity: {name}")
                else:
                    logger.warning(f"⚠ Entity not found: {name}")
            
            # Check typing judgments were created
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM typing_judgments")
            judgment_count = cursor.fetchone()[0]
            logger.info(f"✓ Created {judgment_count} typing judgments")
            
            # Check data mappings
            cursor.execute("SELECT COUNT(*) FROM entity_data_mappings")
            mapping_count = cursor.fetchone()[0]
            logger.info(f"✓ Created {mapping_count} data mappings")
            
            return len(entities) > 0
            
        except Exception as e:
            logger.error(f"✗ Data import test failed: {e}")
            return False
        finally:
            db.close()


def test_rating_session():
    """Test rating session creation and management."""
    logger.info("Testing rating session management...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, 'test.db')
        parquet_path = create_sample_parquet_data(temp_dir)
        
        db = IPDBManager(db_path)
        
        try:
            db.initialize_database()
            
            # Create a user
            admin = db.create_user("admin", "admin@test.com", UserRole.ADMIN)
            
            # Import some entities
            db.import_pdb_data(parquet_path)
            
            # Create a rating session
            session_id = db.create_rating_session(
                name="Test Rating Session",
                description="A test session for validating functionality",
                methodology="composite_review",
                session_type="panel",
                created_by=admin.id
            )
            
            logger.info(f"✓ Created rating session: {session_id}")
            
            # Verify session was created
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT name, methodology, status FROM rating_sessions WHERE id = ?", (session_id,))
            session = cursor.fetchone()
            
            if session:
                logger.info(f"✓ Session details: {session[0]} ({session[1]}) - Status: {session[2]}")
                return True
            else:
                logger.error("✗ Session not found after creation")
                return False
                
        except Exception as e:
            logger.error(f"✗ Rating session test failed: {e}")
            return False
        finally:
            db.close()


def test_typing_summary():
    """Test typing summary functionality."""
    logger.info("Testing typing summary queries...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, 'test.db')
        parquet_path = create_sample_parquet_data(temp_dir)
        
        db = IPDBManager(db_path)
        
        try:
            db.initialize_database()
            db.import_pdb_data(parquet_path)
            
            # Get entities
            entities = db.get_entities(limit=5)
            
            if entities:
                entity_id = entities[0]['id']
                entity_name = entities[0]['name']
                
                # Get typing summary
                summary = db.get_typing_summary(entity_id)
                logger.info(f"✓ Retrieved typing summary for '{entity_name}':")
                
                for s in summary:
                    logger.info(f"  - {s['system_display']}: {s['type_code']} "
                               f"({s['judgment_count']} judgments, avg confidence: {s['avg_confidence']:.2f})")
                
                return len(summary) > 0
            else:
                logger.error("✗ No entities found for typing summary test")
                return False
                
        except Exception as e:
            logger.error(f"✗ Typing summary test failed: {e}")
            return False
        finally:
            db.close()


def run_all_tests():
    """Run all test functions."""
    logger.info("=" * 60)
    logger.info("IPDB Database Schema Test Suite")
    logger.info("=" * 60)
    
    tests = [
        ("Database Initialization", test_database_initialization),
        ("User Management", test_user_management),
        ("Data Import", test_data_import),
        ("Rating Session", test_rating_session),
        ("Typing Summary", test_typing_summary)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n--- Running {test_name} Test ---")
        try:
            results[test_name] = test_func()
        except Exception as e:
            logger.error(f"✗ Test '{test_name}' failed with exception: {e}")
            results[test_name] = False
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST RESULTS SUMMARY")
    logger.info("=" * 60)
    
    passed = 0
    total = len(tests)
    
    for test_name, result in results.items():
        status = "PASS" if result else "FAIL"
        logger.info(f"{test_name}: {status}")
        if result:
            passed += 1
    
    logger.info(f"\nPassed: {passed}/{total} tests")
    
    if passed == total:
        logger.info("🎉 All tests passed! Database schema is working correctly.")
        return True
    else:
        logger.warning(f"⚠ {total - passed} test(s) failed. Please check the issues above.")
        return False


def test_with_real_data():
    """Test with real PDB data if available."""
    logger.info("\n--- Testing with Real PDB Data (if available) ---")
    
    # Look for actual PDB data files
    data_dir = Path(__file__).parent.parent / "data" / "bot_store"
    normalized_file = data_dir / "pdb_profiles_normalized.parquet"
    
    if normalized_file.exists():
        logger.info(f"Found real PDB data at: {normalized_file}")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, 'real_data_test.db')
            db = IPDBManager(db_path)
            
            try:
                db.initialize_database()
                db.import_pdb_data(str(normalized_file))
                
                entities = db.get_entities(limit=10)
                logger.info(f"✓ Successfully imported {len(entities)} entities from real PDB data")
                
                # Show some examples
                for i, entity in enumerate(entities[:3], 1):
                    logger.info(f"  {i}. {entity['name']} ({entity['entity_type']})")
                    
                    summary = db.get_typing_summary(entity['id'])
                    for s in summary:
                        logger.info(f"     {s['system_display']}: {s['type_code']}")
                
                return True
                
            except Exception as e:
                logger.error(f"✗ Real data test failed: {e}")
                return False
            finally:
                db.close()
    else:
        logger.info("No real PDB data found, skipping real data test")
        return True


if __name__ == "__main__":
    success = run_all_tests()
    
    # Also test with real data if available
    real_data_success = test_with_real_data()
    
    if success and real_data_success:
        logger.info("\n🎉 All tests completed successfully!")
        sys.exit(0)
    else:
        logger.error("\n💥 Some tests failed!")
        sys.exit(1)