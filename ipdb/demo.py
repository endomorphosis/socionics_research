#!/usr/bin/env python3
"""
IPDB Database Demo Script
========================

This script demonstrates how to use the IPDB database schema with real PDB data
to support user ratings and personality typings for socionics research.

Usage:
    python3 demo.py
"""

import os
import sys
import logging
from pathlib import Path

# Add the ipdb module to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database_manager import IPDBManager, UserRole, ExperienceLevel

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Main demo function."""
    logger.info("=" * 60)
    logger.info("IPDB Database Schema Demo")
    logger.info("Socionics Research User Rating & Typing System")
    logger.info("=" * 60)
    
    # Look for PDB data
    data_dir = Path(__file__).parent.parent / "data" / "bot_store"
    normalized_file = data_dir / "pdb_profiles_normalized.parquet"
    
    if not normalized_file.exists():
        logger.error(f"PDB data file not found at: {normalized_file}")
        logger.info("Please ensure the PDB data files are available in data/bot_store/")
        return
    
    # Initialize database
    db_path = "/tmp/socionics_demo.db"
    if os.path.exists(db_path):
        os.remove(db_path)  # Start fresh
    
    logger.info(f"Initializing database at: {db_path}")
    db = IPDBManager(db_path)
    db.initialize_database()
    
    try:
        # Import PDB data
        logger.info("Importing PDB data...")
        db.import_pdb_data(str(normalized_file))
        logger.info("✓ PDB data imported successfully")
        
        # Create research team users
        logger.info("\nCreating research team users...")
        
        admin = db.create_user("research_admin", "admin@socionics.research", 
                              UserRole.ADMIN, ExperienceLevel.EXPERT)
        logger.info(f"✓ Created admin: {admin.username}")
        
        expert_rater = db.create_user("expert_rater1", "expert@socionics.research", 
                                     UserRole.PANEL_RATER, ExperienceLevel.EXPERT)
        logger.info(f"✓ Created expert rater: {expert_rater.username}")
        
        intermediate_rater = db.create_user("intermediate_rater1", "rater@socionics.research", 
                                           UserRole.PANEL_RATER, ExperienceLevel.INTERMEDIATE)
        logger.info(f"✓ Created intermediate rater: {intermediate_rater.username}")
        
        annotator = db.create_user("annotator1", "annotator@socionics.research", 
                                  UserRole.ANNOTATOR, ExperienceLevel.INTERMEDIATE)
        logger.info(f"✓ Created annotator: {annotator.username}")
        
        # Create rating sessions
        logger.info("\nCreating rating sessions...")
        
        fictional_session = db.create_rating_session(
            name="Fictional Characters Panel Rating - Batch 1",
            description="Panel rating of fictional characters from literature and media",
            methodology="composite_review",
            session_type="panel",
            created_by=admin.id
        )
        logger.info(f"✓ Created fictional characters session")
        
        consensus_session = db.create_rating_session(
            name="Consensus Session - High Disagreement Cases",
            description="Consensus meeting for entities with significant rater disagreement",
            methodology="structured_interview",
            session_type="consensus", 
            created_by=admin.id
        )
        logger.info(f"✓ Created consensus session")
        
        # Show available entities
        logger.info("\nAvailable entities for rating:")
        entities = db.get_entities(limit=10)
        
        for i, entity in enumerate(entities, 1):
            logger.info(f"  {i:2d}. {entity['name'] or 'Unnamed'} ({entity['entity_type']})")
            
            # Show existing typings
            summary = db.get_typing_summary(entity['id'])
            if summary:
                for s in summary:
                    logger.info(f"      {s['system_display']}: {s['type_code']} "
                               f"(confidence: {s['avg_confidence']:.2f})")
            else:
                logger.info("      No current typings")
        
        # Show database statistics
        logger.info(f"\nDatabase Statistics:")
        
        conn = db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM entities")
        entity_count = cursor.fetchone()[0]
        logger.info(f"  Total entities: {entity_count}")
        
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        logger.info(f"  Total users: {user_count}")
        
        cursor.execute("SELECT COUNT(*) FROM typing_judgments")
        judgment_count = cursor.fetchone()[0]
        logger.info(f"  Total typing judgments: {judgment_count}")
        
        cursor.execute("SELECT COUNT(*) FROM rating_sessions")
        session_count = cursor.fetchone()[0]
        logger.info(f"  Total rating sessions: {session_count}")
        
        # Show personality systems and types
        cursor.execute("""
            SELECT ps.display_name, COUNT(pt.id) as type_count
            FROM personality_systems ps
            LEFT JOIN personality_types pt ON ps.id = pt.system_id
            GROUP BY ps.id, ps.display_name
        """)
        
        logger.info(f"\nSupported personality systems:")
        systems = cursor.fetchall()
        for system in systems:
            logger.info(f"  {system[0]}: {system[1]} types")
        
        # Show entities by typing system coverage
        logger.info(f"\nEntity coverage by typing system:")
        cursor.execute("""
            SELECT ps.display_name, COUNT(DISTINCT tj.entity_id) as entities_with_typing
            FROM personality_systems ps
            LEFT JOIN typing_judgments tj ON ps.id = tj.system_id
            WHERE tj.type_id IS NOT NULL
            GROUP BY ps.id, ps.display_name
            ORDER BY entities_with_typing DESC
        """)
        
        coverage = cursor.fetchall()
        for system_coverage in coverage:
            percentage = (system_coverage[1] / entity_count) * 100 if entity_count > 0 else 0
            logger.info(f"  {system_coverage[0]}: {system_coverage[1]} entities ({percentage:.1f}%)")
        
        # Sample queries for research
        logger.info(f"\nSample research queries:")
        
        # Entities needing more ratings
        cursor.execute("""
            SELECT e.name, COUNT(tj.id) as rating_count
            FROM entities e
            LEFT JOIN typing_judgments tj ON e.id = tj.entity_id
            WHERE e.name IS NOT NULL AND e.name != ''
            GROUP BY e.id, e.name
            HAVING rating_count < 3
            ORDER BY rating_count, e.name
            LIMIT 5
        """)
        
        logger.info("  Top 5 entities needing more ratings:")
        low_ratings = cursor.fetchall()
        for entity in low_ratings:
            name = entity[0] or "Unnamed"
            logger.info(f"    {name}: {entity[1]} ratings")
        
        # Most typed entities
        cursor.execute("""
            SELECT e.name, COUNT(tj.id) as rating_count
            FROM entities e
            JOIN typing_judgments tj ON e.id = tj.entity_id
            WHERE e.name IS NOT NULL AND e.name != ''
            GROUP BY e.id, e.name
            ORDER BY rating_count DESC
            LIMIT 5
        """)
        
        logger.info("  Top 5 most-rated entities:")
        top_ratings = cursor.fetchall()
        for entity in top_ratings:
            name = entity[0] or "Unnamed"
            logger.info(f"    {name}: {entity[1]} ratings")
        
        logger.info(f"\n" + "=" * 60)
        logger.info("Demo completed successfully!")
        logger.info(f"Database file saved at: {db_path}")
        logger.info("You can now use this database for:")
        logger.info("  • Adding new rating sessions")
        logger.info("  • Collecting user typings and ratings")
        logger.info("  • Calculating inter-rater reliability")
        logger.info("  • Managing consensus processes")
        logger.info("  • Running statistical analyses")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    main()