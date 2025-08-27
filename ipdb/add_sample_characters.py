#!/usr/bin/env python3
"""
Add sample characters to IPDB for testing
"""

import sys
import os

# Add the ipdb directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database_manager import IPDBManager, EntityType
import json
import uuid

def add_sample_characters():
    """Add a variety of sample characters from different media"""
    db = IPDBManager("/tmp/socionics_demo.db")
    
    sample_characters = [
        {
            "id": str(uuid.uuid4()),
            "name": "Sherlock Holmes",
            "category": "book",
            "source": "Sherlock Holmes series",
            "description": "A brilliant detective known for his logical reasoning and deductive abilities",
            "personality_types": ["LII", "INTJ"],
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Tony Stark",
            "category": "movie", 
            "source": "Marvel Cinematic Universe",
            "description": "Genius inventor and Iron Man, confident and innovative",
            "personality_types": ["LIE", "ENTP"],
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Hermione Granger",
            "category": "book",
            "source": "Harry Potter series",
            "description": "Brilliant witch, studious and rule-following, loyal friend",
            "personality_types": ["LSI", "ESTJ"],
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Tyrion Lannister",
            "category": "tv",
            "source": "Game of Thrones",
            "description": "Clever and witty, strategic thinker with a sharp tongue",
            "personality_types": ["ILE", "ENTP"],
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Naruto Uzumaki",
            "category": "anime",
            "source": "Naruto",
            "description": "Energetic ninja with strong willpower and loyalty to friends",
            "personality_types": ["ESE", "ENFP"],
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Batman",
            "category": "comic",
            "source": "DC Comics",
            "description": "Dark knight detective, strategic and determined to fight crime",
            "personality_types": ["LSI", "INTJ"],
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Gandalf",
            "category": "book",
            "source": "Lord of the Rings",
            "description": "Wise wizard and mentor, patient and insightful",
            "personality_types": ["EII", "INFJ"],
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Princess Leia",
            "category": "movie",
            "source": "Star Wars",
            "description": "Strong leader and diplomat, brave and determined",
            "personality_types": ["LSE", "ESTJ"],
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Link",
            "category": "game",
            "source": "The Legend of Zelda",
            "description": "Silent hero with strong sense of justice and adventure",
            "personality_types": ["SLE", "ISTP"],
        },
        {
            "id": str(uuid.uuid4()),
            "name": "The Joker",
            "category": "comic",
            "source": "DC Comics",
            "description": "Chaotic villain with unpredictable behavior and dark humor",
            "personality_types": ["IEE", "ENTP"],
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Daenerys Targaryen",
            "category": "tv",
            "source": "Game of Thrones", 
            "description": "Dragon queen with strong convictions and idealistic goals",
            "personality_types": ["EIE", "ENFJ"],
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Goku",
            "category": "anime",
            "source": "Dragon Ball",
            "description": "Pure-hearted warrior who loves fighting and protecting others",
            "personality_types": ["SEE", "ESFP"],
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Aragorn",
            "category": "movie",
            "source": "Lord of the Rings",
            "description": "Reluctant king with natural leadership and combat skills",
            "personality_types": ["LSE", "ISTJ"],
        },
        {
            "id": str(uuid.uuid4()),
            "name": "L",
            "category": "anime",
            "source": "Death Note",
            "description": "Eccentric detective with brilliant analytical abilities",
            "personality_types": ["LII", "INTP"],
        },
        {
            "id": str(uuid.uuid4()),
            "name": "Wonder Woman",
            "category": "comic",
            "source": "DC Comics",
            "description": "Amazonian warrior with compassion and strong moral convictions",
            "personality_types": ["ESE", "ENFJ"],
        },
    ]
    
    for character in sample_characters:
        try:
            # Add entity directly to database
            conn = db.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO entities (id, name, description, entity_type, source, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                character['id'],
                character['name'],
                character['description'],
                'fictional_character',  # Use string directly
                character['source'],
                json.dumps({'personality_types': character['personality_types'], 'category': character['category']})
            ))
            conn.commit()
            
            print(f"✓ Added character: {character['name']}")
        except Exception as e:
            print(f"✗ Failed to add {character['name']}: {e}")
    
    # Add some sample ratings and comments
    print("\nAdding sample ratings and comments...")
    
    entities = db.get_all_entities()
    users = ["expert_rater1", "intermediate_rater1", "annotator1"]
    
    for entity in entities[:5]:  # Rate first 5 characters
        for i, user in enumerate(users):
            rating_data = {
                "id": str(uuid.uuid4()),
                "entity_id": entity["id"],
                "user": user,
                "personality_system": "socionics",
                "personality_type": "ILE",  # Default type
                "confidence": 0.7 + (i * 0.1),  # Vary confidence levels
                "reasoning": f"Based on analysis of character traits and behavior patterns by {user}",
                "created_at": db.get_current_timestamp()
            }
            db.add_rating(rating_data)
            
        # Add a comment
        comment_data = {
            "id": str(uuid.uuid4()),
            "entity_id": entity["id"],
            "user": users[0],
            "content": f"This character shows interesting personality patterns that deserve further analysis.",
            "created_at": db.get_current_timestamp()
        }
        db.add_comment(comment_data)
        
        print(f"✓ Added ratings and comment for: {entity['name']}")
    
    db.close()
    print("\n🎉 Sample characters, ratings, and comments added successfully!")
    print(f"Database now contains {len(sample_characters)} characters ready for testing.")

if __name__ == "__main__":
    add_sample_characters()