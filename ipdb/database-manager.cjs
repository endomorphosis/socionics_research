#!/usr/bin/env node

/**
 * IPDB Database Manager for Socionics Research (Node.js)
 * ====================================================
 * 
 * Pure Node.js implementation of database management for the 
 * Socionics Research personality typing and rating system.
 * 
 * Main features:
 * - Database schema management (SQLite)
 * - User and entity management  
 * - Typing session management
 * - Rating and comment management
 * - Collaborative character sheet functionality
 * - Search and taxonomy filtering
 */

const sqlite3 = require('sqlite3').verbose();
const { v4: uuidv4 } = require('uuid');

class IPDBManager {
    constructor(dbPath = '/tmp/socionics_demo.db') {
        this.dbPath = dbPath;
        this.db = null;
    }

    /**
     * Initialize database connection and create tables if needed
     */
    async initialize() {
        return new Promise((resolve, reject) => {
            this.db = new sqlite3.Database(this.dbPath, (err) => {
                if (err) {
                    reject(err);
                } else {
                    console.log(`Connected to SQLite database: ${this.dbPath}`);
                    this.createTables()
                        .then(() => this.insertSampleData())
                        .then(() => resolve())
                        .catch(reject);
                }
            });
        });
    }

    /**
     * Create database tables
     */
    async createTables() {
        const schemas = [
            // Users table
            `CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                display_name TEXT,
                role TEXT DEFAULT 'annotator',
                experience_level TEXT DEFAULT 'novice',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )`,

            // Entities table (characters)
            `CREATE TABLE IF NOT EXISTS entities (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                entity_type TEXT DEFAULT 'fictional_character',
                category TEXT,
                source TEXT,
                external_id TEXT,
                external_source TEXT,
                metadata TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                last_edited_by TEXT,
                personality_notes TEXT,
                source_material TEXT
            )`,

            // Personality types reference table
            `CREATE TABLE IF NOT EXISTS personality_types (
                id TEXT PRIMARY KEY,
                system TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT,
                description TEXT,
                UNIQUE(system, code)
            )`,

            // Entity personality type assignments
            `CREATE TABLE IF NOT EXISTS entity_personality_types (
                id TEXT PRIMARY KEY,
                entity_id TEXT NOT NULL,
                personality_type_id TEXT NOT NULL,
                confidence REAL DEFAULT 0.5,
                assigned_by TEXT,
                assigned_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (entity_id) REFERENCES entities (id),
                FOREIGN KEY (personality_type_id) REFERENCES personality_types (id),
                FOREIGN KEY (assigned_by) REFERENCES users (id)
            )`,

            // Ratings table
            `CREATE TABLE IF NOT EXISTS ratings (
                id TEXT PRIMARY KEY,
                entity_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                personality_system TEXT NOT NULL,
                personality_type TEXT NOT NULL,
                confidence REAL NOT NULL,
                reasoning TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (entity_id) REFERENCES entities (id),
                FOREIGN KEY (user_id) REFERENCES users (id)
            )`,

            // Comments table
            `CREATE TABLE IF NOT EXISTS comments (
                id TEXT PRIMARY KEY,
                entity_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (entity_id) REFERENCES entities (id),
                FOREIGN KEY (user_id) REFERENCES users (id)
            )`,

            // Character sheet edit history
            `CREATE TABLE IF NOT EXISTS edit_history (
                id TEXT PRIMARY KEY,
                entity_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                field_name TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                change_type TEXT DEFAULT 'update',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (entity_id) REFERENCES entities (id),
                FOREIGN KEY (user_id) REFERENCES users (id)
            )`
        ];

        for (const schema of schemas) {
            await this.runQuery(schema);
        }

        // Create indexes for better performance
        const indexes = [
            'CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(name)',
            'CREATE INDEX IF NOT EXISTS idx_entities_category ON entities(category)',
            'CREATE INDEX IF NOT EXISTS idx_ratings_entity ON ratings(entity_id)',
            'CREATE INDEX IF NOT EXISTS idx_comments_entity ON comments(entity_id)',
            'CREATE INDEX IF NOT EXISTS idx_edit_history_entity ON edit_history(entity_id)'
        ];

        for (const index of indexes) {
            await this.runQuery(index);
        }
    }

    /**
     * Insert sample data for testing
     */
    async insertSampleData() {
        // Check if we already have data
        const entityCount = await this.getQuery('SELECT COUNT(*) as count FROM entities');
        if (entityCount.count > 0) {
            return; // Data already exists
        }

        // Insert personality types
        const personalityTypes = [
            // Socionics types
            { system: 'socionics', code: 'ILE', name: 'ILE (ENTp) - Inventor' },
            { system: 'socionics', code: 'SEI', name: 'SEI (ISFp) - Mediator' },
            { system: 'socionics', code: 'ESE', name: 'ESE (ESFj) - Enthusiast' },
            { system: 'socionics', code: 'LII', name: 'LII (INTj) - Analyst' },
            { system: 'socionics', code: 'SLE', name: 'SLE (ESTp) - Marshal' },
            { system: 'socionics', code: 'IEI', name: 'IEI (INFp) - Lyricist' },
            { system: 'socionics', code: 'EIE', name: 'EIE (ENFj) - Mentor' },
            { system: 'socionics', code: 'LSI', name: 'LSI (ISTj) - Inspector' },
            { system: 'socionics', code: 'SEE', name: 'SEE (ESFp) - Ambassador' },
            { system: 'socionics', code: 'ILI', name: 'ILI (INTp) - Critic' },
            { system: 'socionics', code: 'LIE', name: 'LIE (ENTj) - Entrepreneur' },
            { system: 'socionics', code: 'ESI', name: 'ESI (ISFj) - Guardian' },
            { system: 'socionics', code: 'IEE', name: 'IEE (ENFp) - Psychologist' },
            { system: 'socionics', code: 'SLI', name: 'SLI (ISTp) - Craftsman' },
            { system: 'socionics', code: 'LSE', name: 'LSE (ESTj) - Administrator' },
            { system: 'socionics', code: 'EII', name: 'EII (INFj) - Humanist' },
            
            // MBTI types
            { system: 'mbti', code: 'INTJ', name: 'INTJ - The Architect' },
            { system: 'mbti', code: 'INTP', name: 'INTP - The Logician' },
            { system: 'mbti', code: 'ENTJ', name: 'ENTJ - The Commander' },
            { system: 'mbti', code: 'ENTP', name: 'ENTP - The Debater' },
            { system: 'mbti', code: 'INFJ', name: 'INFJ - The Advocate' },
            { system: 'mbti', code: 'INFP', name: 'INFP - The Mediator' },
            { system: 'mbti', code: 'ENFJ', name: 'ENFJ - The Protagonist' },
            { system: 'mbti', code: 'ENFP', name: 'ENFP - The Campaigner' }
        ];

        for (const type of personalityTypes) {
            const id = uuidv4();
            await this.runQuery(
                'INSERT INTO personality_types (id, system, code, name) VALUES (?, ?, ?, ?)',
                [id, type.system, type.code, type.name]
            );
        }

        // Insert sample characters
        const sampleCharacters = [
            {
                name: 'Naruto Uzumaki',
                description: 'Energetic ninja with dreams of becoming Hokage',
                category: 'anime',
                source: 'Naruto',
                personality_notes: 'Shows strong Ne-Fi characteristics with auxiliary Se. Highly optimistic and people-focused.'
            },
            {
                name: 'Sherlock Holmes',
                description: 'Brilliant detective with exceptional deductive reasoning',
                category: 'book',
                source: 'Sherlock Holmes series',
                personality_notes: 'Classic Ti-Ne user with strong Ni development. Logical and analytical approach to problems.'
            },
            {
                name: 'Tony Stark',
                description: 'Genius inventor and Iron Man superhero',
                category: 'movie',
                source: 'Marvel Cinematic Universe',
                personality_notes: 'Strong Te-Ni with well-developed Se. Natural leader and innovator.'
            },
            {
                name: 'Hermione Granger',
                description: 'Brilliant witch and loyal friend',
                category: 'book',
                source: 'Harry Potter series',
                personality_notes: 'Clear Te-Si personality with strong moral compass and systematic approach.'
            },
            {
                name: 'Walter White',
                description: 'Chemistry teacher turned methamphetamine manufacturer',
                category: 'tv',
                source: 'Breaking Bad',
                personality_notes: 'Complex character showing Te-Fi with unhealthy development patterns.'
            },
            {
                name: 'Tyrion Lannister',
                description: 'Witty and intelligent dwarf lord',
                category: 'tv',
                source: 'Game of Thrones',
                personality_notes: 'Strong Ti-Ne with excellent social awareness and strategic thinking.'
            },
            {
                name: 'Master Chief',
                description: 'Supersoldier and hero of humanity',
                category: 'game',
                source: 'Halo series',
                personality_notes: 'Duty-focused Si-Te personality with strong moral convictions.'
            },
            {
                name: 'Batman',
                description: 'Dark knight and protector of Gotham City',
                category: 'comic',
                source: 'DC Comics',
                personality_notes: 'Strategic Ni-Te personality with strong Fi moral core.'
            },
            {
                name: 'Goku',
                description: 'Saiyan warrior with pure heart',
                category: 'anime',
                source: 'Dragon Ball',
                personality_notes: 'Clear Se-Fi with infectious optimism and love of fighting.'
            },
            {
                name: 'Elizabeth Bennet',
                description: 'Independent-minded woman in Georgian England',
                category: 'book',
                source: 'Pride and Prejudice',
                personality_notes: 'Strong Fi-Ne with keen social observations and principled stance.'
            },
            {
                name: 'Tyrael',
                description: 'Archangel of Justice',
                category: 'game',
                source: 'Diablo series',
                personality_notes: 'Principled Fi-Ni personality dedicated to justice and moral order.'
            },
            {
                name: 'Spider-Man',
                description: 'Web-slinging superhero with great responsibility',
                category: 'comic',
                source: 'Marvel Comics',
                personality_notes: 'Ne-Fi personality with strong sense of responsibility and witty humor.'
            },
            {
                name: 'Daenerys Targaryen',
                description: 'Dragon queen with noble intentions',
                category: 'tv',
                source: 'Game of Thrones',
                personality_notes: 'Ni-Fi personality with strong leadership drive and moral convictions.'
            },
            {
                name: 'Light Yagami',
                description: 'Brilliant student who becomes Kira',
                category: 'anime',
                source: 'Death Note',
                personality_notes: 'Te-Ni personality with unhealthy development and god complex.'
            },
            {
                name: 'Princess Leia',
                description: 'Rebel leader and Force-sensitive princess',
                category: 'movie',
                source: 'Star Wars',
                personality_notes: 'Strong Te-Fi leadership personality with diplomatic skills.'
            }
        ];

        for (const char of sampleCharacters) {
            const id = uuidv4();
            await this.runQuery(
                'INSERT INTO entities (id, name, description, category, source, personality_notes, last_edited_by) VALUES (?, ?, ?, ?, ?, ?, ?)',
                [id, char.name, char.description, char.category, char.source, char.personality_notes, 'system']
            );

            // Add some sample ratings
            for (let i = 0; i < Math.floor(Math.random() * 3) + 1; i++) {
                const ratingId = uuidv4();
                const userId = 'demo-user';
                const systems = ['socionics', 'mbti'];
                const system = systems[Math.floor(Math.random() * systems.length)];
                const types = system === 'socionics' ? 
                    ['ILE', 'SEI', 'ESE', 'LII', 'SLE', 'IEI', 'EIE', 'LSI'] :
                    ['INTJ', 'INTP', 'ENTJ', 'ENTP', 'INFJ', 'INFP', 'ENFJ', 'ENFP'];
                const type = types[Math.floor(Math.random() * types.length)];
                const confidence = Math.random() * 0.6 + 0.4; // 0.4 to 1.0

                await this.runQuery(
                    'INSERT INTO ratings (id, entity_id, user_id, personality_system, personality_type, confidence, reasoning) VALUES (?, ?, ?, ?, ?, ?, ?)',
                    [ratingId, id, userId, system, type, confidence, `Sample rating for ${char.name}`]
                );
            }
        }

        console.log(`Inserted ${sampleCharacters.length} sample characters with ratings`);
    }

    /**
     * Helper method to run a query
     */
    runQuery(sql, params = []) {
        return new Promise((resolve, reject) => {
            this.db.run(sql, params, function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve({ lastID: this.lastID, changes: this.changes });
                }
            });
        });
    }

    /**
     * Helper method to get a single row
     */
    getQuery(sql, params = []) {
        return new Promise((resolve, reject) => {
            this.db.get(sql, params, (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(row);
                }
            });
        });
    }

    /**
     * Helper method to get all rows
     */
    allQuery(sql, params = []) {
        return new Promise((resolve, reject) => {
            this.db.all(sql, params, (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows);
                }
            });
        });
    }

    /**
     * Get all entities with optional filtering
     */
    async getAllEntities(limit = 50, offset = 0, filters = {}) {
        let sql = `
            SELECT e.*, 
                   COUNT(r.id) as rating_count,
                   AVG(r.confidence) as avg_confidence
            FROM entities e
            LEFT JOIN ratings r ON e.id = r.entity_id
        `;
        
        const conditions = [];
        const params = [];
        
        if (filters.search) {
            conditions.push('(e.name LIKE ? OR e.description LIKE ? OR e.personality_notes LIKE ?)');
            const searchTerm = `%${filters.search}%`;
            params.push(searchTerm, searchTerm, searchTerm);
        }
        
        if (filters.category) {
            conditions.push('e.category = ?');
            params.push(filters.category);
        }
        
        if (conditions.length > 0) {
            sql += ' WHERE ' + conditions.join(' AND ');
        }
        
        sql += ' GROUP BY e.id';
        
        if (filters.sort) {
            switch (filters.sort) {
                case 'name':
                    sql += ' ORDER BY e.name ASC';
                    break;
                case 'name-desc':
                    sql += ' ORDER BY e.name DESC';
                    break;
                case 'category':
                    sql += ' ORDER BY e.category, e.name';
                    break;
                case 'ratings':
                    sql += ' ORDER BY rating_count DESC, e.name';
                    break;
                case 'recent':
                    sql += ' ORDER BY e.updated_at DESC';
                    break;
                default:
                    sql += ' ORDER BY e.name ASC';
            }
        } else {
            sql += ' ORDER BY e.name ASC';
        }
        
        sql += ' LIMIT ? OFFSET ?';
        params.push(limit, offset);
        
        const entities = await this.allQuery(sql, params);
        
        // Get personality types for each entity
        for (const entity of entities) {
            const types = await this.allQuery(
                `SELECT pt.system, pt.code, pt.name, ept.confidence 
                 FROM entity_personality_types ept
                 JOIN personality_types pt ON ept.personality_type_id = pt.id
                 WHERE ept.entity_id = ?`,
                [entity.id]
            );
            entity.personality_types = types.map(t => t.code);
            entity.personality_type_details = types;
        }
        
        return entities;
    }

    /**
     * Get a specific entity by ID
     */
    async getEntity(entityId) {
        const entity = await this.getQuery('SELECT * FROM entities WHERE id = ?', [entityId]);
        if (!entity) {
            return null;
        }
        
        // Get personality types
        const types = await this.allQuery(
            `SELECT pt.system, pt.code, pt.name, ept.confidence 
             FROM entity_personality_types ept
             JOIN personality_types pt ON ept.personality_type_id = pt.id
             WHERE ept.entity_id = ?`,
            [entityId]
        );
        entity.personality_types = types.map(t => t.code);
        entity.personality_type_details = types;
        
        return entity;
    }

    /**
     * Update entity (character sheet)
     */
    async updateEntity(entityId, updates, userId) {
        const currentEntity = await this.getEntity(entityId);
        if (!currentEntity) {
            throw new Error('Entity not found');
        }

        const allowedFields = ['name', 'description', 'category', 'source', 'personality_notes'];
        const updateFields = [];
        const params = [];
        
        for (const [field, value] of Object.entries(updates)) {
            if (allowedFields.includes(field) && value !== undefined) {
                updateFields.push(`${field} = ?`);
                params.push(value);
                
                // Record change in history
                await this.runQuery(
                    'INSERT INTO edit_history (id, entity_id, user_id, field_name, old_value, new_value) VALUES (?, ?, ?, ?, ?, ?)',
                    [uuidv4(), entityId, userId, field, currentEntity[field], value]
                );
            }
        }
        
        if (updateFields.length === 0) {
            return currentEntity;
        }
        
        updateFields.push('updated_at = CURRENT_TIMESTAMP');
        updateFields.push('last_edited_by = ?');
        params.push(userId);
        params.push(entityId);
        
        const sql = `UPDATE entities SET ${updateFields.join(', ')} WHERE id = ?`;
        await this.runQuery(sql, params);
        
        return await this.getEntity(entityId);
    }

    /**
     * Add user
     */
    async addUser(userData) {
        const id = userData.id || uuidv4();
        await this.runQuery(
            'INSERT OR REPLACE INTO users (id, username, display_name, role, experience_level) VALUES (?, ?, ?, ?, ?)',
            [id, userData.username, userData.display_name, userData.role, userData.experience_level]
        );
        return id;
    }

    /**
     * Add rating
     */
    async addRating(ratingData) {
        const id = ratingData.id || uuidv4();
        await this.runQuery(
            'INSERT INTO ratings (id, entity_id, user_id, personality_system, personality_type, confidence, reasoning) VALUES (?, ?, ?, ?, ?, ?, ?)',
            [id, ratingData.entity_id, ratingData.user, ratingData.personality_system, ratingData.personality_type, ratingData.confidence, ratingData.reasoning]
        );
        return id;
    }

    /**
     * Get ratings for entity
     */
    async getEntityRatings(entityId) {
        return await this.allQuery(
            'SELECT * FROM ratings WHERE entity_id = ? ORDER BY created_at DESC',
            [entityId]
        );
    }

    /**
     * Add comment
     */
    async addComment(commentData) {
        const id = commentData.id || uuidv4();
        await this.runQuery(
            'INSERT INTO comments (id, entity_id, user_id, content) VALUES (?, ?, ?, ?)',
            [id, commentData.entity_id, commentData.user, commentData.content]
        );
        return id;
    }

    /**
     * Get comments for entity
     */
    async getEntityComments(entityId) {
        return await this.allQuery(
            'SELECT c.*, u.display_name as user_name FROM comments c LEFT JOIN users u ON c.user_id = u.id WHERE c.entity_id = ? ORDER BY c.created_at DESC',
            [entityId]
        );
    }

    /**
     * Get edit history for entity
     */
    async getEntityHistory(entityId) {
        return await this.allQuery(
            'SELECT eh.*, u.display_name as user_name FROM edit_history eh LEFT JOIN users u ON eh.user_id = u.id WHERE eh.entity_id = ? ORDER BY eh.created_at DESC',
            [entityId]
        );
    }

    /**
     * Get database statistics
     */
    async getStats() {
        const stats = {};
        
        stats.entities = (await this.getQuery('SELECT COUNT(*) as count FROM entities')).count;
        stats.users = (await this.getQuery('SELECT COUNT(*) as count FROM users')).count;
        stats.personality_types = (await this.getQuery('SELECT COUNT(*) as count FROM personality_types')).count;
        stats.ratings = (await this.getQuery('SELECT COUNT(*) as count FROM ratings')).count;
        stats.comments = (await this.getQuery('SELECT COUNT(*) as count FROM comments')).count;
        
        return stats;
    }

    /**
     * Create a new entity (for data ingestion)
     */
    async createEntity(entityData) {
        const sql = `INSERT INTO entities (
            id, name, description, entity_type, source, external_id, external_source, metadata
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`;
        
        return this.runQuery(sql, [
            entityData.id,
            entityData.name,
            entityData.description,
            entityData.entity_type,
            entityData.source,
            entityData.external_id,
            entityData.external_source,
            entityData.metadata
        ]);
    }

    /**
     * Create a new user (for data ingestion)
     */
    async createUser(userData) {
        const sql = `INSERT INTO users (
            id, username, display_name, role, experience_level
        ) VALUES (?, ?, ?, ?, ?)`;
        
        return this.runQuery(sql, [
            userData.id,
            userData.username,
            userData.display_name,
            userData.role,
            userData.experience_level
        ]);
    }

    /**
     * Get personality systems (simplified)
     */
    async getPersonalitySystems() {
        // Return hardcoded systems since we don't have a systems table
        return [
            { id: 1, name: 'socionics' },
            { id: 2, name: 'mbti' },
            { id: 3, name: 'enneagram' }
        ];
    }

    /**
     * Get personality types for a system (simplified)
     */
    async getPersonalityTypes(systemId) {
        const systemName = systemId === 1 ? 'socionics' : systemId === 2 ? 'mbti' : 'enneagram';
        return this.allQuery('SELECT * FROM personality_types WHERE system = ?', [systemName]);
    }

    /**
     * Create a rating (for data ingestion)
     */
    async createRating(ratingData) {
        const sql = `INSERT INTO ratings (
            id, entity_id, user_id, personality_system, personality_type, confidence, reasoning
        ) VALUES (?, ?, ?, ?, ?, ?, ?)`;
        
        return this.runQuery(sql, [
            ratingData.id,
            ratingData.entity_id,
            ratingData.rater_id,
            ratingData.system_name || 'imported',
            ratingData.type_code || 'unknown',
            ratingData.confidence,
            ratingData.rationale
        ]);
    }

    /**
     * Close database connection
     */
    close() {
        if (this.db) {
            this.db.close();
            this.db = null;
        }
    }

    /**
     * Get current timestamp in ISO format
     */
    getCurrentTimestamp() {
        return new Date().toISOString();
    }
}

module.exports = IPDBManager;