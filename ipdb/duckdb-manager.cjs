#!/usr/bin/env node

/**
 * IPDB DuckDB Manager for Socionics Research (Node.js)
 * ===================================================
 * 
 * Pure Node.js implementation of database management for the 
 * Socionics Research personality typing and rating system using DuckDB.
 * 
 * Main features:
 * - Database schema management (DuckDB)
 * - User and entity management  
 * - Typing session management
 * - Rating and comment management
 * - Collaborative character sheet functionality
 * - Search and taxonomy filtering
 */

// Simple UUID generator to avoid dependency
function generateUUID() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        const r = Math.random() * 16 | 0;
        const v = c === 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

function uuidv4() {
    return generateUUID();
}

// Try to load DuckDB, fallback to a simple file-based system if not available
let Database;
let dbAvailable = false;

try {
    Database = require('duckdb').Database;
    dbAvailable = true;
    console.log('✅ DuckDB available');
} catch (error) {
    console.log('⚠️  DuckDB not available, using fallback mode');
    // Simple fallback using file system for development
    const fs = require('fs');
    const path = require('path');
}

class IPDBDuckDBManager {
    constructor(dbPath = '/tmp/socionics_demo.duckdb') {
        this.dbPath = dbPath;
        this.db = null;
        this.fallbackData = {
            entities: [],
            users: [],
            ratings: [],
            comments: [],
            personality_types: []
        };
        this.fallbackPath = '/tmp/ipdb_fallback.json';
    }

    /**
     * Initialize database connection and create tables if needed
     */
    async initialize() {
        if (dbAvailable) {
            return this.initializeDuckDB();
        } else {
            return this.initializeFallback();
        }
    }

    async initializeDuckDB() {
        return new Promise((resolve, reject) => {
            this.db = new Database(this.dbPath, (err) => {
                if (err) {
                    reject(err);
                } else {
                    console.log(`Connected to DuckDB database: ${this.dbPath}`);
                    this.createTables()
                        .then(() => this.insertSampleData())
                        .then(() => resolve())
                        .catch(reject);
                }
            });
        });
    }

    async initializeFallback() {
        const fs = require('fs');
        try {
            if (fs.existsSync(this.fallbackPath)) {
                const data = fs.readFileSync(this.fallbackPath, 'utf8');
                this.fallbackData = JSON.parse(data);
            } else {
                await this.createFallbackData();
            }
            console.log(`Using fallback JSON database: ${this.fallbackPath}`);
            return Promise.resolve();
        } catch (error) {
            console.error('Fallback initialization error:', error);
            await this.createFallbackData();
            return Promise.resolve();
        }
    }

    async createFallbackData() {
        // Initialize with sample data structure
        this.fallbackData = {
            entities: [
                {
                    id: uuidv4(),
                    name: 'Naruto Uzumaki',
                    description: 'Energetic ninja with dreams of becoming Hokage',
                    category: 'anime',
                    source: 'Naruto',
                    personality_notes: 'Shows strong Ne-Fi characteristics with auxiliary Se.',
                    created_at: new Date().toISOString(),
                    updated_at: new Date().toISOString()
                },
                {
                    id: uuidv4(),
                    name: 'Sherlock Holmes',
                    description: 'Brilliant detective with exceptional deductive reasoning',
                    category: 'book',
                    source: 'Sherlock Holmes series',
                    personality_notes: 'Classic Ti-Ne user with strong Ni development.',
                    created_at: new Date().toISOString(),
                    updated_at: new Date().toISOString()
                }
            ],
            users: [
                {
                    id: 'demo-user',
                    username: 'demo',
                    display_name: 'Demo User',
                    role: 'annotator',
                    experience_level: 'novice',
                    created_at: new Date().toISOString()
                }
            ],
            ratings: [
                {
                    id: uuidv4(),
                    entity_id: null, // Will be set to first entity
                    user_id: 'demo-user',
                    personality_system: 'socionics',
                    personality_type: 'ILE',
                    confidence: 0.8,
                    reasoning: 'Sample rating',
                    created_at: new Date().toISOString()
                }
            ],
            comments: [],
            personality_types: [
                { id: uuidv4(), system: 'socionics', code: 'ILE', name: 'ILE (ENTp) - Inventor' },
                { id: uuidv4(), system: 'socionics', code: 'SEI', name: 'SEI (ISFp) - Mediator' },
                { id: uuidv4(), system: 'mbti', code: 'INTJ', name: 'INTJ - The Architect' },
                { id: uuidv4(), system: 'mbti', code: 'INTP', name: 'INTP - The Logician' }
            ]
        };

        // Set entity_id for sample rating
        if (this.fallbackData.entities.length > 0) {
            this.fallbackData.ratings[0].entity_id = this.fallbackData.entities[0].id;
        }

        await this.saveFallbackData();
    }

    async saveFallbackData() {
        const fs = require('fs');
        try {
            fs.writeFileSync(this.fallbackPath, JSON.stringify(this.fallbackData, null, 2));
        } catch (error) {
            console.error('Error saving fallback data:', error);
        }
    }

    /**
     * Create database tables for DuckDB
     */
    async createTables() {
        if (!dbAvailable) return;

        const schemas = [
            // Users table
            `CREATE TABLE IF NOT EXISTS users (
                id VARCHAR PRIMARY KEY,
                username VARCHAR UNIQUE NOT NULL,
                display_name VARCHAR,
                role VARCHAR DEFAULT 'annotator',
                experience_level VARCHAR DEFAULT 'novice',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Entities table (characters)
            `CREATE TABLE IF NOT EXISTS entities (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                description TEXT,
                entity_type VARCHAR DEFAULT 'fictional_character',
                category VARCHAR,
                source VARCHAR,
                external_id VARCHAR,
                external_source VARCHAR,
                metadata TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_edited_by VARCHAR,
                personality_notes TEXT,
                source_material TEXT
            )`,

            // Personality types reference table
            `CREATE TABLE IF NOT EXISTS personality_types (
                id VARCHAR PRIMARY KEY,
                system VARCHAR NOT NULL,
                code VARCHAR NOT NULL,
                name VARCHAR,
                description TEXT,
                UNIQUE(system, code)
            )`,

            // Entity personality type assignments
            `CREATE TABLE IF NOT EXISTS entity_personality_types (
                id VARCHAR PRIMARY KEY,
                entity_id VARCHAR NOT NULL,
                personality_type_id VARCHAR NOT NULL,
                confidence REAL DEFAULT 0.5,
                assigned_by VARCHAR,
                assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Ratings table
            `CREATE TABLE IF NOT EXISTS ratings (
                id VARCHAR PRIMARY KEY,
                entity_id VARCHAR NOT NULL,
                user_id VARCHAR NOT NULL,
                personality_system VARCHAR NOT NULL,
                personality_type VARCHAR NOT NULL,
                confidence REAL NOT NULL,
                reasoning TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Comments table
            `CREATE TABLE IF NOT EXISTS comments (
                id VARCHAR PRIMARY KEY,
                entity_id VARCHAR NOT NULL,
                user_id VARCHAR NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,

            // Character sheet edit history
            `CREATE TABLE IF NOT EXISTS edit_history (
                id VARCHAR PRIMARY KEY,
                entity_id VARCHAR NOT NULL,
                user_id VARCHAR NOT NULL,
                field_name VARCHAR NOT NULL,
                old_value TEXT,
                new_value TEXT,
                change_type VARCHAR DEFAULT 'update',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        if (!dbAvailable) return;

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
            // MBTI types
            { system: 'mbti', code: 'INTJ', name: 'INTJ - The Architect' },
            { system: 'mbti', code: 'INTP', name: 'INTP - The Logician' },
            { system: 'mbti', code: 'ENTJ', name: 'ENTJ - The Commander' },
            { system: 'mbti', code: 'ENTP', name: 'ENTP - The Debater' }
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
                personality_notes: 'Shows strong Ne-Fi characteristics with auxiliary Se.'
            },
            {
                name: 'Sherlock Holmes',
                description: 'Brilliant detective with exceptional deductive reasoning',
                category: 'book',
                source: 'Sherlock Holmes series',
                personality_notes: 'Classic Ti-Ne user with strong Ni development.'
            }
        ];

        for (const char of sampleCharacters) {
            const id = uuidv4();
            await this.runQuery(
                'INSERT INTO entities (id, name, description, category, source, personality_notes, last_edited_by) VALUES (?, ?, ?, ?, ?, ?, ?)',
                [id, char.name, char.description, char.category, char.source, char.personality_notes, 'system']
            );

            // Add sample rating
            const ratingId = uuidv4();
            await this.runQuery(
                'INSERT INTO ratings (id, entity_id, user_id, personality_system, personality_type, confidence, reasoning) VALUES (?, ?, ?, ?, ?, ?, ?)',
                [ratingId, id, 'demo-user', 'socionics', 'ILE', 0.8, `Sample rating for ${char.name}`]
            );
        }

        console.log(`Inserted ${sampleCharacters.length} sample characters with ratings`);
    }

    /**
     * Helper method to run a query (DuckDB or fallback)
     */
    runQuery(sql, params = []) {
        if (dbAvailable) {
            return new Promise((resolve, reject) => {
                this.db.run(sql, params, function(err) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve({ lastID: this.lastID, changes: this.changes });
                    }
                });
            });
        } else {
            // Fallback mode - simulate query execution
            return Promise.resolve({ lastID: null, changes: 0 });
        }
    }

    /**
     * Helper method to get a single row
     */
    getQuery(sql, params = []) {
        if (dbAvailable) {
            return new Promise((resolve, reject) => {
                this.db.get(sql, params, (err, row) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(row);
                    }
                });
            });
        } else {
            // Fallback mode - simulate query
            if (sql.includes('COUNT(*)')) {
                if (sql.includes('entities')) {
                    return Promise.resolve({ count: this.fallbackData.entities.length });
                } else if (sql.includes('ratings')) {
                    return Promise.resolve({ count: this.fallbackData.ratings.length });
                } else if (sql.includes('users')) {
                    return Promise.resolve({ count: this.fallbackData.users.length });
                }
            }
            return Promise.resolve(null);
        }
    }

    /**
     * Helper method to get all rows
     */
    allQuery(sql, params = []) {
        if (dbAvailable) {
            return new Promise((resolve, reject) => {
                this.db.all(sql, params, (err, rows) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(rows);
                    }
                });
            });
        } else {
            // Fallback mode - return appropriate data
            if (sql.includes('FROM entities')) {
                return Promise.resolve(this.fallbackData.entities.map(e => ({
                    ...e,
                    rating_count: this.fallbackData.ratings.filter(r => r.entity_id === e.id).length,
                    avg_confidence: 0.8,
                    personality_types: ['ILE'],
                    personality_type_details: [{ system: 'socionics', code: 'ILE', name: 'ILE (ENTp) - Inventor', confidence: 0.8 }]
                })));
            }
            return Promise.resolve([]);
        }
    }

    /**
     * Get all entities with optional filtering
     */
    async getAllEntities(limit = 50, offset = 0, filters = {}) {
        if (!dbAvailable) {
            const entities = this.fallbackData.entities.map(e => ({
                ...e,
                rating_count: this.fallbackData.ratings.filter(r => r.entity_id === e.id).length,
                avg_confidence: 0.8,
                personality_types: ['ILE'],
                personality_type_details: [{ system: 'socionics', code: 'ILE', name: 'ILE (ENTp) - Inventor', confidence: 0.8 }]
            }));
            
            // Apply filters
            let filtered = entities;
            if (filters.search) {
                const searchTerm = filters.search.toLowerCase();
                filtered = filtered.filter(e => 
                    e.name.toLowerCase().includes(searchTerm) ||
                    (e.description && e.description.toLowerCase().includes(searchTerm))
                );
            }
            if (filters.category) {
                filtered = filtered.filter(e => e.category === filters.category);
            }
            
            return filtered.slice(offset, offset + limit);
        }

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
     * Get database statistics - REAL DATA ONLY
     */
    async getStats() {
        if (!dbAvailable) {
            return {
                entities: this.fallbackData.entities.length,
                users: this.fallbackData.users.length,
                personality_types: this.fallbackData.personality_types.length,
                ratings: this.fallbackData.ratings.length,
                comments: this.fallbackData.comments.length
            };
        }

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
        if (!dbAvailable) {
            const entity = {
                id: entityData.id || uuidv4(),
                name: entityData.name,
                description: entityData.description,
                entity_type: entityData.entity_type,
                source: entityData.source,
                external_id: entityData.external_id,
                external_source: entityData.external_source,
                metadata: entityData.metadata,
                category: entityData.category || 'unknown',
                created_at: new Date().toISOString(),
                updated_at: new Date().toISOString()
            };
            this.fallbackData.entities.push(entity);
            await this.saveFallbackData();
            return { lastID: entity.id, changes: 1 };
        }

        const sql = `INSERT INTO entities (
            id, name, description, entity_type, source, external_id, external_source, metadata, category
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;
        
        return this.runQuery(sql, [
            entityData.id || uuidv4(),
            entityData.name,
            entityData.description,
            entityData.entity_type,
            entityData.source,
            entityData.external_id,
            entityData.external_source,
            entityData.metadata,
            entityData.category || 'unknown'
        ]);
    }

    /**
     * Create a rating (for data ingestion)
     */
    async createRating(ratingData) {
        if (!dbAvailable) {
            const rating = {
                id: ratingData.id || uuidv4(),
                entity_id: ratingData.entity_id,
                user_id: ratingData.rater_id,
                personality_system: ratingData.system_name || 'imported',
                personality_type: ratingData.type_code || 'unknown',
                confidence: ratingData.confidence || 0.5,
                reasoning: ratingData.rationale,
                created_at: new Date().toISOString()
            };
            this.fallbackData.ratings.push(rating);
            await this.saveFallbackData();
            return { lastID: rating.id, changes: 1 };
        }

        const sql = `INSERT INTO ratings (
            id, entity_id, user_id, personality_system, personality_type, confidence, reasoning
        ) VALUES (?, ?, ?, ?, ?, ?, ?)`;
        
        return this.runQuery(sql, [
            ratingData.id || uuidv4(),
            ratingData.entity_id,
            ratingData.rater_id,
            ratingData.system_name || 'imported',
            ratingData.type_code || 'unknown',
            ratingData.confidence || 0.5,
            ratingData.rationale
        ]);
    }

    /**
     * Close database connection
     */
    close() {
        if (this.db && dbAvailable) {
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

module.exports = IPDBDuckDBManager;