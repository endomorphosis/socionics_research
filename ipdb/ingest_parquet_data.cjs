#!/usr/bin/env node

/**
 * Parquet Data Ingestion Script
 * ============================
 * 
 * Ingests the existing parquet data from the old personality database wiki
 * into the new Wikia-style database schema.
 */

const fs = require('fs');
const path = require('path');
const IPDBDuckDBManager = require('./duckdb-manager.cjs');
const NodeJSParquetReader = require('./parquet_reader_nodejs.cjs');
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

class ParquetDataIngester {
    constructor() {
        this.dbManager = new IPDBDuckDBManager();
        this.parquetReader = new NodeJSParquetReader();
        this.stats = {
            totalProcessed: 0,
            entitiesCreated: 0,
            ratingsCreated: 0,
            errors: 0,
            skipped: 0
        };
        
        // Category mapping from PDB categories to our taxonomy
        this.categoryMapping = {
            'anime': 'Anime',
            'movies': 'Movies', 
            'tv shows': 'TV Shows',
            'books': 'Books',
            'games': 'Games',
            'comics': 'Comics',
            'general vloggers': 'Other',
            'youtubers': 'Other',
            'celebrities': 'Other',
            'politicians': 'Other',
            'historical figures': 'Other',
            'scientists': 'Other',
            'writers': 'Other',
            'default': 'Other'
        };
    }

    async initialize() {
        console.log('🚀 Initializing data ingestion...');
        await this.dbManager.initialize();
        console.log('✅ Database initialized');
    }

    /**
     * Read and parse the parquet data using pure Node.js
     */
    async readParquetData() {
        console.log('📂 Reading parquet data with Node.js...');
        
        const parquetPath = 'data/bot_store/pdb_profiles.parquet';
        
        try {
            const data = await this.parquetReader.readParquetFile(parquetPath);
            const readerStats = this.parquetReader.getStats();
            
            console.log(`✅ Successfully parsed ${data.length} records from parquet file`);
            console.log(`📊 Reader stats:`, readerStats);
            
            return data;
        } catch (error) {
            console.error('❌ Error reading parquet data:', error.message);
            throw error;
        }
    }

    /**
     * Normalize and clean character name
     */
    normalizeCharacterName(payload) {
        return payload.name || payload.title || payload.character_name || 'Unknown Character';
    }

    /**
     * Extract category from payload
     */
    extractCategory(payload) {
        const category = (payload.subcategory || payload.category || 'other').toLowerCase();
        
        // Map common categories
        for (const [key, value] of Object.entries(this.categoryMapping)) {
            if (category.includes(key)) {
                return value;
            }
        }
        
        return this.categoryMapping.default;
    }

    /**
     * Extract source information
     */
    extractSource(payload) {
        if (payload.subcategory) {
            return payload.subcategory;
        }
        if (payload.category) {
            return payload.category;
        }
        return 'Unknown Source';
    }

    /**
     * Process a single record and create entity + ratings
     */
    async processRecord(record) {
        try {
            const { cid, payload } = record;
            this.stats.totalProcessed++;

            // Skip records without essential data
            if (!payload || (!payload.name && !payload.title)) {
                this.stats.skipped++;
                return;
            }

            const name = this.normalizeCharacterName(payload);
            const category = this.extractCategory(payload);
            const source = this.extractSource(payload);
            const description = payload.description || payload.bio || '';

            // Create entity
            const entityId = uuidv4();
            const entityData = {
                id: entityId,
                name: name,
                description: description,
                entity_type: 'fictional_character',
                source: 'pdb_import',
                external_id: cid,
                external_source: 'personality_database',
                metadata: JSON.stringify({
                    category: category,
                    source: source,
                    vote_count: payload.vote_count || 0,
                    comment_count: payload.comment_count || 0,
                    original_payload_keys: Object.keys(payload)
                })
            };

            // Insert entity
            await this.dbManager.createEntity(entityData);
            this.stats.entitiesCreated++;

            // Create personality type ratings
            await this.createPersonalityRatings(entityId, payload);

            if (this.stats.totalProcessed % 100 === 0) {
                console.log(`📊 Processed ${this.stats.totalProcessed} records...`);
            }

        } catch (error) {
            console.error(`❌ Error processing record ${record.cid}:`, error.message);
            this.stats.errors++;
        }
    }

    /**
     * Create personality type ratings from the payload data
     */
    async createPersonalityRatings(entityId, payload) {
        const raterId = uuidv4(); // Create a system rater for imported data
        
        // Create MBTI rating
        if (payload.mbti) {
            await this.createRating(entityId, raterId, 'mbti', payload.mbti, payload);
        }

        // Create Socionics rating  
        if (payload.socionics) {
            await this.createRating(entityId, raterId, 'socionics', payload.socionics, payload);
        }

        // Create Enneagram rating
        if (payload.enneagram) {
            await this.createRating(entityId, raterId, 'enneagram', payload.enneagram, payload);
        }
    }

    /**
     * Create a single personality rating
     */
    async createRating(entityId, raterId, systemName, typeCode, payload) {
        try {
            // Get system info
            const systems = await this.dbManager.getPersonalitySystems();
            const system = systems.find(s => s.name === systemName);
            if (!system) {
                console.warn(`⚠️ Personality system '${systemName}' not found`);
                return;
            }

            // Get type info (check if type exists in our database)
            const types = await this.dbManager.getPersonalityTypes(system.id);
            const type = types.find(t => t.code === typeCode);
            if (!type) {
                console.warn(`⚠️ Personality type '${typeCode}' not found in system '${systemName}'`);
                return;
            }

            // Calculate confidence based on vote count
            const voteCount = payload.vote_count || 0;
            const confidence = Math.min(0.95, Math.max(0.3, voteCount / 100)); // Scale vote count to confidence

            const ratingData = {
                id: uuidv4(),
                entity_id: entityId,
                rater_id: raterId,
                system_name: systemName,
                type_code: typeCode,
                confidence: confidence,
                rationale: `Imported from PDB with ${voteCount} votes. ${payload.description || payload.bio || 'No description available'}`
            };

            await this.dbManager.createRating(ratingData);
            this.stats.ratingsCreated++;

        } catch (error) {
            console.error(`❌ Error creating ${systemName} rating:`, error.message);
        }
    }

    /**
     * Create a system user for imported ratings
     */
    async createSystemUser() {
        const userId = uuidv4();
        const userData = {
            id: userId,
            username: 'pdb_importer',
            display_name: 'PDB Import System',
            role: 'annotator',
            experience_level: 'expert',
            qualifications: JSON.stringify({
                source: 'automated_import',
                description: 'System user for importing PDB data'
            })
        };

        try {
            await this.dbManager.createUser(userData);
            console.log('✅ Created system user for imports');
            return userId;
        } catch (error) {
            // User might already exist
            console.log('ℹ️ System user already exists or error creating:', error.message);
            return userId;
        }
    }

    /**
     * Main ingestion process
     */
    async ingest() {
        console.log('🎯 Starting parquet data ingestion...');
        
        try {
            // Initialize database and create system user
            await this.initialize();
            await this.createSystemUser();

            // Read parquet data
            const records = await this.readParquetData();

            console.log(`📊 Starting to process ${records.length} records...`);

            // Process records in batches for better performance
            const batchSize = 50;
            for (let i = 0; i < records.length; i += batchSize) {
                const batch = records.slice(i, i + batchSize);
                
                console.log(`🔄 Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(records.length/batchSize)}...`);
                
                // Process batch
                await Promise.all(batch.map(record => this.processRecord(record)));
            }

            // Print final statistics
            console.log('\n🎉 Ingestion completed!');
            console.log('📈 Final Statistics:');
            console.log(`   Total Processed: ${this.stats.totalProcessed}`);
            console.log(`   Entities Created: ${this.stats.entitiesCreated}`);
            console.log(`   Ratings Created: ${this.stats.ratingsCreated}`);
            console.log(`   Errors: ${this.stats.errors}`);
            console.log(`   Skipped: ${this.stats.skipped}`);

        } catch (error) {
            console.error('❌ Ingestion failed:', error);
            throw error;
        }
    }
}

// Run ingestion if called directly
if (require.main === module) {
    const ingester = new ParquetDataIngester();
    
    ingester.ingest()
        .then(() => {
            console.log('✅ Data ingestion completed successfully!');
            process.exit(0);
        })
        .catch((error) => {
            console.error('❌ Data ingestion failed:', error);
            process.exit(1);
        });
}

module.exports = ParquetDataIngester;