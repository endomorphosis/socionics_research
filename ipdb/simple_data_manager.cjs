#!/usr/bin/env node

/**
 * Simple Data Manager - File-based approach for IPDB
 * ==================================================
 * 
 * Reads directly from parquet data without sqlite3 dependency
 * Uses Python bridge to process parquet files
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

// Simple UUID alternative
function generateId() {
    return Date.now().toString(36) + Math.random().toString(36).substr(2);
}

class SimpleDataManager {
    constructor() {
        this.dataPath = path.join(__dirname, '../data/bot_store/pdb_profiles.parquet');
        this.cacheFile = path.join(__dirname, '../data/bot_store/ipdb_cache.json');
        this.entities = [];
        this.stats = {
            entities: 0,
            ratings: 0,
            users: 0,
            comments: 0
        };
    }

    async initialize() {
        console.log('🚀 Initializing Simple Data Manager...');
        
        try {
            // Try to load cached data first
            if (fs.existsSync(this.cacheFile)) {
                console.log('📂 Loading cached data...');
                const cached = JSON.parse(fs.readFileSync(this.cacheFile, 'utf8'));
                this.entities = cached.entities || [];
                this.stats = cached.stats || this.stats;
                console.log(`✅ Loaded ${this.entities.length} cached entities`);
                return;
            }

            // Load from parquet if cache doesn't exist
            await this.loadFromParquet();
        } catch (error) {
            console.error('Error initializing data:', error);
            // Use sample data as fallback
            this.loadSampleData();
        }
    }

    async loadFromParquet() {
        console.log('📦 Loading data from parquet file...');
        
        if (!fs.existsSync(this.dataPath)) {
            console.warn('⚠️ Parquet file not found, using sample data');
            this.loadSampleData();
            return;
        }

        try {
            // Use the Python script to parse parquet data
            console.log('🐍 Using Python script to parse parquet data...');
            const result = execSync('python3 parse_parquet.py', { 
                encoding: 'utf8',
                cwd: __dirname,
                maxBuffer: 50 * 1024 * 1024  // 50MB buffer
            });
            
            const data = JSON.parse(result);
            this.entities = data.entities;
            this.stats = data.stats;
            
            // Cache the processed data
            fs.writeFileSync(this.cacheFile, JSON.stringify(data, null, 2));
            
            console.log(`✅ Loaded ${this.entities.length} REAL entities from parquet`);
            console.log(`📊 Real Stats: ${this.stats.entities} entities, ${this.stats.ratings} ratings, ${this.stats.users} users`);
            console.log('🚫 Zero placeholder data used');
            
        } catch (error) {
            console.error('Error processing parquet:', error);
            console.warn('⚠️ Falling back to sample data');
            this.loadSampleData();
        }
    }

    loadSampleData() {
        console.log('📝 Loading sample data as fallback...');
        
        this.entities = [
            {
                id: '1',
                name: 'Sherlock Holmes',
                description: 'Brilliant detective from Victorian London',
                category: 'Books',
                source: 'Sherlock Holmes Stories',
                mbti: 'INTJ',
                socionics: 'LII',
                enneagram: '5w6',
                big5: '',
                rating_count: 12,
                avg_confidence: 0.85
            },
            {
                id: '2', 
                name: 'Dr. Watson',
                description: 'Loyal companion and physician',
                category: 'Books',
                source: 'Sherlock Holmes Stories',
                mbti: 'ISFJ',
                socionics: 'ESI',
                enneagram: '6w5',
                big5: '',
                rating_count: 8,
                avg_confidence: 0.78
            }
        ];

        this.stats = {
            entities: this.entities.length,
            ratings: this.entities.length * 3,
            users: 5,
            comments: this.entities.length
        };
    }

    async getAllEntities(limit = 50, offset = 0, filters = {}) {
        let filteredEntities = [...this.entities];
        
        // Apply search filter
        if (filters.search) {
            const search = filters.search.toLowerCase();
            filteredEntities = filteredEntities.filter(entity => 
                entity.name.toLowerCase().includes(search) ||
                entity.description.toLowerCase().includes(search) ||
                entity.source.toLowerCase().includes(search)
            );
        }
        
        // Apply category filter
        if (filters.category) {
            filteredEntities = filteredEntities.filter(entity => 
                entity.category.toLowerCase() === filters.category.toLowerCase()
            );
        }
        
        // Apply sorting
        if (filters.sort) {
            switch (filters.sort) {
                case 'name':
                    filteredEntities.sort((a, b) => a.name.localeCompare(b.name));
                    break;
                case 'ratings':
                    filteredEntities.sort((a, b) => b.rating_count - a.rating_count);
                    break;
                case 'category':
                    filteredEntities.sort((a, b) => a.category.localeCompare(b.category) || a.name.localeCompare(b.name));
                    break;
            }
        }
        
        // Apply pagination
        return filteredEntities.slice(offset, offset + limit);
    }

    async getEntity(entityId) {
        return this.entities.find(e => e.id === entityId) || null;
    }

    async getStats() {
        return this.stats;
    }

    async createRating(data) {
        // In real implementation, would save to database
        return generateId();
    }

    async getRatings(entityId) {
        // Mock ratings
        return [];
    }

    async createComment(data) {
        // In real implementation, would save to database
        return generateId();
    }

    async getComments(entityId) {
        // Mock comments
        return [];
    }
}

module.exports = SimpleDataManager;