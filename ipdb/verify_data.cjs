#!/usr/bin/env node

/**
 * Verify Data Ingestion Script
 * ============================
 * 
 * Quick verification that the parquet data was successfully ingested
 */

const IPDBManager = require('./database-manager.cjs');

class DataVerifier {
    constructor() {
        this.dbManager = new IPDBManager();
    }

    async verify() {
        console.log('🔍 Verifying data ingestion...');
        
        try {
            await this.dbManager.initialize();
            
            // Get statistics
            const stats = await this.dbManager.getStats();
            console.log('\n📊 Database Statistics:');
            console.log(`   Entities: ${stats.entities}`);
            console.log(`   Users: ${stats.users}`);
            console.log(`   Personality Types: ${stats.personality_types}`);
            console.log(`   Ratings: ${stats.ratings}`);
            console.log(`   Comments: ${stats.comments}`);

            // Get some sample entities
            console.log('\n🎭 Sample Characters from PDB Import:');
            const entities = await this.dbManager.getAllEntities(10);
            
            let importedCount = 0;
            for (const entity of entities) {
                if (entity.source === 'pdb_import') {
                    importedCount++;
                    console.log(`   • ${entity.name} (${entity.category || 'Unknown Category'})`);
                    
                    // Show metadata if available
                    if (entity.metadata) {
                        try {
                            const metadata = JSON.parse(entity.metadata);
                            console.log(`     - Category: ${metadata.category}`);
                            console.log(`     - Votes: ${metadata.vote_count}`);
                            console.log(`     - Comments: ${metadata.comment_count}`);
                        } catch (e) {
                            // Skip malformed metadata
                        }
                    }
                }
            }
            
            console.log(`\n✅ Found ${importedCount} imported characters in sample`);
            
            // Check for personality types
            console.log('\n🧠 Available Personality Types:');
            const personalityTypes = await this.dbManager.allQuery('SELECT system, code, name FROM personality_types LIMIT 10');
            for (const type of personalityTypes) {
                console.log(`   • ${type.system}: ${type.code} - ${type.name || 'No name'}`);
            }
            
            console.log('\n🎉 Data verification completed!');
            
        } catch (error) {
            console.error('❌ Verification failed:', error);
        } finally {
            this.dbManager.close();
        }
    }
}

// Run verification
const verifier = new DataVerifier();
verifier.verify();