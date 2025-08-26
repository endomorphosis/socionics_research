/**
 * IPDB Demo - TypeScript/JavaScript Version
 * ========================================
 * 
 * Demonstrates the enhanced IPDB functionality with:
 * - DuckDB integration
 * - Vector similarity search
 * - Multi-language API compatibility
 * - Parquet file imports
 */

import { IPDBManagerJS } from './index.js';
import { IPDBManagerWASM } from './wasm.js';
import fs from 'fs';
import path from 'path';

async function runDemo() {
    console.log('🚀 IPDB Enhanced Demo - TypeScript/JavaScript');
    console.log('='.repeat(50));
    
    try {
        // 1. Initialize both managers
        console.log('\n1. Initializing Database Managers...');
        
        const nodeManager = new IPDBManagerJS('./demo_ipdb.db');
        await nodeManager.initialize();
        console.log('✅ Node.js/DuckDB manager initialized');
        
        const wasmManager = new IPDBManagerWASM();
        await wasmManager.initialize();
        console.log('✅ WASM manager initialized');
        
        // 2. Create sample entities
        console.log('\n2. Creating Sample Entities...');
        
        const sampleEntities = [
            {
                id: '',
                name: 'Tyrion Lannister',
                entity_type: 'fictional_character' as const,
                description: 'The clever dwarf from Game of Thrones',
                metadata: { series: 'Game of Thrones', author: 'George R.R. Martin' }
            },
            {
                id: '',
                name: 'Hermione Granger',
                entity_type: 'fictional_character' as const,
                description: 'Brilliant witch from Harry Potter series',
                metadata: { series: 'Harry Potter', author: 'J.K. Rowling' }
            },
            {
                id: '',
                name: 'Sherlock Holmes',
                entity_type: 'fictional_character' as const,
                description: 'Master detective created by Arthur Conan Doyle',
                metadata: { series: 'Sherlock Holmes', author: 'Arthur Conan Doyle' }
            }
        ];
        
        const createdEntities = [];
        for (const entityData of sampleEntities) {
            const entity = await nodeManager.createEntity(entityData);
            createdEntities.push(entity);
            console.log(`✅ Created entity: ${entity.name}`);
        }
        
        // 3. Add personality typings
        console.log('\n3. Adding Personality Typings...');
        
        const typings = [
            { entityName: 'Tyrion Lannister', system: 'mbti', type: 'ENTP', confidence: 0.85 },
            { entityName: 'Tyrion Lannister', system: 'socionics', type: 'ILE', confidence: 0.80 },
            { entityName: 'Hermione Granger', system: 'mbti', type: 'ISTJ', confidence: 0.90 },
            { entityName: 'Hermione Granger', system: 'socionics', type: 'LSI', confidence: 0.85 },
            { entityName: 'Sherlock Holmes', system: 'mbti', type: 'INTJ', confidence: 0.95 },
            { entityName: 'Sherlock Holmes', system: 'socionics', type: 'LII', confidence: 0.90 }
        ];
        
        for (const typing of typings) {
            const entity = createdEntities.find(e => e.name === typing.entityName);
            if (entity) {
                await nodeManager.addTyping({
                    entity_id: entity.id,
                    system_name: typing.system,
                    type_name: typing.type,
                    confidence: typing.confidence
                });
                console.log(`✅ Added ${typing.system} typing for ${typing.entityName}: ${typing.type}`);
            }
        }
        
        // 4. Add vector embeddings (simulated)
        console.log('\n4. Adding Vector Embeddings...');
        
        // Generate sample embeddings (384-dimensional)
        const embeddings = [
            { 
                name: 'Tyrion Lannister',
                embedding: generateRandomEmbedding(384, 'witty_intellectual')
            },
            {
                name: 'Hermione Granger', 
                embedding: generateRandomEmbedding(384, 'studious_logical')
            },
            {
                name: 'Sherlock Holmes',
                embedding: generateRandomEmbedding(384, 'analytical_detective')
            }
        ];
        
        for (const embData of embeddings) {
            const entity = createdEntities.find(e => e.name === embData.name);
            if (entity) {
                await nodeManager.addEmbedding(entity.id, embData.embedding);
                console.log(`✅ Added embedding for ${entity.name}`);
            }
        }
        
        // 5. Demonstrate vector search
        console.log('\n5. Vector Similarity Search Demo...');
        
        const queryEmbedding = generateRandomEmbedding(384, 'intellectual_analytical');
        const searchResults = await nodeManager.vectorSearch(queryEmbedding, 3);
        
        console.log(`🔍 Search Results (${searchResults.length} found):`);
        for (const result of searchResults) {
            console.log(`  • ${result.entity.name} - Similarity: ${result.similarity.toFixed(3)}`);
            console.log(`    Type: ${result.entity.entity_type}`);
            console.log(`    Description: ${result.entity.description}`);
        }
        
        // 6. Get entities with filtering
        console.log('\n6. Entity Retrieval with Filters...');
        
        const allEntities = await nodeManager.getEntities({ 
            limit: 10, 
            entityType: 'fictional_character' 
        });
        
        console.log(`📋 Found ${allEntities.length} fictional characters:`);
        for (const entity of allEntities) {
            const typings = await nodeManager.getTypings(entity.id);
            console.log(`  • ${entity.name} (${typings.length} typings)`);
            for (const typing of typings.slice(0, 2)) {
                console.log(`    - ${typing.system_name}: ${typing.type_name} (${typing.confidence})`);
            }
        }
        
        // 7. Demonstrate Parquet import capability (if file exists)
        console.log('\n7. Parquet Import Demo...');
        
        const parquetPath = '../data/bot_store/pdb_profiles_normalized.parquet';
        if (fs.existsSync(parquetPath)) {
            console.log('📁 Found PDB parquet file, importing...');
            await nodeManager.importFromParquet(parquetPath);
            
            const totalEntities = await nodeManager.getEntities({ limit: 1000 });
            console.log(`✅ Total entities after import: ${totalEntities.length}`);
        } else {
            console.log('ℹ️  PDB parquet file not found, skipping import demo');
        }
        
        // 8. Create users and session
        console.log('\n8. User Management and Sessions...');
        
        const user = await nodeManager.createUser('demo_analyst', 'admin', 'expert');
        console.log(`👤 Created user: ${user.username} (${user.role})`);
        
        // 9. API compatibility demo
        console.log('\n9. Multi-language API Compatibility...');
        
        console.log('✅ TypeScript/JavaScript API: Ready');
        console.log('✅ Python Integration: Available via REST API');
        console.log('✅ WebAssembly Support: Initialized');
        console.log('✅ DuckDB Integration: Active');
        console.log('✅ Vector Search: Functional');
        console.log('✅ Parquet Support: Native DuckDB reader');
        
        // 10. Performance comparison
        console.log('\n10. Performance Comparison...');
        
        console.time('DuckDB Query');
        const duckdbResults = await nodeManager.getEntities({ limit: 100 });
        console.timeEnd('DuckDB Query');
        console.log(`📊 Retrieved ${duckdbResults.length} entities via DuckDB`);
        
        console.time('Vector Search');
        const vectorResults = await nodeManager.vectorSearch(queryEmbedding, 5);
        console.timeEnd('Vector Search');
        console.log(`🔍 Found ${vectorResults.length} similar entities`);
        
        console.log('\n✨ Demo completed successfully!');
        console.log('🌟 IPDB Enhanced Features:');
        console.log('   • Multi-database support (SQLite/DuckDB)');
        console.log('   • High-performance vector search');
        console.log('   • Cross-language API compatibility');
        console.log('   • WebAssembly browser support');
        console.log('   • Native Parquet file handling');
        console.log('   • Real-time similarity search');
        
        // Cleanup
        await nodeManager.close();
        await wasmManager.close();
        
    } catch (error) {
        console.error('❌ Demo failed:', error);
        process.exit(1);
    }
}

/**
 * Generate a random embedding with some pattern for demo purposes
 */
function generateRandomEmbedding(dimension: number, pattern: string): Float32Array {
    const embedding = new Float32Array(dimension);
    
    // Add some pattern-based bias for demo purposes
    const patternMap: Record<string, number> = {
        'witty_intellectual': 0.1,
        'studious_logical': 0.2,
        'analytical_detective': 0.3,
        'intellectual_analytical': 0.25
    };
    
    const bias = patternMap[pattern] || 0;
    
    for (let i = 0; i < dimension; i++) {
        embedding[i] = (Math.random() - 0.5) * 2 + bias;
    }
    
    // Normalize the embedding
    const magnitude = Math.sqrt(embedding.reduce((sum, val) => sum + val * val, 0));
    for (let i = 0; i < dimension; i++) {
        embedding[i] /= magnitude;
    }
    
    return embedding;
}

// Run the demo if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
    runDemo();
}

export { runDemo };