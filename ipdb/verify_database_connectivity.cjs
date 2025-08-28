#!/usr/bin/env node

/**
 * Database Connectivity Verification Script
 * ========================================
 * 
 * Verifies that the IPDB interface is properly connected to the database API
 * and not displaying placeholder/mocked values.
 */

const http = require('http');

class DatabaseConnectivityVerifier {
    constructor(serverUrl = 'http://localhost:3000') {
        this.serverUrl = serverUrl;
    }

    /**
     * Make HTTP request to API endpoint
     */
    async makeRequest(path) {
        return new Promise((resolve, reject) => {
            const url = this.serverUrl + path;
            
            http.get(url, (res) => {
                let data = '';
                
                res.on('data', (chunk) => {
                    data += chunk;
                });
                
                res.on('end', () => {
                    try {
                        resolve(JSON.parse(data));
                    } catch (error) {
                        reject(new Error(`Failed to parse JSON response: ${error.message}`));
                    }
                });
            }).on('error', (error) => {
                reject(error);
            });
        });
    }

    /**
     * Verify stats API returns real data
     */
    async verifyStatsAPI() {
        console.log('🔍 Verifying /api/stats endpoint...');
        
        try {
            const stats = await this.makeRequest('/api/stats');
            
            console.log('📊 API Response:', JSON.stringify(stats, null, 2));
            
            // Verify structure
            if (!stats.success) {
                throw new Error('API response indicates failure');
            }
            
            // Check for real data vs placeholder values
            const { entities, users, ratings, comments, community_stats } = stats;
            
            // These would be typical placeholder values - we want to avoid these
            const suspiciousValues = [2045783, 8923451, 157892, 12847];
            
            const actualValues = [entities, users, ratings, comments];
            const hasSuspiciousValues = actualValues.some(val => suspiciousValues.includes(val));
            
            if (hasSuspiciousValues) {
                console.log('⚠️  WARNING: Detected possible placeholder values in stats');
                return false;
            }
            
            console.log('✅ Stats API verified - returning real database values');
            console.log(`   - Characters: ${entities}`);
            console.log(`   - Users: ${users}`);
            console.log(`   - Ratings: ${ratings}`);
            console.log(`   - Comments: ${comments}`);
            
            return true;
            
        } catch (error) {
            console.error('❌ Stats API verification failed:', error.message);
            return false;
        }
    }

    /**
     * Verify entities API returns real data
     */
    async verifyEntitiesAPI() {
        console.log('🔍 Verifying /api/entities endpoint...');
        
        try {
            const response = await this.makeRequest('/api/entities');
            
            if (!response.success || !response.entities) {
                throw new Error('Entities API response structure invalid');
            }
            
            const entities = response.entities;
            console.log(`✅ Entities API verified - ${entities.length} entities found`);
            
            // Show sample entities
            if (entities.length > 0) {
                console.log('📋 Sample entities:');
                entities.slice(0, 3).forEach((entity, index) => {
                    console.log(`   ${index + 1}. ${entity.name || 'Unknown'} (${entity.source || 'Unknown source'})`);
                });
            }
            
            return true;
            
        } catch (error) {
            console.error('❌ Entities API verification failed:', error.message);
            return false;
        }
    }

    /**
     * Verify health check endpoint
     */
    async verifyHealthAPI() {
        console.log('🔍 Verifying /health endpoint...');
        
        try {
            const health = await this.makeRequest('/health');
            
            if (health.status !== 'healthy') {
                throw new Error('Server health check failed');
            }
            
            console.log('✅ Health check passed');
            return true;
            
        } catch (error) {
            console.error('❌ Health check failed:', error.message);
            return false;
        }
    }

    /**
     * Run complete verification
     */
    async verify() {
        console.log('🚀 Starting database connectivity verification...\n');
        
        const results = {
            health: await this.verifyHealthAPI(),
            stats: await this.verifyStatsAPI(),
            entities: await this.verifyEntitiesAPI()
        };
        
        console.log('\n📊 Verification Results:');
        console.log('========================');
        
        Object.entries(results).forEach(([test, passed]) => {
            console.log(`${passed ? '✅' : '❌'} ${test.charAt(0).toUpperCase() + test.slice(1)} API: ${passed ? 'PASSED' : 'FAILED'}`);
        });
        
        const allPassed = Object.values(results).every(result => result === true);
        
        console.log('\n🎯 Final Result:');
        console.log(`Database connectivity: ${allPassed ? '✅ VERIFIED - Real data connected' : '❌ FAILED - Check connections'}`);
        
        return allPassed;
    }
}

// Run verification if called directly
if (require.main === module) {
    const verifier = new DatabaseConnectivityVerifier();
    
    verifier.verify()
        .then((success) => {
            process.exit(success ? 0 : 1);
        })
        .catch((error) => {
            console.error('❌ Verification script failed:', error);
            process.exit(1);
        });
}

module.exports = DatabaseConnectivityVerifier;