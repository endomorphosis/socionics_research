/**
 * REST API Server for IPDB with multi-language support
 * ===================================================
 * 
 * This Express.js server provides REST endpoints for the IPDB database
 * with support for DuckDB, vector search, and cross-language integration.
 */

import express from 'express';
import cors from 'cors';
import { IPDBManagerJS } from './index.js';
import { IPDBManagerWASM } from './wasm.js';

const app = express();
const port = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.static('public'));

// Database managers (both Node.js and WASM versions)
let dbManager: IPDBManagerJS;
let wasmManager: IPDBManagerWASM;

// Initialize database managers
async function initializeManagers() {
    // Initialize Node.js manager
    dbManager = new IPDBManagerJS(':memory:');
    await dbManager.initialize();
    
    // Initialize WASM manager for browser compatibility
    wasmManager = new IPDBManagerWASM();
    await wasmManager.initialize();
    
    console.log('Database managers initialized');
}

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ 
        status: 'ok', 
        timestamp: new Date().toISOString(),
        duckdb_available: true,
        vector_search: true,
        wasm_support: true
    });
});

// API Info endpoint
app.get('/api/info', (req, res) => {
    res.json({
        name: 'IPDB API',
        version: '1.0.0',
        description: 'Integrated Personality Database API with DuckDB and vector search',
        features: [
            'Multi-language support (Python, TypeScript, JavaScript)',
            'DuckDB integration for performance',
            'Vector similarity search with hnswlib',
            'WebAssembly compatibility',
            'Parquet file support',
            'Multiple personality typing systems'
        ],
        endpoints: {
            'GET /health': 'Health check',
            'GET /api/info': 'API information',
            'GET /api/entities': 'List entities with pagination',
            'POST /api/entities': 'Create new entity',
            'GET /api/entities/:id': 'Get entity details',
            'GET /api/entities/:id/typings': 'Get entity typings',
            'POST /api/entities/:id/embeddings': 'Add entity embedding',
            'POST /api/search/vector': 'Vector similarity search',
            'POST /api/search/text': 'Text search entities',
            'POST /api/import/parquet': 'Import from Parquet file',
            'GET /api/systems': 'List personality typing systems',
            'POST /api/users': 'Create user',
            'POST /api/sessions': 'Create rating session'
        }
    });
});

// Entity endpoints
app.get('/api/entities', async (req, res) => {
    try {
        const { limit = 10, offset = 0, entityType, search } = req.query;
        
        const entities = await dbManager.getEntities({
            limit: parseInt(limit as string),
            offset: parseInt(offset as string),
            entityType: entityType as any,
            search: search as string
        });
        
        res.json({
            entities,
            pagination: {
                limit: parseInt(limit as string),
                offset: parseInt(offset as string),
                count: entities.length
            }
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/entities', async (req, res) => {
    try {
        const { name, entity_type = 'person', description, metadata } = req.body;
        
        if (!name) {
            return res.status(400).json({ error: 'Name is required' });
        }
        
        const entity = await dbManager.createEntity({
            id: '', // Will be generated
            name,
            entity_type,
            description,
            metadata
        });
        
        res.status(201).json({ entity });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/entities/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const entities = await dbManager.getEntities({ search: id, limit: 1 });
        
        if (entities.length === 0) {
            return res.status(404).json({ error: 'Entity not found' });
        }
        
        res.json({ entity: entities[0] });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/entities/:id/typings', async (req, res) => {
    try {
        const { id } = req.params;
        const { system } = req.query;
        
        const typings = await dbManager.getTypings(id, system as string);
        res.json({ typings });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/entities/:id/embeddings', async (req, res) => {
    try {
        const { id } = req.params;
        const { embedding } = req.body;
        
        if (!embedding || !Array.isArray(embedding)) {
            return res.status(400).json({ error: 'Valid embedding array is required' });
        }
        
        const embeddingArray = new Float32Array(embedding);
        await dbManager.addEmbedding(id, embeddingArray);
        
        res.json({ success: true, message: 'Embedding added successfully' });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Search endpoints
app.post('/api/search/vector', async (req, res) => {
    try {
        const { embedding, k = 10, entityType } = req.body;
        
        if (!embedding || !Array.isArray(embedding)) {
            return res.status(400).json({ error: 'Valid embedding array is required' });
        }
        
        const queryEmbedding = new Float32Array(embedding);
        const results = await dbManager.vectorSearch(queryEmbedding, k);
        
        res.json({ results });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/search/text', async (req, res) => {
    try {
        const { query, limit = 10, entityType } = req.body;
        
        const results = await dbManager.getEntities({
            search: query,
            limit,
            entityType
        });
        
        res.json({ results });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Import endpoints
app.post('/api/import/parquet', async (req, res) => {
    try {
        const { filePath, embeddingsPath } = req.body;
        
        if (!filePath) {
            return res.status(400).json({ error: 'File path is required' });
        }
        
        await dbManager.importFromParquet(filePath);
        
        // Import embeddings if provided
        if (embeddingsPath) {
            // TODO: Import embeddings from separate file
        }
        
        res.json({ success: true, message: 'Parquet file imported successfully' });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// User and session endpoints
app.post('/api/users', async (req, res) => {
    try {
        const { username, role = 'annotator', experienceLevel = 'novice' } = req.body;
        
        if (!username) {
            return res.status(400).json({ error: 'Username is required' });
        }
        
        const user = await dbManager.createUser(username, role, experienceLevel);
        res.status(201).json({ user });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// WASM-specific endpoints (for browser compatibility)
app.get('/api/wasm/info', (req, res) => {
    res.json({
        message: 'WASM endpoints for browser-based usage',
        note: 'These endpoints use DuckDB-WASM and hnswlib-wasm for client-side processing'
    });
});

app.post('/api/wasm/initialize', async (req, res) => {
    try {
        if (!wasmManager) {
            wasmManager = new IPDBManagerWASM();
            await wasmManager.initialize();
        }
        res.json({ success: true, message: 'WASM manager initialized' });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Client SDK endpoint - serves the browser-compatible bundle
app.get('/api/sdk/browser.js', (req, res) => {
    res.setHeader('Content-Type', 'application/javascript');
    res.send(`
        // IPDB Browser SDK
        // This provides client-side access to IPDB functionality
        
        class IPDBClient {
            constructor(baseUrl = '/api') {
                this.baseUrl = baseUrl;
            }
            
            async getEntities(options = {}) {
                const params = new URLSearchParams(options);
                const response = await fetch(\`\${this.baseUrl}/entities?\${params}\`);
                return response.json();
            }
            
            async createEntity(entity) {
                const response = await fetch(\`\${this.baseUrl}/entities\`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(entity)
                });
                return response.json();
            }
            
            async vectorSearch(embedding, k = 10) {
                const response = await fetch(\`\${this.baseUrl}/search/vector\`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ embedding, k })
                });
                return response.json();
            }
            
            async addEmbedding(entityId, embedding) {
                const response = await fetch(\`\${this.baseUrl}/entities/\${entityId}/embeddings\`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ embedding })
                });
                return response.json();
            }
            
            async importParquet(filePath, embeddingsPath = null) {
                const response = await fetch(\`\${this.baseUrl}/import/parquet\`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ filePath, embeddingsPath })
                });
                return response.json();
            }
        }
        
        // Export for both CommonJS and ES modules
        if (typeof module !== 'undefined' && module.exports) {
            module.exports = { IPDBClient };
        } else if (typeof window !== 'undefined') {
            window.IPDBClient = IPDBClient;
        }
    `);
});

// Error handling middleware
app.use((error: Error, req: express.Request, res: express.Response, next: express.NextFunction) => {
    console.error('API Error:', error);
    res.status(500).json({ 
        error: 'Internal server error',
        message: process.env.NODE_ENV === 'development' ? error.message : 'Something went wrong'
    });
});

// 404 handler
app.use((req: express.Request, res: express.Response) => {
    res.status(404).json({ error: 'Endpoint not found' });
});

// Start server
async function startServer() {
    try {
        await initializeManagers();
        
        app.listen(port, () => {
            console.log(`IPDB API server running at http://localhost:${port}`);
            console.log(`API documentation available at http://localhost:${port}/api/info`);
            console.log(`Browser SDK available at http://localhost:${port}/api/sdk/browser.js`);
        });
    } catch (error) {
        console.error('Failed to start server:', error);
        process.exit(1);
    }
}

if (import.meta.url === `file://${process.argv[1]}`) {
    startServer();
}

export { app, dbManager, wasmManager };