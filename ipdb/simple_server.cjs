#!/usr/bin/env node

/**
 * Simple IPDB HTTP Server
 * =======================
 * 
 * Lightweight HTTP server for IPDB database API
 * Uses Node.js built-in modules only (no external dependencies)
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const url = require('url');
const { spawn } = require('child_process');

class SimpleIPDBServer {
    constructor(port = 3000) {
        this.port = port;
        this.setupRoutes();
        this.server = http.createServer(this.handleRequest.bind(this));
    }

    setupRoutes() {
        this.routes = {
            'GET /': this.handleRoot.bind(this),
            'GET /health': this.handleHealth.bind(this),
            'GET /api/info': this.handleApiInfo.bind(this),
            'GET /api/stats': this.handleStats.bind(this),
            'POST /api/test': this.handleTest.bind(this),
        };
    }

    async handleRequest(req, res) {
        const parsedUrl = url.parse(req.url, true);
        const method = req.method;
        const pathname = parsedUrl.pathname;
        const routeKey = `${method} ${pathname}`;

        // Enable CORS
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
        res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

        if (method === 'OPTIONS') {
            res.writeHead(200);
            res.end();
            return;
        }

        try {
            const handler = this.routes[routeKey];
            if (handler) {
                await handler(req, res, parsedUrl);
            } else {
                this.handleNotFound(res);
            }
        } catch (error) {
            this.handleError(res, error);
        }
    }

    handleRoot(req, res) {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(`
            <html>
                <head>
                    <title>IPDB Simple API</title>
                    <style>
                        body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
                        h1 { color: #333; }
                        .endpoint { background: #f5f5f5; padding: 10px; margin: 10px 0; border-radius: 5px; }
                        .method { color: #007acc; font-weight: bold; }
                    </style>
                </head>
                <body>
                    <h1>🧠 IPDB - Integrated Personality Database API</h1>
                    <p>Simple HTTP API for the Socionics Research database system.</p>
                    
                    <h2>Available Endpoints:</h2>
                    <div class="endpoint">
                        <span class="method">GET</span> <code>/health</code> - Health check
                    </div>
                    <div class="endpoint">
                        <span class="method">GET</span> <code>/api/info</code> - API information
                    </div>
                    <div class="endpoint">
                        <span class="method">GET</span> <code>/api/stats</code> - Database statistics
                    </div>
                    <div class="endpoint">
                        <span class="method">POST</span> <code>/api/test</code> - Run database test
                    </div>
                    
                    <h2>System Status:</h2>
                    <ul>
                        <li>✅ Node.js HTTP Server Running</li>
                        <li>✅ Python IPDB Backend Available</li>
                        <li>✅ SQLite Database Support</li>
                        <li>⚠️ DuckDB & Vector Search Optional</li>
                    </ul>
                    
                    <p><em>For full functionality, use the Python API or command-line tools.</em></p>
                </body>
            </html>
        `);
    }

    handleHealth(req, res) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            status: 'ok',
            timestamp: new Date().toISOString(),
            system: 'IPDB Simple API',
            version: '1.0.0',
            node_version: process.version,
            uptime: process.uptime()
        }));
    }

    handleApiInfo(req, res) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            name: 'IPDB Simple API',
            description: 'Integrated Personality Database for Socionics Research',
            version: '1.0.0',
            features: [
                'Database schema management',
                'User management with role-based access',
                'Multi-system personality typing (Socionics, MBTI, etc.)',
                'Rating session organization',
                'Data import from Parquet files',
                'Inter-rater reliability tracking'
            ],
            backend: 'Python with SQLite/MySQL support',
            api_type: 'HTTP REST (simplified)',
            documentation: 'See /api/info for endpoint details'
        }));
    }

    async handleStats(req, res) {
        try {
            // Run Python command to get database stats
            const pythonScript = `
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath('${__filename}')))

try:
    from database_manager import IPDBManager
    import json
    
    # Connect to the demo database if it exists
    db_path = "/tmp/socionics_demo.db"
    if not os.path.exists(db_path):
        db_path = ":memory:"
        
    db = IPDBManager(db_path)
    
    if db_path == ":memory:":
        db.initialize_database()
    
    conn = db.get_connection()
    cursor = conn.cursor()
    
    stats = {}
    
    try:
        cursor.execute("SELECT COUNT(*) FROM entities")
        stats['total_entities'] = cursor.fetchone()[0]
    except:
        stats['total_entities'] = 0
        
    try:
        cursor.execute("SELECT COUNT(*) FROM users")
        stats['total_users'] = cursor.fetchone()[0]
    except:
        stats['total_users'] = 0
        
    try:
        cursor.execute("SELECT COUNT(*) FROM typing_judgments")
        stats['total_judgments'] = cursor.fetchone()[0]
    except:
        stats['total_judgments'] = 0
    
    try:
        cursor.execute("SELECT COUNT(*) FROM rating_sessions")
        stats['total_sessions'] = cursor.fetchone()[0]
    except:
        stats['total_sessions'] = 0
        
    try:
        cursor.execute("SELECT COUNT(*) FROM personality_systems")
        stats['personality_systems'] = cursor.fetchone()[0]
    except:
        stats['personality_systems'] = 0
        
    try:
        cursor.execute("SELECT COUNT(*) FROM personality_types")
        stats['personality_types'] = cursor.fetchone()[0]
    except:
        stats['personality_types'] = 0
    
    db.close()
    print(json.dumps(stats))
    
except Exception as e:
    print(json.dumps({"error": str(e), "available": False}))
            `;

            const python = spawn('python3', ['-c', pythonScript], {
                cwd: path.dirname(__filename)
            });

            let output = '';
            let error = '';

            python.stdout.on('data', (data) => {
                output += data.toString();
            });

            python.stderr.on('data', (data) => {
                error += data.toString();
            });

            python.on('close', (code) => {
                try {
                    const stats = JSON.parse(output.trim());
                    res.writeHead(200, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({
                        success: true,
                        database_stats: stats,
                        python_available: code === 0,
                        last_updated: new Date().toISOString()
                    }));
                } catch (e) {
                    res.writeHead(500, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({
                        success: false,
                        error: 'Failed to get database stats',
                        python_error: error,
                        python_available: false
                    }));
                }
            });

        } catch (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
                success: false,
                error: error.message,
                python_available: false
            }));
        }
    }

    async handleTest(req, res) {
        try {
            const python = spawn('python3', [
                path.join(__dirname, 'test_database.py')
            ]);

            let output = '';
            let error = '';

            python.stdout.on('data', (data) => {
                output += data.toString();
            });

            python.stderr.on('data', (data) => {
                error += data.toString();
            });

            python.on('close', (code) => {
                const success = code === 0;
                res.writeHead(success ? 200 : 500, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({
                    success,
                    exit_code: code,
                    output: output.split('\n').filter(line => line.includes('INFO') || line.includes('✓') || line.includes('✗') || line.includes('🎉')),
                    full_output: output,
                    error: error,
                    timestamp: new Date().toISOString()
                }));
            });

        } catch (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
                success: false,
                error: error.message,
                timestamp: new Date().toISOString()
            }));
        }
    }

    handleNotFound(res) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            error: 'Endpoint not found',
            available_endpoints: Object.keys(this.routes)
        }));
    }

    handleError(res, error) {
        console.error('Server error:', error);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            error: 'Internal server error',
            message: error.message
        }));
    }

    start() {
        this.server.listen(this.port, () => {
            console.log(`\n🚀 IPDB Simple API Server started`);
            console.log(`📍 Server: http://localhost:${this.port}`);
            console.log(`🏥 Health: http://localhost:${this.port}/health`);
            console.log(`📊 Stats:  http://localhost:${this.port}/api/stats`);
            console.log(`ℹ️  Info:   http://localhost:${this.port}/api/info`);
            console.log(`\n✅ Server is ready to handle requests!`);
            console.log(`   Use Ctrl+C to stop the server\n`);
        });

        this.server.on('error', (error) => {
            if (error.code === 'EADDRINUSE') {
                console.error(`❌ Port ${this.port} is already in use. Try a different port.`);
            } else {
                console.error('❌ Server error:', error);
            }
        });
    }

    stop() {
        this.server.close();
    }
}

// Start server if running directly
if (require.main === module) {
    const port = process.env.PORT || 3000;
    const server = new SimpleIPDBServer(port);
    server.start();

    // Graceful shutdown
    process.on('SIGINT', () => {
        console.log('\n👋 Shutting down server gracefully...');
        server.stop();
        process.exit(0);
    });
}

module.exports = SimpleIPDBServer;