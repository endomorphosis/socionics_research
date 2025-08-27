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
            'GET /app': this.handleApp.bind(this),
            'GET /health': this.handleHealth.bind(this),
            'GET /api/info': this.handleApiInfo.bind(this),
            'GET /api/stats': this.handleStats.bind(this),
            'POST /api/test': this.handleTest.bind(this),
            'GET /api/entities': this.handleGetEntities.bind(this),
            'GET /api/entities/:id': this.handleGetEntity.bind(this),
            'POST /api/ratings': this.handleCreateRating.bind(this),
            'GET /api/ratings/:entityId': this.handleGetRatings.bind(this),
            'POST /api/comments': this.handleCreateComment.bind(this),
            'GET /api/comments/:entityId': this.handleGetComments.bind(this),
            'POST /api/users': this.handleCreateUser.bind(this),
        };
    }

    async handleRequest(req, res) {
        const parsedUrl = url.parse(req.url, true);
        const method = req.method;
        const pathname = parsedUrl.pathname;
        
        // Handle parameterized routes
        let routeKey = `${method} ${pathname}`;
        let params = {};
        
        // Check for entity ID route
        const entityMatch = pathname.match(/^\/api\/entities\/([^\/]+)$/);
        if (entityMatch) {
            routeKey = `${method} /api/entities/:id`;
            params.id = entityMatch[1];
        }
        
        const ratingsMatch = pathname.match(/^\/api\/ratings\/([^\/]+)$/);
        if (ratingsMatch) {
            routeKey = `${method} /api/ratings/:entityId`;
            params.entityId = ratingsMatch[1];
        }
        
        const commentsMatch = pathname.match(/^\/api\/comments\/([^\/]+)$/);
        if (commentsMatch) {
            routeKey = `${method} /api/comments/:entityId`;
            params.entityId = commentsMatch[1];
        }

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
                await handler(req, res, parsedUrl, params);
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
                    
                    <p><strong><a href="/app">→ Open IPDB Rating Application</a></strong></p>
                    
                    <h2>Available Endpoints:</h2>
                    <div class="endpoint">
                        <span class="method">GET</span> <code>/app</code> - Web application interface
                    </div>
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
                        <span class="method">GET</span> <code>/api/entities</code> - List entities
                    </div>
                    <div class="endpoint">
                        <span class="method">POST</span> <code>/api/ratings</code> - Create rating
                    </div>
                    <div class="endpoint">
                        <span class="method">POST</span> <code>/api/comments</code> - Create comment
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

    // Helper method to get request body
    getRequestBody(req) {
        return new Promise((resolve, reject) => {
            let body = '';
            req.on('data', (chunk) => {
                body += chunk.toString();
            });
            req.on('end', () => {
                try {
                    resolve(body ? JSON.parse(body) : {});
                } catch (error) {
                    reject(error);
                }
            });
            req.on('error', reject);
        });
    }

    // Enhanced web application interface
    async handleApp(req, res) {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(`
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>IPDB - Character Rating System</title>
                <style>
                    body { 
                        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                        max-width: 1200px; 
                        margin: 0 auto; 
                        padding: 20px;
                        background-color: #f5f5f5;
                    }
                    .header { 
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        padding: 20px;
                        border-radius: 10px;
                        margin-bottom: 30px;
                        text-align: center;
                    }
                    .container { 
                        background: white;
                        padding: 30px;
                        border-radius: 10px;
                        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                        margin-bottom: 20px;
                    }
                    .entity-card {
                        border: 1px solid #ddd;
                        border-radius: 8px;
                        padding: 20px;
                        margin: 15px 0;
                        background: #fff;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                        transition: transform 0.2s ease;
                    }
                    .entity-card:hover {
                        transform: translateY(-2px);
                        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
                    }
                    .personality-type {
                        background: #4CAF50;
                        color: white;
                        padding: 5px 10px;
                        border-radius: 15px;
                        font-size: 12px;
                        margin: 5px;
                        display: inline-block;
                    }
                    .rating-section {
                        background: #f8f9fa;
                        padding: 20px;
                        border-radius: 8px;
                        margin: 15px 0;
                    }
                    .star-rating {
                        font-size: 24px;
                        cursor: pointer;
                        color: #ddd;
                        transition: color 0.2s;
                    }
                    .star-rating.active {
                        color: #ffc107;
                    }
                    .btn {
                        background: #007bff;
                        color: white;
                        border: none;
                        padding: 10px 20px;
                        border-radius: 5px;
                        cursor: pointer;
                        margin: 5px;
                        transition: background-color 0.2s;
                    }
                    .btn:hover {
                        background: #0056b3;
                    }
                    .btn-success { background: #28a745; }
                    .btn-success:hover { background: #218838; }
                    .form-group {
                        margin: 15px 0;
                    }
                    .form-control {
                        width: 100%;
                        padding: 10px;
                        border: 1px solid #ddd;
                        border-radius: 5px;
                        font-size: 14px;
                    }
                    .comments-section {
                        background: #fff;
                        border: 1px solid #ddd;
                        border-radius: 8px;
                        padding: 20px;
                        margin-top: 15px;
                    }
                    .comment {
                        background: #f8f9fa;
                        padding: 15px;
                        border-radius: 5px;
                        margin: 10px 0;
                        border-left: 4px solid #007bff;
                    }
                    .loading {
                        text-align: center;
                        padding: 40px;
                        color: #666;
                    }
                    .error {
                        background: #f8d7da;
                        color: #721c24;
                        padding: 15px;
                        border-radius: 5px;
                        margin: 15px 0;
                    }
                    .success {
                        background: #d4edda;
                        color: #155724;
                        padding: 15px;
                        border-radius: 5px;
                        margin: 15px 0;
                    }
                    .stats-bar {
                        background: #e9ecef;
                        height: 20px;
                        border-radius: 10px;
                        overflow: hidden;
                        margin: 5px 0;
                    }
                    .stats-fill {
                        height: 100%;
                        background: linear-gradient(90deg, #28a745, #20c997);
                        transition: width 0.3s ease;
                    }
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>🧠 IPDB Character Rating System</h1>
                    <p>Rate personality types and share your insights on characters</p>
                </div>

                <div class="container">
                    <h2>📊 System Stats</h2>
                    <div id="stats-container" class="loading">Loading system statistics...</div>
                </div>

                <div class="container">
                    <h2>👥 Character Database</h2>
                    <p>Select a character below to view details, rate their personality type, and leave comments.</p>
                    <div id="entities-container" class="loading">Loading entities...</div>
                </div>

                <!-- Rating Modal -->
                <div id="rating-modal" style="display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 1000;">
                    <div style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); background: white; padding: 30px; border-radius: 10px; max-width: 500px; width: 90%;">
                        <h3>Rate Character</h3>
                        <div id="rating-content"></div>
                        <button onclick="closeRatingModal()" class="btn">Close</button>
                    </div>
                </div>

                <script>
                    let currentUser = 'demo-user-' + Date.now();
                    let entities = [];
                    
                    // Initialize the application
                    async function init() {
                        await loadStats();
                        await loadEntities();
                        await createDemoUser();
                    }

                    async function createDemoUser() {
                        try {
                            const response = await fetch('/api/users', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({
                                    username: currentUser,
                                    display_name: 'Demo User',
                                    role: 'annotator',
                                    experience_level: 'intermediate'
                                })
                            });
                            const result = await response.json();
                            console.log('Demo user created:', result);
                        } catch (error) {
                            console.error('Failed to create demo user:', error);
                        }
                    }

                    async function loadStats() {
                        try {
                            const response = await fetch('/api/stats');
                            const data = await response.json();
                            
                            const statsHtml = \`
                                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px;">
                                    <div class="entity-card">
                                        <h4>📊 Entities</h4>
                                        <div class="stats-bar">
                                            <div class="stats-fill" style="width: 100%"></div>
                                        </div>
                                        <p>\${data.database_stats?.entities || 0} characters</p>
                                    </div>
                                    <div class="entity-card">
                                        <h4>🧠 Personality Types</h4>
                                        <div class="stats-bar">
                                            <div class="stats-fill" style="width: 85%"></div>
                                        </div>
                                        <p>\${data.database_stats?.personality_types || 0} types</p>
                                    </div>
                                    <div class="entity-card">
                                        <h4>📝 Active Users</h4>
                                        <div class="stats-bar">
                                            <div class="stats-fill" style="width: 60%"></div>
                                        </div>
                                        <p>\${data.database_stats?.users || 0} users</p>
                                    </div>
                                </div>
                            \`;
                            
                            document.getElementById('stats-container').innerHTML = statsHtml;
                        } catch (error) {
                            document.getElementById('stats-container').innerHTML = \`
                                <div class="error">Failed to load stats: \${error.message}</div>
                            \`;
                        }
                    }

                    async function loadEntities() {
                        try {
                            const response = await fetch('/api/entities');
                            const data = await response.json();
                            entities = data.entities || [];
                            
                            if (entities.length === 0) {
                                document.getElementById('entities-container').innerHTML = \`
                                    <div class="error">
                                        No entities found. Please run the database initialization first.
                                        <button onclick="runTest()" class="btn btn-success">Initialize Database</button>
                                    </div>
                                \`;
                                return;
                            }

                            const entitiesHtml = entities.slice(0, 20).map(entity => \`
                                <div class="entity-card">
                                    <h3>\${entity.name}</h3>
                                    <p><strong>Type:</strong> \${entity.entity_type}</p>
                                    \${entity.description ? \`<p><strong>Description:</strong> \${entity.description}</p>\` : ''}
                                    <div class="rating-section">
                                        <h4>Rate this character's personality</h4>
                                        <button onclick="openRatingModal('\${entity.id}', '\${entity.name}')" class="btn">
                                            ⭐ Rate Character
                                        </button>
                                        <button onclick="showComments('\${entity.id}')" class="btn">
                                            💬 View Comments
                                        </button>
                                    </div>
                                </div>
                            \`).join('');
                            
                            document.getElementById('entities-container').innerHTML = entitiesHtml;
                        } catch (error) {
                            document.getElementById('entities-container').innerHTML = \`
                                <div class="error">Failed to load entities: \${error.message}</div>
                                <button onclick="runTest()" class="btn btn-success">Try Initialize Database</button>
                            \`;
                        }
                    }

                    function openRatingModal(entityId, entityName) {
                        const modalContent = \`
                            <h4>Rating: \${entityName}</h4>
                            <div class="form-group">
                                <label>Personality System:</label>
                                <select id="personality-system" class="form-control">
                                    <option value="socionics">Socionics</option>
                                    <option value="mbti">MBTI</option>
                                    <option value="enneagram">Enneagram</option>
                                </select>
                            </div>
                            <div class="form-group">
                                <label>Personality Type:</label>
                                <select id="personality-type" class="form-control">
                                    <option value="ILE">ILE (ENTp)</option>
                                    <option value="SEI">SEI (ISFp)</option>
                                    <option value="ESE">ESE (ESFj)</option>
                                    <option value="LII">LII (INTj)</option>
                                    <option value="SLE">SLE (ESTp)</option>
                                    <option value="IEI">IEI (INFp)</option>
                                    <option value="EIE">EIE (ENFj)</option>
                                    <option value="LSI">LSI (ISTj)</option>
                                    <option value="SEE">SEE (ESFp)</option>
                                    <option value="ILI">ILI (INTp)</option>
                                    <option value="LIE">LIE (ENTj)</option>
                                    <option value="ESI">ESI (ISFj)</option>
                                    <option value="IEE">IEE (ENFp)</option>
                                    <option value="SLI">SLI (ISTp)</option>
                                    <option value="LSE">LSE (ESTj)</option>
                                    <option value="EII">EII (INFj)</option>
                                </select>
                            </div>
                            <div class="form-group">
                                <label>Confidence Level:</label>
                                <div id="star-rating">
                                    <span class="star-rating" onclick="setRating(1)">★</span>
                                    <span class="star-rating" onclick="setRating(2)">★</span>
                                    <span class="star-rating" onclick="setRating(3)">★</span>
                                    <span class="star-rating" onclick="setRating(4)">★</span>
                                    <span class="star-rating" onclick="setRating(5)">★</span>
                                </div>
                                <p><small>1 = Very uncertain, 5 = Very confident</small></p>
                            </div>
                            <div class="form-group">
                                <label>Reasoning (optional):</label>
                                <textarea id="rating-reasoning" class="form-control" rows="3" placeholder="Explain your reasoning for this typing..."></textarea>
                            </div>
                            <button onclick="submitRating('\${entityId}')" class="btn btn-success">Submit Rating</button>
                        \`;
                        
                        document.getElementById('rating-content').innerHTML = modalContent;
                        document.getElementById('rating-modal').style.display = 'block';
                        currentRating = 3; // default
                        setRating(3);
                    }

                    let currentRating = 3;
                    
                    function setRating(rating) {
                        currentRating = rating;
                        const stars = document.querySelectorAll('.star-rating');
                        stars.forEach((star, index) => {
                            star.className = index < rating ? 'star-rating active' : 'star-rating';
                        });
                    }

                    async function submitRating(entityId) {
                        try {
                            const system = document.getElementById('personality-system').value;
                            const type = document.getElementById('personality-type').value;
                            const reasoning = document.getElementById('rating-reasoning').value;
                            
                            const response = await fetch('/api/ratings', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({
                                    entity_id: entityId,
                                    personality_system: system,
                                    personality_type: type,
                                    confidence: currentRating / 5.0,
                                    reasoning: reasoning,
                                    user: currentUser
                                })
                            });
                            
                            const result = await response.json();
                            
                            if (result.success) {
                                closeRatingModal();
                                showSuccessMessage('Rating submitted successfully!');
                            } else {
                                showErrorMessage('Failed to submit rating: ' + result.error);
                            }
                        } catch (error) {
                            showErrorMessage('Error submitting rating: ' + error.message);
                        }
                    }

                    function closeRatingModal() {
                        document.getElementById('rating-modal').style.display = 'none';
                    }

                    async function showComments(entityId) {
                        try {
                            const response = await fetch('/api/comments/' + entityId);
                            const data = await response.json();
                            
                            const entity = entities.find(e => e.id === entityId);
                            const commentsHtml = \`
                                <div class="comments-section">
                                    <h4>Comments for \${entity?.name || 'Entity'}</h4>
                                    \${data.comments?.length ? 
                                        data.comments.map(comment => \`
                                            <div class="comment">
                                                <strong>\${comment.user || 'Anonymous'}</strong>
                                                <p>\${comment.content}</p>
                                                <small>\${comment.created_at}</small>
                                            </div>
                                        \`).join('') : 
                                        '<p>No comments yet.</p>'
                                    }
                                    <div class="form-group">
                                        <textarea id="new-comment-\${entityId}" class="form-control" rows="3" placeholder="Add your comment..."></textarea>
                                        <button onclick="addComment('\${entityId}')" class="btn btn-success">Add Comment</button>
                                    </div>
                                </div>
                            \`;
                            
                            const entityCard = document.querySelector(\`.entity-card:has(button[onclick*="\${entityId}"])\`);
                            if (entityCard) {
                                const existingComments = entityCard.querySelector('.comments-section');
                                if (existingComments) existingComments.remove();
                                entityCard.insertAdjacentHTML('beforeend', commentsHtml);
                            }
                        } catch (error) {
                            showErrorMessage('Failed to load comments: ' + error.message);
                        }
                    }

                    async function addComment(entityId) {
                        try {
                            const content = document.getElementById('new-comment-' + entityId).value.trim();
                            if (!content) return;
                            
                            const response = await fetch('/api/comments', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({
                                    entity_id: entityId,
                                    content: content,
                                    user: currentUser
                                })
                            });
                            
                            const result = await response.json();
                            
                            if (result.success) {
                                showSuccessMessage('Comment added successfully!');
                                showComments(entityId); // Refresh comments
                            } else {
                                showErrorMessage('Failed to add comment: ' + result.error);
                            }
                        } catch (error) {
                            showErrorMessage('Error adding comment: ' + error.message);
                        }
                    }

                    async function runTest() {
                        try {
                            document.getElementById('entities-container').innerHTML = '<div class="loading">Initializing database...</div>';
                            
                            const response = await fetch('/api/test', { method: 'POST' });
                            const result = await response.json();
                            
                            if (result.success) {
                                showSuccessMessage('Database initialized successfully!');
                                setTimeout(() => {
                                    loadEntities();
                                }, 2000);
                            } else {
                                showErrorMessage('Database initialization failed: ' + result.error);
                            }
                        } catch (error) {
                            showErrorMessage('Error initializing database: ' + error.message);
                        }
                    }

                    function showErrorMessage(message) {
                        const errorDiv = document.createElement('div');
                        errorDiv.className = 'error';
                        errorDiv.textContent = message;
                        errorDiv.style.position = 'fixed';
                        errorDiv.style.top = '20px';
                        errorDiv.style.right = '20px';
                        errorDiv.style.zIndex = '2000';
                        document.body.appendChild(errorDiv);
                        setTimeout(() => errorDiv.remove(), 5000);
                    }

                    function showSuccessMessage(message) {
                        const successDiv = document.createElement('div');
                        successDiv.className = 'success';
                        successDiv.textContent = message;
                        successDiv.style.position = 'fixed';
                        successDiv.style.top = '20px';
                        successDiv.style.right = '20px';
                        successDiv.style.zIndex = '2000';
                        document.body.appendChild(successDiv);
                        setTimeout(() => successDiv.remove(), 3000);
                    }

                    // Initialize the application when the page loads
                    document.addEventListener('DOMContentLoaded', init);
                </script>
            </body>
            </html>
        `);
    }

    // Get entities from database
    async handleGetEntities(req, res) {
        try {
            const pythonScript = `
import sys
sys.path.append('${path.dirname(__filename)}')
from database_manager import IPDBManager
import json

try:
    db = IPDBManager("/tmp/socionics_demo.db")
    entities = db.get_all_entities(limit=50)
    print(json.dumps({"success": True, "entities": entities}))
except Exception as e:
    print(json.dumps({"success": False, "error": str(e)}))
            `;

            const python = spawn('python3', ['-c', pythonScript]);
            let output = '';
            let error = '';

            python.stdout.on('data', (data) => output += data.toString());
            python.stderr.on('data', (data) => error += data.toString());

            python.on('close', (code) => {
                try {
                    const result = JSON.parse(output.trim());
                    res.writeHead(200, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify(result));
                } catch (e) {
                    res.writeHead(500, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({
                        success: false,
                        error: 'Failed to get entities',
                        python_error: error
                    }));
                }
            });

        } catch (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Get specific entity
    async handleGetEntity(req, res, parsedUrl, params) {
        try {
            const pythonScript = `
import sys
sys.path.append('${path.dirname(__filename)}')
from database_manager import IPDBManager
import json

try:
    db = IPDBManager("/tmp/socionics_demo.db")
    entity = db.get_entity('${params.id}')
    if entity:
        print(json.dumps({"success": True, "entity": entity}))
    else:
        print(json.dumps({"success": False, "error": "Entity not found"}))
except Exception as e:
    print(json.dumps({"success": False, "error": str(e)}))
            `;

            const python = spawn('python3', ['-c', pythonScript]);
            let output = '';

            python.stdout.on('data', (data) => output += data.toString());
            python.on('close', () => {
                try {
                    const result = JSON.parse(output.trim());
                    res.writeHead(result.success ? 200 : 404, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify(result));
                } catch (e) {
                    res.writeHead(500, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ success: false, error: 'Failed to get entity' }));
                }
            });

        } catch (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Create rating
    async handleCreateRating(req, res) {
        try {
            const rating = await this.getRequestBody(req);
            
            const pythonScript = `
import sys
sys.path.append('${path.dirname(__filename)}')
from database_manager import IPDBManager
import json
import uuid

try:
    db = IPDBManager("/tmp/socionics_demo.db")
    
    # Create rating record
    rating_id = str(uuid.uuid4())
    rating_data = {
        'id': rating_id,
        'entity_id': '${rating.entity_id}',
        'user': '${rating.user}',
        'personality_system': '${rating.personality_system}',
        'personality_type': '${rating.personality_type}',
        'confidence': ${rating.confidence || 0.6},
        'reasoning': '''${rating.reasoning || ''}''',
        'created_at': db.get_current_timestamp()
    }
    
    # Store in simple JSON format for now
    db.add_rating(rating_data)
    print(json.dumps({"success": True, "rating_id": rating_id}))
    
except Exception as e:
    print(json.dumps({"success": False, "error": str(e)}))
            `;

            const python = spawn('python3', ['-c', pythonScript]);
            let output = '';

            python.stdout.on('data', (data) => output += data.toString());
            python.on('close', () => {
                try {
                    const result = JSON.parse(output.trim());
                    res.writeHead(result.success ? 201 : 400, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify(result));
                } catch (e) {
                    res.writeHead(500, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ success: false, error: 'Failed to create rating' }));
                }
            });

        } catch (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Get ratings for entity
    async handleGetRatings(req, res, parsedUrl, params) {
        try {
            const pythonScript = `
import sys
sys.path.append('${path.dirname(__filename)}')
from database_manager import IPDBManager
import json

try:
    db = IPDBManager("/tmp/socionics_demo.db")
    ratings = db.get_entity_ratings('${params.entityId}')
    print(json.dumps({"success": True, "ratings": ratings}))
except Exception as e:
    print(json.dumps({"success": False, "error": str(e)}))
            `;

            const python = spawn('python3', ['-c', pythonScript]);
            let output = '';

            python.stdout.on('data', (data) => output += data.toString());
            python.on('close', () => {
                try {
                    const result = JSON.parse(output.trim());
                    res.writeHead(200, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify(result));
                } catch (e) {
                    res.writeHead(500, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ success: false, error: 'Failed to get ratings' }));
                }
            });

        } catch (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Create comment
    async handleCreateComment(req, res) {
        try {
            const comment = await this.getRequestBody(req);
            
            const pythonScript = `
import sys
sys.path.append('${path.dirname(__filename)}')
from database_manager import IPDBManager
import json
import uuid
from datetime import datetime

try:
    db = IPDBManager("/tmp/socionics_demo.db")
    
    # Create comment record
    comment_id = str(uuid.uuid4())
    comment_data = {
        'id': comment_id,
        'entity_id': '${comment.entity_id}',
        'user': '${comment.user}',
        'content': '''${comment.content}''',
        'created_at': datetime.now().isoformat()
    }
    
    db.add_comment(comment_data)
    print(json.dumps({"success": True, "comment_id": comment_id}))
    
except Exception as e:
    print(json.dumps({"success": False, "error": str(e)}))
            `;

            const python = spawn('python3', ['-c', pythonScript]);
            let output = '';

            python.stdout.on('data', (data) => output += data.toString());
            python.on('close', () => {
                try {
                    const result = JSON.parse(output.trim());
                    res.writeHead(result.success ? 201 : 400, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify(result));
                } catch (e) {
                    res.writeHead(500, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ success: false, error: 'Failed to create comment' }));
                }
            });

        } catch (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Get comments for entity
    async handleGetComments(req, res, parsedUrl, params) {
        try {
            const pythonScript = `
import sys
sys.path.append('${path.dirname(__filename)}')
from database_manager import IPDBManager
import json

try:
    db = IPDBManager("/tmp/socionics_demo.db")
    comments = db.get_entity_comments('${params.entityId}')
    print(json.dumps({"success": True, "comments": comments}))
except Exception as e:
    print(json.dumps({"success": False, "error": str(e)}))
            `;

            const python = spawn('python3', ['-c', pythonScript]);
            let output = '';

            python.stdout.on('data', (data) => output += data.toString());
            python.on('close', () => {
                try {
                    const result = JSON.parse(output.trim());
                    res.writeHead(200, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify(result));
                } catch (e) {
                    res.writeHead(500, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ success: false, error: 'Failed to get comments' }));
                }
            });

        } catch (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Create user
    async handleCreateUser(req, res) {
        try {
            const user = await this.getRequestBody(req);
            
            const pythonScript = `
import sys
sys.path.append('${path.dirname(__filename)}')
from database_manager import IPDBManager
import json
import uuid

try:
    db = IPDBManager("/tmp/socionics_demo.db")
    
    # Create user record
    user_id = str(uuid.uuid4())
    user_data = {
        'id': user_id,
        'username': '${user.username}',
        'display_name': '${user.display_name || user.username}',
        'role': '${user.role || 'annotator'}',
        'experience_level': '${user.experience_level || 'novice'}',
        'created_at': db.get_current_timestamp()
    }
    
    db.add_user(user_data)
    print(json.dumps({"success": True, "user_id": user_id}))
    
except Exception as e:
    print(json.dumps({"success": False, "error": str(e)}))
            `;

            const python = spawn('python3', ['-c', pythonScript]);
            let output = '';

            python.stdout.on('data', (data) => output += data.toString());
            python.on('close', () => {
                try {
                    const result = JSON.parse(output.trim());
                    res.writeHead(result.success ? 201 : 400, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify(result));
                } catch (e) {
                    res.writeHead(500, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ success: false, error: 'Failed to create user' }));
                }
            });

        } catch (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
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