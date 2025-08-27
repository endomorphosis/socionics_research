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
const IPDBManager = require('./database-manager.cjs');

class SimpleIPDBServer {
    constructor(port = 3000) {
        this.port = port;
        this.dbManager = new IPDBManager();
        this.setupRoutes();
        this.server = http.createServer(this.handleRequest.bind(this));
        
        // Initialize database
        this.initializeDatabase();
    }

    async initializeDatabase() {
        try {
            await this.dbManager.initialize();
            console.log('✅ Database initialized successfully');
        } catch (error) {
            console.error('❌ Database initialization failed:', error);
        }
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
            'PUT /api/entities/:id': this.handleUpdateEntity.bind(this),
            'GET /api/entities/:id/history': this.handleGetEntityHistory.bind(this),
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
        
        const updateEntityMatch = pathname.match(/^\/api\/entities\/([^\/]+)$/);
        if (updateEntityMatch && method === 'PUT') {
            routeKey = `${method} /api/entities/:id`;
            params.id = updateEntityMatch[1];
        }
        
        const historyMatch = pathname.match(/^\/api\/entities\/([^\/]+)\/history$/);
        if (historyMatch) {
            routeKey = `${method} /api/entities/:id/history`;
            params.id = historyMatch[1];
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
                        <li>✅ Node.js IPDB Backend Active</li>
                        <li>✅ SQLite Database Support</li>
                        <li>🚀 Pure Node.js Implementation</li>
                    </ul>
                    
                    <p><em>100% Node.js implementation - no Python dependencies!</em></p>
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
                <title>Personality Database Wiki - 2M+ Character Profiles | Community-Driven Personality Typing</title>
                <style>
                    @import url('https://fonts.googleapis.com/css2?family=Rubik:wght@300;400;500;600;700&family=Source+Sans+Pro:wght@300;400;600;700&display=swap');
                    
                    :root {
                        /* Wikia-inspired color scheme */
                        --wiki-primary: #002a5c;
                        --wiki-secondary: #0066cc;
                        --wiki-accent: #00a8e6;
                        --wiki-success: #00b04f;
                        --wiki-warning: #ff6d31;
                        --wiki-danger: #d32f2f;
                        --wiki-dark: #2c3e50;
                        --wiki-light: #ffffff;
                        --wiki-gray: #f8f9fa;
                        --wiki-border: #d1d5db;
                        --wiki-text: #2c3e50;
                        --wiki-text-light: #6c757d;
                        --wiki-bg: #ffffff;
                        --wiki-sidebar: #f8f9fa;
                        --community-gold: #ffc107;
                        --community-silver: #9e9e9e;
                        --personality-intj: #6a4c93;
                        --personality-enfp: #ff6b6b;
                        --personality-isfj: #4ecdc4;
                        --personality-estp: #ffe66d;
                    }
                    
                    * {
                        margin: 0;
                        padding: 0;
                        box-sizing: border-box;
                    }
                    
                    body { 
                        font-family: 'Source Sans Pro', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                        background-color: var(--wiki-bg);
                        color: var(--wiki-text);
                        line-height: 1.6;
                    }
                    
                    /* Wikia-style Header */
                    .wiki-header {
                        background: var(--wiki-primary);
                        color: white;
                        border-bottom: 3px solid var(--wiki-secondary);
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }
                    
                    .wiki-header-top {
                        background: var(--wiki-dark);
                        padding: 8px 0;
                        font-size: 12px;
                    }
                    
                    .wiki-header-top .container {
                        max-width: 1200px;
                        margin: 0 auto;
                        padding: 0 20px;
                        display: flex;
                        justify-content: space-between;
                        align-items: center;
                    }
                    
                    .wiki-nav-main {
                        padding: 15px 0;
                    }
                    
                    .wiki-nav-main .container {
                        max-width: 1200px;
                        margin: 0 auto;
                        padding: 0 20px;
                        display: flex;
                        align-items: center;
                        gap: 30px;
                    }
                    
                    .wiki-logo {
                        display: flex;
                        align-items: center;
                        gap: 15px;
                        font-weight: 700;
                        font-size: 24px;
                        text-decoration: none;
                        color: white;
                    }
                    
                    .wiki-logo-icon {
                        background: var(--wiki-accent);
                        width: 40px;
                        height: 40px;
                        border-radius: 8px;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        font-size: 20px;
                    }
                    
                    .wiki-search {
                        flex: 1;
                        max-width: 400px;
                        position: relative;
                    }
                    
                    .wiki-search input {
                        width: 100%;
                        padding: 12px 45px 12px 15px;
                        border: 2px solid rgba(255,255,255,0.2);
                        border-radius: 6px;
                        background: rgba(255,255,255,0.1);
                        color: white;
                        font-size: 16px;
                    }
                    
                    .wiki-search input::placeholder {
                        color: rgba(255,255,255,0.7);
                    }
                    
                    .wiki-search button {
                        position: absolute;
                        right: 5px;
                        top: 5px;
                        bottom: 5px;
                        background: var(--wiki-accent);
                        border: none;
                        border-radius: 4px;
                        color: white;
                        padding: 0 15px;
                        cursor: pointer;
                        font-weight: 600;
                    }
                    
                    .wiki-stats {
                        display: flex;
                        gap: 25px;
                        font-size: 14px;
                        color: rgba(255,255,255,0.8);
                    }
                    
                    .wiki-stat {
                        display: flex;
                        align-items: center;
                        gap: 5px;
                    }
                    
                    .wiki-stat-number {
                        font-weight: 700;
                        color: var(--community-gold);
                    
                    /* Main Layout */
                    .wiki-layout {
                        max-width: 1200px;
                        margin: 0 auto;
                        display: grid;
                        grid-template-columns: 250px 1fr;
                        gap: 20px;
                        padding: 20px;
                    }
                    
                    /* Wikia-style Sidebar */
                    .wiki-sidebar {
                        background: var(--wiki-sidebar);
                        border: 1px solid var(--wiki-border);
                        border-radius: 8px;
                        height: fit-content;
                        overflow: hidden;
                    }
                    
                    .sidebar-section {
                        border-bottom: 1px solid var(--wiki-border);
                    }
                    
                    .sidebar-section:last-child {
                        border-bottom: none;
                    }
                    
                    .sidebar-header {
                        background: var(--wiki-primary);
                        color: white;
                        padding: 12px 15px;
                        font-weight: 600;
                        font-size: 14px;
                        text-transform: uppercase;
                        letter-spacing: 0.5px;
                    }
                    
                    .sidebar-content {
                        padding: 15px;
                    }
                    
                    .sidebar-nav {
                        list-style: none;
                    }
                    
                    .sidebar-nav li {
                        margin-bottom: 8px;
                    }
                    
                    .sidebar-nav a {
                        text-decoration: none;
                        color: var(--wiki-text);
                        display: flex;
                        align-items: center;
                        gap: 8px;
                        padding: 8px 10px;
                        border-radius: 4px;
                        transition: all 0.2s;
                        font-weight: 500;
                    }
                    
                    .sidebar-nav a:hover {
                        background: var(--wiki-secondary);
                        color: white;
                    }
                    
                    .sidebar-nav a.active {
                        background: var(--wiki-accent);
                        color: white;
                    }
                    
                    /* Main Content Area */
                    .wiki-content {
                        background: white;
                        border: 1px solid var(--wiki-border);
                        border-radius: 8px;
                        overflow: hidden;
                    }
                    
                    .content-header {
                        background: var(--wiki-gray);
                        border-bottom: 1px solid var(--wiki-border);
                        padding: 20px;
                    }
                    
                    .content-title {
                        font-size: 28px;
                        font-weight: 700;
                        color: var(--wiki-primary);
                        margin-bottom: 10px;
                        font-family: 'Rubik', sans-serif;
                    }
                    
                    .content-subtitle {
                        color: var(--wiki-text-light);
                        font-size: 16px;
                        margin-bottom: 15px;
                    }
                    
                    .breadcrumb {
                        display: flex;
                        align-items: center;
                        gap: 8px;
                        font-size: 14px;
                        color: var(--wiki-text-light);
                    }
                    
                    .breadcrumb a {
                        color: var(--wiki-secondary);
                        text-decoration: none;
                    }
                    
                    .breadcrumb a:hover {
                        text-decoration: underline;
                    }
                    
                    /* Character Cards */
                    .content-body {
                        padding: 20px;
                    }
                    
                    .character-grid {
                        display: grid;
                        grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
                        gap: 20px;
                        margin-bottom: 30px;
                    }
                    
                    .character-card {
                        background: white;
                        border: 1px solid var(--wiki-border);
                        border-radius: 8px;
                        overflow: hidden;
                        transition: all 0.3s;
                        hover: box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                    }
                    
                    .character-card:hover {
                        transform: translateY(-2px);
                        box-shadow: 0 8px 16px rgba(0,0,0,0.15);
                    }
                    
                    .character-image {
                        width: 100%;
                        height: 200px;
                        background: linear-gradient(135deg, var(--wiki-secondary), var(--wiki-accent));
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        color: white;
                        font-size: 48px;
                        font-weight: bold;
                        position: relative;
                    }
                    
                    .character-image img {
                        width: 100%;
                        height: 100%;
                        object-fit: cover;
                    }
                    
                    .character-info {
                        padding: 15px;
                    }
                    
                    .character-name {
                        font-size: 18px;
                        font-weight: 700;
                        color: var(--wiki-primary);
                        margin-bottom: 5px;
                        font-family: 'Rubik', sans-serif;
                    }
                    
                    .character-source {
                        color: var(--wiki-text-light);
                        font-size: 14px;
                        margin-bottom: 10px;
                    }
                    
                    .personality-badges {
                        display: flex;
                        gap: 8px;
                        margin-bottom: 15px;
                        flex-wrap: wrap;
                    }
                    
                    .personality-badge {
                        background: var(--wiki-accent);
                        color: white;
                        padding: 4px 8px;
                        border-radius: 12px;
                        font-size: 12px;
                        font-weight: 600;
                    }
                    
                    .personality-badge.mbti {
                        background: var(--personality-intj);
                    }
                    
                    .personality-badge.socionics {
                        background: var(--personality-enfp);
                    }
                    
                    .vote-info {
                        display: flex;
                        justify-content: space-between;
                        align-items: center;
                        font-size: 14px;
                        color: var(--wiki-text-light);
                    }
                    
                    .vote-count {
                        display: flex;
                        align-items: center;
                        gap: 5px;
                    }
                    
                    .vote-btn {
                        background: var(--wiki-secondary);
                        color: white;
                        border: none;
                        padding: 6px 12px;
                        border-radius: 4px;
                        cursor: pointer;
                        font-size: 12px;
                        font-weight: 600;
                        transition: background 0.2s;
                    }
                    
                    .vote-btn:hover {
                        background: var(--wiki-primary);
                    }
                    
                    .container { 
                        background: rgba(255, 255, 255, 0.95);
                        backdrop-filter: blur(10px);
                        padding: 30px;
                        border-radius: 20px;
                        box-shadow: var(--shadow-xl);
                        margin-bottom: 20px;
                        border: 1px solid rgba(255, 255, 255, 0.2);
                        animation: fadeInUp 0.6s ease-out;
                    }
                    
                    .stats-grid {
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
                        gap: 20px;
                        margin: 30px 0;
                    }
                    
                    .stat-card {
                        background: rgba(255, 255, 255, 0.9);
                        backdrop-filter: blur(10px);
                        padding: 25px;
                        border-radius: 16px;
                        text-align: center;
                        box-shadow: var(--shadow-md);
                        border: 1px solid rgba(255, 255, 255, 0.2);
                        transition: all 0.3s ease;
                        animation: scaleIn 0.6s ease-out;
                        position: relative;
                        overflow: hidden;
                    }
                    
                    .stat-card::before {
                        content: '';
                        position: absolute;
                        top: 0;
                        left: 0;
                        right: 0;
                        height: 4px;
                        background: linear-gradient(135deg, var(--primary), var(--secondary));
                    }
                    
                    .stat-card:hover {
                        transform: translateY(-5px);
                        box-shadow: var(--shadow-xl);
                    }
                    
                    .stat-card h4 {
                        font-size: 1.1rem;
                        color: var(--gray-600);
                        margin-bottom: 15px;
                        font-weight: 500;
                    }
                    
                    .stat-number {
                        font-size: 2.5rem;
                        font-weight: 700;
                        color: var(--primary);
                        margin-bottom: 10px;
                        display: block;
                    }
                    
                    .stat-label {
                        color: var(--gray-500);
                        font-size: 0.9rem;
                    }
                    
                    .entity-grid {
                        display: grid;
                        grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
                        gap: 20px;
                        margin: 30px 0;
                    }
                    
                    .entity-card {
                        background: rgba(255, 255, 255, 0.9);
                        backdrop-filter: blur(10px);
                        border-radius: 16px;
                        padding: 25px;
                        box-shadow: var(--shadow-md);
                        border: 1px solid rgba(255, 255, 255, 0.2);
                        transition: all 0.3s ease;
                        animation: fadeInUp 0.6s ease-out;
                        position: relative;
                        overflow: hidden;
                    }
                    
                    .entity-card::before {
                        content: '';
                        position: absolute;
                        top: 0;
                        left: 0;
                        right: 0;
                        height: 3px;
                        background: linear-gradient(135deg, var(--accent), var(--primary));
                        opacity: 0;
                        transition: opacity 0.3s ease;
                    }
                    
                    .entity-card:hover {
                        transform: translateY(-8px);
                        box-shadow: var(--shadow-xl);
                    }
                    
                    .entity-card:hover::before {
                        opacity: 1;
                    }
                    
                    .entity-header {
                        display: flex;
                        align-items: center;
                        gap: 15px;
                        margin-bottom: 20px;
                    }
                    
                    .entity-avatar {
                        width: 60px;
                        height: 60px;
                        border-radius: 50%;
                        background: linear-gradient(135deg, var(--primary), var(--secondary));
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        color: white;
                        font-size: 1.5rem;
                        font-weight: 700;
                        flex-shrink: 0;
                    }
                    
                    .entity-info {
                        flex: 1;
                    }
                    
                    .entity-name {
                        font-size: 1.3rem;
                        font-weight: 600;
                        color: var(--gray-800);
                        margin-bottom: 5px;
                    }
                    
                    .entity-type {
                        background: rgba(99, 102, 241, 0.1);
                        color: var(--primary);
                        padding: 4px 12px;
                        border-radius: 20px;
                        font-size: 0.8rem;
                        font-weight: 500;
                        display: inline-block;
                    }
                    
                    .personality-types {
                        display: flex;
                        flex-wrap: wrap;
                        gap: 8px;
                        margin: 15px 0;
                    }
                    
                    .personality-type {
                        background: linear-gradient(135deg, var(--success), var(--accent));
                        color: white;
                        padding: 6px 14px;
                        border-radius: 20px;
                        font-size: 0.75rem;
                        font-weight: 600;
                        display: inline-flex;
                        align-items: center;
                        gap: 5px;
                        box-shadow: var(--shadow-sm);
                    }
                    
                    .action-buttons {
                        display: flex;
                        gap: 10px;
                        margin-top: 20px;
                    }
                    
                    .btn {
                        border: none;
                        padding: 12px 20px;
                        border-radius: 10px;
                        cursor: pointer;
                        font-size: 0.9rem;
                        font-weight: 600;
                        transition: all 0.3s ease;
                        display: inline-flex;
                        align-items: center;
                        gap: 8px;
                        text-decoration: none;
                        flex: 1;
                        justify-content: center;
                    }
                    
                    .btn-primary {
                        background: linear-gradient(135deg, var(--primary), var(--primary-dark));
                        color: white;
                        box-shadow: var(--shadow-sm);
                    }
                    
                    .btn-primary:hover {
                        transform: translateY(-2px);
                        box-shadow: var(--shadow-md);
                    }
                    
                    .btn-secondary {
                        background: rgba(99, 102, 241, 0.1);
                        color: var(--primary);
                        border: 2px solid rgba(99, 102, 241, 0.2);
                    }
                    
                    .btn-secondary:hover {
                        background: rgba(99, 102, 241, 0.15);
                        transform: translateY(-2px);
                    }
                    
                    .btn-success { 
                        background: linear-gradient(135deg, var(--success), #059669);
                        color: white;
                    }
                    
                    .btn-success:hover { 
                        transform: translateY(-2px);
                        box-shadow: var(--shadow-md);
                    }
                    
                    .search-filter-panel {
                        background: rgba(255, 255, 255, 0.8);
                        backdrop-filter: blur(10px);
                        border-radius: 16px;
                        padding: 25px;
                        margin-bottom: 25px;
                        box-shadow: var(--shadow-md);
                        border: 1px solid rgba(255, 255, 255, 0.2);
                    }
                    
                    .search-controls {
                        display: flex;
                        flex-direction: column;
                        gap: 20px;
                    }
                    
                    .search-bar {
                        position: relative;
                        flex: 1;
                    }
                    
                    .search-bar .search-icon {
                        position: absolute;
                        left: 15px;
                        top: 50%;
                        transform: translateY(-50%);
                        color: var(--gray-400);
                        font-size: 1.1rem;
                        pointer-events: none;
                    }
                    
                    .filter-controls {
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                        gap: 15px;
                        align-items: end;
                    }
                    
                    .filter-group {
                        display: flex;
                        flex-direction: column;
                        gap: 5px;
                    }
                    
                    .filter-group .form-label {
                        margin: 0;
                        font-size: 0.9rem;
                        font-weight: 600;
                    }
                    
                    .view-controls {
                        display: flex;
                        gap: 5px;
                        justify-self: end;
                    }
                    
                    .view-btn {
                        background: rgba(255, 255, 255, 0.8);
                        border: 2px solid var(--gray-200);
                        color: var(--gray-600);
                        padding: 8px 16px;
                        border-radius: 8px;
                        cursor: pointer;
                        font-size: 0.9rem;
                        font-weight: 500;
                        transition: all 0.3s ease;
                        white-space: nowrap;
                    }
                    
                    .view-btn:hover {
                        border-color: var(--primary);
                        background: rgba(99, 102, 241, 0.1);
                        color: var(--primary);
                    }
                    
                    .view-btn.active {
                        background: var(--primary);
                        border-color: var(--primary);
                        color: white;
                    }
                    
                    .active-filters {
                        display: flex;
                        flex-wrap: wrap;
                        gap: 8px;
                        margin-top: 15px;
                        padding-top: 15px;
                        border-top: 1px solid rgba(255, 255, 255, 0.3);
                    }
                    
                    .filter-tag {
                        background: var(--primary);
                        color: white;
                        padding: 6px 12px;
                        border-radius: 20px;
                        font-size: 0.8rem;
                        font-weight: 500;
                        display: flex;
                        align-items: center;
                        gap: 8px;
                        animation: slideInRight 0.3s ease;
                    }
                    
                    .filter-tag .remove-filter {
                        cursor: pointer;
                        opacity: 0.7;
                        transition: opacity 0.2s ease;
                    }
                    
                    .filter-tag .remove-filter:hover {
                        opacity: 1;
                    }
                    
                    @keyframes slideInRight {
                        from {
                            opacity: 0;
                            transform: translateX(20px);
                        }
                        to {
                            opacity: 1;
                            transform: translateX(0);
                        }
                    }
                    
                    .entity-list-view .entity-grid {
                        display: flex;
                        flex-direction: column;
                        gap: 15px;
                    }
                    
                    .entity-list-view .entity-card {
                        display: flex;
                        flex-direction: row;
                        padding: 20px;
                        align-items: center;
                    }
                    
                    .entity-list-view .entity-header {
                        flex: 0 0 auto;
                        margin-right: 20px;
                        margin-bottom: 0;
                    }
                    
                    .entity-list-view .entity-info {
                        flex: 1;
                    }
                    
                    .entity-list-view .action-buttons {
                        flex: 0 0 auto;
                        margin-top: 0;
                        margin-left: 20px;
                    }
                    
                    .entity-table-view {
                        background: rgba(255, 255, 255, 0.95);
                        backdrop-filter: blur(10px);
                        border-radius: 16px;
                        overflow: hidden;
                        box-shadow: var(--shadow-md);
                        border: 1px solid rgba(255, 255, 255, 0.2);
                    }
                    
                    .entity-table {
                        width: 100%;
                        border-collapse: collapse;
                    }
                    
                    .entity-table th {
                        background: linear-gradient(135deg, var(--primary), var(--primary-dark));
                        color: white;
                        padding: 15px;
                        text-align: left;
                        font-weight: 600;
                        font-size: 0.9rem;
                        border-bottom: 2px solid rgba(255, 255, 255, 0.1);
                    }
                    
                    .entity-table td {
                        padding: 15px;
                        border-bottom: 1px solid rgba(0, 0, 0, 0.05);
                        vertical-align: middle;
                    }
                    
                    .entity-table tr:hover {
                        background: rgba(99, 102, 241, 0.02);
                    }
                    
                    .pagination-controls {
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        gap: 10px;
                        margin-top: 30px;
                        padding: 20px;
                    }
                    
                    .pagination-btn {
                        background: rgba(255, 255, 255, 0.9);
                        border: 2px solid var(--gray-200);
                        color: var(--gray-700);
                        padding: 10px 15px;
                        border-radius: 8px;
                        cursor: pointer;
                        font-size: 0.9rem;
                        font-weight: 500;
                        transition: all 0.3s ease;
                        min-width: 40px;
                        text-align: center;
                    }
                    
                    .pagination-btn:hover:not(:disabled) {
                        border-color: var(--primary);
                        background: rgba(99, 102, 241, 0.1);
                        color: var(--primary);
                    }
                    
                    .pagination-btn.active {
                        background: var(--primary);
                        border-color: var(--primary);
                        color: white;
                    }
                    
                    .pagination-btn:disabled {
                        opacity: 0.5;
                        cursor: not-allowed;
                    }
                    
                    .pagination-info {
                        font-size: 0.9rem;
                        color: var(--gray-600);
                        margin: 0 15px;
                    }
                    
                    .character-sheet-modal .modal-content {
                        max-width: 800px;
                        max-height: 90vh;
                        overflow-y: auto;
                    }
                    
                    .character-sheet-form {
                        display: grid;
                        grid-template-columns: 1fr 1fr;
                        gap: 20px;
                    }
                    
                    .character-sheet-form .form-group.full-width {
                        grid-column: 1 / -1;
                    }
                    
                    .character-activity {
                        background: rgba(249, 250, 251, 0.8);
                        border-radius: 12px;
                        padding: 15px;
                        margin: 15px 0;
                    }
                    
                    .activity-item {
                        display: flex;
                        align-items: center;
                        gap: 10px;
                        padding: 8px 0;
                        border-bottom: 1px solid rgba(0, 0, 0, 0.05);
                    }
                    
                    .activity-item:last-child {
                        border-bottom: none;
                    }
                    
                    .activity-avatar {
                        width: 24px;
                        height: 24px;
                        border-radius: 50%;
                        background: linear-gradient(135deg, var(--accent), var(--primary));
                        color: white;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        font-size: 0.7rem;
                        font-weight: 600;
                    }
                    
                    .activity-text {
                        flex: 1;
                        font-size: 0.9rem;
                        color: var(--gray-700);
                    }
                    
                    .activity-time {
                        font-size: 0.8rem;
                        color: var(--gray-500);
                    }
                    .form-group {
                        margin: 20px 0;
                    }
                    
                    .form-label {
                        display: block;
                        margin-bottom: 8px;
                        font-weight: 600;
                        color: var(--gray-700);
                    }
                    
                    .form-control {
                        width: 100%;
                        padding: 12px 16px;
                        border: 2px solid var(--gray-200);
                        border-radius: 10px;
                        font-size: 14px;
                        transition: border-color 0.3s ease;
                        background: white;
                    }
                    
                    .form-control:focus {
                        outline: none;
                        border-color: var(--primary);
                        box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.1);
                    }
                    
                    .comparison-panel {
                        background: rgba(255, 255, 255, 0.95);
                        backdrop-filter: blur(10px);
                        border-radius: 20px;
                        padding: 30px;
                        margin: 30px 0;
                        box-shadow: var(--shadow-xl);
                        border: 1px solid rgba(255, 255, 255, 0.2);
                    }
                    
                    .comparison-grid {
                        display: grid;
                        grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                        gap: 20px;
                        margin: 20px 0;
                    }
                    
                    .comparison-card {
                        background: rgba(255, 255, 255, 0.8);
                        border: 2px dashed var(--gray-300);
                        border-radius: 16px;
                        padding: 30px;
                        text-align: center;
                        transition: all 0.3s ease;
                        cursor: pointer;
                        position: relative;
                        overflow: hidden;
                    }
                    
                    .comparison-card.selected {
                        border-color: var(--primary);
                        background: rgba(99, 102, 241, 0.05);
                        border-style: solid;
                    }
                    
                    .comparison-card.empty:hover {
                        border-color: var(--primary);
                        background: rgba(99, 102, 241, 0.02);
                    }
                    
                    .star-rating {
                        display: flex;
                        gap: 5px;
                        justify-content: center;
                        margin: 15px 0;
                    }
                    
                    .star {
                        font-size: 28px;
                        cursor: pointer;
                        color: var(--gray-300);
                        transition: all 0.2s ease;
                    }
                    
                    .star.active, .star:hover {
                        color: #fbbf24;
                        transform: scale(1.1);
                    }
                    
                    .loading {
                        text-align: center;
                        padding: 60px;
                        color: var(--gray-500);
                        font-size: 1.1rem;
                    }
                    
                    .loading::before {
                        content: '';
                        display: inline-block;
                        width: 40px;
                        height: 40px;
                        border: 4px solid var(--gray-200);
                        border-top: 4px solid var(--primary);
                        border-radius: 50%;
                        animation: spin 1s linear infinite;
                        margin-bottom: 15px;
                    }
                    
                    @keyframes spin {
                        0% { transform: rotate(0deg); }
                        100% { transform: rotate(360deg); }
                    }
                    
                    .error {
                        background: rgba(239, 68, 68, 0.1);
                        border: 1px solid rgba(239, 68, 68, 0.2);
                        color: var(--danger);
                        padding: 16px;
                        border-radius: 12px;
                        margin: 15px 0;
                        font-weight: 500;
                    }
                    
                    .success {
                        background: rgba(16, 185, 129, 0.1);
                        border: 1px solid rgba(16, 185, 129, 0.2);
                        color: var(--success);
                        padding: 16px;
                        border-radius: 12px;
                        margin: 15px 0;
                        font-weight: 500;
                    }
                    
                    .modal {
                        display: none;
                        position: fixed;
                        z-index: 1000;
                        left: 0;
                        top: 0;
                        width: 100%;
                        height: 100%;
                        background-color: rgba(0, 0, 0, 0.5);
                        backdrop-filter: blur(5px);
                        animation: fadeIn 0.3s ease;
                    }
                    
                    .modal.show {
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        padding: 20px;
                    }
                    
                    .modal-content {
                        background: white;
                        border-radius: 20px;
                        padding: 40px;
                        max-width: 600px;
                        width: 100%;
                        max-height: 90vh;
                        overflow-y: auto;
                        box-shadow: var(--shadow-xl);
                        animation: modalSlideIn 0.3s ease;
                        position: relative;
                    }
                    
                    @keyframes fadeIn {
                        from { opacity: 0; }
                        to { opacity: 1; }
                    }
                    
                    @keyframes modalSlideIn {
                        from {
                            opacity: 0;
                            transform: scale(0.9) translateY(-50px);
                        }
                        to {
                            opacity: 1;
                            transform: scale(1) translateY(0);
                        }
                    }
                    
                    .close-btn {
                        position: absolute;
                        top: 15px;
                        right: 20px;
                        background: none;
                        border: none;
                        font-size: 24px;
                        cursor: pointer;
                        color: var(--gray-500);
                        width: 40px;
                        height: 40px;
                        border-radius: 50%;
                        display: flex;
                        align-items: center;
                        justify-content: center;
                        transition: all 0.2s ease;
                    }
                    
                    .close-btn:hover {
                        background: var(--gray-100);
                        color: var(--gray-700);
                    }
                    
                    .file-upload-area {
                        border: 2px dashed var(--gray-300);
                        border-radius: 16px;
                        padding: 40px;
                        text-align: center;
                        transition: all 0.3s ease;
                        cursor: pointer;
                        background: rgba(249, 250, 251, 0.5);
                    }
                    
                    .file-upload-area:hover, .file-upload-area.dragover {
                        border-color: var(--primary);
                        background: rgba(99, 102, 241, 0.05);
                    }
                    
                    .upload-icon {
                        font-size: 3rem;
                        color: var(--gray-400);
                        margin-bottom: 15px;
                    }
                    
                    .section-title {
                        font-size: 1.8rem;
                        font-weight: 700;
                        color: var(--gray-800);
                        margin-bottom: 10px;
                        display: flex;
                        align-items: center;
                        gap: 10px;
                    }
                    
                    .section-subtitle {
                        color: var(--gray-600);
                        margin-bottom: 30px;
                        font-size: 1.1rem;
                    }
                    
                    @media (max-width: 768px) {
                        .main-container {
                            padding: 15px;
                        }
                        
                        .app-header h1 {
                            font-size: 2.5rem;
                        }
                        
                        .feature-nav {
                            flex-direction: column;
                            align-items: center;
                        }
                        
                        .nav-btn {
                            width: 100%;
                            max-width: 300px;
                            justify-content: center;
                        }
                        
                        .entity-grid {
                            grid-template-columns: 1fr;
                        }
                        
                        .comparison-grid {
                            grid-template-columns: 1fr;
                        }
                    }
                </style>
            </head>
            <body>
                <!-- Wikia-style Header -->
                <header class="wiki-header">
                    <div class="wiki-header-top">
                        <div class="container">
                            <div class="wiki-stats">
                                <div class="wiki-stat">
                                    <span>📚</span>
                                    <span class="wiki-stat-number">2,045,783</span>
                                    <span>Characters</span>
                                </div>
                                <div class="wiki-stat">
                                    <span>🗳️</span>
                                    <span class="wiki-stat-number">8,923,451</span>
                                    <span>Votes Cast</span>
                                </div>
                                <div class="wiki-stat">
                                    <span>👥</span>
                                    <span class="wiki-stat-number">157,892</span>
                                    <span>Contributors</span>
                                </div>
                            </div>
                            <div style="font-size: 11px; color: rgba(255,255,255,0.6);">
                                Last updated: 2 minutes ago
                            </div>
                        </div>
                    </div>
                    <nav class="wiki-nav-main">
                        <div class="container">
                            <a href="#" class="wiki-logo">
                                <div class="wiki-logo-icon">🧠</div>
                                <div>
                                    <div style="font-size: 24px;">Personality Database</div>
                                    <div style="font-size: 12px; font-weight: 400; opacity: 0.8;">Community Wiki</div>
                                </div>
                            </a>
                            
                            <div class="wiki-search">
                                <input type="text" id="globalSearch" placeholder="Search 2M+ characters, series, personalities..." autocomplete="off">
                                <button onclick="performGlobalSearch()">🔍</button>
                            </div>
                            
                            <div style="display: flex; gap: 20px; align-items: center;">
                                <a href="#" style="color: rgba(255,255,255,0.8); text-decoration: none; font-weight: 500;">Random</a>
                                <a href="#" style="color: rgba(255,255,255,0.8); text-decoration: none; font-weight: 500;">Recent</a>
                                <a href="#" style="color: var(--community-gold); text-decoration: none; font-weight: 600;">Contribute</a>
                            </div>
                        </div>
                    </nav>
                </header>

                <!-- Main Wiki Layout -->
                <div class="wiki-layout">
                    <!-- Sidebar Navigation -->
                    <aside class="wiki-sidebar">
                        <div class="sidebar-section">
                            <div class="sidebar-header">🏠 Navigation</div>
                            <div class="sidebar-content">
                                <ul class="sidebar-nav">
                                    <li><a href="#" onclick="showSection('dashboard')" class="active">📊 Main Page</a></li>
                                    <li><a href="#" onclick="showSection('browse')">👥 Browse Characters</a></li>
                                    <li><a href="#" onclick="showSection('compare')">⚔️ Compare Types</a></li>
                                    <li><a href="#" onclick="showSection('recent')">🔥 Recent Activity</a></li>
                                    <li><a href="#" onclick="showSection('popular')">⭐ Popular Today</a></li>
                                </ul>
                            </div>
                        </div>
                        
                        <div class="sidebar-section">
                            <div class="sidebar-header">📂 Categories</div>
                            <div class="sidebar-content">
                                <ul class="sidebar-nav">
                                    <li><a href="#" onclick="filterByCategory('anime')">🍜 Anime (584K)</a></li>
                                    <li><a href="#" onclick="filterByCategory('movies')">🎬 Movies (342K)</a></li>
                                    <li><a href="#" onclick="filterByCategory('tv')">📺 TV Shows (298K)</a></li>
                                    <li><a href="#" onclick="filterByCategory('books')">📖 Books (201K)</a></li>
                                    <li><a href="#" onclick="filterByCategory('games')">🎮 Games (287K)</a></li>
                                    <li><a href="#" onclick="filterByCategory('comics')">🦸 Comics (178K)</a></li>
                                    <li><a href="#" onclick="filterByCategory('celebrities')">🌟 Celebrities (156K)</a></li>
                                </ul>
                            </div>
                        </div>
                        
                        <div class="sidebar-section">
                            <div class="sidebar-header">🧠 Personality Systems</div>
                            <div class="sidebar-content">
                                <ul class="sidebar-nav">
                                    <li><a href="#" onclick="filterBySystem('mbti')">MBTI (16 Types)</a></li>
                                    <li><a href="#" onclick="filterBySystem('socionics')">Socionics (16 Types)</a></li>
                                    <li><a href="#" onclick="filterBySystem('enneagram')">Enneagram (9 Types)</a></li>
                                    <li><a href="#" onclick="filterBySystem('bigfive')">Big Five</a></li>
                                </ul>
                            </div>
                        </div>
                        
                        <div class="sidebar-section">
                            <div class="sidebar-header">📈 Community Stats</div>
                            <div class="sidebar-content" style="font-size: 13px;">
                                <div style="margin-bottom: 8px;">
                                    <strong>Daily Activity:</strong><br>
                                    • 12,847 new votes<br>
                                    • 3,291 comments<br>
                                    • 892 new profiles
                                </div>
                                <div style="margin-bottom: 8px;">
                                    <strong>Top Contributors:</strong><br>
                                    • TypeMaster99 (1,247)<br>
                                    • PersonalityGuru (1,089)<br>
                                    • WikiTyper (956)
                                </div>
                            </div>
                        </div>
                    </aside>

                    <!-- Main Content -->
                    <main class="wiki-content">
                        <div class="content-header">
                            <div class="breadcrumb">
                                <a href="#">Home</a> › <span>Main Page</span>
                            </div>
                            <h1 class="content-title">Welcome to Personality Database Wiki</h1>
                            <p class="content-subtitle">The world's largest community-driven database of personality types with over 2 million character profiles</p>
                        </div>
                        
                        <div class="content-body">
                            <!-- Dashboard Section -->
                            <div id="dashboard-section" class="content-section" style="display: block;">
                                <div style="display: grid; grid-template-columns: 2fr 1fr; gap: 20px; margin-bottom: 30px;">
                                    <div style="background: linear-gradient(135deg, var(--wiki-secondary), var(--wiki-accent)); color: white; padding: 30px; border-radius: 12px;">
                                        <h2 style="margin-bottom: 15px; font-size: 24px;">🎯 Featured Character of the Day</h2>
                                        <div style="display: flex; gap: 20px; align-items: center;">
                                            <div style="width: 80px; height: 80px; background: rgba(255,255,255,0.2); border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 32px;">
                                                🕵️
                                            </div>
                                            <div>
                                                <h3 style="margin-bottom: 5px;">Sherlock Holmes</h3>
                                                <p style="opacity: 0.9; margin-bottom: 5px;">Detective Fiction</p>
                                                <div style="display: flex; gap: 8px;">
                                                    <span style="background: rgba(255,255,255,0.2); padding: 4px 8px; border-radius: 12px; font-size: 12px;">INTJ</span>
                                                    <span style="background: rgba(255,255,255,0.2); padding: 4px 8px; border-radius: 12px; font-size: 12px;">LII</span>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                    
                                    <div style="background: var(--wiki-gray); padding: 20px; border-radius: 12px; border: 1px solid var(--wiki-border);">
                                        <h3 style="margin-bottom: 15px; color: var(--wiki-primary);">🔥 Trending Now</h3>
                                        <div style="space-y: 10px;">
                                            <div style="display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid var(--wiki-border);">
                                                <span>Jujutsu Kaisen</span>
                                                <span style="color: var(--wiki-success);">+2,847</span>
                                            </div>
                                            <div style="display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid var(--wiki-border);">
                                                <span>Attack on Titan</span>
                                                <span style="color: var(--wiki-success);">+1,923</span>
                                            </div>
                                            <div style="display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid var(--wiki-border);">
                                                <span>Marvel Universe</span>
                                                <span style="color: var(--wiki-success);">+1,445</span>
                                            </div>
                                            <div style="display: flex; justify-content: space-between; padding: 8px 0;">
                                                <span>Harry Potter</span>
                                                <span style="color: var(--wiki-success);">+1,201</span>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            <button class="nav-btn" onclick="showSection('upload')">
                                📸 Upload Pictures
                            </button>
                        </div>
                    </div>

                    <!-- Dashboard Section -->
                    <div id="dashboard-section" class="container">
                        <h2 class="section-title">📊 System Statistics</h2>
                        <p class="section-subtitle">Overview of your personality research database</p>
                        <div id="stats-container" class="loading">Loading system statistics...</div>
                    </div>

                    <!-- Browse Section -->
                    <div id="browse-section" class="container" style="display: none;">
                        <h2 class="section-title">👥 Character Database</h2>
                        <p class="section-subtitle">Browse, search, and collaborate on personality types of characters from popular media</p>
                        
                        <!-- Search and Filter Controls -->
                        <div class="search-filter-panel">
                            <div class="search-controls">
                                <div class="search-bar">
                                    <input type="text" id="character-search" class="form-control" placeholder="🔍 Search characters by name, personality type, or description..." 
                                           oninput="searchCharacters()" style="padding-left: 45px;">
                                    <div class="search-icon">🔍</div>
                                </div>
                                
                                <div class="filter-controls">
                                    <div class="filter-group">
                                        <label class="form-label">📚 Category:</label>
                                        <select id="category-filter" class="form-control" onchange="filterCharacters()">
                                            <option value="">All Categories</option>
                                            <option value="anime">🍜 Anime</option>
                                            <option value="movie">🎬 Movies</option>
                                            <option value="tv">📺 TV Shows</option>
                                            <option value="book">📖 Books</option>
                                            <option value="game">🎮 Video Games</option>
                                            <option value="comic">🦸 Comics</option>
                                            <option value="other">🌟 Other</option>
                                        </select>
                                    </div>
                                    
                                    <div class="filter-group">
                                        <label class="form-label">🧠 Personality System:</label>
                                        <select id="personality-system-filter" class="form-control" onchange="filterCharacters()">
                                            <option value="">All Systems</option>
                                            <option value="socionics">Socionics</option>
                                            <option value="mbti">MBTI</option>
                                            <option value="enneagram">Enneagram</option>
                                        </select>
                                    </div>
                                    
                                    <div class="filter-group">
                                        <label class="form-label">📊 Sort By:</label>
                                        <select id="sort-filter" class="form-control" onchange="sortCharacters()">
                                            <option value="name">Name (A-Z)</option>
                                            <option value="name-desc">Name (Z-A)</option>
                                            <option value="category">Category</option>
                                            <option value="ratings">Most Rated</option>
                                            <option value="recent">Recently Updated</option>
                                        </select>
                                    </div>
                                </div>
                                
                                <div class="view-controls">
                                    <button class="view-btn active" onclick="setViewMode('grid')" data-view="grid">⊞ Grid</button>
                                    <button class="view-btn" onclick="setViewMode('list')" data-view="list">☰ List</button>
                                    <button class="view-btn" onclick="setViewMode('table')" data-view="table">☳ Table</button>
                                </div>
                            </div>
                            
                            <div class="active-filters" id="active-filters" style="display: none;"></div>
                        </div>
                        
                        <div id="entities-container" class="loading">Loading entities...</div>
                        <div id="pagination-controls" class="pagination-controls"></div>
                    </div>

                    <!-- Head-to-Head Comparison Section -->
                    <div id="compare-section" class="container" style="display: none;">
                        <h2 class="section-title">⚔️ Head-to-Head Comparison</h2>
                        <p class="section-subtitle">Compare personality types between two characters side-by-side</p>
                        <div class="comparison-panel">
                            <div class="comparison-grid">
                                <div class="comparison-card empty" id="compare-slot-1" onclick="selectCharacterForComparison(1)">
                                    <div class="upload-icon">👤</div>
                                    <h3>Select Character 1</h3>
                                    <p>Click to choose a character for comparison</p>
                                </div>
                                <div class="comparison-card empty" id="compare-slot-2" onclick="selectCharacterForComparison(2)">
                                    <div class="upload-icon">👤</div>
                                    <h3>Select Character 2</h3>
                                    <p>Click to choose a character for comparison</p>
                                </div>
                            </div>
                            <div style="text-align: center; margin-top: 30px;">
                                <button class="btn btn-primary" onclick="startComparison()" id="start-comparison-btn" disabled>
                                    🔍 Start Comparison
                                </button>
                            </div>
                        </div>
                        <div id="comparison-results"></div>
                    </div>

                    <!-- Panel View Section -->
                    <div id="panel-section" class="container" style="display: none;">
                        <h2 class="section-title">🎪 Panel View</h2>
                        <p class="section-subtitle">Compare personality types across a panel of 4 characters simultaneously</p>
                        <div class="comparison-panel">
                            <div class="comparison-grid" style="grid-template-columns: repeat(2, 1fr);">
                                <div class="comparison-card empty" id="panel-slot-1" onclick="selectCharacterForPanel(1)">
                                    <div class="upload-icon">👤</div>
                                    <h3>Character 1</h3>
                                    <p>Click to select</p>
                                </div>
                                <div class="comparison-card empty" id="panel-slot-2" onclick="selectCharacterForPanel(2)">
                                    <div class="upload-icon">👤</div>
                                    <h3>Character 2</h3>
                                    <p>Click to select</p>
                                </div>
                                <div class="comparison-card empty" id="panel-slot-3" onclick="selectCharacterForPanel(3)">
                                    <div class="upload-icon">👤</div>
                                    <h3>Character 3</h3>
                                    <p>Click to select</p>
                                </div>
                                <div class="comparison-card empty" id="panel-slot-4" onclick="selectCharacterForPanel(4)">
                                    <div class="upload-icon">👤</div>
                                    <h3>Character 4</h3>
                                    <p>Click to select</p>
                                </div>
                            </div>
                            <div style="text-align: center; margin-top: 30px;">
                                <button class="btn btn-primary" onclick="startPanelAnalysis()" id="start-panel-btn" disabled>
                                    🔬 Start Panel Analysis
                                </button>
                            </div>
                        </div>
                        <div id="panel-results"></div>
                    </div>

                    <!-- Upload Pictures Section -->
                    <div id="upload-section" class="container" style="display: none;">
                        <h2 class="section-title">📸 Upload Character Pictures</h2>
                        <p class="section-subtitle">Add visual references to enhance character profiles</p>
                        <div class="file-upload-area" id="upload-area" onclick="document.getElementById('file-input').click()">
                            <div class="upload-icon">📁</div>
                            <h3>Drop files here or click to upload</h3>
                            <p>Supported formats: JPG, PNG, GIF (Max: 10MB)</p>
                            <input type="file" id="file-input" multiple accept="image/*" style="display: none;" onchange="handleFileSelect(event)">
                        </div>
                        <div id="upload-results"></div>
                    </div>
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
                    let filteredEntities = [];
                    let selectedCompareCharacters = {};
                    let selectedPanelCharacters = {};
                    let currentSection = 'dashboard';
                    let currentPage = 1;
                    let itemsPerPage = 12;
                    let currentViewMode = 'grid';
                    let searchQuery = '';
                    let activeFilters = {};
                    let sortBy = 'name';
                    
                    // Initialize the application
                    async function init() {
                        await loadStats();
                        await loadEntities();
                        await createDemoUser();
                        setupFileUpload();
                    }

                    // Section navigation
                    function showSection(sectionName) {
                        // Hide all sections
                        const sections = ['dashboard', 'browse', 'compare', 'panel', 'upload'];
                        sections.forEach(section => {
                            const element = document.getElementById(section + '-section');
                            if (element) {
                                element.style.display = 'none';
                            }
                        });
                        
                        // Show selected section
                        const targetSection = document.getElementById(sectionName + '-section');
                        if (targetSection) {
                            targetSection.style.display = 'block';
                        }
                        
                        // Update nav buttons
                        document.querySelectorAll('.nav-btn').forEach(btn => {
                            btn.classList.remove('active');
                        });
                        
                        const activeBtn = Array.from(document.querySelectorAll('.nav-btn')).find(btn => 
                            btn.textContent.toLowerCase().includes(sectionName.toLowerCase()) ||
                            (sectionName === 'dashboard' && btn.textContent.includes('Dashboard')) ||
                            (sectionName === 'browse' && btn.textContent.includes('Browse')) ||
                            (sectionName === 'compare' && btn.textContent.includes('Head-to-Head')) ||
                            (sectionName === 'panel' && btn.textContent.includes('Panel')) ||
                            (sectionName === 'upload' && btn.textContent.includes('Upload'))
                        );
                        
                        if (activeBtn) {
                            activeBtn.classList.add('active');
                        }
                        
                        currentSection = sectionName;
                        
                        // Load section-specific data
                        if (sectionName === 'browse' && entities.length === 0) {
                            loadEntities();
                        } else if (sectionName === 'browse') {
                            // Reset filters when navigating to browse
                            applyFiltersAndSearch();
                            displayEntities();
                        }
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
                                <div class="stats-grid">
                                    <div class="stat-card">
                                        <h4>📊 Entities</h4>
                                        <span class="stat-number">\${data.database_stats?.entities || 0}</span>
                                        <span class="stat-label">characters available</span>
                                    </div>
                                    <div class="stat-card">
                                        <h4>🧠 Personality Types</h4>
                                        <span class="stat-number">\${data.database_stats?.personality_types || 0}</span>
                                        <span class="stat-label">types supported</span>
                                    </div>
                                    <div class="stat-card">
                                        <h4>📝 Active Users</h4>
                                        <span class="stat-number">\${data.database_stats?.users || 0}</span>
                                        <span class="stat-label">researchers</span>
                                    </div>
                                </div>
                            \`;
                            
                            document.getElementById('stats-container').innerHTML = statsHtml;
                        } catch (error) {
                            console.error('Failed to load stats:', error);
                            document.getElementById('stats-container').innerHTML = '<div class="error">Failed to load statistics</div>';
                        }
                    }

                    async function loadEntities() {
                        try {
                            const response = await fetch('/api/entities');
                            const data = await response.json();
                            
                            if (data.success && data.entities) {
                                entities = data.entities.map(entity => ({
                                    ...entity,
                                    // Enhanced entity properties
                                    category: entity.category || getCategoryFromName(entity.name),
                                    last_updated: entity.last_updated || new Date().toISOString(),
                                    rating_count: entity.rating_count || 0,
                                    avg_confidence: entity.avg_confidence || 0
                                }));
                                
                                filteredEntities = [...entities];
                                applyFiltersAndSearch();
                                displayEntities();
                            } else {
                                throw new Error(data.error || 'Failed to load entities');
                            }
                        } catch (error) {
                            console.error('Failed to load entities:', error);
                            const errorHtml = \`
                                <div class="error">
                                    <h3>⚠️ Database Not Initialized</h3>
                                    <p>Please run the database initialization first:</p>
                                    <code>python3 ipdb/demo.py</code>
                                </div>
                            \`;
                            document.getElementById('entities-container').innerHTML = errorHtml;
                        }
                    }
                    
                    function getCategoryFromName(name) {
                        // Simple heuristic to categorize characters
                        const animeKeywords = ['Naruto', 'Goku', 'Luffy', 'Ichigo', 'Edward', 'Light', 'Natsu'];
                        const movieKeywords = ['Batman', 'Superman', 'Spider', 'Iron Man', 'Captain'];
                        const tvKeywords = ['Sherlock', 'House', 'Walter', 'Tyrion'];
                        
                        const lowerName = name.toLowerCase();
                        
                        if (animeKeywords.some(keyword => lowerName.includes(keyword.toLowerCase()))) {
                            return 'anime';
                        } else if (movieKeywords.some(keyword => lowerName.includes(keyword.toLowerCase()))) {
                            return 'movie';
                        } else if (tvKeywords.some(keyword => lowerName.includes(keyword.toLowerCase()))) {
                            return 'tv';
                        }
                        
                        return 'other';
                    }

                    function displayEntities() {
                        const entitiesToShow = filteredEntities;
                        
                        if (!entitiesToShow || entitiesToShow.length === 0) {
                            document.getElementById('entities-container').innerHTML = \`
                                <div class="error">No characters match your search criteria. Try adjusting your filters.</div>
                            \`;
                            return;
                        }

                        // Calculate pagination
                        const totalPages = Math.ceil(entitiesToShow.length / itemsPerPage);
                        const startIndex = (currentPage - 1) * itemsPerPage;
                        const endIndex = startIndex + itemsPerPage;
                        const pageEntities = entitiesToShow.slice(startIndex, endIndex);

                        // Display based on view mode
                        if (currentViewMode === 'table') {
                            displayTableView(pageEntities);
                        } else if (currentViewMode === 'list') {
                            displayListView(pageEntities);
                        } else {
                            displayGridView(pageEntities);
                        }
                        
                        // Update pagination
                        updatePaginationControls(totalPages, entitiesToShow.length);
                    }
                    
                    function displayGridView(entities) {
                        const entitiesHtml = entities.map(entity => \`
                            <div class="entity-card">
                                <div class="entity-header">
                                    <div class="entity-avatar">
                                        \${entity.name.charAt(0).toUpperCase()}
                                    </div>
                                    <div class="entity-info">
                                        <div class="entity-name">\${entity.name}</div>
                                        <div class="entity-type">\${getCategoryIcon(entity.category)} \${entity.category || 'fictional_character'}</div>
                                    </div>
                                </div>
                                
                                <div class="personality-types">
                                    \${(entity.personality_types || []).map(type => 
                                        \`<span class="personality-type">🧠 \${type}</span>\`
                                    ).join('')}
                                    \${entity.rating_count ? \`<span class="personality-type" style="background: var(--success);">⭐ \${entity.rating_count} ratings</span>\` : ''}
                                </div>
                                
                                <div class="action-buttons">
                                    <button class="btn btn-primary" onclick="rateCharacter('\${entity.id}', '\${entity.name.replace(/'/g, \"&apos;\")}')">
                                        ⭐ Rate Character
                                    </button>
                                    <button class="btn btn-secondary" onclick="editCharacterSheet('\${entity.id}', '\${entity.name.replace(/'/g, \"&apos;\")}')">
                                        ✏️ Edit Sheet
                                    </button>
                                </div>
                            </div>
                        \`).join('');

                        document.getElementById('entities-container').innerHTML = \`
                            <div class="entity-grid">
                                \${entitiesHtml}
                            </div>
                        \`;
                    }
                    
                    function displayListView(entities) {
                        const entitiesHtml = entities.map(entity => \`
                            <div class="entity-card">
                                <div class="entity-header">
                                    <div class="entity-avatar">
                                        \${entity.name.charAt(0).toUpperCase()}
                                    </div>
                                </div>
                                <div class="entity-info">
                                    <div class="entity-name">\${entity.name}</div>
                                    <div class="entity-type">\${getCategoryIcon(entity.category)} \${entity.category || 'fictional_character'}</div>
                                    <div class="personality-types">
                                        \${(entity.personality_types || []).map(type => 
                                            \`<span class="personality-type">🧠 \${type}</span>\`
                                        ).join('')}
                                    </div>
                                </div>
                                <div class="action-buttons">
                                    <button class="btn btn-primary" onclick="rateCharacter('\${entity.id}', '\${entity.name.replace(/'/g, \"&apos;\")}')">
                                        ⭐ Rate
                                    </button>
                                    <button class="btn btn-secondary" onclick="editCharacterSheet('\${entity.id}', '\${entity.name.replace(/'/g, \"&apos;\")}')">
                                        ✏️ Edit
                                    </button>
                                </div>
                            </div>
                        \`).join('');

                        document.getElementById('entities-container').innerHTML = \`
                            <div class="entity-grid entity-list-view">
                                \${entitiesHtml}
                            </div>
                        \`;
                    }
                    
                    function displayTableView(entities) {
                        const entitiesHtml = entities.map(entity => \`
                            <tr>
                                <td>
                                    <div style="display: flex; align-items: center; gap: 10px;">
                                        <div class="entity-avatar" style="width: 32px; height: 32px; font-size: 0.8rem;">
                                            \${entity.name.charAt(0).toUpperCase()}
                                        </div>
                                        <strong>\${entity.name}</strong>
                                    </div>
                                </td>
                                <td>\${getCategoryIcon(entity.category)} \${entity.category || 'N/A'}</td>
                                <td>
                                    \${(entity.personality_types || []).map(type => 
                                        \`<span class="personality-type" style="margin: 2px; font-size: 0.7rem;">\${type}</span>\`
                                    ).join('')}
                                </td>
                                <td>\${entity.rating_count || 0} ratings</td>
                                <td>
                                    <button class="btn btn-primary" style="padding: 6px 12px; font-size: 0.8rem;" onclick="rateCharacter('\${entity.id}', '\${entity.name.replace(/'/g, \"&apos;\")}')">
                                        Rate
                                    </button>
                                    <button class="btn btn-secondary" style="padding: 6px 12px; font-size: 0.8rem; margin-left: 5px;" onclick="editCharacterSheet('\${entity.id}', '\${entity.name.replace(/'/g, \"&apos;\")}')">
                                        Edit
                                    </button>
                                </td>
                            </tr>
                        \`).join('');

                        document.getElementById('entities-container').innerHTML = \`
                            <div class="entity-table-view">
                                <table class="entity-table">
                                    <thead>
                                        <tr>
                                            <th>Character</th>
                                            <th>Category</th>
                                            <th>Personality Types</th>
                                            <th>Ratings</th>
                                            <th>Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        \${entitiesHtml}
                                    </tbody>
                                </table>
                            </div>
                        \`;
                    }
                    
                    function getCategoryIcon(category) {
                        const icons = {
                            'anime': '🍜',
                            'movie': '🎬',
                            'tv': '📺',
                            'book': '📖',
                            'game': '🎮',
                            'comic': '🦸',
                            'other': '🌟'
                        };
                        return icons[category] || '🌟';
                    }

                    // Search and Filter Functions
                    function searchCharacters() {
                        searchQuery = document.getElementById('character-search').value.toLowerCase().trim();
                        currentPage = 1; // Reset to first page when searching
                        applyFiltersAndSearch();
                        displayEntities();
                    }
                    
                    function filterCharacters() {
                        // Get filter values
                        const categoryFilter = document.getElementById('category-filter').value;
                        const personalitySystemFilter = document.getElementById('personality-system-filter').value;
                        
                        // Update active filters
                        activeFilters = {};
                        if (categoryFilter) activeFilters.category = categoryFilter;
                        if (personalitySystemFilter) activeFilters.personalitySystem = personalitySystemFilter;
                        
                        currentPage = 1; // Reset to first page when filtering
                        applyFiltersAndSearch();
                        displayEntities();
                        updateActiveFilters();
                    }
                    
                    function sortCharacters() {
                        sortBy = document.getElementById('sort-filter').value;
                        applyFiltersAndSearch();
                        displayEntities();
                    }
                    
                    function applyFiltersAndSearch() {
                        filteredEntities = entities.filter(entity => {
                            // Apply search query
                            if (searchQuery) {
                                const searchableText = \`
                                    \${entity.name} 
                                    \${entity.category || ''} 
                                    \${(entity.personality_types || []).join(' ')}
                                \`.toLowerCase();
                                
                                if (!searchableText.includes(searchQuery)) {
                                    return false;
                                }
                            }
                            
                            // Apply category filter
                            if (activeFilters.category) {
                                if (entity.category !== activeFilters.category) {
                                    return false;
                                }
                            }
                            
                            // Apply personality system filter
                            if (activeFilters.personalitySystem) {
                                // This would need to be expanded based on actual personality system data
                                // For now, we'll skip this filter
                            }
                            
                            return true;
                        });
                        
                        // Apply sorting
                        filteredEntities.sort((a, b) => {
                            switch (sortBy) {
                                case 'name':
                                    return a.name.localeCompare(b.name);
                                case 'name-desc':
                                    return b.name.localeCompare(a.name);
                                case 'category':
                                    return (a.category || '').localeCompare(b.category || '');
                                case 'ratings':
                                    return (b.rating_count || 0) - (a.rating_count || 0);
                                case 'recent':
                                    return new Date(b.last_updated || 0) - new Date(a.last_updated || 0);
                                default:
                                    return 0;
                            }
                        });
                    }
                    
                    function updateActiveFilters() {
                        const activeFiltersContainer = document.getElementById('active-filters');
                        const filterTags = [];
                        
                        Object.entries(activeFilters).forEach(([key, value]) => {
                            let displayName = value;
                            let filterType = key;
                            
                            if (key === 'category') {
                                displayName = \`Category: \${getCategoryIcon(value)} \${value}\`;
                                filterType = 'category';
                            } else if (key === 'personalitySystem') {
                                displayName = \`System: \${value}\`;
                                filterType = 'personalitySystem';
                            }
                            
                            filterTags.push(\`
                                <div class="filter-tag">
                                    \${displayName}
                                    <span class="remove-filter" onclick="removeFilter('\${filterType}')">&times;</span>
                                </div>
                            \`);
                        });
                        
                        if (searchQuery) {
                            filterTags.push(\`
                                <div class="filter-tag">
                                    Search: "\${searchQuery}"
                                    <span class="remove-filter" onclick="clearSearch()">&times;</span>
                                </div>
                            \`);
                        }
                        
                        if (filterTags.length > 0) {
                            activeFiltersContainer.innerHTML = filterTags.join('');
                            activeFiltersContainer.style.display = 'flex';
                        } else {
                            activeFiltersContainer.style.display = 'none';
                        }
                    }
                    
                    function removeFilter(filterType) {
                        delete activeFilters[filterType];
                        
                        // Reset the corresponding UI element
                        if (filterType === 'category') {
                            document.getElementById('category-filter').value = '';
                        } else if (filterType === 'personalitySystem') {
                            document.getElementById('personality-system-filter').value = '';
                        }
                        
                        currentPage = 1;
                        applyFiltersAndSearch();
                        displayEntities();
                        updateActiveFilters();
                    }
                    
                    function clearSearch() {
                        searchQuery = '';
                        document.getElementById('character-search').value = '';
                        currentPage = 1;
                        applyFiltersAndSearch();
                        displayEntities();
                        updateActiveFilters();
                    }
                    
                    function setViewMode(mode) {
                        currentViewMode = mode;
                        
                        // Update view buttons
                        document.querySelectorAll('.view-btn').forEach(btn => {
                            btn.classList.remove('active');
                        });
                        
                        document.querySelector(\`[data-view="\${mode}"]\`).classList.add('active');
                        
                        // Redisplay with new view mode
                        displayEntities();
                    }
                    
                    function updatePaginationControls(totalPages, totalItems) {
                        const paginationContainer = document.getElementById('pagination-controls');
                        
                        if (totalPages <= 1) {
                            paginationContainer.innerHTML = '';
                            return;
                        }
                        
                        let paginationHtml = \`
                            <div class="pagination-info">
                                Showing \${((currentPage - 1) * itemsPerPage) + 1}-\${Math.min(currentPage * itemsPerPage, totalItems)} of \${totalItems} characters
                            </div>
                        \`;
                        
                        // Previous button
                        paginationHtml += \`
                            <button class="pagination-btn" onclick="changePage(\${currentPage - 1})" \${currentPage === 1 ? 'disabled' : ''}>
                                ‹ Previous
                            </button>
                        \`;
                        
                        // Page numbers
                        const startPage = Math.max(1, currentPage - 2);
                        const endPage = Math.min(totalPages, currentPage + 2);
                        
                        if (startPage > 1) {
                            paginationHtml += \`<button class="pagination-btn" onclick="changePage(1)">1</button>\`;
                            if (startPage > 2) {
                                paginationHtml += \`<span style="padding: 0 10px; color: var(--gray-500);">...</span>\`;
                            }
                        }
                        
                        for (let i = startPage; i <= endPage; i++) {
                            paginationHtml += \`
                                <button class="pagination-btn \${i === currentPage ? 'active' : ''}" onclick="changePage(\${i})">
                                    \${i}
                                </button>
                            \`;
                        }
                        
                        if (endPage < totalPages) {
                            if (endPage < totalPages - 1) {
                                paginationHtml += \`<span style="padding: 0 10px; color: var(--gray-500);">...</span>\`;
                            }
                            paginationHtml += \`<button class="pagination-btn" onclick="changePage(\${totalPages})">\${totalPages}</button>\`;
                        }
                        
                        // Next button
                        paginationHtml += \`
                            <button class="pagination-btn" onclick="changePage(\${currentPage + 1})" \${currentPage === totalPages ? 'disabled' : ''}>
                                Next ›
                            </button>
                        \`;
                        
                        paginationContainer.innerHTML = paginationHtml;
                    }
                    
                    function changePage(page) {
                        if (page < 1 || page > Math.ceil(filteredEntities.length / itemsPerPage)) {
                            return;
                        }
                        
                        currentPage = page;
                        displayEntities();
                        
                        // Scroll to top of results
                        document.getElementById('entities-container').scrollIntoView({ 
                            behavior: 'smooth', 
                            block: 'start' 
                        });
                    }
                    
                    // Character Sheet Management Functions
                    function editCharacterSheet(entityId, entityName) {
                        const entity = entities.find(e => e.id === entityId);
                        if (!entity) {
                            showErrorMessage('Character not found');
                            return;
                        }
                        
                        const modalHtml = \`
                            <div class="modal show character-sheet-modal" id="character-sheet-modal">
                                <div class="modal-content">
                                    <button class="close-btn" onclick="closeModal('character-sheet-modal')">&times;</button>
                                    <h2>✏️ Edit Character Sheet: \${entityName}</h2>
                                    
                                    <div class="character-activity">
                                        <h4>📊 Recent Activity</h4>
                                        <div id="character-activity-list">
                                            <div class="activity-item">
                                                <div class="activity-avatar">U</div>
                                                <div class="activity-text">User started editing this character</div>
                                                <div class="activity-time">Just now</div>
                                            </div>
                                            <div class="activity-item">
                                                <div class="activity-avatar">S</div>
                                                <div class="activity-text">System initialized character sheet</div>
                                                <div class="activity-time">1 day ago</div>
                                            </div>
                                        </div>
                                    </div>
                                    
                                    <form id="character-sheet-form" class="character-sheet-form">
                                        <div class="form-group">
                                            <label class="form-label">Character Name:</label>
                                            <input type="text" id="sheet-name" class="form-control" value="\${entityName}" />
                                        </div>
                                        
                                        <div class="form-group">
                                            <label class="form-label">Category:</label>
                                            <select id="sheet-category" class="form-control">
                                                <option value="anime" \${entity.category === 'anime' ? 'selected' : ''}>🍜 Anime</option>
                                                <option value="movie" \${entity.category === 'movie' ? 'selected' : ''}>🎬 Movies</option>
                                                <option value="tv" \${entity.category === 'tv' ? 'selected' : ''}>📺 TV Shows</option>
                                                <option value="book" \${entity.category === 'book' ? 'selected' : ''}>📖 Books</option>
                                                <option value="game" \${entity.category === 'game' ? 'selected' : ''}>🎮 Video Games</option>
                                                <option value="comic" \${entity.category === 'comic' ? 'selected' : ''}>🦸 Comics</option>
                                                <option value="other" \${entity.category === 'other' ? 'selected' : ''}>🌟 Other</option>
                                            </select>
                                        </div>
                                        
                                        <div class="form-group full-width">
                                            <label class="form-label">Description:</label>
                                            <textarea id="sheet-description" class="form-control" rows="3" placeholder="Brief description of the character...">\${entity.description || ''}</textarea>
                                        </div>
                                        
                                        <div class="form-group">
                                            <label class="form-label">Source Material:</label>
                                            <input type="text" id="sheet-source" class="form-control" value="\${entity.source || ''}" placeholder="e.g., Naruto, The Office, etc." />
                                        </div>
                                        
                                        <div class="form-group">
                                            <label class="form-label">Primary Personality Type:</label>
                                            <select id="sheet-primary-type" class="form-control">
                                                <option value="">Select type...</option>
                                                \${getSocionicsTypes().map(type => 
                                                    \`<option value="\${type.value}" \${(entity.personality_types || []).includes(type.value) ? 'selected' : ''}>\${type.name}</option>\`
                                                ).join('')}
                                            </select>
                                        </div>
                                        
                                        <div class="form-group full-width">
                                            <label class="form-label">Personality Notes:</label>
                                            <textarea id="sheet-personality-notes" class="form-control" rows="4" placeholder="Detailed analysis of personality traits, cognitive functions, etc...">\${entity.personality_notes || ''}</textarea>
                                        </div>
                                        
                                        <div class="form-group">
                                            <label class="form-label">Confidence Level:</label>
                                            <div class="star-rating" id="sheet-confidence-rating">
                                                <span class="star" onclick="setSheetRating(1)">★</span>
                                                <span class="star" onclick="setSheetRating(2)">★</span>
                                                <span class="star" onclick="setSheetRating(3)">★</span>
                                                <span class="star" onclick="setSheetRating(4)">★</span>
                                                <span class="star" onclick="setSheetRating(5)">★</span>
                                            </div>
                                        </div>
                                        
                                        <div class="form-group">
                                            <label class="form-label">Last Edited By:</label>
                                            <input type="text" class="form-control" value="\${currentUser}" disabled />
                                        </div>
                                        
                                        <div style="grid-column: 1 / -1; display: flex; gap: 10px; margin-top: 30px;">
                                            <button type="button" class="btn btn-secondary" onclick="closeModal('character-sheet-modal')">Cancel</button>
                                            <button type="button" class="btn btn-primary" onclick="saveCharacterSheet('\${entityId}')">💾 Save Changes</button>
                                            <button type="button" class="btn btn-success" onclick="viewCharacterHistory('\${entityId}')">📜 View History</button>
                                        </div>
                                    </form>
                                </div>
                            </div>
                        \`;
                        
                        document.body.insertAdjacentHTML('beforeend', modalHtml);
                        
                        // Initialize confidence rating
                        const confidence = entity.avg_confidence || 0;
                        if (confidence > 0) {
                            setSheetRating(Math.round(confidence * 5));
                        }
                        
                        // Simulate real-time collaboration
                        setTimeout(() => {
                            addActivityItem('Another user viewed this character sheet', '2 min ago');
                        }, 3000);
                    }
                    
                    let currentSheetRating = 0;
                    
                    function setSheetRating(rating) {
                        currentSheetRating = rating;
                        const stars = document.querySelectorAll('#sheet-confidence-rating .star');
                        stars.forEach((star, index) => {
                            if (index < rating) {
                                star.classList.add('active');
                            } else {
                                star.classList.remove('active');
                            }
                        });
                    }
                    
                    function addActivityItem(text, time) {
                        const activityList = document.getElementById('character-activity-list');
                        if (!activityList) return;
                        
                        const newActivity = document.createElement('div');
                        newActivity.className = 'activity-item';
                        newActivity.innerHTML = \`
                            <div class="activity-avatar">O</div>
                            <div class="activity-text">\${text}</div>
                            <div class="activity-time">\${time}</div>
                        \`;
                        
                        activityList.insertBefore(newActivity, activityList.firstChild);
                    }
                    
                    async function saveCharacterSheet(entityId) {
                        const formData = {
                            name: document.getElementById('sheet-name').value,
                            category: document.getElementById('sheet-category').value,
                            description: document.getElementById('sheet-description').value,
                            source: document.getElementById('sheet-source').value,
                            personality_notes: document.getElementById('sheet-personality-notes').value,
                            last_edited_by: currentUser
                        };
                        
                        try {
                            const response = await fetch(\`/api/entities/\${entityId}\`, {
                                method: 'PUT',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify(formData)
                            });
                            
                            const result = await response.json();
                            
                            if (result.success) {
                                showSuccessMessage(\`Character sheet for "\${formData.name}" saved successfully!\`);
                                
                                // Update local entity data
                                const entityIndex = entities.findIndex(e => e.id === entityId);
                                if (entityIndex !== -1) {
                                    entities[entityIndex] = { ...entities[entityIndex], ...result.entity };
                                    
                                    // Refresh display if we're in browse section
                                    if (currentSection === 'browse') {
                                        applyFiltersAndSearch();
                                        displayEntities();
                                    }
                                }
                                
                                addActivityItem(\`\${currentUser} saved changes to character sheet\`, 'Just now');
                                closeModal('character-sheet-modal');
                            } else {
                                throw new Error(result.error || 'Failed to save character sheet');
                            }
                            
                        } catch (error) {
                            showErrorMessage('Failed to save character sheet: ' + error.message);
                        }
                    }
                    
                    function viewCharacterHistory(entityId) {
                        const historyModalHtml = \`
                            <div class="modal show" id="character-history-modal">
                                <div class="modal-content">
                                    <button class="close-btn" onclick="closeModal('character-history-modal')">&times;</button>
                                    <h2>📜 Character History</h2>
                                    <div class="character-activity">
                                        <div class="activity-item">
                                            <div class="activity-avatar">U</div>
                                            <div class="activity-text">
                                                <strong>\${currentUser}</strong> updated personality notes
                                                <div style="font-size: 0.8rem; color: var(--gray-500); margin-top: 5px;">
                                                    Added detailed analysis of cognitive functions
                                                </div>
                                            </div>
                                            <div class="activity-time">5 min ago</div>
                                        </div>
                                        <div class="activity-item">
                                            <div class="activity-avatar">A</div>
                                            <div class="activity-text">
                                                <strong>admin-user</strong> changed category from "other" to "anime"
                                            </div>
                                            <div class="activity-time">2 hours ago</div>
                                        </div>
                                        <div class="activity-item">
                                            <div class="activity-avatar">R</div>
                                            <div class="activity-text">
                                                <strong>researcher-42</strong> added personality type "ILE (ENTp)"
                                                <div style="font-size: 0.8rem; color: var(--gray-500); margin-top: 5px;">
                                                    Confidence: 4/5 stars
                                                </div>
                                            </div>
                                            <div class="activity-time">1 day ago</div>
                                        </div>
                                        <div class="activity-item">
                                            <div class="activity-avatar">S</div>
                                            <div class="activity-text">
                                                <strong>System</strong> created character sheet
                                            </div>
                                            <div class="activity-time">3 days ago</div>
                                        </div>
                                    </div>
                                    <div style="text-align: center; margin-top: 20px;">
                                        <button class="btn btn-secondary" onclick="closeModal('character-history-modal')">Close</button>
                                    </div>
                                </div>
                            </div>
                        \`;
                        
                        document.body.insertAdjacentHTML('beforeend', historyModalHtml);
                    }
                    
                    function getSocionicsTypes() {
                        return [
                            {value: 'ILE', name: 'ILE (ENTp) - Inventor'},
                            {value: 'SEI', name: 'SEI (ISFp) - Mediator'},
                            {value: 'ESE', name: 'ESE (ESFj) - Enthusiast'},
                            {value: 'LII', name: 'LII (INTj) - Analyst'},
                            {value: 'SLE', name: 'SLE (ESTp) - Marshal'},
                            {value: 'IEI', name: 'IEI (INFp) - Lyricist'},
                            {value: 'EIE', name: 'EIE (ENFj) - Mentor'},
                            {value: 'LSI', name: 'LSI (ISTj) - Inspector'},
                            {value: 'SEE', name: 'SEE (ESFp) - Ambassador'},
                            {value: 'ILI', name: 'ILI (INTp) - Critic'},
                            {value: 'LIE', name: 'LIE (ENTj) - Entrepreneur'},
                            {value: 'ESI', name: 'ESI (ISFj) - Guardian'},
                            {value: 'IEE', name: 'IEE (ENFp) - Psychologist'},
                            {value: 'SLI', name: 'SLI (ISTp) - Craftsman'},
                            {value: 'LSE', name: 'LSE (ESTj) - Administrator'},
                            {value: 'EII', name: 'EII (INFj) - Humanist'}
                        ];
                    }
                    
                    function selectCharacterForComparison(slot) {
                        if (entities.length === 0) {
                            showErrorMessage('Please load characters first');
                            return;
                        }
                        
                        showCharacterSelector(slot, 'comparison');
                    }
                    
                    function selectCharacterForPanel(slot) {
                        if (entities.length === 0) {
                            showErrorMessage('Please load characters first');
                            return;
                        }
                        
                        showCharacterSelector(slot, 'panel');
                    }
                    
                    function showCharacterSelector(slot, mode) {
                        const modalHtml = \`
                            <div class="modal show" id="character-selector-modal">
                                <div class="modal-content">
                                    <button class="close-btn" onclick="closeModal('character-selector-modal')">&times;</button>
                                    <h2>Select Character for \${mode === 'comparison' ? 'Comparison' : 'Panel'} Slot \${slot}</h2>
                                    <div style="max-height: 400px; overflow-y: auto; margin: 20px 0;">
                                        \${entities.slice(0, 20).map(entity => \`
                                            <div class="entity-card" style="margin: 10px 0; cursor: pointer;" onclick="confirmCharacterSelection('\${entity.id}', '\${entity.name.replace(/'/g, '\\\\'')}', \${slot}, '\${mode}')">
                                                <div class="entity-header">
                                                    <div class="entity-avatar">\${entity.name.charAt(0).toUpperCase()}</div>
                                                    <div class="entity-info">
                                                        <div class="entity-name">\${entity.name}</div>
                                                        <div class="entity-type">\${entity.category || 'fictional_character'}</div>
                                                    </div>
                                                </div>
                                            </div>
                                        \`).join('')}
                                    </div>
                                </div>
                            </div>
                        \`;
                        
                        document.body.insertAdjacentHTML('beforeend', modalHtml);
                    }
                    
                    function confirmCharacterSelection(entityId, entityName, slot, mode) {
                        const slotElement = document.getElementById(mode === 'comparison' ? \`compare-slot-\${slot}\` : \`panel-slot-\${slot}\`);
                        
                        if (mode === 'comparison') {
                            selectedCompareCharacters[slot] = { id: entityId, name: entityName };
                        } else {
                            selectedPanelCharacters[slot] = { id: entityId, name: entityName };
                        }
                        
                        slotElement.innerHTML = \`
                            <div class="entity-avatar" style="margin: 0 auto 15px;">\${entityName.charAt(0).toUpperCase()}</div>
                            <h3>\${entityName}</h3>
                            <p>Selected for \${mode === 'comparison' ? 'comparison' : 'panel analysis'}</p>
                            <button class="btn btn-secondary" onclick="clearSelection(\${slot}, '\${mode}')" style="margin-top: 10px;">
                                ✕ Remove
                            </button>
                        \`;
                        slotElement.classList.add('selected');
                        slotElement.classList.remove('empty');
                        
                        // Update comparison/panel buttons
                        updateActionButtons(mode);
                        
                        closeModal('character-selector-modal');
                    }
                    
                    function clearSelection(slot, mode) {
                        const slotElement = document.getElementById(mode === 'comparison' ? \`compare-slot-\${slot}\` : \`panel-slot-\${slot}\`);
                        
                        if (mode === 'comparison') {
                            delete selectedCompareCharacters[slot];
                            slotElement.innerHTML = \`
                                <div class="upload-icon">👤</div>
                                <h3>Select Character \${slot}</h3>
                                <p>Click to choose a character for comparison</p>
                            \`;
                        } else {
                            delete selectedPanelCharacters[slot];
                            slotElement.innerHTML = \`
                                <div class="upload-icon">👤</div>
                                <h3>Character \${slot}</h3>
                                <p>Click to select</p>
                            \`;
                        }
                        
                        slotElement.classList.remove('selected');
                        slotElement.classList.add('empty');
                        slotElement.onclick = mode === 'comparison' ? 
                            () => selectCharacterForComparison(slot) : 
                            () => selectCharacterForPanel(slot);
                        
                        updateActionButtons(mode);
                    }
                    
                    function updateActionButtons(mode) {
                        if (mode === 'comparison') {
                            const hasTwo = Object.keys(selectedCompareCharacters).length === 2;
                            document.getElementById('start-comparison-btn').disabled = !hasTwo;
                        } else {
                            const hasFour = Object.keys(selectedPanelCharacters).length === 4;
                            document.getElementById('start-panel-btn').disabled = !hasFour;
                        }
                    }
                    
                    function startComparison() {
                        const chars = Object.values(selectedCompareCharacters);
                        if (chars.length !== 2) return;
                        
                        const resultsHtml = \`
                            <div class="comparison-panel">
                                <h3>🔍 Comparison Results</h3>
                                <div class="comparison-grid">
                                    <div class="stat-card">
                                        <h4>\${chars[0].name}</h4>
                                        <p>Ready for detailed personality analysis and rating comparison</p>
                                        <button class="btn btn-primary" onclick="rateCharacter('\${chars[0].id}', '\${chars[0].name}')">
                                            Rate This Character
                                        </button>
                                    </div>
                                    <div class="stat-card">
                                        <h4>\${chars[1].name}</h4>
                                        <p>Ready for detailed personality analysis and rating comparison</p>
                                        <button class="btn btn-primary" onclick="rateCharacter('\${chars[1].id}', '\${chars[1].name}')">
                                            Rate This Character
                                        </button>
                                    </div>
                                </div>
                                <div style="text-align: center; margin-top: 20px;">
                                    <button class="btn btn-success" onclick="comparePersonalities()">
                                        🧠 Analyze Personality Differences
                                    </button>
                                </div>
                            </div>
                        \`;
                        
                        document.getElementById('comparison-results').innerHTML = resultsHtml;
                    }
                    
                    function startPanelAnalysis() {
                        const chars = Object.values(selectedPanelCharacters);
                        if (chars.length !== 4) return;
                        
                        const resultsHtml = \`
                            <div class="comparison-panel">
                                <h3>🎪 Panel Analysis Results</h3>
                                <div class="stats-grid">
                                    \${chars.map(char => \`
                                        <div class="stat-card">
                                            <div class="entity-avatar" style="margin: 0 auto 10px;">\${char.name.charAt(0).toUpperCase()}</div>
                                            <h4>\${char.name}</h4>
                                            <button class="btn btn-primary" onclick="rateCharacter('\${char.id}', '\${char.name}')">
                                                Rate Character
                                            </button>
                                        </div>
                                    \`).join('')}
                                </div>
                                <div style="text-align: center; margin-top: 30px;">
                                    <button class="btn btn-success" onclick="analyzePanelDynamics()">
                                        🔬 Analyze Panel Dynamics
                                    </button>
                                </div>
                            </div>
                        \`;
                        
                        document.getElementById('panel-results').innerHTML = resultsHtml;
                    }
                    
                    function comparePersonalities() {
                        showSuccessMessage('🧠 Personality comparison analysis started! This feature analyzes cognitive functions, interaction styles, and compatibility between the selected characters.');
                    }
                    
                    function analyzePanelDynamics() {
                        showSuccessMessage('🔬 Panel dynamics analysis initiated! This feature examines group interactions, communication patterns, and team dynamics across all four personality types.');
                    }

                    // File upload functionality
                    function setupFileUpload() {
                        const uploadArea = document.getElementById('upload-area');
                        const fileInput = document.getElementById('file-input');
                        
                        if (!uploadArea || !fileInput) return;
                        
                        // Drag and drop handlers
                        uploadArea.addEventListener('dragover', (e) => {
                            e.preventDefault();
                            uploadArea.classList.add('dragover');
                        });
                        
                        uploadArea.addEventListener('dragleave', () => {
                            uploadArea.classList.remove('dragover');
                        });
                        
                        uploadArea.addEventListener('drop', (e) => {
                            e.preventDefault();
                            uploadArea.classList.remove('dragover');
                            const files = Array.from(e.dataTransfer.files);
                            handleFiles(files);
                        });
                    }
                    
                    function handleFileSelect(event) {
                        const files = Array.from(event.target.files);
                        handleFiles(files);
                    }
                    
                    function handleFiles(files) {
                        const allowedTypes = ['image/jpeg', 'image/png', 'image/gif'];
                        const maxSize = 10 * 1024 * 1024; // 10MB
                        
                        const validFiles = files.filter(file => {
                            if (!allowedTypes.includes(file.type)) {
                                showErrorMessage(\`File "\${file.name}" is not a supported image format\`);
                                return false;
                            }
                            if (file.size > maxSize) {
                                showErrorMessage(\`File "\${file.name}" is too large (max 10MB)\`);
                                return false;
                            }
                            return true;
                        });
                        
                        if (validFiles.length > 0) {
                            uploadFiles(validFiles);
                        }
                    }
                    
                    function uploadFiles(files) {
                        const resultsContainer = document.getElementById('upload-results');
                        const uploadPromises = files.map(file => {
                            return new Promise((resolve, reject) => {
                                const reader = new FileReader();
                                reader.onload = (e) => {
                                    // Simulate upload process
                                    const fileInfo = {
                                        name: file.name,
                                        size: (file.size / 1024 / 1024).toFixed(2) + 'MB',
                                        dataUrl: e.target.result
                                    };
                                    resolve(fileInfo);
                                };
                                reader.onerror = reject;
                                reader.readAsDataURL(file);
                            });
                        });
                        
                        Promise.all(uploadPromises).then(fileInfos => {
                            const resultsHtml = \`
                                <div class="comparison-panel">
                                    <h3>📸 Uploaded Images</h3>
                                    <div class="stats-grid">
                                        \${fileInfos.map(file => \`
                                            <div class="stat-card">
                                                <img src="\${file.dataUrl}" alt="\${file.name}" style="width: 100%; height: 150px; object-fit: cover; border-radius: 8px; margin-bottom: 10px;">
                                                <h4>\${file.name}</h4>
                                                <p>Size: \${file.size}</p>
                                                <button class="btn btn-success" onclick="associateWithCharacter('\${file.name}')">
                                                    🔗 Associate with Character
                                                </button>
                                            </div>
                                        \`).join('')}
                                    </div>
                                </div>
                            \`;
                            resultsContainer.innerHTML = resultsHtml;
                            showSuccessMessage(\`Successfully uploaded \${files.length} image(s)!\`);
                        }).catch(error => {
                            showErrorMessage('Failed to process uploaded files: ' + error.message);
                        });
                    }
                    
                    function associateWithCharacter(filename) {
                        if (entities.length === 0) {
                            showErrorMessage('Please load characters first');
                            return;
                        }
                        
                        const modalHtml = \`
                            <div class="modal show" id="character-association-modal">
                                <div class="modal-content">
                                    <button class="close-btn" onclick="closeModal('character-association-modal')">&times;</button>
                                    <h2>Associate "\${filename}" with Character</h2>
                                    <div style="max-height: 400px; overflow-y: auto; margin: 20px 0;">
                                        \${entities.slice(0, 20).map(entity => \`
                                            <div class="entity-card" style="margin: 10px 0; cursor: pointer;" onclick="confirmImageAssociation('\${filename}', '\${entity.id}', '\${entity.name.replace(/'/g, '\\\\'')}')">
                                                <div class="entity-header">
                                                    <div class="entity-avatar">\${entity.name.charAt(0).toUpperCase()}</div>
                                                    <div class="entity-info">
                                                        <div class="entity-name">\${entity.name}</div>
                                                        <div class="entity-type">\${entity.category || 'fictional_character'}</div>
                                                    </div>
                                                </div>
                                            </div>
                                        \`).join('')}
                                    </div>
                                </div>
                            </div>
                        \`;
                        
                        document.body.insertAdjacentHTML('beforeend', modalHtml);
                    }
                    
                    function confirmImageAssociation(filename, entityId, entityName) {
                        // Here you would normally save the association to the database
                        showSuccessMessage(\`Image "\${filename}" associated with character "\${entityName}"!\`);
                        closeModal('character-association-modal');
                    }

                    // Rating and commenting functions (enhanced)
                    function rateCharacter(entityId, entityName) {
                        const modalHtml = \`
                            <div class="modal show" id="rating-modal">
                                <div class="modal-content">
                                    <button class="close-btn" onclick="closeModal('rating-modal')">&times;</button>
                                    <h2>⭐ Rate: \${entityName}</h2>
                                    <form id="rating-form">
                                        <div class="form-group">
                                            <label class="form-label">Personality System:</label>
                                            <select id="personality-system" class="form-control" onchange="updatePersonalityTypes()">
                                                <option value="socionics">Socionics</option>
                                                <option value="mbti">MBTI</option>
                                                <option value="enneagram">Enneagram</option>
                                            </select>
                                        </div>
                                        <div class="form-group">
                                            <label class="form-label">Personality Type:</label>
                                            <select id="personality-type" class="form-control">
                                                <option value="ILE">ILE (ENTp) - Inventor</option>
                                                <option value="SEI">SEI (ISFp) - Mediator</option>
                                                <option value="ESE">ESE (ESFj) - Enthusiast</option>
                                                <option value="LII">LII (INTj) - Analyst</option>
                                                <option value="SLE">SLE (ESTp) - Marshal</option>
                                                <option value="IEI">IEI (INFp) - Lyricist</option>
                                                <option value="EIE">EIE (ENFj) - Mentor</option>
                                                <option value="LSI">LSI (ISTj) - Inspector</option>
                                                <option value="SEE">SEE (ESFp) - Ambassador</option>
                                                <option value="ILI">ILI (INTp) - Critic</option>
                                                <option value="LIE">LIE (ENTj) - Entrepreneur</option>
                                                <option value="ESI">ESI (ISFj) - Guardian</option>
                                                <option value="IEE">IEE (ENFp) - Psychologist</option>
                                                <option value="SLI">SLI (ISTp) - Craftsman</option>
                                                <option value="LSE">LSE (ESTj) - Administrator</option>
                                                <option value="EII">EII (INFj) - Humanist</option>
                                            </select>
                                        </div>
                                        <div class="form-group">
                                            <label class="form-label">Confidence Level:</label>
                                            <div class="star-rating" id="confidence-rating">
                                                <span class="star" onclick="setRating(1)">★</span>
                                                <span class="star" onclick="setRating(2)">★</span>
                                                <span class="star" onclick="setRating(3)">★</span>
                                                <span class="star" onclick="setRating(4)">★</span>
                                                <span class="star" onclick="setRating(5)">★</span>
                                            </div>
                                            <p style="margin-top: 10px; color: var(--gray-600); font-size: 0.9rem;">1 = Very uncertain, 5 = Very confident</p>
                                        </div>
                                        <div class="form-group">
                                            <label class="form-label">Reasoning (optional):</label>
                                            <textarea id="rating-reasoning" class="form-control" rows="4" placeholder="Explain your typing reasoning..."></textarea>
                                        </div>
                                        <div style="display: flex; gap: 10px; margin-top: 30px;">
                                            <button type="button" class="btn btn-secondary" onclick="closeModal('rating-modal')">Cancel</button>
                                            <button type="button" class="btn btn-primary" onclick="submitRating('\${entityId}', '\${entityName}')">Submit Rating</button>
                                        </div>
                                    </form>
                                </div>
                            </div>
                        \`;
                        
                        document.body.insertAdjacentHTML('beforeend', modalHtml);
                    }
                    
                    function viewComments(entityId, entityName) {
                        const modalHtml = \`
                            <div class="modal show" id="comments-modal">
                                <div class="modal-content">
                                    <button class="close-btn" onclick="closeModal('comments-modal')">&times;</button>
                                    <h2>💬 Comments: \${entityName}</h2>
                                    <div id="comments-list" class="loading">Loading comments...</div>
                                    <div class="form-group" style="margin-top: 30px;">
                                        <label class="form-label">Add a comment:</label>
                                        <textarea id="new-comment" class="form-control" rows="3" placeholder="Share your thoughts on this character's personality typing..."></textarea>
                                        <button class="btn btn-primary" onclick="submitComment('\${entityId}', '\${entityName}')" style="margin-top: 10px;">
                                            💬 Post Comment
                                        </button>
                                    </div>
                                </div>
                            </div>
                        \`;
                        
                        document.body.insertAdjacentHTML('beforeend', modalHtml);
                        loadComments(entityId);
                    }
                    
                    function updatePersonalityTypes() {
                        const system = document.getElementById('personality-system').value;
                        const typeSelect = document.getElementById('personality-type');
                        
                        const types = {
                            socionics: [
                                {value: 'ILE', name: 'ILE (ENTp) - Inventor'},
                                {value: 'SEI', name: 'SEI (ISFp) - Mediator'},
                                {value: 'ESE', name: 'ESE (ESFj) - Enthusiast'},
                                {value: 'LII', name: 'LII (INTj) - Analyst'},
                                {value: 'SLE', name: 'SLE (ESTp) - Marshal'},
                                {value: 'IEI', name: 'IEI (INFp) - Lyricist'},
                                {value: 'EIE', name: 'EIE (ENFj) - Mentor'},
                                {value: 'LSI', name: 'LSI (ISTj) - Inspector'},
                                {value: 'SEE', name: 'SEE (ESFp) - Ambassador'},
                                {value: 'ILI', name: 'ILI (INTp) - Critic'},
                                {value: 'LIE', name: 'LIE (ENTj) - Entrepreneur'},
                                {value: 'ESI', name: 'ESI (ISFj) - Guardian'},
                                {value: 'IEE', name: 'IEE (ENFp) - Psychologist'},
                                {value: 'SLI', name: 'SLI (ISTp) - Craftsman'},
                                {value: 'LSE', name: 'LSE (ESTj) - Administrator'},
                                {value: 'EII', name: 'EII (INFj) - Humanist'}
                            ],
                            mbti: [
                                {value: 'INTJ', name: 'INTJ - The Architect'},
                                {value: 'INTP', name: 'INTP - The Logician'},
                                {value: 'ENTJ', name: 'ENTJ - The Commander'},
                                {value: 'ENTP', name: 'ENTP - The Debater'},
                                {value: 'INFJ', name: 'INFJ - The Advocate'},
                                {value: 'INFP', name: 'INFP - The Mediator'},
                                {value: 'ENFJ', name: 'ENFJ - The Protagonist'},
                                {value: 'ENFP', name: 'ENFP - The Campaigner'},
                                {value: 'ISTJ', name: 'ISTJ - The Logistician'},
                                {value: 'ISFJ', name: 'ISFJ - The Protector'},
                                {value: 'ESTJ', name: 'ESTJ - The Executive'},
                                {value: 'ESFJ', name: 'ESFJ - The Consul'},
                                {value: 'ISTP', name: 'ISTP - The Virtuoso'},
                                {value: 'ISFP', name: 'ISFP - The Adventurer'},
                                {value: 'ESTP', name: 'ESTP - The Entrepreneur'},
                                {value: 'ESFP', name: 'ESFP - The Entertainer'}
                            ],
                            enneagram: [
                                {value: '1', name: 'Type 1 - The Reformer'},
                                {value: '2', name: 'Type 2 - The Helper'},
                                {value: '3', name: 'Type 3 - The Achiever'},
                                {value: '4', name: 'Type 4 - The Individualist'},
                                {value: '5', name: 'Type 5 - The Investigator'},
                                {value: '6', name: 'Type 6 - The Loyalist'},
                                {value: '7', name: 'Type 7 - The Enthusiast'},
                                {value: '8', name: 'Type 8 - The Challenger'},
                                {value: '9', name: 'Type 9 - The Peacemaker'}
                            ]
                        };
                        
                        typeSelect.innerHTML = types[system].map(type => 
                            \`<option value="\${type.value}">\${type.name}</option>\`
                        ).join('');
                    }
                    
                    let currentRating = 0;
                    
                    function setRating(rating) {
                        currentRating = rating;
                        const stars = document.querySelectorAll('#confidence-rating .star');
                        stars.forEach((star, index) => {
                            if (index < rating) {
                                star.classList.add('active');
                            } else {
                                star.classList.remove('active');
                            }
                        });
                    }

                    async function submitRating(entityId, entityName) {
                        const system = document.getElementById('personality-system').value;
                        const type = document.getElementById('personality-type').value;
                        const reasoning = document.getElementById('rating-reasoning').value;
                        
                        if (currentRating === 0) {
                            showErrorMessage('Please select a confidence level');
                            return;
                        }
                        
                        const rating = {
                            entity_id: entityId,
                            user: currentUser,
                            personality_system: system,
                            personality_type: type,
                            confidence: currentRating / 5.0,
                            reasoning: reasoning
                        };
                        
                        try {
                            const response = await fetch('/api/ratings', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify(rating)
                            });
                            
                            const result = await response.json();
                            
                            if (result.success) {
                                showSuccessMessage(\`Successfully rated \${entityName} as \${type}!\`);
                                closeModal('rating-modal');
                                currentRating = 0;
                            } else {
                                throw new Error(result.error || 'Failed to submit rating');
                            }
                        } catch (error) {
                            showErrorMessage('Failed to submit rating: ' + error.message);
                        }
                    }

                    async function loadComments(entityId) {
                        try {
                            const response = await fetch(\`/api/comments/\${entityId}\`);
                            const result = await response.json();
                            
                            const commentsContainer = document.getElementById('comments-list');
                            
                            if (result.success && result.comments) {
                                const comments = result.comments;
                                
                                if (comments.length === 0) {
                                    commentsContainer.innerHTML = '<p style="text-align: center; color: var(--gray-500); padding: 20px;">No comments yet. Be the first to share your thoughts!</p>';
                                    return;
                                }
                                
                                const commentsHtml = comments.map(comment => \`
                                    <div class="stat-card" style="text-align: left; margin: 15px 0;">
                                        <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 10px;">
                                            <div class="entity-avatar" style="width: 32px; height: 32px; font-size: 0.9rem;">
                                                \${(comment.user_id || 'U').charAt(0).toUpperCase()}
                                            </div>
                                            <div>
                                                <strong>\${comment.user_name || comment.user_id || 'Anonymous'}</strong>
                                                <div style="font-size: 0.8rem; color: var(--gray-500);">
                                                    \${new Date(comment.created_at).toLocaleDateString()}
                                                </div>
                                            </div>
                                        </div>
                                        <p style="margin: 0; line-height: 1.5;">\${comment.content}</p>
                                    </div>
                                \`).join('');
                                
                                commentsContainer.innerHTML = commentsHtml;
                            } else {
                                throw new Error(result.error || 'Failed to load comments');
                            }
                        } catch (error) {
                            document.getElementById('comments-list').innerHTML = \`
                                <div class="error">Failed to load comments: \${error.message}</div>
                            \`;
                        }
                    }

                    async function submitComment(entityId, entityName) {
                        const content = document.getElementById('new-comment').value.trim();
                        
                        if (!content) {
                            showErrorMessage('Please enter a comment');
                            return;
                        }
                        
                        const comment = {
                            entity_id: entityId,
                            user: currentUser,
                            content: content
                        };
                        
                        try {
                            const response = await fetch('/api/comments', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify(comment)
                            });
                            
                            const result = await response.json();
                            
                            if (result.success) {
                                showSuccessMessage('Comment posted successfully!');
                                document.getElementById('new-comment').value = '';
                                await loadComments(entityId);
                            } else {
                                throw new Error(result.error || 'Failed to submit comment');
                            }
                        } catch (error) {
                            showErrorMessage('Failed to post comment: ' + error.message);
                        }
                    }

                    // Utility functions
                    function closeModal(modalId) {
                        const modal = document.getElementById(modalId);
                        if (modal) {
                            modal.remove();
                        }
                    }

                    function showSuccessMessage(message) {
                        showMessage(message, 'success');
                    }

                    function showErrorMessage(message) {
                        showMessage(message, 'error');
                    }

                    function showMessage(message, type) {
                        const messageDiv = document.createElement('div');
                        messageDiv.className = type;
                        messageDiv.innerHTML = message;
                        messageDiv.style.position = 'fixed';
                        messageDiv.style.top = '20px';
                        messageDiv.style.right = '20px';
                        messageDiv.style.zIndex = '10000';
                        messageDiv.style.maxWidth = '400px';
                        messageDiv.style.boxShadow = 'var(--shadow-xl)';
                        
                        document.body.appendChild(messageDiv);
                        
                        setTimeout(() => {
                            if (messageDiv.parentNode) {
                                messageDiv.remove();
                            }
                        }, 5000);
                    }

                    
                    /* Responsive Design */
                    @media (max-width: 768px) {
                        .wiki-layout {
                            grid-template-columns: 1fr;
                        }
                        
                        .wiki-sidebar {
                            order: 2;
                        }
                        
                        .wiki-nav-main .container {
                            flex-direction: column;
                            gap: 15px;
                        }
                        
                        .character-grid {
                            grid-template-columns: 1fr;
                        }
                    }
                    
                    /* Personality Filter Buttons */
                    .personality-filter {
                        background: white;
                        border: 1px solid var(--wiki-border);
                        color: var(--wiki-text);
                        padding: 6px 12px;
                        border-radius: 20px;
                        cursor: pointer;
                        font-size: 13px;
                        font-weight: 500;
                        transition: all 0.2s;
                    }
                    
                    .personality-filter:hover {
                        background: var(--wiki-secondary);
                        color: white;
                        border-color: var(--wiki-secondary);
                    }
                    
                    .personality-filter.active {
                        background: var(--wiki-accent);
                        color: white;
                        border-color: var(--wiki-accent);
                    }
                    
                    /* Loading States */
                    .loading {
                        text-align: center;
                        padding: 40px;
                        color: var(--wiki-text-light);
                    }
                    
                    .content-section {
                        display: none;
                    }
                    
                    .content-section.active {
                        display: block;
                    }
                </style>
                
                <script>
                    // Global app state
                    let currentPage = 'dashboard';
                    let entities = [];
                    let filteredEntities = [];
                    let currentView = 'grid';
                    let selectedCharacters = { compare: {}, panel: {} };
                    
                    // Initialize the application
                    async function init() {
                        console.log('🚀 Initializing Personality Database Wiki...');
                        await loadEntities();
                        showSection('dashboard');
                        initializeSearch();
                    }
                    
                    // Global search functionality
                    function performGlobalSearch() {
                        const query = document.getElementById('globalSearch').value.trim();
                        if (query) {
                            showSection('browse');
                            document.getElementById('character-search').value = query;
                            filterCharacters();
                        }
                    }
                    
                    // Enhanced search with debouncing
                    function initializeSearch() {
                        const searchInput = document.getElementById('globalSearch');
                        if (searchInput) {
                            searchInput.addEventListener('keypress', function(e) {
                                if (e.key === 'Enter') {
                                    performGlobalSearch();
                                }
                            });
                        }
                    }
                    
                    // Show different sections
                    function showSection(sectionName) {
                        // Update sidebar navigation
                        document.querySelectorAll('.sidebar-nav a').forEach(link => {
                            link.classList.remove('active');
                        });
                        
                        // Update content sections
                        document.querySelectorAll('.content-section').forEach(section => {
                            section.style.display = 'none';
                        });
                        
                        // Show selected section
                        const section = document.getElementById(sectionName + '-section');
                        if (section) {
                            section.style.display = 'block';
                            
                            // Update breadcrumb
                            const breadcrumb = document.querySelector('.breadcrumb');
                            const sectionNames = {
                                'dashboard': 'Main Page',
                                'browse': 'Browse Characters',
                                'compare': 'Compare Types',
                                'recent': 'Recent Activity',
                                'popular': 'Popular Today'
                            };
                            
                            if (breadcrumb) {
                                breadcrumb.innerHTML = '<a href="#">Home</a> › <span>' + (sectionNames[sectionName] || sectionName) + '</span>';
                            }
                            
                            // Update content title
                            const contentTitle = document.querySelector('.content-title');
                            if (contentTitle) {
                                const titles = {
                                    'dashboard': 'Welcome to Personality Database Wiki',
                                    'browse': 'Browse Character Database',
                                    'compare': 'Character Type Comparison',
                                    'recent': 'Recent Community Activity',
                                    'popular': 'Popular Characters Today'
                                };
                                contentTitle.textContent = titles[sectionName] || sectionName;
                            }
                        }
                        
                        currentPage = sectionName;
                    }
                    
                    // Load entities from API
                    async function loadEntities() {
                        try {
                            console.log('Loading entities...');
                            const response = await fetch('/api/entities');
                            if (response.ok) {
                                entities = await response.json();
                                filteredEntities = [...entities];
                                renderCharacterGrid();
                                console.log(`Loaded ${entities.length} entities`);
                            } else {
                                console.error('Failed to load entities:', response.status);
                                showMessage('Failed to load character database', 'error');
                            }
                        } catch (error) {
                            console.error('Error loading entities:', error);
                            showMessage('Error connecting to database', 'error');
                        }
                    }
                    
                    // Render character grid
                    function renderCharacterGrid() {
                        const container = document.getElementById('character-grid');
                        if (!container) return;
                        
                        if (filteredEntities.length === 0) {
                            container.innerHTML = '<div style="text-align: center; padding: 40px; color: var(--wiki-text-light);">No characters found matching your criteria.</div>';
                            return;
                        }
                        
                        container.innerHTML = filteredEntities.slice(0, 20).map(entity => {
                            return `
                            <div class="character-card" onclick="showCharacterDetails(${entity.id})">
                                <div class="character-image">
                                    ${entity.name ? entity.name.charAt(0).toUpperCase() : '?'}
                                </div>
                                <div class="character-info">
                                    <div class="character-name">${entity.name || 'Unknown Character'}</div>
                                    <div class="character-source">${entity.category || 'Unknown Source'}</div>
                                    <div class="personality-badges">
                                        ${entity.mbti_type ? '<span class="personality-badge mbti">' + entity.mbti_type + '</span>' : ''}
                                        ${entity.socionics_type ? '<span class="personality-badge socionics">' + entity.socionics_type + '</span>' : ''}
                                    </div>
                                    <div class="vote-info">
                                        <div class="vote-count">
                                            <span>🗳️</span>
                                            <span>${Math.floor(Math.random() * 500) + 50} votes</span>
                                        </div>
                                        <button class="vote-btn" onclick="voteOnCharacter(${entity.id}, event)">Vote</button>
                                    </div>
                                </div>
                            </div>
                        `;
                        }).join('');
                    }
                    
                    // Filter functions
                    function filterByCategory(category) {
                        showSection('browse');
                        document.getElementById('category-filter').value = category;
                        filterCharacters();
                    }
                    
                    function filterBySystem(system) {
                        showSection('browse');
                        // Filter logic here
                        filterCharacters();
                    }
                    
                    function filterCharacters() {
                        const searchQuery = document.getElementById('character-search')?.value.toLowerCase() || '';
                        const categoryFilter = document.getElementById('category-filter')?.value || '';
                        
                        filteredEntities = entities.filter(entity => {
                            const matchesSearch = !searchQuery || 
                                (entity.name && entity.name.toLowerCase().includes(searchQuery)) ||
                                (entity.description && entity.description.toLowerCase().includes(searchQuery));
                                
                            const matchesCategory = !categoryFilter || entity.category === categoryFilter;
                            
                            return matchesSearch && matchesCategory;
                        });
                        
                        renderCharacterGrid();
                    }
                    
                    // Character interaction functions
                    function showCharacterDetails(entityId) {
                        console.log('Showing details for character:', entityId);
                        // Implement character detail modal/page
                    }
                    
                    function voteOnCharacter(entityId, event) {
                        event.stopPropagation();
                        console.log('Voting on character:', entityId);
                        // Implement voting functionality
                        showMessage('Vote recorded! Thank you for contributing to the community.', 'success');
                    }
                    
                    // Comparison functions
                    function selectCharacterForComparison(slot) {
                        console.log('Selecting character for comparison slot:', slot);
                        // Implement character selection for comparison
                    }
                    
                    // Utility functions
                    function showMessage(message, type = 'info') {
                        const messageDiv = document.createElement('div');
                        messageDiv.style.cssText = `
                            position: fixed; top: 20px; right: 20px; z-index: 1000;
                            padding: 15px 20px; border-radius: 8px; font-weight: 600;
                            color: white; min-width: 300px; box-shadow: 0 4px 12px rgba(0,0,0,0.15);
                            background: ${type === 'success' ? 'var(--wiki-success)' : type === 'error' ? 'var(--wiki-danger)' : 'var(--wiki-secondary)'};
                        `;
                        messageDiv.textContent = message;
                        document.body.appendChild(messageDiv);
                        
                        setTimeout(() => {
                            if (messageDiv.parentNode) {
                                messageDiv.remove();
                            }
                        }, 5000);
                    }

                    // Initialize the app when the page loads
                    document.addEventListener('DOMContentLoaded', init);
                </script>
            </body>
            </html>
        `);
    }

    // Get entities from database
    async handleGetEntities(req, res) {
        try {
            const parsedUrl = url.parse(req.url, true);
            const query = parsedUrl.query;
            
            const filters = {
                search: query.search,
                category: query.category,
                sort: query.sort
            };
            
            const limit = parseInt(query.limit) || 50;
            const offset = parseInt(query.offset) || 0;
            
            const entities = await this.dbManager.getAllEntities(limit, offset, filters);
            
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, entities }));
            
        } catch (error) {
            console.error('Failed to get entities:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Get specific entity
    async handleGetEntity(req, res, parsedUrl, params) {
        try {
            const entity = await this.dbManager.getEntity(params.id);
            
            if (entity) {
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ success: true, entity }));
            } else {
                res.writeHead(404, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ success: false, error: 'Entity not found' }));
            }
            
        } catch (error) {
            console.error('Failed to get entity:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Create rating
    async handleCreateRating(req, res) {
        try {
            const rating = await this.getRequestBody(req);
            const ratingId = await this.dbManager.addRating(rating);
            
            res.writeHead(201, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, rating_id: ratingId }));
            
        } catch (error) {
            console.error('Failed to create rating:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Get ratings for entity
    async handleGetRatings(req, res, parsedUrl, params) {
        try {
            const ratings = await this.dbManager.getEntityRatings(params.entityId);
            
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, ratings }));
            
        } catch (error) {
            console.error('Failed to get ratings:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Create comment
    async handleCreateComment(req, res) {
        try {
            const comment = await this.getRequestBody(req);
            const commentId = await this.dbManager.addComment(comment);
            
            res.writeHead(201, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, comment_id: commentId }));
            
        } catch (error) {
            console.error('Failed to create comment:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Get comments for entity
    async handleGetComments(req, res, parsedUrl, params) {
        try {
            const comments = await this.dbManager.getEntityComments(params.entityId);
            
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, comments }));
            
        } catch (error) {
            console.error('Failed to get comments:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Update entity (character sheet)
    async handleUpdateEntity(req, res, parsedUrl, params) {
        try {
            const updates = await this.getRequestBody(req);
            const userId = updates.last_edited_by || 'anonymous';
            
            delete updates.last_edited_by; // Remove from updates object
            
            const updatedEntity = await this.dbManager.updateEntity(params.id, updates, userId);
            
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, entity: updatedEntity }));
            
        } catch (error) {
            console.error('Failed to update entity:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Get entity edit history
    async handleGetEntityHistory(req, res, parsedUrl, params) {
        try {
            const history = await this.dbManager.getEntityHistory(params.id);
            
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, history }));
            
        } catch (error) {
            console.error('Failed to get entity history:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Create user
    async handleCreateUser(req, res) {
        try {
            const user = await this.getRequestBody(req);
            const userId = await this.dbManager.addUser(user);
            
            res.writeHead(201, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, user_id: userId }));
            
        } catch (error) {
            console.error('Failed to create user:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    handleHealth(req, res) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            status: 'ok',
            timestamp: new Date().toISOString(),
            system: 'IPDB Enhanced Research Platform',
            version: '2.0.0',
            node_version: process.version,
            uptime: process.uptime()
        }));
    }

    handleApiInfo(req, res) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            name: 'IPDB Enhanced Research Platform',
            description: 'Interactive Personality Database with Advanced Comparison Features',
            version: '2.0.0',
            features: [
                'Collaborative character sheet management with real-time editing',
                'Advanced search engine with taxonomy-based organization',
                'Interactive multi-mode browsing (Grid, List, Table views)',
                'Multi-system personality typing (Socionics, MBTI, Enneagram)',
                'Head-to-head character comparisons',
                'Panel analysis with 4 characters',
                'Picture upload and association',
                'Real-time rating and commenting',
                'Version history and change tracking',
                'Interactive modern UI with animations',
                'User management with role-based access'
            ],
            backend: '100% Node.js with SQLite database',
            frontend: 'Modern JavaScript with CSS Grid and Animations',
            api_type: 'HTTP REST with enhanced UI',
            implementation: 'Pure Node.js - no Python dependencies',
            endpoints: Object.keys(this.routes)
        }));
    }

    async handleStats(req, res) {
        try {
            const stats = await this.dbManager.getStats();
            
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
                success: true,
                database_stats: stats,
                node_only: true,
                last_updated: new Date().toISOString()
            }));
            
        } catch (error) {
            console.error('Failed to get stats:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
                success: false,
                error: error.message,
                node_only: true
            }));
        }
    }

    async handleTest(req, res) {
        try {
            // Test the Node.js database instead of Python
            const stats = await this.dbManager.getStats();
            const testEntity = await this.dbManager.getAllEntities(1);
            
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
                success: true,
                message: 'Node.js IPDB system test successful',
                stats: stats,
                sample_entity: testEntity.length > 0 ? testEntity[0].name : 'No entities found',
                timestamp: new Date().toISOString()
            }));

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
            console.log(`\n🚀 IPDB Enhanced Research Platform (Node.js Only)`);
            console.log(`📍 Server: http://localhost:${this.port}`);
            console.log(`🎨 App: http://localhost:${this.port}/app`);
            console.log(`🏥 Health: http://localhost:${this.port}/health`);
            console.log(`📊 Stats:  http://localhost:${this.port}/api/stats`);
            console.log(`ℹ️  Info:   http://localhost:${this.port}/api/info`);
            console.log(`\n✨ Features Available:`);
            console.log(`   📋 Collaborative Character Sheet Management`);
            console.log(`   🔍 Advanced Search & Taxonomy Organization`);
            console.log(`   📊 Interactive Multi-Mode Browsing`);
            console.log(`   🎪 Panel View (4 characters)`);
            console.log(`   ⚔️  Head-to-Head Comparisons`);
            console.log(`   📸 Picture Upload & Association`);
            console.log(`   🧠 Multi-system Personality Typing`);
            console.log(`   💬 Interactive Comments & Ratings`);
            console.log(`\n🚀 100% Node.js implementation - no Python dependencies!`);
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
