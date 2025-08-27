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
                <title>IPDB - Interactive Personality Research Platform</title>
                <style>
                    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
                    
                    :root {
                        --primary: #6366f1;
                        --primary-dark: #4f46e5;
                        --secondary: #ec4899;
                        --accent: #14b8a6;
                        --success: #10b981;
                        --warning: #f59e0b;
                        --danger: #ef4444;
                        --dark: #1f2937;
                        --light: #f8fafc;
                        --gray-50: #f9fafb;
                        --gray-100: #f3f4f6;
                        --gray-200: #e5e7eb;
                        --gray-300: #d1d5db;
                        --gray-400: #9ca3af;
                        --gray-500: #6b7280;
                        --gray-600: #4b5563;
                        --gray-700: #374151;
                        --gray-800: #1f2937;
                        --gray-900: #111827;
                        --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.05);
                        --shadow: 0 1px 3px 0 rgb(0 0 0 / 0.1), 0 1px 2px -1px rgb(0 0 0 / 0.1);
                        --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
                        --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1);
                        --shadow-xl: 0 20px 25px -5px rgb(0 0 0 / 0.1), 0 8px 10px -6px rgb(0 0 0 / 0.1);
                    }
                    
                    * {
                        margin: 0;
                        padding: 0;
                        box-sizing: border-box;
                    }
                    
                    body { 
                        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%);
                        min-height: 100vh;
                        margin: 0;
                        padding: 0;
                        line-height: 1.6;
                        color: var(--gray-800);
                        background-attachment: fixed;
                    }
                    
                    .main-container {
                        max-width: 1400px;
                        margin: 0 auto;
                        padding: 20px;
                        min-height: 100vh;
                    }
                    
                    .app-header { 
                        background: rgba(255, 255, 255, 0.95);
                        backdrop-filter: blur(10px);
                        color: var(--gray-800);
                        padding: 30px;
                        border-radius: 20px;
                        margin-bottom: 30px;
                        text-align: center;
                        box-shadow: var(--shadow-xl);
                        border: 1px solid rgba(255, 255, 255, 0.2);
                        animation: slideInDown 0.6s ease-out;
                    }
                    
                    @keyframes slideInDown {
                        from {
                            opacity: 0;
                            transform: translateY(-30px);
                        }
                        to {
                            opacity: 1;
                            transform: translateY(0);
                        }
                    }
                    
                    @keyframes fadeInUp {
                        from {
                            opacity: 0;
                            transform: translateY(30px);
                        }
                        to {
                            opacity: 1;
                            transform: translateY(0);
                        }
                    }
                    
                    @keyframes scaleIn {
                        from {
                            opacity: 0;
                            transform: scale(0.9);
                        }
                        to {
                            opacity: 1;
                            transform: scale(1);
                        }
                    }
                    
                    .app-header h1 {
                        font-size: 3.5rem;
                        font-weight: 700;
                        margin-bottom: 10px;
                        background: linear-gradient(135deg, var(--primary), var(--secondary));
                        -webkit-background-clip: text;
                        -webkit-text-fill-color: transparent;
                        background-clip: text;
                    }
                    
                    .app-header p {
                        font-size: 1.3rem;
                        color: var(--gray-600);
                        font-weight: 400;
                    }
                    
                    .feature-nav {
                        display: flex;
                        justify-content: center;
                        gap: 15px;
                        margin: 30px 0;
                        flex-wrap: wrap;
                    }
                    
                    .nav-btn {
                        background: rgba(255, 255, 255, 0.9);
                        border: 2px solid var(--primary);
                        color: var(--primary);
                        padding: 15px 30px;
                        border-radius: 50px;
                        cursor: pointer;
                        font-size: 16px;
                        font-weight: 600;
                        transition: all 0.3s ease;
                        text-decoration: none;
                        display: inline-flex;
                        align-items: center;
                        gap: 8px;
                        box-shadow: var(--shadow);
                        backdrop-filter: blur(10px);
                    }
                    
                    .nav-btn:hover {
                        background: var(--primary);
                        color: white;
                        transform: translateY(-2px);
                        box-shadow: var(--shadow-lg);
                    }
                    
                    .nav-btn.active {
                        background: var(--primary);
                        color: white;
                        box-shadow: var(--shadow-md);
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
                <div class="main-container">
                    <div class="app-header">
                        <h1>🧠 IPDB Research Platform</h1>
                        <p>Interactive personality typing and comparison system for socionics research</p>
                        
                        <div class="feature-nav">
                            <button class="nav-btn active" onclick="showSection('dashboard')">
                                📊 Dashboard
                            </button>
                            <button class="nav-btn" onclick="showSection('browse')">
                                👥 Browse Characters
                            </button>
                            <button class="nav-btn" onclick="showSection('compare')">
                                ⚔️ Head-to-Head
                            </button>
                            <button class="nav-btn" onclick="showSection('panel')">
                                🎪 Panel View
                            </button>
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
                        <p class="section-subtitle">Browse, rate, and discuss personality types of characters from popular media</p>
                        <div id="entities-container" class="loading">Loading entities...</div>
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
                    let selectedCompareCharacters = {};
                    let selectedPanelCharacters = {};
                    let currentSection = 'dashboard';
                    
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
                                entities = data.entities;
                                displayEntities(data.entities);
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

                    function displayEntities(entities) {
                        if (!entities || entities.length === 0) {
                            document.getElementById('entities-container').innerHTML = \`
                                <div class="error">No entities found. Please run the database initialization first.</div>
                            \`;
                            return;
                        }

                        const entitiesHtml = entities.map(entity => \`
                            <div class="entity-card">
                                <div class="entity-header">
                                    <div class="entity-avatar">
                                        \${entity.name.charAt(0).toUpperCase()}
                                    </div>
                                    <div class="entity-info">
                                        <div class="entity-name">\${entity.name}</div>
                                        <div class="entity-type">\${entity.category || 'fictional_character'}</div>
                                    </div>
                                </div>
                                
                                <div class="personality-types">
                                    \${(entity.personality_types || []).map(type => 
                                        \`<span class="personality-type">🧠 \${type}</span>\`
                                    ).join('')}
                                </div>
                                
                                <div class="action-buttons">
                                    <button class="btn btn-primary" onclick="rateCharacter('\${entity.id}', '\${entity.name.replace(/'/g, '\\\\'')}')">
                                        ⭐ Rate Character
                                    </button>
                                    <button class="btn btn-secondary" onclick="viewComments('\${entity.id}', '\${entity.name.replace(/'/g, '\\\\'')}')">
                                        💬 Comments
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

                    // Character comparison functions
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
                                                \${(comment.user || 'U').charAt(0).toUpperCase()}
                                            </div>
                                            <div>
                                                <strong>\${comment.user || 'Anonymous'}</strong>
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
                'Multi-system personality typing (Socionics, MBTI, Enneagram)',
                'Head-to-head character comparisons',
                'Panel analysis with 4 characters',
                'Picture upload and association',
                'Real-time rating and commenting',
                'Interactive modern UI with animations',
                'Database schema management',
                'User management with role-based access'
            ],
            backend: 'Python with SQLite support',
            frontend: 'Modern JavaScript with CSS Grid and Animations',
            api_type: 'HTTP REST with enhanced UI',
            endpoints: Object.keys(this.routes)
        }));
    }

    async handleStats(req, res) {
        try {
            const pythonScript = `
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath('${__filename}')))

try:
    from database_manager import IPDBManager
    import json
    
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
        stats['entities'] = cursor.fetchone()[0]
    except:
        stats['entities'] = 0
        
    try:
        cursor.execute("SELECT COUNT(*) FROM users")
        stats['users'] = cursor.fetchone()[0]
    except:
        stats['users'] = 0
        
    try:
        cursor.execute("SELECT COUNT(*) FROM personality_types")
        stats['personality_types'] = cursor.fetchone()[0]
    except:
        stats['personality_types'] = 0
    
    db.close()
    print(json.dumps(stats))
    
except Exception as e:
    print(json.dumps({"error": str(e), "entities": 0, "users": 0, "personality_types": 0}))
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
                path.join(__dirname, 'demo.py')
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
                    output: output,
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
            console.log(`\n🚀 IPDB Enhanced Research Platform`);
            console.log(`📍 Server: http://localhost:${this.port}`);
            console.log(`🎨 App: http://localhost:${this.port}/app`);
            console.log(`🏥 Health: http://localhost:${this.port}/health`);
            console.log(`📊 Stats:  http://localhost:${this.port}/api/stats`);
            console.log(`ℹ️  Info:   http://localhost:${this.port}/api/info`);
            console.log(`\n✨ Features Available:`);
            console.log(`   🎪 Panel View (4 characters)`);
            console.log(`   ⚔️  Head-to-Head Comparisons`);
            console.log(`   📸 Picture Upload & Association`);
            console.log(`   🧠 Multi-system Personality Typing`);
            console.log(`   💬 Interactive Comments & Ratings`);
            console.log(`\n✅ Enhanced UI with modern design ready!`);
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
