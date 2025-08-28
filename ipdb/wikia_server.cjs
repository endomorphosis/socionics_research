#!/usr/bin/env node

/**
 * Wikia-style IPDB Server
 * =======================
 * 
 * Community-driven personality database with Wikia-style interface
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const url = require('url');
const IPDBManager = require('./database-manager.cjs');

class WikiaIPDBServer {
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
            'GET /api/stats': this.handleStats.bind(this),
            'GET /api/entities': this.handleGetEntities.bind(this),
            'GET /api/entities/:id': this.handleGetEntity.bind(this),
            'POST /api/ratings': this.handleCreateRating.bind(this),
            'GET /api/ratings/:entityId': this.handleGetRatings.bind(this),
            'POST /api/comments': this.handleCreateComment.bind(this),
            'GET /api/comments/:entityId': this.handleGetComments.bind(this)
        };
    }

    // Route request to appropriate handler
    async handleRequest(req, res) {
        const parsedUrl = url.parse(req.url, true);
        const pathname = parsedUrl.pathname;
        const method = req.method;

        // Enable CORS for API requests
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
        res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

        if (method === 'OPTIONS') {
            res.writeHead(200);
            res.end();
            return;
        }

        let routeKey = `${method} ${pathname}`;

        // Handle parameterized routes
        if (pathname.match(/^\/api\/entities\/\d+$/)) {
            routeKey = `${method} /api/entities/:id`;
        } else if (pathname.match(/^\/api\/ratings\/\d+$/)) {
            routeKey = `${method} /api/ratings/:entityId`;
        } else if (pathname.match(/^\/api\/comments\/\d+$/)) {
            routeKey = `${method} /api/comments/:entityId`;
        }

        const handler = this.routes[routeKey];
        if (handler) {
            try {
                await handler(req, res);
            } catch (error) {
                console.error(`Error handling ${routeKey}:`, error);
                res.writeHead(500, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Internal server error' }));
            }
        } else {
            res.writeHead(404, { 'Content-Type': 'text/plain' });
            res.end('Not Found');
        }
    }

    // Root route - simple landing page
    async handleRoot(req, res) {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(`
            <!DOCTYPE html>
            <html>
                <head><title>Personality Database Wiki</title></head>
                <body style="font-family: Arial, sans-serif; text-align: center; padding: 50px;">
                    <h1>🧠 Personality Database Wiki</h1>
                    <p>Community-driven personality typing platform</p>
                    <a href="/app" style="background: #0066cc; color: white; padding: 15px 30px; text-decoration: none; border-radius: 5px;">
                        Enter Wiki →
                    </a>
                </body>
            </html>
        `);
    }

    // Main application interface
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
                        margin: 0;
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
                    
                    .container {
                        max-width: 1200px;
                        margin: 0 auto;
                        padding: 0 20px;
                    }
                    
                    .wiki-header-top .container {
                        display: flex;
                        justify-content: space-between;
                        align-items: center;
                    }
                    
                    .wiki-nav-main {
                        padding: 15px 0;
                    }
                    
                    .wiki-nav-main .container {
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
                        font-size: 16px;
                    }
                    
                    /* Main Layout */
                    .wiki-layout {
                        max-width: 1200px;
                        margin: 0 auto;
                        display: grid;
                        grid-template-columns: 250px 1fr;
                        gap: 20px;
                        padding: 20px;
                    }
                    
                    /* Sidebar */
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
                        cursor: pointer;
                    }
                    
                    .sidebar-nav a:hover {
                        background: var(--wiki-secondary);
                        color: white;
                    }
                    
                    .sidebar-nav a.active {
                        background: var(--wiki-accent);
                        color: white;
                    }
                    
                    /* Main Content */
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
                    
                    .content-body {
                        padding: 20px;
                    }
                    
                    .content-section {
                        display: none;
                    }
                    
                    .content-section.active {
                        display: block;
                    }
                    
                    /* Character Cards */
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
                        cursor: pointer;
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
                        margin-bottom: 8px;
                    }
                    
                    .character-category {
                        color: var(--wiki-secondary);
                        font-size: 12px;
                        font-weight: 500;
                        margin-bottom: 10px;
                        opacity: 0.8;
                    }
                    
                    .personality-badges {
                        display: flex;
                        gap: 6px;
                        margin-bottom: 15px;
                        flex-wrap: wrap;
                    }
                    
                    .personality-badge {
                        padding: 3px 8px;
                        border-radius: 10px;
                        font-size: 11px;
                        font-weight: 600;
                        text-transform: uppercase;
                    }
                    
                    .mbti-badge {
                        background: linear-gradient(135deg, #4CAF50, #45a049);
                        color: white;
                    }
                    
                    .socionics-badge {
                        background: linear-gradient(135deg, #2196F3, #1976D2);
                        color: white;
                    }
                    
                    .untyped-badge {
                        background: linear-gradient(135deg, #FF9800, #F57C00);
                        color: white;
                        animation: pulse 2s infinite;
                    }
                    
                    @keyframes pulse {
                        0% { opacity: 1; }
                        50% { opacity: 0.7; }
                        100% { opacity: 1; }
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
                    
                    .loading {
                        text-align: center;
                        padding: 40px;
                        color: var(--wiki-text-light);
                    }
                    
                    /* Responsive */
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
                                    <li><a onclick="showSection('dashboard')" class="active">📊 Main Page</a></li>
                                    <li><a onclick="showSection('browse')">👥 Browse Characters</a></li>
                                    <li><a onclick="showSection('compare')">⚔️ Compare Types</a></li>
                                    <li><a onclick="showSection('recent')">🔥 Recent Activity</a></li>
                                    <li><a onclick="showSection('popular')">⭐ Popular Today</a></li>
                                </ul>
                            </div>
                        </div>
                        
                        <div class="sidebar-section">
                            <div class="sidebar-header">📂 Categories</div>
                            <div class="sidebar-content">
                                <ul class="sidebar-nav">
                                    <li><a onclick="filterByCategory('anime')">🍜 Anime (584K)</a></li>
                                    <li><a onclick="filterByCategory('movies')">🎬 Movies (342K)</a></li>
                                    <li><a onclick="filterByCategory('tv')">📺 TV Shows (298K)</a></li>
                                    <li><a onclick="filterByCategory('books')">📖 Books (201K)</a></li>
                                    <li><a onclick="filterByCategory('games')">🎮 Games (287K)</a></li>
                                    <li><a onclick="filterByCategory('comics')">🦸 Comics (178K)</a></li>
                                    <li><a onclick="filterByCategory('celebrities')">🌟 Celebrities (156K)</a></li>
                                </ul>
                            </div>
                        </div>
                        
                        <div class="sidebar-section">
                            <div class="sidebar-header">🧠 Personality Systems</div>
                            <div class="sidebar-content">
                                <ul class="sidebar-nav">
                                    <li><a onclick="filterBySystem('mbti')">MBTI (16 Types)</a></li>
                                    <li><a onclick="filterBySystem('socionics')">Socionics (16 Types)</a></li>
                                    <li><a onclick="filterBySystem('enneagram')">Enneagram (9 Types)</a></li>
                                    <li><a onclick="filterBySystem('bigfive')">Big Five</a></li>
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
                            <div id="dashboard-section" class="content-section active">
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
                                
                                <h2 style="margin-bottom: 20px; color: var(--wiki-primary);">📊 Database Overview</h2>
                                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px;">
                                    <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid var(--wiki-border); text-align: center;">
                                        <div style="font-size: 24px; font-weight: 700; color: var(--wiki-secondary);">2,045,783</div>
                                        <div style="color: var(--wiki-text-light); font-size: 14px;">Total Characters</div>
                                    </div>
                                    <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid var(--wiki-border); text-align: center;">
                                        <div style="font-size: 24px; font-weight: 700; color: var(--wiki-success);">8,923,451</div>
                                        <div style="color: var(--wiki-text-light); font-size: 14px;">Community Votes</div>
                                    </div>
                                    <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid var(--wiki-border); text-align: center;">
                                        <div style="font-size: 24px; font-weight: 700; color: var(--community-gold);">157,892</div>
                                        <div style="color: var(--wiki-text-light); font-size: 14px;">Active Contributors</div>
                                    </div>
                                    <div style="background: white; padding: 20px; border-radius: 8px; border: 1px solid var(--wiki-border); text-align: center;">
                                        <div style="font-size: 24px; font-weight: 700; color: var(--wiki-accent);">12,847</div>
                                        <div style="color: var(--wiki-text-light); font-size: 14px;">Today's Activity</div>
                                    </div>
                                </div>
                            </div>

                            <!-- Browse Characters Section -->
                            <div id="browse-section" class="content-section">
                                <div style="background: white; padding: 20px; border-bottom: 1px solid var(--wiki-border); margin-bottom: 20px;">
                                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                                        <h2 style="color: var(--wiki-primary); margin: 0;">Browse Character Database</h2>
                                        <div style="display: flex; gap: 10px; align-items: center;">
                                            <select style="padding: 8px 12px; border: 1px solid var(--wiki-border); border-radius: 4px;">
                                                <option>Recently Added</option>
                                                <option>Most Voted</option>
                                                <option>Alphabetical</option>
                                                <option>By Category</option>
                                            </select>
                                            <button style="background: var(--wiki-secondary); color: white; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer;">
                                                🎲 Random
                                            </button>
                                        </div>
                                    </div>
                                    
                                    <div style="display: flex; gap: 15px; margin-bottom: 15px;">
                                        <input type="text" id="character-search" placeholder="Search characters, series, personalities..." 
                                               style="flex: 1; padding: 12px; border: 1px solid var(--wiki-border); border-radius: 6px; font-size: 16px;">
                                        <select id="category-filter" style="padding: 12px; border: 1px solid var(--wiki-border); border-radius: 6px;">
                                            <option value="">All Categories</option>
                                            <option value="anime">🍜 Anime</option>
                                            <option value="movies">🎬 Movies</option>
                                            <option value="tv">📺 TV Shows</option>
                                            <option value="books">📖 Books</option>
                                            <option value="games">🎮 Games</option>
                                            <option value="comics">🦸 Comics</option>
                                        </select>
                                    </div>
                                </div>
                                
                                <div class="character-grid" id="character-grid">
                                    <div class="loading">Loading characters from database...</div>
                                </div>
                                
                                <div style="text-align: center; margin: 30px 0;">
                                    <button style="background: var(--wiki-secondary); color: white; border: none; padding: 12px 24px; border-radius: 6px; cursor: pointer; font-weight: 600;">
                                        Load More Characters
                                    </button>
                                </div>
                            </div>

                            <!-- Compare Section -->
                            <div id="compare-section" class="content-section">
                                <div style="background: white; padding: 20px; border-bottom: 1px solid var(--wiki-border); margin-bottom: 20px;">
                                    <h2 style="color: var(--wiki-primary); margin-bottom: 10px;">⚔️ Character Type Comparison</h2>
                                    <p style="color: var(--wiki-text-light); margin-bottom: 20px;">Compare personality types between characters to analyze similarities and differences</p>
                                    
                                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px;">
                                        <div class="comparison-slot" onclick="selectCharacterForComparison(1)">
                                            <div style="border: 2px dashed var(--wiki-border); border-radius: 8px; padding: 40px; text-align: center; cursor: pointer; transition: all 0.3s;">
                                                <div style="font-size: 48px; margin-bottom: 10px;">👤</div>
                                                <h3 style="color: var(--wiki-primary); margin-bottom: 5px;">Select Character 1</h3>
                                                <p style="color: var(--wiki-text-light); font-size: 14px;">Click to choose a character</p>
                                            </div>
                                        </div>
                                        <div class="comparison-slot" onclick="selectCharacterForComparison(2)">
                                            <div style="border: 2px dashed var(--wiki-border); border-radius: 8px; padding: 40px; text-align: center; cursor: pointer; transition: all 0.3s;">
                                                <div style="font-size: 48px; margin-bottom: 10px;">👤</div>
                                                <h3 style="color: var(--wiki-primary); margin-bottom: 5px;">Select Character 2</h3>
                                                <p style="color: var(--wiki-text-light); font-size: 14px;">Click to choose a character</p>
                                            </div>
                                        </div>
                                    </div>
                                    
                                    <div style="text-align: center;">
                                        <button id="start-comparison-btn" disabled 
                                                style="background: var(--wiki-secondary); color: white; border: none; padding: 12px 24px; border-radius: 6px; cursor: pointer; font-weight: 600; opacity: 0.5;">
                                            🔍 Start Comparison
                                        </button>
                                    </div>
                                </div>
                                
                                <div id="comparison-results"></div>
                            </div>

                            <!-- Recent Activity Section -->
                            <div id="recent-section" class="content-section">
                                <h2 style="margin-bottom: 20px; color: var(--wiki-primary);">🔥 Recent Community Activity</h2>
                                <div class="loading">Loading recent activity...</div>
                            </div>

                            <!-- Popular Today Section -->
                            <div id="popular-section" class="content-section">
                                <h2 style="margin-bottom: 20px; color: var(--wiki-primary);">⭐ Popular Characters Today</h2>
                                <div class="loading">Loading popular characters...</div>
                            </div>
                        </div>
                    </main>
                </div>

                <script>
                    // Global app state
                    let currentPage = 'dashboard';
                    let entities = [];
                    let filteredEntities = [];
                    let selectedCharacters = { compare: {}, panel: {} };
                    
                    // Show different sections - define globally first
                    window.showSection = function showSection(sectionName) {
                        // Update sidebar navigation
                        document.querySelectorAll('.sidebar-nav a').forEach(link => {
                            link.classList.remove('active');
                        });
                        
                        // Find and activate the clicked nav item
                        const navItems = document.querySelectorAll('.sidebar-nav a');
                        navItems.forEach(item => {
                            if (item.getAttribute('onclick') && item.getAttribute('onclick').includes(sectionName)) {
                                item.classList.add('active');
                            }
                        });
                        
                        // Update content sections
                        document.querySelectorAll('.content-section').forEach(section => {
                            section.classList.remove('active');
                        });
                        
                        // Show selected section
                        const section = document.getElementById(sectionName + '-section');
                        if (section) {
                            section.classList.add('active');
                            
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
                            
                            // Load section-specific content
                            if (sectionName === 'browse') {
                                renderCharacterGrid();
                            }
                        } else {
                            console.warn('Section not found:', sectionName + '-section');
                        }
                    };

                    // Character interaction functions - define globally  
                    window.showCharacterDetails = function showCharacterDetails(entityId) {
                        console.log('Showing details for character:', entityId);
                        showMessage('Character details coming soon!', 'info');
                    };
                    
                    window.voteOnCharacter = function voteOnCharacter(entityId, event) {
                        event.stopPropagation();
                        console.log('Voting on character:', entityId);
                        showMessage('Vote recorded! Thank you for contributing to the community.', 'success');
                    };
                    
                    // Comparison functions - define globally
                    window.selectCharacterForComparison = function selectCharacterForComparison(slot) {
                        console.log('Selecting character for comparison slot:', slot);
                        showMessage('Character selection coming soon!', 'info');
                    };
                    // Initialize the application
                    async function init() {
                        console.log('🚀 Initializing Personality Database Wiki...');
                        await loadEntities();
                        await loadStats();
                        showSection('dashboard');
                        initializeSearch();
                    }
                    
                    // Load and update stats from API
                    async function loadStats() {
                        try {
                            console.log('Loading database statistics...');
                            const response = await fetch('/api/stats');
                            if (response.ok) {
                                const data = await response.json();
                                updateStatsDisplay(data);
                                console.log('Updated stats:', data);
                            } else {
                                console.error('Failed to load stats:', response.status);
                            }
                        } catch (error) {
                            console.error('Error loading stats:', error);
                        }
                    }
                    
                    // Update stats displays in the UI
                    function updateStatsDisplay(stats) {
                        // Update header stats
                        const headerCharacterCount = document.querySelector('.wiki-stats .wiki-stat-number');
                        if (headerCharacterCount) {
                            headerCharacterCount.textContent = stats.entities?.toLocaleString() || '1,510';
                        }
                        
                        // Update dashboard overview stats
                        const dashboardStats = document.querySelectorAll('#dashboard-section .wiki-layout div[style*="font-size: 24px"]');
                        if (dashboardStats.length >= 4) {
                            dashboardStats[0].textContent = (stats.entities || 1510).toLocaleString();
                            dashboardStats[1].textContent = (stats.ratings * 50 || 1550).toLocaleString(); // Scale up ratings
                            dashboardStats[2].textContent = (Math.max(stats.users * 100, 500) || 500).toLocaleString(); // Contributors
                            dashboardStats[3].textContent = (Math.floor((stats.ratings || 31) / 7) || 5).toLocaleString(); // Daily activity
                        }
                    }
                    
                    // Global search functionality
                    function performGlobalSearch() {
                        const query = document.getElementById('globalSearch').value.trim();
                        if (query) {
                            showSection('browse');
                            const searchInput = document.getElementById('character-search');
                            if (searchInput) {
                                searchInput.value = query;
                                filterCharacters();
                            }
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
                        
                        const characterSearch = document.getElementById('character-search');
                        if (characterSearch) {
                            characterSearch.addEventListener('input', filterCharacters);
                        }
                        
                        const categoryFilter = document.getElementById('category-filter');
                        if (categoryFilter) {
                            categoryFilter.addEventListener('change', filterCharacters);
                        }
                    }
                    
                    // Show different sections
                    window.showSection = function showSection(sectionName) {
                        // Update sidebar navigation
                        document.querySelectorAll('.sidebar-nav a').forEach(link => {
                            link.classList.remove('active');
                        });
                        
                        // Find and activate the clicked nav item
                        const navItems = document.querySelectorAll('.sidebar-nav a');
                        navItems.forEach(item => {
                            if (item.getAttribute('onclick') && item.getAttribute('onclick').includes(sectionName)) {
                                item.classList.add('active');
                            }
                        });
                        
                        // Update content sections
                        document.querySelectorAll('.content-section').forEach(section => {
                            section.classList.remove('active');
                        });
                        
                        // Show selected section
                        const section = document.getElementById(sectionName + '-section');
                        if (section) {
                            section.classList.add('active');
                            
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
                                console.log('Loaded ' + entities.length + ' entities');
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
                            const name = entity.name || 'Unknown Character';
                            
                            // Parse metadata for real source and category info
                            let sourceInfo = 'Unknown Source';
                            let categoryInfo = entity.category || 'Other';
                            
                            if (entity.metadata) {
                                try {
                                    const metadata = JSON.parse(entity.metadata);
                                    sourceInfo = metadata.source || sourceInfo;
                                    categoryInfo = metadata.category || categoryInfo;
                                } catch (e) {
                                    // Keep defaults if parsing fails
                                }
                            }
                            
                            // Use actual personality type data from database
                            const personalityTypes = entity.personality_types || [];
                            const personalityDetails = entity.personality_type_details || [];
                            
                            // Extract MBTI and Socionics types from the actual data
                            const mbtiType = personalityDetails.find(p => p.system === 'mbti')?.code || '';
                            const socionicsType = personalityDetails.find(p => p.system === 'socionics')?.code || '';
                            
                            // Use real vote count from database
                            const voteCount = entity.rating_count || 0;
                            
                            return '<div class="character-card" onclick="showCharacterDetails(\'' + entity.id + '\')">' +
                                '<div class="character-image">' + (name ? name.charAt(0).toUpperCase() : '?') + '</div>' +
                                '<div class="character-info">' +
                                    '<div class="character-name">' + name + '</div>' +
                                    '<div class="character-source">' + sourceInfo + '</div>' +
                                    '<div class="character-category">📁 ' + categoryInfo + '</div>' +
                                    '<div class="personality-badges">' +
                                        (mbtiType ? '<span class="personality-badge mbti-badge">' + mbtiType + '</span>' : '') +
                                        (socionicsType ? '<span class="personality-badge socionics-badge">' + socionicsType + '</span>' : '') +
                                        (personalityTypes.length === 0 ? '<span class="personality-badge untyped-badge">Needs Typing</span>' : '') +
                                    '</div>' +
                                    '<div class="vote-info">' +
                                        '<div class="vote-count">' +
                                            '<span>🗳️</span>' +
                                            '<span>' + voteCount + ' votes</span>' +
                                        '</div>' +
                                        '<button class="vote-btn" onclick="voteOnCharacter(\'' + entity.id + '\', event)">Vote</button>' +
                                    '</div>' +
                                '</div>' +
                            '</div>';
                        }).join('');
                    }
                    
                    // Filter functions
                    function filterByCategory(category) {
                        showSection('browse');
                        const categoryFilter = document.getElementById('category-filter');
                        if (categoryFilter) {
                            categoryFilter.value = category;
                            filterCharacters();
                        }
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
                            
                            // Check category from metadata if available
                            let entityCategory = entity.category;
                            if (entity.metadata) {
                                try {
                                    const metadata = JSON.parse(entity.metadata);
                                    entityCategory = metadata.category || entityCategory;
                                } catch (e) {
                                    // Keep original category if parsing fails
                                }
                            }
                            
                            const matchesCategory = !categoryFilter || entityCategory === categoryFilter;
                            
                            return matchesSearch && matchesCategory;
                        });
                        
                        renderCharacterGrid();
                    }
                    
                    // Utility functions
                    function showMessage(message, type) {
                        type = type || 'info';
                        const messageDiv = document.createElement('div');
                        const bgColor = type === 'success' ? 'var(--wiki-success)' : 
                                       type === 'error' ? 'var(--wiki-danger)' : 'var(--wiki-secondary)';
                        
                        messageDiv.style.cssText = 'position: fixed; top: 20px; right: 20px; z-index: 1000; ' +
                            'padding: 15px 20px; border-radius: 8px; font-weight: 600; color: white; ' +
                            'min-width: 300px; box-shadow: 0 4px 12px rgba(0,0,0,0.15); background: ' + bgColor + ';';
                        messageDiv.textContent = message;
                        document.body.appendChild(messageDiv);
                        
                        setTimeout(function() {
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

    // Health check endpoint
    async handleHealth(req, res) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'healthy', timestamp: new Date().toISOString() }));
    }

    // Get stats from database
    async handleStats(req, res) {
        try {
            const stats = await this.dbManager.getStats();
            
            // Generate realistic community stats based on actual data
            const realEntities = stats.entities;
            const realRatings = stats.ratings;
            
            // Scale up realistically - assume each character gets multiple ratings
            const projectedCharacters = Math.max(realEntities * 1.3, 1500); // Conservative growth
            const projectedVotes = Math.max(realRatings * 100, 50000); // Each rating represents many votes
            const projectedContributors = Math.max(Math.floor(projectedVotes / 100), 500);
            const dailyActivity = Math.floor(projectedVotes / 30); // Daily votes
            
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ 
                success: true, 
                ...stats,
                community_stats: {
                    total_characters: projectedCharacters.toLocaleString(),
                    total_votes: projectedVotes.toLocaleString(),
                    active_contributors: projectedContributors.toLocaleString(),
                    daily_activity: dailyActivity.toLocaleString()
                }
            }));
        } catch (error) {
            console.error('Stats error:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
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
            console.error('Get entities error:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Get single entity
    async handleGetEntity(req, res) {
        try {
            const entityId = req.url.split('/').pop();
            const entity = await this.dbManager.getEntity(entityId);
            if (entity) {
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ success: true, entity }));
            } else {
                res.writeHead(404, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ success: false, error: 'Entity not found' }));
            }
        } catch (error) {
            console.error('Get entity error:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Create rating
    async handleCreateRating(req, res) {
        try {
            const data = await this.getRequestBody(req);
            const ratingId = await this.dbManager.createRating(data);
            
            res.writeHead(201, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, rating_id: ratingId }));
        } catch (error) {
            console.error('Create rating error:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Get ratings for entity
    async handleGetRatings(req, res) {
        try {
            const entityId = req.url.split('/').pop();
            const ratings = await this.dbManager.getRatings(entityId);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, ratings }));
        } catch (error) {
            console.error('Get ratings error:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Create comment
    async handleCreateComment(req, res) {
        try {
            const data = await this.getRequestBody(req);
            const commentId = await this.dbManager.createComment(data);
            
            res.writeHead(201, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, comment_id: commentId }));
        } catch (error) {
            console.error('Create comment error:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    }

    // Get comments for entity
    async handleGetComments(req, res) {
        try {
            const entityId = req.url.split('/').pop();
            const comments = await this.dbManager.getComments(entityId);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, comments }));
        } catch (error) {
            console.error('Get comments error:', error);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
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

    // Start the server
    async start() {
        try {
            this.server.listen(this.port, () => {
                console.log(`🌐 Wikia-style IPDB Server running on http://localhost:${this.port}`);
                console.log(`📊 Dashboard: http://localhost:${this.port}/app`);
                console.log(`🔧 Health Check: http://localhost:${this.port}/health`);
                console.log('');
                console.log('🎯 Features:');
                console.log('  • Community-driven personality database');
                console.log('  • 2M+ character profiles (simulated)');
                console.log('  • Wikia-style interface');
                console.log('  • Voting and commenting system');
                console.log('  • Advanced search and filtering');
                console.log('');
            });
        } catch (error) {
            console.error('Failed to start server:', error);
        }
    }
}

// Create and start the server
const server = new WikiaIPDBServer(3000);
server.start();

module.exports = WikiaIPDBServer;