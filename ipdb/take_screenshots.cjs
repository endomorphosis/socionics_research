#!/usr/bin/env node

/**
 * Take Screenshots with Puppeteer
 * ===============================
 * 
 * Simple script to take screenshots of the updated interface
 */

const fs = require('fs');

async function takeScreenshots() {
    console.log('📸 Taking screenshots of the updated interface...');
    
    try {
        // First install puppeteer which is lighter than playwright
        const { execSync } = require('child_process');
        console.log('Installing puppeteer...');
        execSync('npm install puppeteer --no-save', { stdio: 'inherit' });
        
        const puppeteer = require('puppeteer');
        
        console.log('Launching browser...');
        const browser = await puppeteer.launch({
            headless: true,
            args: ['--no-sandbox', '--disable-setuid-sandbox']
        });
        
        const page = await browser.newPage();
        await page.setViewport({ width: 1920, height: 1080 });
        
        // Wait for server to be ready
        console.log('Connecting to server...');
        await page.goto('http://localhost:3000/app', { waitUntil: 'networkidle2', timeout: 10000 });
        
        // Screenshot 1: Main dashboard
        console.log('Taking dashboard screenshot...');
        await page.screenshot({ 
            path: '/tmp/dashboard_with_data.png',
            fullPage: true 
        });
        
        // Navigate to browse page
        console.log('Navigating to browse page...');
        await page.click('text=Browse Characters');
        await page.waitForSelector('.character-grid', { timeout: 5000 });
        await page.screenshot({ 
            path: '/tmp/browse_characters_with_data.png',
            fullPage: true 
        });
        
        console.log('✅ Screenshots saved to /tmp/');
        
        await browser.close();
        
    } catch (error) {
        console.error('❌ Screenshot failed:', error.message);
        
        // Try with curl to test if server is responding
        console.log('Testing server response...');
        try {
            const { execSync } = require('child_process');
            const response = execSync('curl -s http://localhost:3000/health', { encoding: 'utf8' });
            console.log('Server response:', response);
        } catch (e) {
            console.log('Server not responding to curl');
        }
    }
}

takeScreenshots();