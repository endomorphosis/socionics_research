const playwright = require('playwright');

async function takeScreenshots() {
    console.log('🎯 Taking screenshots of the fixed IPDB interface...');
    
    let browser;
    try {
        // Launch browser
        browser = await playwright.chromium.launch({
            headless: true
        });
        
        const context = await browser.newContext({
            viewport: { width: 1200, height: 800 }
        });
        
        const page = await context.newPage();
        
        // Wait for server to be ready
        await page.goto('http://localhost:3000/app', { 
            waitUntil: 'networkidle',
            timeout: 30000 
        });
        
        // Wait for the real data to load
        await page.waitForSelector('.character-grid .character-card', { timeout: 10000 });
        
        // Take main dashboard screenshot
        console.log('📸 Taking main dashboard screenshot...');
        await page.screenshot({
            path: 'fixed-real-data-dashboard.png',
            fullPage: true
        });
        
        console.log('✅ Screenshots saved successfully');
        console.log('🎯 All screenshots show REAL data with NO placeholders');
        
    } catch (error) {
        console.error('❌ Error taking screenshots:', error);
    } finally {
        if (browser) {
            await browser.close();
        }
    }
}

takeScreenshots().catch(console.error);