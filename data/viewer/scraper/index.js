const PlaywrightScraper = require('./playwright-scraper');
const SeleniumScraper = require('./selenium-scraper');

class ScraperManager {
    constructor(options = {}) {
        this.options = options;
        this.activeScraper = null;
        this.scraperType = options.browser || 'playwright';
    }

    async createScraper(type = null) {
        const scraperType = type || this.scraperType;
        
        try {
            if (scraperType === 'playwright') {
                console.log('Attempting to create Playwright scraper...');
                this.activeScraper = new PlaywrightScraper(this.options);
                await this.activeScraper.init();
                return this.activeScraper;
            } else if (scraperType === 'selenium') {
                console.log('Attempting to create Selenium scraper...');
                this.activeScraper = new SeleniumScraper(this.options);
                await this.activeScraper.init();
                return this.activeScraper;
            } else {
                throw new Error(`Unknown scraper type: ${scraperType}`);
            }
        } catch (error) {
            console.error(`Failed to create ${scraperType} scraper:`, error);
            
            // Fallback logic
            if (scraperType === 'playwright') {
                console.log('Falling back to Selenium scraper...');
                try {
                    this.activeScraper = new SeleniumScraper(this.options);
                    await this.activeScraper.init();
                    this.scraperType = 'selenium';
                    return this.activeScraper;
                } catch (fallbackError) {
                    console.error('Selenium fallback also failed:', fallbackError);
                    throw new Error('Both Playwright and Selenium scrapers failed to initialize');
                }
            } else {
                // If selenium failed, try playwright
                console.log('Falling back to Playwright scraper...');
                try {
                    this.activeScraper = new PlaywrightScraper(this.options);
                    await this.activeScraper.init();
                    this.scraperType = 'playwright';
                    return this.activeScraper;
                } catch (fallbackError) {
                    console.error('Playwright fallback also failed:', fallbackError);
                    throw new Error('Both Selenium and Playwright scrapers failed to initialize');
                }
            }
        }
    }

    async scrapeProfile(profileUrl) {
        if (!this.activeScraper) {
            await this.createScraper();
        }
        return await this.activeScraper.scrapeProfile(profileUrl);
    }

    async scrapeSearch(query, maxResults = 50) {
        if (!this.activeScraper) {
            await this.createScraper();
        }
        return await this.activeScraper.scrapeSearch(query, maxResults);
    }

    async scrapeCategory(categoryUrl, maxProfiles = 100) {
        if (!this.activeScraper) {
            await this.createScraper();
        }
        return await this.activeScraper.scrapeCategory(categoryUrl, maxProfiles);
    }

    async fullScrape() {
        if (!this.activeScraper) {
            await this.createScraper();
        }
        return await this.activeScraper.fullScrape();
    }

    async close() {
        if (this.activeScraper) {
            await this.activeScraper.close();
            this.activeScraper = null;
        }
    }

    getScraperType() {
        return this.scraperType;
    }

    isRunning() {
        return this.activeScraper !== null;
    }
}

// CLI interface for running scraper directly
async function main() {
    const args = process.argv.slice(2);
    const command = args[0];
    
    if (!command) {
        console.log('Usage:');
        console.log('  node scraper/index.js full-scrape [--browser playwright|selenium]');
        console.log('  node scraper/index.js profile <url> [--browser playwright|selenium]');
        console.log('  node scraper/index.js search <query> [--browser playwright|selenium]');
        console.log('  node scraper/index.js category <url> [--browser playwright|selenium]');
        process.exit(1);
    }

    // Parse options
    const browserIndex = args.findIndex(arg => arg === '--browser');
    const browser = browserIndex !== -1 ? args[browserIndex + 1] : 'playwright';
    
    const scraper = new ScraperManager({
        browser: browser,
        delay: 1500,
        maxPages: 10
    });

    try {
        switch (command) {
            case 'full-scrape':
                console.log('Starting full scrape...');
                const profiles = await scraper.fullScrape();
                console.log(`Scrape completed. Found ${profiles.length} profiles.`);
                break;

            case 'profile':
                if (!args[1]) {
                    console.error('Profile URL required');
                    process.exit(1);
                }
                console.log(`Scraping profile: ${args[1]}`);
                const profile = await scraper.scrapeProfile(args[1]);
                console.log('Profile data:', JSON.stringify(profile, null, 2));
                break;

            case 'search':
                if (!args[1]) {
                    console.error('Search query required');
                    process.exit(1);
                }
                console.log(`Searching for: ${args[1]}`);
                const results = await scraper.scrapeSearch(args[1]);
                console.log(`Found ${results.length} results:`, JSON.stringify(results, null, 2));
                break;

            case 'category':
                if (!args[1]) {
                    console.error('Category URL required');
                    process.exit(1);
                }
                console.log(`Scraping category: ${args[1]}`);
                const categoryProfiles = await scraper.scrapeCategory(args[1]);
                console.log(`Found ${categoryProfiles.length} profiles in category.`);
                break;

            default:
                console.error(`Unknown command: ${command}`);
                process.exit(1);
        }

    } catch (error) {
        console.error('Scraping failed:', error);
        process.exit(1);
    } finally {
        await scraper.close();
    }
}

// Run CLI if called directly
if (require.main === module) {
    main().catch(console.error);
}

module.exports = ScraperManager;