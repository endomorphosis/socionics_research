const PlaywrightScraper = require('./playwright-scraper');
const SeleniumScraper = require('./selenium-scraper');
const HttpScraper = require('./http-scraper');
const DataEnhancer = require('./data-enhancer');
const PythonBotScraper = require('./python-bot-scraper');
const ScrapingReportGenerator = require('./report-generator');

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
            } else if (scraperType === 'http') {
                console.log('Attempting to create HTTP scraper...');
                this.activeScraper = new HttpScraper(this.options);
                await this.activeScraper.init();
                return this.activeScraper;
            } else if (scraperType === 'python-bot') {
                console.log('Attempting to create Python Bot scraper...');
                this.activeScraper = new PythonBotScraper(this.options);
                await this.activeScraper.init();
                return this.activeScraper;
            } else {
                throw new Error(`Unknown scraper type: ${scraperType}`);
            }
        } catch (error) {
            console.error(`Failed to create ${scraperType} scraper:`, error);
            
            // Fallback logic - try Python bot first, then HTTP scraper if others fail
            if (scraperType !== 'python-bot') {
                console.log('Falling back to Python Bot scraper...');
                try {
                    this.activeScraper = new PythonBotScraper(this.options);
                    await this.activeScraper.init();
                    this.scraperType = 'python-bot';
                    return this.activeScraper;
                } catch (fallbackError) {
                    console.error('Python Bot fallback also failed:', fallbackError);
                    
                    // Final fallback to HTTP
                    console.log('Falling back to HTTP scraper...');
                    try {
                        this.activeScraper = new HttpScraper(this.options);
                        await this.activeScraper.init();
                        this.scraperType = 'http';
                        return this.activeScraper;
                    } catch (httpError) {
                        console.error('All scrapers failed to initialize');
                        throw new Error('All scrapers failed to initialize');
                    }
                }
            } else {
                throw error;
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
        console.log('  node scraper/index.js full-scrape [--browser playwright|selenium|http|python-bot]');
        console.log('  node scraper/index.js profile <url> [--browser playwright|selenium|http|python-bot]');
        console.log('  node scraper/index.js search <query> [--browser playwright|selenium|http|python-bot]');
        console.log('  node scraper/index.js category <url> [--browser playwright|selenium|http|python-bot]');
        console.log('  node scraper/index.js enhance-data');
        console.log('  node scraper/index.js analyze-missing');
        console.log('  node scraper/index.js generate-report');
        console.log('  node scraper/index.js quick-summary');
        process.exit(1);
    }

    // Parse options
    const browserIndex = args.findIndex(arg => arg === '--browser');
    const browser = browserIndex !== -1 ? args[browserIndex + 1] : 'playwright';
    
    const scraper = new ScraperManager({
        browser: browser,
        delay: 1500,
        maxPages: 10,
        concurrency: 3,
        ratePerMinute: 90
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

            case 'enhance-data':
                console.log('Enhancing existing data...');
                const enhancer = new DataEnhancer();
                const enhancementResult = await enhancer.enhanceData();
                console.log(`Data enhancement completed. Enhanced ${enhancementResult.enhancedCount} profiles.`);
                await enhancer.close();
                break;

            case 'analyze-missing':
                console.log('Analyzing missing data...');
                const analyzer = new DataEnhancer();
                const missingProfiles = await analyzer.findMissingProfiles();
                console.log(`Analysis completed. Found ${missingProfiles.length} profiles with missing data.`);
                await analyzer.close();
                break;

            case 'generate-report':
                console.log('Generating comprehensive improvement report...');
                const reportGen = new ScrapingReportGenerator();
                const report = await reportGen.generateComprehensiveReport();
                console.log('Report generation completed.');
                break;

            case 'quick-summary':
                console.log('Generating quick data summary...');
                const summaryGen = new ScrapingReportGenerator();
                const summary = await summaryGen.quickDataSummary();
                console.log('Summary completed.');
                break;

            default:
                console.error(`Unknown command: ${command}`);
                process.exit(1);
        }

    } catch (error) {
        console.error('Operation failed:', error);
        process.exit(1);
    } finally {
        if (scraper) {
            await scraper.close();
        }
    }
}

// Run CLI if called directly
if (require.main === module) {
    main().catch(console.error);
}

module.exports = ScraperManager;