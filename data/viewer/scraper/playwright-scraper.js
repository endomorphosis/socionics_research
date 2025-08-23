const { chromium } = require('playwright');
const fs = require('fs').promises;
const path = require('path');

class PlaywrightScraper {
    constructor(options = {}) {
        this.browser = null;
        this.page = null;
        this.baseUrl = options.baseUrl || 'https://www.personality-database.com';
        this.delay = options.delay || 1000;
        this.maxPages = options.maxPages || 10;
        this.outputDir = options.outputDir || path.resolve(__dirname, '../../bot_store');
    }

    async init() {
        try {
            console.log('Initializing Playwright browser...');
            this.browser = await chromium.launch({
                headless: true,
                args: ['--no-sandbox', '--disable-setuid-sandbox']
            });
            
            this.page = await this.browser.newPage();
            
            // Set user agent to avoid blocking
            await this.page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36');
            
            console.log('Playwright browser initialized successfully');
            return true;
        } catch (error) {
            console.error('Failed to initialize Playwright:', error);
            throw error;
        }
    }

    async scrapeProfile(profileUrl) {
        if (!this.page) {
            await this.init();
        }

        try {
            console.log(`Scraping profile: ${profileUrl}`);
            
            await this.page.goto(profileUrl, { waitUntil: 'networkidle' });
            await this.page.waitForTimeout(this.delay);

            // Extract profile data
            const profileData = await this.page.evaluate(() => {
                const data = {};
                
                // Try to extract name
                const nameElement = document.querySelector('h1') || 
                                  document.querySelector('.profile-name') ||
                                  document.querySelector('[data-testid="profile-name"]');
                data.name = nameElement ? nameElement.textContent.trim() : '';

                // Try to extract MBTI type
                const mbtiElement = document.querySelector('[data-testid="mbti"]') ||
                                  document.querySelector('.mbti-type') ||
                                  document.querySelector('.personality-type');
                data.mbti = mbtiElement ? mbtiElement.textContent.trim() : '';

                // Try to extract description
                const descElement = document.querySelector('.profile-description') ||
                                  document.querySelector('.bio') ||
                                  document.querySelector('.description');
                data.description = descElement ? descElement.textContent.trim() : '';

                // Extract any additional metadata
                data.url = window.location.href;
                data.scrapedAt = new Date().toISOString();

                return data;
            });

            console.log(`Successfully scraped profile: ${profileData.name || 'Unknown'}`);
            return profileData;

        } catch (error) {
            console.error(`Error scraping profile ${profileUrl}:`, error);
            throw error;
        }
    }

    async scrapeSearch(query, maxResults = 50) {
        if (!this.page) {
            await this.init();
        }

        try {
            console.log(`Searching for: ${query}`);
            
            const searchUrl = `${this.baseUrl}/search?q=${encodeURIComponent(query)}`;
            await this.page.goto(searchUrl, { waitUntil: 'networkidle' });
            await this.page.waitForTimeout(this.delay);

            // Extract search results
            const results = await this.page.evaluate((maxResults) => {
                const resultElements = document.querySelectorAll('.search-result, .profile-card, .result-item');
                const results = [];

                for (let i = 0; i < Math.min(resultElements.length, maxResults); i++) {
                    const element = resultElements[i];
                    const result = {};

                    // Extract profile link
                    const linkElement = element.querySelector('a[href*="/profile/"]') || element.querySelector('a');
                    if (linkElement) {
                        result.url = linkElement.href;
                        result.name = linkElement.textContent.trim();
                    }

                    // Extract type information if available
                    const typeElement = element.querySelector('.type, .mbti, .personality-type');
                    if (typeElement) {
                        result.type = typeElement.textContent.trim();
                    }

                    if (result.url) {
                        results.push(result);
                    }
                }

                return results;
            }, maxResults);

            console.log(`Found ${results.length} search results`);
            return results;

        } catch (error) {
            console.error(`Error searching for ${query}:`, error);
            throw error;
        }
    }

    async scrapeCategory(categoryUrl, maxProfiles = 100) {
        if (!this.page) {
            await this.init();
        }

        try {
            console.log(`Scraping category: ${categoryUrl}`);
            
            await this.page.goto(categoryUrl, { waitUntil: 'networkidle' });
            await this.page.waitForTimeout(this.delay);

            const profiles = [];
            let currentPage = 1;

            while (profiles.length < maxProfiles && currentPage <= this.maxPages) {
                console.log(`Scraping page ${currentPage}...`);

                // Extract profiles from current page
                const pageProfiles = await this.page.evaluate(() => {
                    const profileElements = document.querySelectorAll('.profile-card, .character-card, .profile-item');
                    const profiles = [];

                    profileElements.forEach(element => {
                        const profile = {};

                        // Extract profile link and name
                        const linkElement = element.querySelector('a[href*="/profile/"]') || element.querySelector('a');
                        if (linkElement) {
                            profile.url = linkElement.href;
                            profile.name = linkElement.textContent.trim();
                        }

                        // Extract type information
                        const typeElement = element.querySelector('.type, .mbti');
                        if (typeElement) {
                            profile.mbti = typeElement.textContent.trim();
                        }

                        // Extract image if available
                        const imgElement = element.querySelector('img');
                        if (imgElement) {
                            profile.image = imgElement.src;
                        }

                        if (profile.url && profile.name) {
                            profiles.push(profile);
                        }
                    });

                    return profiles;
                });

                profiles.push(...pageProfiles);
                console.log(`Found ${pageProfiles.length} profiles on page ${currentPage}`);

                // Try to navigate to next page
                const nextButton = await this.page.$('a[aria-label="Next"], .next-page, .pagination-next');
                if (!nextButton || profiles.length >= maxProfiles) {
                    break;
                }

                await nextButton.click();
                await this.page.waitForTimeout(this.delay);
                currentPage++;
            }

            console.log(`Total profiles found: ${profiles.length}`);
            return profiles.slice(0, maxProfiles);

        } catch (error) {
            console.error(`Error scraping category ${categoryUrl}:`, error);
            throw error;
        }
    }

    async fullScrape() {
        try {
            console.log('Starting full scrape of Personality Database...');
            
            if (!this.page) {
                await this.init();
            }

            const allProfiles = [];
            
            // Start from the main page and discover categories
            await this.page.goto(this.baseUrl, { waitUntil: 'networkidle' });
            await this.page.waitForTimeout(this.delay);

            // Extract category links
            const categories = await this.page.evaluate(() => {
                const categoryLinks = document.querySelectorAll('a[href*="/category/"], a[href*="/characters/"]');
                const categories = [];
                
                categoryLinks.forEach(link => {
                    if (link.href && link.textContent.trim()) {
                        categories.push({
                            name: link.textContent.trim(),
                            url: link.href
                        });
                    }
                });
                
                return categories.slice(0, 20); // Limit to first 20 categories
            });

            console.log(`Found ${categories.length} categories to scrape`);

            // Scrape each category
            for (let i = 0; i < categories.length; i++) {
                const category = categories[i];
                console.log(`Scraping category ${i + 1}/${categories.length}: ${category.name}`);
                
                try {
                    const categoryProfiles = await this.scrapeCategory(category.url, 50);
                    categoryProfiles.forEach(profile => {
                        profile.category = category.name;
                    });
                    allProfiles.push(...categoryProfiles);
                    
                    console.log(`Category completed. Total profiles so far: ${allProfiles.length}`);
                } catch (error) {
                    console.error(`Error scraping category ${category.name}:`, error);
                }

                // Add delay between categories
                await new Promise(resolve => setTimeout(resolve, this.delay * 2));
            }

            // Save results
            await this.saveProfiles(allProfiles);
            
            console.log(`Full scrape completed. Total profiles: ${allProfiles.length}`);
            return allProfiles;

        } catch (error) {
            console.error('Error during full scrape:', error);
            throw error;
        }
    }

    async saveProfiles(profiles) {
        try {
            const filename = `pdb_scraped_${Date.now()}.json`;
            const filepath = path.join(this.outputDir, filename);
            
            await fs.mkdir(this.outputDir, { recursive: true });
            await fs.writeFile(filepath, JSON.stringify(profiles, null, 2));
            
            console.log(`Saved ${profiles.length} profiles to ${filepath}`);
        } catch (error) {
            console.error('Error saving profiles:', error);
        }
    }

    async close() {
        if (this.browser) {
            await this.browser.close();
            this.browser = null;
            this.page = null;
            console.log('Browser closed');
        }
    }
}

module.exports = PlaywrightScraper;