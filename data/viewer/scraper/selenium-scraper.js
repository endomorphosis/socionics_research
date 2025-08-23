const { Builder, By, until } = require('selenium-webdriver');
const chrome = require('selenium-webdriver/chrome');
const fs = require('fs').promises;
const path = require('path');

class SeleniumScraper {
    constructor(options = {}) {
        this.driver = null;
        this.baseUrl = options.baseUrl || 'https://www.personality-database.com';
        this.delay = options.delay || 1000;
        this.maxPages = options.maxPages || 10;
        this.outputDir = options.outputDir || path.resolve(__dirname, '../../bot_store');
    }

    async init() {
        try {
            console.log('Initializing Selenium WebDriver...');
            
            const chromeOptions = new chrome.Options();
            chromeOptions.addArguments('--headless');
            chromeOptions.addArguments('--no-sandbox');
            chromeOptions.addArguments('--disable-dev-shm-usage');
            chromeOptions.addArguments('--disable-gpu');
            chromeOptions.addArguments('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36');

            this.driver = await new Builder()
                .forBrowser('chrome')
                .setChromeOptions(chromeOptions)
                .build();

            console.log('Selenium WebDriver initialized successfully');
            return true;
        } catch (error) {
            console.error('Failed to initialize Selenium:', error);
            throw error;
        }
    }

    async scrapeProfile(profileUrl) {
        if (!this.driver) {
            await this.init();
        }

        try {
            console.log(`Scraping profile: ${profileUrl}`);
            
            await this.driver.get(profileUrl);
            await this.driver.sleep(this.delay);

            const profileData = {};

            // Try to extract name
            try {
                const nameElement = await this.driver.findElement(By.css('h1'));
                profileData.name = await nameElement.getText();
            } catch (error) {
                try {
                    const nameElement = await this.driver.findElement(By.css('.profile-name'));
                    profileData.name = await nameElement.getText();
                } catch (e) {
                    profileData.name = '';
                }
            }

            // Try to extract MBTI type
            try {
                const mbtiElement = await this.driver.findElement(By.css('[data-testid="mbti"], .mbti-type, .personality-type'));
                profileData.mbti = await mbtiElement.getText();
            } catch (error) {
                profileData.mbti = '';
            }

            // Try to extract description
            try {
                const descElement = await this.driver.findElement(By.css('.profile-description, .bio, .description'));
                profileData.description = await descElement.getText();
            } catch (error) {
                profileData.description = '';
            }

            // Add metadata
            profileData.url = profileUrl;
            profileData.scrapedAt = new Date().toISOString();

            console.log(`Successfully scraped profile: ${profileData.name || 'Unknown'}`);
            return profileData;

        } catch (error) {
            console.error(`Error scraping profile ${profileUrl}:`, error);
            throw error;
        }
    }

    async scrapeSearch(query, maxResults = 50) {
        if (!this.driver) {
            await this.init();
        }

        try {
            console.log(`Searching for: ${query}`);
            
            const searchUrl = `${this.baseUrl}/search?q=${encodeURIComponent(query)}`;
            await this.driver.get(searchUrl);
            await this.driver.sleep(this.delay);

            const results = [];
            
            try {
                const resultElements = await this.driver.findElements(By.css('.search-result, .profile-card, .result-item'));
                
                for (let i = 0; i < Math.min(resultElements.length, maxResults); i++) {
                    const element = resultElements[i];
                    const result = {};

                    try {
                        // Extract profile link
                        const linkElement = await element.findElement(By.css('a[href*="/profile/"], a'));
                        result.url = await linkElement.getAttribute('href');
                        result.name = await linkElement.getText();

                        // Extract type information if available
                        try {
                            const typeElement = await element.findElement(By.css('.type, .mbti, .personality-type'));
                            result.type = await typeElement.getText();
                        } catch (e) {
                            // Type not found, continue
                        }

                        if (result.url && result.name) {
                            results.push(result);
                        }
                    } catch (e) {
                        // Skip this result if we can't extract basic info
                        continue;
                    }
                }
            } catch (error) {
                console.warn('No search results found or error extracting results');
            }

            console.log(`Found ${results.length} search results`);
            return results;

        } catch (error) {
            console.error(`Error searching for ${query}:`, error);
            throw error;
        }
    }

    async scrapeCategory(categoryUrl, maxProfiles = 100) {
        if (!this.driver) {
            await this.init();
        }

        try {
            console.log(`Scraping category: ${categoryUrl}`);
            
            await this.driver.get(categoryUrl);
            await this.driver.sleep(this.delay);

            const profiles = [];
            let currentPage = 1;

            while (profiles.length < maxProfiles && currentPage <= this.maxPages) {
                console.log(`Scraping page ${currentPage}...`);

                try {
                    const profileElements = await this.driver.findElements(By.css('.profile-card, .character-card, .profile-item'));
                    
                    for (const element of profileElements) {
                        if (profiles.length >= maxProfiles) break;

                        const profile = {};

                        try {
                            // Extract profile link and name
                            const linkElement = await element.findElement(By.css('a[href*="/profile/"], a'));
                            profile.url = await linkElement.getAttribute('href');
                            profile.name = await linkElement.getText();

                            // Extract type information
                            try {
                                const typeElement = await element.findElement(By.css('.type, .mbti'));
                                profile.mbti = await typeElement.getText();
                            } catch (e) {
                                // Type not found
                            }

                            // Extract image if available
                            try {
                                const imgElement = await element.findElement(By.css('img'));
                                profile.image = await imgElement.getAttribute('src');
                            } catch (e) {
                                // Image not found
                            }

                            if (profile.url && profile.name) {
                                profiles.push(profile);
                            }
                        } catch (e) {
                            // Skip this profile if we can't extract basic info
                            continue;
                        }
                    }
                } catch (error) {
                    console.warn(`Error extracting profiles from page ${currentPage}:`, error);
                }

                console.log(`Found ${profiles.length} profiles so far`);

                // Try to navigate to next page
                try {
                    const nextButton = await this.driver.findElement(By.css('a[aria-label="Next"], .next-page, .pagination-next'));
                    await nextButton.click();
                    await this.driver.sleep(this.delay);
                    currentPage++;
                } catch (error) {
                    console.log('No next page found or error clicking next button');
                    break;
                }
            }

            console.log(`Total profiles found in category: ${profiles.length}`);
            return profiles.slice(0, maxProfiles);

        } catch (error) {
            console.error(`Error scraping category ${categoryUrl}:`, error);
            throw error;
        }
    }

    async fullScrape() {
        try {
            console.log('Starting full scrape of Personality Database with Selenium...');
            
            if (!this.driver) {
                await this.init();
            }

            const allProfiles = [];
            
            // Start from the main page and discover categories
            await this.driver.get(this.baseUrl);
            await this.driver.sleep(this.delay);

            const categories = [];
            
            try {
                const categoryElements = await this.driver.findElements(By.css('a[href*="/category/"], a[href*="/characters/"]'));
                
                for (let i = 0; i < Math.min(categoryElements.length, 20); i++) {
                    const element = categoryElements[i];
                    try {
                        const href = await element.getAttribute('href');
                        const text = await element.getText();
                        
                        if (href && text.trim()) {
                            categories.push({
                                name: text.trim(),
                                url: href
                            });
                        }
                    } catch (e) {
                        // Skip this category link
                        continue;
                    }
                }
            } catch (error) {
                console.warn('Error finding category links, will try some default categories');
                
                // Add some default categories as fallback
                categories.push(
                    { name: 'Fictional Characters', url: `${this.baseUrl}/category/fictional-characters` },
                    { name: 'Anime Characters', url: `${this.baseUrl}/category/anime` },
                    { name: 'Movie Characters', url: `${this.baseUrl}/category/movies` }
                );
            }

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
                await this.driver.sleep(this.delay * 2);
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
            const filename = `pdb_scraped_selenium_${Date.now()}.json`;
            const filepath = path.join(this.outputDir, filename);
            
            await fs.mkdir(this.outputDir, { recursive: true });
            await fs.writeFile(filepath, JSON.stringify(profiles, null, 2));
            
            console.log(`Saved ${profiles.length} profiles to ${filepath}`);
        } catch (error) {
            console.error('Error saving profiles:', error);
        }
    }

    async close() {
        if (this.driver) {
            await this.driver.quit();
            this.driver = null;
            console.log('WebDriver closed');
        }
    }
}

module.exports = SeleniumScraper;