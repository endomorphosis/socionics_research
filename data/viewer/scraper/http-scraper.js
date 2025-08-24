const https = require('https');
const fs = require('fs').promises;
const path = require('path');

class HttpScraper {
    constructor(options = {}) {
        this.baseUrl = options.baseUrl || 'https://www.personality-database.com';
        this.delay = options.delay || 2000;
        this.outputDir = options.outputDir || path.resolve(__dirname, '../../bot_store');
        this.userAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36';
    }

    async init() {
        console.log('HTTP scraper initialized');
        return true;
    }

    async makeRequest(url, retries = 3) {
        return new Promise((resolve, reject) => {
            const request = https.get(url, {
                headers: {
                    'User-Agent': this.userAgent,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
            }, (response) => {
                let data = '';
                
                response.on('data', (chunk) => {
                    data += chunk;
                });
                
                response.on('end', () => {
                    if (response.statusCode === 200) {
                        resolve(data);
                    } else if (response.statusCode === 429 && retries > 0) {
                        // Rate limited, retry after delay
                        console.log(`Rate limited, retrying in ${this.delay * 2}ms...`);
                        setTimeout(() => {
                            this.makeRequest(url, retries - 1).then(resolve).catch(reject);
                        }, this.delay * 2);
                    } else {
                        reject(new Error(`HTTP ${response.statusCode}: ${response.statusMessage}`));
                    }
                });
            });
            
            request.on('error', (error) => {
                if (retries > 0) {
                    console.log(`Request failed, retrying: ${error.message}`);
                    setTimeout(() => {
                        this.makeRequest(url, retries - 1).then(resolve).catch(reject);
                    }, this.delay);
                } else {
                    reject(error);
                }
            });
            
            request.setTimeout(30000, () => {
                request.destroy();
                reject(new Error('Request timeout'));
            });
        });
    }

    extractProfileData(html, url) {
        const data = {
            url: url,
            scrapedAt: new Date().toISOString()
        };

        // Extract name - try multiple patterns
        let nameMatch = html.match(/<h1[^>]*>(.*?)<\/h1>/i);
        if (!nameMatch) {
            nameMatch = html.match(/<title[^>]*>([^|]+)/i);
        }
        if (!nameMatch) {
            nameMatch = html.match(/["']profile[_-]?name["'][^>]*>([^<]+)/i);
        }
        data.name = nameMatch ? nameMatch[1].trim().replace(/&[^;]+;/g, '') : '';

        // Extract MBTI type - look for common patterns
        const mbtiPatterns = [
            /MBTI[:\s]*([A-Z]{4})/i,
            /Myers[- ]Briggs[:\s]*([A-Z]{4})/i,
            /Personality Type[:\s]*([A-Z]{4})/i,
            /Type[:\s]*([A-Z]{4})/i,
            /\b([IE][NS][FT][JP])\b/g
        ];
        
        for (const pattern of mbtiPatterns) {
            const match = html.match(pattern);
            if (match && match[1] && /^[IE][NS][FT][JP]$/.test(match[1])) {
                data.mbti = match[1];
                break;
            }
        }

        // Extract Socionics type
        const socionicsMatch = html.match(/Socionics[:\s]*([A-Z]{3}|[A-Z]{2}[a-z])/i);
        if (socionicsMatch) {
            data.socionics = socionicsMatch[1];
        }

        // Extract description/bio
        const descPatterns = [
            /<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i,
            /<div[^>]*class="[^"]*bio[^"]*"[^>]*>(.*?)<\/div>/is,
            /<div[^>]*class="[^"]*description[^"]*"[^>]*>(.*?)<\/div>/is,
            /<p[^>]*class="[^"]*description[^"]*"[^>]*>(.*?)<\/p>/is
        ];
        
        for (const pattern of descPatterns) {
            const match = html.match(pattern);
            if (match && match[1]) {
                data.description = match[1].replace(/<[^>]*>/g, '').trim();
                if (data.description.length > 10) break;
            }
        }

        // Extract category/subcategory info
        const categoryMatch = html.match(/category[\/:]([^\/\s'"]+)/i);
        if (categoryMatch) {
            data.category = categoryMatch[1].replace(/-/g, ' ').trim();
        }

        return data;
    }

    async scrapeProfile(profileUrl) {
        try {
            console.log(`Scraping profile: ${profileUrl}`);
            
            const html = await this.makeRequest(profileUrl);
            await new Promise(resolve => setTimeout(resolve, this.delay));
            
            const profileData = this.extractProfileData(html, profileUrl);
            
            console.log(`Successfully scraped profile: ${profileData.name || 'Unknown'}`);
            return profileData;

        } catch (error) {
            console.error(`Error scraping profile ${profileUrl}:`, error.message);
            return {
                url: profileUrl,
                error: error.message,
                scrapedAt: new Date().toISOString()
            };
        }
    }

    async scrapeSearch(query, maxResults = 50) {
        try {
            console.log(`Searching for: ${query}`);
            
            const searchUrl = `${this.baseUrl}/search?q=${encodeURIComponent(query)}`;
            const html = await this.makeRequest(searchUrl);
            await new Promise(resolve => setTimeout(resolve, this.delay));
            
            const results = [];
            
            // Extract profile links from search results
            const linkMatches = html.match(/href=["']([^"']*\/profile\/[^"']*)["']/g) || [];
            const nameMatches = html.match(/>([^<]{3,50})<\/a>/g) || [];
            
            for (let i = 0; i < Math.min(linkMatches.length, maxResults, nameMatches.length); i++) {
                const urlMatch = linkMatches[i].match(/href=["']([^"']*)["']/);
                const nameMatch = nameMatches[i].match(/>([^<]+)<\/a>/);
                
                if (urlMatch && nameMatch) {
                    let url = urlMatch[1];
                    if (url.startsWith('/')) {
                        url = this.baseUrl + url;
                    }
                    
                    results.push({
                        name: nameMatch[1].trim(),
                        url: url
                    });
                }
            }

            console.log(`Found ${results.length} search results`);
            return results;

        } catch (error) {
            console.error(`Error searching for ${query}:`, error.message);
            return [];
        }
    }

    async scrapeCategory(categoryUrl, maxProfiles = 100) {
        try {
            console.log(`Scraping category: ${categoryUrl}`);
            
            const html = await this.makeRequest(categoryUrl);
            await new Promise(resolve => setTimeout(resolve, this.delay));
            
            const profiles = [];
            
            // Extract profile links from category page
            const linkMatches = html.match(/href=["']([^"']*\/profile\/[^"']*)["']/g) || [];
            
            for (let i = 0; i < Math.min(linkMatches.length, maxProfiles); i++) {
                const match = linkMatches[i].match(/href=["']([^"']*)["']/);
                if (match) {
                    let url = match[1];
                    if (url.startsWith('/')) {
                        url = this.baseUrl + url;
                    }
                    
                    profiles.push({
                        url: url,
                        category: categoryUrl
                    });
                }
            }

            console.log(`Found ${profiles.length} profiles in category`);
            return profiles;

        } catch (error) {
            console.error(`Error scraping category ${categoryUrl}:`, error.message);
            return [];
        }
    }

    async fullScrape() {
        try {
            console.log('Starting full scrape of Personality Database...');
            
            const allProfiles = [];
            
            // Try to discover some common category URLs
            const commonCategories = [
                `${this.baseUrl}/category/fictional-characters`,
                `${this.baseUrl}/category/anime-characters`, 
                `${this.baseUrl}/category/movie-characters`,
                `${this.baseUrl}/category/tv-show-characters`,
                `${this.baseUrl}/category/book-characters`,
                `${this.baseUrl}/category/video-game-characters`,
                `${this.baseUrl}/category/celebrities`,
                `${this.baseUrl}/category/politicians`,
                `${this.baseUrl}/category/musicians`,
                `${this.baseUrl}/category/actors`
            ];

            console.log(`Attempting to scrape ${commonCategories.length} common categories`);

            // Scrape each category
            for (let i = 0; i < commonCategories.length; i++) {
                const categoryUrl = commonCategories[i];
                console.log(`Scraping category ${i + 1}/${commonCategories.length}: ${categoryUrl}`);
                
                try {
                    const categoryProfiles = await this.scrapeCategory(categoryUrl, 50);
                    allProfiles.push(...categoryProfiles);
                    
                    console.log(`Category completed. Total profiles so far: ${allProfiles.length}`);
                } catch (error) {
                    console.error(`Error scraping category ${categoryUrl}:`, error.message);
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
        console.log('HTTP scraper closed');
    }
}

module.exports = HttpScraper;