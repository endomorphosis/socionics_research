const { spawn } = require('child_process');
const fs = require('fs').promises;
const path = require('path');

class PythonBotScraper {
    constructor(options = {}) {
        this.baseUrl = options.baseUrl || 'https://api.personality-database.com/api/v1';
        this.pythonPath = options.pythonPath || 'python';
        this.workingDir = options.workingDir || path.resolve(__dirname, '../../../');
        this.outputDir = options.outputDir || path.resolve(__dirname, '../../bot_store');
        this.concurrency = options.concurrency || 3;
        this.ratePerMinute = options.ratePerMinute || 90;
        this.timeout = options.timeout || 30;
    }

    async init() {
        console.log('Python Bot Scraper initialized');
        // Set environment variables for the Python bot
        process.env.PYTHONPATH = path.join(this.workingDir, 'bot/src');
        process.env.PDB_CONCURRENCY = this.concurrency.toString();
        process.env.PDB_RPM = this.ratePerMinute.toString();
        process.env.PDB_TIMEOUT_S = this.timeout.toString();
        process.env.PDB_CACHE = '1';
        process.env.PDB_API_BASE_URL = this.baseUrl;
        return true;
    }

    async runPythonCommand(args, options = {}) {
        return new Promise((resolve, reject) => {
            const { showOutput = true, timeout = 120000 } = options;
            
            const command = ['-m', 'bot.pdb_cli', ...args];
            console.log(`Running: python ${command.join(' ')}`);
            
            const child = spawn(this.pythonPath, command, {
                cwd: this.workingDir,
                env: process.env,
                stdio: showOutput ? 'inherit' : 'pipe'
            });

            let stdout = '';
            let stderr = '';

            if (!showOutput) {
                child.stdout?.on('data', (data) => {
                    stdout += data.toString();
                });

                child.stderr?.on('data', (data) => {
                    stderr += data.toString();
                });
            }

            const timer = setTimeout(() => {
                child.kill('SIGTERM');
                reject(new Error(`Command timed out after ${timeout}ms`));
            }, timeout);

            child.on('close', (code) => {
                clearTimeout(timer);
                if (code === 0) {
                    resolve({ code, stdout, stderr });
                } else {
                    reject(new Error(`Command failed with code ${code}: ${stderr}`));
                }
            });

            child.on('error', (error) => {
                clearTimeout(timer);
                reject(error);
            });
        });
    }

    async searchProfiles(query, options = {}) {
        const {
            limit = 20,
            pages = 3,
            untilEmpty = true,
            filterCharacters = true,
            autoEmbed = false,
            autoIndex = false
        } = options;

        try {
            console.log(`Searching for profiles: ${query}`);
            
            const args = [
                'search-top',
                '--query', query,
                '--limit', limit.toString(),
                '--pages', pages.toString(),
                '--only-profiles',
                '--verbose'
            ];

            if (untilEmpty) args.push('--until-empty');
            if (filterCharacters) args.push('--filter-characters');
            if (autoEmbed) args.push('--auto-embed');
            if (autoIndex) args.push('--auto-index');

            await this.runPythonCommand(args, { timeout: 180000 });
            
            console.log(`Search completed for: ${query}`);
            return { success: true, query };

        } catch (error) {
            console.error(`Error searching for ${query}:`, error.message);
            return { success: false, query, error: error.message };
        }
    }

    async searchMultipleQueries(queries, options = {}) {
        const {
            limit = 20,
            pages = 3,
            untilEmpty = true,
            filterCharacters = true,
            autoEmbed = false,
            autoIndex = false
        } = options;

        try {
            console.log(`Searching for multiple queries: ${queries.join(', ')}`);
            
            const args = [
                'search-keywords',
                '--queries', queries.join(','),
                '--limit', limit.toString(),
                '--pages', pages.toString(),
                '--only-profiles',
                '--verbose'
            ];

            if (untilEmpty) args.push('--until-empty');
            if (filterCharacters) args.push('--filter-characters');
            if (autoEmbed) args.push('--auto-embed');
            if (autoIndex) args.push('--auto-index');

            await this.runPythonCommand(args, { timeout: 300000 });
            
            console.log(`Multi-search completed for ${queries.length} queries`);
            return { success: true, queries };

        } catch (error) {
            console.error(`Error in multi-search:`, error.message);
            return { success: false, queries, error: error.message };
        }
    }

    async fullScrape(options = {}) {
        try {
            console.log('Starting comprehensive personality database scrape...');
            
            // Define comprehensive search terms for better coverage
            const characterQueries = [
                'batman', 'superman', 'spider-man', 'joker', 'wolverine',
                'naruto', 'goku', 'luffy', 'sasuke', 'vegeta',
                'harry potter', 'hermione', 'ron weasley', 'dumbledore', 'voldemort',
                'frodo', 'gandalf', 'aragorn', 'legolas', 'gimli',
                'luke skywalker', 'darth vader', 'han solo', 'leia', 'obi-wan',
                'tony stark', 'captain america', 'thor', 'hulk', 'black widow',
                'sherlock holmes', 'watson', 'moriarty',
                'tyrion lannister', 'jon snow', 'daenerys', 'arya stark',
                'walter white', 'jesse pinkman', 'saul goodman',
                'rick sanchez', 'morty smith', 'homer simpson', 'bart simpson'
            ];

            const franchiseQueries = [
                'marvel', 'dc comics', 'star wars', 'harry potter', 'lord of the rings',
                'game of thrones', 'breaking bad', 'rick and morty', 'the simpsons',
                'naruto', 'dragon ball', 'one piece', 'attack on titan',
                'friends', 'the office', 'stranger things', 'squid game',
                'anime', 'disney', 'pixar', 'studio ghibli'
            ];

            const personalityQueries = [
                'INTJ', 'ENTP', 'INFP', 'ENFP', 'ISTJ', 'ESTJ', 'ISFJ', 'ESFJ',
                'ISTP', 'ESTP', 'ISFP', 'ESFP', 'INTP', 'ENFJ', 'INFJ', 'ENTJ'
            ];

            // Start with character searches
            console.log('Phase 1: Searching for popular characters...');
            await this.searchMultipleQueries(characterQueries, {
                ...options,
                pages: 3,
                limit: 30,
                untilEmpty: true
            });

            console.log('Phase 2: Searching for franchises...');
            await this.searchMultipleQueries(franchiseQueries, {
                ...options,
                pages: 5,
                limit: 50,
                untilEmpty: true
            });

            console.log('Phase 3: Searching by personality types...');
            await this.searchMultipleQueries(personalityQueries, {
                ...options,
                pages: 10,
                limit: 100,
                untilEmpty: true
            });

            // Run comprehensive scan-all command if available
            console.log('Phase 4: Running comprehensive scan-all...');
            try {
                const scanArgs = [
                    'scan-all',
                    '--max-iterations', '0',
                    '--initial-frontier-size', '1000',
                    '--search-names',
                    '--limit', '20',
                    '--pages', '3',
                    '--until-empty',
                    '--sweep-queries', 'a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z',
                    '--sweep-pages', '20',
                    '--sweep-until-empty',
                    '--sweep-into-frontier',
                    '--max-no-progress-pages', '3',
                    '--auto-embed',
                    '--auto-index'
                ];

                await this.runPythonCommand(scanArgs, { timeout: 1800000 }); // 30 minutes
                console.log('Comprehensive scan completed');
            } catch (error) {
                console.log('Comprehensive scan not available or failed, continuing with basic search');
            }

            console.log('Full scrape completed!');
            return { success: true, phases: 4 };

        } catch (error) {
            console.error('Error during full scrape:', error.message);
            return { success: false, error: error.message };
        }
    }

    async getScrapingStats() {
        try {
            const args = ['coverage', '--sample', '10'];
            const result = await this.runPythonCommand(args, { showOutput: false });
            return { success: true, output: result.stdout };
        } catch (error) {
            console.error('Error getting stats:', error.message);
            return { success: false, error: error.message };
        }
    }

    async embedAndIndex() {
        try {
            console.log('Embedding profiles and building index...');
            const args = ['embed'];
            await this.runPythonCommand(args, { timeout: 600000 });
            
            const indexArgs = ['index', '--output', path.join(this.outputDir, 'pdb_faiss.index')];
            await this.runPythonCommand(indexArgs, { timeout: 300000 });
            
            console.log('Embedding and indexing completed');
            return { success: true };
        } catch (error) {
            console.error('Error embedding/indexing:', error.message);
            return { success: false, error: error.message };
        }
    }

    async scrapeProfile(profileUrl) {
        // For individual profile scraping, we would need the profile ID
        // This is more complex with the API-based approach
        console.log(`Profile scraping not directly supported - use search instead for: ${profileUrl}`);
        return { success: false, message: 'Use search functionality instead' };
    }

    async scrapeSearch(query, maxResults = 50) {
        const result = await this.searchProfiles(query, {
            limit: Math.min(maxResults, 100),
            pages: Math.ceil(maxResults / 20),
            untilEmpty: false
        });
        return result;
    }

    async scrapeCategory(categoryUrl, maxProfiles = 100) {
        // Extract category name from URL and use as search query
        const categoryName = categoryUrl.split('/').pop()?.replace(/-/g, ' ') || 'category';
        return this.searchProfiles(categoryName, {
            limit: Math.min(maxProfiles, 100),
            pages: Math.ceil(maxProfiles / 20)
        });
    }

    async close() {
        console.log('Python Bot Scraper closed');
    }
}

module.exports = PythonBotScraper;