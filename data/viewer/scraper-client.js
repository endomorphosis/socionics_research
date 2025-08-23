// Scraper client for invoking personality database scraping

export class ScraperClient {
    constructor() {
        this.isRunning = false;
        this.currentProcess = null;
        this.logs = [];
        this.eventSource = null;
    }

    // Start scraping process
    async startScraping(config = {}) {
        if (this.isRunning) {
            throw new Error('Scraper is already running');
        }

        try {
            this.isRunning = true;
            this.updateStatus('running');
            this.addLog('Starting scraping process...');

            const response = await fetch('/api/scraper/start', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    mode: config.mode || 'full',
                    maxPages: config.maxPages || 10,
                    delay: config.delay || 1000,
                    browser: config.browser || 'playwright',
                    url: config.url || 'https://www.personality-database.com'
                })
            });

            if (!response.ok) {
                throw new Error(`Failed to start scraper: ${response.status}`);
            }

            const result = await response.json();
            this.currentProcess = result.processId;
            
            // Start listening for progress updates
            this.startProgressListener();
            
            this.addLog(`Scraper started with process ID: ${this.currentProcess}`);
            return result;

        } catch (error) {
            this.isRunning = false;
            this.updateStatus('error');
            this.addLog(`Error starting scraper: ${error.message}`);
            throw error;
        }
    }

    // Stop scraping process
    async stopScraping() {
        if (!this.isRunning || !this.currentProcess) {
            return;
        }

        try {
            const response = await fetch('/api/scraper/stop', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    processId: this.currentProcess
                })
            });

            if (!response.ok) {
                throw new Error(`Failed to stop scraper: ${response.status}`);
            }

            this.addLog('Scraper stopped by user');
            this.cleanup();

        } catch (error) {
            this.addLog(`Error stopping scraper: ${error.message}`);
            throw error;
        }
    }

    // Scrape a specific profile
    async scrapeProfile(url) {
        try {
            this.addLog(`Scraping specific profile: ${url}`);

            const response = await fetch('/api/scraper/profile', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    url: url,
                    browser: document.getElementById('scraper-browser').value || 'playwright'
                })
            });

            if (!response.ok) {
                throw new Error(`Failed to scrape profile: ${response.status}`);
            }

            const result = await response.json();
            this.addLog(`Successfully scraped profile: ${result.name || 'Unknown'}`);
            return result;

        } catch (error) {
            this.addLog(`Error scraping profile: ${error.message}`);
            throw error;
        }
    }

    // Get scraper status
    async getStatus() {
        if (!this.currentProcess) {
            return { status: 'idle' };
        }

        try {
            const response = await fetch(`/api/scraper/status/${this.currentProcess}`);
            if (!response.ok) {
                throw new Error(`Failed to get status: ${response.status}`);
            }

            return await response.json();
        } catch (error) {
            console.error('Error getting scraper status:', error);
            return { status: 'error', error: error.message };
        }
    }

    // Start listening for progress updates
    startProgressListener() {
        if (!this.currentProcess) return;

        this.eventSource = new EventSource(`/api/scraper/progress/${this.currentProcess}`);
        
        this.eventSource.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                this.handleProgressUpdate(data);
            } catch (error) {
                console.error('Error parsing progress update:', error);
            }
        };

        this.eventSource.onerror = (error) => {
            console.error('Progress listener error:', error);
            this.eventSource.close();
            this.eventSource = null;
        };
    }

    // Handle progress updates
    handleProgressUpdate(data) {
        if (data.type === 'progress') {
            this.updateProgress(data.current, data.total);
            this.addLog(`Progress: ${data.current}/${data.total} - ${data.message || ''}`);
        } else if (data.type === 'log') {
            this.addLog(data.message);
        } else if (data.type === 'complete') {
            this.addLog(`Scraping completed: ${data.message || ''}`);
            this.isRunning = false;
            this.updateStatus('idle');
            this.cleanup();
        } else if (data.type === 'error') {
            this.addLog(`Error: ${data.error}`);
            this.isRunning = false;
            this.updateStatus('error');
            this.cleanup();
        }
    }

    // Update progress bar
    updateProgress(current, total) {
        const percentage = total > 0 ? (current / total) * 100 : 0;
        const progressFill = document.getElementById('progress-fill');
        const progressText = document.getElementById('progress-text');
        
        if (progressFill) {
            progressFill.style.width = `${percentage}%`;
        }
        
        if (progressText) {
            progressText.textContent = `${current}/${total} (${percentage.toFixed(1)}%)`;
        }
    }

    // Update scraper status
    updateStatus(status) {
        const statusElement = document.getElementById('scraper-status');
        const stopButton = document.getElementById('stop-scraper-btn');
        const startButtons = document.querySelectorAll('#start-full-scrape-btn, #start-incremental-scrape-btn');
        
        if (statusElement) {
            statusElement.textContent = status.charAt(0).toUpperCase() + status.slice(1);
            statusElement.className = `status-${status}`;
        }

        if (stopButton && startButtons) {
            if (status === 'running') {
                stopButton.disabled = false;
                startButtons.forEach(btn => btn.disabled = true);
            } else {
                stopButton.disabled = true;
                startButtons.forEach(btn => btn.disabled = false);
            }
        }
    }

    // Add log message
    addLog(message) {
        const timestamp = new Date().toLocaleTimeString();
        const logMessage = `[${timestamp}] ${message}`;
        
        this.logs.push(logMessage);
        
        // Limit logs to last 100 messages
        if (this.logs.length > 100) {
            this.logs.shift();
        }

        // Update logs display
        const logsElement = document.getElementById('scraper-logs');
        if (logsElement) {
            logsElement.textContent = this.logs.join('\n');
            logsElement.scrollTop = logsElement.scrollHeight;
        }
    }

    // Cleanup after scraping
    cleanup() {
        this.currentProcess = null;
        
        if (this.eventSource) {
            this.eventSource.close();
            this.eventSource = null;
        }
        
        // Reset progress
        this.updateProgress(0, 0);
    }

    // Get available scrapers
    async getAvailableScrapers() {
        try {
            const response = await fetch('/api/scraper/available');
            if (!response.ok) {
                throw new Error(`Failed to get available scrapers: ${response.status}`);
            }

            return await response.json();
        } catch (error) {
            console.error('Error getting available scrapers:', error);
            return { playwright: false, selenium: false };
        }
    }
}

// Export default instance
export const scraperClient = new ScraperClient();