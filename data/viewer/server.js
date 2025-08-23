const express = require('express');
const cors = require('cors');
const path = require('path');
const { spawn } = require('child_process');
const fs = require('fs').promises;

const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

// Store active scraper processes
const activeProcesses = new Map();
const processLogs = new Map();

// Serve the main dashboard
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// API Routes

// Start scraping process
app.post('/api/scraper/start', async (req, res) => {
    try {
        const { mode, maxPages, delay, browser, url } = req.body;
        
        // Generate process ID
        const processId = Date.now().toString();
        
        // Prepare command arguments
        const args = [];
        const botPath = path.resolve(__dirname, '../../bot');
        
        // Set up environment
        const env = { 
            ...process.env, 
            PYTHONPATH: path.join(botPath, 'src')
        };

        let command, commandArgs;

        if (mode === 'full') {
            // Use the existing PDB CLI for full scraping
            command = 'python';
            commandArgs = [
                '-m', 'bot.pdb_cli', 
                'scan-all',
                '--pages', maxPages.toString(),
                '--rpm', '60',
                '--concurrency', '3',
                '--auto-embed',
                '--auto-index'
            ];
            
            // Add headers if available
            const headersPath = path.resolve(__dirname, '../../.secrets/pdb_headers.json');
            try {
                await fs.access(headersPath);
                const headers = await fs.readFile(headersPath, 'utf8');
                commandArgs.push('--headers', headers);
            } catch (error) {
                console.warn('No headers file found, scraping may be limited');
            }
        } else if (mode === 'incremental') {
            // Incremental scraping
            command = 'python';
            commandArgs = [
                '-m', 'bot.pdb_cli',
                'follow-hot',
                '--pages', Math.min(maxPages, 5).toString(),
                '--auto-embed'
            ];
        }

        console.log(`Starting scraper: ${command} ${commandArgs.join(' ')}`);

        // Spawn the process
        const scraperProcess = spawn(command, commandArgs, {
            cwd: botPath,
            env: env,
            stdio: ['pipe', 'pipe', 'pipe']
        });

        // Store process info
        activeProcesses.set(processId, {
            process: scraperProcess,
            startTime: new Date(),
            config: { mode, maxPages, delay, browser, url },
            status: 'running',
            progress: { current: 0, total: 0 }
        });

        // Initialize logs
        processLogs.set(processId, []);

        // Handle process output
        scraperProcess.stdout.on('data', (data) => {
            const log = data.toString();
            addProcessLog(processId, log);
            console.log(`[${processId}] ${log}`);
        });

        scraperProcess.stderr.on('data', (data) => {
            const log = data.toString();
            addProcessLog(processId, `ERROR: ${log}`);
            console.error(`[${processId}] ${log}`);
        });

        scraperProcess.on('close', (code) => {
            const processInfo = activeProcesses.get(processId);
            if (processInfo) {
                processInfo.status = code === 0 ? 'completed' : 'failed';
                processInfo.endTime = new Date();
                addProcessLog(processId, `Process finished with code: ${code}`);
            }
            console.log(`[${processId}] Process finished with code: ${code}`);
        });

        scraperProcess.on('error', (error) => {
            const processInfo = activeProcesses.get(processId);
            if (processInfo) {
                processInfo.status = 'failed';
                processInfo.error = error.message;
            }
            addProcessLog(processId, `Process error: ${error.message}`);
            console.error(`[${processId}] Process error:`, error);
        });

        res.json({
            success: true,
            processId: processId,
            message: 'Scraper started successfully',
            config: { mode, maxPages, delay, browser, url }
        });

    } catch (error) {
        console.error('Error starting scraper:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Stop scraping process
app.post('/api/scraper/stop', (req, res) => {
    try {
        const { processId } = req.body;
        
        const processInfo = activeProcesses.get(processId);
        if (!processInfo) {
            return res.status(404).json({
                success: false,
                error: 'Process not found'
            });
        }

        // Kill the process
        processInfo.process.kill('SIGTERM');
        processInfo.status = 'stopped';
        addProcessLog(processId, 'Process stopped by user');

        res.json({
            success: true,
            message: 'Scraper stopped successfully'
        });

    } catch (error) {
        console.error('Error stopping scraper:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get scraper status
app.get('/api/scraper/status/:processId', (req, res) => {
    try {
        const { processId } = req.params;
        
        const processInfo = activeProcesses.get(processId);
        if (!processInfo) {
            return res.status(404).json({
                success: false,
                error: 'Process not found'
            });
        }

        res.json({
            success: true,
            processId: processId,
            status: processInfo.status,
            startTime: processInfo.startTime,
            endTime: processInfo.endTime,
            config: processInfo.config,
            progress: processInfo.progress,
            error: processInfo.error
        });

    } catch (error) {
        console.error('Error getting scraper status:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Server-Sent Events for progress updates
app.get('/api/scraper/progress/:processId', (req, res) => {
    const { processId } = req.params;
    
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*',
    });

    // Send initial status
    const processInfo = activeProcesses.get(processId);
    if (processInfo) {
        res.write(`data: ${JSON.stringify({
            type: 'status',
            status: processInfo.status,
            progress: processInfo.progress
        })}\n\n`);
    }

    // Set up periodic updates
    const interval = setInterval(() => {
        const processInfo = activeProcesses.get(processId);
        if (!processInfo) {
            res.write(`data: ${JSON.stringify({
                type: 'error',
                error: 'Process not found'
            })}\n\n`);
            clearInterval(interval);
            res.end();
            return;
        }

        // Send recent logs
        const logs = processLogs.get(processId) || [];
        const recentLogs = logs.slice(-5); // Last 5 log entries
        
        recentLogs.forEach(log => {
            res.write(`data: ${JSON.stringify({
                type: 'log',
                message: log.message,
                timestamp: log.timestamp
            })}\n\n`);
        });

        // Send status update
        res.write(`data: ${JSON.stringify({
            type: 'progress',
            status: processInfo.status,
            progress: processInfo.progress,
            current: processInfo.progress.current,
            total: processInfo.progress.total
        })}\n\n`);

        // End stream if process is complete
        if (processInfo.status === 'completed' || processInfo.status === 'failed') {
            res.write(`data: ${JSON.stringify({
                type: 'complete',
                status: processInfo.status,
                message: `Scraping ${processInfo.status}`
            })}\n\n`);
            clearInterval(interval);
            res.end();
        }
    }, 2000);

    // Clean up on client disconnect
    req.on('close', () => {
        clearInterval(interval);
        res.end();
    });
});

// Scrape specific profile
app.post('/api/scraper/profile', async (req, res) => {
    try {
        const { url, browser } = req.body;
        
        // For now, use a simple approach - this could be enhanced with actual profile scraping
        const botPath = path.resolve(__dirname, '../../bot');
        const command = 'python';
        const args = [
            '-m', 'bot.pdb_cli',
            'dump-profile',
            '--url', url
        ];

        const env = { 
            ...process.env, 
            PYTHONPATH: path.join(botPath, 'src')
        };

        const scraperProcess = spawn(command, args, {
            cwd: botPath,
            env: env,
            stdio: ['pipe', 'pipe', 'pipe']
        });

        let output = '';
        let error = '';

        scraperProcess.stdout.on('data', (data) => {
            output += data.toString();
        });

        scraperProcess.stderr.on('data', (data) => {
            error += data.toString();
        });

        scraperProcess.on('close', (code) => {
            if (code === 0) {
                res.json({
                    success: true,
                    message: 'Profile scraped successfully',
                    output: output,
                    url: url
                });
            } else {
                res.status(500).json({
                    success: false,
                    error: error || 'Unknown error occurred',
                    code: code
                });
            }
        });

    } catch (error) {
        console.error('Error scraping profile:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Check available scrapers
app.get('/api/scraper/available', async (req, res) => {
    try {
        // Check if playwright is available
        let playwrightAvailable = false;
        try {
            const { spawn } = require('child_process');
            const checkPlaywright = spawn('python', ['-c', 'import playwright; print("available")']);
            await new Promise((resolve, reject) => {
                checkPlaywright.on('close', (code) => {
                    playwrightAvailable = code === 0;
                    resolve();
                });
                checkPlaywright.on('error', reject);
            });
        } catch (error) {
            playwrightAvailable = false;
        }

        // Check if selenium is available
        let seleniumAvailable = false;
        try {
            const checkSelenium = spawn('python', ['-c', 'import selenium; print("available")']);
            await new Promise((resolve, reject) => {
                checkSelenium.on('close', (code) => {
                    seleniumAvailable = code === 0;
                    resolve();
                });
                checkSelenium.on('error', reject);
            });
        } catch (error) {
            seleniumAvailable = false;
        }

        res.json({
            playwright: playwrightAvailable,
            selenium: seleniumAvailable
        });

    } catch (error) {
        console.error('Error checking available scrapers:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Data export endpoint
app.get('/api/data/export', async (req, res) => {
    try {
        const { format = 'json' } = req.query;
        const dataPath = path.resolve(__dirname, '../bot_store');
        
        // For now, just return a simple response
        res.json({
            success: true,
            message: 'Export functionality coming soon',
            availableFormats: ['json', 'csv', 'parquet'],
            dataPath: dataPath
        });
        
    } catch (error) {
        console.error('Error exporting data:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Helper function to add logs to process
function addProcessLog(processId, message) {
    if (!processLogs.has(processId)) {
        processLogs.set(processId, []);
    }
    
    const logs = processLogs.get(processId);
    logs.push({
        timestamp: new Date().toISOString(),
        message: message.trim()
    });
    
    // Keep only last 100 log entries
    if (logs.length > 100) {
        logs.splice(0, logs.length - 100);
    }
}

// Clean up old processes periodically
setInterval(() => {
    const now = new Date();
    for (const [processId, processInfo] of activeProcesses.entries()) {
        // Remove processes older than 1 hour
        if (now - processInfo.startTime > 60 * 60 * 1000) {
            activeProcesses.delete(processId);
            processLogs.delete(processId);
        }
    }
}, 15 * 60 * 1000); // Run every 15 minutes

// Start server
app.listen(PORT, () => {
    console.log(`Personality Database Viewer server running on http://localhost:${PORT}`);
    console.log('Make sure the bot store data is available at ../bot_store/');
});

module.exports = app;