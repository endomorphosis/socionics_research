const express = require('express');
const cors = require('cors');
const path = require('path');
const { spawn } = require('child_process');
const fs = require('fs').promises;
const fssync = require('fs');

const app = express();
const PORT = process.env.PORT || 3000; // Single port for frontend + backend
let viteServer = null;

// Detect Python executable (prefer repo venv)
const VENV_PY = path.resolve(__dirname, '../../.venv/bin/python');
const PYTHON_EXEC = fssync.existsSync(VENV_PY) ? VENV_PY : (process.env.PYTHON || 'python3');

// Middleware
app.use(cors());
// Apply JSON parser to all routes except the export-parquet endpoint, which handles raw text
const jsonParser = express.json({ limit: '5mb' });
app.use((req, res, next) => {
    if (req.path === '/api/data/export-parquet') return next();
    return jsonParser(req, res, next);
});
// Note: static serving is configured later to avoid bypassing Vite transforms in dev

// Expose dataset directory for parquet loading
const DATASET_DIR = path.join(__dirname, '..', 'bot_store');
const OVERLAY_FILE = path.join(DATASET_DIR, 'pdb_profiles_overrides.json');
const EXPORTS_DIR = path.join(DATASET_DIR, 'exports');

// Simple caches to avoid heavy reloads each request
let profilesCache = { data: null, mtimeMs: 0 };
let vectorsCache = { data: null, mtimeMs: 0 };

async function readOverlay() {
    try {
        const raw = await fs.readFile(OVERLAY_FILE, 'utf8');
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed === 'object') return parsed;
        return { profiles: {} };
    } catch (e) {
        if (e.code === 'ENOENT') return { profiles: {} };
        throw e;
    }
}

async function writeOverlay(obj) {
    const data = JSON.stringify(obj || { profiles: {} }, null, 2);
    const tmp = OVERLAY_FILE + '.tmp';
    await fs.writeFile(tmp, data);
    await fs.rename(tmp, OVERLAY_FILE);
}

// Helper: set no-store cache headers to avoid stale content in dev/preview
function setNoStore(res) {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
}

// Serve dataset files statically for direct access
app.use('/dataset', express.static(DATASET_DIR, {
    etag: false,
    lastModified: false,
    maxAge: 0,
    setHeaders: (res) => {
        setNoStore(res);
        // Set appropriate content types for parquet files
        if (res.req.path.endsWith('.parquet')) {
            res.setHeader('Content-Type', 'application/octet-stream');
        }
    }
}));

// Dataset directory listing endpoint
app.get('/dataset', async (req, res) => {
    try {
        const files = await fs.readdir(DATASET_DIR);
        let exportsList = [];
        try {
            const ex = await fs.readdir(EXPORTS_DIR);
            exportsList = ex.filter(f => f.endsWith('.parquet')).map(f => `exports/${f}`);
        } catch {}
        res.json({ dir: DATASET_DIR, files, exports: exportsList });
    } catch (err) {
        res.status(500).json({ error: String(err) });
    }
});

// Store active scraper processes
const activeProcesses = new Map();
const processLogs = new Map();

// Serve the main dashboard
app.get('/', async (req, res) => {
    try {
        if (viteServer) {
            const html = await fs.readFile(path.join(__dirname, 'index.html'), 'utf8');
            const transformed = await viteServer.transformIndexHtml(req.originalUrl || '/', html);
            res.setHeader('Content-Type', 'text/html');
            return res.status(200).send(transformed);
        }
    } catch (e) {
        console.warn('Vite transform failed, falling back to static index:', e?.message || e);
    }
    return res.sendFile(path.join(__dirname, 'index.html'));
});

// API Routes

// Start scraping process
app.post('/api/scraper/start', async (req, res) => {
    try {
    const { mode, maxPages, delay, browser, url, rpm, concurrency, timeout } = req.body;
        
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
            command = PYTHON_EXEC;
            // Build args with correct argparse ordering: global flags BEFORE subcommand
            const globalArgs = ['-m', 'bot.pdb_cli'];
            // Global tuning flags
            globalArgs.push('--rpm', String(rpm || 60));
            globalArgs.push('--concurrency', String(concurrency || 3));
            if (timeout) { globalArgs.push('--timeout', String(timeout)); }
            // Global headers-file (if present)
            const headerCandidates = [
                path.resolve(__dirname, '../../data/bot_store/headers.json'),
                path.resolve(__dirname, '../../.secrets/pdb_headers.json')
            ];
            let v1v2HeaderJson = null;
            for (const hp of headerCandidates) {
                try {
                    await fs.access(hp);
                    globalArgs.push('--headers-file', hp);
                    try {
                        const hdrJson = await fs.readFile(hp, 'utf8');
                        if (hdrJson && hdrJson.trim().startsWith('{')) {
                            v1v2HeaderJson = hdrJson;
                        }
                    } catch {}
                    break;
                } catch {}
            }
            const subArgs = [
                'scan-all',
                '--pages', maxPages.toString(),
                '--auto-embed',
                '--auto-index'
            ];
            // Subparser-specific headers for optional v1 scraping during scan-all
            if (v1v2HeaderJson) {
                subArgs.push('--v1-headers', v1v2HeaderJson);
                subArgs.push('--v2-headers', v1v2HeaderJson);
            }
            commandArgs = [...globalArgs, ...subArgs];
        } else if (mode === 'incremental') {
            // Incremental scraping
            command = PYTHON_EXEC;
            // Global flags and headers-file first
            const globalArgs = ['-m', 'bot.pdb_cli', '--rpm', String(rpm || 60), '--concurrency', String(concurrency || 3)];
            if (timeout) { globalArgs.push('--timeout', String(timeout)); }
            const headerCandidates = [
                path.resolve(__dirname, '../../data/bot_store/headers.json'),
                path.resolve(__dirname, '../../.secrets/pdb_headers.json')
            ];
            for (const hp of headerCandidates) {
                try { await fs.access(hp); globalArgs.push('--headers-file', hp); break; } catch {}
            }
            const subArgs = ['follow-hot', '--pages', Math.min(maxPages, 5).toString(), '--auto-embed'];
            commandArgs = [...globalArgs, ...subArgs];
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
            const text = data.toString();
            // Log each line separately for better SSE granularity
            const lines = text.split(/\r?\n/).filter(Boolean);
            for (const line of lines) {
                addProcessLog(processId, line);
                // Lightweight progress parsing
                try {
                    const info = activeProcesses.get(processId);
                    if (info) {
                        const prog = info.progress || { current: 0, total: 0 };
                        // upserted new=12 updated=3
                        let m = line.match(/upserted\s+new\s*=\s*(\d+)\s+updated\s*=\s*(\d+)/i);
                        if (m) {
                            const inc = (parseInt(m[1], 10) || 0) + (parseInt(m[2], 10) || 0);
                            prog.current = (prog.current || 0) + inc;
                            info.progress = prog;
                        }
                        // Done. Upserted total rows: 123
                        m = line.match(/Upserted\s+total\s+rows:\s*(\d+)/i);
                        if (m) {
                            const tot = parseInt(m[1], 10) || 0;
                            prog.current = tot;
                            if ((prog.total || 0) < tot) prog.total = tot;
                            info.progress = prog;
                        }
                        // Dumped 100 profiles ...
                        m = line.match(/Dumped\s+(\d+)\s+profiles/i);
                        if (m) {
                            const c = parseInt(m[1], 10) || 0;
                            prog.current = c;
                            info.progress = prog;
                        }
                    }
                } catch {}
                console.log(`[${processId}] ${line}`);
            }
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
        const { url, browser } = req.body || {};
        if (!url || typeof url !== 'string') {
            return res.status(400).json({ success: false, error: 'url is required' });
        }

        const botPath = path.resolve(__dirname, '../../bot');
    const command = PYTHON_EXEC;
        const args = ['-m', 'bot.pdb_cli', 'expand-from-url', '--urls', url, '--only-profiles'];
        // Map browser hint to --render-js when using Playwright
        if (browser && String(browser).toLowerCase().includes('playwright')) {
            args.push('--render-js');
        }
        // Try to include headers file if present for richer v2 responses
        try {
            const headersCandidates = [
                path.resolve(__dirname, '../../data/bot_store/headers.json'),
                path.resolve(__dirname, '../../.secrets/pdb_headers.json')
            ];
            for (const hp of headersCandidates) {
                if (fssync.existsSync(hp)) {
                    args.push('--headers-file', hp);
                    break;
                }
            }
        } catch {}

        const env = { ...process.env, PYTHONPATH: path.join(botPath, 'src') };

        const scraperProcess = spawn(command, args, {
            cwd: botPath,
            env,
            stdio: ['pipe', 'pipe', 'pipe']
        });

        let output = '';
        let error = '';

        scraperProcess.stdout.on('data', (data) => { output += data.toString(); });
        scraperProcess.stderr.on('data', (data) => { error += data.toString(); });

        scraperProcess.on('close', (code) => {
            if (code === 0) {
                return res.json({ success: true, message: 'expand-from-url completed', url, output });
            }
            return res.status(500).json({ success: false, error: error || 'expand-from-url failed', code });
        });
    } catch (error) {
        console.error('Error scraping profile:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Check available scrapers
app.get('/api/scraper/available', async (req, res) => {
    try {
        // Check if playwright is available
        let playwrightAvailable = false;
        try {
            const { spawn } = require('child_process');
            const checkPlaywright = spawn(PYTHON_EXEC, ['-c', 'import playwright; print("available")']);
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
            const checkSelenium = spawn(PYTHON_EXEC, ['-c', 'import selenium; print("available")']);
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

// Export arbitrary rows to a parquet file in dataset dir
app.post('/api/data/export-parquet', express.text({ type: '*/*', limit: '25mb' }), async (req, res) => {
    try {
        // Ensure exports directory exists
        try { fssync.mkdirSync(EXPORTS_DIR, { recursive: true }); } catch {}
        let payload = req.body;
        if (typeof payload === 'string') {
            try { payload = JSON.parse(payload); }
            catch (e) { return res.status(400).json({ success: false, error: 'Invalid JSON body' }); }
        }
        const { rows, filename } = payload || {};
        if (!Array.isArray(rows) || rows.length === 0) {
            return res.status(400).json({ success: false, error: 'rows array required' });
        }
        // Sanitize filename
        const rawName = (typeof filename === 'string' ? filename : '').trim();
        const baseName = rawName ? path.basename(rawName) : `query_export_${Date.now()}.parquet`;
        let safeName = baseName.replace(/[^a-zA-Z0-9._-]/g, '_');
        if (!safeName.toLowerCase().endsWith('.parquet')) safeName = safeName + '.parquet';
    const destPath = path.join(EXPORTS_DIR, safeName);

        const python = spawn(PYTHON_EXEC, ['-c', `
import pandas as pd
import json
import sys
import pyarrow as pa
import pyarrow.parquet as pq

rows = json.loads(sys.stdin.read())
df = pd.DataFrame(rows)
table = pa.Table.from_pandas(df)
pq.write_table(table, r'''${destPath.replace(/\\/g,'/')}''')
print(json.dumps({'ok': True, 'rows': int(len(df))}))
        `]);

        let out = '', err = '';
        python.stdout.on('data', (c) => out += c.toString());
        python.stderr.on('data', (c) => err += c.toString());
        python.on('close', (code) => {
            if (code !== 0) {
                return res.status(500).json({ success: false, error: err || 'pyarrow write failed' });
            }
            try {
                const parsed = JSON.parse(out);
                // Invalidate caches so UI can pick up new file if needed
                profilesCache.mtimeMs = -1; vectorsCache.mtimeMs = -1;
                res.json({ success: true, file: destPath, filename: safeName, relative: `exports/${safeName}`, ...parsed });
            } catch (e) {
                res.status(500).json({ success: false, error: 'export parse failed' });
            }
        });
        python.stdin.write(JSON.stringify(rows));
        python.stdin.end();
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

// Clear simple data caches
app.post('/api/data/reload', async (req, res) => {
    try {
        profilesCache = { data: null, mtimeMs: 0 };
        vectorsCache = { data: null, mtimeMs: 0 };
        res.json({ success: true });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

// Clean datasets: drop empty rows, dedupe by cid, and export cleaned copies into exports/
app.post('/api/data/clean-datasets', async (req, res) => {
    try {
        // Ensure exports directory exists
        try { fssync.mkdirSync(EXPORTS_DIR, { recursive: true }); } catch {}
        const timestamp = Date.now();
        const targets = [
            { src: path.join(DATASET_DIR, 'pdb_profiles.parquet'), out: path.join(EXPORTS_DIR, `pdb_profiles_cleaned_${timestamp}.parquet`) },
            { src: path.join(DATASET_DIR, 'pdb_profiles_normalized.parquet'), out: path.join(EXPORTS_DIR, `pdb_profiles_normalized_cleaned_${timestamp}.parquet`) },
            { src: path.join(DATASET_DIR, 'pdb_profiles_merged.parquet'), out: path.join(EXPORTS_DIR, `pdb_profiles_merged_cleaned_${timestamp}.parquet`) }
        ];
        const script = `
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json, sys, os

def load_df(path):
    try:
        return pd.read_parquet(path)
    except Exception:
        return None

def normalize_df(df):
    if df is None or df.empty:
        return pd.DataFrame(columns=['cid','name','mbti','socionics','description','category'])
    # Try to extract payload JSON if present
    rows = []
    for _, row in df.iterrows():
        try:
            cid = row.get('cid', None)
            name = None
            mbti = None
            socionics = None
            description = None
            category = None
            payload = None
            if 'payload_bytes' in row.index and row['payload_bytes'] is not None:
                try:
                    pb = row['payload_bytes']
                    if isinstance(pb, (bytes, bytearray)):
                        pb = pb.decode('utf-8', errors='ignore')
                    payload = json.loads(pb)
                except Exception:
                    payload = None
            elif 'payload' in row.index and row['payload'] is not None:
                try:
                    payload = row['payload'] if isinstance(row['payload'], dict) else json.loads(row['payload'])
                except Exception:
                    payload = None
            if payload is not None:
                name = payload.get('name', payload.get('title'))
                mbti = payload.get('mbti')
                socionics = payload.get('socionics')
                description = payload.get('description', payload.get('bio'))
                category = payload.get('category')
            # fallback columns
            if name is None and 'name' in row.index: name = row['name']
            if mbti is None and 'mbti' in row.index: mbti = row['mbti']
            if socionics is None and 'socionics' in row.index: socionics = row['socionics']
            if description is None and 'description' in row.index: description = row['description']
            if category is None and 'category' in row.index: category = row['category']
            rows.append({
                'cid': cid,
                'name': name if isinstance(name, str) else (str(name) if name is not None else ''),
                'mbti': mbti if isinstance(mbti, str) else (str(mbti) if mbti is not None else ''),
                'socionics': socionics if isinstance(socionics, str) else (str(socionics) if socionics is not None else ''),
                'description': description if isinstance(description, str) else (str(description) if description is not None else ''),
                'category': category if isinstance(category, str) else (str(category) if category is not None else '')
            })
        except Exception:
            continue
    out = pd.DataFrame(rows)
    # Drop completely empty rows (all NaN/empty strings)
    if not out.empty:
        out.replace('', pd.NA, inplace=True)
        out.dropna(how='all', inplace=True)
        # Prefer to keep rows with cid; drop rows without cid if they are otherwise empty
        if 'cid' in out.columns:
            # Deduplicate by cid keeping first occurrence
            out = out.drop_duplicates(subset=['cid'], keep='first')
    return out

results = []
`;
        const perTarget = targets.map(t => `
df = load_df(r'''${t.src.replace(/\\/g,'/')}''')
clean = normalize_df(df)
table = pa.Table.from_pandas(clean)
pq.write_table(table, r'''${t.out.replace(/\\/g,'/')}''')
results.append({'src': r'''${t.src.replace(/\\/g,'/')}''', 'out': r'''${t.out.replace(/\\/g,'/')}''', 'rows': int(len(clean))})
`).join('\n');
        const trailer = `
print(json.dumps({'results': results}))
`;
        const python = spawn(PYTHON_EXEC, ['-c', script + perTarget + trailer]);
        let data = '', err = '';
        python.stdout.on('data', (c) => data += c.toString());
        python.stderr.on('data', (c) => err += c.toString());
        python.on('close', async (code) => {
            if (code !== 0) {
                return res.status(500).json({ success: false, error: err || 'Cleaning failed' });
            }
            try {
                const parsed = JSON.parse(data);
                // Invalidate caches
                profilesCache.mtimeMs = -1; vectorsCache.mtimeMs = -1;
                return res.json({ success: true, ...parsed });
            } catch (e) {
                return res.status(500).json({ success: false, error: 'Parse failed' });
            }
        });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

// Backfill missing fields by orchestrating CLI helpers: scrape v1 missing, re-export normalized
app.post('/api/data/fill-missing', async (req, res) => {
    try {
        const botPath = path.resolve(__dirname, '../../bot');
        const env = { ...process.env, PYTHONPATH: path.join(botPath, 'src') };
        const steps = [];
        const rpm = (req.body && req.body.rpm) ? Number(req.body.rpm) : null;
        const concurrency = (req.body && req.body.concurrency) ? Number(req.body.concurrency) : null;
        const timeout = (req.body && req.body.timeout) ? Number(req.body.timeout) : null;
        // Try to include headers file for better API access
        const headerCandidates = [
            path.resolve(__dirname, '../../data/bot_store/headers.json'),
            path.resolve(__dirname, '../../.secrets/pdb_headers.json')
        ];
        let headerArgs = [];
        for (const hp of headerCandidates) {
            try {
                await fs.access(hp);
                const hdrJson = await fs.readFile(hp, 'utf8');
                if (hdrJson && hdrJson.trim().startsWith('{')) {
                    headerArgs = ['--v1-headers', hdrJson, '--v2-headers', hdrJson];
                }
                break;
            } catch {}
        }
    // Step 1: scrape v1 profiles for missing (include global tuning flags if provided)
    const svmArgs = ['-m','bot.pdb_cli'];
    if (rpm) { svmArgs.push('--rpm', String(rpm)); }
    if (concurrency) { svmArgs.push('--concurrency', String(concurrency)); }
    if (timeout) { svmArgs.push('--timeout', String(timeout)); }
    svmArgs.push('scrape-v1-missing', '--max','0', '--auto-embed', '--auto-index', ...headerArgs);
    steps.push({ cmd: PYTHON_EXEC, args: svmArgs });
        // Step 2: re-export normalized parquet
        steps.push({ cmd: PYTHON_EXEC, args: ['-m','bot.pdb_cli','export','--out', path.join(DATASET_DIR, 'pdb_profiles_normalized.parquet')] });
        for (const step of steps) {
            await new Promise((resolve, reject) => {
                const p = spawn(step.cmd, step.args, { cwd: botPath, env, stdio: ['ignore','pipe','pipe'] });
                let err = '';
                p.stderr.on('data', (d) => { err += d.toString(); });
                p.on('close', (code) => {
                    if (code === 0) return resolve(0);
                    return reject(new Error(err || 'step failed'));
                });
                p.on('error', (e) => reject(e));
            });
        }
        // Invalidate caches so UI reloads fresh
        profilesCache.mtimeMs = -1; vectorsCache.mtimeMs = -1;
        res.json({ success: true, summary: 'v1 backfill + normalized export complete' });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

// Commit overlay into a new parquet file
app.post('/api/data/commit-overlay', async (req, res) => {
    try {
        const outFile = (req.body && req.body.filename) || 'pdb_profiles_merged.parquet';
        const destPath = path.join(DATASET_DIR, outFile);

        // Prepare overlay JSON
        const overlay = await readOverlay();
        const overlayJson = JSON.stringify(overlay || { profiles: {} });

        const { spawn } = require('child_process');
        const python = spawn(PYTHON_EXEC, ['-c', `
import pandas as pd
import json
import sys
import pyarrow.parquet as pq
import pyarrow as pa

DATASET_DIR = r'''${DATASET_DIR.replace(/\\/g, '/')}'''
OUT_PATH = r'''${destPath.replace(/\\/g, '/')}'''

def load_profiles_df():
    main = f"{DATASET_DIR}/pdb_profiles.parquet"
    norm = f"{DATASET_DIR}/pdb_profiles_normalized.parquet"
    df = None
    try:
        df = pd.read_parquet(main)
    except Exception:
        try:
            df = pd.read_parquet(norm)
        except Exception as e:
            print(json.dumps({'error': f'Cannot read base parquet: {e}'}), file=sys.stderr)
            sys.exit(1)
    # Normalize to simple columns
    rows = []
    for _, row in df.iterrows():
        try:
            cid = row.get('cid', None)
            name = None
            mbti = None
            socionics = None
            description = None
            category = None

            payload = None
            try:
                if 'payload_bytes' in row.index and row['payload_bytes'] is not None:
                    payload = row['payload_bytes']
                    if isinstance(payload, (bytes, bytearray)):
                        payload = payload.decode('utf-8', errors='ignore')
                    payload = json.loads(payload)
                elif 'payload' in row.index and row['payload'] is not None:
                    payload = row['payload']
                    if not isinstance(payload, dict):
                        payload = json.loads(payload)
            except Exception:
                payload = None

            if payload is not None:
                name = payload.get('name', payload.get('title'))
                mbti = payload.get('mbti')
                socionics = payload.get('socionics')
                description = payload.get('description', payload.get('bio'))
                category = payload.get('category')

            if name is None and 'name' in row.index: name = row['name']
            if mbti is None and 'mbti' in row.index: mbti = row['mbti']
            if socionics is None and 'socionics' in row.index: socionics = row['socionics']
            if description is None and 'description' in row.index: description = row['description']
            if category is None and 'category' in row.index: category = row['category']

            if cid is not None:
                rows.append({
                    'cid': cid,
                    'name': name or 'Unknown',
                    'mbti': mbti or '',
                    'socionics': socionics or '',
                    'description': description or '',
                    'category': category or 'Scraped Profile'
                })
        except Exception:
            continue
    return pd.DataFrame(rows)

try:
    base_df = load_profiles_df()
    # Ensure single row per cid before overlay merge
    if 'cid' in base_df.columns:
        base_df = base_df.drop_duplicates(subset=['cid'], keep='first')
    base_count = len(base_df)
    # Overlay
    overlay = json.loads(r'''${overlayJson.replace(/'/g, "''")}''')
    over = overlay.get('profiles', {}) if isinstance(overlay, dict) else {}

    if over:
        base_df = base_df.set_index('cid', drop=False)
        for cid, patch in over.items():
            if cid in base_df.index:
                for col in ['name','mbti','socionics','description','category']:
                    val = patch.get(col)
                    if val is not None:
                        base_df.at[cid, col] = val
            else:
                base_df.loc[cid] = {
                    'cid': cid,
                    'name': patch.get('name','Unknown'),
                    'mbti': patch.get('mbti',''),
                    'socionics': patch.get('socionics',''),
                    'description': patch.get('description',''),
                    'category': patch.get('category','User Created')
                }
        base_df = base_df.reset_index(drop=True)
        # Deduplicate again after merge to be safe
        if 'cid' in base_df.columns:
            base_df = base_df.drop_duplicates(subset=['cid'], keep='first')

    merged_count = len(base_df)
    # Write parquet
    table = pa.Table.from_pandas(base_df)
    pq.write_table(table, OUT_PATH)

    print(json.dumps({'base_count': int(base_count), 'merged_count': int(merged_count), 'out_file': OUT_PATH}))
except Exception as e:
    print(json.dumps({'error': str(e)}), file=sys.stderr)
    sys.exit(1)
        `]);

        let data = '';
        let error = '';
        python.stdout.on('data', (c) => data += c.toString());
        python.stderr.on('data', (c) => error += c.toString());
        python.on('close', async (code) => {
            if (code !== 0) {
                console.error('Commit overlay error:', error);
                return res.status(500).json({ success: false, error: error || 'Commit failed' });
            }
            try {
                const parsed = JSON.parse(data);
                // Invalidate cache to reflect new file on next request
                profilesCache.mtimeMs = -1;
                return res.json({ success: true, ...parsed });
            } catch (e) {
                console.error('Commit parse error:', e);
                return res.status(500).json({ success: false, error: 'Commit parse failed' });
            }
        });

    } catch (error) {
        console.error('Error committing overlay:', error);
        res.status(500).json({ success: false, error: error.message });
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

// API endpoint to load parquet data as JSON
app.get('/api/data/profiles', async (req, res) => {
    try {
        // Serve from cache if parquet unchanged
        const parquetPath = path.join(DATASET_DIR, 'pdb_profiles.parquet');
        let parquetMtime = 0;
        try {
            const st = await fs.stat(parquetPath);
            parquetMtime = st.mtimeMs;
        } catch {}

        if (profilesCache.data && profilesCache.mtimeMs === parquetMtime) {
            return res.json(profilesCache.data);
        }

        // Try to use pandas to read parquet file robustly
    const { spawn } = require('child_process');
    const python = spawn(PYTHON_EXEC, ['-c', `
import pandas as pd
import json
import sys
import pyarrow as pa
import pyarrow.parquet as pq

try:
    # Read the profiles parquet file (fallback to normalized if needed)
    path_main = '${DATASET_DIR.replace(/\\/g, '/').replace(/'/g, "'")}/pdb_profiles.parquet'
    path_norm = '${DATASET_DIR.replace(/\\/g, '/').replace(/'/g, "'")}/pdb_profiles_normalized.parquet'
    try:
        df_profiles = pd.read_parquet(path_main)
    except Exception as e:
        df_profiles = pd.read_parquet(path_norm)
    
    # Convert to records
    profiles = []
    for _, row in df_profiles.iterrows():
        try:
            cid = row.get('cid', None)
            name = None
            mbti = None
            socionics = None
            description = None
            category = None

            if 'payload_bytes' in row.index and row['payload_bytes'] is not None:
                try:
                    payload = json.loads(row['payload_bytes'])
                except Exception:
                    payload = None
            elif 'payload' in row.index and row['payload'] is not None:
                try:
                    payload = row['payload'] if isinstance(row['payload'], dict) else json.loads(row['payload'])
                except Exception:
                    payload = None
            else:
                payload = None

            if payload is not None:
                name = payload.get('name', payload.get('title'))
                mbti = payload.get('mbti')
                socionics = payload.get('socionics')
                description = payload.get('description', payload.get('bio'))
                category = payload.get('category')

            # Fallback to columns if present
            if name is None and 'name' in row.index:
                name = row['name']
            if mbti is None and 'mbti' in row.index:
                mbti = row['mbti']
            if socionics is None and 'socionics' in row.index:
                socionics = row['socionics']
            if description is None and 'description' in row.index:
                description = row['description']
            if category is None and 'category' in row.index:
                category = row['category']

            profiles.append({
                'cid': cid,
                'name': name or 'Unknown',
                'mbti': mbti or '',
                'socionics': socionics or '',
                'description': description or '',
                'category': category or 'Scraped Profile'
            })
        except Exception as e:
            print(f"Error parsing profile {row['cid']}: {e}", file=sys.stderr)
            continue
    
    print(json.dumps({'profiles': profiles, 'count': len(profiles)}, indent=None, separators=(',', ':')))
except Exception as e:
    print(json.dumps({'error': str(e)}), file=sys.stderr)
    sys.exit(1)
`]);

        let data = '';
        let error = '';
        
        python.stdout.on('data', (chunk) => {
            data += chunk.toString();
        });
        
        python.stderr.on('data', (chunk) => {
            error += chunk.toString();
        });
        
        python.on('close', async (code) => {
            if (code !== 0) {
                console.error('Python error:', error);
                res.status(500).json({ error: 'Failed to load parquet data: ' + error });
                return;
            }
            
            try {
                const parsed = JSON.parse(data);
                let profiles = parsed.profiles || [];
                // Merge overlay
                let overlay = await readOverlay();
                const over = overlay.profiles || {};
                if (over && Object.keys(over).length) {
                    const byCid = new Map();
                    for (const p of profiles) if (p && p.cid) byCid.set(p.cid, p);
                    for (const [cid, op] of Object.entries(over)) {
                        if (byCid.has(cid)) {
                            byCid.set(cid, { ...byCid.get(cid), ...op, cid });
                        } else {
                            byCid.set(cid, { ...op, cid });
                        }
                    }
                    profiles = Array.from(byCid.values());
                }
                const result = { profiles, count: profiles.length };
                profilesCache = { data: result, mtimeMs: parquetMtime };
                res.json(result);
            } catch (parseError) {
                console.error('JSON parse error:', parseError);
                res.status(500).json({ error: 'Failed to parse parquet data' });
            }
        });
        
    } catch (error) {
        console.error('Error loading parquet data:', error);
        res.status(500).json({ error: error.message });
    }
});

// API endpoint to load vectors data as JSON
app.get('/api/data/vectors', async (req, res) => {
    try {
        // Serve from cache if unchanged
        const vecPath = path.join(DATASET_DIR, 'pdb_profile_vectors.parquet');
        let vecMtime = 0;
        try {
            const st = await fs.stat(vecPath);
            vecMtime = st.mtimeMs;
        } catch {}
        if (vectorsCache.data && vectorsCache.mtimeMs === vecMtime) {
            return res.json(vectorsCache.data);
        }

        // Try to use pandas to read vector parquet file
    const { spawn } = require('child_process');
    const python = spawn(PYTHON_EXEC, ['-c', `
import pandas as pd
import json
import sys
import numpy as np

try:
    # Read the vectors parquet file
    df_vectors = pd.read_parquet('${DATASET_DIR}/pdb_profile_vectors.parquet')
    
    # Convert to records
    vectors = []
    for _, row in df_vectors.iterrows():
        try:
            vector_array = row['vector']
            if hasattr(vector_array, 'tolist'):
                vector_list = vector_array.tolist()
            else:
                vector_list = list(vector_array)
            
            vectors.append({
                'cid': row['cid'],
                'vector': vector_list
            })
        except Exception as e:
            print(f"Error processing vector {row['cid']}: {e}", file=sys.stderr)
            continue
    
    print(json.dumps({'vectors': vectors, 'count': len(vectors)}, indent=None, separators=(',', ':')))
except Exception as e:
    print(json.dumps({'error': str(e)}), file=sys.stderr)
    sys.exit(1)
`]);

        let data = '';
        let error = '';
        
        python.stdout.on('data', (chunk) => {
            data += chunk.toString();
        });
        
        python.stderr.on('data', (chunk) => {
            error += chunk.toString();
        });
        
        python.on('close', (code) => {
            if (code !== 0) {
                console.error('Python error (vectors):', error);
                res.status(500).json({ error: 'Failed to load vector data: ' + error });
                return;
            }
            
            try {
                const result = JSON.parse(data);
                vectorsCache = { data: result, mtimeMs: vecMtime };
                res.json(result);
            } catch (parseError) {
                console.error('JSON parse error (vectors):', parseError);
                res.status(500).json({ error: 'Failed to parse vector data' });
            }
        });
        
    } catch (error) {
        console.error('Error loading vector data:', error);
        res.status(500).json({ error: error.message });
    }
});

// CRUD API endpoints for profiles

// Create new profile
app.post('/api/data/profiles', async (req, res) => {
    try {
        const { name, mbti, socionics, description, category } = req.body || {};
        if (!name || typeof name !== 'string') {
            return res.status(400).json({ success: false, error: 'name is required' });
        }
        // Generate overlay CID
        const newCid = `ovr_${Date.now().toString(36)}_${Math.floor(Math.random()*1e6).toString(36)}`;
        const overlay = await readOverlay();
        overlay.profiles[newCid] = {
            cid: newCid,
            name: name || 'Unknown',
            mbti: mbti || '',
            socionics: socionics || '',
            description: description || '',
            category: category || 'User Created',
            _source: 'overlay'
        };
        await writeOverlay(overlay);
        // Invalidate cache so next GET includes new entry
        profilesCache.mtimeMs = -1;
        res.json({ success: true, cid: newCid });
    } catch (error) {
        console.error('Error creating profile:', error);
        res.status(500).json({ error: error.message });
    }
});

// Update existing profile
app.put('/api/data/profiles/:cid', async (req, res) => {
    try {
        const { cid } = req.params;
        if (!cid) return res.status(400).json({ success: false, error: 'cid required' });
        const patch = req.body || {};
        const overlay = await readOverlay();
        const existing = overlay.profiles[cid] || { cid };
        overlay.profiles[cid] = { ...existing, ...patch, cid, _source: 'overlay' };
        await writeOverlay(overlay);
        profilesCache.mtimeMs = -1;
        res.json({ success: true });
    } catch (error) {
        console.error('Error updating profile:', error);
        res.status(500).json({ error: error.message });
    }
});

// Delete profile
app.delete('/api/data/profiles/:cid', async (req, res) => {
    try {
        const { cid } = req.params;
        if (!cid) return res.status(400).json({ success: false, error: 'cid required' });
        const overlay = await readOverlay();
        if (overlay.profiles[cid]) {
            delete overlay.profiles[cid];
            await writeOverlay(overlay);
            profilesCache.mtimeMs = -1;
        }
        res.json({ success: true });
    } catch (error) {
        console.error('Error deleting profile:', error);
        res.status(500).json({ error: error.message });
    }
});

// Expose overlay for debugging
app.get('/api/data/overrides', async (req, res) => {
    try {
        const data = await readOverlay();
        res.json({ success: true, ...data });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

// Start server with optional Vite middleware for single-port dev
async function startServer() {
    if (process.env.NODE_ENV !== 'production') {
        try {
            const vite = require('vite');
            viteServer = await vite.createServer({
                root: __dirname,
                server: { middlewareMode: true },
                appType: 'custom'
            });
            // Attach Vite first so it can transform index.html and modules
            app.use(viteServer.middlewares);
            console.log('Vite dev middleware attached on same port');
        } catch (e) {
            console.warn('Vite middleware not attached:', e?.message || e);
        }
    }

    // Static assets (only after Vite in dev, always in production)
    app.use(express.static(__dirname));

    app.listen(PORT, () => {
        console.log(`Personality Database Viewer server running on http://localhost:${PORT}`);
        console.log('Datasets available at /dataset (', DATASET_DIR, ')');
        console.log('Python executable for parquet:', PYTHON_EXEC);
    });
}

startServer();

module.exports = app;