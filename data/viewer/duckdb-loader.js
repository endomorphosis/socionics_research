// DuckDB loader utilities - adapted from compass
// Simplified version focusing on parquet file loading

import * as duckdb from '@duckdb/duckdb-wasm';

export class DuckDBLoader {
    constructor() {
        this.db = null;
        this.conn = null;
    }

    async init() {
        if (this.db) return;

        // Use locally served worker/wasm so we avoid cross-origin Worker restrictions
        const workerUrl = new URL('@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js', import.meta.url);
        const wasmUrl = new URL('@duckdb/duckdb-wasm/dist/duckdb-eh.wasm', import.meta.url);
        const worker = new Worker(workerUrl);

        let logger;
        try {
            if (duckdb.ConsoleLogger) {
                logger = new duckdb.ConsoleLogger(duckdb.LogLevel && (duckdb.LogLevel.ERROR || duckdb.LogLevel.WARNING));
                if (logger && typeof logger.setLogLevel === 'function') {
                    logger.setLogLevel((duckdb.LogLevel && (duckdb.LogLevel.ERROR || duckdb.LogLevel.WARNING)) || 2);
                }
            }
        } catch {}
        if (!logger) logger = { debug(){}, info(){}, warn(){}, error(){}, log(){} };

        this.db = new duckdb.AsyncDuckDB(logger, worker);
        await this.db.instantiate(wasmUrl.toString());
        this.conn = await this.db.connect();

        try {
            await this.conn.query('LOAD httpfs;');
            await this.conn.query('SET enable_http_metadata_cache=true;');
        } catch (e) {
            console.warn('DuckDB httpfs load failed:', e?.message || e);
        }
    }

    // Resolve dataset URLs to absolute paths
    resolveDatasetUrl(urlPath) {
        // Already absolute
        if (typeof urlPath === 'string' && /^https?:\/\//i.test(urlPath)) {
            return urlPath;
        }
        const origin = (globalThis.location && globalThis.location.origin) || 'http://localhost:3000';
        return new URL(urlPath, origin).toString();
    }

    async loadParquetFile(filePath, tableName = 'data') {
        if (!this.conn) {
            throw new Error('DuckDB not initialized. Call init() first.');
        }

        try {
            // Resolve the URL
            const absUrl = this.resolveDatasetUrl(filePath);
            
            // Quick reachability check
            try {
                const head = await fetch(absUrl, { method: 'HEAD' });
                if (!head.ok) throw new Error(`HTTP ${head.status}`);
            } catch (e) {
                throw new Error(`Cannot access parquet file: ${e.message}`);
            }
            
            // Try to load the parquet file
            await this.conn.query(`CREATE OR REPLACE TABLE ${tableName} AS SELECT * FROM read_parquet('${absUrl}')`);
            console.log(`Loaded ${tableName} from ${absUrl}`);
            
            // Get row count
            const countResult = await this.conn.query(`SELECT COUNT(*) as count FROM ${tableName}`);
            const count = countResult.get(0).count;
            
            return {
                success: true,
                tableName,
                filePath: absUrl,
                rowCount: count
            };
        } catch (error) {
            console.error(`Failed to load ${filePath}:`, error);
            return {
                success: false,
                tableName,
                filePath,
                error: error.message
            };
        }
    }

    async describeTable(tableName) {
        if (!this.conn) {
            throw new Error('DuckDB not initialized. Call init() first.');
        }

        try {
            const result = await this.conn.query(`DESCRIBE ${tableName}`);
            return result.toArray().map(row => Object.fromEntries(row));
        } catch (error) {
            console.error(`Failed to describe table ${tableName}:`, error);
            throw error;
        }
    }

    async query(sql) {
        if (!this.conn) {
            throw new Error('DuckDB not initialized. Call init() first.');
        }

        return await this.conn.query(sql);
    }

    async getConnection() {
        if (!this.conn) {
            await this.init();
        }
        return this.conn;
    }

    async close() {
        if (this.conn) {
            await this.conn.close();
        }
        if (this.db) {
            await this.db.terminate();
        }
    }
}

// Export default instance
export const duckdbLoader = new DuckDBLoader();