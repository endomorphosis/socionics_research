// DuckDB loader utilities - reused from compass
// Simplified version focusing on parquet file loading

export class DuckDBLoader {
    constructor() {
        this.db = null;
        this.conn = null;
    }

    async init() {
        if (this.db) return;

        const JSDELIVR_BUNDLES = {
            '@duckdb/duckdb-wasm': `https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-eh.worker.js`
        };

        const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
        const worker = new Worker(bundle.mainWorker);
        const logger = new duckdb.ConsoleLogger();
        this.db = new duckdb.AsyncDuckDB(logger, worker);
        await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
        this.conn = await this.db.connect();
    }

    async loadParquetFile(filePath, tableName = 'data') {
        if (!this.conn) {
            throw new Error('DuckDB not initialized. Call init() first.');
        }

        try {
            // Try to load the parquet file
            await this.conn.query(`CREATE OR REPLACE TABLE ${tableName} AS SELECT * FROM read_parquet('${filePath}')`);
            console.log(`Loaded ${tableName} from ${filePath}`);
            
            // Get row count
            const countResult = await this.conn.query(`SELECT COUNT(*) as count FROM ${tableName}`);
            const count = countResult.get(0).count;
            
            return {
                success: true,
                tableName,
                filePath,
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