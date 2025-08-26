/**
 * WebAssembly-compatible IPDB Manager using DuckDB-WASM and hnswlib-wasm
 * ====================================================================
 * 
 * This module provides browser-compatible database and vector search functionality
 * using WebAssembly versions of DuckDB and hnswlib.
 */

import * as duckdb from '@duckdb/duckdb-wasm';
import { loadHnswlib } from 'hnswlib-wasm';

export interface Entity {
  id: string;
  name: string;
  entity_type: 'person' | 'fictional_character' | 'public_figure';
  description?: string;
  metadata?: Record<string, any>;
  embedding?: Float32Array;
}

export interface VectorSearchResult {
  entity: Entity;
  similarity: number;
  distance: number;
}

export class IPDBManagerWASM {
  private db?: duckdb.AsyncDuckDB;
  private conn?: duckdb.AsyncDuckDBConnection;
  private vectorIndex?: any; // hnswlib-wasm index
  private hnswlib?: any;
  private entities: Map<string, Entity> = new Map();
  private embeddings: Map<string, Float32Array> = new Map();
  private initialized = false;

  constructor(private vectorDimension: number = 384) {}

  /**
   * Initialize DuckDB-WASM and hnswlib-wasm
   */
  async initialize(): Promise<void> {
    if (this.initialized) return;

    // Initialize DuckDB-WASM
    await this.initializeDuckDB();
    
    // Initialize hnswlib-wasm
    await this.initializeHnswlib();
    
    // Load and execute schema
    await this.loadSchema();
    
    // Initialize typing systems
    await this.initializeTypingSystems();
    
    this.initialized = true;
  }

  /**
   * Initialize DuckDB-WASM
   */
  private async initializeDuckDB(): Promise<void> {
    // Get URLs for worker and WASM files
    const mainWorkerUrl = new URL('@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js', import.meta.url);
    const wasmUrl = new URL('@duckdb/duckdb-wasm/dist/duckdb-eh.wasm', import.meta.url);
    
    // Create worker
    const worker = new Worker(mainWorkerUrl);
    
    // Create logger (quiet mode)
    const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.ERROR);
    
    // Initialize DuckDB
    this.db = new duckdb.AsyncDuckDB(logger, worker);
    await this.db.instantiate(wasmUrl.toString());
    
    // Create connection
    this.conn = await this.db.connect();
    
    // Load HTTP filesystem for Parquet support
    try {
      await this.conn.query('INSTALL httpfs;');
      await this.conn.query('LOAD httpfs;');
      await this.conn.query('SET enable_http_metadata_cache=true;');
    } catch (e) {
      console.warn('DuckDB httpfs setup failed (may already be loaded):', e);
    }
  }

  /**
   * Initialize hnswlib-wasm
   */
  private async initializeHnswlib(): Promise<void> {
    this.hnswlib = await loadHnswlib();
    
    // Create vector index
    this.vectorIndex = new this.hnswlib.HierarchicalNSW('cosine', this.vectorDimension);
    this.vectorIndex.initIndex(1000, 16, 200, 0); // maxElements, M, efConstruction, seed
  }

  /**
   * Load database schema
   */
  private async loadSchema(): Promise<void> {
    // Since we can't read files in WASM, we inline the schema
    const schema = `
      CREATE TABLE IF NOT EXISTS entities (
        id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        entity_type VARCHAR NOT NULL DEFAULT 'person',
        description TEXT,
        metadata JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS users (
        id VARCHAR PRIMARY KEY,
        username VARCHAR NOT NULL UNIQUE,
        role VARCHAR NOT NULL,
        experience_level VARCHAR NOT NULL DEFAULT 'novice',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS typing_systems (
        name VARCHAR PRIMARY KEY,
        display_name VARCHAR NOT NULL,
        description TEXT
      );

      CREATE TABLE IF NOT EXISTS personality_types (
        id INTEGER PRIMARY KEY,
        system_name VARCHAR NOT NULL,
        type_name VARCHAR NOT NULL,
        display_name VARCHAR,
        FOREIGN KEY (system_name) REFERENCES typing_systems(name)
      );

      CREATE TABLE IF NOT EXISTS typings (
        id VARCHAR PRIMARY KEY,
        entity_id VARCHAR NOT NULL,
        system_name VARCHAR NOT NULL,
        type_name VARCHAR NOT NULL,
        confidence DOUBLE NOT NULL DEFAULT 1.0,
        probability_distribution JSON,
        user_id VARCHAR,
        session_id VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (entity_id) REFERENCES entities(id),
        FOREIGN KEY (system_name) REFERENCES typing_systems(name),
        FOREIGN KEY (user_id) REFERENCES users(id)
      );

      CREATE TABLE IF NOT EXISTS entity_embeddings (
        entity_id VARCHAR PRIMARY KEY,
        embedding BLOB NOT NULL,
        dimension INTEGER NOT NULL,
        FOREIGN KEY (entity_id) REFERENCES entities(id)
      );
    `;

    await this.conn!.query(schema);
  }

  /**
   * Import data from Parquet URL (supports HTTP/HTTPS)
   */
  async importFromParquet(parquetUrl: string): Promise<void> {
    const query = `
      SELECT 
        id,
        name,
        COALESCE(entity_type, 'person') as entity_type,
        description,
        metadata
      FROM read_parquet('${parquetUrl}')
    `;

    const result = await this.conn!.query(query);
    
    for (let i = 0; i < result.numRows; i++) {
      const row = result.get(i);
      const entity: Entity = {
        id: row.id || this.generateId(),
        name: row.name,
        entity_type: row.entity_type || 'person',
        description: row.description,
        metadata: row.metadata ? (typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata) : {}
      };

      await this.createEntity(entity);
    }
  }

  /**
   * Create a new entity
   */
  async createEntity(entity: Entity): Promise<Entity> {
    const query = `
      INSERT INTO entities (id, name, entity_type, description, metadata, created_at)
      VALUES (?, ?, ?, ?, ?, ?)
    `;

    await this.conn!.query(query, [
      entity.id,
      entity.name,
      entity.entity_type,
      entity.description || null,
      JSON.stringify(entity.metadata || {}),
      new Date().toISOString()
    ]);

    this.entities.set(entity.id, entity);
    return entity;
  }

  /**
   * Add vector embedding for an entity
   */
  async addEmbedding(entityId: string, embedding: Float32Array): Promise<void> {
    const entity = this.entities.get(entityId);
    if (!entity) {
      throw new Error(`Entity not found: ${entityId}`);
    }

    // Store in vector index
    const index = this.embeddings.size;
    this.vectorIndex.addPoint(embedding, index);
    this.embeddings.set(entityId, embedding);

    // Store in database
    const query = `
      INSERT OR REPLACE INTO entity_embeddings (entity_id, embedding, dimension)
      VALUES (?, ?, ?)
    `;

    const buffer = new Uint8Array(embedding.buffer);
    await this.conn!.query(query, [entityId, buffer, embedding.length]);
  }

  /**
   * Search for similar entities using vector similarity
   */
  async vectorSearch(queryEmbedding: Float32Array, k: number = 10): Promise<VectorSearchResult[]> {
    if (!this.vectorIndex || this.embeddings.size === 0) {
      return [];
    }

    const searchResult = this.vectorIndex.searchKnn(queryEmbedding, k);
    const results: VectorSearchResult[] = [];

    const neighborIndices = Array.from(searchResult.neighbors());
    const distances = Array.from(searchResult.distances());

    for (let i = 0; i < neighborIndices.length; i++) {
      const neighborIndex = neighborIndices[i];
      const distance = distances[i];
      
      // Find entity by embedding index
      let entityId: string | null = null;
      let currentIndex = 0;
      for (const [id] of this.embeddings) {
        if (currentIndex === neighborIndex) {
          entityId = id;
          break;
        }
        currentIndex++;
      }

      if (entityId && this.entities.has(entityId)) {
        const entity = this.entities.get(entityId)!;
        results.push({
          entity,
          similarity: 1 - distance, // Convert distance to similarity
          distance
        });
      }
    }

    return results.sort((a, b) => b.similarity - a.similarity);
  }

  /**
   * Get entities with optional filtering
   */
  async getEntities(options: {
    limit?: number;
    offset?: number;
    entityType?: Entity['entity_type'];
    search?: string;
  } = {}): Promise<Entity[]> {
    let query = 'SELECT * FROM entities WHERE 1=1';
    const params: any[] = [];

    if (options.entityType) {
      query += ' AND entity_type = ?';
      params.push(options.entityType);
    }

    if (options.search) {
      query += ' AND name ILIKE ?';
      params.push(`%${options.search}%`);
    }

    query += ' ORDER BY name';

    if (options.limit) {
      query += ' LIMIT ?';
      params.push(options.limit);
    }

    if (options.offset) {
      query += ' OFFSET ?';
      params.push(options.offset);
    }

    const result = await this.conn!.query(query, params);
    const entities: Entity[] = [];

    for (let i = 0; i < result.numRows; i++) {
      const row = result.get(i);
      entities.push({
        id: row.id,
        name: row.name,
        entity_type: row.entity_type,
        description: row.description,
        metadata: row.metadata ? (typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata) : {}
      });
    }

    return entities;
  }

  /**
   * Initialize default typing systems
   */
  private async initializeTypingSystems(): Promise<void> {
    const systems = [
      {
        name: 'socionics',
        display_name: 'Socionics',
        description: '16 personality types based on Jungian cognitive functions',
        types: [
          'ILE', 'SEI', 'ESE', 'LII', 'EIE', 'LSI', 'SLE', 'IEI',
          'SEE', 'ILI', 'LIE', 'ESI', 'LSE', 'EII', 'IEE', 'SLI'
        ]
      },
      {
        name: 'mbti',
        display_name: 'Myers-Briggs Type Indicator',
        description: '16 personality types based on preferences',
        types: [
          'INTJ', 'INTP', 'ENTJ', 'ENTP', 'INFJ', 'INFP', 'ENFJ', 'ENFP',
          'ISTJ', 'ISFJ', 'ESTJ', 'ESFJ', 'ISTP', 'ISFP', 'ESTP', 'ESFP'
        ]
      }
    ];

    for (const system of systems) {
      await this.conn!.query(`
        INSERT OR REPLACE INTO typing_systems (name, display_name, description)
        VALUES (?, ?, ?)
      `, [system.name, system.display_name, system.description]);

      for (const typeName of system.types) {
        await this.conn!.query(`
          INSERT OR REPLACE INTO personality_types (system_name, type_name, display_name)
          VALUES (?, ?, ?)
        `, [system.name, typeName, typeName]);
      }
    }
  }

  /**
   * Generate a UUID-like ID
   */
  private generateId(): string {
    return 'xxxx-xxxx-4xxx-yxxx'.replace(/[xy]/g, function(c) {
      const r = Math.random() * 16 | 0;
      const v = c === 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }

  /**
   * Close database connection
   */
  async close(): Promise<void> {
    if (this.conn) {
      await this.conn.close();
    }
    if (this.db) {
      await this.db.terminate();
    }
  }
}

export default IPDBManagerWASM;