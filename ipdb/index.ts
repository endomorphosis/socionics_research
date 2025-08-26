/**
 * Enhanced IPDB Database Manager with DuckDB and Vector Search Support
 * =================================================================
 * 
 * This module extends the IPDB system with:
 * - DuckDB integration for better performance and web compatibility
 * - Vector search capabilities using hnswlib
 * - Multi-language API support (Python, TypeScript, JavaScript)
 * - WebAssembly support for browser deployment
 * - Enhanced Parquet file handling
 */

import * as duckdb from 'duckdb';
import { HierarchicalNSW } from 'hnswlib-node';
import { readFileSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

export interface Entity {
  id: string;
  name: string;
  entity_type: 'person' | 'fictional_character' | 'public_figure';
  description?: string;
  metadata?: Record<string, any>;
  embedding?: Float32Array;
}

export interface User {
  id: string;
  username: string;
  role: 'annotator' | 'panel_rater' | 'adjudicator' | 'admin';
  experience_level: 'novice' | 'intermediate' | 'expert' | 'professional';
  created_at: Date;
}

export interface Typing {
  id: string;
  entity_id: string;
  system_name: string;
  type_name: string;
  confidence: number;
  probability_distribution?: Record<string, number>;
  user_id?: string;
  session_id?: string;
  created_at: Date;
}

export interface VectorSearchResult {
  entity: Entity;
  similarity: number;
  distance: number;
}

export class IPDBManagerJS {
  private db: duckdb.Database;
  private vectorIndex?: HierarchicalNSW;
  private entities: Map<string, Entity> = new Map();
  private embeddings: Map<string, Float32Array> = new Map();
  private initialized = false;

  constructor(
    private dbPath: string = ':memory:',
    private vectorDimension: number = 384
  ) {
    this.db = new duckdb.Database(dbPath);
  }

  /**
   * Initialize the database and load schema
   */
  async initialize(): Promise<void> {
    if (this.initialized) return;

    // Load and execute schema
    const schemaPath = join(__dirname, 'database_schema.sql');
    const schema = readFileSync(schemaPath, 'utf-8');
    
    await this.executeQuery(schema);
    
    // Initialize typing systems
    await this.initializeTypingSystems();
    
    // Initialize vector index
    this.vectorIndex = new HierarchicalNSW('cosine', this.vectorDimension);
    this.vectorIndex.initIndex(1000, 16, 200, 0); // maxElements, M, efConstruction, seed
    
    this.initialized = true;
  }

  /**
   * Execute a query on DuckDB
   */
  private executeQuery(query: string, params?: any[]): Promise<any[]> {
    return new Promise((resolve, reject) => {
      this.db.all(query, params || [], (err, rows) => {
        if (err) reject(err);
        else resolve(rows || []);
      });
    });
  }

  /**
   * Import data from Parquet files using DuckDB's native support
   */
  async importFromParquet(filePath: string): Promise<void> {
    if (!existsSync(filePath)) {
      throw new Error(`Parquet file not found: ${filePath}`);
    }

    // Read entities from Parquet
    const query = `
      SELECT 
        id,
        name,
        COALESCE(entity_type, 'person') as entity_type,
        description,
        metadata
      FROM read_parquet('${filePath}')
    `;

    const rows = await this.executeQuery(query);
    
    for (const row of rows) {
      const entity: Entity = {
        id: row.id || this.generateId(),
        name: row.name,
        entity_type: row.entity_type || 'person',
        description: row.description,
        metadata: typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata
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

    await this.executeQuery(query, [
      entity.id,
      entity.name,
      entity.entity_type,
      entity.description,
      JSON.stringify(entity.metadata || {}),
      new Date().toISOString()
    ]);

    this.entities.set(entity.id, entity);
    return entity;
  }

  /**
   * Create a new user
   */
  async createUser(username: string, role: User['role'], experienceLevel: User['experience_level'] = 'novice'): Promise<User> {
    const user: User = {
      id: this.generateId(),
      username,
      role,
      experience_level: experienceLevel,
      created_at: new Date()
    };

    const query = `
      INSERT INTO users (id, username, role, experience_level, created_at)
      VALUES (?, ?, ?, ?, ?)
    `;

    await this.executeQuery(query, [
      user.id,
      user.username,
      user.role,
      user.experience_level,
      user.created_at.toISOString()
    ]);

    return user;
  }

  /**
   * Add a personality typing
   */
  async addTyping(typing: Omit<Typing, 'id' | 'created_at'>): Promise<Typing> {
    const fullTyping: Typing = {
      ...typing,
      id: this.generateId(),
      created_at: new Date()
    };

    const query = `
      INSERT INTO typings (id, entity_id, system_name, type_name, confidence, 
                          probability_distribution, user_id, session_id, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;

    await this.executeQuery(query, [
      fullTyping.id,
      fullTyping.entity_id,
      fullTyping.system_name,
      fullTyping.type_name,
      fullTyping.confidence,
      JSON.stringify(fullTyping.probability_distribution || {}),
      fullTyping.user_id,
      fullTyping.session_id,
      fullTyping.created_at.toISOString()
    ]);

    return fullTyping;
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
    this.vectorIndex?.addPoint(embedding, index);
    this.embeddings.set(entityId, embedding);

    // Store in database as blob
    const query = `
      INSERT OR REPLACE INTO entity_embeddings (entity_id, embedding, dimension)
      VALUES (?, ?, ?)
    `;

    const buffer = Buffer.from(embedding.buffer);
    await this.executeQuery(query, [entityId, buffer, embedding.length]);
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

    for (let i = 0; i < searchResult.neighbors.length; i++) {
      const neighborIndex = searchResult.neighbors[i];
      const distance = searchResult.distances[i];
      
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

    const rows = await this.executeQuery(query, params);
    return rows.map(row => ({
      id: row.id,
      name: row.name,
      entity_type: row.entity_type,
      description: row.description,
      metadata: typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata
    }));
  }

  /**
   * Get personality typings for an entity
   */
  async getTypings(entityId: string, systemName?: string): Promise<Typing[]> {
    let query = 'SELECT * FROM typings WHERE entity_id = ?';
    const params = [entityId];

    if (systemName) {
      query += ' AND system_name = ?';
      params.push(systemName);
    }

    query += ' ORDER BY created_at DESC';

    const rows = await this.executeQuery(query, params);
    return rows.map(row => ({
      id: row.id,
      entity_id: row.entity_id,
      system_name: row.system_name,
      type_name: row.type_name,
      confidence: row.confidence,
      probability_distribution: typeof row.probability_distribution === 'string' 
        ? JSON.parse(row.probability_distribution) 
        : row.probability_distribution,
      user_id: row.user_id,
      session_id: row.session_id,
      created_at: new Date(row.created_at)
    }));
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
      },
      {
        name: 'enneagram',
        display_name: 'Enneagram',
        description: '9 personality types based on core motivations',
        types: ['1', '2', '3', '4', '5', '6', '7', '8', '9']
      }
    ];

    for (const system of systems) {
      // Insert system
      await this.executeQuery(`
        INSERT OR REPLACE INTO typing_systems (name, display_name, description)
        VALUES (?, ?, ?)
      `, [system.name, system.display_name, system.description]);

      // Insert types
      for (const typeName of system.types) {
        await this.executeQuery(`
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
    return new Promise((resolve) => {
      this.db.close((err) => {
        if (err) console.error('Error closing database:', err);
        resolve();
      });
    });
  }
}

export default IPDBManagerJS;