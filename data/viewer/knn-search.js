// KNN search functionality for personality database viewer
// Simplified version of compass vectors_knn.js

class KNNSearch {
    constructor() {
        this.vectors = new Map(); // cid -> Float32Array
        this.profiles = new Map(); // cid -> profile data
        this.isLoaded = false;
    }

    // Load vectors from the server API
    async loadVectors() {
        try {
            console.log('Loading vectors for KNN search...');
            
            // Try to get vectors from dataset endpoint
            const vectorResponse = await fetch('/dataset/pdb_profile_vectors.parquet');
            if (!vectorResponse.ok) {
                throw new Error('Vectors not available');
            }
            
            // For now, we'll create a simpler API endpoint to get vectors as JSON
            const response = await fetch('/api/data/vectors');
            if (!response.ok) {
                throw new Error(`Failed to load vectors: ${response.status}`);
            }
            
            const data = await response.json();
            if (data.error) {
                throw new Error(data.error);
            }
            
            // Process the vector data
            for (const item of data.vectors) {
                if (item.cid && item.vector && Array.isArray(item.vector)) {
                    this.vectors.set(item.cid, new Float32Array(item.vector));
                }
            }
            
            this.isLoaded = true;
            console.log(`Loaded ${this.vectors.size} vectors for KNN search`);
            return true;
            
        } catch (error) {
            console.warn('KNN vectors not available:', error.message);
            this.isLoaded = false;
            return false;
        }
    }

    // Set profile data for KNN search
    setProfiles(profiles) {
        this.profiles.clear();
        for (const profile of profiles) {
            if (profile.cid) {
                this.profiles.set(profile.cid, profile);
            }
        }
    }

    // Cosine similarity between two vectors
    cosineSimilarity(a, b) {
        if (a.length !== b.length) return 0;
        
        let dot = 0, normA = 0, normB = 0;
        for (let i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        
        const denom = Math.sqrt(normA) * Math.sqrt(normB);
        return denom > 0 ? dot / denom : 0;
    }

    // Find k nearest neighbors to a query vector
    findNearest(queryVector, k = 10) {
        if (!this.isLoaded || this.vectors.size === 0) {
            return [];
        }

        const similarities = [];
        
        for (const [cid, vector] of this.vectors.entries()) {
            const similarity = this.cosineSimilarity(queryVector, vector);
            similarities.push({ cid, similarity });
        }

        // Sort by similarity (descending) and take top k
        similarities.sort((a, b) => b.similarity - a.similarity);
        return similarities.slice(0, k);
    }

    // Search by text using vector similarity
    async searchByText(query, k = 10) {
        // For now, this is a placeholder - in a real implementation, 
        // we would need to encode the text query into a vector
        // using the same model that generated the profile vectors
        console.log('Vector-based text search not yet implemented');
        return [];
    }

    // Find similar profiles to a given profile
    findSimilarProfiles(cid, k = 10) {
        if (!this.isLoaded || !this.vectors.has(cid)) {
            return [];
        }

        const queryVector = this.vectors.get(cid);
        const similar = this.findNearest(queryVector, k + 1); // +1 to exclude self
        
        // Remove the query profile itself and add profile data
        const results = [];
        for (const item of similar) {
            if (item.cid !== cid && this.profiles.has(item.cid)) {
                results.push({
                    ...this.profiles.get(item.cid),
                    similarity: item.similarity
                });
            }
        }
        
        return results.slice(0, k);
    }

    // Check if KNN search is available
    isAvailable() {
        return this.isLoaded && this.vectors.size > 0;
    }
}

export const knnSearch = new KNNSearch();