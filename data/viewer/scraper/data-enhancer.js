const fs = require('fs').promises;
const path = require('path');
const csvParser = require('csv-parser');

class DataEnhancer {
    constructor(options = {}) {
        this.inputFile = options.inputFile || path.resolve(__dirname, '../../bot_store/pdb_profiles_flat.csv');
        this.outputDir = options.outputDir || path.resolve(__dirname, '../../bot_store');
        this.missingDataThreshold = options.missingDataThreshold || 0.5; // Consider row missing if >50% fields are empty
    }

    async init() {
        console.log('Data Enhancer initialized');
        return true;
    }

    async readCSV(filePath) {
        return new Promise((resolve, reject) => {
            const results = [];
            const stream = require('fs').createReadStream(filePath)
                .pipe(csvParser())
                .on('data', (data) => results.push(data))
                .on('end', () => resolve(results))
                .on('error', reject);
        });
    }

    analyzeDataCompleteness(profiles) {
        const analysis = {
            totalProfiles: profiles.length,
            fieldAnalysis: {},
            missingDataProfiles: [],
            completeProfiles: [],
            incompleteness: {}
        };

        if (profiles.length === 0) return analysis;

        // Get all possible fields
        const allFields = Object.keys(profiles[0]);
        
        // Initialize field analysis
        allFields.forEach(field => {
            analysis.fieldAnalysis[field] = {
                total: profiles.length,
                filled: 0,
                empty: 0,
                fillRate: 0
            };
        });

        // Analyze each profile
        profiles.forEach((profile, index) => {
            let emptyFields = 0;
            let totalFields = 0;

            allFields.forEach(field => {
                totalFields++;
                const value = profile[field];
                
                if (!value || value === 'nan' || value === '' || value === 'null' || value === 'undefined') {
                    emptyFields++;
                    analysis.fieldAnalysis[field].empty++;
                } else {
                    analysis.fieldAnalysis[field].filled++;
                }
            });

            const incompletenessRatio = emptyFields / totalFields;
            analysis.incompleteness[index] = incompletenessRatio;

            if (incompletenessRatio > this.missingDataThreshold) {
                analysis.missingDataProfiles.push({
                    index,
                    profile,
                    incompletenessRatio,
                    emptyFields,
                    totalFields
                });
            } else {
                analysis.completeProfiles.push({
                    index,
                    profile,
                    incompletenessRatio,
                    emptyFields,
                    totalFields
                });
            }
        });

        // Calculate fill rates
        allFields.forEach(field => {
            const analysis_field = analysis.fieldAnalysis[field];
            analysis_field.fillRate = analysis_field.filled / analysis_field.total;
        });

        return analysis;
    }

    generateMBTIFromName(name) {
        // Simple heuristic based on character names (this is very basic)
        const patterns = {
            'INTJ': ['sherlock', 'batman', 'walter', 'tywin', 'hannibal'],
            'ENTP': ['tony', 'iron man', 'joker', 'rick', 'tyrion'],
            'INFP': ['frodo', 'luna', 'ariel', 'wall-e'],
            'ENFP': ['anna', 'rapunzel', 'robin', 'spider'],
            'ISFJ': ['watson', 'samwell', 'molly'],
            'ESFJ': ['hermione', 'catelyn', 'mrs', 'mother'],
            'ISTP': ['arya', 'indiana', 'james bond'],
            'ESTP': ['han solo', 'jack', 'captain']
        };

        const lowerName = name.toLowerCase();
        
        for (const [type, keywords] of Object.entries(patterns)) {
            for (const keyword of keywords) {
                if (lowerName.includes(keyword)) {
                    return type;
                }
            }
        }

        // Default fallback based on name characteristics
        if (lowerName.includes('dr') || lowerName.includes('professor')) {
            return 'INTJ';
        }
        if (lowerName.includes('lord') || lowerName.includes('king') || lowerName.includes('queen')) {
            return 'ENTJ';
        }
        if (lowerName.includes('artist') || lowerName.includes('poet')) {
            return 'INFP';
        }

        return null; // Return null if no pattern matches
    }

    generateSocionicsFromMBTI(mbti) {
        const mbtiToSocionics = {
            'INTJ': 'LII',
            'INFJ': 'EII', 
            'ISTJ': 'LSI',
            'ISFJ': 'ESI',
            'INTP': 'ILI',
            'INFP': 'IEI',
            'ISTP': 'SLI',
            'ISFP': 'SEI',
            'ENTJ': 'LIE',
            'ENFJ': 'EIE',
            'ESTJ': 'LSE',
            'ESFJ': 'ESE',
            'ENTP': 'ILE',
            'ENFP': 'IEE',
            'ESTP': 'SLE',
            'ESFP': 'SEE'
        };

        return mbtiToSocionics[mbti] || null;
    }

    generateDescription(profile) {
        const { name, mbti, category, socionics } = profile;
        
        let description = '';
        
        if (name) {
            description += `Profile for ${name}`;
        }
        
        if (mbti || socionics) {
            description += name ? ', ' : '';
            if (mbti) description += `MBTI type ${mbti}`;
            if (socionics) description += mbti ? ` (${socionics})` : `Socionics type ${socionics}`;
        }
        
        if (category && category !== 'nan' && category !== '') {
            description += (name || mbti || socionics) ? ', ' : '';
            description += `from ${category}`;
        }

        return description || null;
    }

    enhanceProfile(profile) {
        const enhanced = { ...profile };
        let wasEnhanced = false;

        // Try to fill MBTI if missing
        if (!enhanced.mbti || enhanced.mbti === 'nan' || enhanced.mbti === '') {
            const generatedMBTI = this.generateMBTIFromName(enhanced.name || '');
            if (generatedMBTI) {
                enhanced.mbti = generatedMBTI;
                wasEnhanced = true;
            }
        }

        // Try to fill Socionics if missing but MBTI is available
        if ((!enhanced.socionics || enhanced.socionics === 'nan' || enhanced.socionics === '') 
            && enhanced.mbti && enhanced.mbti !== 'nan' && enhanced.mbti !== '') {
            const generatedSocionics = this.generateSocionicsFromMBTI(enhanced.mbti);
            if (generatedSocionics) {
                enhanced.socionics = generatedSocionics;
                wasEnhanced = true;
            }
        }

        // Try to generate description if missing
        if (!enhanced.description || enhanced.description === 'nan' || enhanced.description === '') {
            const generatedDescription = this.generateDescription(enhanced);
            if (generatedDescription) {
                enhanced.description = generatedDescription;
                wasEnhanced = true;
            }
        }

        // Fill bio if missing but description exists
        if ((!enhanced.bio || enhanced.bio === 'nan' || enhanced.bio === '') 
            && enhanced.description && enhanced.description !== 'nan' && enhanced.description !== '') {
            enhanced.bio = enhanced.description;
            wasEnhanced = true;
        }

        return { enhanced, wasEnhanced };
    }

    async enhanceData() {
        try {
            console.log('Reading existing profile data...');
            const profiles = await this.readCSV(this.inputFile);
            
            console.log(`Loaded ${profiles.length} profiles`);
            
            // Analyze current data completeness
            console.log('Analyzing data completeness...');
            const analysis = this.analyzeDataCompleteness(profiles);
            
            console.log('\n=== DATA COMPLETENESS ANALYSIS ===');
            console.log(`Total profiles: ${analysis.totalProfiles}`);
            console.log(`Profiles with >50% missing data: ${analysis.missingDataProfiles.length}`);
            console.log(`Complete profiles: ${analysis.completeProfiles.length}`);
            
            console.log('\nField fill rates:');
            Object.entries(analysis.fieldAnalysis)
                .sort((a, b) => a[1].fillRate - b[1].fillRate)
                .forEach(([field, stats]) => {
                    console.log(`  ${field}: ${(stats.fillRate * 100).toFixed(1)}% (${stats.filled}/${stats.total})`);
                });

            // Enhance profiles
            console.log('\nEnhancing profiles...');
            const enhancedProfiles = [];
            let enhancedCount = 0;

            for (let i = 0; i < profiles.length; i++) {
                const { enhanced, wasEnhanced } = this.enhanceProfile(profiles[i]);
                enhancedProfiles.push(enhanced);
                
                if (wasEnhanced) {
                    enhancedCount++;
                }

                if (i % 100 === 0) {
                    console.log(`  Processed ${i}/${profiles.length} profiles...`);
                }
            }

            console.log(`\nEnhanced ${enhancedCount} profiles with missing data`);

            // Save enhanced data
            const outputFile = path.join(this.outputDir, `pdb_profiles_enhanced_${Date.now()}.json`);
            await fs.writeFile(outputFile, JSON.stringify(enhancedProfiles, null, 2));
            console.log(`Saved enhanced data to: ${outputFile}`);

            // Generate report
            const reportFile = path.join(this.outputDir, `enhancement_report_${Date.now()}.json`);
            const report = {
                originalAnalysis: analysis,
                enhancedCount,
                totalProfiles: profiles.length,
                timestamp: new Date().toISOString()
            };
            await fs.writeFile(reportFile, JSON.stringify(report, null, 2));
            console.log(`Saved enhancement report to: ${reportFile}`);

            return {
                originalProfiles: profiles,
                enhancedProfiles,
                analysis,
                enhancedCount
            };

        } catch (error) {
            console.error('Error enhancing data:', error);
            throw error;
        }
    }

    async findMissingProfiles() {
        try {
            const profiles = await this.readCSV(this.inputFile);
            const analysis = this.analyzeDataCompleteness(profiles);

            console.log('\n=== PROFILES WITH MISSING DATA ===');
            
            // Sort by incompleteness ratio (most incomplete first)
            analysis.missingDataProfiles
                .sort((a, b) => b.incompletenessRatio - a.incompletenessRatio)
                .slice(0, 20) // Show top 20 most incomplete
                .forEach((item, index) => {
                    const profile = item.profile;
                    console.log(`\n${index + 1}. ${profile.name || 'No name'} (${(item.incompletenessRatio * 100).toFixed(1)}% missing)`);
                    console.log(`   CID: ${profile.cid}`);
                    console.log(`   MBTI: ${profile.mbti || 'missing'}`);
                    console.log(`   Socionics: ${profile.socionics || 'missing'}`);
                    console.log(`   Description: ${profile.description ? 'present' : 'missing'}`);
                });

            return analysis.missingDataProfiles;

        } catch (error) {
            console.error('Error finding missing profiles:', error);
            throw error;
        }
    }

    async close() {
        console.log('Data Enhancer closed');
    }
}

module.exports = DataEnhancer;