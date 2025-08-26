-- ===================================================================
-- IPDB Database Schema for Socionics Research
-- Version: 1.0
-- Purpose: Comprehensive schema for user ratings and personality typings
-- ===================================================================

-- Create database if it doesn't exist
-- CREATE DATABASE IF NOT EXISTS socionics_research;
-- USE socionics_research;

-- ===================================================================
-- CORE ENTITY TABLES
-- ===================================================================

-- Entities that can be typed (people, fictional characters, etc.)
CREATE TABLE entities (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    entity_type ENUM('person', 'fictional_character', 'public_figure') NOT NULL,
    source VARCHAR(100), -- e.g., 'pdb', 'manual_entry', 'import'
    external_id VARCHAR(100), -- For linking to external databases like PDB
    external_source VARCHAR(50), -- e.g., 'personality_database'
    metadata JSON, -- Flexible metadata storage
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_entity_type (entity_type),
    INDEX idx_source (source),
    INDEX idx_external_id (external_id, external_source)
);

-- User accounts for raters/annotators
CREATE TABLE users (
    id UUID PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE,
    display_name VARCHAR(100),
    role ENUM('annotator', 'panel_rater', 'adjudicator', 'admin') NOT NULL,
    experience_level ENUM('novice', 'intermediate', 'expert') NOT NULL DEFAULT 'novice',
    qualifications JSON, -- Store certifications, training records, etc.
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_role (role),
    INDEX idx_active (is_active)
);

-- ===================================================================
-- PERSONALITY TYPING SYSTEMS
-- ===================================================================

-- Supported personality typing systems
CREATE TABLE personality_systems (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50) UNIQUE NOT NULL, -- e.g., 'socionics', 'mbti', 'big5', 'enneagram'
    display_name VARCHAR(100) NOT NULL,
    description TEXT,
    version VARCHAR(20) DEFAULT '1.0',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Types within each system (e.g., ILE, INTJ, etc.)
CREATE TABLE personality_types (
    id INT PRIMARY KEY AUTO_INCREMENT,
    system_id INT NOT NULL,
    code VARCHAR(20) NOT NULL, -- e.g., 'ILE', 'INTJ', 'RCOEI'
    name VARCHAR(100), -- Full name if different from code
    description TEXT,
    
    FOREIGN KEY (system_id) REFERENCES personality_systems(id),
    UNIQUE KEY unique_type_per_system (system_id, code),
    INDEX idx_system (system_id)
);

-- Functions within personality systems (for detailed analysis)
CREATE TABLE personality_functions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    system_id INT NOT NULL,
    code VARCHAR(10) NOT NULL, -- e.g., 'Ne', 'Ti', 'Fe'
    name VARCHAR(50),
    description TEXT,
    function_order TINYINT, -- 1=primary, 2=secondary, etc.
    
    FOREIGN KEY (system_id) REFERENCES personality_systems(id),
    UNIQUE KEY unique_function_per_system (system_id, code),
    INDEX idx_system (system_id)
);

-- ===================================================================
-- RATING SESSIONS AND METHODOLOGY
-- ===================================================================

-- Rating sessions (groups of related ratings)
CREATE TABLE rating_sessions (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    methodology ENUM('structured_interview', 'video_analysis', 'text_analysis', 'composite_review') NOT NULL,
    session_type ENUM('individual', 'panel', 'consensus') NOT NULL,
    created_by UUID NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    status ENUM('active', 'completed', 'cancelled') DEFAULT 'active',
    metadata JSON, -- Session-specific parameters
    
    FOREIGN KEY (created_by) REFERENCES users(id),
    INDEX idx_created_by (created_by),
    INDEX idx_status (status),
    INDEX idx_methodology (methodology)
);

-- Entities included in rating sessions
CREATE TABLE session_entities (
    id UUID PRIMARY KEY,
    session_id UUID NOT NULL,
    entity_id UUID NOT NULL,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    added_by UUID NOT NULL,
    
    FOREIGN KEY (session_id) REFERENCES rating_sessions(id) ON DELETE CASCADE,
    FOREIGN KEY (entity_id) REFERENCES entities(id),
    FOREIGN KEY (added_by) REFERENCES users(id),
    UNIQUE KEY unique_entity_per_session (session_id, entity_id),
    INDEX idx_session (session_id),
    INDEX idx_entity (entity_id)
);

-- ===================================================================
-- TYPING JUDGMENTS AND RATINGS
-- ===================================================================

-- Main typing judgments table
CREATE TABLE typing_judgments (
    id UUID PRIMARY KEY,
    entity_id UUID NOT NULL,
    rater_id UUID NOT NULL,
    session_id UUID,
    system_id INT NOT NULL,
    type_id INT, -- Final type assignment (can be NULL during deliberation)
    confidence DECIMAL(3,2) CHECK (confidence >= 0 AND confidence <= 1),
    method VARCHAR(100), -- e.g., 'structured_interview_v1', 'video_analysis_v2'
    notes TEXT,
    rationale TEXT, -- Detailed reasoning for the typing
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (entity_id) REFERENCES entities(id),
    FOREIGN KEY (rater_id) REFERENCES users(id),
    FOREIGN KEY (session_id) REFERENCES rating_sessions(id),
    FOREIGN KEY (system_id) REFERENCES personality_systems(id),
    FOREIGN KEY (type_id) REFERENCES personality_types(id),
    
    INDEX idx_entity_rater (entity_id, rater_id),
    INDEX idx_session (session_id),
    INDEX idx_system (system_id),
    INDEX idx_created_at (created_at)
);

-- Probability distributions across all types (for when raters give distributions instead of single types)
CREATE TABLE type_probability_distributions (
    id UUID PRIMARY KEY,
    judgment_id UUID NOT NULL,
    type_id INT NOT NULL,
    probability DECIMAL(5,4) NOT NULL CHECK (probability >= 0 AND probability <= 1),
    
    FOREIGN KEY (judgment_id) REFERENCES typing_judgments(id) ON DELETE CASCADE,
    FOREIGN KEY (type_id) REFERENCES personality_types(id),
    UNIQUE KEY unique_type_per_judgment (judgment_id, type_id),
    INDEX idx_judgment (judgment_id)
);

-- Function confidence scores (for detailed Socionics analysis)
CREATE TABLE function_confidence_scores (
    id UUID PRIMARY KEY,
    judgment_id UUID NOT NULL,
    function_id INT NOT NULL,
    confidence DECIMAL(3,2) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    
    FOREIGN KEY (judgment_id) REFERENCES typing_judgments(id) ON DELETE CASCADE,
    FOREIGN KEY (function_id) REFERENCES personality_functions(id),
    UNIQUE KEY unique_function_per_judgment (judgment_id, function_id),
    INDEX idx_judgment (judgment_id)
);

-- ===================================================================
-- CONSENSUS AND INTER-RATER RELIABILITY
-- ===================================================================

-- Consensus meetings and adjudication
CREATE TABLE consensus_sessions (
    id UUID PRIMARY KEY,
    entity_id UUID NOT NULL,
    system_id INT NOT NULL,
    session_id UUID, -- Optional link to parent rating session
    moderator_id UUID NOT NULL,
    status ENUM('scheduled', 'in_progress', 'completed') DEFAULT 'scheduled',
    scheduled_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    outcome ENUM('consensus_reached', 'majority_decision', 'no_consensus') NULL,
    final_type_id INT, -- Agreed-upon type
    final_confidence DECIMAL(3,2),
    notes TEXT,
    
    FOREIGN KEY (entity_id) REFERENCES entities(id),
    FOREIGN KEY (system_id) REFERENCES personality_systems(id),
    FOREIGN KEY (session_id) REFERENCES rating_sessions(id),
    FOREIGN KEY (moderator_id) REFERENCES users(id),
    FOREIGN KEY (final_type_id) REFERENCES personality_types(id),
    
    INDEX idx_entity_system (entity_id, system_id),
    INDEX idx_status (status)
);

-- Participants in consensus sessions
CREATE TABLE consensus_participants (
    id UUID PRIMARY KEY,
    consensus_session_id UUID NOT NULL,
    participant_id UUID NOT NULL,
    initial_judgment_id UUID, -- Link to their original judgment
    final_vote INT, -- Their final vote after discussion
    
    FOREIGN KEY (consensus_session_id) REFERENCES consensus_sessions(id) ON DELETE CASCADE,
    FOREIGN KEY (participant_id) REFERENCES users(id),
    FOREIGN KEY (initial_judgment_id) REFERENCES typing_judgments(id),
    FOREIGN KEY (final_vote) REFERENCES personality_types(id),
    UNIQUE KEY unique_participant_per_session (consensus_session_id, participant_id)
);

-- Inter-rater reliability metrics
CREATE TABLE reliability_metrics (
    id UUID PRIMARY KEY,
    metric_name VARCHAR(50) NOT NULL, -- e.g., 'krippendorff_alpha', 'fleiss_kappa', 'icc'
    entity_id UUID,
    system_id INT,
    session_id UUID,
    calculated_date DATE NOT NULL,
    metric_value DECIMAL(5,4),
    sample_size INT,
    confidence_interval_lower DECIMAL(5,4),
    confidence_interval_upper DECIMAL(5,4),
    calculation_method TEXT,
    metadata JSON,
    
    FOREIGN KEY (entity_id) REFERENCES entities(id),
    FOREIGN KEY (system_id) REFERENCES personality_systems(id),
    FOREIGN KEY (session_id) REFERENCES rating_sessions(id),
    
    INDEX idx_metric_date (metric_name, calculated_date),
    INDEX idx_entity_system (entity_id, system_id)
);

-- ===================================================================
-- ANNOTATIONS AND BEHAVIORAL INDICATORS
-- ===================================================================

-- Behavioral indicators and coding schemes
CREATE TABLE behavioral_indicators (
    id INT PRIMARY KEY AUTO_INCREMENT,
    code VARCHAR(50) UNIQUE NOT NULL, -- e.g., 'INT_INTERRUPTION_RATE'
    name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(100), -- e.g., 'discourse', 'prosody', 'lexical'
    data_type ENUM('binary', 'ordinal', 'continuous') NOT NULL,
    scale_min DECIMAL(10,4),
    scale_max DECIMAL(10,4),
    scale_labels JSON, -- For ordinal scales
    is_active BOOLEAN DEFAULT TRUE,
    version VARCHAR(20) DEFAULT '1.0'
);

-- Annotations linking entities to behavioral indicators
CREATE TABLE behavioral_annotations (
    id UUID PRIMARY KEY,
    entity_id UUID NOT NULL,
    indicator_id INT NOT NULL,
    rater_id UUID NOT NULL,
    session_id UUID,
    value DECIMAL(10,4), -- Numeric value
    confidence DECIMAL(3,2) CHECK (confidence >= 0 AND confidence <= 1),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (entity_id) REFERENCES entities(id),
    FOREIGN KEY (indicator_id) REFERENCES behavioral_indicators(id),
    FOREIGN KEY (rater_id) REFERENCES users(id),
    FOREIGN KEY (session_id) REFERENCES rating_sessions(id),
    
    INDEX idx_entity_indicator (entity_id, indicator_id),
    INDEX idx_rater_session (rater_id, session_id)
);

-- ===================================================================
-- DATA INTEGRATION WITH EXISTING PARQUET FILES
-- ===================================================================

-- Store references to external data sources like parquet files
CREATE TABLE data_sources (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    source_type ENUM('parquet', 'json', 'csv', 'api') NOT NULL,
    file_path VARCHAR(500),
    connection_string TEXT,
    description TEXT,
    schema_version VARCHAR(20),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    
    INDEX idx_source_type (source_type),
    INDEX idx_active (is_active)
);

-- Link entities to external data records
CREATE TABLE entity_data_mappings (
    id UUID PRIMARY KEY,
    entity_id UUID NOT NULL,
    data_source_id UUID NOT NULL,
    external_key VARCHAR(255) NOT NULL, -- e.g., 'cid' for parquet files
    external_value VARCHAR(255) NOT NULL, -- The actual CID or external identifier
    mapping_confidence DECIMAL(3,2) DEFAULT 1.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (entity_id) REFERENCES entities(id),
    FOREIGN KEY (data_source_id) REFERENCES data_sources(id),
    UNIQUE KEY unique_mapping (data_source_id, external_key, external_value),
    INDEX idx_entity (entity_id),
    INDEX idx_external (external_key, external_value)
);

-- ===================================================================
-- VIEWS FOR COMMON QUERIES
-- ===================================================================

-- View for entity typing summary
CREATE VIEW entity_typing_summary AS
SELECT 
    e.id as entity_id,
    e.name as entity_name,
    e.entity_type,
    ps.name as personality_system,
    pt.code as personality_type,
    COUNT(tj.id) as total_judgments,
    AVG(tj.confidence) as average_confidence,
    MAX(tj.updated_at) as last_typed
FROM entities e
LEFT JOIN typing_judgments tj ON e.id = tj.entity_id
LEFT JOIN personality_systems ps ON tj.system_id = ps.id
LEFT JOIN personality_types pt ON tj.type_id = pt.id
GROUP BY e.id, e.name, e.entity_type, ps.name, pt.code;

-- View for rater performance metrics
CREATE VIEW rater_performance_summary AS
SELECT 
    u.id as rater_id,
    u.username,
    u.display_name,
    u.role,
    COUNT(tj.id) as total_judgments,
    AVG(tj.confidence) as average_confidence,
    COUNT(DISTINCT tj.entity_id) as entities_typed,
    COUNT(DISTINCT tj.session_id) as sessions_participated,
    MIN(tj.created_at) as first_judgment,
    MAX(tj.created_at) as last_judgment
FROM users u
LEFT JOIN typing_judgments tj ON u.id = tj.rater_id
WHERE u.is_active = TRUE
GROUP BY u.id, u.username, u.display_name, u.role;

-- ===================================================================
-- INITIAL DATA SETUP
-- ===================================================================

-- Insert basic personality systems
INSERT INTO personality_systems (name, display_name, description) VALUES
('socionics', 'Socionics', 'Information Metabolism theory of personality types'),
('mbti', 'MBTI', 'Myers-Briggs Type Indicator'),
('big5', 'Big Five', 'Five-factor model of personality'),
('enneagram', 'Enneagram', 'Nine personality types system');

-- Insert common Socionics types
INSERT INTO personality_types (system_id, code, name) VALUES
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'ILE', 'Intuitive Logical Extravert (Don Quixote)'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'SEI', 'Sensory Ethical Introvert (Dumas)'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'ESE', 'Ethical Sensory Extravert (Hugo)'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'LII', 'Logical Intuitive Introvert (Robespierre)'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'EIE', 'Ethical Intuitive Extravert (Hamlet)'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'LSI', 'Logical Sensory Introvert (Maxim)'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'SLE', 'Sensory Logical Extravert (Zhukov)'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'IEI', 'Intuitive Ethical Introvert (Yesenin)'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'SEE', 'Sensory Ethical Extravert (Napoleon)'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'ILI', 'Intuitive Logical Introvert (Balzac)'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'LIE', 'Logical Intuitive Extravert (Jack)'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'ESI', 'Ethical Sensory Introvert (Dreiser)'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'LSE', 'Logical Sensory Extravert (Stirlitz)'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'EII', 'Ethical Intuitive Introvert (Dostoyevsky)'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'IEE', 'Intuitive Ethical Extravert (Huxley)'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'SLI', 'Sensory Logical Introvert (Gabin)');

-- Insert MBTI types
INSERT INTO personality_types (system_id, code, name) VALUES
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'INTJ', 'Architect'),
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'INTP', 'Thinker'),
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'ENTJ', 'Commander'),
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'ENTP', 'Debater'),
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'INFJ', 'Advocate'),
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'INFP', 'Mediator'),
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'ENFJ', 'Protagonist'),
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'ENFP', 'Campaigner'),
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'ISTJ', 'Logistician'),
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'ISFJ', 'Protector'),
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'ESTJ', 'Executive'),
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'ESFJ', 'Consul'),
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'ISTP', 'Virtuoso'),
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'ISFP', 'Adventurer'),
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'ESTP', 'Entrepreneur'),
((SELECT id FROM personality_systems WHERE name = 'mbti'), 'ESFP', 'Entertainer');

-- Insert Socionics functions
INSERT INTO personality_functions (system_id, code, name) VALUES
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'Ne', 'Extraverted Intuition'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'Ni', 'Introverted Intuition'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'Se', 'Extraverted Sensing'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'Si', 'Introverted Sensing'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'Te', 'Extraverted Thinking'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'Ti', 'Introverted Thinking'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'Fe', 'Extraverted Feeling'),
((SELECT id FROM personality_systems WHERE name = 'socionics'), 'Fi', 'Introverted Feeling');

-- Insert some common behavioral indicators
INSERT INTO behavioral_indicators (code, name, description, category, data_type, scale_min, scale_max) VALUES
('INT_INTERRUPTION_RATE', 'Interruption Rate', 'Rate of interrupting other speakers', 'discourse', 'continuous', 0, 10),
('SPEECH_RATE', 'Speech Rate', 'Words per minute in natural speech', 'prosody', 'continuous', 50, 300),
('ABSTRACT_LANGUAGE', 'Abstract Language Use', 'Frequency of abstract vs concrete language', 'lexical', 'ordinal', 1, 5),
('TOPIC_SHIFT_FREQ', 'Topic Shift Frequency', 'Rate of changing conversation topics', 'discourse', 'continuous', 0, 20),
('EMOTIONAL_EXPRESSION', 'Emotional Expression', 'Level of emotional expressiveness', 'nonverbal', 'ordinal', 1, 5);