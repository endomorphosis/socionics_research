#!/usr/bin/env python3
"""
Generate a candidate pool of 64,000 questions for socionics survey with embeddings.

This script creates variations of existing survey questions, generates embeddings for each,
and saves them to a parquet file. Includes K-means clustering for question decimation
and uniform distribution analysis.
"""

import json
import re
import random
import itertools
from pathlib import Path
from typing import List, Dict, Tuple, Any
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import sys
import os

# Add bot src to path to use existing embedding infrastructure
sys.path.append(str(Path(__file__).parent.parent / "bot" / "src"))
from bot.pdb_embed_search import embed_texts, _get_embedder

# Socionics axes and their properties
AXES = {
    "EI": {"pos": "E", "neg": "I", "label": "Extraversion vs. Introversion"},
    "NS": {"pos": "N", "neg": "S", "label": "Intuition vs. Sensing"},
    "TF": {"pos": "T", "neg": "F", "label": "Logic vs. Ethics"},
    "JP": {"pos": "J", "neg": "P", "label": "Rational vs. Irrational"},
    "RH": {"pos": "Reductionist", "neg": "Holist", "label": "Reductionist vs. Holist"},
    "IT": {"pos": "Identifier", "neg": "Transformer", "label": "Identifier vs. Transformer"},
    "SF": {"pos": "Stubborn", "neg": "Flexible", "label": "Stubborn vs. Flexible"},
    "ST": {"pos": "Space-locked", "neg": "Time-locked", "label": "Space-locked vs. Time-locked"},
}

# Base question templates for each axis
QUESTION_TEMPLATES = {
    "EI": [
        "After {time_period}, how do you usually recharge?",
        "In a new {social_context}, what happens first?",
        "Your thinking style is more often {thinking_pattern}",
        "When {unexpected_event} happens, you tend to {response_pattern}",
        "Social gatherings feel {feeling_descriptor} to you",
        "You prefer to {communication_style} when solving problems",
        "Energy comes from {energy_source} activities",
        "In meetings, you typically {participation_style}",
        "Your ideal weekend involves {weekend_activity}",
        "When stressed, you {stress_response}",
    ],
    "NS": [
        "When starting something new, you focus more on {focus_type}",
        "You usually notice first {attention_focus}",
        "You're drawn to tasks that are {task_nature}",
        "In problem-solving, you rely more on {problem_approach}",
        "Regarding change, you {change_attitude}",
        "Your conversations tend toward {conversation_topic}",
        "You prefer {work_style} approaches",
        "Details are {detail_preference} in your work",
        "Innovation means {innovation_concept} to you",
        "Learning happens best through {learning_method}",
    ],
    "TF": [
        "When deciding, your default is {decision_basis}",
        "You prefer to uphold {value_system}",
        "Feedback style you gravitate to {feedback_approach}",
        "You most admire {admiration_target}",
        "In conflict, you prioritise {conflict_priority}",
        "Good leadership involves {leadership_style}",
        "Criticism should be {criticism_style}",
        "Team success depends on {team_factor}",
        "Arguments are won by {argument_style}",
        "Fairness means {fairness_concept}",
    ],
    "JP": [
        "Your work style is best with {work_structure}",
        "You prefer plans that are {plan_flexibility}",
        "Deadlines are {deadline_perception}",
        "Your ideal project has {project_structure}",
        "Spontaneity is {spontaneity_view}",
        "You function best with {routine_preference}",
        "Change should be {change_management}",
        "Decisions are made {decision_timing}",
        "Organization means {organization_concept}",
        "Time is {time_perception}",
    ],
    "RH": [
        "When analyzing complex systems, you {analysis_approach}",
        "Problems are best understood by {understanding_method}",
        "You view reality as {reality_concept}",
        "Your thinking naturally moves toward {thinking_direction}",
        "Information processing works by {processing_method}",
        "Connections between ideas are {connection_view}",
        "Truth emerges through {truth_method}",
        "Knowledge is built by {knowledge_building}",
        "Patterns reveal {pattern_meaning}",
        "Understanding comes from {understanding_source}",
    ],
    "IT": [
        "With external standards, you {standard_response}",
        "New concepts are {concept_handling}",
        "Your identity is {identity_basis}",
        "Learning involves {learning_process}",
        "Authority figures represent {authority_view}",
        "Personal growth happens through {growth_method}",
        "Values should be {values_treatment}",
        "Knowledge transforms by {transformation_method}",
        "External frameworks are {framework_approach}",
        "Adaptation means {adaptation_concept}",
    ],
    "SF": [
        "In disagreements, you're more {disagreement_style}",
        "Your approach to conflict is {conflict_approach}",
        "When challenged, you tend to {challenge_response}",
        "Compromise involves {compromise_view}",
        "Standing your ground means {ground_standing}",
        "Flexibility is {flexibility_concept}",
        "Boundaries should be {boundary_management}",
        "Relationships require {relationship_needs}",
        "Personal principles are {principle_flexibility}",
        "Adaptation in groups involves {group_adaptation}",
    ],
    "ST": [
        "You resonate more with {resonance_type}",
        "Your orientation is toward {orientation_focus}",
        "Experience is shaped by {experience_dimension}",
        "Movement through life follows {life_navigation}",
        "Reality is organized by {reality_organization}",
        "Perspective is framed by {perspective_frame}",
        "Flow happens through {flow_medium}",
        "Context is defined by {context_definition}",
        "Presence is felt in {presence_dimension}",
        "Awareness extends through {awareness_extension}",
    ]
}

# Variable options for template substitution
TEMPLATE_VARS = {
    # EI variables
    "time_period": ["a full day", "a long week", "intense meetings", "social activities", "work periods", "challenging tasks"],
    "social_context": ["group", "party", "meeting", "classroom", "team", "community"],
    "thinking_pattern": ["out loud", "internally", "collaboratively", "independently", "visually", "verbally"],
    "unexpected_event": ["plans change suddenly", "crisis emerges", "opportunities arise", "conflicts develop", "deadlines shift", "priorities change"],
    "response_pattern": ["engage immediately", "step back first", "seek input", "reflect privately", "take action", "analyze carefully"],
    "feeling_descriptor": ["energizing", "draining", "exciting", "overwhelming", "inspiring", "tiring"],
    "communication_style": ["discuss openly", "think first", "brainstorm aloud", "process internally", "seek feedback", "work alone"],
    "energy_source": ["social", "solitary", "interactive", "reflective", "external", "internal"],
    "participation_style": ["speak up quickly", "listen first", "lead discussion", "contribute carefully", "share freely", "observe mainly"],
    "weekend_activity": ["social gathering", "quiet time", "group activities", "solo projects", "community events", "personal space"],
    "stress_response": ["talk it out", "need alone time", "seek support", "process privately", "stay active", "withdraw temporarily"],
    
    # NS variables
    "focus_type": ["possibilities", "concrete facts", "big picture", "specific steps", "potential", "current reality"],
    "attention_focus": ["patterns and trends", "details and specifics", "implications", "tangible cues", "connections", "immediate facts"],
    "task_nature": ["open-ended", "hands-on", "creative", "practical", "theoretical", "applied"],
    "problem_approach": ["imagination", "experience", "intuition", "data", "innovation", "proven methods"],
    "change_attitude": ["seek novelty", "prefer stability", "embrace innovation", "value tradition", "pursue possibilities", "maintain routines"],
    "conversation_topic": ["abstract ideas", "practical matters", "future possibilities", "current realities", "theoretical concepts", "concrete examples"],
    "work_style": ["innovative", "systematic", "experimental", "methodical", "creative", "structured"],
    "detail_preference": ["secondary to vision", "essential foundation", "interesting if relevant", "crucial for accuracy", "useful for context", "necessary for completion"],
    "innovation_concept": ["creating possibilities", "improving existing", "exploring unknown", "refining current", "imagining different", "perfecting present"],
    "learning_method": ["conceptual frameworks", "hands-on practice", "theoretical models", "real examples", "abstract principles", "concrete applications"],
    
    # TF variables
    "decision_basis": ["logical analysis", "value considerations", "objective criteria", "people impact", "rational evaluation", "emotional intelligence"],
    "value_system": ["consistent principles", "flexible compassion", "universal rules", "contextual care", "logical standards", "human considerations"],
    "feedback_approach": ["direct critique", "supportive framing", "honest assessment", "encouraging guidance", "factual analysis", "empathetic delivery"],
    "admiration_target": ["competence", "compassion", "expertise", "kindness", "achievement", "harmony"],
    "conflict_priority": ["truth and accuracy", "relationships", "being right", "maintaining peace", "logical resolution", "emotional wellbeing"],
    "leadership_style": ["clear direction", "team harmony", "efficient systems", "people development", "goal achievement", "relationship building"],
    "criticism_style": ["constructive and direct", "gentle and supportive", "focused on improvement", "mindful of feelings", "objective evaluation", "caring guidance"],
    "team_factor": ["clear processes", "good relationships", "defined roles", "mutual support", "efficient systems", "emotional safety"],
    "argument_style": ["logical reasoning", "understanding perspectives", "factual evidence", "empathetic dialogue", "rational analysis", "collaborative discussion"],
    "fairness_concept": ["equal treatment", "individual consideration", "consistent rules", "contextual flexibility", "objective standards", "personal circumstances"],
    
    # JP variables
    "work_structure": ["clear schedules", "flexible timelines", "defined milestones", "adaptive flow", "organized plans", "spontaneous rhythm"],
    "plan_flexibility": ["detailed and structured", "loose and adaptable", "comprehensive and fixed", "general and flexible", "thorough and complete", "open and evolving"],
    "deadline_perception": ["helpful motivation", "stressful pressure", "necessary structure", "limiting constraints", "useful boundaries", "artificial restrictions"],
    "project_structure": ["clear phases", "organic development", "defined steps", "emergent process", "systematic approach", "intuitive flow"],
    "spontaneity_view": ["disruptive to plans", "source of energy", "creates inefficiency", "brings excitement", "causes confusion", "offers opportunity"],
    "routine_preference": ["predictable structure", "flexible options", "organized systems", "varied approaches", "stable patterns", "changing rhythms"],
    "change_management": ["planned carefully", "embraced naturally", "implemented systematically", "allowed to emerge", "controlled tightly", "welcomed openly"],
    "decision_timing": ["after thorough planning", "when opportunities arise", "with sufficient analysis", "based on current needs", "following careful consideration", "responding to circumstances"],
    "organization_concept": ["systematic arrangement", "flexible accessibility", "structured efficiency", "adaptive functionality", "ordered clarity", "responsive utility"],
    "time_perception": ["resource to manage", "flow to experience", "schedule to follow", "opportunity to use", "structure to maintain", "rhythm to feel"],
    
    # RH variables
    "analysis_approach": ["break into components", "view as interconnected whole", "examine parts separately", "see holistic patterns", "reduce to elements", "maintain system view"],
    "understanding_method": ["detailed analysis", "holistic integration", "systematic breakdown", "pattern recognition", "component study", "wholistic synthesis"],
    "reality_concept": ["collection of parts", "unified system", "analyzable components", "integrated whole", "separate elements", "connected network"],
    "thinking_direction": ["specific details", "overarching patterns", "individual facts", "systemic relationships", "discrete items", "unified understanding"],
    "processing_method": ["sequential analysis", "parallel integration", "step-by-step logic", "simultaneous synthesis", "linear reasoning", "circular comprehension"],
    "connection_view": ["built from parts", "inherently present", "constructed logically", "naturally emerging", "analytically derived", "intuitively perceived"],
    "truth_method": ["careful examination", "holistic insight", "systematic investigation", "integrated understanding", "detailed research", "comprehensive awareness"],
    "knowledge_building": ["accumulating facts", "weaving relationships", "gathering evidence", "seeing connections", "collecting data", "understanding systems"],
    "pattern_meaning": ["underlying structure", "surface connections", "hidden organization", "obvious relationships", "deep principles", "apparent links"],
    "understanding_source": ["analytical thinking", "intuitive grasping", "logical deduction", "systemic awareness", "systematic study", "holistic perception"],
    
    # IT variables
    "standard_response": ["adopt for orientation", "transform personally", "use as guidelines", "adapt to self", "follow systematically", "modify individually"],
    "concept_handling": ["integrated unchanged", "transformed through personal lens", "accepted as given", "filtered through experience", "adopted directly", "personalized significantly"],
    "identity_basis": ["external standards", "internal transformation", "social roles", "personal development", "given frameworks", "individual interpretation"],
    "learning_process": ["accepting information", "transforming knowledge", "following curriculum", "creating understanding", "receiving instruction", "developing insight"],
    "authority_view": ["legitimate guidance", "input to transform", "proper direction", "material to process", "established wisdom", "perspective to consider"],
    "growth_method": ["following models", "personal transformation", "meeting standards", "individual development", "external guidance", "internal evolution"],
    "values_treatment": ["accepted standards", "personally developed", "social norms", "individual principles", "given frameworks", "created meaning"],
    "transformation_method": ["maintaining essence", "personal evolution", "preserving core", "individual change", "stable identity", "adaptive growth"],
    "framework_approach": ["adopted wholesale", "personalized significantly", "implemented directly", "modified individually", "accepted completely", "transformed creatively"],
    "adaptation_concept": ["fitting in", "personal evolution", "meeting expectations", "individual growth", "conforming appropriately", "developing uniquely"],
    
    # SF variables
    "disagreement_style": ["stubborn and firm", "flexible and adaptive", "persistent in position", "accommodating to others", "maintaining stance", "adjusting approach"],
    "conflict_approach": ["holding ground firmly", "finding compromise", "standing by principles", "seeking accommodation", "maintaining position", "adapting to others"],
    "challenge_response": ["defend position", "consider alternatives", "strengthen resolve", "explore flexibility", "increase resistance", "show adaptability"],
    "compromise_view": ["weakening position", "collaborative solution", "losing ground", "finding balance", "surrendering principles", "creating harmony"],
    "ground_standing": ["unwavering commitment", "strategic flexibility", "principled persistence", "adaptive strength", "firm boundaries", "responsive limits"],
    "flexibility_concept": ["compromise of integrity", "adaptive strength", "loss of direction", "responsive capability", "abandoning principles", "intelligent adjustment"],
    "boundary_management": ["firm and consistent", "flexible and contextual", "clearly defined", "situationally adapted", "strictly maintained", "responsively adjusted"],
    "relationship_needs": ["clear expectations", "flexible understanding", "defined roles", "adaptive interaction", "consistent behavior", "responsive engagement"],
    "principle_flexibility": ["non-negotiable core", "contextually adaptive", "unchanging foundation", "situationally flexible", "absolute standards", "relative guidelines"],
    "group_adaptation": ["maintaining identity", "fitting group needs", "preserving individuality", "serving collective", "standing apart", "blending in"],
    
    # ST variables
    "resonance_type": ["spatial metaphors", "temporal metaphors", "physical analogies", "time-based concepts", "place-oriented thinking", "process-oriented thinking"],
    "orientation_focus": ["spatial coordinates", "temporal flow", "physical location", "time progression", "present position", "ongoing movement"],
    "experience_dimension": ["where you are", "when you are", "spatial context", "temporal context", "location awareness", "timing sensitivity"],
    "life_navigation": ["spatial landmarks", "temporal rhythms", "geographic reference", "chronological sequence", "positional awareness", "temporal flow"],
    "reality_organization": ["spatial structures", "temporal sequences", "place-based order", "time-based order", "locational logic", "chronological logic"],
    "perspective_frame": ["spatial viewpoint", "temporal perspective", "positional stance", "chronological view", "locational awareness", "timing consciousness"],
    "flow_medium": ["space and place", "time and sequence", "physical dimensions", "temporal dimensions", "spatial relationships", "temporal relationships"],
    "context_definition": ["where things happen", "when things happen", "spatial boundaries", "temporal boundaries", "locational frames", "chronological frames"],
    "presence_dimension": ["spatial awareness", "temporal awareness", "here-ness", "now-ness", "positional consciousness", "timing consciousness"],
    "awareness_extension": ["spatial dimensions", "temporal dimensions", "physical space", "time flow", "locational reach", "chronological span"],
}

def generate_question_variations() -> List[Dict[str, Any]]:
    """Generate variations of questions using templates and variables."""
    questions = []
    question_id = 1
    
    for axis in AXES.keys():
        templates = QUESTION_TEMPLATES.get(axis, [])
        
        for template in templates:
            # Find all variable placeholders in the template
            variables = re.findall(r'\{(\w+)\}', template)
            
            # Get all possible combinations of variables for this template
            if variables:
                var_options = []
                for var in variables:
                    if var in TEMPLATE_VARS:
                        var_options.append(TEMPLATE_VARS[var])
                    else:
                        var_options.append([var])  # fallback to original if not found
                
                # Generate combinations
                combinations = list(itertools.product(*var_options))
                
                # Limit combinations per template to avoid exponential explosion
                max_combinations = min(len(combinations), 800)  # Adjust based on target total
                selected_combinations = random.sample(combinations, max_combinations) if len(combinations) > max_combinations else combinations
                
                for combo in selected_combinations:
                    # Substitute variables in template
                    question_text = template
                    for var, value in zip(variables, combo):
                        question_text = question_text.replace(f'{{{var}}}', value)
                    
                    # Create positive and negative response options based on axis
                    axis_info = AXES[axis]
                    
                    questions.append({
                        'question_id': question_id,
                        'axis': axis,
                        'text': question_text,
                        'axis_label': axis_info['label'],
                        'positive_pole': axis_info['pos'],
                        'negative_pole': axis_info['neg'],
                        'template': template,
                        'variables': dict(zip(variables, combo)) if variables else {}
                    })
                    question_id += 1
            else:
                # Template has no variables
                axis_info = AXES[axis]
                questions.append({
                    'question_id': question_id,
                    'axis': axis,
                    'text': template,
                    'axis_label': axis_info['label'],
                    'positive_pole': axis_info['pos'],
                    'negative_pole': axis_info['neg'],
                    'template': template,
                    'variables': {}
                })
                question_id += 1
    
    return questions

def add_semantic_variations(questions: List[Dict[str, Any]], target_count: int = 64000) -> List[Dict[str, Any]]:
    """Add semantic variations to reach target question count."""
    
    # Semantic transformation patterns
    transforms = [
        # Question format variations
        ("You", "I"),
        ("your", "my"),
        ("When you", "When I"),
        ("How do you", "How do I"),
        ("What do you", "What do I"),
        ("Do you", "Do I"),
        
        # Tense variations
        ("prefer", "would prefer"),
        ("feel", "would feel"),
        ("think", "would think"),
        ("are", "would be"),
        ("tend to", "typically"),
        
        # Intensity modifiers - fixed to not have empty strings
        ("usually", "typically"),
        ("generally", "often"),
        ("typically", "frequently"), 
        ("often", "usually"),
        ("frequently", "generally"),
        ("more", "much more"),
        ("less", "much less"),
    ]
    
    # Question starters that prepend to the entire question
    question_starters = [
        "Generally, ",
        "Typically, ",
        "Usually, ",
        "Most often, ",
        "In general, ",
    ]
    
    # Additional question formats
    format_patterns = [
        "Which statement better describes you: {text}",
        "In most situations: {text}",
        "Generally speaking: {text}",
        "When it comes to this area: {text}",
        "Your natural tendency is: {text}",
        "You would say that: {text}",
        "It's more accurate to say: {text}",
        "Your preference tends toward: {text}",
    ]
    
    extended_questions = list(questions)
    current_count = len(extended_questions)
    max_id = max(q['question_id'] for q in questions)
    
    while current_count < target_count:
        # Select random base question
        base_question = random.choice(questions)
        
        # Apply random transformation
        new_text = base_question['text']
        
        # Apply substitution transforms
        if random.random() < 0.5:  # 50% chance to apply substitution
            transform_pair = random.choice(transforms)
            if transform_pair[0] and transform_pair[0] in new_text:
                new_text = new_text.replace(transform_pair[0], transform_pair[1])
        
        # Apply question starter prepend
        if random.random() < 0.2:  # 20% chance to add starter
            starter = random.choice(question_starters)
            new_text = starter + new_text
        
        # Apply format pattern
        if random.random() < 0.2:  # 20% chance to apply format pattern
            pattern = random.choice(format_patterns)
            new_text = pattern.format(text=new_text)
        
        # Create new question
        max_id += 1
        new_question = base_question.copy()
        new_question.update({
            'question_id': max_id,
            'text': new_text,
            'is_variation': True,
            'base_question_id': base_question['question_id']
        })
        
        extended_questions.append(new_question)
        current_count += 1
        
        if current_count % 5000 == 0:
            print(f"Generated {current_count:,} questions...")
    
    return extended_questions[:target_count]

def compute_embeddings(questions: List[Dict[str, Any]]) -> List[List[float]]:
    """Compute embeddings for all questions using the existing embedding system."""
    print("Computing embeddings...")
    question_texts = [q['text'] for q in questions]
    
    # Use existing embedding infrastructure
    embeddings = embed_texts(question_texts)
    print(f"Generated embeddings for {len(embeddings)} questions")
    
    return embeddings

def perform_kmeans_analysis(embeddings: np.ndarray, n_clusters_range: Tuple[int, int] = (50, 200)) -> Dict[str, Any]:
    """Perform K-means clustering analysis to find optimal number of clusters."""
    print("Performing K-means analysis...")
    
    results = {}
    best_k = None
    best_score = -1
    
    # Test different numbers of clusters
    k_values = range(n_clusters_range[0], min(n_clusters_range[1], len(embeddings)//2), 25)
    
    for k in k_values:
        print(f"Testing k={k}...")
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        cluster_labels = kmeans.fit_predict(embeddings)
        
        # Calculate silhouette score
        silhouette = silhouette_score(embeddings, cluster_labels)
        
        results[k] = {
            'silhouette_score': silhouette,
            'inertia': kmeans.inertia_,
            'cluster_sizes': np.bincount(cluster_labels).tolist(),
            'centroids': kmeans.cluster_centers_
        }
        
        if silhouette > best_score:
            best_score = silhouette
            best_k = k
        
        print(f"k={k}: silhouette_score={silhouette:.3f}, inertia={kmeans.inertia_:.2f}")
    
    print(f"Best k: {best_k} (silhouette score: {best_score:.3f})")
    
    return {
        'best_k': best_k,
        'best_score': best_score,
        'all_results': results
    }

def decimate_questions_by_similarity(questions: List[Dict[str, Any]], 
                                   embeddings: np.ndarray, 
                                   target_size: int = 1000,
                                   method: str = 'kmeans') -> Tuple[List[Dict[str, Any]], List[int]]:
    """Decimate questions based on similarity to achieve uniform distribution."""
    print(f"Decimating {len(questions)} questions to {target_size} using {method}...")
    
    if method == 'kmeans':
        # Use K-means clustering
        kmeans = KMeans(n_clusters=target_size, random_state=42, n_init=10)
        cluster_labels = kmeans.fit_predict(embeddings)
        
        # Select one representative from each cluster (closest to centroid)
        selected_indices = []
        for i in range(target_size):
            cluster_mask = cluster_labels == i
            if np.any(cluster_mask):
                cluster_embeddings = embeddings[cluster_mask]
                cluster_indices = np.where(cluster_mask)[0]
                
                # Find closest to centroid
                centroid = kmeans.cluster_centers_[i]
                distances = np.linalg.norm(cluster_embeddings - centroid, axis=1)
                closest_idx = cluster_indices[np.argmin(distances)]
                selected_indices.append(closest_idx)
    
    else:
        raise ValueError(f"Unknown decimation method: {method}")
    
    # Return selected questions and their indices
    selected_questions = [questions[i] for i in selected_indices]
    
    return selected_questions, selected_indices

def analyze_distribution(questions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze the distribution of questions across axes and other dimensions."""
    
    # Count questions by axis
    axis_counts = {}
    for q in questions:
        axis = q['axis']
        axis_counts[axis] = axis_counts.get(axis, 0) + 1
    
    # Count original vs variations
    original_count = sum(1 for q in questions if not q.get('is_variation', False))
    variation_count = len(questions) - original_count
    
    # Calculate uniformity score (lower is more uniform)
    axis_values = list(axis_counts.values())
    mean_count = np.mean(axis_values)
    uniformity_score = np.std(axis_values) / mean_count if mean_count > 0 else 0
    
    return {
        'total_questions': len(questions),
        'axis_distribution': axis_counts,
        'original_questions': original_count,
        'variation_questions': variation_count,
        'uniformity_score': uniformity_score,
        'most_represented_axis': max(axis_counts.items(), key=lambda x: x[1]),
        'least_represented_axis': min(axis_counts.items(), key=lambda x: x[1])
    }

def save_to_parquet(questions: List[Dict[str, Any]], 
                   embeddings: List[List[float]], 
                   output_path: str):
    """Save questions and embeddings to parquet file."""
    print(f"Saving to {output_path}...")
    
    # Prepare dataframe
    df_data = []
    for i, (question, embedding) in enumerate(zip(questions, embeddings)):
        row = question.copy()
        row['embedding'] = embedding
        row['embedding_dim'] = len(embedding)
        df_data.append(row)
    
    df = pd.DataFrame(df_data)
    
    # Save to parquet
    df.to_parquet(output_path, engine='pyarrow', compression='snappy')
    print(f"Saved {len(df)} questions to {output_path}")

def main():
    """Main function to generate question pool and embeddings."""
    print("=== Socionics Question Pool Generator ===")
    
    # Set random seed for reproducibility
    random.seed(42)
    np.random.seed(42)
    
    # Step 1: Generate base questions from templates
    print("\n1. Generating base questions from templates...")
    base_questions = generate_question_variations()
    print(f"Generated {len(base_questions)} base questions")
    
    # Step 2: Add semantic variations to reach target count
    print("\n2. Adding semantic variations...")
    all_questions = add_semantic_variations(base_questions, target_count=64000)
    print(f"Total questions: {len(all_questions)}")
    
    # Step 3: Analyze initial distribution
    print("\n3. Analyzing question distribution...")
    distribution = analyze_distribution(all_questions)
    print(f"Questions per axis: {distribution['axis_distribution']}")
    print(f"Uniformity score: {distribution['uniformity_score']:.3f}")
    
    # Step 4: Compute embeddings
    print("\n4. Computing embeddings...")
    embeddings = compute_embeddings(all_questions)
    embeddings_array = np.array(embeddings)
    print(f"Embedding dimensions: {embeddings_array.shape}")
    
    # Step 5: K-means analysis
    print("\n5. Performing K-means clustering analysis...")
    kmeans_results = perform_kmeans_analysis(embeddings_array)
    
    # Step 6: Save full dataset
    print("\n6. Saving full dataset...")
    survey_dir = Path(__file__).parent.parent / "survey"
    survey_dir.mkdir(exist_ok=True)
    
    full_output_path = survey_dir / "question_pool_64k.parquet"
    save_to_parquet(all_questions, embeddings, str(full_output_path))
    
    # Step 7: Create decimated versions
    print("\n7. Creating decimated question sets...")
    for target_size in [1000, 500, 200]:
        decimated_questions, selected_indices = decimate_questions_by_similarity(
            all_questions, embeddings_array, target_size=target_size
        )
        
        decimated_embeddings = [embeddings[i] for i in selected_indices]
        decimated_output_path = survey_dir / f"question_pool_{target_size}.parquet"
        save_to_parquet(decimated_questions, decimated_embeddings, str(decimated_output_path))
        
        # Analyze decimated distribution
        decimated_dist = analyze_distribution(decimated_questions)
        print(f"Decimated to {target_size}: uniformity_score={decimated_dist['uniformity_score']:.3f}")
    
    # Step 8: Save analysis results
    print("\n8. Saving analysis results...")
    analysis_results = {
        'full_distribution': distribution,
        'kmeans_analysis': kmeans_results,
        'generation_params': {
            'target_count': 64000,
            'axes': list(AXES.keys()),
            'embedding_dim': embeddings_array.shape[1]
        }
    }
    
    analysis_path = survey_dir / "question_analysis.json"
    with open(analysis_path, 'w') as f:
        # Convert numpy types to regular Python types for JSON serialization
        def convert_numpy(obj):
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            return obj
        
        def clean_for_json(data):
            if isinstance(data, dict):
                return {k: clean_for_json(v) for k, v in data.items()}
            elif isinstance(data, list):
                return [clean_for_json(item) for item in data]
            else:
                return convert_numpy(data)
        
        clean_results = clean_for_json(analysis_results)
        json.dump(clean_results, f, indent=2)
    
    print(f"\n=== Generation Complete ===")
    print(f"Generated {len(all_questions):,} total questions")
    print(f"Embedding dimensions: {embeddings_array.shape[1]}")
    print(f"Best K-means clusters: {kmeans_results['best_k']}")
    print(f"Output files saved to: {survey_dir}")
    print(f"- Full dataset: question_pool_64k.parquet")
    print(f"- Decimated sets: question_pool_1000.parquet, question_pool_500.parquet, question_pool_200.parquet")
    print(f"- Analysis: question_analysis.json")

if __name__ == "__main__":
    main()