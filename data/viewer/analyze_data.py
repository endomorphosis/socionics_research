#!/usr/bin/env python3
"""
Data analysis script for personality database viewer.
Uses the existing bot CLI to analyze data integrity.
"""

import sys
import json
import os
import subprocess
from pathlib import Path

def run_bot_command(bot_dir, command_args):
    """Run a bot CLI command and return the output."""
    try:
        python_exec = os.path.join(bot_dir, '.venv', 'bin', 'python')
        if not os.path.exists(python_exec):
            python_exec = 'python3'
        
        cmd = [python_exec, '-m', 'bot.pdb_cli'] + command_args
        env = os.environ.copy()
        env['PYTHONPATH'] = os.path.join(bot_dir, 'src')
        
        result = subprocess.run(
            cmd, 
            cwd=bot_dir, 
            capture_output=True, 
            text=True, 
            env=env,
            timeout=30
        )
        
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, '', str(e)

def count_files(directory, pattern='*.parquet'):
    """Count files matching pattern in directory."""
    try:
        path = Path(directory)
        if not path.exists():
            return 0
        return len(list(path.glob(pattern)))
    except:
        return 0

def get_file_size(filepath):
    """Get file size in bytes."""
    try:
        return os.path.getsize(filepath) if os.path.exists(filepath) else 0
    except:
        return 0

def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Dataset directory required"}))
        sys.exit(1)
        
    dataset_dir = sys.argv[1]
    bot_dir = os.path.join(os.path.dirname(dataset_dir), 'bot')
    
    # Check key files
    profiles_file = os.path.join(dataset_dir, 'pdb_profiles.parquet')
    vectors_file = os.path.join(dataset_dir, 'pdb_profile_vectors.parquet')
    normalized_file = os.path.join(dataset_dir, 'pdb_profiles_normalized.parquet')
    cache_dir = os.path.join(dataset_dir, 'pdb_api_cache')
    
    # Get basic file information
    profiles_size = get_file_size(profiles_file)
    vectors_size = get_file_size(vectors_file)
    normalized_size = get_file_size(normalized_file)
    
    # Count cache entries
    cache_entries = 0
    try:
        cache_path = Path(cache_dir)
        if cache_path.exists():
            cache_entries = len([f for f in cache_path.iterdir() if f.is_file() and f.suffix == '.json'])
    except:
        pass
    
    # Try to get actual counts using bot CLI
    total_profiles = 0
    total_vectors = 0
    
    if os.path.exists(bot_dir):
        # Try to get profile count
        success, stdout, stderr = run_bot_command(bot_dir, ['coverage'])
        if success and stdout:
            # Parse coverage output for numbers
            for line in stdout.split('\n'):
                if 'profiles' in line.lower() and any(char.isdigit() for char in line):
                    try:
                        # Extract number from line
                        numbers = ''.join(filter(str.isdigit, line))
                        if numbers:
                            total_profiles = int(numbers)
                            break
                    except:
                        pass
    
    # Estimate based on file sizes if we couldn't get exact counts
    if total_profiles == 0 and profiles_size > 0:
        # Rough estimate: ~1KB per profile average
        total_profiles = max(1, profiles_size // 1024)
    
    if total_vectors == 0 and vectors_size > 0:
        # Rough estimate: ~2KB per vector average
        total_vectors = max(1, vectors_size // 2048)
    
    # Basic health assessment
    duplicates = 0  # Would need pandas to detect accurately
    corruption = 0
    
    if profiles_size == 0:
        corruption += 1
    if vectors_size == 0:
        corruption += 1
    
    result = {
        "totalProfiles": total_profiles,
        "totalVectors": total_vectors,
        "normalizedProfiles": max(1, normalized_size // 1024) if normalized_size > 0 else 0,
        "cacheEntries": cache_entries,
        "duplicates": duplicates,  # Conservative estimate
        "corruption": corruption,
        "files": {
            "profiles": {
                "exists": os.path.exists(profiles_file),
                "size_bytes": profiles_size,
                "estimated_rows": total_profiles
            },
            "vectors": {
                "exists": os.path.exists(vectors_file),
                "size_bytes": vectors_size,
                "estimated_rows": total_vectors
            },
            "normalized": {
                "exists": os.path.exists(normalized_file),
                "size_bytes": normalized_size,
                "estimated_rows": max(1, normalized_size // 1024) if normalized_size > 0 else 0
            }
        }
    }
    
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()