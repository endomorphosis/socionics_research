import pandas as pd
import json

# Load the original file
df = pd.read_parquet('/home/devel/socionics_research/data/bot_store/pdb_profiles.parquet')

# Decode payload_bytes and expand JSON fields
def decode_payload(b):
    try:
        return json.loads(b.decode('utf-8'))
    except Exception:
        return {}

payload_df = df['payload_bytes'].apply(decode_payload)
payload_df = pd.DataFrame(list(payload_df))

# Merge with cid
result = pd.concat([df['cid'], payload_df], axis=1)

# Convert all columns to string
result = result.astype(str)

# Save as a new Parquet file with only string columns
result.to_parquet('/home/devel/socionics_research/data/bot_store/pdb_profiles_flat.parquet', index=False)
print('Wrote /home/devel/socionics_research/data/bot_store/pdb_profiles_flat.parquet')

# Also save as CSV for Node.js compatibility
result.to_csv('/home/devel/socionics_research/data/bot_store/pdb_profiles_flat.csv', index=False)
print('Wrote /home/devel/socionics_research/data/bot_store/pdb_profiles_flat.csv')
