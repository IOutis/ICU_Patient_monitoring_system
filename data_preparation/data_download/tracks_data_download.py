import requests
import pandas as pd
import io
import time
import sys
import os

# Ensure root path is included for module import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
root = os.path.abspath(os.path.join(__file__, '../../'))

print(root)
file_path = os.path.join(root, 'data', 'final_signals_info.xlsx')
final_signals_info = pd.read_excel(file_path)

# Base directory to store all track data
BASE_SAVE_DIR = os.path.join(os.path.dirname(__file__), '..', 'tracks')

def save_dataframe_to_csv(df, filepath):
    """Creates directories and saves DataFrame to csv."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    df.to_csv(filepath, index=False)

def fetch_and_save_track_data(row):
    tid = row['tid']
    track_name = row['tname'].replace('/', '_')  # sanitize path if needed

    try:
        response = requests.get(f"https://api.vitaldb.net/{tid}", timeout=10)
        if response.status_code == 200:
            df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
            print(f"✅ Data fetched successfully for {track_name} ({tid})")

            filename = f"track_data_{tid}_{track_name}.csv"
            full_path = os.path.join(BASE_SAVE_DIR, filename)

            save_dataframe_to_csv(df, full_path)
        else:
            print(f"❌ Error fetching for {track_name} ({tid}) — Status: {response.status_code}")
    except Exception as e:
        print(f"⚠️ Exception for {track_name} ({tid}): {e}")
        time.sleep(5)  # optional backoff on error

# Main download loop
for _, row in final_signals_info.iterrows():
    fetch_and_save_track_data(row)
