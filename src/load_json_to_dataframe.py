import pandas as pd
import json
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values
import glob
from dotenv import load_dotenv
import os

def load_json_to_dataframe(file_path):
    """
    Load JSON data from a file into a pandas DataFrame.

    Args:
        file_path (str): Path to the JSON file.

    Returns:
        pd.DataFrame: DataFrame containing the JSON data.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # Convert JSON data to DataFrame
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return None

from datetime import datetime

def dump_data_to_postgres(df, connection_params):
    conn = None
    cursor = None
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        # ... rest of your insertion code ...

    except Exception as e:
        print(f"Error inserting data into PostgreSQL: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        # Define the insert query
        insert_query = """
        INSERT INTO RAW.spotify_events (
            end_time, artist_name, track_name, ms_played, album_name, context, platform, 
            conn_country, ip_addr, spotify_track_uri, episode_name, episode_show_name, 
            spotify_episode_uri, audiobook_title, audiobook_uri, audiobook_chapter_uri, 
            audiobook_chapter_title, reason_start, reason_end, shuffle, skipped, offline, 
            offline_timestamp, incognito_mode
        ) VALUES %s
        """

        # Prepare data for insertion
        values = [
            (
                row['ts'], 
                row.get('master_metadata_album_artist_name'), 
                row.get('master_metadata_track_name'),
                row['ms_played'], 
                row.get('master_metadata_album_album_name'), 
                None, 
                row['platform'],
                row.get('conn_country'), 
                row.get('ip_addr'), 
                row.get('spotify_track_uri'),
                row.get('episode_name'), 
                row.get('episode_show_name'), 
                row.get('spotify_episode_uri'),
                row.get('audiobook_title'), 
                row.get('audiobook_uri'), 
                row.get('audiobook_chapter_uri'),
                row.get('audiobook_chapter_title'), 
                row.get('reason_start'), 
                row.get('reason_end'),
                row.get('shuffle'), 
                row.get('skipped'), 
                row.get('offline'),
                row.get('offline_timestamp'),
                row.get('incognito_mode')
            )
            for _, row in df.iterrows()
        ]

        # Execute batch insert
        execute_values(cursor, insert_query, values)
        conn.commit()

        print("Data successfully inserted into the database.")

    except Exception as e:
        print(f"Error inserting data into PostgreSQL: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    load_dotenv()
    # Folder containing JSON files
    folder_path = "Spotify Extended Streaming History/"

    # Get all JSON files in the folder
    json_files = glob.glob(os.path.join(folder_path, "*.json"))

    # List to hold all DataFrames
    dataframes = []

    for file_path in json_files:
        df = load_json_to_dataframe(file_path)
        if df is not None:
            dataframes.append(df)

    if dataframes:
        # Concatenate all DataFrames into one
        combined_df = pd.concat(dataframes, ignore_index=True)
        print("All JSON files loaded successfully!")
        print(combined_df.head())
        print(combined_df.info())

        # PostgreSQL connection parameters
        connection_params = {
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT')}


        # Dump combined data into PostgreSQL
        dump_data_to_postgres(combined_df, connection_params)
    else:
        print("No JSON files were loaded.")