SELECT current_database(); -- CHECK DATABASE, postgres BY DEFAULT.

-- =========================
-- SCHEMA CREATION
-- =========================
-- Raw schema: original data
CREATE SCHEMA RAW;

-- PRS schema: processed / filtered data
CREATE SCHEMA PRS;

-- =========================
-- RAW DATA TABLE
-- =========================
-- This table stores all raw Spotify events with detailed metadata
CREATE TABLE RAW.SPOTIFY_EVENTS (
    id SERIAL PRIMARY KEY,
    end_time TIMESTAMP NOT NULL,
    artist_name TEXT,
    track_name TEXT,
    ms_played INTEGER NOT NULL,
    album_name TEXT,
    context TEXT,
    platform TEXT,
    user_id TEXT,
    conn_country TEXT,
    ip_addr TEXT,
    spotify_track_uri TEXT,
    episode_name TEXT,
    episode_show_name TEXT,
    spotify_episode_uri TEXT,
    audiobook_title TEXT,
    audiobook_uri TEXT,
    audiobook_chapter_uri TEXT,
    audiobook_chapter_title TEXT,
    reason_start TEXT,
    reason_end TEXT,
    shuffle BOOLEAN,
    skipped BOOLEAN,
    offline BOOLEAN,
    offline_timestamp TEXT,
    incognito_mode BOOLEAN
);

-- Quick check: preview raw data
SELECT * FROM raw.spotify_events;

-- =========================
-- PROCESSED 2025 DATA TABLE
-- =========================
-- This table stores only 2025 data and only relevant columns for analysis

CREATE TABLE PRS.SPOTIFY_EVENTS_2025 (
    id SERIAL PRIMARY KEY,
    end_time TIMESTAMP NOT NULL,
    artist_name TEXT,
    track_name TEXT,
    ms_played INTEGER
);

-- Insert 2025 data from RAW table
INSERT INTO PRS.SPOTIFY_EVENTS_2025 
(end_time, artist_name, track_name, ms_played)
SELECT 
    end_time,
    artist_name,
    track_name,
    ms_played
FROM RAW.SPOTIFY_EVENTS
WHERE end_time >= '2025-01-01'
  AND artist_name IS NOT NULL;