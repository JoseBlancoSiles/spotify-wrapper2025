- =========================
-- TOP 5 TRACKS BY MINUTES/HOURS
-- =========================
-- Sum milliseconds played per track, convert to minutes and hours
SELECT 
    artist_name AS "ARTIST", 
    track_name AS "TRACK",
    SUM(ms_played) / 60000 AS "TOTAL_MINUTES_PLAYED",
    SUM(ms_played) / 3600000 AS "TOTAL_HOURS_PLAYED"
FROM PRS.SPOTIFY_EVENTS_2025
WHERE track_name IS NOT NULL
GROUP BY artist_name, track_name
ORDER BY "TOTAL_MINUTES_PLAYED" DESC
LIMIT 5;

-- =========================
-- TOTAL HOURS LISTENED IN 2025
-- =========================
SELECT
    SUM(ms_played) / 3600000 AS "TOTAL_HOURS_PLAYED_2025"
FROM PRS.SPOTIFY_EVENTS_2025;

-- =========================
-- TOP ARTIST BY TOTAL HOURS
-- =========================
SELECT
    artist_name AS "ARTIST",
    SUM(ms_played) / 3600000 AS "TOTAL_HOURS_PLAYED"
FROM PRS.SPOTIFY_EVENTS_2025
GROUP BY artist_name
ORDER BY "TOTAL_HOURS_PLAYED" DESC
LIMIT 1;

-- =========================
-- TOP DAYS BY MINUTES LISTENED
-- =========================
-- Includes day of week and month for context
SELECT
    DATE(end_time) AS "DAY",
    TO_CHAR(end_time, 'Day') AS "DAY_OF_WEEK",
    TO_CHAR(end_time, 'Month') AS "MONTH",
    SUM(ms_played) / 60000 AS "TOTAL_MINUTES_PLAYED"
FROM PRS.SPOTIFY_EVENTS_2025
GROUP BY DATE(end_time), TO_CHAR(end_time, 'Day'), TO_CHAR(end_time, 'Month') 
ORDER BY "TOTAL_MINUTES_PLAYED" DESC
LIMIT 5;

-- =========================
-- MOST ACTIVE HOUR OF THE DAY
-- =========================
-- Shows the hour (0-23) with most total minutes listened
SELECT
    EXTRACT(HOUR FROM end_time) AS hour,
    SUM(ms_played) / 60000 AS "TOTAL_MINUTES_PLAYED"
FROM PRS.SPOTIFY_EVENTS_2025
GROUP BY EXTRACT(HOUR FROM end_time)
ORDER BY "TOTAL_MINUTES_PLAYED" DESC
LIMIT 1;

-- =========================
-- LISTENING PERIODS (MORNING/AFTERNOON/EVENING/NIGHT)
-- =========================
SELECT 
    CASE 
        WHEN EXTRACT(HOUR FROM end_time) BETWEEN 5 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM end_time) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM end_time) BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night'
    END AS period,
    SUM(ms_played) / 60000 AS "TOTAL_MINUTES_PLAYED"
FROM PRS.SPOTIFY_EVENTS_2025
GROUP BY 1
ORDER BY "TOTAL_MINUTES_PLAYED" DESC;

-- =========================
-- MOST PLAYED TRACK
-- =========================
SELECT
    track_name,
    COUNT(*) AS play_count
FROM PRS.SPOTIFY_EVENTS_2025
GROUP BY track_name
ORDER BY play_count DESC
LIMIT 1;

-- =========================
-- FIRST SONG OF THE YEAR
-- =========================
SELECT *
FROM PRS.SPOTIFY_EVENTS_2025
ORDER BY end_time
LIMIT 1;

-- =========================
-- MOST SKIPPED TRACKS
-- =========================
-- Considering tracks played < 5 seconds as skipped
SELECT
    track_name,
    COUNT(*) AS skips
FROM PRS.SPOTIFY_EVENTS_2025
WHERE ms_played < 5000
GROUP BY track_name
ORDER BY skips DESC;