import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

# Queries to drop staging tables and analytics tables if they exist
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
songplay_time_table_drop = "DROP TABLE IF EXISTS songplay_time"

# CREATE TABLES

# Queries to create staging and analytics tables
staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    event_id INT IDENTITY(0,1) PRIMARY KEY,
    artist_name VARCHAR(255),
    auth VARCHAR(255),
    first_name VARCHAR(255),
    gender VARCHAR(1),
    item_in_session INTEGER,
    last_name VARCHAR(255),
    length DOUBLE PRECISION, 
    level VARCHAR(255),
    location VARCHAR(255),    
    method VARCHAR(25),
    page VARCHAR(35),    
    registration VARCHAR(255),    
    session_id BIGINT,
    song VARCHAR(255),
    status INTEGER,
    ts VARCHAR(255),
    user_agent TEXT,
    user_id INTEGER
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id VARCHAR(255) PRIMARY KEY,
    num_songs INTEGER,
    artist_id VARCHAR(255),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    title VARCHAR(255),
    duration DOUBLE PRECISION,
    year INTEGER
);
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id VARCHAR(255),
    level VARCHAR(255),
    song_id VARCHAR(255),
    artist_id VARCHAR(255),
    session_id BIGINT,
    location VARCHAR(255),
    user_agent TEXT
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(255) PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(1),
    level VARCHAR(255)
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(255),
    artist_id VARCHAR(255),
    year INTEGER,
    duration DOUBLE PRECISION
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);
"""

songplay_time_table_create = """
CREATE TABLE IF NOT EXISTS songplay_time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
);
"""

# STAGING TABLES

# Queries to copy data from S3 into staging tables
staging_events_copy = """
COPY staging_events FROM {}
IAM_ROLE {}
REGION 'us-west-2'
COMPUPDATE OFF STATUPDATE OFF
FORMAT AS JSON {}
""".format(config.get('S3','LOG_DATA'), 
           config.get('IAM_ROLE', 'ARN'), 
           config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = """
COPY staging_songs FROM {}
IAM_ROLE {}
REGION 'us-west-2'
COMPUPDATE OFF STATUPDATE OFF
FORMAT AS JSON 'auto'
""".format(config.get('S3','SONG_DATA'), 
           config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

# Queries to insert data into analytics tables from staging tables
songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts::bigint / 1000 * INTERVAL '1 second' AS start_time,
                se.user_id,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.session_id,
                se.location,
                se.user_agent
FROM staging_events se
LEFT JOIN staging_songs ss 
ON (se.song = ss.title AND se.artist_name = ss.artist_name)
WHERE se.page = 'NextSong'
;
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE page = 'NextSong' AND user_id IS NOT NULL;
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
"""

songplay_time_table_insert = """
INSERT INTO songplay_time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT 
       start_time,
       EXTRACT(hour FROM start_time),
       EXTRACT(day FROM start_time),
       EXTRACT(week FROM start_time),
       EXTRACT(month FROM start_time),
       EXTRACT(year FROM start_time),
       EXTRACT(dow FROM start_time)
FROM songplays;
"""

# QUERY LISTS

# Lists of queries for creating, dropping, copying, and inserting data into tables
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, songplay_time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, songplay_time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, songplay_time_table_insert]
