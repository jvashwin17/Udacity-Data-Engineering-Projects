# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
        user_id INT PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR(255),
        artist_id VARCHAR(255),
        year INT NOT NULL,
        duration NUMERIC
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR(255) PRIMARY KEY,
        name VARCHAR(255),
        location VARCHAR(255),
        latitude float,
        longitude float
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay(
        songplay_id SERIAL PRIMARY KEY, 
        start_time timestamp, 
        user_id int, 
        level varchar, 
        song_id varchar,
        artist_id varchar, 
        session_id int, 
        location varchar, 
        user_agent varchar
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplay(start_time, user_id,level,song_id,artist_id,session_id,location, user_agent) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""
INSERT INTO users VALUES(%s,%s,%s,%s,%s)
ON CONFLICT (user_id) DO 
    UPDATE SET level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs VALUES(%s,%s,%s,%s,%s)
ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists VALUES(%s,%s,%s,%s,%s)
ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time VALUES(%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT songs.song_id, artists.artist_id FROM songs
  JOIN artists ON songs.artist_id = artists.artist_id
  WHERE songs.title = %s
  AND artists.name = %s
  AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]