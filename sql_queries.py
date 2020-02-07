import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('redshift/dwh.cfg')

# Load DWH Params from config file
config_dwh = configparser.ConfigParser()
try:
    config_dwh.read_file(open('redshift/dwh.cfg'))
except Exception as e:
    print(e)

iam_role = config_dwh.get("IAM_ROLE","ARN")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# NOTE: When using COPY on AWS, the order of the columns needs to match the order that is contained in the JSON file.
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
  artist text,
  auth text,
  firstName text,
  gender text,
  itemInSession int NOT NULL,
  lastName text,
  length real,
  level text NOT NULL,
  location text,
  method text,
  page text,
  registration real,
  sessionId int NOT NULL,
  song text,
  status int,
  ts bigint,
  userAgent text,
  userId int,
  PRIMARY KEY(sessionId, itemInSession)
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
  num_songs int,
  artist_id text NOT NULL,
  artist_latitude real,
  artist_longitude real,
  artist_location text,
  artist_name text,
  song_id text NOT NULL,
  title text,
  duration real,
  year int,
  PRIMARY KEY(song_id)
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(1,1) PRIMARY KEY, 
    start_time bigint,
    user_id int,
    level text,
    song_id text,
    artist_id text,
    session_id int,
    location text,
    user_agent text
)""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY,
    first_name text,
    last_name text,
    gender text,
    level text
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id text PRIMARY KEY,
    title text,
    artist_id text NOT NULL,
    year int,
    duration real
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id text PRIMARY KEY,
    name text,
    location text,
    latitude real,
    longitude real
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time bigint PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
)
""")


# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM 's3://udacity-dend/log_data' 
IAM_ROLE {}
JSON 's3://udacity-dend/log_json_path.json'
REGION 'us-west-2';
""").format(iam_role)

staging_songs_copy = ("""
COPY staging_songs from 's3://udacity-dend/song_data' 
IAM_ROLE {}
JSON 'auto'
region 'us-west-2'
""").format(iam_role)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) (
    SELECT e.ts, e.userId, e.level, sa.song_id, sa.artist_id, e.sessionId, e.location, e.userAgent
    FROM staging_events e
    JOIN (
        SELECT s.song_id, a.artist_id, s.title, a.name, s.duration
        FROM songs s
        INNER JOIN artists a
        ON s.artist_id = a.artist_id) sa
    ON sa.title = e.song 
    AND sa.name = e.artist 
    AND sa.duration = e.length
)
""")

# user_table_insert = ("""
# INSERT INTO users (
#     user_id,
#     first_name,
#     last_name,
#     gender,
#     level
# ) VALUES (%s, %s, %s, %s, %s) 
# ON CONFLICT (user_id)
# DO UPDATE
#     SET level = EXCLUDED.level
# """)

user_table_insert = ("""
INSERT INTO users (
    SELECT userId, firstName, lastName, gender, level 
    FROM staging_events 
    WHERE userId IS NOT NULL
) 
""")

song_table_insert = ("""
INSERT INTO songs (
    SELECT song_id, title, artist_id, year, duration 
    FROM staging_songs
)
""")

artist_table_insert = ("""
INSERT INTO artists (
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
)
""")


time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
) VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
#insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

insert_table_queries=[
        user_table_insert,song_table_insert, artist_table_insert, songplay_table_insert]

