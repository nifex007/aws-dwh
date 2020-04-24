import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE  IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events 
  ( 
    artist          VARCHAR(255),
    auth            VARCHAR(25),
    first_name      VARCHAR(25),
    gender          VARCHAR(1),
    item_in_session INTEGER, 
    last_name       VARCHAR(25),
    legnth          DECIMAL(9, 5),
    level           VARCHAR(10),
    location        VARCHAR(255),
    method          VARCHAR(6),
    page            VARCHAR(50),
    registration    DECIMAL(14, 1),
    session_id      INTEGER,
    song            VARCHAR(255),
    status          INTEGER,
    ts              BIGINT,
    user_agent      VARCHAR(150),
    user_id         VARCHAR(10)
  ); 
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
  ( 
    num_songs        INTEGER,
    artist_id        VARCHAR(25), 
    artist_latitude  DECIMAL(10, 5),
    artist_longitude DECIMAL(10, 5),
    artist_location  VARCHAR(255),
    artist_name      VARCHAR(255),
    song_id          VARCHAR(25),
    title            VARCHAR(255),
    duration         DECIMAL(9, 5),
    year             INTEGER
  );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
 (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY NOT NULL,
    start_time  TIMESTAMP NOT NULL, 
    user_id     VARCHAR(10),
    level       VARCHAR(10),
    song_id     VARCHAR(255) NOT NULL,
    artist_id   VARCHAR(25) NOT NULL,
    session_id  INTEGER,
    location    VARCHAR(255),
    user_agent  VARCHAR(255)
    
 )
DISTSTYLE KEY
DISTKEY ( start_time )
SORTKEY ( start_time );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
 (
    user_id     VARCHAR(30) PRIMARY KEY NOT NULL,
    first_name  VARCHAR(50),
    last_name   VARCHAR(50),
    gender      CHAR(1) ENCODE BYTEDICT,
    level       VARCHAR ENCODE BYTEDICT
 )
SORTKEY (user_id);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
 (
    song_id     VARCHAR(30) PRIMARY KEY NOT NULL,
    title       VARCHAR(255),
    artist_id   VARCHAR(30),
    year        INTEGER ENCODE BYTEDICT,
    duration    FLOAT NOT NULL
 )
SORTKEY (song_id);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR(30) PRIMARY KEY NOT NULL,
    name VARCHAR(255),
    location VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT
)
SORTKEY (artist_id);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time  TIMESTAMP PRIMARY KEY NOT NULL,
    hour        INTEGER,
    day         INTEGER,
    week        INTEGER,
    month       INTEGER,
    year        INTEGER ENCODE BYTEDICT ,
    weekday     VARCHAR(9) ENCODE BYTEDICT
)
DISTSTYLE KEY
DISTKEY ( start_time )
SORTKEY (start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
iam_role {}
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role {}
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (START_TIME, USER_ID, LEVEL, SONG_ID, ARTIST_ID, SESSION_ID, LOCATION, USER_AGENT)
SELECT DISTINCT
       TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
                se.user_id,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.session_id,
                se.location,
                se.user_agent
FROM staging_songs ss
INNER JOIN staging_events se
ON (ss.title = se.song AND se.artist = ss.artist_name)
AND se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE user_id IS NOT NULL
AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs
SELECT
    DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT
    DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
insert into time
SELECT DISTINCT
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
