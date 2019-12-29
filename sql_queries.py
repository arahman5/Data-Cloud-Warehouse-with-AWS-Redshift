import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Setup some global variables for using in ETL Pipeline that uses the values of few keys from the Config file
LOG_DATA = config.get("S3","LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Create the events table from the logs containing all the columns about event data on user acitivity in the app
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist          VARCHAR,
auth            VARCHAR, 
firstName       VARCHAR,
gender          VARCHAR,   
itemInSession   INTEGER,
lastName        VARCHAR,
length          FLOAT,
level           VARCHAR, 
location        VARCHAR,
method          VARCHAR,
page            VARCHAR,
registration    BIGINT,
sessionId       INTEGER,
song            VARCHAR,
status          INTEGER,
ts              TIMESTAMP,
userAgent       VARCHAR,
userId          INTEGER
);
""")

# Create the staging songs table containing all the columns from the logs containing information about song metadata
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
song_id            VARCHAR,
num_songs          INTEGER,
title              VARCHAR,
artist_name        VARCHAR,
artist_latitude    FLOAT,
year               INTEGER,
duration           FLOAT,
artist_id          VARCHAR,
artist_longitude   FLOAT,
artist_location    VARCHAR
);
""")

# This is the fact table that contains the primary keys from the below 4 Dimension tables so that JOINs can be made 
# between the fact table and dimension tables. 
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
songplay_id          INTEGER IDENTITY(0,1) PRIMARY KEY,
start_time           TIMESTAMP NOT NULL,
user_id              INTEGER NOT NULL,
level                VARCHAR,
song_id              VARCHAR NOT NULL,
artist_id            VARCHAR NOT NULL,
session_id           INTEGER,
location             VARCHAR,
user_agent           VARCHAR
);
""")

# This is the dimension table containing the information about users of the app
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id INTEGER PRIMARY KEY,
first_name      VARCHAR,
last_name       VARCHAR,
gender          VARCHAR,
level           VARCHAR
);
""")

# This is the dimension table containing information about the songs in the database
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id     VARCHAR PRIMARY KEY,
title       VARCHAR,
artist_id   VARCHAR NOT NULL,
year        INTEGER,
duration    FLOAT
);
""")

# This is the dimension table containing information about the artists in the database
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id          VARCHAR PRIMARY KEY,
name               VARCHAR,
location           VARCHAR,
latitude           FLOAT,
longitude          FLOAT
);
""")

# This is the dimension table containing information about timestamps of records in songplays table
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time    TIMESTAMP PRIMARY KEY,
hour          INTEGER,
day           INTEGER,
week          INTEGER,
month         INTEGER,
year          INTEGER,
weekday       INTEGER
);
""")

# STAGING TABLES

# Extract out the user activity data from S3 and load them into staging table in Redshift

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE, LOG_PATH)

## Extract out the song metadata from S3 and load them into staging table in Redshift

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

"""
    Joining the two tables staging_events and staging_songs on artist name and song title to obtain the information   
    about song_id and artist_id columns. Every song that has a title should have a song id and every artist that 
    has a name should have an aritst id.
"""
songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT to_timestamp(to_char(ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                userId as user_id,
                level as level,
                staging_songs.song_id as song_id,
                staging_songs.artist_id as artist_id,
                sessionId as session_id,
                location as location,
                userAgent as user_agent
FROM staging_events 
JOIN staging_songs ON song = title AND artist = artist_name
AND page  ==  'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId as user_id,
                firstName as first_name,
                lastName as last_name,
                gender as gender,
                level as level
FROM staging_events
WHERE userId IS NOT NULL
AND page  ==  'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id as song_id,
                title as title,
                artist_id as artist_id,
                year as year,
                duration as duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id as artist_id,
                artist_name as name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT start_time,
                EXTRACT(hour from start_time),
                EXTRACT(day from start_time),
                EXTRACT(week from start_time),
                EXTRACT(month from start_time),
                EXTRACT(year from start_time),
                EXTRACT(weekday from start_time)
FROM songplays
WHERE start_time IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
