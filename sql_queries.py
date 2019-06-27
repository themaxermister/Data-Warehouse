import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES

## Staging Tables 
staging_events_table_create= ("""
    CREATE TABLE staging_events_table(
        artist          varchar(255),
        auth            varchar(255),
        firstname       varchar(255),
        gender          char(1),
        iteminsession   int,
        lastname        varchar(255),    
        length          double precision,
        level           varchar(255),
        location        varchar(255),
        method          varchar(255),
        page            varchar(255),
        registration    bigint,
        sessionid       int,
        song            varchar(255),
        status          int,
        ts              bigint,
        useragent       varchar(255),        
        userid          int        
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs_table(
        artist_id           varchar(255),
        artist_latitude     double precision,
        artist_location     varchar(255),
        artist_longitude    double precision,
        artist_name         varchar(255),
        duration            double precision,
        num_songs           int,
        song_id             varchar(255),
        title               varchar(255),
        year                int
    );
""")

## Fact Table 
songplay_table_create = ("""
    CREATE TABLE songplay_table (
        songplay_id     int IDENTITY(0,1)  PRIMARY KEY,
        start_time      timestamp,
        user_id         int,
        level           varchar(255),
        song_id         varchar(255),
        artist_id       varchar(255),
        session_id      varchar(255),
        location        varchar(255),
        user_agent      varchar(255)            
    );
""")

## Dimension Tables
user_table_create = ("""
    CREATE TABLE user_table (
        user_id      int              NOT NULL    PRIMARY KEY,
        first_name   varchar(255),
        last_name    varchar(255),
        gender       char(1),                 
        level        varchar(255)            
    );
""")

song_table_create = ("""
    CREATE TABLE song_table (
        song_id     varchar(255)  NOT NULL    PRIMARY KEY,
        title       varchar(255),
        artist_id   varchar(255),
        year        int,
        duration    double precision     
    );
""")

artist_table_create = ("""
    CREATE TABLE artist_table(
        artist_id   varchar(255)  NOT NULL   PRIMARY KEY,
        name        varchar(255),
        location    varchar(255),
        latitude    double precision,
        longitude   double precision               
    );
""")

time_table_create = ("""
    CREATE TABLE time_table(
        start_time  timestamp  NOT NULL  PRIMARY KEY,
        hour        int,       
        day         int,
        week        int,
        month       int,
        year        int,
        weekday     int
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events_table FROM '{}' 
    credentials 'aws_iam_role={}'
    format as json '{}'
    region 'us-west-2';
""").format(config.get("S3","LOG_DATA"), 
            config.get("IAM_ROLE","ARN"),
            config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
    COPY staging_songs_table FROM '{}' 
    credentials 'aws_iam_role={}'
    format as json 'auto'
    region 'us-west-2';
""").format(config.get("S3","SONG_DATA"), 
            config.get("IAM_ROLE","ARN"))

# FINAL TABLES

# Fact Table
songplay_table_insert = ("""
    INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        timestamp 'epoch' + e.ts * interval '1 second' AS start_time,
        e.userid as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionid AS session_id,
        e.location,
        e.useragent AS user_agent
    FROM staging_events_table e
    JOIN staging_songs_table s ON e.artist = s.artist_name AND e.song = s.title AND e.length = s.duration
    WHERE e.page = 'NextSong';
""")

## Dimension Tables
user_table_insert = ("""
    INSERT INTO user_table(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userid AS user_id,
        firstname AS first_name,
        lastname AS last_name,
        gender,
        level
    FROM staging_events_table
    WHERE e.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO song_table (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs_table;  
""")

artist_table_insert = ("""
    INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs_table;
""")

time_table_insert = ("""
    INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
    SELECT
        start_time,
        EXTRACT (HOUR FROM start_time) AS hour,
        EXTRACT (DAY FROM start_time) AS day,
        EXTRACT (WEEK FROM start_time) AS week,
        EXTRACT (MONTH FROM start_time) AS month,
        EXTRACT (YEAR FROM start_time) AS year,
        EXTRACT (DOW FROM start_time) AS weekday
    FROM songplay_table;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
