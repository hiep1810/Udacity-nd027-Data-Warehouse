import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS stage_events
    (
        artist          TEXT,
        auth            TEXT,
        first_name      TEXT,
        gender          TEXT,
        item_in_session INTEGER,
        last_name       TEXT,
        length          TEXT,
        level           TEXT,
        location        TEXT,
        method          TEXT,
        page            TEXT,
        registration    FLOAT8,
        session_id      INTEGER,
        song            TEXT,
        status          SMALLINT,
        ts              BIGINT,
        user_agent      TEXT,
        user_id         TEXT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stage_songs
    (
    num_songs           INTEGER,
    artist_id           TEXT,
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     TEXT,
    artist_name         TEXT,
    song_id             TEXT,
    title               TEXT,
    duration            FLOAT,
    year                INTEGER
    );
""")

#      +--------------+      +--------------+
#      |   songplays  |      |    users     |
#      +--------------+      +--------------+
#      | songplay_id  |      |  user_id     |
#      | start_time   |      |  first_name  |
#      | user_id      |      |  last_name   |
#      | level        |      |  gender      |
#      | song_id      |      |  level       |
#      | artist_id    |      +--------------+
#      | session_id   |
#      | location     |
#      | user_agent   |
#      +--------------+
#
#      +--------------+      +--------------+
#      |    songs     |      |   artists    |
#      +--------------+      +--------------+
#      | song_id      |      |  artist_id   |
#      | title        |      |  name        |
#      | artist_id    |      |  location    |
#      | year         |      |  latitude    |
#      | duration     |      |  longitude   |
#      +--------------+      +--------------+
#
#      +--------------+
#      |     time     |
#      +--------------+
#      |  start_time  |
#      |  hour        |
#      |  day         |
#      |  week        |
#      |  month       |
#      |  year        |
#      |  weekday     |
#      +--------------+


songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id    BIGINT IDENTITY(1, 1) PRIMARY KEY,
        start_time     TIMESTAMP NOT NULL ,
        user_id        TEXT NOT NULL ,
        level          TEXT,
        song_id        TEXT,
        artist_id      TEXT,
        session_id     INTEGER,
        location       TEXT,
        user_agent     TEXT
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id     TEXT PRIMARY KEY,
        first_name  TEXT,
        last_name   TEXT,
        gender      TEXT,
        level       TEXT
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id     TEXT PRIMARY KEY ,
        title       TEXT,
        artist_id   TEXT ,
        year        SMALLINT,
        duration    FLOAT4
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id   TEXT PRIMARY KEY ,
        name        TEXT,
        location    TEXT,
        latitude    FLOAT4,
        longitude   FLOAT4
    ) ;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time  TIMESTAMP PRIMARY KEY ,
        hour        SMALLINT,
        day         SMALLINT,
        week        SMALLINT,
        month       SMALLINT,
        year        SMALLINT ,
        weekday     SMALLINT
    ) ;
""")

# STAGING TABLES

staging_events_copy = ("""
copy stage_events from {bucket}
    credentials 'aws_iam_role={role}'
    region      '{region}'
    format       as JSON {path}
    timeformat   as 'epochmillisecs'
""").format(
        bucket=config.get("S3", "LOG_DATA"), 
        role=config.get("IAM_ROLE", "ARN"),
        region=config.get("DWH", "DWH_REGION"), 
        path=config.get("S3", "LOG_JSONPATH")
    )


staging_songs_copy = ("""
    copy stage_songs from {bucket}
    credentials  'aws_iam_role={role}'
    region       '{region}'
    format        as JSON 'auto'
""").format(
        bucket = config.get("S3","SONG_DATA"),
        role=config.get("IAM_ROLE", "ARN"),
        region=config.get("DWH", "DWH_REGION")
)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT
        TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second'),
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM stage_events e
    JOIN stage_songs s ON
        e.song = s.title AND
        e.artist = s.artist_name AND
        e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users SELECT DISTINCT (user_id)
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM stage_events
""")

song_table_insert = ("""
    INSERT INTO songs SELECT DISTINCT (song_id)
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM stage_songs
""")

artist_table_insert = ("""
    INSERT INTO artists SELECT DISTINCT (artist_id)
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM stage_songs
""")


time_table_insert = ("""
    INSERT INTO time
        WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM stage_events)
        SELECT DISTINCT
        ts,
        extract(hour from ts),
        extract(day from ts),
        extract(week from ts),
        extract(month from ts),
        extract(year from ts),
        extract(weekday from ts)
        FROM temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]