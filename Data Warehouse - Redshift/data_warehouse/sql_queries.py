import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES
staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (
                                  event_id         BIGINT IDENTITY(0,1),
                                  artist           VARCHAR,
                                  auth             VARCHAR,
                                  firstName        VARCHAR,
                                  gender           VARCHAR,
                                  itemInSession    VARCHAR,
                                  lastName         VARCHAR,
                                  length           VARCHAR,
                                  level            VARCHAR,
                                  location         VARCHAR,
                                  method           VARCHAR,
                                  page             VARCHAR,
                                  registration     VARCHAR,
                                  sessionId        INTEGER,
                                  song             VARCHAR,
                                  status           INTEGER,
                                  ts               BIGINT,
                                  userAgent        VARCHAR,
                                  userId           INTEGER);
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (
                                  num_songs         INTEGER,
                                  artist_id         VARCHAR,
                                  artist_latitude   VARCHAR,
                                  artist_longitude  VARCHAR,
                                  artist_location   VARCHAR,
                                  artist_name       VARCHAR,
                                  song_id           VARCHAR,
                                  title             VARCHAR,
                                  duration          DECIMAL,
                                  year              INTEGER);
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplay (
                             songplay_id INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY SORTKEY,
                             start_time  TIMESTAMP             NOT NULL,
                             user_id     VARCHAR               NOT NULL,
                             level       VARCHAR               NOT NULL,
                             song_id     VARCHAR               NOT NULL,
                             artist_id   VARCHAR               NOT NULL,
                             session_id  VARCHAR               NOT NULL,
                             location    VARCHAR               NULL,
                             user_agent  VARCHAR               NULL);
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
                         user_id    INTEGER   NOT NULL PRIMARY KEY,
                         first_name VARCHAR   NULL,
                         last_name  VARCHAR   NULL,
                         gender     VARCHAR   NULL,
                         level      VARCHAR   NULL) diststyle all;
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
                         song_id   VARCHAR   NOT NULL PRIMARY KEY,
                         title     VARCHAR   NOT NULL,
                         artist_id VARCHAR   NOT NULL DISTKEY,
                         year      INTEGER   NOT NULL,
                         duration  DECIMAL   NOT NULL);
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (
                           artist_id  VARCHAR   NOT NULL SORTKEY,
                           name       VARCHAR   NULL,
                           location   VARCHAR   NULL,
                           latitude   DECIMAL   NULL,
                           longitude  DECIMAL   NULL) diststyle all;
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
                         start_time TIMESTAMP  NOT NULL PRIMARY KEY SORTKEY,
                         hour       INTEGER    NULL,
                         day        INTEGER    NULL,
                         week       INTEGER    NULL,
                         month      INTEGER    NULL,
                         year       INTEGER    NULL,
                         weekday    INTEGER    NULL) diststyle all;
""")


# STAGING TABLES
staging_events_copy = (f""" COPY staging_events
                            FROM {LOG_DATA}
                            IAM_ROLE {IAM_ROLE}
                            FORMAT AS json {LOG_JSONPATH};
""")

staging_songs_copy = (f""" COPY staging_songs
                           FROM {SONG_DATA}
                           IAM_ROLE {IAM_ROLE}
                           FORMAT AS JSON 'auto';
""")


# FINAL TABLES
songplay_table_insert = (""" INSERT INTO songplay (
                                 start_time,
                                 user_id,
                                 level,
                                 song_id,
                                 artist_id,
                                 session_id,
                                 location,
                                 user_agent)
                             SELECT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
                                                        se.userId                        AS user_id,
                                                        se.level                         AS level,
                                                        ss.song_id                       AS song_id,
                                                        ss.artist_id                     AS artist_id,
                                                        se.sessionId                     AS session_id,
                                                        se.location                      AS location,
                                                        se.userAgent                     AS user_agent
                             FROM staging_songs ss
                             JOIN staging_events se ON (se.artist = ss.artist_name)
                             WHERE se.page = 'NextSong';
""")

user_table_insert = (""" INSERT INTO users (
                             user_id,
                             first_name,
                             last_name,
                             gender,
                             level)
                         SELECT DISTINCT userId     AS user_id,
                                         firstName  AS first_name,
                                         lastName   AS last_name,
                                         gender     AS gender,
                                         level      AS level
                         FROM staging_events
                         WHERE page = 'NextSong';
""")

song_table_insert = (""" INSERT INTO songs (
                             song_id,
                             title,
                             artist_id,
                             year,
                             duration)
                         SELECT song_id     AS song_id,
                                title       AS title,
                                artist_id   AS artist_id,
                                year        AS year,
                                duration    AS duration
                         FROM staging_songs;
""")

artist_table_insert = (""" INSERT INTO artists (
                             artist_id,
                             name,
                             location,
                             latitude,
                             longitude)
                         SELECT DISTINCT artist_id              AS artist_id,
                                         artist_name            AS name,
                                         artist_location        AS location,
                                         artist_latitude        AS latitude,
                                         artist_longitude       AS longitude
                         FROM staging_songs;
""")

time_table_insert = (""" INSERT INTO time
                             start_time,
                             hour,
                             day,
                             week
                             month,
                             year,
                             weekday)
                         SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                                                   EXTRACT(hour FROM start_time)             AS hour,
                                                   EXTRACT(day FROM start_time)              AS day,
                                                   EXTRACT(week FROM start_time)             AS week,
                                                   EXTRACT(month FROM start_time)            AS month,
                                                   EXTRACT(year FROM start_time)             AS year,
                                                   EXTRACT(dayofweek FROM start_time)        AS weekday
                         FROM staging_events
                         WHERE page = 'NextSong';
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
