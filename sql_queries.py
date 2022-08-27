import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_sings"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length numeric,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionId bigint,
    song varchar,
    status int,
    ts bigint,
    userAgent varchar,
    userId int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id varchar,
    artist_latitude numeric,
    artist_location varchar,
    artist_longitude numeric,
    artist_name varchar,
    duration numeric,
    num_songs int,
    song_id varchar,
    title varchar,
    year int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay(
                           songplay_id bigint IDENTITY(0,1) PRIMARY KEY, 
                           start_time bigint, 
                           user_id INT, 
                           level VARCHAR, 
                           song_id VARCHAR NOT NULL REFERENCES song (song_id), 
                           artist_id VARCHAR NOT NULL REFERENCES user_table (user_id), 
                           session_id INT NOT NULL REFERENCES artist (artist_id), 
                           location VARCHAR, 
                           user_agent VARCHAR);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table (
    user_id int PRIMARY KEY, 
    first_name varchar NOT NULL, 
    last_name varchar NOT NULL, 
    gender varchar, 
    level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song
(song_id varchar PRIMARY KEY, 
title varchar NOT NULL, 
artist_id varchar NOT NULL, 
year int, 
duration numeric,
time_created timestamp
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist
(artist_id varchar PRIMARY KEY, 
name varchar NOT NULL, 
location varchar, 
latitude numeric, 
longitude numeric,
time_created timestamp
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table(
    start_time numeric PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday boolean
);
""")

# STAGING TABLES

staging_events_copy = (""" 
copy staging_events FROM 's3://udacity-dend/log_data' credentials 'aws_iam_role={}'
     region 'us-west-2' json 's3://udacity-dend/log_json_path.json';
""").format(config['IAM_ROLE']['ARN'])

staging_songs_copy = (""" 
copy staging_songs from 's3://udacity-dend/song_data/A/A/' credentials 'aws_iam_role={}'
     region 'us-west-2' json 'auto';
""").format(config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = (""" 
insert into songplay (
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location,
    user_agent
    )
select 
    s.ts as start_time, 
    s.userId as user_id, 
    s.level, 
    sa.song_id as song_id, 
    sa.artist_id, 
    s.sessionId as session_id, 
    s.location, 
    s.userAgent as user_agent 
from staging_events s
inner join 
(select a.artist_id, 
        a.name as artist, 
        s.song_id, 
        s.title as song
from artist a inner join song s on s.artist_id = a.artist_id
     ) sa on sa.song = s.song and sa.artist = s.artist
where s.page = 'NextSong'
;
""")

user_table_insert = ("""
insert into user_table (user_id, first_name, last_name, gender, level)
select user_id, first_name, 
last_name, gender, level 
from (
select 
    userId as user_id, 
    firstName as first_name, 
    lastName as last_name, 
    gender, 
    level,
    ROW_NUMBER() OVER (PARTITION BY userId ORDER BY ts DESC) as rank
from 
    staging_events
WHERE userId is not NULL
)
where rank = 1
; 
""")

song_table_insert = ("""
insert into  song (song_id, title, artist_id, year, duration, time_created)
WITH
song_table AS
(
select 
    distinct song_id, title, artist_id, year, duration, current_timestamp as time_created 
from staging_songs
UNION ALL
select * from song
)
select song_id, title, 
artist_id, year, duration, time_created 
from 
(select *, row_number() over (partition by song_id order by time_created desc) as rank from song_table)
where song_id is not NULL and rank=1;
""")

artist_table_insert = ("""
insert into artist (artist_id, name, location, latitude, longitude, time_created) 
WITH
artist_table AS
(
select 
    distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude, current_timestamp as time_created 
from staging_songs
UNION ALL
select * from song
)
select artist_id, artist_name as name, 
artist_location as location, 
artist_latitude as latitude, 
artist_longitude as longitude,
time_created
from
(select *, row_number() over (partition by artist_id order by time_created desc) as rank from artist_table)
where rank = 1 and artist_id is not NULL;
""")

time_table_insert = ("""
insert into time_table (start_time, hour, day, week,
month, year, weekday)
select
    distinct
    ts as start_time, 
    extract(hour from (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second')) as hour,
    extract(day from (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second')) as day, 
    extract(week from (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second')) as week,
    extract(month from (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second')) as month, 
    extract(year from (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second')) as year, 
    case when extract(dow from (TIMESTAMP 'epoch' + ts * INTERVAL '1 Second')) in (0,6) then FALSE else TRUE end as weekday
    from staging_events
;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
