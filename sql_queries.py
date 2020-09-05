# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists "
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE songplays (songplay_id SERIAL, start_time bigint, user_id int, level varchar(4), song_id varchar(18), artist_id varchar(18), session_id int NOT NULL, location text NOT NULL, user_agent text, PRIMARY KEY (songplay_id))""")

user_table_create = (""" CREATE TABLE users (user_id int, first_name text, last_name text, gender varchar(1) , level varchar(4), PRIMARY KEY (user_id))""")

song_table_create = ("""CREATE TABLE songs (song_id varchar(18) , title text NOT NULL, artist_id varchar(18) NOT NULL, year int NOT NULL, duration numeric NOT NULL, PRIMARY KEY (song_id))""")

artist_table_create = ("""CREATE TABLE artists (artist_id varchar(18), artist_name text, artist_location text, artist_latitude numeric, artist_longitude numeric, PRIMARY KEY (artist_id))

""")

time_table_create = ("""CREATE TABLE time (start_time bigint, hour int , day int, week int, month int, year int, weekday int, PRIMARY KEY (start_time))

""")


# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT(songplay_id) DO NOTHING;
                        """)

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                        VALUES(%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
                    """)

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
                        VALUES(%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                        
                    """)

artist_table_insert = ("""INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
                          VALUES(%s, %s, %s, %s, %s)
                          ON CONFLICT DO NOTHING;

                      """)

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                        VALUES(%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                    """)

# FIND SONGS

song_select = (""" SELECT songs.song_id, artists.artist_id FROM songs JOIN artists ON songs.artist_id = artists.artist_id WHERE songs.title = %s AND artists.artist_name = %s AND songs.duration = %s
""")

print(song_select)

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]