# DROP TABLES

#tablename: Its the name of table we will drop

def drop_table(tablename):
    drop = "DROP TABLE IF EXISTS "
    droptable = drop + tablename
    print('Dropping Table' ,tablename)
    return droptable

songplay_table_drop = drop_table("songplay")
user_table_drop =  drop_table("users")
song_table_drop = drop_table("songs")
artist_table_drop = drop_table("artists")
time_table_drop = drop_table("time")


# CREATE TABLES

# -tablename: Name of table we will create
# -schema: its the schema or columns that we will have in our table. Here we define our primary key and what type the column should be.

def create_tables(tablename,schema):
    create = "CREATE TABLE "
    createtable = create + tablename + schema 
    print('creating' , tablename)
    return createtable

#we create a list of strings to get all of our schema toghether
create_schema = [
                "(songplay_id SERIAL, start_time bigint, user_id int, level varchar(4),\
                 song_id varchar(18), artist_id varchar(18), session_id int NOT NULL,\
                 location text NOT NULL, user_agent text, PRIMARY KEY (songplay_id))",
                "(user_id int, first_name text, last_name text, gender varchar(1),\
                 level varchar(4), PRIMARY KEY (user_id))",
                "(song_id varchar(18), title text NOT NULL, artist_id varchar(18) NOT NULL,\
                 year int NOT NULL, duration numeric NOT NULL, PRIMARY KEY (song_id))",
                "(artist_id varchar(18), artist_name text, artist_location text, \
                artist_latitude numeric, artist_longitude numeric, PRIMARY KEY (artist_id))",
                " (start_time bigint, hour int , day int, week int, month int, year int,\
                 weekday int, PRIMARY KEY (start_time))"
                ]


#Here we run our function and define the table name and what schema we want to use
songplay_table_create = create_tables("songplays",create_schema[0])
user_table_create = create_tables("users",create_schema[1])
song_table_create = create_tables("songs",create_schema[2])
artist_table_create = create_tables( "artists",create_schema[3])
time_table_create = create_tables("time",create_schema[4])


# INSERT RECORDS

def insert_records(tablename,schema):
    insert = "INSERT INTO "
    insertrecord = insert + tablename + schema 
    print('Inserting record in ', tablename)
    return insertrecord

#we create a list of strings to get all of our schema toghether
insert_schema = [
                "(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)\
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s)\
                ON CONFLICT(songplay_id) DO NOTHING;",
                "(user_id, first_name, last_name, gender, level)\
                VALUES(%s, %s, %s, %s, %s)\
                ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;",
                "(song_id, title, artist_id, year, duration)\
                VALUES(%s, %s, %s, %s, %s)\
                ON CONFLICT DO NOTHING;",
                "(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)\
                VALUES(%s, %s, %s, %s, %s)\
                ON CONFLICT DO NOTHING;",
                "(start_time, hour, day, week, month, year, weekday)\
                VALUES(%s, %s, %s, %s, %s, %s, %s)\
                ON CONFLICT DO NOTHING;"
                ]

#Here we run our function and define the table name and what schema we want to use
songplay_table_insert = insert_records("songplays",insert_schema[0])
user_table_insert = insert_records("users",insert_schema[1])
song_table_insert =insert_records(" songs",insert_schema[2])
artist_table_insert =  insert_records("artists" ,insert_schema[3])
time_table_insert = insert_records("time",insert_schema[4])

# FIND SONGS

song_select = (""" SELECT songs.song_id, artists.artist_id \
                FROM songs JOIN artists ON songs.artist_id = artists.artist_id \
                WHERE songs.title = %s AND artists.artist_name = %s AND songs.duration = %s
                """)


# QUERY LISTS

create_table_queries = [songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]

drop_table_queries = [songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]