import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *



def process_song_file(cur, filepath):

    '''This function opens the song file, then inserts a song record and an artist recordr
    There are 2 arguments:

    - cur: which allows Python code to execute PostgreSQL command in a database session.
    - filepath: path of the file we want to get the song and artist data from.
    '''

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[["song_id","title","artist_id","year","duration"]].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[["artist_id","artist_name","artist_location",
                    "artist_latitude","artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):

    '''This function opens the log  file, then filters by NextSong, converts timestamp column 
    into datetime, then inserts the time data records by iterating over the time_df dataframe.
    After this we load a user table and insert the user and songplay records.
    There are 2 arguments:

    - cur: which allows Python code to execute PostgreSQL command in a database session.
    - filepath: path of the file we want to get the data from.
    '''

    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df["page"]=="NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    time_data = [df.ts,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday]
    column_labels = ["timestamp","hour", "day", "week", "month", "year", "weekday"]
    time={column_labels[each]:time_data[each] for each in range(0,len(time_data))}
    time_df = pd.DataFrame(time,columns=column_labels)
    time_df.head()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId","firstName","lastName","gender","level"]]
    user_df = user_df.drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level]
        songplay_data=songplay_data+[songid, artistid]
        songplay_data=songplay_data+[row.sessionId,row.location,row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):

    '''This function gets all the file from the direxctory and add it to a empty array.
    It then finds the number of files that we found. And then i itirates over the files and process it.

    There are 4 arguments:

    - cur: which allows Python code to execute PostgreSQL command in a database session.
    - conn: The connedtion to the database
    - filepath: path of the file we want to get the data from.
    - func: the function we want to run. song or log.
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    Main is where we put everything together and we process all the data.
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()