# Data Modeling with Postgres

## Intro
Sparkify wants to be able to understand what songs users are listening to. Currently they dont have a way to query their data which its only avaiable in multiple json files. The goal for this project is to build a Postgress database so that the analytics team can query directly there and find important insights.

## Data

We currently have 2 directories that contain different data:
- Song Dataset: Json files that contains metadata about song and artist. The data looks like this:
```json
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- Lopg Data set: Song activity logs from the music streaming app. 


# Schema for database

Using this 2 data set we built 5 different tables: 

- Fact Table
songplays - (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

-Dimension Tables
users - (user_id, first_name, last_name, gender, level)
songs - (song_id, title, artist_id, year, duration)
artists(artist_id, name, location, latitude, longitude)
time - (start_time, hour, day, week, month, year, weekday)


# Files for our pipeline

Our pipeline cosist of different files that do different things.

- sql_queries.py : Here we define our Postgress queries. Here we Create  different functions that will create tables , Insert data into our tables and Run queries.

- create_tables.py: This file uses all the functions of ```sql_queries.py``` to create first drop the tables and then create the tables based on the schema we defined on ```sql_queries.py```.

- etl.py: This is our pipeline that extract data from our datasets, it transform the data into lists so that we can push it into our postgress data base.




