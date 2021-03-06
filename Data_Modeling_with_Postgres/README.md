# Project Overwiew
We will be applying Data Modeling with Postgres concept to build an ETL pipeline using Python. A startup called Sparkify wants to analyze the data they have been collecting on songs and user activity on their new music streaming app. The data is on json format and the analytics team is particularly interested in understanding what songs users are listening to.

# Schema

Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
Table rows - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables

users - users in the app
Table rows - user_id, first_name, last_name, gender, level

songs - songs in music database
Table Rows -song_id, title, artist_id, year, duration

artists - artists in music database
Table rows - artist_id, name, location, latitude, longitude

time - timestamps of records in songplays broken down into specific units
Table rows - start_time, hour, day, week, month, year, weekday

# Project Data Files

Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song.
Sample Data:
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

Log Dataset
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above.
Sample Data :
{"artist": null, "auth": "Logged In", "firstName": "Walter", "gender": "M", "itemInSession": 0, "lastName": "Frye", "length": null, "level": "free", "location": "San Francisco-Oakland-Hayward, CA", "method": "GET","page": "Home", "registration": 1540919166796.0, "sessionId": 38, "song": null, "status": 200, "ts": 1541105830796, "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"", "userId": "39"}


# Project Source Files
| File Names       | Description                                                                                       |
|------------------|---------------------------------------------------------------------------------------------------|
| test.ipnyb       | This file is used to do the validation                                                            |
| create_tables.py | This file is used to create new tables. This must be run at first.                                |
| etl.ipnyb        | Reads and processes a single file from song_data and log_data and loads the data into your tables |
| etl.py           | Reads and process song and data files                                                             |
| sql_queries.py   | Contains all the create table queries and Insert queries.                                         |

# Technology Stack
Python 3.6 or above
PostgresSQL 9.5 or above

# Run the project
First step - Run create_tables.py to create the tables
Second step - Run etl.py to read the datasets and load the data into the tables.
