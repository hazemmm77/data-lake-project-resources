## Description
Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.
the analytics team is particularly interested in understanding what songs users are listening to.
 I create a database schema and ETL pipeline for this analysis to be optimized for queries on song play analysis.
 Using the song and log datasets

 ## Database Design
 ### Fact Table
   **songplays** - records in log data associated with song plays i.e. records with page NextSong
   _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

### Dimension Tables
  **users** - users in the app
  _user_id, first_name, last_name, gender, level_

  **songs** - songs in music database
   _song_id, title, artist_id, year, duration_

  **artists** - artists in music database
  _artist_id, name, location, latitude, longitude_

  **time** - timestamps of records in songplays broken down into specific units
  _start_time, hour, day, week, month, year, weekday_

  ## ETL Process
   build an ETL pipeline using Python . reading data from log files  from directories data\song_data and data\log_data and  make Data Lack  by saving tables in parquet format in s3 .

  ## Project  files
1. ```etl.py``` reads and processes files from song_data and log_data and  make Data Lack by  saving tables in parquet format in s3
2. ```README.md``` provides discussion on your project.
 ## How To Run the Project

### Build ETL Processes
  run  etl.py  to build ETL process
