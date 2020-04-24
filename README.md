# Data-Lake
Data-Lake is a database for the music streaming startup company Sparkify. The data is a collection of songs and user activities on their new music streaming app. The goal of the program is to understand what songs users are listening to. The information comes from two JSON directories the datasets are in S3 AWS. The program is a database code in PySpark with tables designed to optimize queries on song play analysis and an ETL pipeline.

**ETL explanation**
![focus](imagines_DLake/ETL.png)\
\
**Data Lake Architecture**\
\
![focus](imagines_DLake/Project_Architecture_Data_Lake.png)

## JSON directories (Start Dataset)
**Song Dataset**\
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song.\
columns: artist_id, artist_latitude, artist_location,artist_longitude, artist_name, duration, num_songs, song_id, title, year

**Log Dataset**\
The second dataset consists of log files in JSON format generated by this event simulator. These simulate activity logs from a music streaming app based on specified configurations.\
columns: artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId


## Tables data base in parquet format 
Basic Table (Data from S3 AWS)

## Dataset
the datasets reside in S3. Here are the S3 links for each:

### Song Dataset
's3a://udacity-dend/song_data/*/*/*/*.json'

##### columns: 
artist_id, 
artist_latitude, 
artist_location,artist_longitude, 
artist_name, 
duration, 
num_songs, 
song_id, title, 
year

### Log Dataset
folder path: 's3a://udacity-dend/log_data/*.json'

##### columns: 
artist, 
auth, 
firstName, 
gender, 
itemInSession, 
lastName, 
length, 
level, 
location, 
method, 
page, 
registration, 
sessionId, 
song, 
status, 
ts, 
userAgent, 
userId

## Tables in Spark
Basic Table (Data from S3 AWS) staging_song, staging_events 
1) Song Data:   s3://udacity-dend/song_data
   staging_song: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year
   
2) Log Data:    s3://udacity-dend/log_data
   staging_events: staging_event_key, artist, auth, firstName, gender, iteminSession, lastName, length, level, location, method, page =   Next Song,    registration, sessionId, song, status, ts, userAgent, userId  
it will be only load the row if page = NextSong

### Dimension Table
Data from Song Dataset as staging_song (song_table, artist_table)
1) song_table: song_id, title, artist_id, year, duration
2) artist_table: artist_id, artist_name, artist_location, artist_latitude, artist_longitude

Data from Log Dataset as staging_events (time_table, users) + NextSong
1) time_table: start_time, hour, day, week, month, year, weekday
2) users: userId, firstName, lastName, gender, level

### Fact Table 
JOIN beteween staging_song und staging_events (songplay)
songplay:  songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
![focus](imagines_DLake/ER_Diagram.png)

## Program description steps (etl.py)
etl.py is the only skript of this program and create the dimension and fact tables from the two dataset and load the informatio on a S3 folder:

's3a://datalakeeu/'

