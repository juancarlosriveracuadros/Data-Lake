%%spark

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T

def process_song_data(spark, input_data, output_data):
    """
    Description: load the datasets fromo the S3 folder and create and write as parquet the tables songs_table and artists_table 
    Arguments:
        spark: no necesary for this case only local  
        input_data: path of the dataset
        output_data: path of the output dataset
    Returns:
        None
    """
    
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration'].drop_duplicates(subset=['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + 'songs_table', partitionBy=['year', 'artist_id'], mode='Overwrite')

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'].drop_duplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists_table', mode='Overwrite')

def process_log_data(spark, input_data, output_data):
    """
    Description: load the datasets fromo the S3 folder and create and write as parquet the tables users_table, time_table and songplay_table 
    Arguments:
        spark: no necesary for this case only local  
        input_data: path of the dataset
        output_data: path of the output dataset
    Returns:
        None
    """
    # get filepath to log data file
    log_data = input_data

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(F.col("page") == "NextSong")

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level'].dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users_table', mode='Overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    
    # create datetime columns from original timestamp column
    get_hour = F.udf(lambda x: x.hour, T.IntegerType()) 
    get_day = F.udf(lambda x: x.day, T.IntegerType()) 
    get_week = F.udf(lambda x: x.isocalendar()[1], T.IntegerType()) 
    get_month = F.udf(lambda x: x.month, T.IntegerType()) 
    get_year = F.udf(lambda x: x.year, T.IntegerType()) 
    get_weekday = F.udf(lambda x: x.weekday(), T.IntegerType()) 
    
    df = df.withColumn("hour", get_hour(df.timestamp))
    df = df.withColumn("day", get_day(df.timestamp))
    df = df.withColumn("week", get_week(df.timestamp))
    df = df.withColumn("month", get_month(df.timestamp))
    df = df.withColumn("year", get_year(df.timestamp))
    df = df.withColumn("year_ts", get_year(df.timestamp))
    df = df.withColumn("weekday", get_weekday(df.timestamp)) 

    # extract columns to create time table
    time_table =df['timestamp','hour','day','week','month','year','weekday'].dropDuplicates() 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time_table', partitionBy=['year', 'month'], mode='Overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.option("mergeSchema", "true").parquet(output_data + "songs_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplay_table = song_df.join(df, song_df.title == df.song).select('timestamp','year_ts','month', 'userId', 'level', 'song_id','artist_id', 'sessionId')
    songplay_table = songplay_table.withColumn("songplay_id", F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplay_table.write.parquet(output_data + 'songplay_table', partitionBy=['year_ts', 'month'], mode='Overwrite')

def main():
    output_data = 's3a://datalakeeu/'
    intput_data_song = 's3a://udacity-dend/song_data/*/*/*/*.json'
    intpunt_data_log = 's3a://udacity-dend/log_data/*.json'
    
    process_song_data(spark, intput_data_song, output_data)    
    process_log_data(spark, intpunt_data_log, output_data)

    
main()