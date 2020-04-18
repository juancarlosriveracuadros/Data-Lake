# Data-Lake
the program is a database for a music streaming startup Sparkify. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The program is an ETL pipeline in PySpark code. the datasets reside in S3. Here are the S3 links for each:
Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.
