# Data Lake with Spark and AWS

This project created for Udacity Data Engineer course. This project is the third one. The document will discuss the following topics:

1. The purpose of this database in the context of the startup, Sparkify, and their analytical goals.
2. The database schema design and the ETL pipeline.
3. example of queries and results for song play analysis.

___
## Purpose

Sparkify asks for db for improvent in their db, because their user base has grown. As their data engineer, the task is building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.    

___
## database schema

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song.


example from log file:
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

The database scheme is star-scheme based, to improve the queries logic and performance, Which fitting Sparkify request. The star-scheme strogest side is simplicity, which great for queries and aggregations, and because of that better performance. The following diagram show the db structure.  

Songplays table is the fact table, while the artists, songs and users are dimension tables.

___
## Running

1. Create EMR cluster on AWS.
2. Update the output location on _etl.py_
3. Update your AWS keys on _dl.cfg_
4. Run the following command in the console.
`python etl.py`

**Note**: Make sure to not upload your keys to github / another services.
