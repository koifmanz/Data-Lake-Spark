import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: initialize spark sessio, to connect to 
    
    Arguments: 
        None
        
    Returns:
        spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function used to read log files and populate creating songs and artists tables
    
    Arguments: 
        spark: spark session
        input data: path for the input data
        output data: path for saving the resutlts
    
    Returns:
        None
    """
        
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    df = spark.read.json(song_data)
    
    # songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()
    songs_table.write.parquet(os.path.join(output_data, "songs/"), mode="overwrite", partitionBy=["year","artist_id"])
    
    # artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()
    artists_table.write.parquet(os.path.join(output_data, "artists/"), mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Description: This function used to read log files and extract data for users, time and songsplays tabels.
    
    Arguments: 
        spark: spark session
        input data: path for the input data
        output data: path for saving the resutlts
    
    Returns:
        None
    """
        
    log_data = os.path.join(input_data,"log_data/*/*/*.json")
    df = spark.read.json(log_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()
    df = df.filter(df.page == "NextSong")
   
    # users table
    users_table  = df.select("userId","firstName","lastName","gender","level").drop_duplicates()
    users_table = users_table.write.parquet(output_data + "users/", mode="overwrite")

    # time table
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    time_table = df.withColumn("hour",hour("start_time")).withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time")).withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()                      
    time_table.write.parquet(os.path.join(output_data, "time/"), mode='overwrite', partitionBy=["year","month"])
    
    # Songplayes table
    song_df = spark.read.format("parquet").option("basePath", output_data + "songs/").load(output_data +"songs/*/*/")
    
    song_plays_table = df.join(song_df, df.song == song_df.title, how='inner')\
                        .select(monotonically_increasing_id().alias("songplay_id"),col("start_time"),col("userId").alias("user_id"),"level","song_id","artist_id", col("sessionId")\
                        .alias("session_id"), "location", col("userAgent").alias("user_agent"))

    song_plays_table = songplays_table.join(time_table, song_plays_table.start_time == time_table.start_time, how="inner")\
                        .select("songplay_id", song_plays_table.start_time, "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent", "year", "month")

def main():
    """
    Description: running the ETL process, by calling process functions. 
                 need to enter output data location before running.
    
    Arguments: 
        None
    
    Returns:
        None
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
