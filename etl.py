import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"]=config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"]=config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    """
    Creates and returns Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, song_data, output_data):
    """
    Reads the song data files; creates songs and artists tables; writes partitioned parquet files to S3

            Parameters:
                    spark (SparkSession): Spark Session object
                    song_data (String): Link to the song data on S3
                    output_data (String): Link to the output S3 bucket
                    
    """

    # read song data files
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_df.select(
        col("song_id"),
        col("title"), 
        col("artist_id"), 
        col("year"), 
        col("duration")).distinct().filter(col("song_id").isNotNull())

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, "songs"), partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = song_df.select(
        col("artist_id"),
        col("artist_name").alias("name"),
        col("artist_location").alias("location"),
        col("artist_latitude").alias("latitude"),
        col("artist_longitude").alias("longitude")).distinct().filter(col("artist_id").isNotNull())
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists"))


def process_log_data(spark, song_data, log_data, output_data):
    """
    Reads the log data files; creates users, time and songplays tables; writes partitioned parquet files to S3

            Parameters:
                    spark (SparkSession): Spark Session object
                    song_data (String): Link to the song data on S3
                    log_data (String): Link to the log data on S3
                    output_data (String): Link to the output S3 bucket
                    
    """

    # read log data file
    log_df = spark.read.json(log_data)

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)
    
    # filter by actions for song plays
    log_df = log_df.filter(col("page") == "NextSong")

    # extract columns for users table    
    users_table = log_df.select(
        col("userId").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("gender"),
        col("level")).distinct().filter(col("userId").isNotNull())
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users"))

    # create timestamp column from original timestamp column
    timestamps = log_df.select(to_timestamp(from_unixtime(col("ts")/1000)).alias("start_time"))
    
    # extract columns to create time table
    time_table = timestamps.select(
        col("start_time"),
        hour(col("start_time")).alias("hour"),
        dayofmonth(col("start_time")).alias("day"),
        weekofyear(col("start_time")).alias("week"),
        month(col("start_time")).alias("month"),
        year(col("start_time")).alias("year"),
        dayofweek(col("start_time")).alias("weekday"))

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time"), partitionBy=["year","month"])

    # join song_data and log_data
    joined_song_log_df = log_df.join(song_df, 
        on=(log_df.song == song_df.title) & 
           (log_df.artist == song_df.artist_name) &
           (log_df.length == song_df.duration),
        how="inner")
    # joined_song_log_df.printSchema()
 
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = joined_song_log_df.select(
        monotonically_increasing_id().alias("songplay_id"),
        to_timestamp(from_unixtime(col("ts")/1000)).alias("start_time"),
        col("userID").alias("user_id"),
        col("level"),
        col("song_id"),
        col("artist_id"),
        col("sessionId").alias("session_id"),
        col("location"),
        col("userAgent").alias("user_agent")
    )

    songplays_table = songplays_table.withColumn("year", year(col("start_time"))).withColumn("month", month(col("start_time")))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplays"), partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    song_data = os.path.join(input_data, "song_data",'A','A','*')
    log_data = os.path.join(input_data, "log_data",'*','*','*')
    output_data = "s3://ud-de-nd-lrohrer-p4/"
    
    process_song_data(spark, song_data, output_data)    
    process_log_data(spark, song_data, log_data, output_data)


if __name__ == "__main__":
    main()
