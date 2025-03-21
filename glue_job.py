import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, row_number, date_format, hour, minute, second
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
import boto3
import json

# Get Glue job arguments
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "S3_BUCKET_NAME",
    "STREAMS_FOLDER",
    "DATA_FOLDER",
    "GENRE_KPIS",
    "TOP_SONGS",
    "TOP_GENRES"
])

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Glue Job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read Parameters
S3_BUCKET_NAME = args["S3_BUCKET_NAME"]
STREAMS_FOLDER = args["STREAMS_FOLDER"]
DATA_FOLDER = args["DATA_FOLDER"]

# DynamoDB Table Names (Passed as Parameters)
GENRE_KPIS = args["GENRE_KPIS"]
TOP_SONGS = args["TOP_SONGS"]
TOP_GENRES = args["TOP_GENRES"]

# Define S3 Paths
users_path = f"s3://{S3_BUCKET_NAME}/{DATA_FOLDER}/users.csv"
songs_path = f"s3://{S3_BUCKET_NAME}/{DATA_FOLDER}/songs.csv"
streams_folder_path = f"s3://{S3_BUCKET_NAME}/{DATA_FOLDER}/{STREAMS_FOLDER}"

try:
  # Read Data from S3
  users_df = spark.read.option("header", True).csv(users_path)
  songs_df = spark.read.option("header", True).csv(songs_path).select("track_id", "track_genre", "track_name")
  streams_df = spark.read.option("header", True).csv(streams_folder_path)
  
  # Data Cleaning
  songs_df = songs_df.withColumn("track_genre", col("track_genre").cast("string"))
  songs_df = songs_df.withColumn("track_name", col("track_name").cast("string"))
  streams_df = streams_df.withColumn("track_id", col("track_id").cast("string"))
  streams_df = streams_df.withColumn("date", date_format(col("listen_time"), "yyyy-MM-dd"))
  streams_df = streams_df.dropna(subset=["track_id", "user_id", "listen_time"])
  songs_df = songs_df.dropDuplicates(["track_id"])
  songs_df = songs_df.dropna(subset=["track_id", "track_genre", "track_name"])
  
  # Join Streams with Songs Data
  streams_songs_df = streams_df.alias("streams").join(
      songs_df.alias("songs"), col("streams.track_id") == col("songs.track_id"), "left"
  ).select(
      col("streams.date"),
      col("streams.track_id"),
      col("songs.track_name"),
      col("streams.user_id"),
      col("songs.track_genre"),
      col("streams.listen_time"),
  )
  
  # Calculate Listen Time in Seconds
  streams_songs_df = streams_songs_df.withColumn(
      "listen_time_seconds",
      hour(col("listen_time")) * 3600 + minute(col("listen_time")) * 60 + second(col("listen_time"))
  )
  
  # Filter out Invalid Genres
  streams_songs_df = streams_songs_df.filter(~col("track_genre").rlike("^[0-9]+(\\.[0-9]+)?$"))
  
  # Compute KPIs
  song_counts = streams_songs_df.groupBy("date", "track_genre", "track_id", "track_name").agg(
      count("track_id").alias("listen_count")
  )
  window_spec_songs = Window.partitionBy("date", "track_genre").orderBy(col("listen_count").desc())
  top_songs = song_counts.withColumn("rank", row_number().over(window_spec_songs)).filter(col("rank") <= 3)
  window_spec_genre = Window.partitionBy("date").orderBy(col("listen_count").desc())
  top_genres = song_counts.withColumn("rank", row_number().over(window_spec_genre)).filter(col("rank") <= 5)
  
  # Compute Genre-Level KPIs
  genre_kpis = streams_songs_df.groupBy("date", "track_genre").agg(
      count("*").alias("listen_count"),
      count("user_id").alias("unique_listeners"),
      sum("listen_time_seconds").alias("total_listening_time"),
      avg("listen_time_seconds").alias("avg_listening_time")
  )

  # Convert DataFrames to Glue DynamicFrames
  genre_kpis_dyf = DynamicFrame.fromDF(genre_kpis, glueContext, "genre_kpis_dyf")
  top_songs_dyf = DynamicFrame.fromDF(top_songs, glueContext, "top_songs_dyf")
  top_genres_dyf = DynamicFrame.fromDF(top_genres, glueContext, "top_genres_dyf")
  
  print(f"Writing to DynamoDB tables: {GENRE_KPIS}, {TOP_SONGS}, {TOP_GENRES}")

  # Write to DynamoDB using job parameters
  glueContext.write_dynamic_frame.from_options(
      frame=genre_kpis_dyf,
      connection_type="dynamodb",
      connection_options={"tableName": GENRE_KPIS,
                          "dynamodb.output.itemHashKey": "date",
                          "overwrite": "true"}
  )

  glueContext.write_dynamic_frame.from_options(
      frame=top_songs_dyf,
      connection_type="dynamodb",
      connection_options={"tableName": TOP_SONGS,
                          "dynamodb.output.itemHashKey": "track_id",
                          "overwrite": "true"}
  )

  glueContext.write_dynamic_frame.from_options(
      frame=top_genres_dyf,
      connection_type="dynamodb",
      connection_options={"tableName": TOP_GENRES,
                          "dynamodb.output.itemHashKey": "date",
                          "overwrite": "true"}
  )

  print("Data successfully loaded into DynamoDB!")

except Exception as e:
    print(f"Error during ETL process: {str(e)}")

print("Glue Job Completed Successfully! Data written to DynamoDB.")

# Commit the Glue job
job.commit()