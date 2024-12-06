from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

if __name__ == "__main__":
    print("Starting Song Dimension ETL...")

    # Initialize SparkSession
    spark = SparkSession.builder.appName("Song Dimension ETL").getOrCreate()

    # Paths
    base_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_topics/listen_events"
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/dimension_resul/dim_song"

    # Read the source data
    listen_events = spark.read.parquet(base_path)
    print(f"Total rows in source dataset: {listen_events.count()}")

    # Step 1: Extract song-related information
    dim_song = listen_events.select(
        col("song").alias("song_name"),
        col("duration").alias("song_duration")
    )

    # Step 2: Filter out invalid rows
    dim_song = dim_song.filter(
        col("song_name").isNotNull() &
        col("song_duration").isNotNull()
    )
    print(f"Rows after filtering invalid data: {dim_song.count()}")

    # Step 3: Remove duplicates based on song_name and song_duration
    dim_song = dim_song.dropDuplicates(["song_name", "song_duration"])
    print(f"Rows after deduplication: {dim_song.count()}")

    # Step 4: Generate surrogate key using window function
    # Define window specification with ordering
    window_spec = Window.orderBy("song_name", "song_duration")
    
    # Assign song_id using row_number over the window
    dim_song_with_id = dim_song.withColumn(
        "song_id",
        row_number().over(window_spec)
    )

    # Step 5: Save the final song dimension table
    dim_song_with_id.write.mode("overwrite").parquet(output_path)
    print(f"Song Dimension table saved to {output_path}")

    # Stop SparkSession
    spark.stop()
    print("Song Dimension ETL Completed.")
