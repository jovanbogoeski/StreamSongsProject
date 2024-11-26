from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

if __name__ == "__main__":
    print("Starting Song Dimension ETL...")

    # Initialize SparkSession
    spark = SparkSession.builder.appName("Song Dimension ETL").getOrCreate()

    # Paths
    base_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/listen_events"
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_dimension/dim_song"

    # Read the existing Parquet file
    try:
        listen_events = spark.read.parquet(base_path)
        print("Successfully loaded the listen_events dataset.")
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        exit(1)

    # Validate required columns
    required_columns = ["song", "duration"]
    missing_columns = [col for col in required_columns if col not in listen_events.columns]
    if missing_columns:
        print(f"Missing required columns: {missing_columns}")
        exit(1)

    # Extract unique songs
    dim_song = listen_events.select(
        col("song").alias("song_name"),
        col("duration").alias("song_duration")
    ).distinct()

    # Define a window specification to order by song_name
    window_spec = Window.orderBy("song_name")

    # Generate sequential song_id using row_number
    dim_song_with_id = dim_song.withColumn("song_id", row_number().over(window_spec))

    # Rearrange columns to start with song_id
    dim_song_final = dim_song_with_id.select(
        "song_id",  # Surrogate key first
        "song_name",
        "song_duration"
    )

    # Show the output
    print("Song Dimension Sample Output:")
    dim_song_final.show(10, truncate=False)

    # Save the Song Dimension
    try:
        dim_song_final.write.mode("overwrite").parquet(output_path)
        print(f"Song dimension table saved to {output_path}")
    except Exception as e:
        print(f"Error saving the song dimension: {e}")
        exit(1)

    # Stop SparkSession
    spark.stop()
    print("Song Dimension ETL Completed.")
