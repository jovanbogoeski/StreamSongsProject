from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

if __name__ == "__main__":
    print("Starting Artist Dimension ETL...")

    # Initialize SparkSession
    spark = SparkSession.builder.appName("Artist Dimension ETL").getOrCreate()

    # Paths
    base_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/listen_events"
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_dimension/dim_artist"

    # Read the existing Parquet file
    try:
        listen_events = spark.read.parquet(base_path)
        print("Successfully loaded the listen_events dataset.")
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        exit(1)

    # Validate required columns
    if "artist" not in listen_events.columns:
        print("Missing required column: artist")
        exit(1)

    # Extract unique artists
    dim_artist = listen_events.select(
        col("artist").alias("artist_name")
    ).distinct()

    # Define a window specification to order by artist_name
    window_spec = Window.orderBy("artist_name")

    # Generate sequential artist_id using row_number
    dim_artist_with_id = dim_artist.withColumn("artist_id", row_number().over(window_spec))

    # Show the output
    print("Artist Dimension Sample Output:")
    dim_artist_with_id.show(10, truncate=False)

    # Save the Artist Dimension
    try:
        dim_artist_with_id.write.mode("overwrite").parquet(output_path)
        print(f"Artist dimension table saved to {output_path}")
    except Exception as e:
        print(f"Error saving the artist dimension: {e}")
        exit(1)

    # Stop SparkSession
    spark.stop()
    print("Artist Dimension ETL Completed.")
