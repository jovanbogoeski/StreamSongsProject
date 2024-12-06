from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

if __name__ == "__main__":
    print("Starting Artist Dimension ETL...")

    # Initialize SparkSession
    spark = SparkSession.builder.appName("Artist Dimension ETL").getOrCreate()

    # Paths
    base_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_topics/listen_events"
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/dimension_resul/dim_artist"

    # Read the source data
    listen_events = spark.read.parquet(base_path)

    # Extract artist-related information and remove invalid rows
    dim_artist = listen_events.select(
        col("artist").alias("artist_name"),
        col("lat").alias("latitude"),
        col("lon").alias("longitude")
    ).filter(
        (col("artist_name").isNotNull()) & 
        (col("latitude").isNotNull()) & 
        (col("longitude").isNotNull())
    )

    # Remove duplicates
    dim_artist = dim_artist.dropDuplicates(["artist_name", "latitude", "longitude"])

    # Generate surrogate key using window function
    window_spec = Window.orderBy("artist_name", "latitude", "longitude")
    dim_artist_with_id = dim_artist.withColumn(
        "artist_id",
        row_number().over(window_spec)
    )

    # Save the final artist dimension table
    dim_artist_with_id.write.mode("overwrite").parquet(output_path)
    print(f"Artist Dimension with window table saved to {output_path}")

    # Stop SparkSession
    spark.stop()
    print("Artist Dimension ETL Completed.")
