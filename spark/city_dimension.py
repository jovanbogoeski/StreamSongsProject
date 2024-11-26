from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

if __name__ == "__main__":
    print("Starting City Dimension ETL...")

    # Initialize SparkSession
    spark = SparkSession.builder.appName("City Dimension ETL").getOrCreate()

    # Path to processed output
    base_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/listen_events"
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_dimension/dim_city"

    # Read the existing Parquet file
    try:
        listen_events = spark.read.parquet(base_path)
        print("Successfully loaded the listen_events dataset.")
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        exit(1)

    # Print schema and preview the data
    print("Schema of listen_events:")
    listen_events.printSchema()
    print("Sample data from listen_events:")
    listen_events.show(5, truncate=False)

    # Validate required columns
    required_columns = ["city", "state", "zip", "lat", "lon"]
    missing_columns = [col for col in required_columns if col not in listen_events.columns]
    if missing_columns:
        print(f"Missing required columns: {missing_columns}")
        exit(1)

    # Extract unique city-related information
    dim_city = listen_events.select(
        col("city").alias("city_name"),
        col("state"),
        col("zip").alias("zip_code"),
        col("lat").alias("latitude"),
        col("lon").alias("longitude")
    ).distinct()

    # Generate a surrogate key (city_id) for each city
    dim_city_with_id = dim_city.withColumn("city_id", monotonically_increasing_id())

    # Print the extracted city dimension data with city_id
    print("Sample data for City Dimension:")
    dim_city_with_id.show(10, truncate=False)

    # Save the final city dimension table
    try:
        dim_city_with_id.write.mode("overwrite").parquet(output_path)
        print(f"City dimension table saved to {output_path}")
    except Exception as e:
        print(f"Error saving the city dimension: {e}")
        exit(1)

    # Stop SparkSession
    spark.stop()
    print("City Dimension ETL Completed.")
