from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

if __name__ == "__main__":
    print("Starting City Dimension ETL...")

    # Initialize SparkSession
    spark = SparkSession.builder.appName("City Dimension ETL").getOrCreate()

    # Paths
    base_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_topics/listen_events"
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/dimension_resul/dim_city"

    # Read the source data
    listen_events = spark.read.parquet(base_path)

    # Extract unique city-related information and remove duplicates
    dim_city = listen_events.select(
        col("city").alias("city_name"),
        col("state"),
        col("zip").alias("zip_code"),
        col("lat").alias("latitude"),
        col("lon").alias("longitude")
    ).dropDuplicates(["city_name", "state", "zip_code", "latitude", "longitude"])

    # Generate surrogate key using window function
    window_spec = Window.orderBy("city_name", "state", "zip_code", "latitude", "longitude")
    dim_city_with_id = dim_city.withColumn(
        "city_id",
        row_number().over(window_spec)
    )

    # Save the final city dimension table
    dim_city_with_id.write.mode("overwrite").parquet(output_path)
    print(f"City Dimension table saved to {output_path}")

    # Stop SparkSession
    spark.stop()
    print("City Dimension window ETL Completed.")
