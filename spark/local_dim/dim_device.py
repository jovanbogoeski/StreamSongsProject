from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, row_number
from pyspark.sql.window import Window

if __name__ == "__main__":
    print("Starting Device Dimension ETL...")

    # Initialize SparkSession
    spark = SparkSession.builder.appName("Device Dimension ETL").getOrCreate()

    # Paths
    source_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_topics/listen_events"
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/dimension_resul/dim_device"

    # Read the source data
    listen_events = spark.read.parquet(source_path)

    # Extract unique userAgent strings
    user_agents = listen_events.select("userAgent").distinct()

    # Extract device type, operating system, and browser using regex
    device_dimension = user_agents.withColumn(
        "device_type", 
        regexp_extract(col("userAgent"), r"(Mobile|Tablet|iPad|iPhone|Desktop|Bot)", 1)
    ).withColumn(
        "operating_system", 
        regexp_extract(col("userAgent"), r"(Windows|Mac OS X|Linux|Android|iOS|iPad|iPhone)", 1)
    ).withColumn(
        "browser", 
        regexp_extract(col("userAgent"), r"(Chrome|Firefox|Safari|Edge|Opera|IE|Trident|SamsungBrowser)", 1)
    )

    # Default values for unmatched cases
    device_dimension = device_dimension.fillna({
        "device_type": "Unknown",
        "operating_system": "Unknown",
        "browser": "Unknown"
    })

    # Add a surrogate key (device_id) using row_number
    window_spec = Window.orderBy("userAgent")
    device_dimension_with_id = device_dimension.withColumn(
        "device_id", row_number().over(window_spec)
    )

    # Select only the required columns
    device_dimension_final = device_dimension_with_id.select(
        "device_id", "device_type", "operating_system", "browser"
    )

    # Show the output and schema
    print("Device Dimension Sample Output:")
    device_dimension_final.show(10, truncate=False)
    device_dimension_final.printSchema()

    # Save the Device Dimension
    device_dimension_final.write.mode("overwrite").parquet(output_path)
    print(f"Device dimension table saved to {output_path}")

    # Stop SparkSession
    spark.stop()
    print("Device Dimension ETL Completed.")
