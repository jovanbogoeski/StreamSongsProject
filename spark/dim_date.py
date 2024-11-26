from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, dayofweek, from_unixtime, row_number
from pyspark.sql.window import Window

if __name__ == "__main__":
    print("Starting Date Dimension ETL...")

    # Initialize SparkSession
    spark = SparkSession.builder.appName("Date Dimension ETL").getOrCreate()

    # Paths
    base_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/listen_events"
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_dimension/dim_date"

    # Read the existing Parquet file
    try:
        listen_events = spark.read.parquet(base_path)
        print("Successfully loaded the listen_events dataset.")
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        exit(1)

    # Extract unique dates from the `ts` column
    dim_date = listen_events.select(
        from_unixtime(col("ts") / 1000).cast("date").alias("date")  # Convert ts (BIGINT) to DATE
    ).distinct()

    # Add date components
    dim_date_with_components = dim_date.withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("day", dayofmonth(col("date"))) \
        .withColumn("day_of_week", dayofweek(col("date"))) \
        .withColumn("is_weekend", (col("day_of_week") >= 6).cast("boolean"))

    # Add a sequential surrogate key (`date_id`) using `row_number()`
    window_spec = Window.orderBy("date")
    dim_date_with_id = dim_date_with_components.withColumn("date_id", row_number().over(window_spec))

    # Reorder columns for clarity
    dim_date_final = dim_date_with_id.select(
        "date_id", "date", "year", "month", "day", "day_of_week", "is_weekend"
    )

    # Show the output on terminal
    print("Date Dimension Sample Output:")
    dim_date_final.show(10, truncate=False)

    # Save the Date Dimension
    try:
        dim_date_final.write.mode("overwrite").parquet(output_path)
        print(f"Date dimension table saved to {output_path}")
    except Exception as e:
        print(f"Error saving the date dimension: {e}")
        exit(1)

    # Stop SparkSession
    spark.stop()
    print("Date Dimension ETL Completed.")
