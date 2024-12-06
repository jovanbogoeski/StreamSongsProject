from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, dayofweek, hour, minute, second, from_unixtime, row_number
)
from pyspark.sql.window import Window

if __name__ == "__main__":
    print("Starting Date-Time Dimension ETL...")

    # Initialize SparkSession
    spark = SparkSession.builder.appName("Date-Time Dimension ETL").getOrCreate()

    # Paths
    base_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_topics/listen_events"
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/dimension_resul/dim_datetime"

    # Read the existing Parquet file
    try:
        listen_events = spark.read.parquet(base_path)
        print("Successfully loaded the listen_events dataset.")
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        exit(1)

    # Validate timestamps and extract unique timestamps
    listen_events = listen_events.filter(col("ts").isNotNull() & (col("ts") > 0))

    dim_datetime = listen_events.select(
        from_unixtime(col("ts") / 1000).alias("timestamp")  # Convert ts (BIGINT) to TIMESTAMP
    ).distinct()

    # Add date and time components
    dim_datetime_with_components = dim_datetime.withColumn("date", col("timestamp").cast("date")) \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("day", dayofmonth(col("timestamp"))) \
        .withColumn("day_of_week", dayofweek(col("timestamp"))) \
        .withColumn("is_weekend", (col("day_of_week") >= 6).cast("boolean")) \
        .withColumn("hour", hour(col("timestamp"))) \
        .withColumn("minute", minute(col("timestamp"))) \
        .withColumn("second", second(col("timestamp")))

    # Add a sequential surrogate key (`datetime_id`) using `row_number()` ordered by `timestamp`
    window_spec = Window.orderBy("timestamp")
    dim_datetime_with_id = dim_datetime_with_components.withColumn(
        "datetime_id", row_number().over(window_spec)
    )

    # Reorder columns for clarity
    dim_datetime_final = dim_datetime_with_id.select(
        "datetime_id", "timestamp", "date", "year", "month", "day", "day_of_week", 
        "hour", "minute", "second", "is_weekend"
    )

    # Show the output on terminal
    print("Date-Time Dimension Sample Output:")
    dim_datetime_final.show(10, truncate=False)

    # Save the Date-Time Dimension with partitioning by year, month, and day
    try:
        dim_datetime_final.write.mode("overwrite").partitionBy("year", "month", "day").parquet(output_path)
        print(f"Date-Time dimension table saved to {output_path}")
    except Exception as e:
        print(f"Error saving the date-time dimension: {e}")
        exit(1)

    # Stop SparkSession
    spark.stop()
    print("Date-Time Dimension ETL Completed.")
