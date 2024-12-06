from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as spark_min, max as spark_max, row_number, from_unixtime, year, month
from pyspark.sql.window import Window

if __name__ == "__main__":
    print("Starting Simplified Session Dimension ETL...")

    # Initialize SparkSession
    spark = SparkSession.builder.appName("Simplified Session Dimension ETL").getOrCreate()

    # Paths
    base_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_topics/listen_events"
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/dimension_resul/dim_session"

    # Read the existing Parquet file
    try:
        listen_events = spark.read.parquet(base_path)
        print("Successfully loaded the listen_events dataset.")
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        exit(1)

    # Validate required columns
    required_columns = ["sessionId", "ts"]
    missing_columns = [col for col in required_columns if col not in listen_events.columns]
    if missing_columns:
        print(f"Missing required columns: {missing_columns}")
        exit(1)

    # Calculate start and end times for each session and convert epoch to datetime
    print("Calculating session start and end times...")
    session_time_window = listen_events.groupBy("sessionId").agg(
        spark_min(col("ts")).alias("StartTime"),  # Minimum timestamp for session start
        spark_max(col("ts")).alias("EndTime")    # Maximum timestamp for session end
    ).withColumn(
        "StartTime", from_unixtime(col("StartTime") / 1000).cast("timestamp")
    ).withColumn(
        "EndTime", from_unixtime(col("EndTime") / 1000).cast("timestamp")
    )

    # Add a sequential surrogate key for session_id using row_number()
    print("Generating surrogate keys for session_id...")
    window_spec = Window.orderBy("StartTime", "sessionId")  # Tie-breaker for consistent order
    dim_session_with_id = session_time_window.withColumn(
        "session_id", row_number().over(window_spec)
    ).select(
        "session_id",
        col("StartTime"),
        col("EndTime")
    )

    # Add year and month columns for partitioning
    dim_session_partitioned = dim_session_with_id.withColumn("year", year(col("StartTime"))).withColumn("month", month(col("StartTime")))

    # Show the output and schema on terminal
    print("Simplified Session Dimension Sample Output:")
    dim_session_partitioned.show(10, truncate=False)
    dim_session_partitioned.printSchema()

    # Save the Session Dimension with partitioning by year and month
    print(f"Saving Simplified Session Dimension to {output_path} with partitioning...")
    try:
        dim_session_partitioned.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)
        print(f"Simplified Session Dimension table saved to {output_path} with partitioning by year and month.")
    except Exception as e:
        print(f"Error saving the simplified session dimension: {e}")
        exit(1)

    # Stop SparkSession
    spark.stop()
    print("Simplified Session Dimension ETL Completed.")
