from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as spark_min, max as spark_max, row_number, from_unixtime, year, month, lit, current_date
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    print("Starting SCD2 Session Dimension ETL...")

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("SCD2 Session Dimension ETL") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

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
    incoming_data = listen_events.groupBy("sessionId").agg(
        spark_min(col("ts")).alias("StartTime"),  # Minimum timestamp for session start
        spark_max(col("ts")).alias("EndTime")    # Maximum timestamp for session end
    ).withColumn(
        "StartTime", from_unixtime(col("StartTime") / 1000).cast("timestamp")
    ).withColumn(
        "EndTime", from_unixtime(col("EndTime") / 1000).cast("timestamp")
    )

    # Add SCD2 fields
    incoming_data = incoming_data.withColumn("start_date", lit(current_date())) \
                                 .withColumn("end_date", lit(None).cast("date")) \
                                 .withColumn("is_active", lit(True))

    # Read the existing Session Dimension
    try:
        existing_dim_session = spark.read.parquet(output_path)
        print("Successfully loaded the existing Session Dimension.")
    except Exception:
        print("No existing Session Dimension found. Initializing a new table.")
        existing_dim_session = spark.createDataFrame([], incoming_data.schema.add("session_id", IntegerType()))

    # Detect updated records (changes in StartTime or EndTime)
    print("Detecting updated sessions...")
    updates = existing_dim_session.join(
        incoming_data,
        on="sessionId",
        how="inner"
    ).filter(
        (existing_dim_session["StartTime"] != incoming_data["StartTime"]) |
        (existing_dim_session["EndTime"] != incoming_data["EndTime"])
    ).select(existing_dim_session["*"])

    # Mark old records as inactive
    updates = updates.withColumn("is_active", lit(False)).withColumn("end_date", lit(current_date()))

    # Detect new records (not in existing Session Dimension)
    print("Detecting new sessions...")
    new_records = incoming_data.join(
        existing_dim_session.select("sessionId"),
        on="sessionId",
        how="left_anti"
    )

    # Generate surrogate keys for new records
    print("Generating surrogate keys for new records...")
    window_spec = Window.orderBy("StartTime", "sessionId")
    new_records = new_records.withColumn(
        "session_id",
        row_number().over(window_spec) + existing_dim_session.count()
    )

    # Combine active, updated (inactive), and new records
    print("Combining active, updated, and new records...")
    unchanged_records = existing_dim_session.filter("is_active = TRUE").join(
        incoming_data.select("sessionId"), on="sessionId", how="left_anti"
    )
    final_dim_session = unchanged_records.union(updates).union(new_records)

    # Add year and month columns for partitioning
    final_dim_session = final_dim_session.withColumn("year", year(col("StartTime"))).withColumn("month", month(col("StartTime")))

    # Save the SCD2 Session Dimension
    print(f"Saving SCD2 Session Dimension to {output_path} with partitioning...")
    try:
        final_dim_session.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)
        print(f"SCD2 Session Dimension table saved to {output_path} with partitioning by year and month.")
    except Exception as e:
        print(f"Error saving the SCD2 session dimension: {e}")
        exit(1)

    # Stop SparkSession
    spark.stop()
    print("SCD2 Session Dimension ETL Completed.")
