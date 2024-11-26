from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lag, lit, sum as spark_sum, when, rank, min as spark_min, lead, to_date
)
from pyspark.sql.window import Window
import os

if __name__ == "__main__":
    print("Starting User Dimension ETL (SCD2)...")
    
    # Initialize SparkSession
    spark = SparkSession.builder.appName("User Dimension SCD2").getOrCreate()

    # Path to processed output
    base_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/listen_events"

    # Validate input path
    if not os.path.exists(base_path):
        print(f"Input path does not exist: {base_path}")
        exit(1)

    # Read the existing Parquet file
    listen_events = spark.read.parquet(base_path)
    print("Schema of listen_events:")
    listen_events.printSchema()

    # Convert necessary columns to appropriate types and rename
    listen_events = listen_events.select(
        col("userId").cast("bigint").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("gender"),
        to_date((col("registration") / 1000).cast("timestamp")).alias("registration_date"),
        col("level").alias("subscription_level"),
        col("ts").cast("timestamp").alias("event_date")
    ).filter(col("user_id").isNotNull())

    # Lag to detect subscription level changes
    window_user = Window.partitionBy("user_id").orderBy("event_date")
    listen_events_with_lag = listen_events.withColumn(
        "previous_subscription_level",
        lag("subscription_level", 1, "NA").over(window_user)
    ).withColumn(
        "level_changed",
        when(col("previous_subscription_level") != col("subscription_level"), 1).otherwise(0)
    )

    # Group distinct subscription level changes
    listen_events_with_groups = listen_events_with_lag.withColumn(
        "change_group",
        spark_sum("level_changed").over(window_user)
    )

    # Get the earliest event_date for each subscription level group
    window_group = Window.partitionBy("user_id", "change_group")
    min_date_per_group = listen_events_with_groups.withColumn(
        "valid_from",
        spark_min("event_date").over(window_group).cast("date")  # Ensure valid_from is a date
    )

    # Create valid_to for each row and detect current rows
    window_group_ordered = Window.partitionBy("user_id").orderBy("valid_from")
    user_dimension = min_date_per_group.withColumn(
        "valid_to",
        lead("valid_from", 1).over(window_group_ordered)  # Assign next group's valid_from to valid_to
    ).withColumn(
        "valid_to",
        when(col("valid_to").isNull(), lit("9999-12-31").cast("date")).otherwise(col("valid_to"))
    ).withColumn(
        "current_row",
        when(col("valid_to") == lit("9999-12-31").cast("date"), lit(1)).otherwise(lit(0))
    ).select(
        "user_id",
        "first_name",
        "last_name",
        "gender",
        "subscription_level",
        "registration_date",
        "valid_from",
        "valid_to",
        "current_row"
    )

    # Save the final user dimension table
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_dimension/dim_user"
    user_dimension.write.mode("overwrite").parquet(output_path)

    print(f"User dimension table saved to {output_path}")
    
    # Stop SparkSession
    spark.stop()
    print("User Dimension ETL Completed.")
