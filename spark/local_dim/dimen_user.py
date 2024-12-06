from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lag, lit, sum as spark_sum, when, rank, min as spark_min, lead, to_date, monotonically_increasing_id
)
from pyspark.sql.window import Window
import os

if __name__ == "__main__":
    print("Starting User Dimension ETL (SCD2 with Surrogate Key)...")
    
    # Initialize SparkSession
    spark = SparkSession.builder.appName("User Dimension SCD2").getOrCreate()

    # Path to processed output
    base_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/listen_events"
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_dimension/dim_user_surogate"

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
    ).filter(col("user_id").isNotNull()).dropDuplicates()

    print(f"Rows processed: {listen_events.count()}")

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
    )

    # Deduplicate rows before assigning surrogate key
    user_dimension = user_dimension.dropDuplicates(
        ["user_id", "valid_from", "valid_to", "subscription_level"]
    )

    # Add surrogate key using monotonically_increasing_id
    user_dimension = user_dimension.withColumn(
        "user_key",
        monotonically_increasing_id()
    )

    # Select final columns
    user_dimension = user_dimension.select(
        "user_key",               # Surrogate key
        "user_id",                # Original business key
        "first_name",
        "last_name",
        "gender",
        "subscription_level",
        "registration_date",
        "valid_from",
        "valid_to",
        "current_row"
    )

    # Partition output by current_row for query optimization
    user_dimension.write.mode("overwrite").partitionBy("current_row").parquet(output_path)
    print(f"User dimension table with surrogate key saved to {output_path}")

    # Validation: Unique users in dimension table
    unique_users = user_dimension.select("user_id").distinct().count()
    print(f"Unique users in dimension table: {unique_users}")

    # Validation: Total rows in dimension table
    total_rows = user_dimension.count()
    print(f"Total rows in dimension table: {total_rows}")

    # Validation: Check for users with multiple current rows
    current_rows_check = user_dimension.filter(col("current_row") == 1).groupBy("user_id").count()
    incorrect_current_rows = current_rows_check.filter(col("count") > 1).count()
    print(f"Users with multiple current rows: {incorrect_current_rows}")

    # Validation: Check for missing user IDs
    missing_users = listen_events.select("user_id").distinct().subtract(
        user_dimension.select("user_id").distinct()
    )
    print(f"Missing user IDs in dimension table: {missing_users.count()}")

    # Stop SparkSession
    spark.stop()
    print("User Dimension ETL Completed.")
