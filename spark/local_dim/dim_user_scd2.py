from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import (
    col, lag, lead, lit, when, sum as sum_col, min as min_col, row_number, from_unixtime
)
from pyspark.sql.window import Window

if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder.appName("Final User Dimension Fact Table").getOrCreate()

    # Paths
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/dimension_resul/dim_skuser_fact"
    base_path = os.path.dirname(output_path)

    # Ensure the base directory exists
    if not os.path.exists(base_path):
        os.makedirs(base_path)  # Create the base directory if it doesn't exist
        print(f"Created base directory: {base_path}")

    input_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_topics/listen_events"

    # Read the source data
    listen_events = spark.read.parquet(input_path)

    # **Step 1: Deduplicate the source data**
    listen_events = listen_events.dropDuplicates(["userId", "ts", "level"])

    # Convert `ts` from long (Unix timestamp) to Spark `timestamp`
    listen_events = listen_events.withColumn("ts", from_unixtime(col("ts") / 1000).cast("timestamp"))

    # **Step 2: Filter invalid rows (optional, based on your data quality needs)**
    listen_events = listen_events.filter(
        col("userId").isNotNull() & col("level").isNotNull() & col("ts").isNotNull()
    )

    # Step 3: Add a lag column to detect `level` changes
    window_spec = Window.partitionBy("userId", "firstName", "lastName", "gender").orderBy("ts")
    listen_events = listen_events.withColumn(
        "lagged_level", lag("level", 1).over(window_spec)
    ).withColumn(
        "level_changed", when(col("level") != col("lagged_level"), 1).otherwise(0)
    )

    # Step 4: Create distinct groups for each level state using cumulative sum
    listen_events = listen_events.withColumn(
        "grouped", sum_col("level_changed").over(window_spec)
    )

    # Step 5: Calculate rowActivationDate for each group
    level_groups = listen_events.groupBy(
        "userId", "firstName", "lastName", "gender", "level", "grouped"
    ).agg(
        min_col("ts").alias("rowActivationDate"),
        min_col("registration").alias("registration_date")
    )

    # Step 6: Add rowExpirationDate using lead function
    window_group_spec = Window.partitionBy("userId", "firstName", "lastName", "gender").orderBy("rowActivationDate")
    level_groups = level_groups.withColumn(
        "rowExpirationDate", lead("rowActivationDate", 1, "9999-12-31").over(window_group_spec)
    )

    # Step 7: Mark the current row (currentRow = 1 for the latest record)
    level_groups = level_groups.withColumn(
        "currentRow", when(
            row_number().over(window_group_spec.orderBy(col("rowActivationDate").desc())) == 1, 1
        ).otherwise(0)
    )

    # **Step 8: Filter to keep only the current row (most recent record) for each user**
    user_fact_table = level_groups.filter(col("currentRow") == 1)

    # Step 9: Deduplicate the final fact table (to ensure unique userId rows)
    user_fact_table = user_fact_table.dropDuplicates(["userId"])

    # Step 10: Generate a surrogate key
    user_fact_table = user_fact_table.withColumn(
        "userKey",
        col("userId").cast("string") + lit("-") + col("rowActivationDate").cast("string") + lit("-") + col("level")
    )

    # Step 11: Select final columns for the fact table
    user_fact_table = user_fact_table.select(
        "userKey",
        "userId",
        "firstName",
        "lastName",
        "gender",
        "level",
        "registration_date",
        "rowActivationDate",
        "rowExpirationDate",
        "currentRow"
    )

    # Step 12: Save the final fact table
    user_fact_table.write.mode("overwrite").parquet(output_path)
    print(f"Final User Dimension Fact Table Saved to {output_path}")

    # Stop SparkSession
    spark.stop()
