from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, row_number
from pyspark.sql.window import Window

if __name__ == "__main__":
    print("Starting Artist Dimension SCD2 ETL...")

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Artist Dimension SCD2 ETL") \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "12g") \
        .config("spark.executor.cores", "4") \
        .config("spark.sql.shuffle.partitions", "300") \
        .getOrCreate()

    # Paths
    base_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_topics/listen_events"
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/dimension_resul/dim_artist"

    # Read source data
    listen_events = spark.read.parquet(base_path)
    incoming_data = listen_events.select(
        col("artist").alias("artist_name"),
        col("lat").alias("latitude"),
        col("lon").alias("longitude")
    ).filter(
        (col("artist_name").isNotNull()) & 
        (col("latitude").isNotNull()) & 
        (col("longitude").isNotNull())
    ).dropDuplicates(["artist_name", "latitude", "longitude"])

    # Add SCD2 fields
    incoming_data = incoming_data.withColumn("start_date", lit(current_date())) \
                                 .withColumn("end_date", lit(None).cast("date")) \
                                 .withColumn("is_active", lit(True).cast("boolean"))

    # Read existing dimension table
    try:
        existing_dim_artist = spark.read.parquet(output_path)
        print("Existing Artist Dimension table loaded.")
    except Exception as e:
        schema = incoming_data.withColumn("artist_id", lit(None).cast("int")).schema
        existing_dim_artist = spark.createDataFrame([], schema)

    # Cast is_active to BOOLEAN for consistency
    existing_dim_artist = existing_dim_artist.withColumn("is_active", col("is_active").cast("boolean"))

    # Detect updates
    updates = existing_dim_artist.join(
        incoming_data,
        on="artist_name",
        how="inner"
    ).filter(
        (existing_dim_artist["latitude"] != incoming_data["latitude"]) |
        (existing_dim_artist["longitude"] != incoming_data["longitude"])
    ).select(existing_dim_artist["*"]).withColumn("is_active", lit(False).cast("boolean")) \
                                       .withColumn("end_date", lit(current_date()))

    # Find new records
    new_records = incoming_data.join(
        existing_dim_artist.select("artist_name", "latitude", "longitude"),
        on=["artist_name", "latitude", "longitude"],
        how="left_anti"
    )

    # Generate unique artist_id for new records
    window_spec = Window.orderBy("artist_name", "latitude", "longitude")
    new_records = new_records.withColumn(
        "artist_id",
        row_number().over(window_spec) + existing_dim_artist.count()
    ).withColumn("is_active", col("is_active").cast("boolean"))

    # Align schemas for all DataFrames before union
    updates = updates.select(*existing_dim_artist.columns)
    existing_dim_artist = existing_dim_artist.select(*existing_dim_artist.columns)
    new_records = new_records.select(*existing_dim_artist.columns)

    # Combine all records
    final_dim_artist = existing_dim_artist.filter("is_active = True") \
                                          .union(updates) \
                                          .union(new_records)

    # Write partitioned output
    final_dim_artist.coalesce(4).write \
        .partitionBy("is_active") \
        .mode("overwrite") \
        .parquet(output_path)

    print(f"Artist Dimension table updated and saved to {output_path}")
    spark.stop()
    print("Artist Dimension SCD2 ETL Completed.")
