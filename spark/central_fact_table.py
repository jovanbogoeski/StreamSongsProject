from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat

if __name__ == "__main__":
    print("Starting Fact_Event Table ETL...")

    # Initialize SparkSession
    spark = SparkSession.builder.appName("Fact_Event ETL").getOrCreate()

    # Paths
    base_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/listen_events"
    page_view_events_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/page_view_events"
    auth_events_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/auth_events"
    output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_fact/fact_event"

    # Load datasets
    print("Loading datasets...")
    listen_events = spark.read.parquet(base_path).distinct()
    page_view_events = spark.read.parquet(page_view_events_path).distinct()
    auth_events = spark.read.parquet(auth_events_path).distinct()

    # Ensure sessionId consistency
    listen_events = listen_events.withColumn("sessionId", col("sessionId").cast("string"))
    page_view_events = page_view_events.withColumn("sessionId", col("sessionId").cast("string"))
    auth_events = auth_events.withColumn("sessionId", col("sessionId").cast("string"))

    # Generate hash keys for FactEventID and dimensions
    listen_events = listen_events.withColumn(
        "FactEventID",
        sha2(concat(
            col("userId").cast("string"),
            col("sessionId"),
            col("ts").cast("string"),
            col("itemInSession").cast("string")
        ), 256)
    ).withColumn(
        "UserID", sha2(col("userId").cast("string"), 256)
    ).withColumn(
        "SessionID", sha2(col("sessionId"), 256)
    ).withColumn(
        "CityID", sha2(col("city").cast("string"), 256)
    ).withColumn(
        "SongID", sha2(col("song").cast("string"), 256)
    ).withColumn(
        "ArtistID", sha2(col("artist").cast("string"), 256)
    ).withColumn(
        "date_id", sha2(col("ts").cast("string"), 256)
    )

    # Extract page_type and success attributes
    print("Extracting page_type from page_view_events...")
    page_view = page_view_events.select("sessionId", "page").withColumnRenamed("page", "page_type")

    print("Extracting success status from auth_events...")
    auth_status = auth_events.select("sessionId", "success")

    # Combine data for Fact_Event table
    fact_event = listen_events.join(page_view, on="sessionId", how="left") \
                              .join(auth_status, on="sessionId", how="left") \
                              .select(
                                  "FactEventID",
                                  col("ts").alias("EventTimestamp"),
                                  col("userId").alias("UserID"),
                                  col("sessionId").alias("SessionID"),
                                  "CityID",
                                  col("itemInSession").alias("ItemInSession"),
                                  "SongID",
                                  "ArtistID",
                                  "date_id",
                                  "page_type",  # Ensure page_type exists
                                  col("success").cast("boolean")
                              )

    # Replace NULL values
    fact_event = fact_event.fillna({"page_type": "Unknown", "success": False})

    # Show and save Fact_Event table
    fact_event = fact_event.distinct()
    fact_event.show(10, truncate=False)
    fact_event.write.mode("overwrite").parquet(output_path)

    print(f"Fact_Event table saved successfully to {output_path}")
    spark.stop()
