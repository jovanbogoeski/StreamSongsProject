from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_unixtime

if __name__ == "__main__":
    print("Starting Central Fact Table ETL...")

    # Initialize SparkSession
    spark = (
        SparkSession.builder
        .appName("Central Fact Table ETL")
        .config("spark.executor.memory", "4g")  # Executor memory
        .config("spark.executor.cores", "2")  # Number of cores per executor
        .config("spark.driver.memory", "2g")  # Memory for the driver node
        .getOrCreate()
    )

    # Paths for dimension and source data
    listen_events_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_topics/listen_events"
    dim_song_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/dimension_resul/dim_song"
    dim_artist_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/dimension_resul/dim_artist"
    dim_city_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/dimension_resul/dim_city"
    dim_datetime_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/dimension_resul/dim_datetime"
    dim_user_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/dimension_resul/dim_skuser_fact"
    dim_session_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/dimension_resul/dim_session"
    fact_output_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/fact_table"

    # Read the listen_events fact data
    listen_events = spark.read.parquet(listen_events_path)

    # Read the dimension tables
    dim_song = spark.read.parquet(dim_song_path)
    dim_artist = spark.read.parquet(dim_artist_path)
    dim_city = spark.read.parquet(dim_city_path)
    dim_datetime = spark.read.parquet(dim_datetime_path)
    dim_user = spark.read.parquet(dim_user_path)
    dim_session = spark.read.parquet(dim_session_path)

    # Step 1: Enrich the fact data with surrogate keys

    # Join with Song Dimension
    fact_table = listen_events.join(
        dim_song,
        (listen_events["song"] == dim_song["song_name"]) &
        (listen_events["duration"] == dim_song["song_duration"]),
        "left"
    ).select(
        listen_events["*"], dim_song["song_id"]
    )

    # Join with Artist Dimension
    fact_table = fact_table.join(
        dim_artist,
        (fact_table["artist"] == dim_artist["artist_name"]) &
        (fact_table["lat"] == dim_artist["latitude"]) &
        (fact_table["lon"] == dim_artist["longitude"]),
        "left"
    ).select(
        fact_table["*"], dim_artist["artist_id"]
    )

    # Join with City Dimension
    fact_table = fact_table.join(
        dim_city,
        (fact_table["city"] == dim_city["city_name"]) &
        (fact_table["state"] == dim_city["state"]) &
        (fact_table["zip"] == dim_city["zip_code"]) &
        (fact_table["lat"] == dim_city["latitude"]) &
        (fact_table["lon"] == dim_city["longitude"]),
        "left"
    ).select(
        fact_table["*"], dim_city["city_id"]
    )

    # Join with Date-Time Dimension
    fact_table = fact_table.join(
        dim_datetime,
        from_unixtime(fact_table["ts"] / 1000) == dim_datetime["timestamp"],
        "left"
    ).select(
        fact_table["*"], dim_datetime["datetime_id"]
    )

    # Join with User Dimension
    fact_table = fact_table.join(
        dim_user,
        fact_table["userId"] == dim_user["userId"],
        "left"
    ).select(
        fact_table["*"], dim_user["userKey"]
    )

    # Join with Session Dimension
    fact_table = fact_table.join(
        dim_session,
        fact_table["sessionId"] == dim_session["session_id"],
        "left"
    ).select(
        fact_table["*"], dim_session["session_id"].alias("session_id_fk")
    )

    # Step 2: Select Final Columns for the Fact Table
    fact_table_final = fact_table.select(
        "userKey",
        "song_id",
        "artist_id",
        "city_id",
        "datetime_id",
        "session_id_fk",
        "level",
        "ts",
        lit(1).alias("plays_count")  # Example: Add a plays_count metric for aggregation
    )

    # Step 3: Save the Central Fact Table
    try:
        fact_table_final.write.mode("overwrite").parquet(fact_output_path)
        print(f"Central Fact Table saved to {fact_output_path}")
    except Exception as e:
        print(f"Error saving Central Fact Table: {e}")
        exit(1)

    # Stop SparkSession
    spark.stop()
    print("Central Fact Table ETL Completed.")
