from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Initialize SparkSession
spark = SparkSession.builder.appName("GroupBy UserId Sum Duration").getOrCreate()

data_path = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_topics/listen_events"

listen_events = spark.read.parquet(data_path)

listen_events.printSchema()

listen_events.show()
# Assuming `listen_events` is the DataFrame with a `duration` column and a `userId` column
# Group by 'userId' and calculate the sum of 'duration'


result = (
    listen_events.groupBy("userId")
    .agg(spark_sum("duration").alias("total_duration"))
)


# Show the results

unique_city_combinations = data.select(
    col("city").alias("city_name"),
    col("state"),
    col("zip").alias("zip_code"),
    col("lat").alias("latitude"),
    col("lon").alias("longitude"),
    col("duration")
)

# Group by the geographic attributes and calculate the total duration
result = (
    unique_city_combinations.groupBy("city_name", "state", "zip_code", "latitude", "longitude")
    .agg(spark_sum("duration").alias("total_duration"))
)
