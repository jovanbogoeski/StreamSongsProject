from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

# Kafka and output configuration
KAFKA_BROKER = 'kafka:9092'  # Kafka service name in Docker Compose
KAFKA_TOPICS = ['auth_events', 'listen_events', 'page_view_events', 'status_change_events']
OUTPUT_DIRECTORY = '/tmp/processed_output/'

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Define schemas based on your topics
auth_events_schema = StructType([
    StructField("ts", TimestampType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("zip", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("userId", DoubleType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", DoubleType(), True),
    StructField("success", BooleanType(), True)
])

listen_events_schema = StructType([
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("ts", TimestampType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("auth", StringType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("zip", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("userId", IntegerType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", IntegerType(), True)
])

page_view_events_schema = StructType([
    StructField("ts", TimestampType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("page", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("method", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("zip", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("userId", DoubleType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", DoubleType(), True),
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("duration", DoubleType(), True)
])

status_change_events_schema = StructType([
    StructField("ts", TimestampType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("auth", StringType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("zip", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("userId", IntegerType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", IntegerType(), True)
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", ",".join(KAFKA_TOPICS)) \
    .option("startingOffsets", "earliest") \
    .load()

# Decode Kafka message values from JSON for each topic
auth_df = df.filter(df.topic == "auth_events") \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), auth_events_schema).alias("data")) \
    .select("data.*")

listen_df = df.filter(df.topic == "listen_events") \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), listen_events_schema).alias("data")) \
    .select("data.*")

page_view_df = df.filter(df.topic == "page_view_events") \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), page_view_events_schema).alias("data")) \
    .select("data.*")

status_change_df = df.filter(df.topic == "status_change_events") \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), status_change_events_schema).alias("data")) \
    .select("data.*")

# Write each DataFrame to Parquet files in streaming mode
auth_query = auth_df.writeStream \
    .format("parquet") \
    .option("path", f"{OUTPUT_DIRECTORY}/auth_events") \
    .option("checkpointLocation", f"{OUTPUT_DIRECTORY}/checkpoints/auth_events") \
    .trigger(processingTime="1 minute") \
    .start()

listen_query = listen_df.writeStream \
    .format("parquet") \
    .option("path", f"{OUTPUT_DIRECTORY}/listen_events") \
    .option("checkpointLocation", f"{OUTPUT_DIRECTORY}/checkpoints/listen_events") \
    .trigger(processingTime="1 minute") \
    .start()

page_view_query = page_view_df.writeStream \
    .format("parquet") \
    .option("path", f"{OUTPUT_DIRECTORY}/page_view_events") \
    .option("checkpointLocation", f"{OUTPUT_DIRECTORY}/checkpoints/page_view_events") \
    .trigger(processingTime="1 minute") \
    .start()

status_change_query = status_change_df.writeStream \
    .format("parquet") \
    .option("path", f"{OUTPUT_DIRECTORY}/status_change_events") \
    .option("checkpointLocation", f"{OUTPUT_DIRECTORY}/checkpoints/status_change_events") \
    .trigger(processingTime="1 minute") \
    .start()

# Await termination of all streams
auth_query.awaitTermination()
listen_query.awaitTermination()
page_view_query.awaitTermination()
status_change_query.awaitTermination()
