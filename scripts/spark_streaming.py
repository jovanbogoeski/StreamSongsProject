from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

# Kafka and output configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPICS = ['auth_events', 'listen_events', 'page_view_events', 'status_change_events']

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
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
    StructField("userId", IntegerType(), True),
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
    StructField("userId", IntegerType(), True),
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

# Helper function to parse each topic's JSON data
def parse_topic(df, topic, schema):
    return df.filter(col("topic") == topic) \
             .selectExpr("CAST(value AS STRING) as json") \
             .select(from_json(col("json"), schema).alias("data")) \
             .select("data.*")

# Parse each topic with its schema
auth_df = parse_topic(df, "auth_events", auth_events_schema)
listen_df = parse_topic(df, "listen_events", listen_events_schema)
page_view_df = parse_topic(df, "page_view_events", page_view_events_schema)
status_change_df = parse_topic(df, "status_change_events", status_change_events_schema)

# Write each DataFrame to Parquet files in streaming mode
def write_stream(df, path, checkpoint_path):
    return df.writeStream \
             .format("parquet") \
             .option("path", path) \
             .option("checkpointLocation", checkpoint_path) \
             .trigger(processingTime="1 minute") \
             .start()

auth_query = write_stream(auth_df, "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/auth_events", 
                          "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/checkpoints/auth_events")
listen_query = write_stream(listen_df, "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/listen_events", 
                            "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/checkpoints/listen_events")
page_view_query = write_stream(page_view_df, "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/page_view_events", 
                               "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/checkpoints/page_view_events")
status_change_query = write_stream(status_change_df, "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/status_change_events", 
                                   "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/checkpoints/status_change_events")

# Await termination of all streams
auth_query.awaitTermination()
listen_query.awaitTermination()
page_view_query.awaitTermination()
status_change_query.awaitTermination()
