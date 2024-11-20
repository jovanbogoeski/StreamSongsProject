from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from schemas import auth_events_schema, listen_events_schema, page_view_events_schema, status_change_events_schema

# Kafka and output configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPICS = ['auth_events', 'listen_events', 'page_view_events', 'status_change_events']
OUTPUT_PATH = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/"
CHECKPOINT_PATH = "/mnt/c/Users/Jovan Bogoevski/StreamsSongs/processed_output/checkpoints/"

# Initialize Spark session
def initialize_spark(app_name="KafkaSparkStreaming"):
    """
    Initializes and returns a Spark session.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .getOrCreate()

# Read from Kafka
def read_from_kafka(spark, kafka_broker, topics):
    """
    Reads streaming data from Kafka topics.
    """
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", ",".join(topics)) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

# Parse Kafka topic
def parse_topic(df, topic, schema):
    """
    Parses a Kafka topic into a structured DataFrame based on the provided schema.
    """
    return df.filter(col("topic") == topic) \
             .selectExpr("CAST(value AS STRING) as json") \
             .select(from_json(col("json"), schema).alias("data")) \
             .select("data.*")

# Write DataFrame to Parquet
def write_stream(df, path, checkpoint_path):
    """
    Writes a streaming DataFrame to Parquet with checkpointing.
    """
    return df.writeStream \
             .format("parquet") \
             .option("path", path) \
             .option("checkpointLocation", checkpoint_path) \
             .trigger(processingTime="10 seconds") \
             .outputMode('append') \
             .start()

# Main function
def main():
    spark = initialize_spark()

    # Read Kafka streams
    raw_stream = read_from_kafka(spark, KAFKA_BROKER, KAFKA_TOPICS)

    # Parse each topic
    auth_df = parse_topic(raw_stream, "auth_events", auth_events_schema)
    listen_df = parse_topic(raw_stream, "listen_events", listen_events_schema)
    page_view_df = parse_topic(raw_stream, "page_view_events", page_view_events_schema)
    status_change_df = parse_topic(raw_stream, "status_change_events", status_change_events_schema)

    # Write each DataFrame to Parquet
    auth_query = write_stream(auth_df, OUTPUT_PATH + "auth_events", CHECKPOINT_PATH + "auth_events")
    listen_query = write_stream(listen_df, OUTPUT_PATH + "listen_events", CHECKPOINT_PATH + "listen_events")
    page_view_query = write_stream(page_view_df, OUTPUT_PATH + "page_view_events", CHECKPOINT_PATH + "page_view_events")
    status_change_query = write_stream(status_change_df, OUTPUT_PATH + "status_change_events", CHECKPOINT_PATH + "status_change_events")

    # Await termination
    auth_query.awaitTermination()
    listen_query.awaitTermination()
    page_view_query.awaitTermination()
    status_change_query.awaitTermination()

if __name__ == "__main__":
    main()
