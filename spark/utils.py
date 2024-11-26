from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_spark(app_name="SparkApplication"):
    """
    Initializes and returns a Spark session.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

def read_from_kafka(spark, kafka_broker, topics):
    """
    Reads streaming data from Kafka topics.
    """
    if not topics:
        logger.error("No Kafka topics provided. Exiting.")
        raise ValueError("Kafka topics must be provided.")
    
    logger.info(f"Subscribing to Kafka topics: {topics}")
    
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", ",".join(topics)) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

def parse_topic(df, topic, schema):
    """
    Parses a Kafka topic into a structured DataFrame based on the provided schema.
    """
    try:
        if not schema:
            logger.error(f"No schema provided for topic '{topic}'. Skipping parsing.")
            return None
        
        parsed_df = df.filter(col("topic") == topic) \
                      .selectExpr("CAST(value AS STRING) as json") \
                      .select(from_json(col("json"), schema).alias("data")) \
                      .select("data.*")
        
        logger.info(f"Successfully parsed topic: {topic}")
        return parsed_df
    except Exception as e:
        logger.error(f"Error parsing topic '{topic}': {e}")
        return None

def write_stream(df, output_path, checkpoint_path, interval="10 seconds"):
    """
    Writes a streaming DataFrame to storage with checkpointing.
    """
    try:
        if df is None:
            logger.warning(f"No data to write for output path: {output_path}. Skipping...")
            return None
        
        query = df.writeStream \
                 .format("parquet") \
                 .option("path", output_path) \
                 .option("checkpointLocation", checkpoint_path) \
                 .trigger(processingTime=interval) \
                 .outputMode("append") \
                 .start()
        
        logger.info(f"Started writing stream to {output_path}")
        return query
    except Exception as e:
        logger.error(f"Error writing stream to {output_path}: {e}")
        return None
