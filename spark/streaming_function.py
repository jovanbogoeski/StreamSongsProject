from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, from_unixtime, to_date, rand
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def initialize_spark(app_name="SparkApplication", master="yarn"):
    """
    Initializes and returns a Spark session configured for YARN.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
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

def replace_with_random_dates(df, column_name="registration", start_date="2021-01-01", end_date="2021-12-31"):
    """
    Replaces all values in the specified column with random dates within a specified range.
    """
    try:
        if df is None:
            raise ValueError("Input DataFrame is None. Cannot replace values.")
        
        # Calculate the number of days between start_date and end_date
        days_between = f"datediff(date('{end_date}'), date('{start_date}'))"
        
        # Generate random dates by adding a random number of days to the start_date
        updated_df = df.withColumn(
            column_name,
            expr(f"date_add(date('{start_date}'), cast(rand() * {days_between} as int))")
        )
        
        logger.info(f"Successfully replaced all values in '{column_name}' with random dates between {start_date} and {end_date}.")
        return updated_df
    except Exception as e:
        logger.error(f"Error replacing values in '{column_name}': {e}")
        raise e

def convert_timestamp_to_date_inplace(df, column_name="ts"):
    """
    Converts a timestamp column (in milliseconds) to a date column in the given DataFrame,
    overwriting the original column.
    """
    try:
        if df is None:
            raise ValueError("Input DataFrame is None. Cannot convert timestamps.")
        
        # Ensure the column is numeric
        df = df.withColumn(column_name, col(column_name).cast("long"))
        
        # Convert milliseconds to seconds and then to date
        logger.info(f"Converting '{column_name}' from milliseconds to date format.")
        df = df.withColumn(column_name, to_date(from_unixtime(col(column_name) / 1000)))
        
        logger.info(f"Successfully converted '{column_name}' to date format.")
        return df
    except Exception as e:
        logger.error(f"Error converting column '{column_name}' to date: {e}")
        raise e

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

def process_topic(df, topic, schema, output_path, checkpoint_path):
    """
    Processes a single Kafka topic: parse, transform, and write the stream.
    """
    try:
        logger.info(f"Processing topic: {topic}")

        # Parse the topic into a structured DataFrame
        parsed_df = parse_topic(df, topic, schema)
        if parsed_df is None:
            logger.error(f"Failed to parse topic '{topic}'. Skipping.")
            return None

        # Replace registration values with random dates
        updated_df = replace_with_random_dates(parsed_df, column_name="registration", start_date="2021-01-01", end_date="2021-12-31")
        if updated_df is None:
            logger.error(f"Failed to replace registration values for topic '{topic}'. Skipping.")
            return None

        # Convert 'ts' column to a date column
        updated_df = convert_timestamp_to_date_inplace(updated_df, column_name="ts")
        logger.info(f"Successfully converted 'ts' column to date for topic '{topic}'.")

        # Write the updated DataFrame to the specified output
        query = write_stream(updated_df, output_path, checkpoint_path)
        if query:
            logger.info(f"Started query for topic '{topic}'")
        return query

    except Exception as e:
        logger.error(f"Error processing topic '{topic}': {e}")
        return None
