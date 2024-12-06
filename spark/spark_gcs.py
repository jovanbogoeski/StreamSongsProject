import os
from utils import initialize_spark, read_from_kafka, process_topic
from schemas import schema_mapping
import logging

# Configure logging
def setup_logging():
    """Sets up the logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler()  # Logs to the console
        ],
    )
    logger = logging.getLogger(__name__)
    logger.info("Logging has been configured.")
    return logger

# Initialize logging
logger = setup_logging()

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPICS = os.getenv(
    "KAFKA_TOPICS", "auth_events,listen_events,page_view_events,status_change_events"
).split(",")
GCS_BUCKET = os.getenv("GCS_BUCKET", "your-gcs-bucket-name")
OUTPUT_BASE_PATH = f"gs://{GCS_BUCKET}/output"
CHECKPOINT_BASE_PATH = f"gs://{GCS_BUCKET}/checkpoints"
PROCESSING_INTERVAL = os.getenv("PROCESSING_INTERVAL", "1 minute")

def main():
    """Main entry point of the script."""
    try:
        # Initialize Spark session with GCS configurations
        logger.info("Initializing Spark session...")
        spark = initialize_spark("KafkaSparkStreaming")
        spark._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.enable", "true")
        spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

        # Read raw Kafka stream
        logger.info(f"Reading data from Kafka topics: {KAFKA_TOPICS}...")
        raw_stream = read_from_kafka(spark, KAFKA_BROKER, KAFKA_TOPICS)

        # Process each topic
        queries = []
        for topic in KAFKA_TOPICS:
            logger.info(f"Processing topic: {topic}")
            
            # Get the schema for the current topic
            schema = schema_mapping.get(topic)
            if not schema:
                logger.warning(f"Schema for topic '{topic}' not found. Skipping.")
                continue

            # Define output and checkpoint paths
            output_path = os.path.join(OUTPUT_BASE_PATH, topic)
            checkpoint_path = os.path.join(CHECKPOINT_BASE_PATH, topic)

            # Process the topic (parse, transform, and write)
            query = process_topic(raw_stream, topic, schema, output_path, checkpoint_path)
            if query:
                queries.append(query)

        # Await termination for all streaming queries
        for query in queries:
            query.awaitTermination()

    except Exception as e:
        logger.error(f"An error occurred in the main function: {e}")
        raise

if __name__ == "__main__":
    logger.info("Starting the Kafka streaming application...")
    main()
